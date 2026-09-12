"""Bronze: the ACS national population series.

One JSON document rather than a tab-delimited file, so three things differ from
the BLS tables.

The schema is declared, not inferred and not sidestepped. pr_data is read as
raw text because BLS pads its headers and defeats name matching; that problem
does not exist here, and this source ships real types. Declaring them is
reading, not interpreting - and anything the declaration does not account for
lands in _rescued_data rather than being silently absorbed.

Read as a stream, like pr_data, because Silver applies AUTO CDC keyed on year.
ACS estimates carry a vintage: the 2018 figure published in 2019 is not
necessarily the 2018 figure published in 2024. Appending each landed version
and letting Silver sequence them keeps that revision history queryable, which
is the same argument that put BLS observations under SCD Type 2.

acs_population, not population, because the cube matters. The ACS 1-year and
5-year cubes both publish a national population by year and they do not agree;
the expected answers reconcile only with the 1-year series. The name records
which one this is.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()

LANDING = spark.conf.get("landing_path")

POPULATION_PATH = f"{LANDING}/population/"
POPULATION_GLOB = "*.json"

# The source's own declaration of its shape, echoed back inside every payload's
# "columns" array. Checked on every run - the JSON analogue of the BLS header
# contract, though what counts as a breach differs; see the check table below.
EXPECTED_COLUMNS = ["Nation ID", "Nation", "Year", "Population"]


# Field names are the JSON keys exactly as they arrive. "Nation ID" contains a
# space, which Delta rejects in a column name, so the rename in acs_population
# is forced rather than chosen - and it is mechanical (lowercase, space to
# underscore), applied after the read, so the contract check below still
# compares against the source's own spelling.
_RECORD = StructType(
    [
        StructField("Nation ID", StringType()),
        StructField("Nation", StringType()),
        StructField("Year", LongType()),
        StructField("Population", DoubleType()),
    ]
)

_PAYLOAD = StructType(
    [
        # Descriptive metadata: source_name, dataset_name, table_id and friends.
        # A map rather than a struct on purpose - these keys are documentation,
        # not a contract, so a new one should be absorbed, not rescued.
        StructField("annotations", MapType(StringType(), StringType())),
        StructField(
            "page",
            StructType(
                [
                    StructField("limit", LongType()),
                    StructField("offset", LongType()),
                    StructField("total", LongType()),
                ]
            ),
        ),
        StructField("columns", ArrayType(StringType())),
        StructField("data", ArrayType(_RECORD)),
        # Must be declared explicitly: when a schema is supplied, the rescued
        # data column is not added for you.
        StructField("_rescued_data", StringType()),
    ]
)


def _read_payload(schema_location: str):
    """Auto Loader over the landed payloads, one row per file."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", POPULATION_GLOB)
        # The payload arrives on a single line today. multiLine costs nothing
        # on a 1.4 KB file and survives the day the API starts pretty-printing.
        .option("multiLine", "true")
        .schema(_PAYLOAD)
        .load(POPULATION_PATH)
    )


@dlt.table(
    name="acs_population",
    comment=(
        "US national population by year, ACS 1-year estimates, one row per "
        "year per landed version. Eleven years: 2013-2019 and 2021-2024. "
        "There is no 2020 - the Census Bureau suspended 1-year estimates for "
        "the pandemic year - and that gap is a property of the source, not a "
        "defect here. Q3 therefore joins LEFT."
    ),
    table_properties={"quality": "bronze"},
)
@dlt.expect("year_present", "year IS NOT NULL")
@dlt.expect("population_positive", "population > 0")
@dlt.expect("nation_is_us", "nation_id = '01000US'")
@dlt.expect("nothing_rescued", "_rescued_data IS NULL")
def acs_population():
    path = F.col("_metadata.file_path")
    ingest_date = F.regexp_extract(path, r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    record = F.col("_record")

    return (
        _read_payload(f"{LANDING}/_autoloader/acs_population")
        # explode_outer, not explode: a payload that arrived with no data array
        # would vanish entirely under explode, and Bronze does not get to
        # decide that a file never existed. It survives as one null row, which
        # year_present then flags.
        .withColumn("_record", F.explode_outer("data"))
        .select(
            record.getField("Nation ID").alias("nation_id"),
            record.getField("Nation").alias("nation"),
            record.getField("Year").alias("year"),
            record.getField("Population").alias("population"),
            F.col("_rescued_data"),
            path.alias("_source_file"),
            ingest_date.alias("_ingest_date"),
            # Same encoding as pr_data, so Silver's AUTO CDC flows can SEQUENCE
            # BY one column whichever source they read. There is a single
            # population source, so there is no within-ingest rank to break
            # ties on - the *10 is kept purely to keep the two scales aligned.
            (F.regexp_replace(ingest_date, "-", "").cast("long") * 10).alias(
                "_precedence"
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="acs_population_source_check",
    comment=(
        "One row per landed payload: the envelope around the data. Holds the "
        "column contract and the provider's own annotations, which are the "
        "only record of which ACS cube produced these numbers."
    ),
    table_properties={"quality": "bronze"},
)
# acs_population selects fields BY NAME from a declared schema, so a reordered
# or appended column cannot hurt it - but a renamed or removed one yields nulls
# rather than an error. The contract is therefore "every expected column is
# still present", not "the header is identical". Same intent as the BLS header
# check, different breach, because the read strategy differs.
@dlt.expect_or_fail("expected_columns_present", "missing_columns = 0")
# Defence in depth: the ingest layer already refuses to land a truncated
# payload. A short series would make Q1 wrong rather than absent, which is the
# failure mode worth halting on. (A payload with no "page" object yields NULL
# here, which DLT treats as a pass - the ingest-side guard remains primary.)
@dlt.expect_or_fail("payload_not_truncated", "declared_total = actual_rows")
def acs_population_source_check():
    # Batch, not streaming. Streaming buys incremental processing, which is
    # worth nothing across a handful of 1.4 KB files, and costs a checkpoint to
    # keep. The lookups are materialized views for the same reason.
    expected = F.array(*[F.lit(c) for c in EXPECTED_COLUMNS])
    source_file = F.col("_metadata.file_path")

    return (
        spark.read.format("json")
        .option("multiLine", "true")
        .schema(_PAYLOAD)
        .load(f"{POPULATION_PATH}ingest_date=*/{POPULATION_GLOB}")
        .select(
            F.regexp_extract(
                source_file, r"ingest_date=(\d{4}-\d{2}-\d{2})", 1
            ).alias("_ingest_date"),
            F.concat_ws("|", F.col("columns")).alias("columns_actual"),
            F.lit("|".join(EXPECTED_COLUMNS)).alias("columns_expected"),
            F.size(F.array_except(expected, F.col("columns"))).alias(
                "missing_columns"
            ),
            F.col("page.total").alias("declared_total"),
            F.size("data").alias("actual_rows"),
            # Lineage the quest would otherwise throw away. "ACS 1-year
            # Estimate" and table B01003 are the difference between answers
            # that reconcile with the published expectations and answers that
            # look just as plausible and do not.
            F.col("annotations").getItem("dataset_name").alias("dataset_name"),
            F.col("annotations").getItem("source_name").alias("source_name"),
            F.col("annotations").getItem("table_id").alias("table_id"),
            source_file.alias("_source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
