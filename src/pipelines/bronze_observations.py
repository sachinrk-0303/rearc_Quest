"""Bronze: source records with provenance, no interpretation.

Bronze's contract is "what arrived", not "what is usable". Rows are never
dropped here - a malformed record is evidence about the source, and discarding
it at the boundary destroys the only proof it existed. Expectations are
therefore recorded as metrics rather than enforced; Silver applies the strict
contract, because that is where the data is claimed to be correct.

The one exception is the header contract, which IS enforced. Bronze splits each
line by position, so a reordered or dropped column would corrupt every row while
the field count still looked correct. pr_data_header_check exists to make that
failure loud rather than silent.

Read as a stream because AUTO CDC in Silver requires an append-only source, and
because Auto Loader tracks which files it has already consumed - so a re-run
after a new ingest processes only the new dated partitions.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

LANDING = spark.conf.get("landing_path")

# The source's own declaration of its shape, per pr.txt section 5. Compared
# against the first five header fields on every run. Only the first five,
# because a column appended at the end changes no position we index and is
# therefore backwards compatible - that warrants a warning, not a halt.
EXPECTED_HEADER = "series_id|year|period|value|footnote_codes"

DATA_GLOB = "pr.data.*"
DATA_PATH = f"{LANDING}/bls/pr/"


def _read_lines(schema_location: str):
    """Auto Loader over the data files, one row per raw line.

    Read as text rather than CSV on purpose. BLS pads its header row
    ("series_id________", "_______value"), and Auto Loader matches a supplied
    schema against header NAMES - so the padded names match nothing, every
    column parses as null, and all five values land in _rescued_data. Reading
    the line whole and splitting on tab ourselves is immune to that.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", schema_location)
        # Only the data files: the lookup files share this directory and have
        # entirely different shapes.
        .option("pathGlobFilter", DATA_GLOB)
        .load(DATA_PATH)
    )


@dlt.table(
    name="pr_data",
    comment=(
        "Raw observations from pr.data.0.Current and pr.data.1.AllData, with "
        "provenance. Still contains the overlap: Current is a strict subset of "
        "AllData, so this table holds 115,595 rows of which 38,469 are "
        "duplicates. Silver deduplicates on (series_id, year, period)."
    ),
    table_properties={"quality": "bronze"},
)
@dlt.expect("five_columns", "_field_count = 5")
@dlt.expect("series_id_present", "series_id IS NOT NULL AND length(trim(series_id)) > 0")
def pr_data():
    path = F.col("_metadata.file_path")

    # Precedence for the AUTO CDC flow in Silver. Two orderings must hold at
    # once: a later ingest beats an earlier one, and within a single ingest
    # AllData beats Current. Encoding both as one monotonic integer -
    # yyyymmdd * 10 + rank - keeps SEQUENCE BY to a single column.
    ingest_date = F.regexp_extract(path, r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    source_rank = F.when(path.endswith("AllData"), F.lit(2)).otherwise(F.lit(1))

    fields = F.split(F.col("_raw_line"), "\t")

    return (
        _read_lines(f"{LANDING}/_autoloader/pr_data")
        # Renamed before splitting. The text reader emits a column called
        # "value", which is also a BLS field name - overwriting it in place
        # would silently redefine what `fields` refers to.
        .withColumnRenamed("value", "_raw_line")
        # A structurally empty line is file padding, not a record - pr.seasonal
        # ends with one. This is not the same as dropping a malformed row: a
        # malformed row is evidence about the source and Bronze keeps it, but a
        # blank line is evidence of nothing.
        .filter(F.length(F.trim(F.col("_raw_line"))) > 0)
        .withColumn("_field_count", F.size(fields))
        .withColumn("series_id", fields.getItem(0))
        .withColumn("year", fields.getItem(1))
        .withColumn("period", fields.getItem(2))
        .withColumn("value", fields.getItem(3))
        .withColumn("footnote_codes", fields.getItem(4))
        # Every file carries its own header row. Trim before comparing, since
        # the header is exactly the padded string that caused the problem.
        .filter(F.trim(F.col("series_id")) != "series_id")
        .withColumn("_source_file", path)
        .withColumn("_ingest_date", ingest_date)
        .withColumn(
            "_precedence",
            F.regexp_replace(ingest_date, "-", "").cast("long") * 10 + source_rank,
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name="pr_data_header_check",
    comment=(
        "One row per data file, holding that file's header. Exists solely to "
        "make a source schema change loud. pr_data splits by position, so a "
        "reordered or dropped column would corrupt every row while the field "
        "count still looked correct - this table stops the pipeline instead."
    ),
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_fail("header_matches_contract", f"header_prefix = '{EXPECTED_HEADER}'")
def pr_data_header_check():
    fields = F.split(F.col("value"), "\t")

    return (
        _read_lines(f"{LANDING}/_autoloader/pr_header")
        # Keep only the header rows - the inverse of the filter in pr_data.
        .filter(F.trim(fields.getItem(0)) == "series_id")
        .withColumn(
            "header_prefix",
            F.concat_ws("|", *[F.trim(fields.getItem(i)) for i in range(5)]),
        )
        .withColumn("_field_count", F.size(fields))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .select("header_prefix", "_field_count", "_source_file")
    )
