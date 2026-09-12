"""Silver: US national population by year.

Same shape as silver_observations - judge, then route, then AUTO CDC - so only
the differences are worth explaining here.

population becomes BIGINT. Bronze holds DOUBLE because that is what the JSON
declares, and Bronze records what arrived; Silver claims the data is correct,
and a census count is not a fractional quantity. But cast() truncates rather
than refusing, so a non-integral value would quietly lose its fraction. That is
why "population_not_integral" is a quarantine reason rather than an assumption.

nation_id is enforced, not merely warned about. Bronze flags a non-US row; here
it is rejected, because the key is year alone. A state-level row would collide
with the national row for the same year and AUTO CDC would resolve the
collision arbitrarily - a wrong answer with no error, rather than a visibly
wrong one.

2020 is absent and stays absent. The Census Bureau suspended ACS 1-year
estimates for the pandemic year. Emitting a 2020 row with a null population
would be inventing a record that was never published; Q3 joins LEFT instead.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")

# The Tesseract cube's identifier for the nation as a whole.
US_NATION_ID = "01000US"


def silver(name: str) -> str:
    """Fully-qualify a Silver table. See silver_observations for why."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


@dlt.view(
    name="population_typed",
    comment="Bronze population rows, each carrying why it was rejected or NULL.",
)
def population_typed():
    # First failing rule wins. Structural problems first: a row that is not
    # national grain, or that arrived with fields we did not model, is not
    # really a national population figure at all, and its other properties are
    # not worth reporting on.
    reason = (
        F.when(F.col("_rescued_data").isNotNull(), F.lit("unexpected_fields"))
        # Null-guarded: NULL != '01000US' is NULL, not true, so an unguarded
        # comparison lets a null nation fall through every rule and trip the
        # or_fail expectation instead of landing in quarantine.
        .when(
            F.col("nation_id").isNull() | (F.col("nation_id") != US_NATION_ID),
            F.lit("not_national_grain"),
        )
        .when(F.col("year").isNull(), F.lit("year_missing"))
        .when(~F.col("year").between(1900, 2100), F.lit("year_implausible"))
        .when(F.col("population").isNull(), F.lit("population_missing"))
        .when(F.col("population") <= 0, F.lit("population_not_positive"))
        # Guards the BIGINT cast below, which would otherwise truncate in
        # silence. A population with a fraction is not a rounding nuisance -
        # it means the measure is no longer a count of people.
        .when(
            F.col("population") != F.floor(F.col("population")),
            F.lit("population_not_integral"),
        )
    )

    return dlt.read_stream("acs_population").withColumn("_reject_reason", reason)


@dlt.view(
    name="population_valid",
    comment="Rows that satisfy Silver's contract, shaped for the AUTO CDC flow.",
)
def population_valid():
    return (
        dlt.read_stream("population_typed")
        .filter(F.col("_reject_reason").isNull())
        .select(
            # INT to match silver.pr_observations.year, so Q3 joins without an
            # implicit widening cast on either side.
            F.col("year").cast("int").alias("year"),
            F.col("population").cast("long").alias("population"),
            F.col("nation"),
            # Carried only to order the flow; dropped from the target below.
            F.col("_precedence"),
        )
    )


dlt.create_streaming_table(
    name=silver("population"),
    comment=(
        "US national population by year, ACS 1-year estimates, one current row "
        "per year with revision history. Eleven years: 2013-2019 and "
        "2021-2024. 2020 is absent because the Census Bureau did not publish "
        "it, not because it was filtered out."
    ),
    table_properties={"quality": "silver"},
    expect_all_or_fail={
        "year_present": "year IS NOT NULL",
        "population_present": "population IS NOT NULL AND population > 0",
    },
)

dlt.create_auto_cdc_flow(
    target=silver("population"),
    source="population_valid",
    keys=["year"],
    sequence_by=F.col("_precedence"),
    stored_as_scd_type=2,
    # Excluded for the same reason as in silver_observations: _precedence in
    # the target would open a new SCD2 version on every ingest, whether or not
    # the estimate actually changed. __START_AT already records it.
    except_column_list=["_precedence"],
)


@dlt.table(
    name=silver("population_quarantine"),
    comment=(
        "Population rows Silver refused, with the reason. Append-only, like "
        "the observation quarantine: the question is what the source sent and "
        "when, so the same bad row across two ingests is two facts."
    ),
    table_properties={"quality": "silver"},
)
def population_quarantine():
    return (
        dlt.read_stream("population_typed")
        .filter(F.col("_reject_reason").isNotNull())
        .select(
            F.col("_reject_reason"),
            F.col("nation_id"),
            F.col("nation"),
            F.col("year"),
            F.col("population"),
            F.col("_rescued_data"),
            F.col("_source_file"),
            F.col("_ingest_date"),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
