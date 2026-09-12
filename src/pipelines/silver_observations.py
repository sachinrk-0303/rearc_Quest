"""Silver: BLS observations, typed, deduplicated, and history-tracked.

Bronze's contract is "what arrived". Silver's is "this is correct", and the
whole file follows from that one difference.

Rows that cannot be made correct are not nulled and kept - they are routed to
pr_observations_quarantine. A null that means "failed to parse" is
indistinguishable downstream from a null that means "BLS published nothing",
and Q2 sums value, so a silently-null row just quietly shrinks a year's total.
Separating them keeps Silver's promise honest and leaves the rejects queryable
rather than discarded.

Deduplication is AUTO CDC with SCD Type 2. Today it is pure duplicate removal -
pr.data.0.Current and pr.data.1.AllData agree on every one of the 38,469 rows
they share - but BLS revises published figures (174 rows already carry
footnote R), and when a revision lands, the superseded value stays queryable
instead of being overwritten.

Values are stored as DECIMAL, not DOUBLE. Bronze keeps the raw string, so the
choice is Silver's to make, and Silver is where the data is claimed to be
correct: Q2 ranks years by sum(value), and float error makes ties incomparable
and the ranking arbitrary in the last bits.

"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")


def silver(name: str) -> str:
    """Fully-qualify a Silver table.

    The pipeline's default schema is bronze, so Silver tables must name their
    schema explicitly - that is what lets one pipeline publish all three
    medallion layers. Nothing here hardcodes a schema name; both come from the
    bundle, so dev and prod differ without the code knowing.
    """
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


@dlt.view(
    name="pr_typed",
    comment="Bronze rows, trimmed and cast, each carrying why it was rejected or NULL.",
)
def pr_typed():
    """Trim, cast, and judge - but do not yet route.

    Judging and routing are separated so that both destinations are guaranteed
    to partition the same population. If the quarantine filter and the valid
    filter each re-derived their own rules, a row could satisfy both or
    neither, and the count would silently stop reconciling.
    """
    # The provider's own code list, not a regex we invented. A period code BLS
    # documents is accepted automatically; one it does not is quarantined.
    # Stream-static join: pr_period is a materialized view of 5 rows.
    periods = dlt.read("pr_period").select(
        F.trim(F.col("period")).alias("_period_code")
    ).distinct()

    # try_cast, not cast. Serverless runs with ANSI mode on, where a plain cast
    # of "abc" to DOUBLE raises rather than returning NULL - which would fail
    # the whole pipeline on exactly the rows quarantine exists to catch.
    typed = (
        dlt.read_stream("pr_data")
        .withColumn("_series_id", F.trim(F.col("series_id")))
        .withColumn("_year", F.expr("try_cast(trim(year) AS INT)"))
        .withColumn("_period", F.trim(F.col("period")))
        # DECIMAL, not DOUBLE. Q2 ranks years by sum(value), and four doubles
        # summing to "14.6" actually produce 14.600000000000001 - so two years
        # that genuinely tie do not compare equal, the earliest-year tie-break
        # never fires, and max() picks whichever year accumulated more rounding
        # error. Measured: max 3 decimal places, max 3 integer digits, range
        # -60.8 to 412.8, so (18,3) is exact with room to spare.
        .withColumn("_value", F.expr("try_cast(trim(value) AS DECIMAL(18,3))"))
        .withColumn("_footnote", F.trim(F.col("footnote_codes")))
    )

    typed = typed.join(
        periods, typed["_period"] == periods["_period_code"], "left"
    )

    # First failing rule wins, so the reason is deterministic rather than
    # whichever predicate happened to be evaluated first. Ordered cheapest and
    # most structural first: a row with the wrong field count is not really a
    # row at all, and its other failures are consequences, not causes.
    reason = (
        F.when(F.col("_field_count") != 5, F.lit("field_count_not_5"))
         .when(
            F.col("_series_id").isNull() | (F.length("_series_id") == 0),
            F.lit("series_id_blank"),
             )
        .when(F.col("_year").isNull(), F.lit("year_not_an_integer"))
        .when(~F.col("_year").between(1900, 2100), F.lit("year_implausible"))
        .when(F.col("_period_code").isNull(), F.lit("period_not_in_pr_period"))
        .when(F.col("_value").isNull(), F.lit("value_not_numeric"))
    )

    return typed.withColumn("_reject_reason", reason)


@dlt.view(
    name="pr_valid",
    comment="Rows that satisfy Silver's contract, shaped for the AUTO CDC flow.",
)
def pr_valid():
    return (
        dlt.read_stream("pr_typed")
        .filter(F.col("_reject_reason").isNull())
        .select(
            F.col("_series_id").alias("series_id"),
            F.col("_year").alias("year"),
            F.col("_period").alias("period"),
            F.col("_value").alias("value"),
            # Empty string becomes NULL. In Bronze the distinction is
            # meaningless - the field was present and empty - but Silver claims
            # meaning, and "no footnote" is an absence, not a value.
            F.when(F.length("_footnote") == 0, F.lit(None))
            .otherwise(F.col("_footnote"))
            .alias("footnote_codes"),
            # Carried only to order the flow; dropped from the target below.
            F.col("_precedence"),
        )
    )


dlt.create_streaming_table(
    name=silver("pr_observations"),
    comment=(
        "One current row per (series_id, year, period), with full revision "
        "history. __START_AT and __END_AT hold the _precedence at which a "
        "version began and ended; __END_AT IS NULL is the current row. "
        "Provenance stays in bronze.pr_data - __START_AT decodes back to the "
        "ingest date, since it is yyyymmdd * 10 + source rank."
    ),
    table_properties={"quality": "silver"},
    # Cannot fail given the filter above - which is the point. These assert
    # that the routing is correct, so a bug in _reject_reason surfaces here as
    # a halt rather than downstream as a quietly short sum.
    expect_all_or_fail={
        "value_present": "value IS NOT NULL",
        "key_complete": (
            "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL"
        ),
    },
)

dlt.create_auto_cdc_flow(
    target=silver("pr_observations"),
    source="pr_valid",
    keys=["series_id", "year", "period"],
    sequence_by=F.col("_precedence"),
    stored_as_scd_type=2,
    # _precedence must not reach the target. SCD Type 2 opens a new version
    # whenever any target column changes, and the same key arrives twice per
    # ingest with two different _precedence values - Current and AllData. Left
    # in, it would version all 38,469 overlapping keys on provenance alone.
    # Excluded, history moves only when a value genuinely changes. Nothing is
    # lost: __START_AT is the _precedence of the version's first appearance.
    except_column_list=["_precedence"],
)


@dlt.table(
    name=silver("pr_observations_quarantine"),
    comment=(
        "Rows Silver refused, with the raw line and the reason. Append-only, "
        "never deduplicated: the question this table answers is 'what did the "
        "source send us and when', so a repeat offender across two ingests is "
        "two facts, not one."
    ),
    table_properties={"quality": "silver"},
)
def pr_observations_quarantine():
    return (
        dlt.read_stream("pr_typed")
        .filter(F.col("_reject_reason").isNotNull())
        .select(
            F.col("_reject_reason"),
            # The untrimmed originals, deliberately. A quarantine table that
            # shows the cleaned value hides the thing that needs diagnosing.
            F.col("_raw_line"),
            F.col("series_id"),
            F.col("year"),
            F.col("period"),
            F.col("value"),
            F.col("footnote_codes"),
            F.col("_field_count"),
            F.col("_source_file"),
            F.col("_ingest_date"),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
