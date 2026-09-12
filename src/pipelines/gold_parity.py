"""Gold: a second, independent implementation, used only to check the first.

The three answers are computed in Spark SQL, because that is the dialect the
people who read this layer already speak. This file recomputes all three with
the DataFrame API and reports every row on which the two disagree.

What that proves is worth stating precisely. It catches transcription slips,
dialect surprises and join mistakes - errors that differ between two
expressions of the same idea. It cannot catch a misreading of the question,
because both implementations come from the same reading: had Q05 been wrongly
included, both would include it and both would agree. The only real check on
the interpretation is the two published values, PRS30006011 at 16.4 and
PRS30006032 at 14.6, which originate outside this project entirely.

The comparison is exact everywhere except Q1, whose outputs are DOUBLE. Two
independent computations of the same mean can differ in the final bits without
either being wrong, so those columns are rounded before comparison - the same
float behaviour that made DECIMAL necessary in Silver, showing up here as a
tolerance instead of a defect.
"""

from functools import reduce

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")

# The same two values the SQL uses, read from the same place, so the two
# implementations cannot drift apart on which series they are reporting.
Q3_SERIES_ID = spark.conf.get("q3_series_id")
Q3_PERIOD = spark.conf.get("q3_period")

# Q1 alone produces DOUBLE columns. Six decimal places is far finer than any
# meaningful difference in a population count or its spread, and far coarser
# than float noise.
DOUBLE_PLACES = 6


def silver(name: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


def gold(name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"


def _current(table: str):
    """A Silver SCD Type 2 table, current rows only."""
    return spark.read.table(silver(table)).filter(F.col("__END_AT").isNull())


# --------------------------------------------------------------------------
# The three answers, recomputed with the DataFrame API.
# --------------------------------------------------------------------------


def _q1():
    return (
        _current("population")
        .filter(F.col("year").between(2013, 2018))
        .agg(
            F.min("year").alias("year_from"),
            F.max("year").alias("year_to"),
            F.count("*").alias("n_years"),
            F.avg("population").alias("mean_population"),
            F.stddev_samp("population").alias("stddev_sample"),
            F.stddev_pop("population").alias("stddev_population"),
        )
    )


def _q2():
    by_series = Window.partitionBy("series_id")

    scored = _current("pr_observations").withColumn(
        "basis",
        F.when(
            F.max(F.when(F.col("period") != "Q05", 1).otherwise(0)).over(by_series)
            == 1,
            F.lit("quarterly"),
        ).otherwise(F.lit("annual_q05")),
    )

    scoped = scored.filter(
        ((F.col("basis") == "quarterly") & (F.col("period") != "Q05"))
        | ((F.col("basis") == "annual_q05") & (F.col("period") == "Q05"))
    )

    per_year = scoped.groupBy("series_id", "basis", "year").agg(
        F.sum("value").alias("total_value"),
        F.count("*").alias("n_periods"),
    )

    # Ties to the earliest year, matching the SQL. Written as a separate
    # window rather than reusing by_series, because the ordering is the whole
    # point of this one and sharing the definition would hide that.
    best = Window.partitionBy("series_id").orderBy(
        F.col("total_value").desc(), F.col("year").asc()
    )

    return (
        per_year.withColumn("rn", F.row_number().over(best))
        .filter(F.col("rn") == 1)
        .select(
            "series_id",
            "basis",
            F.col("year").alias("best_year"),
            "total_value",
            "n_periods",
        )
    )


def _q3():
    observations = _current("pr_observations").filter(
        (F.col("series_id") == Q3_SERIES_ID) & (F.col("period") == Q3_PERIOD)
    )
    population = _current("population").select("year", "population")

    return observations.join(population, on="year", how="left").select(
        "year", "value", "population"
    )


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------


def _round_doubles(df: DataFrame, places: int) -> DataFrame:
    """Round every DOUBLE column, leaving exact types untouched."""
    for name, dtype in df.dtypes:
        if dtype == "double":
            df = df.withColumn(name, F.round(F.col(name), places))
    return df


def _compare(question: str, sql_df: DataFrame, py_df: DataFrame, columns: list[str]):
    """One row summarising whether two implementations agree.

    exceptAll in both directions, so a row present in one and absent from the
    other is caught whichever side it is missing from - and so duplicates are
    caught too, which distinct-based comparison would silently forgive.
    """
    left = _round_doubles(sql_df.select(*columns), DOUBLE_PLACES)
    right = _round_doubles(py_df.select(*columns), DOUBLE_PLACES)

    differences = left.exceptAll(right).unionByName(right.exceptAll(left))

    # Each side aggregates to exactly one row, including when it is empty, so
    # the cross joins produce a single row and "no differences" is reported as
    # a zero rather than as an absent row.
    return (
        left.agg(F.count("*").alias("sql_rows"))
        .crossJoin(right.agg(F.count("*").alias("pyspark_rows")))
        .crossJoin(differences.agg(F.count("*").alias("differing_rows")))
        .select(
            F.lit(question).alias("question"),
            "sql_rows",
            "pyspark_rows",
            "differing_rows",
        )
    )


@dlt.table(
    name=gold("parity_check"),
    comment=(
        "One row per question, comparing the Spark SQL answer against an "
        "independent DataFrame API implementation. differing_rows is 0 when "
        "they agree. Catches transcription and dialect errors, not "
        "misreadings of the question - both implementations share one author "
        "and one interpretation."
    ),
    table_properties={"quality": "gold"},
)
@dlt.expect_or_fail("implementations_agree", "differing_rows = 0")
@dlt.expect_or_fail("both_produced_rows", "sql_rows > 0 AND pyspark_rows > 0")
def parity_check():
    checks = [
        _compare(
            "q1_population_stats",
            dlt.read(gold("q1_population_stats")),
            _q1(),
            [
                "year_from",
                "year_to",
                "n_years",
                "mean_population",
                "stddev_sample",
                "stddev_population",
            ],
        ),
        _compare(
            "q2_best_year_by_series",
            dlt.read(gold("q2_best_year_by_series")),
            _q2(),
            ["series_id", "basis", "best_year", "total_value", "n_periods"],
        ),
        _compare(
            "q3_series_value_and_population",
            dlt.read(gold("q3_series_value_and_population")),
            _q3(),
            ["year", "value", "population"],
        ),
    ]

    return reduce(DataFrame.unionByName, checks)
