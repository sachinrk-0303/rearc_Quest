"""Silver: the series dimension - what a series_id actually means.

pr.series carries five codes per series; the five lookup tables carry the text.
This is the join that makes a series_id legible, and it is what Q2 needs to
answer "which year was best" with something a person can read.

duration_text is renamed to units, deliberately. Its three values are
"% Change from previous quarter", "% Change same quarter 1 year ago" and
"Index (2017=100)" - that is how the value is expressed, not a span of time,
and "duration" invites exactly the wrong reading. This matters beyond
tidiness: summing four quarters of an index gives roughly 400 while summing
four quarters of a percent change gives roughly 14.6, so Q2's best-year sum is
comparable only within a units value. Exposing it is what stops a reader
comparing the two.

The joins are LEFT, not INNER, and a null resolution fails the pipeline. INNER
would drop a series whose code stopped resolving, and a series missing from Q2
is indistinguishable from a series that never existed. LEFT plus expect_or_fail
turns the same event into a halt that names the offender.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA = spark.conf.get("silver_schema")


def silver(name: str) -> str:
    """Fully-qualify a Silver table. See silver_observations for why."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


# (bronze lookup, its code column, its text column, column to join on, output name)
DIMENSIONS = [
    ("pr_sector", "sector_code", "sector_name", "sector_code", "sector"),
    ("pr_class", "class_code", "class_text", "class_code", "worker_class"),
    ("pr_measure", "measure_code", "measure_text", "measure_code", "measure"),
    ("pr_duration", "duration_code", "duration_text", "duration_code", "units"),
    # Capitalised in the file; the data is authoritative over pr.txt section 6.
    ("pr_seasonal", "Seasonal_code", "Seasonal_text", "seasonal_code",
     "seasonal_adjustment"),
]

LABEL_PARTS = ["sector", "worker_class", "measure", "units", "seasonal_adjustment"]


def _series_base():
    """pr.series, trimmed and typed, with its sentinels resolved."""
    return dlt.read("pr_series").select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("sector_code")).alias("sector_code"),
        F.trim(F.col("class_code")).alias("class_code"),
        F.trim(F.col("measure_code")).alias("measure_code"),
        F.trim(F.col("duration_code")).alias("duration_code"),
        # pr.series names this column "seasonal" where every other file names
        # the same concept a code. Normalised so the joins read uniformly.
        F.trim(F.col("seasonal")).alias("seasonal_code"),
        # "-" means not applicable: percent-change series have no base year,
        # only index series do. It is a sentinel, not a value, and under ANSI
        # mode cast('-' AS INT) raises rather than returning null - so nullif
        # has to come first and try_cast has to be the cast.
        F.expr("try_cast(nullif(trim(base_year), '-') AS INT)").alias("base_year"),
        F.expr("try_cast(trim(begin_year) AS INT)").alias("begin_year"),
        F.trim(F.col("begin_period")).alias("begin_period"),
        F.expr("try_cast(trim(end_year) AS INT)").alias("end_year"),
        F.trim(F.col("end_period")).alias("end_period"),
        F.expr("nullif(trim(footnote_codes), '')").alias("footnote_codes"),
    )


@dlt.table(
    name=silver("series"),
    comment=(
        "One row per series_id with its five codes resolved to text, its "
        "coverage window, and a concatenated label. units is BLS's "
        "duration_text: the basis on which the value is expressed, which "
        "determines whether a best-year sum is on an index scale or a "
        "percent-change scale."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_fail(
    "every_code_resolves",
    " AND ".join(f"{part} IS NOT NULL" for part in LABEL_PARTS),
)
# A duplicate code in any lookup would fan the join out and quietly count a
# series twice in Q2. Measured per row rather than assumed, because a row
# count is the one thing a silent fan-out does not preserve.
@dlt.expect_or_fail("one_row_per_series", "_rows_per_series = 1")
def series():
    df = _series_base()

    for table, code_col, text_col, join_col, out_name in DIMENSIONS:
        dim = dlt.read(table).select(
            F.trim(F.col(code_col)).alias("_code"),
            F.trim(F.col(text_col)).alias(out_name),
        )
        df = df.join(dim, df[join_col] == dim["_code"], "left").drop("_code")

    return (
        df.withColumn(
            "series_label",
            F.concat_ws(" | ", *[F.col(part) for part in LABEL_PARTS]),
        )
        .withColumn(
            "_rows_per_series",
            F.count("*").over(Window.partitionBy("series_id")),
        )
    )
