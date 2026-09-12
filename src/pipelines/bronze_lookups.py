"""Bronze: the eight lookup files.

Materialized views, not streaming tables. Only pr_data needs to be append-only,
because only it feeds an AUTO CDC flow; the lookups are small dimensions
(1 to 282 rows) that Silver joins against, so recomputing them each run is both
correct and simpler than maintaining eight checkpoints.

pr.txt and pr.contacts are deliberately absent. Both are prose - a survey
description and a list of phone numbers - so they are landed in the Volume, as
the quest requires, but modelling them as tables would invent structure that
does not exist.

Column names come from the files, never from pr.txt section 6, which documents
pr.seasonal as lowercase `seasonal_code` while the file says `Seasonal_code`.
Where the provider's spec and the provider's data disagree, the data wins.
"""

from functools import reduce

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

LANDING = spark.conf.get("landing_path")

# filename -> column names, transcribed from each file's own header row.
LOOKUPS = {
    "pr_class": (
        "pr.class",
        ["class_code", "class_text", "display_level", "selectable", "sort_sequence"],
    ),
    "pr_duration": (
        "pr.duration",
        ["duration_code", "duration_text", "display_level", "selectable", "sort_sequence"],
    ),
    "pr_footnote": ("pr.footnote", ["footnote_code", "footnote_text"]),
    "pr_measure": (
        "pr.measure",
        ["measure_code", "measure_text", "display_level", "selectable", "sort_sequence"],
    ),
    "pr_period": ("pr.period", ["period", "period_abbr", "period_name"]),
    # Capitalised in the file, lowercase in pr.txt. The file is authoritative.
    "pr_seasonal": ("pr.seasonal", ["Seasonal_code", "Seasonal_text"]),
    "pr_sector": (
        "pr.sector",
        ["sector_code", "sector_name", "display_level", "selectable", "sort_sequence"],
    ),
    "pr_series": (
        "pr.series",
        [
            "series_id", "sector_code", "class_code", "measure_code",
            "duration_code", "seasonal", "base_year", "footnote_codes",
            "begin_year", "begin_period", "end_year", "end_period",
        ],
    ),
}


def _load(filename: str):
    """Every landed version of one lookup file, with provenance."""
    return (
        spark.read.format("text")
        .load(f"{LANDING}/bls/pr/ingest_date=*/{filename}")
        .withColumnRenamed("value", "_raw_line")
        # A structurally empty line is file padding, not a record - pr.seasonal
        # ends with one, which otherwise arrives as a third row with empty code
        # and text. Distinct from dropping a malformed row: a malformed row is
        # evidence about the source and Bronze keeps it; a blank line is not.
        .filter(F.length(F.trim(F.col("_raw_line"))) > 0)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_ingest_date",
            F.regexp_extract(F.col("_source_file"), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1),
        )
    )


def _read_lookup(filename: str, columns: list[str]):
    fields = F.split(F.col("_raw_line"), "\t")

    df = _load(filename)

    # A lookup file only re-lands when its content changed, so older partitions
    # are superseded rather than complementary. Keeping only the newest means a
    # code BLS has withdrawn disappears, which is correct for a dimension.
    df = (
        df.withColumn("_latest", F.max("_ingest_date").over(Window.partitionBy()))
        .filter(F.col("_ingest_date") == F.col("_latest"))
        .drop("_latest")
    )

    # Values are left untrimmed, exactly as pr_data leaves them. Bronze records
    # what arrived; trimming is an interpretation and belongs in Silver.
    for i, name in enumerate(columns):
        df = df.withColumn(name, fields.getItem(i))

    return (
        df.withColumn("_field_count", F.size(fields))
        # Drop the file's own header row. Trimmed for the comparison only,
        # because pr.series pads its first field like the data files do.
        .filter(F.trim(fields.getItem(0)) != columns[0])
        .withColumn("_ingested_at", F.current_timestamp())
    )


def _register(table_name: str, filename: str, columns: list[str]) -> None:
    """Define one lookup table.

    Wrapped in a function so each closure binds its own filename and columns.
    Defining these inline in the loop would give every table the last values
    the loop variables happened to hold - the classic late-binding bug, and
    silent here because all eight would still build, just from one file.
    """

    @dlt.table(
        name=table_name,
        comment=f"Lookup {filename}, newest landed version. {len(columns)} columns.",
        table_properties={"quality": "bronze"},
    )
    @dlt.expect("expected_column_count", f"_field_count = {len(columns)}")
    def _lookup():
        return _read_lookup(filename, columns)


for _name, (_file, _cols) in LOOKUPS.items():
    _register(_name, _file, _cols)


@dlt.table(
    name="lookup_header_check",
    comment=(
        "One row per lookup file, comparing its header against the contract "
        "this pipeline was written for. Like pr_data, the lookups are split by "
        "position, so a reordered column would corrupt a dimension silently."
    ),
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_fail("all_lookup_headers_match", "header_actual = header_expected")
def lookup_header_check():
    fields = F.split(F.col("_raw_line"), "\t")

    frames = [
        _load(filename)
        .filter(F.trim(fields.getItem(0)) == columns[0])
        .select(
            F.lit(filename).alias("file"),
            F.concat_ws(
                "|", *[F.trim(fields.getItem(i)) for i in range(len(columns))]
            ).alias("header_actual"),
            F.lit("|".join(columns)).alias("header_expected"),
        )
        for filename, columns in LOOKUPS.values()
    ]
    # distinct(): the same header appears once per landed partition.
    return reduce(DataFrame.unionByName, frames).distinct()
