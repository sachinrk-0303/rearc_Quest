"""Manifest persistence backed by a Unity Catalog Delta table.

Deliberately a separate module from manifest.py, which must stay importable
without Spark - the change-detection logic is unit-tested on a laptop, and the
JSON store remains the local path. Nothing here is imported unless a caller
asks for the Delta backend.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from .manifest import FileState

# Declared explicitly rather than inferred. Inference would guess types from
# whatever the first batch happens to contain, and would make a nullable
# last_modified indistinguishable from a column that is always null.
_SCHEMA = T.StructType(
    [
        T.StructField("key", T.StringType(), nullable=False),
        T.StructField("sha256", T.StringType(), nullable=False),
        T.StructField("size_bytes", T.LongType(), nullable=False),
        T.StructField("last_modified", T.StringType(), nullable=True),
        T.StructField("landed_path", T.StringType(), nullable=False),
        T.StructField("ingested_at", T.StringType(), nullable=False),
        T.StructField("removed_at", T.StringType(), nullable=True),
    ]
)


class DeltaManifestStore:
    """Ingest state as a Delta table. Satisfies the ManifestStore protocol.

    The table is small and bounded by the number of files the sources publish -
    currently 14 - so it is read and written whole. That would be the wrong
    shape for a large table; it is the right one for a state snapshot.
    """

    def __init__(self, table: str, spark: SparkSession | None = None) -> None:
        self.table = table
        self.spark = spark or SparkSession.builder.getOrCreate()

    def load(self) -> dict[str, FileState]:
        # A missing table is the first run, not an error - the same contract
        # JsonManifestStore honours when the file does not exist.
        if not self.spark.catalog.tableExists(self.table):
            return {}
        rows = self.spark.table(self.table).collect()
        return {row["key"]: FileState(**row.asDict()) for row in rows}

    def save(self, states: dict[str, FileState]) -> None:
        # Tuples in schema order rather than dicts: explicit, and immune to any
        # field-ordering surprise in createDataFrame.
        rows = [
            (
                s.key, s.sha256, s.size_bytes, s.last_modified,
                s.landed_path, s.ingested_at, s.removed_at,
            )
            for s in sorted(states.values(), key=lambda s: s.key)
        ]
        df = self.spark.createDataFrame(rows, schema=_SCHEMA)

        # Overwrite, not merge. The manifest is a complete snapshot of ingest
        # state, so replacing it wholesale is the honest operation - and Delta
        # keeps every prior version, so DESCRIBE HISTORY and VERSION AS OF give
        # the audit trail for free rather than through append-only bookkeeping.
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.table)
        )
