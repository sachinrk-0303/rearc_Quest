"""Ingest entry point.

Run as a Databricks job task, or locally:

    uv run python -m ingest.main \
        --landing-root /Volumes/<catalog>/<schema>/landing \
        --contact-email you@example.com

`run()` is kept separate from `main()` so a notebook task can call it directly
with widget values instead of shelling out with argv.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from . import bls, population
from .landing import SyncResult
from .manifest import FileState, JsonManifestStore, ManifestStore, utc_now

DEFAULT_CONTACT = "sachin.kamthankar@gmail.com"

# Leading underscore keeps the manifest out of the way of Auto Loader, which
# reads the source subdirectories rather than the landing root.
MANIFEST_RELPATH = "_manifest/manifest.json"

_ACTIONS = ("landed_new", "landed_changed", "unchanged_content", "skipped_metadata")


def _report(label: str, results: list[SyncResult]) -> int:
    """Print one source's outcome and return bytes transferred."""
    counts = Counter(r.action for r in results)
    total_bytes = sum(r.bytes_downloaded for r in results)

    print(label)
    for action in _ACTIONS:
        if counts[action]:
            print(f"  {action:<20} {counts[action]:>5}")
    print(f"  {'bytes downloaded':<20} {total_bytes:>5,}")
    print()
    return total_bytes


def run(
    landing_root: str | Path,
    contact_email: str = DEFAULT_CONTACT,
    ingest_date: str | None = None,
    store: ManifestStore | None = None,
) -> dict[str, FileState]:
    """Bring the landing zone up to date with both sources.

    Returns the merged manifest so callers and tests can assert on it.
    """
    root = Path(landing_root)
    ingest_date = ingest_date or datetime.now(UTC).strftime("%Y-%m-%d")

    store = store or JsonManifestStore(root / MANIFEST_RELPATH)
    prior = store.load()

    print(f"landing root : {root}")
    print(f"ingest date  : {ingest_date}")
    print(f"manifest     : {len(prior)} files known")
    print()

    # One session for both sources: connection reuse, and the contact
    # User-Agent is applied uniformly even though only BLS requires it.
    session = bls.build_session(contact_email)

    bls_results, removed = bls.sync(session, root, ingest_date, prior)
    bls_bytes = _report("BLS", bls_results)

    pop_result = population.sync(session, root, ingest_date, prior)
    pop_bytes = _report("POPULATION", [pop_result])

    # Summarise the file that is actually on disk, which is what the pipeline
    # will read - not the response we just fetched. When content was unchanged,
    # landed_path still points at the previously landed copy.
    rows, missing = population.summarise(Path(pop_result.state.landed_path).read_bytes())
    print(f"  population rows      {rows}")
    print(f"  missing years        {missing or 'none'}")
    print()

    # Written once, at the end. A mid-run failure therefore re-lands some files
    # on the next attempt - which is harmless: a same-day retry writes to the
    # same dated partition, overwriting identical bytes, and Silver deduplicates
    # regardless.
    seen_keys = {r.state.key for r in [*bls_results, pop_result]}
    merged = {**prior, **{r.state.key: r.state for r in [*bls_results, pop_result]}}

    # Tombstone, never delete. A withdrawn file keeps its manifest row and its
    # landed partitions, so Bronze continues to read what it already ingested.
    now = utc_now()
    for key in removed:
        if merged[key].removed_at is None:
            merged[key] = replace(merged[key], removed_at=now)

    # A file BLS re-publishes is no longer withdrawn. This matters because a
    # reappearing file whose content is unchanged takes the skipped_metadata
    # path, which carries the prior state forward verbatim - tombstone included.
    for key in seen_keys:
        if merged[key].removed_at is not None:
            merged[key] = replace(merged[key], removed_at=None)

    tombstoned = {k: v for k, v in merged.items() if v.removed_at is not None}
    if tombstoned:
        print("WITHDRAWN UPSTREAM (manifest row and landed data retained):")
        for key, state in sorted(tombstoned.items()):
            print(f"  {key:<40} since {state.removed_at}")
        print()

    store.save(merged)

    print(f"total bytes downloaded : {bls_bytes + pop_bytes:,}")
    print(f"manifest now tracks    : {len(merged)} files")
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land BLS and population data.")
    parser.add_argument(
        "--landing-root",
        required=True,
        help="Volume path to land into, e.g. /Volumes/cat/schema/landing",
    )
    parser.add_argument(
        "--contact-email",
        default=DEFAULT_CONTACT,
        help="Address sent in the User-Agent; BLS returns 403 without one.",
    )
    parser.add_argument(
        "--ingest-date",
        default=None,
        help="Partition date, YYYY-MM-DD. Defaults to today (UTC).",
    )
    parser.add_argument(
        "--manifest-store",
        choices=("json", "delta"),
        default="json",
        help="Where ingest state lives. json for local runs, delta on Databricks.",
    )
    parser.add_argument(
        "--manifest-table",
        default=None,
        help="Fully-qualified table, e.g. cat.raw.ingest_manifest." 
              "Required for --manifest-store delta.",
    )
    args = parser.parse_args(argv)
    store = None
    if args.manifest_store == "delta":
        if not args.manifest_table:
            parser.error("--manifest-table is required when --manifest-store is delta")
        # Imported here, not at module scope, so pyspark is only required when
        # the Delta backend is actually selected. A local run never touches it.
        from .delta_manifest import DeltaManifestStore

        store = DeltaManifestStore(args.manifest_table)
    run(args.landing_root, args.contact_email, args.ingest_date, store)
    return 0


if __name__ == "__main__":
    sys.exit(main())
