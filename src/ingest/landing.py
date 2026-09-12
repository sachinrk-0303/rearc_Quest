"""Writing fetched content into the landing Volume.

Shared by both sources. The rule for what a fetched body means - land it, or
refresh its metadata and leave it alone - is identical whether the bytes came
from BLS or from the population API. Only the digest computation differs, so
callers compute the digest themselves and pass it in.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from .manifest import FileState, content_changed, utc_now


@dataclass(frozen=True)
class SyncResult:
    """What happened to one file this run."""

    key: str
    action: str            # landed_new | landed_changed | unchanged_content | skipped_metadata
    bytes_downloaded: int
    state: FileState


def land(landing_root: Path, ingest_date: str, key: str, data: bytes) -> str:
    """Write a file into its dated partition and return the path.

    The partition is inserted just above the filename, so
    "bls/pr/pr.class" lands at "bls/pr/ingest_date=YYYY-MM-DD/pr.class".

    Every landing is a new path rather than an overwrite. Auto Loader can then
    detect it reliably - overwriting in place is a known way to have a change
    either missed or re-read - and the path itself becomes provenance,
    recording when each version arrived.
    """
    rel = Path(key)
    dest = landing_root / rel.parent / f"ingest_date={ingest_date}" / rel.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return str(dest)


def apply_fetch(
    *,
    key: str,
    data: bytes,
    digest: str,
    last_modified: str | None,
    downloaded: int,
    prior: FileState | None,
    landing_root: Path,
    ingest_date: str,
) -> SyncResult:
    """Decide what a fetched body means, and act on it."""
    if not content_changed(prior, digest):
        # Identical content republished under fresh metadata. Refresh the stored
        # size and timestamp so the next run's cheap pre-filter matches again -
        # otherwise this file is re-downloaded forever - but do NOT land it.
        # Bronze must not see a new file where nothing changed.
        return SyncResult(
            key=key,
            action="unchanged_content",
            bytes_downloaded=downloaded,
            state=replace(prior, size_bytes=len(data), last_modified=last_modified),
        )

    path = land(landing_root, ingest_date, key, data)
    return SyncResult(
        key=key,
        action="landed_new" if prior is None else "landed_changed",
        bytes_downloaded=downloaded,
        state=FileState(
            key=key,
            sha256=digest,
            size_bytes=len(data),
            last_modified=last_modified,
            landed_path=path,
            ingested_at=utc_now(),
        ),
    )
