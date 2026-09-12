"""Change detection for the ingest job.

Two sources need two different strategies, and the reason came from measurement
rather than assumption:

  BLS files are byte-stable. A HEAD request costs zero content bytes and returns
  Content-Length and Last-Modified, so metadata is a usable pre-filter and the
  SHA-256 of the raw bytes is a valid identity.

  The population API is not byte-stable. It offers no Last-Modified to check,
  and two responses carrying identical data hash differently because the server
  serialises the `annotations` object's keys in a non-deterministic order.
  Hashing the raw body would report "changed" on every single run, so its
  content is canonicalised before hashing.

ETag is deliberately unused. BLS answers If-None-Match with 200 and a full body
rather than 304, so relying on it would silently defeat the whole mechanism.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FileState:
    """Everything known about one landed file.

    `key` is the logical identity of the file and is stable across ingests
    (e.g. "bls/pr/pr.data.1.AllData"), while `landed_path` records the specific
    dated partition this version was written to. The two differ because the
    landing zone is partitioned by ingest date - the same logical file lands at
    a new path each time its content actually changes.
    """

    key: str
    sha256: str
    size_bytes: int
    last_modified: str | None   # HTTP header; None for sources that don't send one
    landed_path: str
    ingested_at: str            # ISO-8601 UTC


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def sha256_bytes(data: bytes) -> str:
    """Identity of raw bytes. Correct for BLS, wrong for the population API."""
    return hashlib.sha256(data).hexdigest()


def canonical_json_sha256(payload: bytes) -> str:
    """Hash a JSON document by value rather than by byte sequence.

    Parsing and re-serialising with sorted keys and no incidental whitespace
    means two responses carrying the same data produce the same digest,
    regardless of the order the server happened to emit its keys in.

    Without this the population source reports a change on every run: its
    `annotations` object is serialised from an unordered map, so identical
    payloads differ in byte order while being identical in content.
    """
    obj = json.loads(payload)
    canonical = json.dumps(obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------

def needs_download(
    prior: FileState | None,
    size_bytes: int | None,
    last_modified: str | None,
) -> bool:
    """Should we spend bandwidth fetching the body?

    Metadata agreement is treated as sufficient evidence of NO change, so we
    skip. Metadata disagreement is NOT treated as evidence of change - it only
    means we cannot rule it out from the headers, so the body must be fetched
    and hashed. BLS republishes byte-identical files with fresh timestamps, and
    trusting the timestamp alone would re-land unchanged data every quarter.

    Absent headers mean no pre-filter is possible, so we fetch.
    """
    if prior is None:
        return True
    if size_bytes is None or last_modified is None:
        return True
    return not (prior.size_bytes == size_bytes and prior.last_modified == last_modified)


def content_changed(prior: FileState | None, digest: str) -> bool:
    """The authoritative check. Only the hash decides whether a file lands."""
    return prior is None or prior.sha256 != digest


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class ManifestStore(Protocol):
    """Where file state lives between runs.

    Kept behind an interface so the storage choice is independent of the
    download logic: this step uses a JSON file in the Volume, and P1.3 replaces
    it with a Delta table without any caller changing.
    """

    def load(self) -> dict[str, FileState]: ...

    def save(self, states: dict[str, FileState]) -> None: ...


class JsonManifestStore:
    """Manifest as a single JSON document. Adequate for one writer."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, FileState]:
        if not self.path.exists():
            return {}
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return {k: FileState(**v) for k, v in raw.items()}

    def save(self, states: dict[str, FileState]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: asdict(v) for k, v in sorted(states.items())}
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")