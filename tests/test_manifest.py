"""Change detection: the logic that decides what gets downloaded.

None of this needs a network or a Spark session, which is the point - these are
the decisions that cost bandwidth when they are wrong and correctness when they
are very wrong, so they should be the cheapest thing in the project to verify.
"""

import json

import pytest

from ingest.manifest import (
    FileState,
    JsonManifestStore,
    canonical_json_sha256,
    content_changed,
    needs_download,
    sha256_bytes,
)


def state(**overrides) -> FileState:
    base = {
        "key": "bls/pr/pr.class",
        "sha256": "a" * 64,
        "size_bytes": 100,
        "last_modified": "Wed, 10 Sep 2026 12:00:00 GMT",
        "landed_path": "/Volumes/x/bls/pr/ingest_date=2026-09-10/pr.class",
        "ingested_at": "2026-09-10T12:00:00+00:00",
    }
    base.update(overrides)
    return FileState(**base)


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

# The same data serialised with its keys in a different order. Not a
# hypothetical: the population API emits `annotations` from an unordered map,
# and two consecutive responses really do differ exactly like this.
PAYLOAD_A = (
    b'{"annotations":{"source_name":"Census Bureau","topic":"Diversity"},'
    b'"data":[{"Year":2013}]}'
)
PAYLOAD_B = (
    b'{"annotations":{"topic":"Diversity","source_name":"Census Bureau"},'
    b'"data":[{"Year":2013}]}'
)


def test_canonical_hash_ignores_key_order():
    """The whole reason this function exists: same data, different bytes."""
    assert canonical_json_sha256(PAYLOAD_A) == canonical_json_sha256(PAYLOAD_B)


def test_raw_hash_does_not_ignore_key_order():
    """Why hashing the raw body was the wrong answer.

    Without this contrast the canonical hash reads as needless ceremony. Raw
    hashing reports a change on every single run and re-lands identical data
    forever - which is exactly what it did before this was measured.
    """
    assert sha256_bytes(PAYLOAD_A) != sha256_bytes(PAYLOAD_B)


def test_canonical_hash_still_detects_a_real_change():
    """Order-insensitivity must not quietly become change-insensitivity."""
    changed = PAYLOAD_A.replace(b"2013", b"2014")
    assert canonical_json_sha256(PAYLOAD_A) != canonical_json_sha256(changed)


# ---------------------------------------------------------------------------
# needs_download
# ---------------------------------------------------------------------------

def test_unknown_file_is_always_fetched():
    assert needs_download(None, 100, "Wed, 10 Sep 2026 12:00:00 GMT") is True


def test_metadata_agreement_is_evidence_of_no_change():
    prior = state()
    assert needs_download(prior, prior.size_bytes, prior.last_modified) is False


@pytest.mark.parametrize(
    "size, last_modified",
    [
        (101, "Wed, 10 Sep 2026 12:00:00 GMT"),   # size moved
        (100, "Thu, 11 Sep 2026 12:00:00 GMT"),   # timestamp moved
        (None, "Wed, 10 Sep 2026 12:00:00 GMT"),  # no Content-Length offered
        (100, None),                              # no Last-Modified offered
    ],
    ids=["size", "timestamp", "no-size", "no-timestamp"],
)
def test_metadata_disagreement_only_forces_a_fetch(size, last_modified):
    """Disagreement is not evidence of change.

    It means only that the headers cannot rule change out, so the body is
    fetched and the hash decides. BLS republishes byte-identical files with
    fresh timestamps; treating a moved timestamp as proof of change would
    re-land unchanged data every quarter.
    """
    assert needs_download(state(), size, last_modified) is True


# ---------------------------------------------------------------------------
# content_changed
# ---------------------------------------------------------------------------

def test_content_changed_without_prior_state():
    assert content_changed(None, "b" * 64) is True


def test_content_unchanged_when_digest_matches():
    assert content_changed(state(sha256="b" * 64), "b" * 64) is False


def test_content_changed_when_digest_differs():
    assert content_changed(state(sha256="b" * 64), "c" * 64) is True


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_manifest_round_trip(tmp_path):
    store = JsonManifestStore(tmp_path / "nested" / "manifest.json")
    states = {
        "bls/pr/pr.class": state(),
        "population/us.json": state(key="population/us.json", last_modified=None),
    }
    store.save(states)
    assert store.load() == states


def test_absent_manifest_loads_as_empty(tmp_path):
    """First run. An empty manifest means everything is new, not an error."""
    assert JsonManifestStore(tmp_path / "absent.json").load() == {}


def test_manifest_written_before_removed_at_existed_still_loads(tmp_path):
    """removed_at was added after the first manifests had been written.

    A stored document lacking the field must still load, or the upgrade that
    introduced tombstones would have thrown away the whole manifest and
    re-downloaded every file while appearing to work.
    """
    path = tmp_path / "manifest.json"
    legacy = {
        "bls/pr/pr.class": {
            "key": "bls/pr/pr.class",
            "sha256": "a" * 64,
            "size_bytes": 100,
            "last_modified": None,
            "landed_path": "/x",
            "ingested_at": "2026-09-10T12:00:00+00:00",
        }
    }
    path.write_text(json.dumps(legacy), encoding="utf-8")

    loaded = JsonManifestStore(path).load()

    assert loaded["bls/pr/pr.class"].removed_at is None
