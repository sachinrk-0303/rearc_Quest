"""Landing: what a fetched body means, and whether it hits the disk.

The rule these tests pin down is the one that makes re-running the job cheap:
a body that arrived is not automatically a file that lands.
"""

from pathlib import Path

from ingest.landing import apply_fetch, land
from ingest.manifest import FileState

KEY = "bls/pr/pr.class"
DIGEST = "a" * 64


def prior(sha256: str = DIGEST) -> FileState:
    return FileState(
        key=KEY,
        sha256=sha256,
        size_bytes=1,
        last_modified="Wed, 10 Sep 2026 12:00:00 GMT",
        landed_path="/previous/ingest_date=2026-09-10/pr.class",
        ingested_at="2026-09-10T12:00:00+00:00",
    )


def fetch(tmp_path, *, data=b"data", digest=DIGEST, prior_state=None):
    return apply_fetch(
        key=KEY,
        data=data,
        digest=digest,
        last_modified="Thu, 11 Sep 2026 12:00:00 GMT",
        downloaded=len(data),
        prior=prior_state,
        landing_root=tmp_path,
        ingest_date="2026-09-13",
    )


def test_land_writes_into_a_dated_partition(tmp_path):
    """The partition sits above the filename, so the path is itself
    provenance and every landing is a new path rather than an overwrite."""
    path = Path(land(tmp_path, "2026-09-13", KEY, b"data"))

    assert path == tmp_path / "bls" / "pr" / "ingest_date=2026-09-13" / "pr.class"
    assert path.read_bytes() == b"data"


def test_a_new_file_lands(tmp_path):
    result = fetch(tmp_path, prior_state=None)

    assert result.action == "landed_new"
    assert Path(result.state.landed_path).read_bytes() == b"data"


def test_changed_content_lands_again(tmp_path):
    result = fetch(tmp_path, prior_state=prior(sha256="b" * 64))

    assert result.action == "landed_changed"
    assert Path(result.state.landed_path).exists()


def test_identical_content_does_not_land(tmp_path):
    """Republished with a fresh timestamp but the same bytes.

    This is the case that makes re-runs nearly free, and the one with a real
    trap in it. Landing the file would put a new path in front of Auto Loader
    and duplicate every row it contains. Not refreshing the stored metadata
    would leave the cheap pre-filter permanently mismatched, so the file would
    be re-downloaded on every run forever.

    So: metadata updated, nothing written.
    """
    result = fetch(tmp_path, prior_state=prior())

    assert result.action == "unchanged_content"
    assert result.state.landed_path == "/previous/ingest_date=2026-09-10/pr.class"
    assert result.state.last_modified == "Thu, 11 Sep 2026 12:00:00 GMT"
    assert result.state.size_bytes == 4
    assert not (tmp_path / "bls").exists()
