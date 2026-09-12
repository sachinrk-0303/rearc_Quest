"""BLS discovery: the logic that decides which files exist.

parse_listing is pure precisely so it can be tested against a saved copy of the
page rather than against BLS.
"""

import pytest

from ingest.bls import detect_removed, parse_listing
from ingest.manifest import FileState

# Both href shapes the listing has been observed to use, plus a duplicate, the
# parent-directory link, and a file belonging to a different survey that lives
# in the same tree.
LISTING = """
<html><body>
<a href="/pub/time.series/pr/pr.class">pr.class</a>
<a href="pr.data.0.Current">pr.data.0.Current</a>
<a href="/pub/time.series/pr/pr.class">pr.class again</a>
<a href="/pub/time.series/">Parent Directory</a>
<a href="/pub/time.series/ap/ap.data.0.Current">ap.data.0.Current</a>
</body></html>
"""


def test_parse_listing_handles_both_href_shapes():
    """Absolute and relative hrefs, deduplicated and ordered."""
    assert parse_listing(LISTING) == ["pr.class", "pr.data.0.Current"]


def test_parse_listing_ignores_other_surveys():
    """ap.* shares the directory tree and is not ours."""
    assert not any(name.startswith("ap") for name in parse_listing(LISTING))


def test_parse_listing_refuses_to_return_nothing():
    """A listing that yields no files means the page format changed.

    Returning an empty list would let the run report success while ingesting
    nothing, which is far worse than failing.
    """
    with pytest.raises(RuntimeError, match="no pr"):
        parse_listing("<html><body>nothing to see</body></html>")


def _state(key: str) -> FileState:
    return FileState(
        key=key,
        sha256="a" * 64,
        size_bytes=1,
        last_modified=None,
        landed_path="/x",
        ingested_at="2026-09-10T00:00:00+00:00",
    )


def test_detect_removed_reports_keys_no_longer_listed():
    prior = {k: _state(k) for k in ("bls/pr/pr.class", "bls/pr/pr.withdrawn")}
    assert detect_removed(prior, {"bls/pr/pr.class"}) == ["bls/pr/pr.withdrawn"]


def test_detect_removed_ignores_the_population_source():
    """A different source with a different discovery mechanism.

    Its absence from the BLS listing says nothing about it, and flagging it
    would tombstone a file that is still perfectly current.
    """
    prior = {k: _state(k) for k in ("bls/pr/pr.class", "population/us.json")}
    assert detect_removed(prior, {"bls/pr/pr.class"}) == []
