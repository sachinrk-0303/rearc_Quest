"""BLS source: dynamic discovery and conditional download.

The quest requires that re-running ingestion after the source has added,
changed or removed data still works, and does not reprocess what it already
holds. Three mechanisms implement that:

  * filenames are discovered from the directory listing, never hardcoded
  * every file is HEAD-checked before any body is transferred
  * only a SHA-256 difference causes a file to land

BLS returns 403 to requests carrying no contact information. Their access
policy reserves the right to block robots that cannot be contacted, so the
User-Agent carries an email address. It deliberately does not impersonate a
browser: that would be evasion, where the policy asks for identification.
"""

from __future__ import annotations

import re
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .landing import SyncResult, apply_fetch
from .manifest import FileState, needs_download, sha256_bytes

BLS_DIR = "https://download.bls.gov/pub/time.series/pr/"
HEAD_TIMEOUT = 30
GET_TIMEOUT = 300

# Matches pr.* filenames in the listing's anchors, tolerating either an
# absolute href (/pub/time.series/pr/pr.class) or a bare relative one.
_HREF = re.compile(r'href="(?:[^"]*/)?(pr\.[A-Za-z0-9._-]+)"', re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def build_session(contact_email: str) -> requests.Session:
    """A session that identifies itself and backs off politely.

    Only plausibly transient failures are retried. 403 is deliberately absent
    from the retry list: it is a policy decision by BLS, not a blip, and
    repeatedly hammering it would be precisely the robot behaviour their access
    policy exists to discourage. A 403 should surface as a failure so the cause
    is fixed, not smothered by retries.
    """
    session = requests.Session()
    session.headers["User-Agent"] = f"rearc-data-quest/0.1 ({contact_email})"

    retry = Retry(
        total=4,
        backoff_factor=1.0,                       # 1s, 2s, 4s, 8s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def head_metadata(session: requests.Session, url: str) -> tuple[int | None, str | None]:
    """Size and Last-Modified, at a cost of zero content bytes."""
    response = session.head(url, timeout=HEAD_TIMEOUT)
    response.raise_for_status()
    size = response.headers.get("Content-Length")
    return (int(size) if size is not None else None), response.headers.get("Last-Modified")


def _get(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=GET_TIMEOUT)
    response.raise_for_status()
    return response.content


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def parse_listing(html: str) -> list[str]:
    """Extract pr.* filenames from the directory listing.

    Pure, so it can be tested against a saved copy of the page.

    Raises rather than returning an empty list. A listing that yields no files
    means the page format has changed - and a run that silently ingests nothing
    while reporting success is far worse than one that fails.
    """
    names = sorted(set(_HREF.findall(html)))
    if not names:
        raise RuntimeError(
            "BLS listing contained no pr.* files. The page format has changed "
            "and discovery must be updated; refusing to proceed with an empty "
            "file set."
        )
    return names


def detect_removed(prior: dict[str, FileState], seen: set[str]) -> list[str]:
    """Keys we hold that the source no longer advertises."""
    return sorted(k for k in prior if k.startswith("bls/") and k not in seen)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def sync(
    session: requests.Session,
    landing_root: Path,
    ingest_date: str,
    prior: dict[str, FileState],
) -> tuple[list[SyncResult], list[str]]:
    """Bring the landing zone up to date with BLS.

    Returns the per-file results and any keys the source no longer lists.
    """
    results: list[SyncResult] = []

    # The listing is always fetched: it is how discovery happens, so there is no
    # metadata short-circuit available for it. It is still subject to the same
    # content rule, and lands only when it actually changes.
    listing_body = _get(session, BLS_DIR)
    results.append(
        apply_fetch(
            key="bls/listing.html",
            data=listing_body,
            digest=sha256_bytes(listing_body),
            last_modified=None,
            downloaded=len(listing_body),
            prior=prior.get("bls/listing.html"),
            landing_root=landing_root,
            ingest_date=ingest_date,
        )
    )

    names = parse_listing(listing_body.decode("utf-8", errors="replace"))

    for name in names:
        key = f"bls/pr/{name}"
        url = BLS_DIR + name
        size, last_modified = head_metadata(session, url)
        prior_state = prior.get(key)

        if not needs_download(prior_state, size, last_modified):
            results.append(
                SyncResult(key, "skipped_metadata", 0, prior_state)
            )
            continue

        body = _get(session, url)
        results.append(
            apply_fetch(
                key=key,
                data=body,
                digest=sha256_bytes(body),
                last_modified=last_modified,
                downloaded=len(body),
                prior=prior_state,
                landing_root=landing_root,
                ingest_date=ingest_date,
            )
        )

    seen = {"bls/listing.html"} | {f"bls/pr/{n}" for n in names}
    return results, detect_removed(prior, seen)