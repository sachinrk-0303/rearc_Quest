"""Data USA population source: fetch and conditional landing.

The ACS 1-year national population series, from the Tesseract API.

Two differences from BLS shape the approach:

  * There is no HEAD to ask and no Last-Modified header, so no cheap
    pre-filter exists. Every run fetches the body.
  * The response is not byte-stable. The server serialises the `annotations`
    object from an unordered map, so two payloads carrying identical data
    differ in byte order. Change detection therefore hashes canonicalised
    content rather than raw bytes.

The widely-cited legacy endpoint datausa.io/api/data now returns 404 with an
HTML body; Tesseract on api.datausa.io is the current API. The 1-year cube is
pinned deliberately - the 5-year cube returns different values that will not
reconcile with the expected answers.
"""

from __future__ import annotations

import json
from pathlib import Path

import requests

from .landing import SyncResult, apply_fetch
from .manifest import FileState, canonical_json_sha256

POPULATION_URL = "https://api.datausa.io/tesseract/data.jsonrecords"
POPULATION_PARAMS = {
    "cube": "acs_yg_total_population_1",
    "drilldowns": "Year,Nation",
    "locale": "en",
    "measures": "Population",
}

KEY = "population/us_population_acs1_year_nation.json"
GET_TIMEOUT = 120


def summarise(body: bytes) -> tuple[int, list[int]]:
    """Row count and any gaps in the year sequence.

    Surfaced in the run log so the ACS 2020 gap is visible at ingest time
    rather than discovered later as a mysteriously short join. Reporting only -
    a gap is a property of the source, not an error.
    """
    payload = json.loads(body)
    years = sorted(int(row["Year"]) for row in payload["data"])
    missing = [y for y in range(years[0], years[-1] + 1) if y not in years]
    return len(years), missing


def sync(
    session: requests.Session,
    landing_root: Path,
    ingest_date: str,
    prior: dict[str, FileState],
) -> SyncResult:
    """Fetch the population series and land it if its content changed."""
    response = session.get(POPULATION_URL, params=POPULATION_PARAMS, timeout=GET_TIMEOUT)
    response.raise_for_status()
    body = response.content

    payload = json.loads(body)
    rows = payload.get("data", [])
    total = payload.get("page", {}).get("total")

    # The endpoint reports its own total. Observed responses carry
    # {"limit": 0, "offset": 0, "total": 11} - limit 0 meaning unpaged - so no
    # pagination is implemented. This assertion is what makes that safe: if the
    # API ever starts capping results, the run fails instead of silently
    # landing a truncated series.
    if total is not None and len(rows) != total:
        raise RuntimeError(
            f"Population response is truncated: {len(rows)} rows of {total}. "
            "The endpoint has begun paging and ingestion must page with it."
        )

    return apply_fetch(
        key=KEY,
        data=body,
        digest=canonical_json_sha256(body),   # by value, not by bytes
        last_modified=None,                   # none offered; forces a fetch every run
        downloaded=len(body),
        prior=prior.get(KEY),
        landing_root=landing_root,
        ingest_date=ingest_date,
    )
