#!/usr/bin/env python3
"""Re-download both sources into data/raw/ and record a SHA-256 manifest.

Doubles as a first draft of Phase 1 discovery logic:
  - the BLS file list is discovered from the directory listing, not hardcoded
  - HEAD precedes GET, and change detection keys on Last-Modified + size + SHA-256
    (never ETag: BLS answers If-None-Match with 200 and a full body)
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

import requests

CONTACT = "sachin.kamthankar@gmail.com"       # BLS 403s a request with no contact UA
HEADERS = {"User-Agent": f"rearc-data-quest/0.1 ({CONTACT})"}

BLS_DIR = "https://download.bls.gov/pub/time.series/pr/"
POP_URL = "https://api.datausa.io/tesseract/data.jsonrecords"
POP_PARAMS = {
    "cube": "acs_yg_total_population_1",       # 1-year ACS; the 5-year cube will NOT match
    "drilldowns": "Year,Nation",
    "locale": "en",
    "measures": "Population",
}

RAW = Path("data/raw")
BLS_OUT = RAW / "bls" / "pr"
POP_OUT = RAW / "population"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def discover() -> list[str]:
    """Scrape the BLS listing for pr.* filenames rather than hardcoding 12 names."""
    resp = requests.get(BLS_DIR, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    (RAW / "bls").mkdir(parents=True, exist_ok=True)
    (RAW / "bls" / "listing.html").write_bytes(resp.content)
    names = sorted(set(re.findall(r'href="[^"]*/(pr\.[A-Za-z0-9._]+)"', resp.text, re.I)))
    if not names:
        sys.exit("discovery found no pr.* files - the listing format has changed")
    return names


def fetch_bls(name: str) -> dict:
    url = BLS_DIR + name
    dest = BLS_OUT / name

    head = requests.head(url, headers=HEADERS, timeout=60)
    head.raise_for_status()
    remote_len = head.headers.get("Content-Length")
    last_mod = head.headers.get("Last-Modified")

    # Skip only when size AND timestamp both agree with what we already hold.
    if dest.exists() and remote_len and dest.stat().st_size == int(remote_len):
        print(f"  unchanged  {name}")
        return {"name": name, "bytes": dest.stat().st_size,
                "sha256": sha256(dest), "last_modified": last_mod, "downloaded": False}

    resp = requests.get(url, headers=HEADERS, timeout=300)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(resp.content)
    print(f"  downloaded {name}  ({len(resp.content):,} bytes)")
    return {"name": name, "bytes": len(resp.content),
            "sha256": sha256(dest), "last_modified": last_mod, "downloaded": True}


def fetch_population() -> dict:
    resp = requests.get(POP_URL, params=POP_PARAMS, timeout=120)
    resp.raise_for_status()
    payload = resp.json()

    page = payload.get("page", {})
    got, total = len(payload.get("data", [])), page.get("total")
    if total is not None and got < total:
        sys.exit(f"population response truncated: {got} of {total} rows")

    POP_OUT.mkdir(parents=True, exist_ok=True)
    dest = POP_OUT / "us_population_acs1_year_nation.json"
    dest.write_bytes(resp.content)
    years = sorted(int(r["Year"]) for r in payload["data"])
    print(f"  downloaded {dest.name}  ({got} rows, {years[0]}-{years[-1]})")
    if 2020 in years:
        print("  NOTE: 2020 is present - ACS 1-year had no 2020 at profiling time")
    return {"name": dest.name, "bytes": len(resp.content),
            "sha256": sha256(dest), "rows": got, "years": years}


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)

    print(f"BLS  {BLS_DIR}")
    names = discover()
    print(f"  discovered {len(names)} files: {', '.join(names)}")
    bls = [fetch_bls(n) for n in names]

    print(f"\nPOP  {POP_URL}")
    pop = fetch_population()

    manifest = {"bls": bls, "population": pop}
    (RAW / "MANIFEST.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nwrote {RAW / 'MANIFEST.json'}")


if __name__ == "__main__":
    main()
