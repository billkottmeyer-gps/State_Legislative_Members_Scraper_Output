#!/usr/bin/env python3
"""
Build a complete current roster of elected U.S. state legislators from Open States.

Output columns:
- State
- Chamber
- District
- Party
- Name

This script downloads one CSV per state from:
https://data.openstates.org/people/current/[ABBR].csv

It standardizes the result into a single national CSV.
"""

from __future__ import annotations

import io
import sys
import time
from typing import List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

STATE_ABBRS: List[str] = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

BASE_URL = "https://data.openstates.org/people/current/{abbr}.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://open.pluralpolicy.com/data/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

CHAMBER_MAP = {
    "upper": "State Senate",
    "lower": "State House",
}

def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def normalize_chamber(state: str, chamber_value: str) -> str:
    val = (chamber_value or "").strip().lower()
    if state == "NE":
        return "Unicameral Legislature"
    return CHAMBER_MAP.get(val, chamber_value)

def fetch_state_csv(session: requests.Session, abbr: str) -> pd.DataFrame:
    url = BASE_URL.format(abbr=abbr.lower())
    resp = session.get(url, timeout=60)

    if resp.status_code == 403:
        raise ValueError(
            f"{abbr}: HTTP 403 Forbidden for {url}. "
            "The server rejected the download request."
        )

    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text), dtype=str).fillna("")
    required = ["name", "current_party", "current_district", "current_chamber"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{abbr}: missing expected columns: {missing}")

    out = pd.DataFrame({
        "State": abbr,
        "Chamber": df["current_chamber"].map(lambda x: normalize_chamber(abbr, x)),
        "District": df["current_district"],
        "Party": df["current_party"],
        "Name": df["name"],
    })

    out = out[
        (out["Name"].str.strip() != "") &
        (out["Chamber"].str.strip() != "") &
        (out["District"].str.strip() != "")
    ].copy()

    return out

def main() -> int:
    session = build_session()
    frames = []
    errors = []

    for abbr in STATE_ABBRS:
        try:
            df = fetch_state_csv(session, abbr)
            frames.append(df)
            print(f"Fetched {abbr}: {len(df)} rows")
            time.sleep(0.5)
        except Exception as e:
            errors.append((abbr, str(e)))
            print(f"ERROR {abbr}: {e}", file=sys.stderr)

    if not frames:
        print("No data fetched.", file=sys.stderr)
        return 1

    combined = pd.concat(frames, ignore_index=True)

    combined["District_sort"] = pd.to_numeric(combined["District"], errors="coerce")
    combined = combined.sort_values(
        by=["State", "Chamber", "District_sort", "District", "Name"],
        kind="stable"
    ).drop(columns=["District_sort"])

    combined.to_csv("us_state_legislators_current.csv", index=False)
    print(f"\nWrote us_state_legislators_current.csv with {len(combined)} rows.")

    if errors:
        err_df = pd.DataFrame(errors, columns=["State", "Error"])
        err_df.to_csv("us_state_legislators_errors.csv", index=False)
        print(f"Wrote us_state_legislators_errors.csv with {len(err_df)} errors.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
