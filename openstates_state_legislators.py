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
from typing import List
import requests
import pandas as pd

STATE_ABBRS: List[str] = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

BASE_URL = "https://data.openstates.org/people/current/{abbr}.csv"

CHAMBER_MAP = {
    "upper": "State Senate",
    "lower": "State House",
}

def normalize_chamber(state: str, chamber_value: str) -> str:
    val = (chamber_value or "").strip().lower()
    if state == "NE":
        return "Unicameral Legislature"
    return CHAMBER_MAP.get(val, chamber_value)

def fetch_state_csv(abbr: str) -> pd.DataFrame:
    url = BASE_URL.format(abbr=abbr)
    resp = requests.get(url, timeout=60)
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

    # Keep only rows that look like active legislators with a chamber and district.
    out = out[
        (out["Name"].str.strip() != "") &
        (out["Chamber"].str.strip() != "") &
        (out["District"].str.strip() != "")
    ].copy()

    return out

def main() -> int:
    frames = []
    errors = []

    for abbr in STATE_ABBRS:
        try:
            df = fetch_state_csv(abbr)
            frames.append(df)
            print(f"Fetched {abbr}: {len(df)} rows")
        except Exception as e:
            errors.append((abbr, str(e)))
            print(f"ERROR {abbr}: {e}", file=sys.stderr)

    if not frames:
        print("No data fetched.", file=sys.stderr)
        return 1

    combined = pd.concat(frames, ignore_index=True)

    # Sort for easy downstream use
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
