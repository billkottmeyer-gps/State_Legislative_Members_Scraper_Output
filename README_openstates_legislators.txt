# Open States legislator pull

This folder contains a one-file Python script that downloads current legislators for all 50 states from Open States and writes a single CSV.

## Files
- `openstates_state_legislators.py`
- `openstates_urls.txt`

## Expected output
Running the script creates:
- `us_state_legislators_current.csv`
- `us_state_legislators_errors.csv` (only if any states fail)

## Required packages
- pandas
- requests

Install:
`pip install pandas requests`

Run:
`python openstates_state_legislators.py`
