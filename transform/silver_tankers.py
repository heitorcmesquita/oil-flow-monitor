#!/usr/bin/env python3

"""
transform/silver_tankers.py

Reads Bronze enriched vessel position parquet files,
normalizes/enriches data,
and writes Silver parquet outputs.

IMPORTANT:
This version DOES NOT deduplicate by MMSI.

It keeps ALL vessel position events so movement density
and chokepoint traffic remain visible on maps.
"""

from pathlib import Path

import pandas as pd

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

DATA_ROOT = Path("./data")

BRONZE_DIR = (
    DATA_ROOT / "bronze"
)

SILVER_DIR = (
    DATA_ROOT / "silver"
)

REFERENCE_DIR = (
    DATA_ROOT / "reference"
)

COUNTRY_CODES_FILE = (
    REFERENCE_DIR
    / "country_codes.csv"
)

# ------------------------------------------------------------------------------
# Load Bronze
# ------------------------------------------------------------------------------

bronze_files = list(
    BRONZE_DIR.glob("*.parquet")
)

if not bronze_files:

    raise ValueError(
        "No bronze parquet files found"
    )

df = pd.concat(
    [
        pd.read_parquet(f)
        for f in bronze_files
    ],
    ignore_index=True
)

print(
    f"Loaded Bronze rows: {len(df)}"
)

# ------------------------------------------------------------------------------
# Normalize destination
# ------------------------------------------------------------------------------

df["destination"] = (
    df["destination"]
    .fillna("")
    .astype(str)
    .str.upper()
    .str.replace(
        " ",
        "",
        regex=False
    )
)

# ------------------------------------------------------------------------------
# Extract country code
# ------------------------------------------------------------------------------

df["destination_country_code"] = (
    df["destination"]
    .str[:2]
)

# ------------------------------------------------------------------------------
# Load country reference
# ------------------------------------------------------------------------------

countries = pd.read_csv(
    COUNTRY_CODES_FILE
)

countries["CountryCode"] = (
    countries["CountryCode"]
    .astype(str)
    .str.upper()
)

# ------------------------------------------------------------------------------
# Join country names
# ------------------------------------------------------------------------------

df = df.merge(
    countries,
    left_on=(
        "destination_country_code"
    ),
    right_on="CountryCode",
    how="left"
)

# ------------------------------------------------------------------------------
# Tanker category mapping
# ------------------------------------------------------------------------------

TANKER_MAP = {
    80: "Oil Tanker",
    81: "Chemical Tanker",
    82: "Oil Tanker",
    83: "Oil Tanker",
    84: "LNG/LPG Tanker",
    85: "Oil Tanker",
    86: "Oil Tanker",
    87: "Oil Tanker",
    88: "Oil Tanker",
    89: "Oil Tanker",
}

df["cargo_category"] = (
    df["ship_type"]
    .map(TANKER_MAP)
)

# ------------------------------------------------------------------------------
# Datetime normalization
# ------------------------------------------------------------------------------

df["event_time"] = pd.to_datetime(
    df["event_time"],
    errors="coerce"
)

# ------------------------------------------------------------------------------
# Numeric normalization
# ------------------------------------------------------------------------------

numeric_columns = [
    "latitude",
    "longitude",
    "draught",
    "sog",
    "cog",
    "true_heading",
]

for col in numeric_columns:

    if col in df.columns:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

# ------------------------------------------------------------------------------
# Remove invalid coordinates
# ------------------------------------------------------------------------------

df = df.dropna(
    subset=["latitude", "longitude"]
)

# ------------------------------------------------------------------------------
# Remove exact duplicate rows only
# ------------------------------------------------------------------------------

before = len(df)

df = df.drop_duplicates()

after = len(df)

print(
    f"Removed duplicates: {before - after}"
)

print(
    f"Silver rows retained: {len(df)}"
)

# ------------------------------------------------------------------------------
# Write Silver
# ------------------------------------------------------------------------------

SILVER_DIR.mkdir(
    parents=True,
    exist_ok=True
)

output_file = (
    SILVER_DIR
    / "tankers_silver.parquet"
)

df.to_parquet(
    output_file,
    index=False
)

print(
    f"Saved Silver parquet: {output_file}"
)

# ------------------------------------------------------------------------------
# Quick analytics
# ------------------------------------------------------------------------------

print("\nTop destination countries:\n")

print(
    df["CountryName"]
    .fillna("UNKNOWN")
    .value_counts()
    .head(20)
)

print("\nCargo categories:\n")

print(
    df["cargo_category"]
    .value_counts()
)