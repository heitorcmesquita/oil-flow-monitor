import pandas as pd

path = r"./data/silver/tankers_silver.parquet"

df = pd.read_parquet(path)
print(len(df))
# ------------------------------------------------------------------------------
# Strait of Hormuz outbound chokepoint filter
# ------------------------------------------------------------------------------

# Approximate narrow outbound corridor BEFORE entering the Gulf
# Focus:
# - ships exiting Persian Gulf
# - avoids most UAE local port traffic
# - captures likely globally relevant tanker flows

HORMUZ_FILTER = (
    (df["latitude"] >= 24.0)
    &
    (df["latitude"] <= 27.5)
    &
    (df["longitude"] >= 56.0)
    &
    (df["longitude"] <= 50.5)
)

hormuz = df[HORMUZ_FILTER].copy()

print("\nHormuz tanker rows:\n")
print(hormuz.shape)

# ------------------------------------------------------------------------------
# Inspect vessels
# ------------------------------------------------------------------------------

print("\nSample vessels:\n")

print(
    hormuz[
        [
            "ship_name",
            "destination",
            "CountryName",
            "cargo_category",
            "draught",
            "latitude",
            "longitude",
        ]
    ]
    .sort_values("draught", ascending=False)
    .head(50)
)

# ------------------------------------------------------------------------------
# Destination countries
# ------------------------------------------------------------------------------

print("\nDestination Countries:\n")

print(
    hormuz["CountryName"]
    .fillna("UNKNOWN")
    .value_counts()
)

# ------------------------------------------------------------------------------
# Raw destinations
# ------------------------------------------------------------------------------

print("\nRaw Destinations:\n")

print(
    hormuz["destination"]
    .fillna("UNKNOWN")
    .value_counts()
    .head(50)
)

# ------------------------------------------------------------------------------
# LNG-only flows
# ------------------------------------------------------------------------------

lng = hormuz[
    hormuz["cargo_category"]
    == "LNG/LPG Tanker"
]

print("\nLNG/LPG Destinations:\n")

print(
    lng["CountryName"]
    .fillna("UNKNOWN")
    .value_counts()
)

# ------------------------------------------------------------------------------
# Largest draught vessels
# ------------------------------------------------------------------------------

print("\nLargest Draught Tankers:\n")

print(
    hormuz[
        [
            "ship_name",
            "CountryName",
            "draught",
            "destination",
        ]
    ]
    .sort_values(
        "draught",
        ascending=False
    )
    .head(20)
)