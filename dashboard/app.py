#!/usr/bin/env python3

import pandas as pd
import streamlit as st
import pydeck as pdk
from pathlib import Path

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------

st.set_page_config(
    page_title="Oil Flow Monitor",
    layout="wide"
)

st.markdown(
    """
    <style>
    .stApp {
        background-color: #0e1117;
        color: #f0f6fc;
    }

    header {
        visibility: hidden;
    }

    section[data-testid="stSidebar"] {
        background-color: #161b22;
        border-right: 1px solid #30363d;
    }

    div[data-testid="metric-container"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        padding: 20px;
        border-radius: 14px;
        box-shadow: 0 0 12px rgba(0,0,0,0.4);
    }

    div[data-testid="metric-container"] * {
        color: #ffffff !important;
    }

    .stSelectbox div[data-baseweb="select"] > div {
        background-color: #161b22 !important;
        color: #ffffff !important;
        border: 1px solid #30363d !important;
    }

    .stMultiSelect div[data-baseweb="select"] > div {
        background-color: #161b22 !important;
        color: #ffffff !important;
        border: 1px solid #30363d !important;
    }

    h1, h2, h3, label, p, span {
        color: #f0f6fc !important;
    }

    .stDataFrame {
        background-color: #161b22;
    }
    </style>
    """,
    unsafe_allow_html=True
)

SILVER_PATH = Path(
    "./data/silver/tankers_silver.parquet"
)

# ------------------------------------------------------------------------------
# Load data
# ------------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_data():

    if not SILVER_PATH.exists():
        return pd.DataFrame()

    df = pd.read_parquet(
        SILVER_PATH
    )

    return df


df = load_data()

# ------------------------------------------------------------------------------
# Title
# ------------------------------------------------------------------------------

st.title("Global Oil & LNG Tanker Monitor")

st.caption(
    "Live maritime tanker intelligence powered by AIS (Automatic Identification System) data via AISStream"
)

if df.empty:

    st.warning(
        "No Silver parquet data found."
    )

    st.stop()

# ------------------------------------------------------------------------------
# Normalize columns
# ------------------------------------------------------------------------------

numeric_columns = [
    "latitude",
    "longitude",
    "draught",
    "sog",
]

for col in numeric_columns:

    if col in df.columns:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

# ------------------------------------------------------------------------------
# Sidebar filters
# ------------------------------------------------------------------------------

st.sidebar.header("Filters")

cargo_options = sorted(
    df["cargo_category"]
    .dropna()
    .unique()
)

selected_cargo = st.sidebar.multiselect(
    "Cargo Categories",
    cargo_options,
    default=cargo_options
)

country_options = sorted(
    df["CountryName"]
    .fillna("UNKNOWN")
    .unique()
)

selected_countries = st.sidebar.multiselect(
    "Destination Countries",
    country_options,
    default=country_options
)

min_speed = st.sidebar.slider(
    "Minimum Speed",
    min_value=0.0,
    max_value=40.0,
    value=0.0,
    step=0.5
)

min_draught = st.sidebar.slider(
    "Minimum Draught",
    min_value=0.0,
    max_value=25.0,
    value=0.0,
    step=0.5
)

# ------------------------------------------------------------------------------
# Apply filters
# ------------------------------------------------------------------------------

filtered = df.copy()

filtered = filtered[
    filtered["cargo_category"]
    .isin(selected_cargo)
]

filtered = filtered[
    filtered["CountryName"]
    .fillna("UNKNOWN")
    .isin(selected_countries)
]

filtered = filtered[
    filtered["draught"]
    .fillna(0)
    >= min_draught
]

filtered = filtered[
    filtered["sog"]
    .fillna(0)
    >= min_speed
]

filtered = filtered.dropna(
    subset=["latitude", "longitude"]
)

# ------------------------------------------------------------------------------
# Metrics
# ------------------------------------------------------------------------------

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Position Events",
    len(filtered)
)

col2.metric(
    "Unique Ships",
    filtered["mmsi"].nunique()
)

col3.metric(
    "Avg Speed",
    round(
        filtered["sog"]
        .fillna(0)
        .mean(),
        2
    )
)

col4.metric(
    "Countries",
    filtered["CountryName"]
    .nunique()
)

# ------------------------------------------------------------------------------
# Map
# ------------------------------------------------------------------------------

st.subheader(
    "Live Tanker Position Events"
)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered,
    get_position='[longitude, latitude]',
    get_radius=12000,
    get_fill_color='[255, 255, 255, 180]',
    pickable=True,
    auto_highlight=True,
)

view_state = pdk.ViewState(
    latitude=20,
    longitude=10,
    zoom=1.2,
)

tooltip = {
    "html": """
    <div style='font-size:14px;'>
    <b>Ship:</b> {ship_name}<br/>
    <b>Destination:</b> {destination}<br/>
    <b>Country:</b> {CountryName}<br/>
    <b>Cargo:</b> {cargo_category}<br/>
    <b>Speed:</b> {sog}<br/>
    <b>Draught:</b> {draught}<br/>
    <b>Heading:</b> {true_heading}<br/>
    <b>Status:</b> {nav_status}
    </div>
    """,
    "style": {
        "backgroundColor": "#161b22",
        "color": "white",
        "border": "1px solid #30363d"
    }
}

st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="dark"
    )
)

# ------------------------------------------------------------------------------
# Chokepoint analytics
# ------------------------------------------------------------------------------

st.subheader("Hormuz Traffic")

hormuz = filtered[
    (
        filtered["latitude"]
        >= 24
    )
    &
    (
        filtered["latitude"]
        <= 28
    )
    &
    (
        filtered["longitude"]
        >= 54
    )
    &
    (
        filtered["longitude"]
        <= 58
    )
]

st.metric(
    "Hormuz Position Events",
    len(hormuz)
)

st.dataframe(
    hormuz[
        [
            "ship_name",
            "CountryName",
            "destination",
            "cargo_category",
            "sog",
            "draught"
        ]
    ].head(50),
    use_container_width=True
)

# ------------------------------------------------------------------------------
# Fastest vessels
# ------------------------------------------------------------------------------

st.subheader(
    "Fastest Moving Tankers"
)

movement = filtered[
    filtered["sog"]
    .fillna(0)
    > 1
]

fastest = movement.sort_values(
    "sog",
    ascending=False
)

st.dataframe(
    fastest[
        [
            "ship_name",
            "CountryName",
            "cargo_category",
            "sog",
            "draught",
            "destination"
        ]
    ].head(20),
    use_container_width=True
)

# ------------------------------------------------------------------------------
# Top destinations
# ------------------------------------------------------------------------------

st.subheader(
    "Top Destination Countries"
)

country_counts = (
    filtered["CountryName"]
    .fillna("UNKNOWN")
    .value_counts()
    .reset_index()
)

country_counts.columns = [
    "Country",
    "Events"
]

st.dataframe(
    country_counts,
    use_container_width=True
)

# ------------------------------------------------------------------------------
# Raw data
# ------------------------------------------------------------------------------

with st.expander(
    "Raw Silver Data"
):

    st.dataframe(
        filtered,
        use_container_width=True
    )