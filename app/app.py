# ============================================================
# App bootstrap (DO NOT MOVE)
# ============================================================
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ============================================================
# Imports
# ============================================================
import json
import streamlit as st
import pandas as pd
import plotly.express as px

from etl.db_utils import get_engine

# ============================================================
# Page config
# ============================================================
st.set_page_config(
    page_title="PhonePe Pulse Dashboard",
    layout="wide"
)

# ============================================================
# Database connection
# ============================================================
engine = get_engine()

# ============================================================
# Helper functions
# ============================================================
def normalize_state(name: str) -> str:
    mapping = {
        "Andaman & Nicobar Islands": "Andaman and Nicobar",
        "Dadra & Nagar Haveli & Daman & Diu": "Dadra and Nagar Haveli",
        "Jammu & Kashmir": "Jammu and Kashmir",
        "Odisha": "Orissa",
        "Uttarakhand": "Uttaranchal",
        # GeoJSON limitations
        "Ladakh": "Jammu and Kashmir",
        "Telangana": "Andhra Pradesh",
    }
    return mapping.get(name, name)


def normalize_state_for_district(state: str) -> str:
    if state in ["Ladakh", "Jammu & Kashmir"]:
        return "Jammu and Kashmir"
    return normalize_state(state)

def normalize_district(name: str) -> str:
    name = name.lower()

    replacements = [
        " district",
        " districts",
        " urban",
        " rural",
        " metropolitan",
        " city"
    ]

    for r in replacements:
        name = name.replace(r, "")

    return name.strip().title()

@st.cache_data
def load_state_geojson():
    with open("data/india_states.geojson", "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def load_district_geojson():
    with open("data/india_districts.geojson", "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def load_years():
    return sorted(
        pd.read_sql(
            "SELECT DISTINCT year FROM aggregated_transaction ORDER BY year",
            engine
        )["year"]
    )

# ============================================================
# Sidebar filters
# ============================================================
st.sidebar.title("Filters")

year = st.sidebar.selectbox(
    "Select Year",
    load_years(),
    key="sidebar_year"
)

quarter = st.sidebar.selectbox(
    "Select Quarter",
    [1, 2, 3, 4],
    key="sidebar_quarter"
)

st.sidebar.caption(
    "Changing filters updates all charts and maps."
)

# ============================================================
# Page title & tabs
# ============================================================
st.title("📊 PhonePe Pulse Transaction Analysis")

tab_txn, tab_users, tab_top, tab_maps, tab_ins = st.tabs(
    ["💳 Transactions", "👥 Users", "🏆 Top Performers", "🗺️ Maps", "🛡️ Insurance"]
)

# ============================================================
# TRANSACTIONS TAB
# ============================================================
with tab_txn:
    st.subheader("Top States by Transaction Amount")

    df_txn_state = pd.read_sql(
        f"""
        SELECT state, SUM(txn_amount) AS total_amount
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        ORDER BY total_amount DESC
        LIMIT 10
        """,
        engine
    )

    st.plotly_chart(
        px.bar(
            df_txn_state,
            x="state",
            y="total_amount",
            title="Top 10 States by Transaction Amount"
        ),
        use_container_width=True
    )

    st.subheader("Transaction Breakdown by Category")

    df_txn_type = pd.read_sql(
        f"""
        SELECT transaction_type, SUM(txn_amount) AS amount
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY transaction_type
        """,
        engine
    )

    st.plotly_chart(
        px.pie(
            df_txn_type,
            names="transaction_type",
            values="amount",
            title="Transaction Amount by Category"
        ),
        use_container_width=True
    )

# ============================================================
# USERS TAB
# ============================================================
with tab_users:
    st.subheader("User Engagement by State")

    df_users = pd.read_sql(
        """
        SELECT state,
               SUM(registered_users) AS users,
               SUM(app_opens) AS app_opens
        FROM map_user
        GROUP BY state
        ORDER BY users DESC
        LIMIT 10
        """,
        engine
    )

    st.plotly_chart(
        px.bar(
            df_users,
            x="state",
            y=["users", "app_opens"],
            barmode="group",
            title="User Engagement Metrics"
        ),
        use_container_width=True
    )

# ============================================================
# TOP PERFORMERS TAB
# ============================================================
with tab_top:
    st.subheader("Top Districts by Transaction Amount")

    state_selected = st.selectbox(
        "Select State",
        sorted(
            pd.read_sql(
                "SELECT DISTINCT state FROM map_transaction ORDER BY state",
                engine
            )["state"]
        ),
        key="top_state_select"
    )

    df_top_dist = pd.read_sql(
        f"""
        SELECT district, SUM(txn_amount) AS total_amount
        FROM map_transaction
        WHERE state = '{state_selected}'
          AND year = {year}
          AND quarter = {quarter}
        GROUP BY district
        ORDER BY total_amount DESC
        LIMIT 10
        """,
        engine
    )

    st.plotly_chart(
        px.bar(
            df_top_dist,
            x="district",
            y="total_amount",
            title=f"Top Districts in {state_selected}"
        ),
        use_container_width=True
    )

# ============================================================
# MAPS TAB
# ============================================================
with tab_maps:
    st.subheader("🗺️ Transaction Amount by State")

    # ---------------- STATE MAP ----------------
    df_state = pd.read_sql(
        f"""
        SELECT state, SUM(txn_amount) AS total_amount
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        """,
        engine
    )

    # Normalize state names for GeoJSON
    df_state["state"] = df_state["state"].apply(normalize_state)

    fig_state = px.choropleth(
        df_state,
        geojson=load_state_geojson(),
        locations="state",
        featureidkey="properties.NAME_1",
        color="total_amount",
        hover_name="state",
        hover_data={"total_amount": ":,.0f"},
        color_continuous_scale="Blues",
        title="Transaction Amount by State"
    )

    fig_state.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_state, use_container_width=True)

    st.caption(
        "White regions indicate zero or unavailable transaction data for the selected period."
    )

    st.divider()

    # ---------------- DISTRICT MAP ----------------
    st.subheader("📍 Transaction Amount by District")

    # IMPORTANT: use ORIGINAL DB state names for SQL
    map_state = st.selectbox(
        "Select State",
        sorted(
            pd.read_sql(
                "SELECT DISTINCT state FROM map_transaction ORDER BY state",
                engine
            )["state"]
        ),
        key="district_map_state"
    )

    df_dist = pd.read_sql(
        f"""
        SELECT district, SUM(txn_amount) AS total_amount
        FROM map_transaction
        WHERE state = '{map_state}'
          AND year = {year}
          AND quarter = {quarter}
        GROUP BY district
        """,
        engine
    )

    if df_dist.empty:
        st.warning("No district data available for this state.")
        st.stop()

    # -------- Normalize district names (GENERIC, ALL STATES) --------
    df_dist["district_clean"] = df_dist["district"].apply(normalize_district)

    geo_state = normalize_state_for_district(map_state)
    full_geojson = load_district_geojson()

    # Filter GeoJSON to selected state
    district_geojson = {
        "type": "FeatureCollection",
        "features": [
            f for f in full_geojson["features"]
            if f["properties"].get("NAME_1") == geo_state
        ]
    }

    # Normalize district names INSIDE GeoJSON
    for f in district_geojson["features"]:
        f["properties"]["district_clean"] = normalize_district(
            f["properties"].get("NAME_2", "")
        )

    # Keep only districts that exist in GeoJSON
    geo_districts = {
        f["properties"]["district_clean"]
        for f in district_geojson["features"]
    }

    df_dist_matched = df_dist[
        df_dist["district_clean"].isin(geo_districts)
    ]

    fig_dist = px.choropleth(
        df_dist_matched,
        geojson=district_geojson,
        locations="district_clean",
        featureidkey="properties.district_clean",
        color="total_amount",
        hover_name="district",
        hover_data={"total_amount": ":,.0f"},
        color_continuous_scale="Reds",
        title=f"Transaction Amount by District – {map_state}"
    )

    fig_dist.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_dist, use_container_width=True)

    if map_state in ["Ladakh", "Jammu & Kashmir"]:
        st.caption(
            "Note: Ladakh and Jammu & Kashmir are merged due to GeoJSON boundary limitations."
        )

    st.caption(
        "District-level visualization is exploratory due to administrative name variations."
    )

    st.caption(
    "Only districts with matching geographic names are colored. "
    "Unmatched districts are excluded to ensure accurate visualization."
    )

# ============================================================
# INSURANCE TAB
# ============================================================
with tab_ins:
    st.subheader("Insurance Transaction Analysis")

    df_ins_state = pd.read_sql(
        f"""
        SELECT state, SUM(insurance_amount) AS amount
        FROM aggregated_insurance
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        """,
        engine
    )

    st.plotly_chart(
        px.bar(
            df_ins_state.sort_values("amount", ascending=False),
            x="state",
            y="amount",
            title="Insurance Amount by State"
        ),
        use_container_width=True
    )

    st.subheader("Top Insurance Regions")

    df_top_ins = pd.read_sql(
        f"""
        SELECT
            state,
            SUM(insurance_amount) AS insurance_amount
        FROM top_insurance
        WHERE year = {year}
        AND quarter = {quarter}
        GROUP BY state
        ORDER BY insurance_amount DESC
        LIMIT 10
        """,
        engine
    )

    st.caption(
        "Note: The above table lists the top 10 states by insurance amount for the selected period."
    )

    st.dataframe(
        df_top_ins.style.format({"insurance_amount": "{:,.0f}"}),
        use_container_width=True
    )

