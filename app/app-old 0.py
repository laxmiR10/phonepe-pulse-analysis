import json
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

DISTRICT_GEOJSON_MAP = {
    "Karnataka": "karnataka",
    "Tamil Nadu": "tamil_nadu",
    "Maharashtra": "maharashtra",
    "Kerala": "kerala",
    "Andhra Pradesh": "andhra_pradesh",
    "Telangana": "telangana",
    "Uttar Pradesh": "uttar_pradesh",
    "Gujarat": "gujarat",
    "Rajasthan": "rajasthan",
    "West Bengal": "west_bengal"
}

SUPPORTED_DISTRICT_STATES = sorted([
    "Karnataka",
    "Tamil Nadu",
    "Maharashtra",
    "Kerala",
    "Andhra Pradesh",
    "Telangana",
    "Uttar Pradesh",
    "Gujarat",
    "Rajasthan",
    "West Bengal"
])

def is_district_map_available(state_name):
    key = DISTRICT_GEOJSON_MAP.get(state_name)
    if not key:
        return False

    url = (
        "https://raw.githubusercontent.com/datameet/maps/master/"
        f"Districts/{key}.geojson"
    )

    try:
        return requests.head(url, timeout=5).status_code == 200
    except requests.RequestException:
        return False


# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="PhonePe Pulse Dashboard",
    layout="wide"
)

# ---------------- DB CONNECTION ----------------
engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

# ---------------- TITLE ----------------
st.title("📊 PhonePe Pulse Data Analysis Dashboard")

# ---------------- SIDEBAR FILTERS ----------------
st.sidebar.header("Filters")

year = st.sidebar.selectbox(
    "Select Year",
    sorted(pd.read_sql(
        "SELECT DISTINCT year FROM aggregated_transaction ORDER BY year",
        engine
    )["year"])
)


@st.cache_data
def load_district_geojson(state_name):
    key = DISTRICT_GEOJSON_MAP.get(state_name)

    if not key:
        return None

    url = (
        "https://raw.githubusercontent.com/datameet/maps/master/"
        f"Districts/{key}.geojson"
    )

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()

    return None

quarter = st.sidebar.selectbox(
    "Select Quarter",
    [1, 2, 3, 4]
)

metric = st.sidebar.radio(
    "Select Metric",
    ["Transaction Amount", "Transaction Count"]
)

metric_column = "txn_amount" if metric == "Transaction Amount" else "txn_count"

# ---------------- TABS ----------------
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Transactions", "Users", "Top Performers", "🗺️ Maps", "🛡️ Insurance"]
)


# =================================================
# TAB 1: TRANSACTIONS
# =================================================
with tab1:
    st.subheader("Top 10 States by Transactions")

    df_txn = pd.read_sql(f"""
        SELECT state, SUM({metric_column}) AS value
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        ORDER BY value DESC
        LIMIT 10;
    """, engine)

    fig = px.bar(
        df_txn,
        x="state",
        y="value",
        title=f"Top 10 States by {metric} ({year} Q{quarter})"
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Payment Category Distribution")

    df_cat = pd.read_sql(f"""
        SELECT transaction_type, SUM(txn_count) AS total_txns
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY transaction_type;
    """, engine)

    fig2 = px.pie(
        df_cat,
        names="transaction_type",
        values="total_txns",
        title="Payment Category Share"
    )

    st.plotly_chart(fig2, use_container_width=True)

# =================================================
# TAB 2: USERS
# =================================================
with tab2:
    st.subheader("User Engagement by State")

    df_user = pd.read_sql(f"""
        SELECT state,
               SUM(registered_users) AS users,
               SUM(app_opens) AS app_opens
        FROM map_user
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        ORDER BY users DESC
        LIMIT 10;
    """, engine)

    fig3 = px.bar(
        df_user,
        x="state",
        y=["users", "app_opens"],
        barmode="group",
        title="Registered Users vs App Opens"
    )

    st.plotly_chart(fig3, use_container_width=True)

# =================================================
# TAB 3: TOP PERFORMERS
# =================================================
with tab3:
    st.subheader("Top Districts by Transactions")

    selected_state = st.selectbox(
        "Select State",
        sorted(pd.read_sql(
            "SELECT DISTINCT state FROM map_transaction",
            engine
        )["state"])
    )

    df_dist = pd.read_sql(f"""
        SELECT district, SUM({metric_column}) AS value
        FROM map_transaction
        WHERE state = '{selected_state}'
          AND year = {year}
          AND quarter = {quarter}
        GROUP BY district
        ORDER BY value DESC
        LIMIT 10;
    """, engine)

    fig4 = px.bar(
        df_dist,
        x="district",
        y="value",
        title=f"Top Districts in {selected_state} ({metric})"
    )

    fig4.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig4, use_container_width=True)

# =================================================
# TAB 4: MAPS
# =================================================
with tab4:
    st.subheader("🗺️ Geographical Analysis")

    # ---------------- STATE SELECTOR ----------------
#    map_state = st.selectbox(
#        "Select State (for District View)",
#        sorted(pd.read_sql(
#            "SELECT DISTINCT state FROM map_transaction ORDER BY state",
#            engine
#        )["state"])
#    )
#--------------------------------------------------------------------------

    map_state = st.selectbox(
    "Select State (District Map Available)",
    SUPPORTED_DISTRICT_STATES
)

    st.caption(
    "ℹ️ District-level maps are available only for selected states "
    "based on GeoJSON boundary availability."
)
    
    # ---------------- STATE-WISE INDIA MAP ----------------
    st.markdown("### State-wise Transaction Map (India)")

    df_state_map = pd.read_sql(f"""
        SELECT state, SUM({metric_column}) AS value
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state;
    """, engine)

    fig_state = px.choropleth(
        df_state_map,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="state",
        color="value",
        color_continuous_scale="Blues",
        title=f"{metric} by State ({year} Q{quarter})"
    )

    fig_state.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig_state, use_container_width=True)

    st.markdown("---")

    # ---------------- DISTRICT-LEVEL MAP ----------------
    st.markdown(f"### District-wise {metric} – {map_state}")

    district_geojson = load_district_geojson(map_state)

    if district_geojson is None:
        st.warning("District map not available for this state.")
    else:
        df_district = pd.read_sql(f"""
            SELECT district, SUM({metric_column}) AS value
            FROM map_transaction
            WHERE state = '{map_state}'
              AND year = {year}
              AND quarter = {quarter}
            GROUP BY district;
        """, engine)

        fig_dist = px.choropleth(
            df_district,
            geojson=district_geojson,
            featureidkey="properties.DISTRICT",
            locations="district",
            color="value",
            color_continuous_scale="Reds",
            title=f"{metric} by District – {map_state}"
        )

        fig_dist.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig_dist, use_container_width=True)

# =================================================
# TAB 5: INSURANCE
# =================================================
with tab5:
    st.subheader("🛡️ Insurance Transactions Analysis")

    # -------------------------------------------------
    # Load ALL insurance data first (safe approach)
    # -------------------------------------------------
    df_ins = pd.read_sql("""
        SELECT state,
               year,
               quarter,
               SUM(insurance_amount) AS total_amount,
               SUM(insurance_count) AS total_count
        FROM aggregated_insurance
        GROUP BY state, year, quarter;
    """, engine)

    # -------------------------------------------------
    # Apply global filters (year & quarter)
    # -------------------------------------------------
    df_filtered = df_ins[
        (df_ins["year"] == year) &
        (df_ins["quarter"] == quarter)
    ]

    # -------------------------------------------------
    # Fallback if no data exists for selected filters
    # -------------------------------------------------
    if df_filtered.empty:
        st.warning(
            f"No insurance data for {year} Q{quarter}. "
            "Showing latest available data instead."
        )

        latest_year = df_ins["year"].max()
        latest_quarter = (
            df_ins[df_ins["year"] == latest_year]["quarter"].max()
        )

        df_filtered = df_ins[
            (df_ins["year"] == latest_year) &
            (df_ins["quarter"] == latest_quarter)
        ]

    # -------------------------------------------------
    # Metric selector
    # -------------------------------------------------
    metric_ins = st.radio(
        "Select Insurance Metric",
        ["Insurance Amount", "Insurance Count"],
        horizontal=True
    )

    value_col = (
        "total_amount"
        if metric_ins == "Insurance Amount"
        else "total_count"
    )

    # -------------------------------------------------
    # Top States Bar Chart
    # -------------------------------------------------
    fig_bar = px.bar(
        df_filtered.sort_values(value_col, ascending=False).head(10),
        x="state",
        y=value_col,
        title=f"Top States by {metric_ins}"
    )

    st.plotly_chart(fig_bar, use_container_width=True)

    # -------------------------------------------------
    # State-wise Insurance Map
    # -------------------------------------------------
    fig_map = px.choropleth(
        df_filtered,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="state",
        color=value_col,
        color_continuous_scale="Greens",
        title=f"{metric_ins} by State"
    )

    fig_map.update_geos(
        fitbounds="locations",
        visible=False
    )

    st.plotly_chart(fig_map, use_container_width=True)
