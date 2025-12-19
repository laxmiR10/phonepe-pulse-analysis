import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from sqlalchemy import create_engine

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="PhonePe Pulse Dashboard",
    layout="wide"
)

# ---------------- DB CONNECTION ----------------
engine = create_engine(
    "mysql+mysqlconnector://root:YOUR_PASSWORD@localhost/phonepe_pulse"
)

# ---------------- CONSTANTS ----------------
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

SUPPORTED_DISTRICT_STATES = sorted(DISTRICT_GEOJSON_MAP.keys())

# ---------------- HELPERS ----------------
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

quarter = st.sidebar.selectbox("Select Quarter", [1, 2, 3, 4])

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
    df_txn = pd.read_sql(f"""
        SELECT state, SUM({metric_column}) AS value
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state
        ORDER BY value DESC
        LIMIT 10;
    """, engine)

    st.plotly_chart(
        px.bar(df_txn, x="state", y="value",
               title=f"Top States by {metric} ({year} Q{quarter})"),
        use_container_width=True
    )

# =================================================
# TAB 2: USERS
# =================================================
with tab2:
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

    st.plotly_chart(
        px.bar(df_user, x="state",
               y=["users", "app_opens"],
               barmode="group",
               title="Registered Users vs App Opens"),
        use_container_width=True
    )

# =================================================
# TAB 3: TOP PERFORMERS
# =================================================
with tab3:
    state_tp = st.selectbox(
        "Select State",
        sorted(pd.read_sql(
            "SELECT DISTINCT state FROM map_transaction ORDER BY state",
            engine
        )["state"])
    )

    df_dist = pd.read_sql(f"""
        SELECT district, SUM({metric_column}) AS value
        FROM map_transaction
        WHERE state = '{state_tp}'
          AND year = {year}
          AND quarter = {quarter}
        GROUP BY district
        ORDER BY value DESC
        LIMIT 10;
    """, engine)

    st.plotly_chart(
        px.bar(df_dist, x="district", y="value",
               title=f"Top Districts in {state_tp}"),
        use_container_width=True
    )

# =================================================
# TAB 4: MAPS
# =================================================
with tab4:
    map_state = st.selectbox(
        "Select State (District Map Available)",
        SUPPORTED_DISTRICT_STATES
    )

    # State-level map
    df_state_map = pd.read_sql(f"""
        SELECT state, SUM({metric_column}) AS value
        FROM aggregated_transaction
        WHERE year = {year} AND quarter = {quarter}
        GROUP BY state;
    """, engine)

    st.plotly_chart(
        px.choropleth(
            df_state_map,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="state",
            color="value",
            color_continuous_scale="Blues",
            title=f"{metric} by State"
        ).update_geos(fitbounds="locations", visible=False),
        use_container_width=True
    )

    # District-level map
    geojson = load_district_geojson(map_state)

    df_district = pd.read_sql(f"""
        SELECT district, SUM({metric_column}) AS value
        FROM map_transaction
        WHERE state = '{map_state}'
          AND year = {year}
          AND quarter = {quarter}
        GROUP BY district;
    """, engine)

    st.plotly_chart(
        px.choropleth(
            df_district,
            geojson=geojson,
            featureidkey="properties.DISTRICT",
            locations="district",
            color="value",
            color_continuous_scale="Reds",
            title=f"{metric} by District – {map_state}"
        ).update_geos(fitbounds="locations", visible=False),
        use_container_width=True
    )

# =================================================
# TAB 5: INSURANCE
# =================================================
with tab5:
    df_ins = pd.read_sql("""
        SELECT state, year, quarter,
               SUM(insurance_amount) AS total_amount,
               SUM(insurance_count) AS total_count
        FROM aggregated_insurance
        GROUP BY state, year, quarter;
    """, engine)

    df_filtered = df_ins[
        (df_ins["year"] == year) &
        (df_ins["quarter"] == quarter)
    ]

    if df_filtered.empty:
        latest_year = df_ins["year"].max()
        latest_quarter = df_ins[df_ins["year"] == latest_year]["quarter"].max()
        df_filtered = df_ins[
            (df_ins["year"] == latest_year) &
            (df_ins["quarter"] == latest_quarter)
        ]
        st.warning("Showing latest available insurance data.")

    metric_ins = st.radio(
        "Insurance Metric",
        ["Insurance Amount", "Insurance Count"],
        horizontal=True
    )

    col = "total_amount" if metric_ins == "Insurance Amount" else "total_count"

    st.plotly_chart(
        px.bar(df_filtered.sort_values(col, ascending=False).head(10),
               x="state", y=col,
               title=f"Top States by {metric_ins}"),
        use_container_width=True
    )

    st.plotly_chart(
        px.choropleth(
            df_filtered,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="state",
            color=col,
            color_continuous_scale="Greens",
            title=f"{metric_ins} by State"
        ).update_geos(fitbounds="locations", visible=False),
        use_container_width=True
    )
