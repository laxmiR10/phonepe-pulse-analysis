import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

st.set_page_config(page_title="PhonePe Pulse Dashboard", layout="wide")

engine = create_engine(
    "mysql+mysqlconnector://root:Roqmes-tavha0-tirnan@localhost/phonepe_pulse"
)

st.title("📊 PhonePe Pulse Data Analysis")

# ---------------- GLOBAL FILTERS ----------------
st.sidebar.header("Filters")

year = st.sidebar.selectbox(
    "Select Year",
    sorted(pd.read_sql(
        "SELECT DISTINCT year FROM aggregated_transaction ORDER BY year",
        engine
    )["year"])
)

quarter = st.sidebar.selectbox(
    "Select Quarter",
    [1, 2, 3, 4]
)


#tab1, tab2, tab3 = st.tabs(["Transactions", "Users", "Top Performers"])
tab1, tab2, tab3, tab4 = st.tabs(
    ["Transactions", "Users", "Top Performers", "🗺️ Maps"]
)

# ---------------- TRANSACTIONS ----------------
with tab1:
    st.subheader("Top States by Transaction Amount")

    df = pd.read_sql("""
        SELECT state, SUM(txn_amount) AS total_amount
        FROM aggregated_transaction
        GROUP BY state
        ORDER BY total_amount DESC
        LIMIT 10;
    """, engine)

    fig = px.bar(df, x="state", y="total_amount", title="Top 10 States")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Payment Category Distribution")
    df2 = pd.read_sql("""
        SELECT transaction_type, SUM(txn_count) AS total_txns
        FROM aggregated_transaction
        GROUP BY transaction_type;
    """, engine)

    fig2 = px.pie(df2, names="transaction_type", values="total_txns")
    st.plotly_chart(fig2, use_container_width=True)

# ---------------- USERS ----------------
with tab2:
    st.subheader("User Engagement by State")

    df3 = pd.read_sql("""
        SELECT state,
               SUM(registered_users) AS users,
               SUM(app_opens) AS app_opens
        FROM map_user
        GROUP BY state
        ORDER BY users DESC
        LIMIT 10;
    """, engine)

    fig3 = px.bar(
        df3,
        x="state",
        y=["users", "app_opens"],
        barmode="group",
        title="Registered Users vs App Opens"
    )
    st.plotly_chart(fig3, use_container_width=True)

# ---------------- TOP PERFORMERS ----------------
with tab3:
    st.subheader("Top Districts by Transaction Amount")

    state_selected = st.selectbox(
        "Select State",
        sorted(pd.read_sql("SELECT DISTINCT state FROM map_transaction", engine)["state"])
    )

    df4 = pd.read_sql(f"""
        SELECT district, SUM(txn_amount) AS total_amount
        FROM map_transaction
        WHERE state = '{state_selected}'
        GROUP BY district
        ORDER BY total_amount DESC
        LIMIT 10;
    """, engine)

    fig4 = px.bar(df4, x="district", y="total_amount")
    st.plotly_chart(fig4, use_container_width=True)

# ---------------- Maps  ----------------

with tab4:
    st.subheader("State-wise Transaction Amount (India Map)")

    df_map = pd.read_sql("""
        SELECT state, SUM(txn_amount) AS total_amount
        FROM aggregated_transaction
        GROUP BY state;
    """, engine)

    fig = px.choropleth(
        df_map,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="state",
        color="total_amount",
        color_continuous_scale="Blues",
        title="Total Transaction Amount by State",
    )

    fig.update_geos(
        fitbounds="locations",
        visible=False
    )

    metric = st.radio(
    "Select Metric",
    ["Transaction Amount", "Transaction Count"]
)

    
    st.plotly_chart(fig, use_container_width=True)
