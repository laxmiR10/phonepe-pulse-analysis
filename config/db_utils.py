from sqlalchemy import create_engine
import os

def get_engine():
    # Try Streamlit secrets first
    try:
        import streamlit as st
        db_user = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        db_host = st.secrets["DB_HOST"]
        db_name = st.secrets["DB_NAME"]
    except Exception:
        # Fallback to environment variables
        db_user = "root"
        db_password = os.getenv("PHONEPE_DB_PASSWORD")
        db_host = "localhost"
        db_name = "phonepe_pulse"

    if not db_password:
        raise RuntimeError("Database password not found")

    return create_engine(
        f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}",
        pool_pre_ping=True
    )
