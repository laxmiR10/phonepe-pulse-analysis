from sqlalchemy import create_engine
from config.db_config import (
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_NAME
)

def get_engine():
    """
    Create and return a SQLAlchemy engine for MySQL connection.
    """
    return create_engine(
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}",
        pool_pre_ping=True
    )
