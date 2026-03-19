import os
import json
import pandas as pd
from sqlalchemy import text
from etl.db_utils import get_engine
import logging

BASE_PATH = r"D:\pulse\data\top\insurance\country\india\state"

def extract_top_insurance():
    logging.info("Starting top insurance ETL")
    engine = get_engine()
    rows = []

    for state in os.listdir(BASE_PATH):
        for year in os.listdir(os.path.join(BASE_PATH, state)):
            for file in os.listdir(os.path.join(BASE_PATH, state, year)):
                quarter = int(file.replace(".json", ""))
                with open(os.path.join(BASE_PATH, state, year, file), "r", encoding="utf-8") as f:
                    data = json.load(f)

                for item in data["data"].get("pincodes", []):
                    metric = item["metric"]
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "insurance_count": metric["count"],
                        "insurance_amount": metric["amount"]
                    })

    if not rows:
        logging.warning("No top insurance data found")
        return

    df = pd.DataFrame(rows)

    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS top_insurance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            state VARCHAR(100),
            year INT,
            quarter INT,
            insurance_count BIGINT,
            insurance_amount DOUBLE
        )
        """))
        conn.commit()

    df.to_sql("top_insurance", engine, if_exists="append", index=False)
    logging.info("Top insurance ETL completed")
