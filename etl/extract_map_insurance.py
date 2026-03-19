import os
import json
import pandas as pd
from sqlalchemy import text
from etl.db_utils import get_engine
import logging

BASE_PATH = r"D:\pulse\data\map\insurance\hover\country\india\state"

def extract_map_insurance():
    logging.info("Starting map insurance ETL")
    engine = get_engine()
    rows = []

    for state in os.listdir(BASE_PATH):
        state_path = os.path.join(BASE_PATH, state)
        if not os.path.isdir(state_path):
            continue

        for year in os.listdir(state_path):
            for file in os.listdir(os.path.join(state_path, year)):
                if not file.endswith(".json"):
                    continue

                quarter = int(file.replace(".json", ""))
                with open(os.path.join(state_path, year, file), "r", encoding="utf-8") as f:
                    data = json.load(f)

                hover = data.get("data", {}).get("hoverDataList", [])
                for d in hover:
                    metric = d["metric"][0]
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "district": d["name"],
                        "insurance_count": metric["count"],
                        "insurance_amount": metric["amount"]
                    })

    if not rows:
        logging.warning("No map insurance data found")
        return

    df = pd.DataFrame(rows)

    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS map_insurance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            state VARCHAR(100),
            year INT,
            quarter INT,
            district VARCHAR(100),
            insurance_count BIGINT,
            insurance_amount DOUBLE
        )
        """))
        conn.commit()

    df.to_sql("map_insurance", engine, if_exists="append", index=False)
    logging.info("Map insurance ETL completed")
