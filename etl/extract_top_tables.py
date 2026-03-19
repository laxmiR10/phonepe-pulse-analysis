import os
import json
import logging
import pandas as pd
from .db_utils import get_engine

logger = logging.getLogger(__name__)

# -------------------------------------------------
# TOP TRANSACTION ETL
# -------------------------------------------------
TOP_TXN_PATH = r"D:\pulse\data\top\transaction\country\india\state"

def extract_top_transaction():
    logger.info("Starting top transaction ETL")
    rows = []

    for state in os.listdir(TOP_TXN_PATH):
        state_path = os.path.join(TOP_TXN_PATH, state)
        if not os.path.isdir(state_path):
            continue

        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)

            for file in os.listdir(year_path):
                if not file.endswith(".json"):
                    continue

                quarter = int(file.replace(".json", ""))

                with open(os.path.join(year_path, file), "r", encoding="utf-8") as f:
                    content = json.load(f)

                data_block = content.get("data", {})
                if not data_block:
                    continue

                for category in ["states", "districts", "pincodes"]:
                    items = data_block.get(category, [])

                    if not isinstance(items, list):
                        continue

                    for item in items:
                        metric = item.get("metric", {})
                        if isinstance(metric, list):
                            metric = metric[0] if metric else {}

                        rows.append({
                            "state": state.replace("-", " ").title(),
                            "year": int(year),
                            "quarter": quarter,
                            "entity_type": category[:-1],
                            "entity_name": item.get("entityName"),
                            "txn_count": metric.get("count", 0),
                            "txn_amount": metric.get("amount", 0.0)
                        })

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql(
            "top_transaction",
            get_engine(),
            if_exists="append",
            index=False
        )
        logger.info("Top transaction ETL completed")
    else:
        logger.warning("No top transaction data found")

# -------------------------------------------------
# TOP USER ETL
# -------------------------------------------------
TOP_USER_PATH = r"D:\pulse\data\top\user\country\india\state"

def extract_top_user():
    logger.info("Starting top user ETL")
    rows = []

    for state in os.listdir(TOP_USER_PATH):
        state_path = os.path.join(TOP_USER_PATH, state)
        if not os.path.isdir(state_path):
            continue

        for year in os.listdir(state_path):
            for file in os.listdir(os.path.join(state_path, year)):
                if not file.endswith(".json"):
                    continue

                quarter = int(file.replace(".json", ""))

                with open(os.path.join(state_path, year, file), "r", encoding="utf-8") as f:
                    content = json.load(f)

                data_block = content.get("data", {})
                for category in ["districts", "pincodes"]:
                    for item in data_block.get(category, []):
                        rows.append({
                            "state": state.replace("-", " ").title(),
                            "year": int(year),
                            "quarter": quarter,
                            "entity_type": category[:-1],
                            "entity_name": item.get("name"),
                            "registered_users": item.get("registeredUsers", 0)
                        })

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql(
            "top_user",
            get_engine(),
            if_exists="append",
            index=False
        )
        logger.info("Top user ETL completed")
    else:
        logger.warning("No top user data found")


# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    extract_top_transaction()
    extract_top_user()
