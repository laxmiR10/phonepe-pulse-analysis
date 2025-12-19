import os
import json
import logging
import pandas as pd

from .db_utils import get_engine

# ---------------- CONFIG ----------------
BASE_PATH = r"D:\pulse\data\aggregated\transaction\country\india\state"

logger = logging.getLogger(__name__)

# ---------------- ETL FUNCTION ----------------
def extract_aggregated_transaction():
    logger.info("Starting aggregated transaction ETL")

    rows = []

    for state in os.listdir(BASE_PATH):
        state_path = os.path.join(BASE_PATH, state)
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
                for item in data_block.get("transactionData", []):
                    metric = item["paymentInstruments"][0]

                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "transaction_type": item["name"],
                        "txn_count": metric.get("count", 0),
                        "txn_amount": metric.get("amount", 0.0)
                    })

    if not rows:
        logger.warning("No aggregated transaction data found")
        return

    df = pd.DataFrame(rows)
    engine = get_engine()

    df.to_sql(
        "aggregated_transaction",
        engine,
        if_exists="append",
        index=False
    )

    logger.info("Aggregated transaction ETL completed successfully")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    extract_aggregated_transaction()
