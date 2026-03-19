import os
import json
import logging
import pandas as pd
from .db_utils import get_engine

# ---------------- CONFIG ----------------
BASE_PATH = r"D:\pulse\data\map\transaction\hover\country\india\state"

logger = logging.getLogger(__name__)

# ---------------- ETL FUNCTION ----------------
def extract_map_transaction():
    logger.info("Starting map transaction ETL")

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

                hover_list = content.get("data", {}).get("hoverDataList")
                if not hover_list:
                    continue

                for district in hover_list:
                    metric = district.get("metric", [])
                    metric = metric[0] if metric else {}

                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "district": district.get("name"),
                        "txn_count": metric.get("count", 0),
                        "txn_amount": metric.get("amount", 0.0)
                    })

    if not rows:
        logger.warning("No map transaction data found")
        return

    df = pd.DataFrame(rows)
    engine = get_engine()

    df.to_sql(
        "map_transaction",
        con=engine,
        if_exists="append",
        index=False
    )

    logger.info("Map transaction ETL completed successfully")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    extract_map_transaction()
