import os
import json
import logging
import pandas as pd
from .db_utils import get_engine

# ---------------- CONFIG ----------------
BASE_PATH = r"D:\pulse\data\aggregated\user\country\india\state"

logger = logging.getLogger(__name__)

# ---------------- ETL FUNCTION ----------------
def extract_aggregated_user():
    logger.info("Starting aggregated user ETL")

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

                aggregated = content.get("data", {}).get("aggregated")
                if not aggregated:
                    continue

                for device in aggregated.get("usersByDevice", []):
                    rows.append({
                        "state": state.replace("-", " ").title(),
                        "year": int(year),
                        "quarter": quarter,
                        "brand": device.get("brand"),
                        "user_count": device.get("count", 0),
                        "percentage": device.get("percentage", 0.0)
                    })

    if not rows:
        logger.warning("No aggregated user data found")
        return

    df = pd.DataFrame(rows)
    engine = get_engine()

    df.to_sql(
        "aggregated_user",
        con=engine,
        if_exists="append",
        index=False
    )

    logger.info("Aggregated user ETL completed successfully")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    extract_aggregated_user()
