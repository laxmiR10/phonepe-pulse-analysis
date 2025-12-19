import logging
from .extract_aggregated_transaction import extract_aggregated_transaction
from .extract_aggregated_user import extract_aggregated_user
from .extract_map_transaction import extract_map_transaction
from .extract_map_user import extract_map_user
from .extract_top_tables import extract_top_tables
from .extract_insurance import extract_insurance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def main():
    extract_aggregated_transaction()
    extract_aggregated_user()
    extract_map_transaction()
    extract_map_user()
    extract_top_tables()
    extract_insurance()

if __name__ == "__main__":
    main()
