import logging
from .extract_aggregated_transaction import extract_aggregated_transaction
from .extract_aggregated_user import extract_aggregated_user
from .extract_map_transaction import extract_map_transaction
from .extract_map_user import extract_map_user
from .extract_top_tables import extract_top_transaction, extract_top_user
from .extract_insurance import extract_insurance
from .extract_map_insurance import extract_map_insurance
from .extract_top_insurance import extract_top_insurance


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def main():
    extract_aggregated_transaction()
    extract_aggregated_user()
    extract_map_transaction()
    extract_map_user()
    extract_top_transaction()
    extract_top_user()
    extract_insurance()
    extract_map_insurance()   # NEW
    extract_top_insurance()   # NEW

if __name__ == "__main__":
    main()
