import sys
import os
import logging

sys.path.insert(0, os.path.dirname(__file__))

from bronze import generate_bronze
from silver import run_silver
from gold import run_gold
from quality_checks import run_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

DB = os.environ.get("DB_PATH", "ecommerce_lakehouse.db")


def main():
    logging.info("starting pipeline")

    generate_bronze(db_path=DB)
    run_silver(db_path=DB)
    run_gold(db_path=DB)

    ok = run_checks(db_path=DB)
    if not ok:
        logging.error("quality checks failed")
        sys.exit(1)

    logging.info("pipeline done")


if __name__ == "__main__":
    main()
