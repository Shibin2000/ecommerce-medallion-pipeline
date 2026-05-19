import sys
import os
import logging

sys.path.insert(0, os.path.dirname(__file__))

from bronze import generate_bronze
from silver import run_silver
from gold import run_gold
from quality_checks import run_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def main():
    logging.info("starting pipeline")
    generate_bronze()
    run_silver()
    run_gold()
    ok = run_checks()
    if not ok:
        logging.error("quality checks failed")
        sys.exit(1)
    logging.info("pipeline done")


if __name__ == "__main__":
    main()
