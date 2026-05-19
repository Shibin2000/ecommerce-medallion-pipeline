import sys
import os
import subprocess
import logging

sys.path.insert(0, os.path.dirname(__file__))

from bronze import generate_bronze
from silver import run_silver
from gold import run_gold
from quality_checks import run_checks

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

DBT_DIR = os.path.join(os.path.dirname(__file__), '..', 'dbt_project')


def run_dbt(command):
    result = subprocess.run(
        ['dbt', command, '--profiles-dir', '.'],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt {command} failed")


def main():
    logging.info("starting pipeline")
    generate_bronze()
    run_silver()

    # gold.py builds the raw aggregation tables
    # dbt then re-models them as proper mart tables with tests
    run_gold()
    run_dbt('run')
    run_dbt('test')

    ok = run_checks()
    if not ok:
        logging.error("quality checks failed")
        sys.exit(1)

    logging.info("pipeline done")


if __name__ == "__main__":
    main()
