from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import subprocess
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))  # took a while to figure this out

from bronze import generate_bronze
from silver import run_silver
from gold import run_gold
from quality_checks import run_checks

DB_PATH = os.environ.get("DB_PATH", "/opt/airflow/data/ecommerce_lakehouse.db")
DBT_DIR = os.path.join(os.path.dirname(__file__), '..', 'dbt_project')

default_args = {
    'owner': 'shibin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


def run_dbt(command):
    # using subprocess here because the DbtOperator needs a separate package
    # and for a local pipeline subprocess is simpler and works fine
    result = subprocess.run(
        ['dbt', command, '--profiles-dir', '.'],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"dbt {command} failed - see logs above")


with DAG(
    dag_id='ecommerce_medallion_pipeline',
    description='bronze to gold pipeline for ecommerce orders',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=['ecommerce', 'duckdb'],
) as dag:

    t_bronze = PythonOperator(
        task_id='bronze_load',
        python_callable=generate_bronze,
        op_kwargs={'db_path': DB_PATH},
    )

    t_silver = PythonOperator(
        task_id='silver_clean',
        python_callable=run_silver,
        op_kwargs={'db_path': DB_PATH},
    )

    t_gold = PythonOperator(
        task_id='gold_aggregate',
        python_callable=run_gold,
        op_kwargs={'db_path': DB_PATH},
    )

    t_dbt_run = PythonOperator(
        task_id='dbt_run',
        python_callable=run_dbt,
        op_kwargs={'command': 'run'},
    )

    t_dbt_test = PythonOperator(
        task_id='dbt_test',
        python_callable=run_dbt,
        op_kwargs={'command': 'test'},
    )

    t_checks = PythonOperator(
        task_id='quality_checks',
        python_callable=run_checks,
        op_kwargs={'db_path': DB_PATH},
    )

    t_bronze >> t_silver >> t_gold >> t_dbt_run >> t_dbt_test >> t_checks

