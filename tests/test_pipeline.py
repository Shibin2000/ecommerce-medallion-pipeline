import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))

from snowflake_conn import get_connection
from bronze import generate_bronze
from silver import run_silver
from gold import run_gold

# using a separate schema so tests don't touch the real tables
# set SNOWFLAKE_SCHEMA=ECOMMERCE_TEST in your .env before running pytest


@pytest.fixture(scope="module", autouse=True)
def pipeline():
    generate_bronze(n=1000, incremental=False)
    run_silver()
    run_gold()
    yield
    conn = get_connection()
    cur = conn.cursor()
    for t in ['BRONZE_ORDERS', 'SILVER_ORDERS', 'GOLD_DAILY_SALES',
              'GOLD_CATEGORY_METRICS', 'GOLD_CUSTOMER_SEGMENTS', 'GOLD_CITY_METRICS']:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    cur.close()
    conn.close()


def _count(query):
    conn = get_connection()
    cur = conn.cursor()
    val = cur.execute(query).fetchone()[0]
    cur.close()
    conn.close()
    return val


def test_bronze_row_count():
    assert _count("SELECT COUNT(*) FROM BRONZE_ORDERS") == 1000

def test_silver_no_cancelled():
    assert _count("SELECT COUNT(*) FROM SILVER_ORDERS WHERE STATUS = 'cancelled'") == 0

def test_silver_no_bad_prices():
    assert _count("SELECT COUNT(*) FROM SILVER_ORDERS WHERE UNIT_PRICE <= 0") == 0

def test_silver_no_null_customers():
    assert _count("SELECT COUNT(*) FROM SILVER_ORDERS WHERE CUSTOMER_ID IS NULL") == 0

def test_silver_positive_totals():
    assert _count("SELECT COUNT(*) FROM SILVER_ORDERS WHERE TOTAL_AMOUNT <= 0") == 0

def test_gold_categories():
    assert _count("SELECT COUNT(*) FROM GOLD_CATEGORY_METRICS") == 9

def test_gold_segments():
    conn = get_connection()
    cur = conn.cursor()
    segs = {r[0] for r in cur.execute("SELECT DISTINCT CUSTOMER_SEGMENT FROM GOLD_CUSTOMER_SEGMENTS").fetchall()}
    cur.close()
    conn.close()
    assert segs.issubset({'VIP', 'Premium', 'Regular', 'New'})

def test_gold_cities():
    assert _count("SELECT COUNT(*) FROM GOLD_CITY_METRICS") == 6

def test_gold_revenue():
    assert _count("SELECT SUM(TOTAL_REVENUE) FROM GOLD_CATEGORY_METRICS") > 0
