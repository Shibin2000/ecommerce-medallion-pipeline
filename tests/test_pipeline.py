import pytest
import duckdb
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pipeline'))

from bronze import generate_bronze
from silver import run_silver
from gold import run_gold

DB = "test_pipeline.db"


@pytest.fixture(scope="module", autouse=True)
def pipeline():
    generate_bronze(db_path=DB, n=1000)
    run_silver(db_path=DB)
    run_gold(db_path=DB)
    yield
    if os.path.exists(DB):
        os.remove(DB)


def test_bronze_row_count():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM bronze_orders").fetchone()[0]
    assert n == 1000


def test_silver_no_cancelled():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM silver_orders WHERE status = 'cancelled'").fetchone()[0]
    assert n == 0


def test_silver_no_bad_prices():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM silver_orders WHERE unit_price <= 0").fetchone()[0]
    assert n == 0


def test_silver_no_null_customers():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM silver_orders WHERE customer_id IS NULL").fetchone()[0]
    assert n == 0


def test_silver_positive_totals():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM silver_orders WHERE total_amount <= 0").fetchone()[0]
    assert n == 0


def test_gold_categories():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM gold_category_metrics").fetchone()[0]
    assert n == 9


def test_gold_segments():
    with duckdb.connect(DB) as conn:
        segs = conn.execute(
            "SELECT DISTINCT customer_segment FROM gold_customer_segments"
        ).fetchdf()['customer_segment'].tolist()
    assert set(segs).issubset({'VIP', 'Premium', 'Regular', 'New'})


def test_gold_cities():
    with duckdb.connect(DB) as conn:
        n = conn.execute("SELECT COUNT(*) FROM gold_city_metrics").fetchone()[0]
    assert n == 6


def test_gold_revenue():
    with duckdb.connect(DB) as conn:
        total = conn.execute("SELECT SUM(total_revenue) FROM gold_category_metrics").fetchone()[0]
    assert total > 0


