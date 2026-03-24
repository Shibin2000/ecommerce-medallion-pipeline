import duckdb


def run_checks(db_path="ecommerce_lakehouse.db"):

    failures = []

    with duckdb.connect(db_path) as conn:

        def check(name, query, expected=0):
            val = conn.execute(query).fetchone()[0]
            if val != expected:
                failures.append(f"FAIL - {name}: got {val}, expected {expected}")
            else:
                print(f"  pass - {name}")

        print("checking silver_orders...")
        check("no negative prices", "SELECT COUNT(*) FROM silver_orders WHERE unit_price < 0")
        check("no zero quantities", "SELECT COUNT(*) FROM silver_orders WHERE quantity = 0")
        check("no null customer ids", "SELECT COUNT(*) FROM silver_orders WHERE customer_id IS NULL")
        check("no cancelled orders", "SELECT COUNT(*) FROM silver_orders WHERE status = 'cancelled'")
        check("no negative totals", "SELECT COUNT(*) FROM silver_orders WHERE total_amount < 0")

        print("checking gold tables...")

        # derive expected counts from the data itself so checks don't break if source data changes
        n_categories = conn.execute("SELECT COUNT(DISTINCT category) FROM silver_orders").fetchone()[0]
        n_cities     = conn.execute("SELECT COUNT(DISTINCT city) FROM silver_orders").fetchone()[0]
        check("category count matches source", "SELECT COUNT(*) FROM gold_category_metrics", expected=n_categories)
        check("4 segments exist", "SELECT COUNT(DISTINCT customer_segment) FROM gold_customer_segments", expected=4)
        check("city count matches source", "SELECT COUNT(*) FROM gold_city_metrics", expected=n_cities)

        total_rev = conn.execute("SELECT SUM(total_revenue) FROM gold_category_metrics").fetchone()[0]
        print(f"  total revenue across all categories: ${total_rev:,.2f}")

    if failures:
        print(f"\n{len(failures)} check(s) failed:")
        for f in failures:
            print(f"  {f}")
        return False

    print("\nall quality checks passed!")
    return True


if __name__ == "__main__":
    run_checks()
