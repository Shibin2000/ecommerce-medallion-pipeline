from snowflake_conn import get_connection


def run_checks():

    failures = []
    conn = get_connection()
    cur = conn.cursor()

    def check(name, query, expected=0):
        val = cur.execute(query).fetchone()[0]
        if val != expected:
            failures.append(f"FAIL - {name}: got {val}, expected {expected}")
        else:
            print(f"  pass - {name}")

    print("checking silver_orders...")
    check("no negative prices",   "SELECT COUNT(*) FROM SILVER_ORDERS WHERE UNIT_PRICE < 0")
    check("no zero quantities",   "SELECT COUNT(*) FROM SILVER_ORDERS WHERE QUANTITY = 0")
    check("no null customer ids", "SELECT COUNT(*) FROM SILVER_ORDERS WHERE CUSTOMER_ID IS NULL")
    check("no cancelled orders",  "SELECT COUNT(*) FROM SILVER_ORDERS WHERE STATUS = 'cancelled'")
    check("no negative totals",   "SELECT COUNT(*) FROM SILVER_ORDERS WHERE TOTAL_AMOUNT < 0")

    print("checking gold tables...")
    n_categories = cur.execute("SELECT COUNT(DISTINCT CATEGORY) FROM SILVER_ORDERS").fetchone()[0]
    n_cities     = cur.execute("SELECT COUNT(DISTINCT CITY) FROM SILVER_ORDERS").fetchone()[0]
    check("category count matches source", "SELECT COUNT(*) FROM GOLD_CATEGORY_METRICS", expected=n_categories)
    check("4 segments exist", "SELECT COUNT(DISTINCT CUSTOMER_SEGMENT) FROM GOLD_CUSTOMER_SEGMENTS", expected=4)
    check("city count matches source", "SELECT COUNT(*) FROM GOLD_CITY_METRICS", expected=n_cities)

    total_rev = cur.execute("SELECT SUM(TOTAL_REVENUE) FROM GOLD_CATEGORY_METRICS").fetchone()[0]
    print(f"  total revenue across all categories: ${total_rev:,.2f}")

    cur.close()
    conn.close()

    if failures:
        print(f"\n{len(failures)} check(s) failed:")
        for f in failures:
            print(f"  {f}")
        return False

    print("\nall quality checks passed!")
    return True


if __name__ == "__main__":
    run_checks()
