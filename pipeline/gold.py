from snowflake_conn import get_connection


def run_gold():

    conn = get_connection()
    cur = conn.cursor()

    # daily sales rollup
    cur.execute("DROP TABLE IF EXISTS GOLD_DAILY_SALES")
    cur.execute("""
        CREATE TABLE GOLD_DAILY_SALES AS
        SELECT
            ORDER_DATE::date as SALE_DATE,
            ORDER_YEAR,
            ORDER_MONTH,
            ORDER_QUARTER,
            COUNT(*) as TOTAL_ORDERS,
            COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
            SUM(QUANTITY) as TOTAL_ITEMS_SOLD,
            ROUND(SUM(GROSS_AMOUNT), 2) as GROSS_REVENUE,
            ROUND(SUM(DISCOUNT_AMOUNT), 2) as TOTAL_DISCOUNTS,
            ROUND(SUM(NET_AMOUNT), 2) as NET_REVENUE,
            ROUND(SUM(TOTAL_AMOUNT), 2) as TOTAL_REVENUE,
            ROUND(AVG(TOTAL_AMOUNT), 2) as AVG_ORDER_VALUE,
            SUM(CASE WHEN IS_RETURNED THEN 1 ELSE 0 END) as RETURNS
        FROM SILVER_ORDERS
        GROUP BY ORDER_DATE::date, ORDER_YEAR, ORDER_MONTH, ORDER_QUARTER
        ORDER BY SALE_DATE
    """)

    cur.execute("DROP TABLE IF EXISTS GOLD_CATEGORY_METRICS")
    cur.execute("""
        CREATE TABLE GOLD_CATEGORY_METRICS AS
        SELECT
            CATEGORY,
            COUNT(*) as TOTAL_ORDERS,
            ROUND(SUM(TOTAL_AMOUNT), 2) as TOTAL_REVENUE,
            ROUND(AVG(TOTAL_AMOUNT), 2) as AVG_ORDER_VALUE,
            ROUND(AVG(CUSTOMER_RATING), 2) as AVG_RATING,
            ROUND(AVG(DISCOUNT_PCT), 2) as AVG_DISCOUNT_PCT,
            ROUND(
                SUM(CASE WHEN IS_RETURNED THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2) as RETURN_RATE_PCT
        FROM SILVER_ORDERS
        GROUP BY CATEGORY
        ORDER BY TOTAL_REVENUE DESC
    """)

    # note: snowflake doesn't have date_diff() - it's DATEDIFF(unit, start, end)
    # the arg order is also flipped from duckdb, caught that bug on first run
    cur.execute("DROP TABLE IF EXISTS GOLD_CUSTOMER_SEGMENTS")
    cur.execute("""
        CREATE TABLE GOLD_CUSTOMER_SEGMENTS AS
        WITH rfm AS (
            SELECT
                CUSTOMER_ID,
                COUNT(*) as FREQUENCY,
                ROUND(SUM(TOTAL_AMOUNT), 2) as MONETARY,
                DATEDIFF('day', MAX(ORDER_DATE::date), CURRENT_DATE) as RECENCY_DAYS,
                MIN(ORDER_DATE::date) as FIRST_ORDER,
                MAX(ORDER_DATE::date) as LAST_ORDER
            FROM SILVER_ORDERS
            GROUP BY CUSTOMER_ID
        )
        SELECT *,
            CASE
                WHEN MONETARY > 2000 AND FREQUENCY >= 5 THEN 'VIP'
                WHEN MONETARY > 1000 AND FREQUENCY >= 3 THEN 'Premium'
                WHEN MONETARY > 500 THEN 'Regular'
                ELSE 'New'
            END AS CUSTOMER_SEGMENT
        FROM rfm
        ORDER BY MONETARY DESC
    """)

    cur.execute("DROP TABLE IF EXISTS GOLD_CITY_METRICS")
    cur.execute("""
        CREATE TABLE GOLD_CITY_METRICS AS
        SELECT
            CITY,
            COUNT(*) as TOTAL_ORDERS,
            ROUND(SUM(TOTAL_AMOUNT), 2) as TOTAL_REVENUE,
            ROUND(AVG(TOTAL_AMOUNT), 2) as AVG_ORDER_VALUE,
            COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS
        FROM SILVER_ORDERS
        GROUP BY CITY
        ORDER BY TOTAL_REVENUE DESC
    """)

    # sanity check - had a bug where gold_daily_sales came back empty, keeping this
    for table in ['GOLD_DAILY_SALES', 'GOLD_CATEGORY_METRICS',
                  'GOLD_CUSTOMER_SEGMENTS', 'GOLD_CITY_METRICS']:
        count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_gold()
