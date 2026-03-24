import duckdb


def run_gold(db_path="ecommerce_lakehouse.db"):

    with duckdb.connect(db_path) as conn:

        # daily sales rollup
        conn.execute("DROP TABLE IF EXISTS gold_daily_sales")
        conn.execute("""
            CREATE TABLE gold_daily_sales AS
            SELECT
                order_date::date as sale_date,
                order_year,
                order_month,
                order_quarter,
                count(*) as total_orders,
                count(distinct customer_id) as unique_customers,
                sum(quantity) as total_items_sold,
                round(sum(gross_amount), 2) as gross_revenue,
                round(sum(discount_amount), 2) as total_discounts,
                round(sum(net_amount), 2) as net_revenue,
                round(sum(total_amount), 2) as total_revenue,
                round(avg(total_amount), 2) as avg_order_value,
                sum(case when is_returned then 1 else 0 end) as returns
            FROM silver_orders
            GROUP BY order_date::date, order_year, order_month, order_quarter
            ORDER BY sale_date
        """)

        # category level metrics
        conn.execute("DROP TABLE IF EXISTS gold_category_metrics")
        conn.execute("""
            CREATE TABLE gold_category_metrics AS
            SELECT
                category,
                count(*) as total_orders,
                round(sum(total_amount), 2) as total_revenue,
                round(avg(total_amount), 2) as avg_order_value,
                round(avg(customer_rating), 2) as avg_rating,
                round(avg(discount_pct), 2) as avg_discount_pct,
                round(
                    sum(case when is_returned then 1 else 0 end) * 100.0 / count(*),
                2) as return_rate_pct
            FROM silver_orders
            GROUP BY category
            ORDER BY total_revenue DESC
        """)

        # RFM customer segmentation
        # thresholds from percentile analysis:
        # conn.execute("SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY monetary) FROM rfm").fetchone()
        # roughly maps to 25th/50th/75th spend percentile in this dataset
        # thresholds based on looking at the spend distribution manually
        # $500 / $1000 / $2000 roughly splits the customers into 4 groups
        conn.execute("DROP TABLE IF EXISTS gold_customer_segments")
        conn.execute("""
            CREATE TABLE gold_customer_segments AS
            WITH rfm AS (
                SELECT
                    customer_id,
                    count(*) as frequency,
                    round(sum(total_amount), 2) as monetary,
                    date_diff('day', max(order_date::date), current_date) as recency_days,
                    min(order_date::date) as first_order,
                    max(order_date::date) as last_order
                FROM silver_orders
                GROUP BY customer_id
            )
            SELECT *,
                CASE
                    WHEN monetary > 2000 AND frequency >= 5 THEN 'VIP'
                    WHEN monetary > 1000 AND frequency >= 3 THEN 'Premium'
                    WHEN monetary > 500 THEN 'Regular'
                    ELSE 'New'
                END AS customer_segment
            FROM rfm
            ORDER BY monetary DESC
        """)

        conn.execute("DROP TABLE IF EXISTS gold_city_metrics")
        conn.execute("""
            CREATE TABLE gold_city_metrics AS
            SELECT
                city,
                count(*) as total_orders,
                round(sum(total_amount), 2) as total_revenue,
                round(avg(total_amount), 2) as avg_order_value,
                count(distinct customer_id) as unique_customers
            FROM silver_orders
            GROUP BY city
            ORDER BY total_revenue DESC
        """)

        # sanity check - at some point gold_daily_sales had 0 rows bc of a date cast bug, keeping this
        # quick sanity check on row counts
        for table in ['gold_daily_sales', 'gold_category_metrics',
                      'gold_customer_segments', 'gold_city_metrics']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count} rows")


if __name__ == "__main__":
    run_gold()

