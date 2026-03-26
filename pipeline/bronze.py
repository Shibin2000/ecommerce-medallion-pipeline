import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
import random


# TODO: maybe add an option to generate realistic seasonal trends later
# right now order dates are fully random which makes the charts kinda flat
def generate_bronze(db_path="ecommerce_lakehouse.db", n=100_000, incremental=True):
    np.random.seed(42)
    random.seed(42)

    categories = ['Electronics', 'Fashion', 'Home & Garden', 'Sports',
                  'Books', 'Beauty', 'Toys', 'Food & Beverage', 'Automotive']
    cities = ['Houston', 'Dallas', 'Austin', 'New York', 'Chicago', 'Los Angeles']
    payments = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']
    statuses = ['delivered', 'shipped', 'cancelled', 'returned', 'pending']
    return_reasons = ['Defective', 'Wrong item', 'Changed mind']

    start_date = datetime(2023, 1, 1)
    order_dates = [start_date + timedelta(days=random.randint(0, 730)) for _ in range(n)]

    # some orders wont have a return reason - thats fine, only returned ones do
    return_reason_col = [random.choice(return_reasons + [None, None, None]) for _ in range(n)]

    df = pd.DataFrame({
        'order_id': [f'ORD-{i:07d}' for i in range(n)],
        'customer_id': [f'CUST-{random.randint(1, 20000):06d}' for _ in range(n)],
        'product_id': [f'PROD-{random.randint(1, 5000):05d}' for _ in range(n)],
        'category': [random.choice(categories) for _ in range(n)],
        'order_date': order_dates,
        'quantity': np.random.randint(1, 6, n),
        'unit_price': np.random.uniform(5, 500, n).round(2),
        'discount_pct': np.random.randint(0, 20, n),
        'shipping_cost': np.random.uniform(2, 20, n).round(2),
        'customer_rating': np.random.randint(1, 6, n),
        'payment_method': [random.choice(payments) for _ in range(n)],
        'city': [random.choice(cities) for _ in range(n)],
        'status': [random.choice(statuses) for _ in range(n)],
        'return_reason': return_reason_col,
    })

    # duckdb doesnt like plain python str columns
    # tried polars first but arrow conversion errors, pandas StringDtype works fine, need to cast explicitly
    # spent like an hour debugging this before i figured it out
    str_cols = ['order_id', 'customer_id', 'product_id', 'category',
                'payment_method', 'city', 'status', 'return_reason']
    for col in str_cols:
        df[col] = df[col].astype(pd.StringDtype())

    with duckdb.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_orders AS
            SELECT * FROM df WHERE 1=0
        """)

        if incremental:
            result = conn.execute("SELECT MAX(order_date) FROM bronze_orders").fetchone()
            last_loaded = result[0] if result else None

            if last_loaded:
                df = df[df['order_date'] > pd.Timestamp(last_loaded)]
                print(f"incremental mode - last loaded: {last_loaded}, new rows: {len(df)}")
            else:
                print("first load, loading everything")

            conn.register("df", df)
            conn.execute("INSERT INTO bronze_orders SELECT * FROM df")
            conn.unregister("df")
        else:
            conn.execute("DROP TABLE IF EXISTS bronze_orders")
            conn.register("df", df)
            conn.execute("CREATE TABLE bronze_orders AS SELECT * FROM df")
            conn.unregister("df")

    print(f"bronze done - {len(df)} rows loaded to {db_path}")
    return len(df)


if __name__ == "__main__":
    generate_bronze()


