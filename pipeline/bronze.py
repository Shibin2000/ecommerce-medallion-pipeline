import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from snowflake.connector.pandas_tools import write_pandas
from snowflake_conn import get_connection


# TODO: maybe add an option to generate realistic seasonal trends later
# right now order dates are fully random which makes the charts kinda flat
def generate_bronze(n=100_000, incremental=True):
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

    return_reason_col = [random.choice(return_reasons + [None, None, None]) for _ in range(n)]

    df = pd.DataFrame({
        'ORDER_ID': [f'ORD-{i:07d}' for i in range(n)],
        'CUSTOMER_ID': [f'CUST-{random.randint(1, 20000):06d}' for _ in range(n)],
        'PRODUCT_ID': [f'PROD-{random.randint(1, 5000):05d}' for _ in range(n)],
        'CATEGORY': [random.choice(categories) for _ in range(n)],
        'ORDER_DATE': order_dates,
        'QUANTITY': np.random.randint(1, 6, n),
        'UNIT_PRICE': np.random.uniform(5, 500, n).round(2),
        'DISCOUNT_PCT': np.random.randint(0, 20, n),
        'SHIPPING_COST': np.random.uniform(2, 20, n).round(2),
        'CUSTOMER_RATING': np.random.randint(1, 6, n),
        'PAYMENT_METHOD': [random.choice(payments) for _ in range(n)],
        'CITY': [random.choice(cities) for _ in range(n)],
        'STATUS': [random.choice(statuses) for _ in range(n)],
        'RETURN_REASON': return_reason_col,
    })

    # snowflake wants uppercase column names by default - burned me the first time
    # write_pandas matches df columns to table columns case-sensitively if you use quotes
    # just keeping everything uppercase is simpler

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS BRONZE_ORDERS (
            ORDER_ID        VARCHAR,
            CUSTOMER_ID     VARCHAR,
            PRODUCT_ID      VARCHAR,
            CATEGORY        VARCHAR,
            ORDER_DATE      DATE,
            QUANTITY        INTEGER,
            UNIT_PRICE      FLOAT,
            DISCOUNT_PCT    INTEGER,
            SHIPPING_COST   FLOAT,
            CUSTOMER_RATING INTEGER,
            PAYMENT_METHOD  VARCHAR,
            CITY            VARCHAR,
            STATUS          VARCHAR,
            RETURN_REASON   VARCHAR
        )
    """)

    if incremental:
        result = cur.execute("SELECT MAX(ORDER_DATE) FROM BRONZE_ORDERS").fetchone()
        last_loaded = result[0] if result else None

        if last_loaded:
            df = df[df['ORDER_DATE'] > pd.Timestamp(last_loaded)]
            print(f"incremental mode - last loaded: {last_loaded}, new rows: {len(df)}")
        else:
            print("first load, loading everything")

    if len(df) > 0:
        # quote_identifiers=False is important here - without it write_pandas wraps column
        # names in quotes and snowflake treats them as case-sensitive lowercase, then
        # the INSERT fails because the table has uppercase columns. took way too long to debug
        write_pandas(conn, df, 'BRONZE_ORDERS', quote_identifiers=False)
    else:
        print("no new rows to load")

    cur.close()
    conn.close()

    print(f"bronze done - {len(df)} rows loaded to snowflake")
    return len(df)


if __name__ == "__main__":
    generate_bronze()
