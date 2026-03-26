import pandas as pd
import duckdb


def run_silver(db_path="ecommerce_lakehouse.db"):

    with duckdb.connect(db_path) as conn:
        df = conn.execute("SELECT * FROM bronze_orders").fetchdf()

    print(f"loaded {len(df)} rows from bronze")

    # duckdb fetchdf gives object dtype for strings which breaks when you try
    # to re-register the df back. casting to StringDtype fixes it
    for col in df.select_dtypes(include=['object', 'str']).columns:
        df[col] = df[col].astype(pd.StringDtype())

    original_count = len(df)

    # drop bad rows
    # tried filtering only delivered+shipped but removed too much, keeping pending
    # df = df[df['status'].isin(['delivered', 'shipped'])]  # too aggressive
    df = df[df['unit_price'] > 0]
    df = df[df['quantity'] > 0]
    df = df[df['status'] != 'cancelled']

    df['order_date'] = pd.to_datetime(df['order_date'])

    # fill nulls before doing calculations
    df['customer_rating'] = df['customer_rating'].fillna(0).astype(float)
    df['return_reason'] = df['return_reason'].fillna('No Return')

    # revenue calcs
    df['gross_amount'] = (df['unit_price'] * df['quantity']).round(2)
    df['discount_amount'] = (df['gross_amount'] * df['discount_pct'] / 100).round(2)
    df['net_amount'] = (df['gross_amount'] - df['discount_amount']).round(2)
    df['total_amount'] = (df['net_amount'] + df['shipping_cost']).round(2)

    # date parts - useful for grouping in the gold layer
    df['order_year'] = df['order_date'].dt.year
    df['order_month'] = df['order_date'].dt.month
    df['order_quarter'] = df['order_date'].dt.quarter
    df['order_day_name'] = df['order_date'].dt.day_name().astype(pd.StringDtype())
    df['is_weekend'] = df['order_day_name'].isin(['Saturday', 'Sunday'])
    df['is_returned'] = df['status'] == 'returned'

    # make sure all string cols are proper StringDtype before writing back to duckdb
    for col in ['order_id', 'customer_id', 'product_id', 'category',
                'payment_method', 'city', 'status', 'return_reason', 'order_day_name']:
        if col in df.columns:
            df[col] = df[col].astype(pd.StringDtype())

    with duckdb.connect(db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS silver_orders")
        conn.register("df", df)
        conn.execute("CREATE TABLE silver_orders AS SELECT * FROM df")
        conn.unregister("df")
        final_count = conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]

    dropped = original_count - final_count
    print(f"silver done - kept {final_count} rows, dropped {dropped} ({round(dropped/original_count*100, 1)}%)")
    return final_count


if __name__ == "__main__":
    run_silver()


