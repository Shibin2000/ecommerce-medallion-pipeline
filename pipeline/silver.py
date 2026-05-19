import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake_conn import get_connection


def run_silver():

    conn = get_connection()

    # fetch_pandas_all() is way faster than fetchdf() for bigger tables
    # learned that the hard way on the first run when it sat there for 3 minutes
    cur = conn.cursor()
    cur.execute("SELECT * FROM BRONZE_ORDERS")
    df = cur.fetch_pandas_all()

    print(f"loaded {len(df)} rows from bronze")
    original_count = len(df)

    df = df[df['UNIT_PRICE'] > 0]
    df = df[df['QUANTITY'] > 0]
    df = df[df['STATUS'] != 'cancelled']

    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'])
    df['CUSTOMER_RATING'] = df['CUSTOMER_RATING'].fillna(0).astype(float)
    df['RETURN_REASON'] = df['RETURN_REASON'].fillna('No Return')

    df['GROSS_AMOUNT']    = (df['UNIT_PRICE'] * df['QUANTITY']).round(2)
    df['DISCOUNT_AMOUNT'] = (df['GROSS_AMOUNT'] * df['DISCOUNT_PCT'] / 100).round(2)
    df['NET_AMOUNT']      = (df['GROSS_AMOUNT'] - df['DISCOUNT_AMOUNT']).round(2)
    df['TOTAL_AMOUNT']    = (df['NET_AMOUNT'] + df['SHIPPING_COST']).round(2)

    df['ORDER_YEAR']     = df['ORDER_DATE'].dt.year
    df['ORDER_MONTH']    = df['ORDER_DATE'].dt.month
    df['ORDER_QUARTER']  = df['ORDER_DATE'].dt.quarter
    df['ORDER_DAY_NAME'] = df['ORDER_DATE'].dt.day_name()
    df['IS_WEEKEND']     = df['ORDER_DAY_NAME'].isin(['Saturday', 'Sunday'])
    df['IS_RETURNED']    = df['STATUS'] == 'returned'

    cur.execute("DROP TABLE IF EXISTS SILVER_ORDERS")

    # write_pandas needs quote_identifiers=False here too, same reason as bronze
    write_pandas(conn, df, 'SILVER_ORDERS', auto_create_table=True, quote_identifiers=False)

    final_count = cur.execute("SELECT COUNT(*) FROM SILVER_ORDERS").fetchone()[0]
    dropped = original_count - final_count

    cur.close()
    conn.close()

    print(f"silver done - kept {final_count} rows, dropped {dropped} ({round(dropped/original_count*100,1)}%)")
    return final_count


if __name__ == "__main__":
    run_silver()
