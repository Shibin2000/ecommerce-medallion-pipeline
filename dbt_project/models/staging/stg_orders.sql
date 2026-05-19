-- thin view on silver_orders
-- mostly just selecting the columns the marts need + adding a surrogate key
-- keeping it as a view so it stays in sync automatically

with source as (
    select * from {{ source('silver', 'SILVER_ORDERS') }}
)

select
    md5(ORDER_ID)    as order_sk,
    ORDER_ID         as order_id,
    CUSTOMER_ID      as customer_id,
    PRODUCT_ID       as product_id,
    CATEGORY         as category,
    ORDER_DATE       as order_date,
    QUANTITY         as quantity,
    UNIT_PRICE       as unit_price,
    DISCOUNT_PCT     as discount_pct,
    SHIPPING_COST    as shipping_cost,
    PAYMENT_METHOD   as payment_method,
    CITY             as city,
    STATUS           as status,
    IS_WEEKEND       as is_weekend,
    IS_RETURNED      as is_returned,
    GROSS_AMOUNT     as gross_amount,
    DISCOUNT_AMOUNT  as discount_amount,
    NET_AMOUNT       as net_amount,
    TOTAL_AMOUNT     as total_amount,
    ORDER_YEAR       as order_year,
    ORDER_MONTH      as order_month,
    ORDER_QUARTER    as order_quarter,
    ORDER_DAY_NAME   as order_day_name,
    CUSTOMER_RATING  as customer_rating,
    RETURN_REASON    as return_reason
from source
