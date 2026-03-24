-- thin view on silver_orders
-- mostly just selecting the columns the marts need + adding a surrogate key
-- keeping it as a view so it stays in sync automatically

with source as (
    select * from {{ source('silver', 'silver_orders') }}
)

select
    md5(order_id) as order_sk,
    order_id,
    customer_id,
    product_id,
    category,
    order_date,
    quantity,
    unit_price,
    discount_pct,
    shipping_cost,
    payment_method,
    city,
    status,
    is_weekend,
    is_returned,
    gross_amount,
    discount_amount,
    net_amount,
    total_amount,
    order_year,
    order_month,
    order_quarter,
    order_day_name,
    customer_rating,
    return_reason
from source
