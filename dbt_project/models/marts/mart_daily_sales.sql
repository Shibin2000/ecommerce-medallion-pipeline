-- daily rollup used by the revenue dashboard
-- granularity: one row per day
-- note: return_rate_pct can be 0 on days with no returns, that's fine

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
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
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(
        sum(case when is_returned then 1 else 0 end) * 100.0 / count(*),
        2
    ) as return_rate_pct
from orders
group by order_date::date, order_year, order_month, order_quarter
order by sale_date
