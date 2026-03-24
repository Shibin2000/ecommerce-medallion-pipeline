-- revenue and order breakdown by city
-- used to compare regional performance across the 6 cities in the dataset

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    city,
    count(*) as total_orders,
    count(distinct customer_id) as unique_customers,
    sum(quantity) as total_items_sold,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(total_amount), 2) as avg_order_value,
    round(avg(customer_rating), 2) as avg_rating,
    sum(case when is_returned then 1 else 0 end) as total_returns,
    round(
        sum(case when is_returned then 1 else 0 end) * 100.0 / count(*),
        2
    ) as return_rate_pct
from orders
group by city
order by total_revenue desc
