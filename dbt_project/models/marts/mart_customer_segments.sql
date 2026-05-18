-- customer segmentation by spend and frequency
-- thresholds are rough - I looked at the spend histogram and picked
-- breakpoints that gave reasonable group sizes. not scientific but works for this dataset
-- in prod you'd probably want percentile-based cutoffs

-- note: duckdb used date_diff('day', date1, date2) but snowflake is
-- DATEDIFF('day', start, end) and the arg order matters. fixed after first run failed

with orders as (
    select * from {{ ref('stg_orders') }}
),

rfm as (
    select
        customer_id,
        count(*) as frequency,
        round(sum(total_amount), 2) as monetary,
        DATEDIFF('day', max(order_date::date), CURRENT_DATE) as recency_days,
        min(order_date::date) as first_order_date,
        max(order_date::date) as last_order_date,
        round(avg(customer_rating), 2) as avg_rating,
        count(distinct category) as categories_bought
    from orders
    group by customer_id
)

select
    *,
    case
        when monetary > 2000 and frequency >= 5 then 'VIP'
        when monetary > 1000 and frequency >= 3 then 'Premium'
        when monetary > 500 then 'Regular'
        else 'New'
    end as customer_segment
from rfm
order by monetary desc
