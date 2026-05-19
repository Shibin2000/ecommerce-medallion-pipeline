# ecommerce-medallion-pipeline

End-to-end data pipeline that moves 100k synthetic ecommerce orders through bronze -> silver -> gold. Built this to get hands-on with the patterns I keep seeing in data engineering job descriptions -- incremental loading, dbt, Airflow, data quality checks.

Migrated from DuckDB to Snowflake as the warehouse layer. The local DuckDB setup was fine for getting things working but wanted to push it to a cloud warehouse since that's what most job specs actually ask for.

---

## what it does

Raw orders come in with intentional problems: negative prices, missing return reasons, cancelled orders in the mix. The pipeline cleans them up, builds out revenue columns, and loads aggregated tables ready for reporting.

```
bronze_orders (100k raw rows)
       |  filter + enrich
silver_orders (~80k clean rows)
       |
gold_*.py          -> raw aggregated tables (daily sales, categories, RFM, cities)
       |  dbt models on top of silver
mart_daily_sales        -> revenue KPIs by day
mart_category_metrics   -> performance by product category
mart_customer_segments  -> RFM: VIP / Premium / Regular / New
mart_city_metrics       -> orders and revenue by city
```

gold.py builds intermediate aggregation tables directly. dbt then runs on top of silver independently to produce the mart tables with proper schema tests. The mart tables are what you'd actually connect a BI tool to.

---

## stack

- **Snowflake** -- cloud warehouse, replaced DuckDB
- **pandas** -- data generation and silver transforms
- **dbt-snowflake** -- mart models + schema tests
- **Apache Airflow** -- daily DAG with retries
- **PySpark** -- used for some exploratory queries on silver
- **pytest** -- 9 tests covering bronze through gold
- **Plotly** -- charts saved to images/

---

## numbers

- 100,000 orders in bronze
- ~80k rows survive into silver (dropped cancelled + bad data)
- $55.9M total revenue across 9 product categories
- 19,584 unique customers segmented into VIP / Premium / Regular / New

---

## folder layout

```
ecommerce-medallion-pipeline/
|-- pipeline/
|   |-- snowflake_conn.py   # connection helper, reads from .env
|   |-- bronze.py           # synthetic data gen -> BRONZE_ORDERS
|   |-- silver.py           # cleaning, revenue calcs, date features
|   |-- gold.py             # 4 aggregated gold tables
|   |-- quality_checks.py   # assertions after each run
|   `-- run_pipeline.py     # runs full pipeline including dbt
|-- dbt_project/
|   |-- models/staging/     # stg_orders view on top of silver
|   |-- models/marts/       # mart_daily_sales, mart_category_metrics,
|   |                       # mart_customer_segments, mart_city_metrics
|   `-- dbt_tests/          # dbt schema tests
|-- dags/
|   `-- ecommerce_dag.py    # Airflow DAG, daily schedule
|-- tests/
|   `-- test_pipeline.py    # pytest against ECOMMERCE_TEST schema
`-- images/
```

---

## setup

Copy `.env.example` to `.env` and fill in your Snowflake creds.

```bash
git clone https://github.com/Shibin2000/ecommerce-medallion-pipeline
cd ecommerce-medallion-pipeline
pip install -r requirements.txt

# run the full pipeline (bronze -> silver -> gold -> dbt -> quality checks)
cd pipeline
python run_pipeline.py

# or run dbt separately
cd ../dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .

# pytest -- set SNOWFLAKE_SCHEMA=ECOMMERCE_TEST in .env first
cd ..
pytest tests/test_pipeline.py -v
```

---

## charts

**revenue by category**
![category revenue](images/chart_category_revenue.png)

**monthly revenue trend**
![monthly trend](images/chart_weekly_trend.png)

**customer segments**
![customer segments](images/chart_customer_segments.png)

---

## notes

**incremental loading** -- bronze only loads rows where `order_date > MAX(order_date)` already in the table. `write_pandas` needs `quote_identifiers=False` or Snowflake treats column names as case-sensitive lowercase and the insert fails against the uppercase table columns. Took a while to figure out.

**gold vs dbt marts** -- gold.py writes aggregated tables directly to Snowflake. dbt runs on top of silver separately and produces the mart tables. Both exist in the repo because I built gold.py first to verify the aggregation logic before moving it into dbt. The mart tables are the ones to use downstream.

**RFM thresholds** -- the $500/$1000/$2000 breakpoints came from eyeballing the spend histogram. In a real project you'd want percentile-based cutoffs.

**dbt on Snowflake** -- `date_diff('day', ...)` from DuckDB doesn't exist in Snowflake. It's `DATEDIFF('day', start, end)` and the arg order is flipped. Fixed after first run blew up.
