-- ============================================================================
-- Northwind Outdoors — Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables)
-- Used by: notebook 04 (concepts) and the bundle in databricks.yml (deployment).
--
-- You declare WHAT each table is; the pipeline works out the order (the DAG),
-- incremental processing, retries, checkpoints and data quality metrics.
--
-- Pipeline configuration value required:
--   source_path = /Volumes/<catalog>/<schema>/landing
-- ============================================================================


-- ---------------------------------------------------------------- BRONZE ----
-- STREAMING TABLE + STREAM read_files(...) = Auto Loader under the hood:
-- each file is processed exactly once, schema is inferred and evolved.

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders, one row per JSON line, as delivered'
AS SELECT *,
          _metadata.file_path  AS source_file,
          current_timestamp()  AS ingested_at
FROM STREAM read_files('${source_path}/orders', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'Raw customer snapshots and updates'
AS SELECT *,
          _metadata.file_path  AS source_file,
          current_timestamp()  AS ingested_at
FROM STREAM read_files('${source_path}/customers', format => 'csv', header => true);

CREATE OR REFRESH MATERIALIZED VIEW bronze_products
COMMENT 'Small reference table, fully recomputed each update'
AS SELECT * FROM read_files('${source_path}/products', format => 'csv', header => true);


-- ---------------------------------------------------------------- SILVER ----
-- Expectations = data quality rules. Three behaviours:
--   EXPECT (...)                          -> keep the row, count the violation (warn)
--   EXPECT (...) ON VIOLATION DROP ROW    -> drop the row, count it
--   EXPECT (...) ON VIOLATION FAIL UPDATE -> stop the whole update

CREATE OR REFRESH STREAMING TABLE silver_orders_clean (
  CONSTRAINT has_customer   EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_order_id   EXPECT (order_id IS NOT NULL)    ON VIOLATION FAIL UPDATE,
  CONSTRAINT known_status   EXPECT (status IN ('placed','paid','shipped','delivered','cancelled'))
)
COMMENT 'Typed and validated orders; may still contain duplicates'
AS SELECT order_id,
          customer_id,
          to_timestamp(order_ts)  AS order_ts,
          status,
          shipping.country        AS ship_country,
          shipping.express        AS express,
          items,
          ingested_at
FROM STREAM(bronze_orders);

-- Deduplicate with AUTO CDC as SCD Type 1: one row per order_id, latest arrival wins.
-- This is the declarative replacement for a hand-written MERGE INTO.
CREATE OR REFRESH STREAMING TABLE silver_orders
COMMENT 'One row per order (SCD Type 1 on order_id)';

CREATE FLOW orders_dedup AS AUTO CDC INTO silver_orders
FROM STREAM(silver_orders_clean)
KEYS (order_id)
SEQUENCE BY ingested_at
COLUMNS * EXCEPT (ingested_at)
STORED AS SCD TYPE 1;


CREATE OR REFRESH MATERIALIZED VIEW silver_order_lines (
    CONSTRAINT positive_qty EXPECT (qty > 0) ON VIOLATION DROP ROW,
    CONSTRAINT has_price EXPECT (unit_price IS NOT NULL)
)
COMMENT 'Order items exploded to one row per line, priced'
AS
SELECT
    o.order_id,
    o.order_ts,
    li.pos AS line_no,
    li.item.product_id AS product_id,
    li.item.qty AS qty,
    coalesce(li.item.unit_price, p.list_price) AS unit_price,
    p.category,
    round(li.item.qty * coalesce(li.item.unit_price, p.list_price), 2) AS line_amount
FROM silver_orders o
LEFT JOIN bronze_products p
    ON p.product_id = o.items[0].product_id
LATERAL VIEW posexplode(o.items) li AS pos, item;

-- SCD Type 2 customer dimension with AUTO CDC (formerly APPLY CHANGES INTO).
-- Every change in country/region/tier creates a new row with __START_AT / __END_AT.

CREATE OR REFRESH STREAMING TABLE silver_customers_scd2
COMMENT 'Customer history, SCD Type 2';

CREATE OR REFRESH STREAMING TABLE customers_clean
COMMENT 'Typed customer change records feeding the SCD2 flow'
AS SELECT customer_id,
          initcap(trim(name))             AS name,
          nullif(lower(trim(email)), '')  AS email,
          upper(country)                  AS country,
          region,
          coalesce(loyalty_tier, 'none')  AS loyalty_tier,
          to_timestamp(updated_at)        AS updated_at
FROM STREAM(bronze_customers);

CREATE FLOW customers_cdc AS AUTO CDC INTO silver_customers_scd2
FROM STREAM(customers_clean)
KEYS (customer_id)
SEQUENCE BY updated_at
COLUMNS * EXCEPT (updated_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON country, region, loyalty_tier;


-- ------------------------------------------------------------------ GOLD ----

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
COMMENT 'Revenue and orders per day and shipping country, excluding cancellations'
AS SELECT date(o.order_ts)              AS order_date,
          o.ship_country,
          count(DISTINCT o.order_id)    AS orders,
          round(sum(l.line_amount), 2)  AS revenue
FROM silver_orders o
JOIN silver_order_lines l ON l.order_id = o.order_id
WHERE o.status <> 'cancelled'
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_region_current
COMMENT 'Revenue by the customer''s CURRENT region (uses only open SCD2 rows)'
AS SELECT c.region,
          c.loyalty_tier,
          count(DISTINCT o.order_id)    AS orders,
          round(sum(l.line_amount), 2)  AS revenue
FROM silver_orders o
JOIN silver_order_lines l ON l.order_id = o.order_id
JOIN silver_customers_scd2 c
  ON c.customer_id = o.customer_id AND c.`__END_AT` IS NULL
GROUP BY ALL;
