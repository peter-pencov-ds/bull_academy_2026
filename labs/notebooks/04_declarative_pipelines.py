# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC **Modules:** C2/C3 (exam) and B3 (deep dive) · **Time:** 1 day
# MAGIC
# MAGIC *Old name: Delta Live Tables (DLT). The exam uses the new name; you'll still see `dlt` in older docs and code.*
# MAGIC
# MAGIC In notebooks 02–03 you wrote **imperative** code: read this, write that, in this order, with this checkpoint.
# MAGIC A declarative pipeline lets you state **what each table is**, and the platform handles ordering, incremental
# MAGIC processing, checkpoints, retries and data quality tracking.
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Create and run a pipeline from the SQL source file `pipeline/northwind_pipeline.sql`
# MAGIC - Explain streaming tables vs materialized views inside a pipeline
# MAGIC - Use expectations (warn / drop / fail) and read their metrics
# MAGIC - Build SCD Type 1 and Type 2 tables with `AUTO CDC` (formerly `APPLY CHANGES`)
# MAGIC - Query the pipeline event log

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Read the source before you run it (20 min)
# MAGIC Open `pipeline/northwind_pipeline.sql` in a new tab. Answer in the cell below:
# MAGIC 1. Which tables are **streaming tables** and which are **materialized views**? Why was each choice made?
# MAGIC 2. Which expectation stops the whole update? Which one only counts violations?
# MAGIC 3. `silver_orders` has no `AS SELECT`. Where does its data come from?
# MAGIC 4. Draw (on paper) the DAG: which tables depend on which?
# MAGIC
# MAGIC *Your answers:*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Create the pipeline in the UI (20 min)
# MAGIC In notebook 05 you'll deploy the same pipeline with a bundle. Doing it once by hand first shows you what the bundle automates.
# MAGIC
# MAGIC 1. **Jobs & Pipelines → Create → ETL pipeline**.
# MAGIC 2. Name: `northwind_<your name>`.
# MAGIC 3. Source code: add the existing file `pipeline/northwind_pipeline.sql` from your folder.
# MAGIC 4. Default catalog = the value printed below; default schema = `<your schema>_lakeflow` (create it first with the next cell).
# MAGIC 5. Compute: **Serverless**.
# MAGIC 6. **Configuration** → add key `source_path` with the landing value printed below.
# MAGIC 7. Click **Start**. Watch the graph build left to right.

# COMMAND ----------

pipeline_schema = f"{schema}_lakeflow"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{pipeline_schema}")
print(f"catalog      = {catalog}")
print(f"schema       = {pipeline_schema}")
print(f"source_path  = {landing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Inspect the results

# COMMAND ----------

def q(sql):
    display(spark.sql(sql.format(c=catalog, s=pipeline_schema)))

q("SHOW TABLES IN {c}.{s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2: who moved?
# MAGIC Batch 2 changed the country of 60 customers. Each of them should now have **two** rows: the old one closed (`__END_AT` set) and the current one open.

# COMMAND ----------

q("""
SELECT customer_id, country, region, loyalty_tier, `__START_AT`, `__END_AT`
FROM {c}.{s}.silver_customers_scd2
WHERE customer_id IN (
  SELECT customer_id FROM {c}.{s}.silver_customers_scd2 GROUP BY customer_id HAVING count(*) > 1)
ORDER BY customer_id, `__START_AT`
LIMIT 20
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality metrics from the event log
# MAGIC Every pipeline writes an **event log**. The `flow_progress` events carry expectation results.

# COMMAND ----------

q("""
SELECT timestamp,
       details:flow_progress.data_quality.expectations AS expectations
FROM event_log(TABLE({c}.{s}.silver_orders_clean))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — Exercises
# MAGIC
# MAGIC **D1. Incremental processing.** If you haven't yet, land batch 3 (`land_batch(3, landing)` after `%run ./_data_generator`).
# MAGIC Start the pipeline again. In the graph, compare *rows written* for `bronze_orders` with the first run. Why is it ~830 and not ~3,630?
# MAGIC
# MAGIC **D2. Duplicates.** Batch 3 contains 30 duplicate orders. Prove `silver_orders` has exactly one row per `order_id`.
# MAGIC
# MAGIC **D3. Add an expectation.** Add `CONSTRAINT recent_order EXPECT (order_ts >= '2026-01-01')` to `silver_orders_clean`, rerun, and find its metrics in the event log.
# MAGIC
# MAGIC **D4. Full refresh.** What does **Full refresh all** do to the streaming tables? When would you use it, and why is it dangerous if source files have been deleted?
# MAGIC
# MAGIC **D5 (stretch, B3).** Batch 3 contains 40 *late* orders (`order_ts` 10+ days old). Rewrite `gold_daily_sales` as a streaming table aggregation with a 3-day watermark. Which late orders are dropped, and why is that sometimes the right trade-off?

# COMMAND ----------

# TODO — D2


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. `CONSTRAINT x EXPECT (amount > 0)` with no `ON VIOLATION` clause. A row with `amount = -5`: A) is dropped B) fails the update C) is written, and the violation is counted D) goes to a quarantine table
# MAGIC 2. Which object processes only **new** source rows on each update? A) Materialized view B) Streaming table C) View D) Temp view
# MAGIC 3. You need full change history of customer addresses. A) `AUTO CDC ... STORED AS SCD TYPE 1` B) `AUTO CDC ... STORED AS SCD TYPE 2` C) `MERGE INTO` with `WHEN MATCHED UPDATE` D) `INSERT OVERWRITE`
# MAGIC 4. A pipeline's old name in documentation is: A) Delta Engine B) Delta Live Tables C) Workflows D) Unity Pipelines
# MAGIC
# MAGIC ### print solution in cell below

# COMMAND ----------

print(answers["04_declarative_pipelines"]["Q1"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)
# MAGIC **A:** streaming tables = bronze_orders, bronze_customers, silver_orders_clean, customers_clean, silver_orders, silver_customers_scd2 (append/incremental sources); MVs = bronze_products (tiny, full recompute), silver_order_lines and gold (joins/aggregations over changing data). `has_order_id` fails the update; `known_status` only counts. `silver_orders` is filled by the `orders_dedup` flow.
# MAGIC
# MAGIC **D1:** streaming tables keep a checkpoint; only batch 3's file is new.
# MAGIC
# MAGIC **D4:** truncates streaming tables and reprocesses all source files from scratch. If raw files were deleted (retention), that data is lost from bronze.

# COMMAND ----------

# SOLUTION — D2
q("SELECT count(*) AS rows, count(DISTINCT order_id) AS distinct_ids FROM {c}.{s}.silver_orders")