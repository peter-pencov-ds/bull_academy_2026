# Databricks notebook source
# MAGIC %md
# MAGIC ### Job task: quarantine report (runs only when `bad_rows > 0`)
# MAGIC Copies rejected orders into a `quarantine_orders` table so someone can follow up with the source system.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

dbutils.widgets.text("pipeline_schema", f"{schema}_lakeflow")
ps = f"{catalog}.{dbutils.widgets.get('pipeline_schema')}"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema}.quarantine_orders AS
SELECT order_id, customer_id, order_ts, source_file, ingested_at,
       CASE WHEN customer_id IS NULL THEN 'null_customer' ELSE 'duplicate' END AS reason
FROM {ps}.bronze_orders
WHERE customer_id IS NULL
   OR order_id IN (SELECT order_id FROM {ps}.bronze_orders GROUP BY order_id HAVING count(*) > 1)
""")
n = spark.table(f"{catalog}.{schema}.quarantine_orders").count()
print(f"{n} rows quarantined")