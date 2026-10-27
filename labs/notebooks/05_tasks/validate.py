# Databricks notebook source
# MAGIC %md
# MAGIC ### Job task: validate the pipeline output
# MAGIC Counts rows the pipeline rejected or merged away and publishes the number as a **task value** (`bad_rows`).
# MAGIC A downstream **if/else condition task** reads it with `{{tasks.validate.values.bad_rows}}`.
# MAGIC
# MAGIC Set parameter `simulate_flaky = true` to make the **first attempt fail on purpose**: use it to practice retries and *Repair run*.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

import os

dbutils.widgets.text("pipeline_schema", f"{schema}_lakeflow")
dbutils.widgets.text("simulate_flaky", "false")
ps = f"{catalog}.{dbutils.widgets.get('pipeline_schema')}"

marker = f"{checkpoints}/_flaky_marker"
if dbutils.widgets.get("simulate_flaky").lower() == "true" and not os.path.exists(marker):
    open(marker, "w").write("first attempt failed here")
    raise RuntimeError("Simulated transient failure (e.g. a source system timeout). A retry will succeed.")

# COMMAND ----------

bronze = spark.table(f"{ps}.bronze_orders").count()
silver = spark.table(f"{ps}.silver_orders").count()
null_customers = spark.table(f"{ps}.bronze_orders").filter("customer_id IS NULL").count()
bad_rows = bronze - silver

print(f"bronze={bronze} silver={silver} null_customers={null_customers} bad_rows={bad_rows}")
dbutils.jobs.taskValues.set(key="bad_rows", value=bad_rows)
dbutils.jobs.taskValues.set(key="null_customers", value=null_customers)