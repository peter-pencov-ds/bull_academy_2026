# Databricks notebook source
# MAGIC %md
# MAGIC ### Job task: publish run summary
# MAGIC Appends one row per run to `job_run_log`, using **dynamic value references** passed in as parameters
# MAGIC (`{{job.run_id}}`, `{{job.start_time.iso_datetime}}`) and task values from upstream tasks.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("run_started", "")
dbutils.widgets.text("pipeline_schema", f"{schema}_lakeflow")
ps = f"{catalog}.{dbutils.widgets.get('pipeline_schema')}"

bad_rows = dbutils.jobs.taskValues.get(taskKey="validate", key="bad_rows", default=-1, debugValue=-1)
revenue = spark.sql(f"SELECT round(sum(revenue), 2) AS r FROM {ps}.gold_daily_sales").first()["r"]

spark.sql("""CREATE TABLE IF NOT EXISTS job_run_log
             (run_id STRING, run_started STRING, bad_rows BIGINT, total_revenue DOUBLE, logged_at TIMESTAMP)""")
spark.createDataFrame(
    [(dbutils.widgets.get("run_id"), dbutils.widgets.get("run_started"), int(bad_rows), float(revenue or 0))],
    "run_id STRING, run_started STRING, bad_rows BIGINT, total_revenue DOUBLE",
).selectExpr("*", "current_timestamp() AS logged_at").write.mode("append").saveAsTable("job_run_log")

display(spark.table("job_run_log").orderBy("logged_at", ascending=False))