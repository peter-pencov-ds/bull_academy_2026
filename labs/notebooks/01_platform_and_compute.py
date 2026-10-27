# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · The Data Intelligence Platform and compute
# MAGIC
# MAGIC **Module:** C1 · **Exam domain:** Databricks Intelligence Platform (6%) · **Time:** 2–3 h
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Tell the control plane from the compute plane, and say where your data actually lives
# MAGIC - Navigate the Unity Catalog namespace `catalog.schema.object`
# MAGIC - Explain what makes a table a Delta table (files + transaction log)
# MAGIC - Pick the right compute type for a described workload
# MAGIC - See Spark's lazy evaluation with your own eyes

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Where am I running?
# MAGIC
# MAGIC | Plane | Runs | Owns |
# MAGIC | --- | --- | --- |
# MAGIC | **Control plane** (Databricks account) | Web UI, notebooks, job scheduler, Unity Catalog metastore | Metadata, permissions, code |
# MAGIC | **Compute plane** (serverless: Databricks; classic: your cloud account) | Spark clusters, SQL warehouses | The actual processing |
# MAGIC | **Your cloud storage** | Object storage behind managed tables and volumes | **Your data** |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user()      AS me,
# MAGIC        current_catalog()   AS catalog,
# MAGIC        current_schema()    AS schema,
# MAGIC        current_version()   AS dbr_version

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The Unity Catalog namespace
# MAGIC Three levels: **catalog → schema → object** (table, view, volume, function, model).
# MAGIC Open **Catalog Explorer** in the left sidebar and find the `landing` volume you created in `00_setup`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. What is a Delta table, physically?
# MAGIC Create a small table from the built-in `samples` catalog and inspect it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE trips_small AS
# MAGIC SELECT * FROM samples.nyctaxi.trips LIMIT 10000;
# MAGIC
# MAGIC DESCRIBE DETAIL trips_small;

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE DETAIL` shows `format = delta`, a storage `location`, `numFiles` and `sizeInBytes`.
# MAGIC Every write adds a commit to the **transaction log** (`_delta_log`). That log is what gives you ACID
# MAGIC transactions, time travel and schema enforcement on top of plain Parquet files.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE trips_small SET fare_amount = fare_amount * 1.1 WHERE trip_distance > 10;
# MAGIC DESCRIBE HISTORY trips_small;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lazy evaluation — transformations vs actions
# MAGIC Transformations (`filter`, `select`, `groupBy`) only build a plan. Actions (`count`, `display`, `write`) run it.

# COMMAND ----------

import time
from pyspark.sql import functions as F

t0 = time.time()
plan = (spark.table("samples.nyctaxi.trips")
        .filter(F.col("trip_distance") > 5)
        .groupBy("pickup_zip")
        .agg(F.avg("fare_amount").alias("avg_fare")))
print(f"building the plan took {time.time() - t0:.3f}s — nothing ran yet")

# Serverless compute uses Photon by default, so Photon-related operators may appear in the plan.
plan.explain()  # Read bottom-up: scan → filter → partial aggregation → exchange (shuffle) → final aggregation

# COMMAND ----------

t0 = time.time()
n = plan.count()  # action: now Spark actually reads data
print(f"{n} zips, took {time.time() - t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Choosing compute
# MAGIC
# MAGIC | Compute | Best for | Billing | Watch out |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **All-purpose (classic)** | Interactive development, shared exploration | Per uptime, higher DBU rate | Expensive if left running; set auto-termination |
# MAGIC | **Job compute (classic)** | Scheduled production jobs | Per job run, lower DBU rate | Startup time per run |
# MAGIC | **Serverless (notebooks / jobs / pipelines)** | Most workloads; fast start, no tuning | Per use | Limited Spark config; no Spark UI access to cluster internals |
# MAGIC | **SQL warehouse** (serverless / pro / classic) | SQL, dashboards, dbt, BI tools | Per uptime, auto-stop | SQL only |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1 — pick the compute (write your answers in this cell)
# MAGIC 1. A nightly job that rebuilds gold tables from 200 GB of silver data.
# MAGIC 2. Three analysts exploring data in notebooks all afternoon.
# MAGIC 3. A Power BI dashboard refreshed every 15 minutes.
# MAGIC 4. A pipeline that must start within seconds when a file arrives.
# MAGIC
# MAGIC *Your answers:*
# MAGIC
# MAGIC ### print solution in cell below

# COMMAND ----------

print(answers["01_platform_and_compute"]["Q1"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2 — Liquid Clustering preview
# MAGIC Liquid Clustering replaces partitioning + Z-ORDER for most tables. Cluster `trips_small` by `pickup_zip`,
# MAGIC run `OPTIMIZE`, and compare `DESCRIBE DETAIL` before and after (look at `clusteringColumns` and `numFiles`).

# COMMAND ----------

# TODO: write the SQL (use spark.sql or a %sql cell)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. Where is the data of a Unity Catalog **managed** table physically stored?
# MAGIC    A) In the control plane  B) In cloud object storage  C) On the driver's local disk  D) In the metastore database
# MAGIC 2. Which statement triggers Spark to execute a job? A) `df.filter(...)` B) `df.select(...)` C) `df.count()` D) `df.withColumn(...)`
# MAGIC 3. A scheduled production ETL job should usually run on: A) all-purpose cluster B) job compute or serverless jobs C) SQL warehouse D) driver-only cluster
# MAGIC
# MAGIC ### print solution in cell below

# COMMAND ----------

print(answers["01_platform_and_compute"]["Q2"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)
# MAGIC **Ex 1:** 1 job compute / serverless job · 2 all-purpose with auto-termination (or serverless notebooks) · 3 SQL warehouse · 4 serverless (fast start) with a file-arrival trigger.

# COMMAND ----------

# SOLUTION — Exercise 2
spark.sql("ALTER TABLE trips_small CLUSTER BY (pickup_zip)")
spark.sql("OPTIMIZE trips_small")
display(spark.sql("DESCRIBE DETAIL trips_small").select("clusteringColumns", "numFiles", "sizeInBytes"))