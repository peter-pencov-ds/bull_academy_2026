# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 07 · Troubleshooting, monitoring and optimization
# MAGIC
# MAGIC **Module:** C6 · **Exam domain:** Troubleshooting, Monitoring and Optimization (10%) · **Time:** 1 day
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Read the Spark UI: jobs → stages → tasks, and spot **skew**, **shuffle** and **spill**
# MAGIC - Compare job run times against a baseline using run history and system tables
# MAGIC - Diagnose out-of-memory errors, cluster start failures and library conflicts
# MAGIC - Fix the small-files problem and apply Liquid Clustering / predictive optimization
# MAGIC
# MAGIC > **Compute:** run this notebook on a **classic all-purpose cluster** (Academy Cluster).
# MAGIC > Serverless hides the Spark UI and won't let you change the configs we break on purpose.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to read the Spark UI in 60 seconds
# MAGIC Click **View** next to a Spark job under any cell (or *Compute → your cluster → Spark UI*).
# MAGIC
# MAGIC | Symptom | Where you see it | Typical cause | Typical fix |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Skew** | Stage → *Summary metrics*: Max task duration ≫ Median | A few keys hold most rows | AQE skew join (on by default), salting, filter hot keys, broadcast small side |
# MAGIC | **Heavy shuffle** | Stage → *Shuffle Read/Write* in GB | Wide joins / groupBy on big tables | Broadcast join, filter + project earlier, Liquid Clustering on join keys |
# MAGIC | **Spill** | Stage → *Spill (Memory) / Spill (Disk)* columns | Partitions too big for executor memory | More shuffle partitions (or let AQE), bigger workers |
# MAGIC | **Driver OOM** | Cell error `java.lang.OutOfMemoryError` / driver restarted | `collect()`, `toPandas()` on big data, huge broadcast | Keep data distributed; `limit`, write to a table |
# MAGIC | **Slow start** | Cluster *Event log* | Cloud capacity, init script failure, bad library | Pools, fix init script, check cluster event log |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1 — Data skew (30 min)
# MAGIC We build 20 M "events" where **one customer owns ~60% of the rows**, then join and aggregate.
# MAGIC AQE normally fixes skew, so we switch it **off** first to see the raw problem.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # force a shuffle join

events = (spark.range(20_000_000)
          .withColumn("customer_id",
                      F.when(F.rand(seed=1) < 0.6, F.lit("C00001"))                       # the hot key
                       .otherwise(F.format_string("C%05d", (F.rand(seed=2) * 500 + 1).cast("int"))))
          .withColumn("amount", F.round(F.rand(seed=3) * 100, 2)))
customers = spark.table("silver_customers").select("customer_id", "region")

skewed = events.join(customers, "customer_id").groupBy("region").agg(F.sum("amount").alias("amount"))
skewed.write.mode("overwrite").saveAsTable("tmp_skew_result")

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise 1a.** Open the Spark UI for the cell above. In the join stage, compare *Max* vs *Median* task duration and *Shuffle Read Size*. Write the numbers here:
# MAGIC
# MAGIC | Metric | Median | Max |
# MAGIC | --- | --- | --- |
# MAGIC | Duration | | |
# MAGIC | Shuffle read | | |
# MAGIC
# MAGIC **Exercise 1b.** Turn AQE back on (next cell), rerun, and compare. What changed in the stage list?

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
skewed.write.mode("overwrite").saveAsTable("tmp_skew_result")

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise 1c.** `customers` is tiny. What single change avoids the shuffle entirely? Implement it and confirm with `.explain()`.

# COMMAND ----------

# TODO — 1c
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 2 — Spill (20 min)
# MAGIC Too few shuffle partitions → each partition is huge → it doesn't fit in memory → Spark spills to disk.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "2")

sample = (spark.range(50_000_000)
 .withColumn("k", (F.col("id") % 5_000_000))
 .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
 .groupBy("k").agg(F.max("payload").alias("p"), F.count("*").alias("n"))
 )

sample.write.mode("overwrite").saveAsTable("tmp_spill_result")

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise 2.** Find *Spill (Memory)* and *Spill (Disk)* in the stage details. Then set `spark.sql.shuffle.partitions` to `200`, rerun, and record duration and spill again. Finally restore AQE (next cell).

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.unset("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 3 — Driver out-of-memory (15 min, read before running!)
# MAGIC The next cell pulls 30 M rows onto the driver. On a small cluster it will fail or restart the driver. **That's the point.**
# MAGIC Read the error message, then find the driver restart in *Compute → Event log*. Or if the job failed, open the jobs in spark UI and find the failure reason

# COMMAND ----------

# big = spark.range(30_000_000).withColumn("s", F.sha2(F.col("id").cast("string"), 512))
# rows = big.collect()            # ← anti-pattern: every row travels to one machine
# pdf  = big.toPandas()           # ← same problem

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise 3.** Rewrite the anti-pattern so the work stays distributed: you only need the number of rows whose hash starts with `"ab"`.

# COMMAND ----------

# TODO — 3


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 4 — Small files, OPTIMIZE and Liquid Clustering (30 min)

# COMMAND ----------

(spark.table("silver_order_lines")
 .repartition(400)                    # simulate many tiny writes (e.g. a stream with a short trigger)
 .write.mode("overwrite").saveAsTable("tmp_lines_small_files"))

display(spark.sql("DESCRIBE DETAIL tmp_lines_small_files").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tmp_lines_small_files CLUSTER BY (product_id);
# MAGIC OPTIMIZE tmp_lines_small_files;
# MAGIC DESCRIBE DETAIL tmp_lines_small_files;

# COMMAND ----------

# MAGIC %md
# MAGIC | Technique | What it does | When |
# MAGIC | --- | --- | --- |
# MAGIC | `OPTIMIZE` | Compacts small files into ~1 GB files | After many small writes |
# MAGIC | **Liquid Clustering** `CLUSTER BY (cols)` | Co-locates rows by the columns you filter/join on; keys can change later without rewriting | Default choice for new tables; replaces partitioning + `ZORDER` |
# MAGIC | `CLUSTER BY AUTO` | Databricks picks clustering keys from your query patterns | Managed tables when you're unsure |
# MAGIC | **Predictive optimization** | Databricks runs `OPTIMIZE`, `VACUUM`, `ANALYZE` for you on UC managed tables | Enabled at account / catalog / schema level |
# MAGIC | `VACUUM` | Deletes data files no longer referenced (default retention 7 days) | Storage cleanup; limits time travel |
# MAGIC
# MAGIC Check whether predictive optimization is on for your schema:

# COMMAND ----------

display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 5 — Is my job getting slower? (20 min)
# MAGIC **UI:** open your `northwind_job` → **Runs** tab. The bar chart shows duration per run; click a slow bar and compare its task timeline with a normal run to find the task that grew.
# MAGIC
# MAGIC **SQL:** the `system.lakeflow` system tables hold the same history for every job (ask your mentor if you lack access).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT j.name,
# MAGIC        t.run_id,
# MAGIC        t.result_state,
# MAGIC        timestampdiff(SECOND, t.period_start_time, t.period_end_time) AS seconds
# MAGIC FROM system.lakeflow.job_run_timeline t
# MAGIC JOIN system.lakeflow.jobs j USING (workspace_id, job_id)
# MAGIC WHERE j.name ILIKE '%northwind%'
# MAGIC   AND t.period_start_time >= current_date() - INTERVAL 14 DAYS
# MAGIC ORDER BY t.period_start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 6 — Broken job clinic (mentor-led, 45 min)
# MAGIC Your mentor deploys a copy of the job with three seeded faults. For each: find it, fix it, and write a 3-line root-cause note (symptom → cause → fix).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. In one stage, median task time is 2 s and max is 4 min. Most likely? A) Spill B) Data skew C) Driver OOM D) Too many partitions
# MAGIC 2. A stage shows large *Spill (Disk)*. First thing to try? A) Fewer shuffle partitions B) More shuffle partitions, or more memory per executor C) `collect()` the data D) Disable AQE
# MAGIC 3. Which replaces partitioning + Z-ORDER for most new tables? A) Bloom filters B) Liquid Clustering C) `VACUUM` D) Caching
# MAGIC 4. A notebook fails with `OutOfMemoryError` on the **driver** after `df.toPandas()`. Fix? A) Add workers B) Keep the computation in Spark and only bring back a small result C) Increase shuffle partitions D) Enable AQE
# MAGIC
# MAGIC <details><summary>Answers</summary>1 B · 2 B (more shuffle partitions or bigger executors) · 3 B · 4 B (adding workers does not grow the driver)</details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)

# COMMAND ----------

# SOLUTION — 1c: broadcast the small side, no shuffle of the 20 M rows
fixed = events.join(F.broadcast(customers), "customer_id").groupBy("region").agg(F.sum("amount").alias("amount"))
fixed.explain()   # look for BroadcastHashJoin

# COMMAND ----------

# SOLUTION — 3
big = spark.range(30_000_000).withColumn("s", F.sha2(F.col("id").cast("string"), 512))
print(big.filter(F.col("s").startswith("ab")).count())

# COMMAND ----------

# Clean up lab tables
for t in ("tmp_skew_result", "tmp_spill_result", "tmp_lines_small_files"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")