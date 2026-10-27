# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 02 · Data ingestion and loading
# MAGIC
# MAGIC **Module:** C2 · **Exam domain:** Data Ingestion and Loading (21%) · **Time:** 2 days
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Load files incrementally and idempotently with `COPY INTO`
# MAGIC - Build an Auto Loader stream with schema inference, schema evolution and rescued data
# MAGIC - Explain directory listing vs file notification mode
# MAGIC - Choose between `COPY INTO`, Auto Loader and Lakeflow Connect for a given scenario
# MAGIC - Flatten nested JSON into bronze-ready columns
# MAGIC
# MAGIC **Before you start:** `00_setup` has landed batch 1.
# MAGIC
# MAGIC 📚 **Key references for this module:**
# MAGIC [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) · [Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) · [Lakeflow Connect](https://docs.databricks.com/en/connect/index.html) · [Structured Streaming Programming Guide (Spark)](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_data_generator

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %md
# MAGIC ## Medallion architecture in one table
# MAGIC
# MAGIC | Layer | Contains | Rules |
# MAGIC | --- | --- | --- |
# MAGIC | **Bronze** | Raw data as it arrived + ingestion metadata (file name, load time) | Append only; never "fix" data here |
# MAGIC | **Silver** | Cleaned, typed, deduplicated, conformed | One row per business entity/event |
# MAGIC | **Gold** | Aggregates and models for a specific consumer | Shaped for BI / ML / apps |
# MAGIC
# MAGIC In this notebook we build **bronze** tables only.
# MAGIC
# MAGIC 📚 [Medallion lakehouse architecture](https://docs.databricks.com/en/lakehouse/medallion.html)

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %md
# MAGIC ## Part A — `COPY INTO`
# MAGIC `COPY INTO` is a SQL command that loads files from a path into a Delta table and **remembers which files it already loaded**.
# MAGIC Rerunning it is safe (idempotent). Good for: thousands of files, scheduled batch loads, SQL-first teams.
# MAGIC
# MAGIC 📚 [COPY INTO reference](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) · [COPY INTO examples & patterns](https://docs.databricks.com/en/ingestion/cloud-object-storage/copy-into/examples.html) · [FORMAT_OPTIONS & COPY_OPTIONS](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html#format_options)

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS bronze_products_copy")   # schemaless placeholder: COPY INTO will set the schema

spark.sql(f"""
COPY INTO bronze_products_copy
FROM '{landing}/products'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""").display()

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md
# MAGIC ### Exercise A1 — prove idempotency
# MAGIC Run the previous cell again. How many rows were inserted the second time? Why?
# MAGIC
# MAGIC *Your answer:*
# MAGIC
# MAGIC 📚 [COPY INTO — COPY_OPTIONS](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html#copy_options) (note the `force` option that controls idempotency)

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %md
# MAGIC ### Exercise A2 — load customers with `COPY INTO`
# MAGIC Load `{landing}/customers` into `bronze_customers_copy`. Add two metadata columns using a `SELECT` inside `COPY INTO`:
# MAGIC `_metadata.file_name AS source_file` and `current_timestamp() AS ingested_at`.
# MAGIC
# MAGIC Hint: `COPY INTO t FROM (SELECT *, ... FROM '<path>') FILEFORMAT = CSV ...`
# MAGIC
# MAGIC 📚 [COPY INTO with SELECT (syntax)](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html#syntax) · [File metadata column (`_metadata`)](https://docs.databricks.com/en/ingestion/file-metadata-column.html)

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %md
# MAGIC ## Part B — Auto Loader (`cloudFiles`)
# MAGIC Auto Loader is a **Structured Streaming source** that discovers new files and processes each exactly once,
# MAGIC tracking progress in a checkpoint. Good for: millions of files, continuous or frequent arrival, evolving schemas.
# MAGIC
# MAGIC Key options:
# MAGIC
# MAGIC | Option | What it does |
# MAGIC | --- | --- |
# MAGIC | `cloudFiles.format` | `json`, `csv`, `parquet`, `avro`, `text`, `binaryFile`, ... |
# MAGIC | `cloudFiles.schemaLocation` | Where the inferred schema is stored and versioned |
# MAGIC | `cloudFiles.inferColumnTypes` | Infer real types for JSON/CSV (otherwise everything is `STRING`) |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` (default), `rescue`, `failOnNewColumns`, `none` |
# MAGIC | `cloudFiles.schemaHints` | Force types for specific columns, e.g. `"order_ts TIMESTAMP"` |
# MAGIC | `cloudFiles.useNotifications` | `false` = directory listing (default, simple); `true` = file notification via cloud queue (scales to huge directories) |
# MAGIC
# MAGIC 📚 [Auto Loader overview](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) · [All Auto Loader options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/options.html) · [Schema inference & evolution](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html) · [File detection modes (directory listing vs notification)](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/file-detection-modes.html)

# COMMAND ----------

from pyspark.sql import functions as F

def autoload(source: str, fmt: str, target: str, extra_options: dict | None = None):
    """Incrementally load every new file under landing/<source> into <target>, then stop."""
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{checkpoints}/{target}/_schema")
              .option("cloudFiles.inferColumnTypes", "true"))
    for k, v in (extra_options or {}).items():
        reader = reader.option(k, v)
    query = (reader.load(f"{landing}/{source}")
             .withColumn("source_file", F.col("_metadata.file_path"))
             .withColumn("ingested_at", F.current_timestamp())
             .writeStream
             .option("checkpointLocation", f"{checkpoints}/{target}/_checkpoint")
             .option("mergeSchema", "true")
             .trigger(availableNow=True)          # process what's there, then stop (batch-style streaming)
             .toTable(target))
    query.awaitTermination()
    print(f"{target}: {spark.table(target).count()} rows")

autoload("orders", "json", "bronze_orders")

# COMMAND ----------

display(spark.table("bronze_orders").limit(5))
spark.table("bronze_orders").printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 13
# MAGIC %md
# MAGIC Notice the `_rescued_data` column. Auto Loader puts anything that doesn't fit the schema there instead of dropping it.
# MAGIC Now let's load customers the same way.
# MAGIC
# MAGIC 📚 [Rescued data column](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html#rescued-data-column)

# COMMAND ----------

autoload("customers", "csv", "bronze_customers", {"header": "true"})

# COMMAND ----------

# DBTITLE 1,Cell 15
# MAGIC %md
# MAGIC ## Part C — Schema evolution in action
# MAGIC Batch 2 adds a new column `loyalty_tier` to the customer files. Land it, then rerun the customer stream.
# MAGIC
# MAGIC **Expected:** the first run **fails** with an `UnknownFieldException`. This is by design in `addNewColumns` mode:
# MAGIC Auto Loader records the new schema and stops so you notice. **Run the cell again** and it succeeds with the new column.
# MAGIC (In production, job retries handle this automatically.)
# MAGIC
# MAGIC 📚 [Schema evolution with Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html#schema-evolution) · [Delta schema evolution (`mergeSchema`)](https://docs.databricks.com/en/delta/update-schema.html)

# COMMAND ----------

land_batch(2, landing)

# COMMAND ----------

autoload("customers", "csv", "bronze_customers", {"header": "true"})   # run me twice

# COMMAND ----------

display(spark.sql("SELECT loyalty_tier, count(*) AS n FROM bronze_customers GROUP BY ALL"))

# COMMAND ----------

autoload("orders", "json", "bronze_orders")

# COMMAND ----------

# DBTITLE 1,Cell 20
# MAGIC %md
# MAGIC ### Exercise C1 — rescued data
# MAGIC Land batch 3 and load it into `bronze_orders`. Batch 3 contains prices written as strings like `"12,50"`.
# MAGIC Write a query that finds the affected rows. Look in `_rescued_data` **and** check whether any `unit_price` came through as null.
# MAGIC Then answer: what would `cloudFiles.schemaEvolutionMode = rescue` change?
# MAGIC
# MAGIC 📚 [Rescued data column](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html#rescued-data-column) · [schemaEvolutionMode options](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/schema.html#schema-evolution)

# COMMAND ----------

# TODO: land_batch(3, landing), autoload(...), then your query


# COMMAND ----------

# DBTITLE 1,Cell 22
# MAGIC %md
# MAGIC ## Part D — Nested JSON
# MAGIC `bronze_orders.items` is an `ARRAY<STRUCT<...>>` and `shipping` is a `STRUCT`. Access struct fields with dots; turn arrays into rows with `explode`.
# MAGIC
# MAGIC 📚 [explode function (Databricks SQL)](https://docs.databricks.com/en/sql/language-manual/functions/explode.html) · [pyspark.sql.functions.explode (Spark)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) · [Struct type](https://docs.databricks.com/en/sql/language-manual/data-types/struct-type.html)

# COMMAND ----------

order_lines = (spark.table("bronze_orders")
               .select("order_id", "customer_id",
                       F.col("shipping.country").alias("ship_country"),
                       F.explode("items").alias("item"))
               .select("*", "item.*").drop("item"))
display(order_lines.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 24
# MAGIC %md
# MAGIC The same in SQL — note the `:` path syntax also works on JSON **strings**, and `explode` / `LATERAL VIEW` / `inline` all flatten arrays.
# MAGIC
# MAGIC 📚 [LATERAL VIEW (Spark SQL)](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html) · [Semi-structured data access (: syntax)](https://docs.databricks.com/en/sql/language-manual/sql-ref-json-path-expression.html) · [inline function](https://docs.databricks.com/en/sql/language-manual/functions/inline.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, shipping.country AS ship_country, item.product_id, item.qty
# MAGIC FROM bronze_orders
# MAGIC LATERAL VIEW explode(items) AS item
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Cell 26
# MAGIC %md
# MAGIC ## Part E — Other ingestion paths (read, then discuss with your mentor)
# MAGIC
# MAGIC | Need | Tool |
# MAGIC | --- | --- |
# MAGIC | Files in cloud storage, SQL users, batch | `COPY INTO` |
# MAGIC | Files in cloud storage, many/continuous files, schema drift | Auto Loader (in notebooks or declarative pipelines) |
# MAGIC | SaaS apps and databases (Salesforce, Workday, SQL Server, ...) with no code | **Lakeflow Connect managed connectors** |
# MAGIC | Message buses, file sources, custom endpoints with some code | **Lakeflow Connect standard connectors** / Structured Streaming (Kafka, Kinesis, Event Hubs) |
# MAGIC | A one-off pull from an API or JDBC source | Notebook with `spark.read.jdbc(...)` or `requests`, landing to a volume or table, scheduled by Lakeflow Jobs |
# MAGIC | Ad-hoc upload of a CSV | **Add data** UI → creates a table |
# MAGIC
# MAGIC 📚 [COPY INTO](https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html) · [Auto Loader](https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html) · [Lakeflow Connect managed connectors](https://docs.databricks.com/en/connect/index.html) · [Structured Streaming + Kafka](https://docs.databricks.com/en/structured-streaming/kafka.html) · [spark.read.jdbc (Spark)](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) · [Add data UI](https://docs.databricks.com/en/ingestion/add-data/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. You rerun `COPY INTO` on the same directory with no new files. What happens? A) Duplicates are inserted B) It fails C) 0 rows are loaded D) The table is overwritten
# MAGIC 2. A directory receives millions of files per day and listing has become slow. Which Auto Loader setting helps? A) `cloudFiles.inferColumnTypes` B) file notification mode C) `mergeSchema` D) `trigger(once=True)`
# MAGIC 3. In `addNewColumns` mode, a new column appears. What does the stream do? A) Silently drops the column B) Stops with an exception after updating the schema; restart picks it up C) Writes it to `_rescued_data` forever D) Fails permanently
# MAGIC 4. Which column can you select to get the source file of each row? A) `input_file()` B) `_metadata.file_path` C) `_source` D) `cloudFiles.path`
# MAGIC
# MAGIC ### See hidden cell for answers

# COMMAND ----------

# 1 C · 2 B · 3 B · 4 B

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)

# COMMAND ----------

# SOLUTION — A2
spark.sql("CREATE TABLE IF NOT EXISTS bronze_customers_copy")
spark.sql(f"""
COPY INTO bronze_customers_copy
FROM (SELECT *, _metadata.file_name AS source_file, current_timestamp() AS ingested_at
      FROM '{landing}/customers')
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
""").display()

# COMMAND ----------

# SOLUTION — C1
land_batch(3, landing)
autoload("orders", "json", "bronze_orders")
display(spark.sql("""
  SELECT order_id, source_file, _rescued_data, items
  FROM bronze_orders
  WHERE _rescued_data IS NOT NULL
     OR exists(items, i -> i.unit_price IS NULL)
"""))
# With schemaEvolutionMode = rescue the schema never changes: new columns also go to _rescued_data and the stream never stops.