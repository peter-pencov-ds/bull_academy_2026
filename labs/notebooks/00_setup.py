# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — run this once before any lab
# MAGIC
# MAGIC **Time:** 10 min · **Compute:** any (serverless is fine)
# MAGIC
# MAGIC This notebook creates your personal schema and volumes, then lands **batch 1** of the
# MAGIC Northwind Outdoors dataset. Later labs tell you when to land batches 2 and 3.
# MAGIC
# MAGIC > Mentors: set the `catalog` widget to the program catalog. Interns leave `schema` blank.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_data_generator

# COMMAND ----------

land_batch(1, landing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check what landed
# MAGIC Volumes behave like folders. `dbutils.fs.ls`, `%fs ls` and plain Python `os.listdir` all work on `/Volumes/...` paths.

# COMMAND ----------

for sub in ("products", "customers", "orders"):
    display(dbutils.fs.ls(f"{landing}/{sub}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Peek at raw content without creating a table — `read_files` reads files directly with SQL.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM read_files('{landing}/orders', format => 'json') LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starting over
# MAGIC If a lab goes wrong and you want a clean slate, uncomment and run the next cell, then run `99_cleanup`.

# COMMAND ----------

# reset_landing(landing)