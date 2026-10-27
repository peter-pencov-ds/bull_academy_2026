# Databricks notebook source
# MAGIC %md
# MAGIC ### Job task: land a batch of raw files
# MAGIC Parameter `batch` (1, 2 or 3). Landing the same batch twice is harmless: Auto Loader ignores files it has already seen.

# COMMAND ----------

# MAGIC %run ../_config

# COMMAND ----------

# MAGIC %run ../_data_generator

# COMMAND ----------

dbutils.widgets.text("batch", "1")
result = land_batch(int(dbutils.widgets.get("batch")), landing)
dbutils.jobs.taskValues.set(key="files_landed", value=len(result["files"]))