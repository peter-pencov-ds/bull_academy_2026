# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Cleanup
# MAGIC Drops **everything** in your lab schemas (tables, views, volumes incl. landing files and checkpoints) so you can restart from `00_setup`.
# MAGIC
# MAGIC If you deployed the bundle, remove its job and pipeline first from a terminal: `databricks bundle destroy`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("confirm", "no", ["no", "yes"], "Really drop everything?")

# COMMAND ----------

if dbutils.widgets.get("confirm") != "yes":
    dbutils.notebook.exit("Set the 'confirm' widget to 'yes' to drop your lab schemas.")

for s in (schema, f"{schema}_lakeflow", f"{schema}_qa"):
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{s} CASCADE")
    print(f"dropped {catalog}.{s}")