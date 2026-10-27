# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Shared lab configuration
# MAGIC
# MAGIC Every lab notebook starts with `%run ./_config`. It:
# MAGIC 1. Picks a **catalog** (default `interns`) and a **schema** (default: your user name).
# MAGIC 2. Creates the schema and two Unity Catalog **volumes**: `landing` (raw files) and `checkpoints` (streaming state).
# MAGIC 3. Exposes Python variables `catalog`, `schema`, `landing`, `checkpoints` to the calling notebook.
# MAGIC
# MAGIC You never need to edit this notebook; change the widgets at the top of the calling notebook instead.

# COMMAND ----------

# DBTITLE 1,Import answers
# Import answers
import os, sys
sys.path.append(os.path.abspath(".."))
from answers import *

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "interns", "Catalog")
dbutils.widgets.text("schema", "", "Schema (blank = your user name)")

catalog = dbutils.widgets.get("catalog").strip()
_user = spark.sql("SELECT current_user()").first()[0]
schema = dbutils.widgets.get("schema").strip() or re.sub(r"[^a-z0-9_]", "_", _user.split("@")[0].lower())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql("CREATE VOLUME IF NOT EXISTS landing COMMENT 'Raw files dropped by the data generator'")
spark.sql("CREATE VOLUME IF NOT EXISTS checkpoints COMMENT 'Auto Loader schema + streaming checkpoints'")

landing = f"/Volumes/{catalog}/{schema}/landing"
checkpoints = f"/Volumes/{catalog}/{schema}/checkpoints"

# Make the same values usable from SQL cells as ${catalog} / ${schema}-style session variables.
spark.sql(f"DECLARE OR REPLACE VARIABLE lab_landing STRING DEFAULT '{landing}'")

print(f"user        : {_user}")
print(f"catalog     : {catalog}")
print(f"schema      : {schema}")
print(f"landing     : {landing}")
print(f"checkpoints : {checkpoints}")