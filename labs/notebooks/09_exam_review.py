# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 · Exam review — read the code, predict, then prove it
# MAGIC
# MAGIC **Week 8** · **Time:** 2 h per pass
# MAGIC
# MAGIC The real exam is 45 questions in 90 minutes, and many of them show near-identical code blocks.
# MAGIC Each question below works the same way:
# MAGIC 1. **Predict** the answer without running anything (write it in the markdown cell).
# MAGIC 2. **Run** the proof cell and check.
# MAGIC 3. Wrong? Log it in your error log with the domain, then re-read that notebook's section.
# MAGIC
# MAGIC Questions are original practice items written for this program, not real exam questions.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
df = spark.createDataFrame(
    [("C1", "EU", 10.0), ("C1", "EU", 10.0), ("C2", "NA", 5.0), ("C3", None, 7.5), ("C2", "NA", 2.5)],
    "customer_id STRING, region STRING, amount DOUBLE")
df.createOrReplaceTempView("t")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1 · Transformation — How many rows does each return?
# MAGIC ```python
# MAGIC A) df.distinct().count()
# MAGIC B) df.dropDuplicates(["customer_id"]).count()
# MAGIC C) df.union(df).count()
# MAGIC D) spark.sql("SELECT * FROM t UNION SELECT * FROM t").count()
# MAGIC ```
# MAGIC *Prediction:* A = ? · B = ? · C = ? · D = ?

# COMMAND ----------

print(df.distinct().count(), df.dropDuplicates(["customer_id"]).count(),
      df.union(df).count(), spark.sql("SELECT * FROM t UNION SELECT * FROM t").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2 · Transformation — Which block returns **3** for EU+NA+null groups and counts the null region as its own group?
# MAGIC ```python
# MAGIC A) df.groupBy("region").count().count()
# MAGIC B) df.filter("region IS NOT NULL").groupBy("region").count().count()
# MAGIC C) df.select(F.countDistinct("region")).first()[0]
# MAGIC D) df.select(F.approx_count_distinct("region")).first()[0]
# MAGIC ```
# MAGIC *Prediction:*

# COMMAND ----------

print(df.groupBy("region").count().count(),
      df.filter("region IS NOT NULL").groupBy("region").count().count(),
      df.select(F.countDistinct("region")).first()[0],
      df.select(F.approx_count_distinct("region")).first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3 · Transformation — `sum` vs `count` vs `count_distinct`
# MAGIC Which expression gives the number of **distinct customers per region**?
# MAGIC ```python
# MAGIC A) df.groupBy("region").agg(F.sum("customer_id"))
# MAGIC B) df.groupBy("region").agg(F.count("customer_id"))
# MAGIC C) df.groupBy("region").agg(F.count_distinct("customer_id"))
# MAGIC D) df.groupBy("customer_id").agg(F.count("region"))
# MAGIC ```
# MAGIC *Prediction:*

# COMMAND ----------

display(df.groupBy("region").agg(F.count("customer_id").alias("B"), F.count_distinct("customer_id").alias("C")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4 · Delta — After these statements, what does `VERSION AS OF 1` contain?
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE q4 (id INT);   -- v0
# MAGIC INSERT INTO q4 VALUES (1), (2);        -- v1
# MAGIC DELETE FROM q4 WHERE id = 1;           -- v2
# MAGIC INSERT INTO q4 VALUES (3);             -- v3
# MAGIC ```
# MAGIC A) 1, 2 · B) 2 · C) 2, 3 · D) empty
# MAGIC
# MAGIC *Prediction:*

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE q4 (id INT)")
spark.sql("INSERT INTO q4 VALUES (1), (2)")
spark.sql("DELETE FROM q4 WHERE id = 1")
spark.sql("INSERT INTO q4 VALUES (3)")
display(spark.sql("SELECT * FROM q4 VERSION AS OF 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5 · MERGE — What is in `q5_target` afterwards?
# MAGIC ```sql
# MAGIC target: (1,'a'), (2,'b')        source: (2,'B'), (3,'C')
# MAGIC MERGE INTO q5_target t USING q5_source s ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET t.v = s.v
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE
# MAGIC ```
# MAGIC A) (1,a),(2,B),(3,C) · B) (2,B) · C) (2,B),(3,C) · D) (1,a),(2,B)
# MAGIC
# MAGIC *Prediction:*

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE q5_target AS SELECT * FROM VALUES (1,'a'),(2,'b') AS x(id, v)")
spark.sql("CREATE OR REPLACE TEMP VIEW q5_source AS SELECT * FROM VALUES (2,'B'),(3,'C') AS x(id, v)")
spark.sql("""MERGE INTO q5_target t USING q5_source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET t.v = s.v
             WHEN NOT MATCHED BY SOURCE THEN DELETE""")
display(spark.table("q5_target").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6 · Ingestion — scenario
# MAGIC A team receives ~50 files a day into a volume, loads them nightly with SQL only, and must never load a file twice. Simplest correct tool?
# MAGIC A) Auto Loader with file notifications · B) `COPY INTO` · C) `INSERT INTO ... SELECT * FROM read_files(...)` · D) Lakeflow Connect managed connector
# MAGIC
# MAGIC ### Q7 · Ingestion — scenario
# MAGIC JSON files arrive continuously; producers add new fields without warning; the pipeline must not stop. Which Auto Loader `schemaEvolutionMode`?
# MAGIC A) `addNewColumns` · B) `rescue` · C) `failOnNewColumns` · D) `none`
# MAGIC
# MAGIC ### Q8 · Pipelines
# MAGIC `CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW`. 1,000 rows arrive, 12 have `amount <= 0`. Result?
# MAGIC A) Update fails · B) 1,000 rows written, 12 flagged · C) 988 rows written, 12 counted as dropped · D) 12 rows go to a quarantine table automatically
# MAGIC
# MAGIC ### Q9 · Jobs
# MAGIC Task `notify` must run whenever `load` **or** `transform` fails, but not when both succeed. Run if = ?
# MAGIC A) All done · B) At least one failed · C) None failed · D) All failed
# MAGIC
# MAGIC ### Q10 · CI/CD
# MAGIC In `databricks.yml`, the `prod` target sets `variables: {catalog: prod}`. The job YAML uses `${var.catalog}`. `databricks bundle deploy -t prod --var="catalog=prod_hotfix"` deploys to:
# MAGIC A) `prod` · B) `prod_hotfix` · C) error: conflicting values · D) the variable's default
# MAGIC
# MAGIC ### Q11 · Troubleshooting
# MAGIC A job ran in 12 minutes for a month; this week it takes 55. The Spark UI shows one task in the join stage running 40 minutes while the rest finish in seconds. First suspicion?
# MAGIC A) Driver OOM · B) Data skew on the join key · C) Cluster start failure · D) Too many shuffle partitions
# MAGIC
# MAGIC ### Q12 · Governance
# MAGIC Which statement lets group `analysts` read **every current and future table** in schema `sales.gold`, assuming they already have `USE CATALOG`?
# MAGIC A) `GRANT SELECT ON TABLE sales.gold.* TO analysts` · B) `GRANT USE SCHEMA, SELECT ON SCHEMA sales.gold TO analysts` · C) `GRANT ALL PRIVILEGES ON CATALOG sales TO analysts` · D) `GRANT READ ON SCHEMA sales.gold TO analysts`
# MAGIC
# MAGIC *Predictions Q6–Q12:*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer key
# MAGIC <details><summary>Show answers</summary>
# MAGIC
# MAGIC | Q | Answer | Why |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | A 4 · B 3 · C 10 · D 4 | `distinct` drops the exact duplicate; PySpark `union` keeps duplicates; SQL `UNION` removes them |
# MAGIC | 2 | A | `groupBy` keeps null as a group; `countDistinct` ignores nulls (returns 2) |
# MAGIC | 3 | C | `count` counts rows (EU = 2), `count_distinct` counts customers (EU = 1); `sum` on strings is meaningless |
# MAGIC | 4 | A | Version 1 is right after the first insert |
# MAGIC | 5 | B | Row 1 exists only in target → deleted; row 3 exists only in source and there's no `WHEN NOT MATCHED` → not inserted |
# MAGIC | 6 | B | Idempotent, SQL, file tracking built in; C would reload everything each night |
# MAGIC | 7 | B | `rescue` never changes the schema and never stops; new fields go to `_rescued_data` |
# MAGIC | 8 | C | `DROP ROW` drops and counts; nothing is quarantined unless you build it |
# MAGIC | 9 | B | |
# MAGIC | 10 | B | Command-line `--var` overrides target values |
# MAGIC | 11 | B | One straggler task in a join stage is the signature of skew |
# MAGIC | 12 | B | Privileges on a schema are inherited by all tables in it, including future ones |
# MAGIC </details>

# COMMAND ----------

for t in ("q4", "q5_target"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")