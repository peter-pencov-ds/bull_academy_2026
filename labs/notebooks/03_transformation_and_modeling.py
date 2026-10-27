# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data transformation and modeling
# MAGIC
# MAGIC **Module:** C3 · **Exam domain:** Data Transformation and Modeling (22%, the largest) · **Time:** 2–3 days
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Clean bronze data into typed, deduplicated silver tables (PySpark **and** SQL)
# MAGIC - Use joins (incl. broadcast), unions, `explode`, window functions and aggregations
# MAGIC - Upsert with `MERGE INTO`
# MAGIC - Choose between tables, views, materialized views and streaming tables for gold
# MAGIC - Use Delta time travel and `RESTORE`
# MAGIC
# MAGIC **Before you start:** `02_ingestion` has loaded batches 1–3 into `bronze_orders` and `bronze_customers`.
# MAGIC
# MAGIC > The exam shows you 4 near-identical code blocks and asks which one is right. Throughout this notebook,
# MAGIC > **read the code before you run it** and predict the output.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_orders = spark.table("bronze_orders")
bronze_customers = spark.table("bronze_customers")
print(bronze_orders.count(), "bronze orders rows ·", bronze_customers.count(), "bronze customers rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Profile before you clean
# MAGIC Never clean blind. Count the problems first so you can prove you fixed them.

# COMMAND ----------

display(bronze_orders.select(
    F.count("*").alias("rows"),
    F.countDistinct("order_id").alias("distinct_orders"),
    F.approx_count_distinct("customer_id").alias("approx_customers"),   # HyperLogLog: fast, ~5% error
    F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer"),
    F.min("order_ts").alias("first_ts"), F.max("order_ts").alias("last_ts"),
))

# COMMAND ----------

display(bronze_customers.summary())   # count, mean, stddev, min, quartiles, max per column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Silver customers: keep the latest version of each customer
# MAGIC Batch 1 has exact duplicate rows; batch 2 has **updated** versions of 60 customers.
# MAGIC `dropDuplicates()` removes exact copies but not older versions — for that we need a **window function**.

# COMMAND ----------

latest_first = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

silver_customers = (bronze_customers
    .withColumn("rn", F.row_number().over(latest_first))
    .filter("rn = 1")
    .select(
        "customer_id",
        F.initcap(F.trim("name")).alias("name"),
        F.nullif(F.lower(F.trim("email")), F.lit("")).alias("email"),   # empty string -> NULL
        F.upper("country").alias("country"),
        "region",
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.coalesce("loyalty_tier", F.lit("none")).alias("loyalty_tier"),
    ))

silver_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_customers")
print(spark.table("silver_customers").count(), "customers (expect 540)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise B1 — same thing in SQL
# MAGIC Write a `CREATE OR REPLACE TABLE silver_customers_sql AS ...` that produces the same result.
# MAGIC Hint: `QUALIFY row_number() OVER (...) = 1` filters on a window function without a subquery.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Silver orders and order lines
# MAGIC Problems to fix: duplicate orders, orphan orders (null customer), and order lines whose price arrived as text.
# MAGIC Lines with a missing price fall back to the product's list price.

# COMMAND ----------

products = spark.table("bronze_products_copy")

silver_orders = (bronze_orders
    .dropDuplicates(["order_id"])
    .filter(F.col("customer_id").isNotNull())
    .select("order_id", "customer_id",
            F.to_timestamp("order_ts").alias("order_ts"),
            "status",
            F.col("shipping.country").alias("ship_country"),
            F.col("shipping.express").alias("express"),
            "items"))

silver_order_lines = (silver_orders
    .select("order_id", "order_ts", F.posexplode("items").alias("line_no", "item"))
    .select("order_id", "order_ts", "line_no", "item.product_id", "item.qty", "item.unit_price")
    .join(F.broadcast(products.select("product_id", "list_price", "category")), "product_id", "left")
    .withColumn("price_fixed", F.col("unit_price").isNull())
    .withColumn("unit_price", F.coalesce("unit_price", "list_price"))
    .withColumn("line_amount", F.round(F.col("qty") * F.col("unit_price"), 2))
    .drop("list_price"))

silver_orders.drop("items").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
silver_order_lines.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_order_lines")

display(spark.sql("SELECT count(*) AS orders FROM silver_orders"))
display(spark.sql("SELECT price_fixed, count(*) AS lines FROM silver_order_lines GROUP BY ALL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why `F.broadcast(products)`?
# MAGIC A normal join shuffles **both** sides across the cluster. Broadcasting copies the small table to every executor,
# MAGIC so the big table never moves. Spark does this automatically under `spark.sql.autoBroadcastJoinThreshold` (10 MB by default);
# MAGIC the hint forces it. Run `.explain()` on `silver_order_lines` and find `BroadcastHashJoin`.

# COMMAND ----------

silver_order_lines.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — Joins and unions: the exam's favourite traps
# MAGIC
# MAGIC | You write | You get |
# MAGIC | --- | --- |
# MAGIC | `a.join(b, "id")` | inner join, one `id` column |
# MAGIC | `a.join(b, a.id == b.id)` | inner join, **two** `id` columns (ambiguous later!) |
# MAGIC | `a.join(b, "id", "left")` | all rows of `a`, nulls where `b` has no match |
# MAGIC | `a.join(b, "id", "left_anti")` | rows of `a` with **no** match in `b` |
# MAGIC | `a.crossJoin(b)` | every combination (rows a × rows b) |
# MAGIC | PySpark `a.union(b)` / `a.unionAll(b)` | identical: both **keep duplicates**, match columns **by position** |
# MAGIC | PySpark `a.unionByName(b, allowMissingColumns=True)` | match by **name**, fill missing with null |
# MAGIC | SQL `UNION` vs `UNION ALL` | `UNION` **removes** duplicates, `UNION ALL` keeps them |

# COMMAND ----------

a = spark.createDataFrame([(1, "x"), (2, "y")], "id INT, v STRING")
b = spark.createDataFrame([("y", 2), ("z", 3)], "v STRING, id INT")   # same columns, different order!

# this one will fail, explain why
# print("union (by position):");      a.union(b).show()

# this one will succeed, explain why
# print("unionByName:");              a.unionByName(b).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise D1
# MAGIC Using a join, find customers in `silver_customers` who have **never** placed an order. How many are there?

# COMMAND ----------

# TODO


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E — Incremental upserts with `MERGE INTO`
# MAGIC Overwriting silver every run doesn't scale. `MERGE` updates matched rows and inserts new ones in one atomic transaction.
# MAGIC Below we simulate a small update feed and merge it into `silver_customers`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_updates AS
SELECT * FROM VALUES
  ('C00001', 'gold',   current_timestamp()),
  ('C09999', 'silver', current_timestamp())
AS t(customer_id, loyalty_tier, updated_at)
""")

display(spark.sql("""
MERGE INTO silver_customers AS t
USING customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN
  UPDATE SET t.loyalty_tier = s.loyalty_tier, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, loyalty_tier, updated_at) VALUES (s.customer_id, s.loyalty_tier, s.updated_at)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part F — Gold: pick the right object
# MAGIC
# MAGIC | Object | Stores data? | Refresh | Use when |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Table** | Yes | You write to it | You control the logic and the schedule |
# MAGIC | **View** | No | Computed at every query | Light logic, always-fresh, cheap to define |
# MAGIC | **Temp view** | No | Session only | Scratch work inside one notebook |
# MAGIC | **Materialized view** | Yes (precomputed) | Refreshed by Databricks, incrementally when possible | BI on expensive aggregations |
# MAGIC | **Streaming table** | Yes | Processes only new input rows | Append-heavy incremental ingestion |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_sales AS
# MAGIC SELECT date(o.order_ts)          AS order_date,
# MAGIC        o.ship_country,
# MAGIC        count(DISTINCT o.order_id) AS orders,
# MAGIC        round(sum(l.line_amount), 2) AS revenue
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_order_lines l USING (order_id)
# MAGIC WHERE o.status <> 'cancelled'
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_top_categories AS
# MAGIC SELECT category, round(sum(line_amount), 2) AS revenue
# MAGIC FROM silver_order_lines
# MAGIC GROUP BY category
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise F1 — materialized view
# MAGIC Create `gold_customer_ltv_mv` as a **materialized view**: lifetime revenue and order count per customer, with region and loyalty tier.
# MAGIC Materialized views need serverless compute or a SQL warehouse. Then run `REFRESH MATERIALIZED VIEW gold_customer_ltv_mv`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part G — Time travel and RESTORE
# MAGIC Every Delta write creates a version. You can query, compare and roll back.

# COMMAND ----------

spark.sql("DELETE FROM gold_daily_sales WHERE ship_country = 'AT'")   # oops
display(spark.sql("DESCRIBE HISTORY gold_daily_sales").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'now' AS v, count(*) FROM gold_daily_sales
# MAGIC UNION ALL
# MAGIC SELECT 'v0', count(*) FROM gold_daily_sales VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE gold_daily_sales TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part H — Tuning knobs you must recognize by name
# MAGIC
# MAGIC | Config | Controls | Default |
# MAGIC | --- | --- | --- |
# MAGIC | `spark.sql.shuffle.partitions` | Partitions after a shuffle (joins, groupBy) | 200 (AQE coalesces automatically) |
# MAGIC | `spark.default.parallelism` | Default partitions for RDD operations | total executor cores |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | Max size of a table Spark broadcasts automatically | 10 MB |
# MAGIC | `spark.executor.memory` / `spark.driver.memory` | Memory per executor / driver JVM | cluster dependent |
# MAGIC | `spark.sql.adaptive.enabled` | Adaptive Query Execution (re-plans at runtime, fixes skew) | true |
# MAGIC
# MAGIC Serverless compute manages most of these for you; on classic clusters you set them in the cluster's Spark config.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. `df.union(other)` where both have columns `(id, name)` but `other` lists them as `(name, id)`. Result?
# MAGIC    A) Spark matches by name B) Values end up in the wrong columns (or a type error) C) Duplicates are removed D) Error always
# MAGIC 2. Which keeps only the most recent row per `customer_id`? A) `dropDuplicates(["customer_id"])` B) `row_number()` over `partitionBy(customer_id).orderBy(updated_at desc)` = 1 C) `distinct()` D) `groupBy(customer_id).count()`
# MAGIC 3. A BI dashboard runs the same heavy aggregation 200 times a day on data that changes hourly. Best gold object? A) View B) Temp view C) Materialized view D) External table
# MAGIC 4. `approx_count_distinct` is preferred over `countDistinct` when: A) exact counts are legally required B) data is huge and ~5% error is acceptable C) the column has nulls D) never
# MAGIC
# MAGIC ### See hidden cell for answers

# COMMAND ----------

# 1 B · 2 B · 3 C · 4 B

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION — B1
# MAGIC CREATE OR REPLACE TABLE silver_customers_sql AS
# MAGIC SELECT customer_id,
# MAGIC        initcap(trim(name))                 AS name,
# MAGIC        nullif(lower(trim(email)), '')      AS email,
# MAGIC        upper(country)                      AS country,
# MAGIC        region,
# MAGIC        to_date(signup_date)                AS signup_date,
# MAGIC        to_timestamp(updated_at)            AS updated_at,
# MAGIC        coalesce(loyalty_tier, 'none')      AS loyalty_tier
# MAGIC FROM bronze_customers
# MAGIC QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;

# COMMAND ----------

# SOLUTION — D1
never_ordered = spark.table("silver_customers").join(spark.table("silver_orders"), "customer_id", "left_anti")
print(never_ordered.count(), "customers never ordered")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION — F1
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW gold_customer_ltv_mv AS
# MAGIC SELECT c.customer_id, c.region, c.loyalty_tier,
# MAGIC        count(DISTINCT o.order_id)    AS orders,
# MAGIC        round(sum(l.line_amount), 2)  AS lifetime_revenue
# MAGIC FROM silver_customers c
# MAGIC JOIN silver_orders o      ON o.customer_id = c.customer_id
# MAGIC JOIN silver_order_lines l ON l.order_id = o.order_id
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC REFRESH MATERIALIZED VIEW gold_customer_ltv_mv;