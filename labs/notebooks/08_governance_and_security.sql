-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 · Governance and security with Unity Catalog
-- MAGIC
-- MAGIC **Module:** C7 · **Exam domain:** Governance and Security (15%) · **Time:** 1 day
-- MAGIC
-- MAGIC **You will be able to:**
-- MAGIC - Explain the securable hierarchy and which privileges are needed at each level
-- MAGIC - `GRANT` and `REVOKE` privileges to users, groups and service principals, and audit them with `SHOW GRANTS`
-- MAGIC - Tell managed from external tables (and volumes), and what `DROP` does to each
-- MAGIC - Apply **row filters** and **column masks**, and explain **ABAC** policies
-- MAGIC - Use lineage and the audit system table to answer "who touched this?"
-- MAGIC
-- MAGIC **Work in pairs.** You'll grant access to your buddy and check what they see.
-- MAGIC
-- MAGIC > **Mentor prerequisites:** every intern has `USE CATALOG` on the program catalog; an account group `interns_analysts` exists and contains every intern's *buddy*; interns are **not** in `admins`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "interns")
-- MAGIC dbutils.widgets.text("schema", "")
-- MAGIC dbutils.widgets.text("buddy", "", "Buddy's e-mail")
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC buddy = dbutils.widgets.get("buddy")
-- MAGIC
-- MAGIC print(catalog)
-- MAGIC print(schema)
-- MAGIC print(buddy)

-- COMMAND ----------

-- MAGIC %run ./_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part A — The hierarchy and the privileges you need
-- MAGIC
-- MAGIC ```
-- MAGIC metastore
-- MAGIC  └── catalog            USE CATALOG
-- MAGIC       └── schema        USE SCHEMA, CREATE TABLE, CREATE VOLUME, ...
-- MAGIC            ├── table/view       SELECT, MODIFY
-- MAGIC            ├── volume           READ VOLUME, WRITE VOLUME
-- MAGIC            └── function         EXECUTE
-- MAGIC ```
-- MAGIC
-- MAGIC To **read one table** a user needs all three: `USE CATALOG` on the catalog, `USE SCHEMA` on the schema, `SELECT` on the table.
-- MAGIC Privileges granted on a parent are **inherited** by everything inside it. Unity Catalog is deny-by-default: no grant, no access.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schema}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part B — Grant your buddy read access to one gold table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC buddy = dbutils.widgets.get("buddy")
-- MAGIC assert buddy, "Fill in the 'buddy' widget with your buddy's e-mail first"
-- MAGIC spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{buddy}`")
-- MAGIC spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.gold_daily_sales TO `{buddy}`")
-- MAGIC display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.{schema}.gold_daily_sales"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise B1
-- MAGIC 1. Ask your buddy to run `SELECT * FROM <catalog>.<your schema>.gold_daily_sales LIMIT 5`. Works?
-- MAGIC 2. Ask them to query `silver_orders`. What error do they get, and why?
-- MAGIC 3. Revoke their `SELECT` and have them retry. Write the `REVOKE` statement below.

-- COMMAND ----------

-- TODO: REVOKE ...

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### About `DENY`
-- MAGIC Unity Catalog has **no `DENY`**: access is simply absent until granted, and `REVOKE` removes a grant.
-- MAGIC `DENY` exists in the **legacy Hive metastore table ACLs**. If you meet it in practice questions, check which governance model the question is about.
-- MAGIC *(Mentors: re-check this against the current official exam guide before teaching.)*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part C — Managed vs external
-- MAGIC
-- MAGIC | | Managed table / volume | External table / volume |
-- MAGIC | --- | --- | --- |
-- MAGIC | Where files live | Storage Unity Catalog manages for the catalog/schema | A path **you** specify in an *external location* |
-- MAGIC | `DROP TABLE` | Deletes metadata **and** data (recoverable with `UNDROP` for a limited period) | Deletes metadata only; files stay |
-- MAGIC | Predictive optimization, auto compaction | Yes | No |
-- MAGIC | Use when | Default for almost everything | Data shared with non-Databricks tools that need a fixed path |

-- COMMAND ----------

DESCRIBE TABLE EXTENDED gold_daily_sales;
-- look for: Type = MANAGED, Location = managed storage path

-- COMMAND ----------

-- MAGIC %md
-- MAGIC An external table needs an **external location** (a storage credential + path) that an admin has created and granted you `CREATE EXTERNAL TABLE` on.
-- MAGIC Read-only example for discussion; don't run unless your mentor gives you a location:
-- MAGIC ```sql
-- MAGIC CREATE TABLE ext_orders
-- MAGIC LOCATION 'abfss://lab@<storage>.dfs.core.windows.net/ext/orders'   -- or s3://..., gs://...
-- MAGIC AS SELECT * FROM silver_orders;
-- MAGIC ```
-- MAGIC Converting an existing external table to managed is supported on recent runtimes (`ALTER TABLE ... SET MANAGED`); check the current docs for prerequisites.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part D — Row filters and column masks
-- MAGIC Both are **SQL UDFs** attached to a table. They run for every query, for every user, everywhere (notebooks, SQL, BI, dbt).
-- MAGIC First, a gold table that contains personal data:

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_customer_contacts AS
SELECT customer_id, name, email, country, region, loyalty_tier
FROM silver_customers;

-- COMMAND ----------

-- Column mask: only members of 'admins' see real e-mails.
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('admins') THEN email
  ELSE concat('***@', split_part(email, '@', 2))
END;

ALTER TABLE gold_customer_contacts ALTER COLUMN email SET MASK mask_email;

-- COMMAND ----------

-- Row filter: admins see every row; everyone else only EU customers.
CREATE OR REPLACE FUNCTION eu_only(region STRING)
RETURN is_account_group_member('admins') OR region = 'EU';

ALTER TABLE gold_customer_contacts SET ROW FILTER eu_only ON (region);

-- COMMAND ----------

-- You are not an admin, so you should see masked e-mails and only EU rows.
SELECT region, count(*) AS customers, max(email) AS sample_email
FROM gold_customer_contacts
GROUP BY region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise D1
-- MAGIC Replace the hard-coded `'EU'` with a **mapping table** `region_access(group_name STRING, region STRING)` so that members of
-- MAGIC `users` see `NA` rows and admins see everything. Grant your buddy `SELECT` and confirm what they see.
-- MAGIC (They don't need `EXECUTE` on the function or `SELECT` on `region_access`: filters and masks run with the function owner's rights.)
-- MAGIC
-- MAGIC Hint: a row filter function can query a table: `EXISTS (SELECT 1 FROM region_access a WHERE a.region = region AND is_account_group_member(a.group_name))`.

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part E — ABAC: policies instead of per-table functions
-- MAGIC Per-table filters and masks don't scale to hundreds of tables. **Attribute-based access control** works like this:
-- MAGIC 1. An admin defines **governed tags**, e.g. `pii = email`.
-- MAGIC 2. Data owners tag columns (`email` → `pii: email`).
-- MAGIC 3. A **policy** on a catalog or schema says *"for every column tagged `pii`, apply `mask_email` unless the user is in `pii_readers`"*.
-- MAGIC 4. New tables with tagged columns are protected automatically.
-- MAGIC
-- MAGIC **Demo (mentor):** create a governed tag and a masking policy in **Catalog Explorer → your schema → Policies**, tag `gold_customer_contacts.email`, and show that a *new* table with a tagged column is masked without any `ALTER TABLE`.
-- MAGIC
-- MAGIC Tag a column yourself:

-- COMMAND ----------

ALTER TABLE gold_customer_contacts ALTER COLUMN email SET TAGS ('pii' = 'email');
SELECT * FROM information_schema.column_tags WHERE table_name = 'gold_customer_contacts';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part F — Lineage and audit
-- MAGIC 1. Open `gold_daily_sales` in Catalog Explorer → **Lineage** tab → *See lineage graph*. Trace it back to the landing volume.
-- MAGIC 2. Who has queried your contacts table? (needs access to `system.access`; ask your mentor)

-- COMMAND ----------

SELECT event_time, user_identity.email AS who, action_name
FROM system.access.audit
WHERE request_params.full_name_arg ILIKE concat('%', current_schema(), '.gold_customer_contacts')
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check yourself (exam style)
-- MAGIC 1. A user has `SELECT` on a table but gets a permission error. Most likely missing? A) `MODIFY` B) `USE SCHEMA` / `USE CATALOG` C) `EXECUTE` D) `ALL PRIVILEGES` on the metastore
-- MAGIC 2. You `DROP` a **managed** table. The underlying files: A) remain in storage forever B) are deleted (recoverable for a limited time with `UNDROP`) C) move to an external location D) are archived to a volume
-- MAGIC 3. Analysts must see only rows for their own country, in every tool. Best feature? A) A view per country B) A row filter function C) A column mask D) Separate catalogs
-- MAGIC 4. Hundreds of tables have e-mail columns that must be masked, including future tables. Best approach? A) `ALTER COLUMN ... SET MASK` on each B) ABAC policy on tagged columns C) Views D) Delta Sharing
-- MAGIC
-- MAGIC ### See hidden cell for answers

-- COMMAND ----------

-- 1 B · 2 B · 3 B · 4 B

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Solutions (mentors)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # SOLUTION — B1.3
-- MAGIC spark.sql(f"REVOKE SELECT ON TABLE {catalog}.{schema}.gold_daily_sales FROM `{dbutils.widgets.get('buddy')}`")

-- COMMAND ----------

-- SOLUTION — D1
CREATE OR REPLACE TABLE region_access (group_name STRING, region STRING);
INSERT INTO region_access VALUES ('users', 'NA');

CREATE OR REPLACE FUNCTION region_by_group(r STRING)
RETURN is_account_group_member('admins')
    OR EXISTS (SELECT 1 FROM region_access a
               WHERE a.region = r AND is_account_group_member(a.group_name));

ALTER TABLE gold_customer_contacts SET ROW FILTER region_by_group ON (region);
-- then, for the buddy:
--   GRANT SELECT ON TABLE gold_customer_contacts TO `<buddy>`;