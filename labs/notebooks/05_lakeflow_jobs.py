# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Orchestration with Lakeflow Jobs
# MAGIC
# MAGIC **Module:** C4 · **Exam domain:** Working with Lakeflow Jobs (16%) · **Time:** 1–2 days
# MAGIC
# MAGIC *Old name: Databricks Workflows.*
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Build a multi-task job with notebook, pipeline and condition tasks
# MAGIC - Pass values between tasks (task values, dynamic value references)
# MAGIC - Configure retries, timeouts and notifications
# MAGIC - Choose between scheduled, file-arrival and table-update triggers
# MAGIC - Repair a failed run without rerunning what already succeeded
# MAGIC
# MAGIC **Before you start:** the pipeline from notebook 04 exists and has run at least once.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The job you will build
# MAGIC
# MAGIC ```
# MAGIC land_files ──► refresh_pipeline ──► validate ──► check_quality (bad_rows > 0 ?)
# MAGIC                                     (retries)        │ true            │ false
# MAGIC                                                      ▼                 │
# MAGIC                                             quarantine_report          │
# MAGIC                                                      └──────► publish ◄┘   (run if: at least one succeeded)
# MAGIC ```
# MAGIC
# MAGIC | Task | Type | Notebook / object | Teaches |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `land_files` | Notebook | `05_tasks/land_batch` | Parameters, task values |
# MAGIC | `refresh_pipeline` | Pipeline | your pipeline from 04 | Orchestrating declarative pipelines |
# MAGIC | `validate` | Notebook | `05_tasks/validate` | Retries, publishing a task value |
# MAGIC | `check_quality` | If/else condition | `{{tasks.validate.values.bad_rows}} > 0` | Branching |
# MAGIC | `quarantine_report` | Notebook | `05_tasks/quarantine_report` | Conditional path |
# MAGIC | `publish` | Notebook | `05_tasks/publish` | `run_if`, dynamic value references |
# MAGIC
# MAGIC The complete definition is in `resources/job.yml`. **Build it by hand in the UI first**; in notebook 06 you'll deploy it from YAML.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Build the job in the UI (45 min)
# MAGIC 1. **Jobs & Pipelines → Create → Job**. Name it `northwind_job_<your name>`.
# MAGIC 2. **Job parameters** (right panel): `catalog`, `schema` (your schema), `pipeline_schema` (`<schema>_lakeflow`). Job parameters reach every task as widgets.
# MAGIC 3. Add the six tasks from the table above. For `land_files` set task parameter `batch = 1`.
# MAGIC 4. On `validate`: **Retries = 2**, retry interval 30 s.
# MAGIC 5. `check_quality`: task type **If/else condition**, left `{{tasks.validate.values.bad_rows}}`, operator `>`, right `0`.
# MAGIC 6. `quarantine_report` depends on `check_quality` (**true**). `publish` depends on `check_quality` (**false**) **and** `quarantine_report`, with **Run if: At least one succeeded**.
# MAGIC 7. On `publish` add parameters `run_id = {{job.run_id}}` and `run_started = {{job.start_time.iso_datetime}}`.
# MAGIC 8. Compute: leave **Serverless**.
# MAGIC 9. **Run now.** With only batches 1–2 landed, which branch runs? Check the run graph.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Retries and Repair run (30 min)
# MAGIC 1. Set `validate`'s parameter `simulate_flaky = true` and run the job. The first attempt fails, the retry succeeds. Find both attempts in the run view.
# MAGIC 2. Remove the `simulate_flaky` checkpoint (2 cells below) and set `validate` **Retries = 0** and run the job.
# MAGIC 3. The run fails at `validate`. Click **Repair run**: only `validate` and its downstream tasks will rerun; `land_files` and `refresh_pipeline` keep their successful results.
# MAGIC 4. remove `simulate_flaky` parameter from `validate` and set **Retries = 2**

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.fs.rm(f"{checkpoints}/_flaky_marker")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Take the other branch (15 min)
# MAGIC Change `land_files`' parameter to `batch = 3` and run again. Batch 3 has duplicates and orphan orders, so `bad_rows > 0`
# MAGIC and `quarantine_report` runs. Check table `quarantine_orders` and `job_run_log`.

# COMMAND ----------

display(spark.sql("SELECT reason, count(*) AS n FROM quarantine_orders GROUP BY ALL"))
display(spark.table("job_run_log").orderBy("logged_at", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — Triggers
# MAGIC
# MAGIC | Trigger | Starts the job when | Use it when |
# MAGIC | --- | --- | --- |
# MAGIC | **Scheduled** (cron) | A time is reached | The source is ready at a known time, or the business needs a fixed SLA |
# MAGIC | **File arrival** | New files appear in a volume / external location | Files arrive at unpredictable times |
# MAGIC | **Table update** | A Unity Catalog table you depend on is updated | You depend on a table owned by another team |
# MAGIC | **Continuous** | Immediately after the previous run ends | Near-real-time, always-on workloads |
# MAGIC | **Manual / API** | Someone or something calls *Run now* / the Jobs API | Backfills, external orchestrators |
# MAGIC
# MAGIC ### Exercise D1
# MAGIC Add a **file arrival** trigger on `/Volumes/<catalog>/<schema>/landing/orders/` to your job. Then run `land_batch(4, landing)` from a notebook and watch the job start on its own (allow ~1 min).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E — More control flow (read + try)
# MAGIC - **For each** task: runs a nested task once per item of a list, e.g. `["orders", "customers", "products"]`, with configurable concurrency.
# MAGIC - **Run if** options: All succeeded (default), At least one succeeded, None failed, All done, At least one failed, All failed.
# MAGIC - **Run Job** task: call another job as a task (modular orchestration).
# MAGIC - **SQL / dashboard / dbt tasks**: run a query, refresh a dashboard, run a dbt project (Phase 2, module B2).
# MAGIC
# MAGIC ### Exercise E1
# MAGIC Add a **For each** task that loops over `["bronze_orders", "silver_orders", "gold_daily_sales"]` and runs a one-line notebook printing the row count of `{{input}}`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. Task C should run only if task B **failed**. Run if = ? A) All succeeded B) At least one failed C) All done D) None failed
# MAGIC 2. A job failed at task 4 of 6 after tasks 1–3 took 40 minutes. Fastest way to finish? A) Run now B) Repair run C) Clone the job D) Restart the cluster
# MAGIC 3. How does a downstream task read `bad_rows` set by task `validate`? A) `spark.conf.get` B) `{{tasks.validate.values.bad_rows}}` / `dbutils.jobs.taskValues.get` C) A global temp view D) Environment variable
# MAGIC 4. Another team's table `sales.gold.orders` is refreshed at unpredictable times. Your job must run after each refresh. Trigger? A) Cron every 5 min B) Table update C) File arrival D) Continuous
# MAGIC
# MAGIC ### print solution in cell below

# COMMAND ----------

print(answers["05_lakeflow_jobs"]["Q1"])