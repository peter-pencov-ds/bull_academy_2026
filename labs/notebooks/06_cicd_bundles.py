# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · CI/CD with Git folders and Declarative Automation Bundles
# MAGIC
# MAGIC **Module:** C5 (exam) → B1 (deep dive) · **Exam domain:** Implementing CI/CD (10%) · **Time:** 1–2 days
# MAGIC
# MAGIC *Old name: Databricks Asset Bundles (DABs). The exam writes "Automation Bundle (formerly Databricks Asset Bundles)".*
# MAGIC
# MAGIC **You will be able to:**
# MAGIC - Branch, commit, push and open a pull request from a Databricks Git folder
# MAGIC - Read a bundle: `databricks.yml`, `include`, `variables`, `targets`, `resources`
# MAGIC - Deploy the same code to dev and a second target with different variables
# MAGIC - Use the Databricks CLI to `validate`, `deploy`, `run` and `destroy`
# MAGIC
# MAGIC This lab is mostly done in the **Git folder UI** and a **terminal**. Notebook cells are for checking results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A — Git folders (30 min)
# MAGIC This repo is already a Git folder in your workspace (you cloned it in week 1).
# MAGIC 1. Click the **branch name** at the top of this notebook → **Create branch** `feature/<you>-bundle-lab`.
# MAGIC 2. Edit `resources/job.yml`: change `timeout_seconds` to `5400`.
# MAGIC 3. In the Git dialog: review the diff → write a commit message → **Commit & Push**.
# MAGIC 4. Open the pull request in your Git provider. Ask your buddy to review it.
# MAGIC
# MAGIC Rules of the road: never commit to `main` directly; one PR per lab; no secrets in YAML (use secret scopes or CI variables).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B — Anatomy of the bundle (30 min)
# MAGIC Open `databricks.yml` and `resources/*.yml` side by side and fill in the table in the next cell.
# MAGIC
# MAGIC ```
# MAGIC databricks.yml          ← bundle name, variables, targets (dev / staging / prod)
# MAGIC resources/
# MAGIC   pipeline.yml          ← UC schema + declarative pipeline
# MAGIC   job.yml               ← the 6-task job from notebook 05
# MAGIC pipeline/
# MAGIC   northwind_pipeline.sql
# MAGIC notebooks/
# MAGIC   05_tasks/*.py         ← notebooks the job runs
# MAGIC ```
# MAGIC
# MAGIC Key ideas:
# MAGIC - **Variables** (`${var.schema}`) are declared once and **overridden per target**, or on the command line with `--var`.
# MAGIC - **Substitutions** read values the CLI knows: `${workspace.current_user.userName}`, `${bundle.target}`, `${resources.pipelines.northwind_pipeline.id}`.
# MAGIC - **`mode: development`** prefixes every resource with `[dev <you>]`, pauses schedules and triggers, and lets every developer deploy their own copy.
# MAGIC - **`mode: production`** enforces discipline: deploy from a clean Git state, use a shared `root_path`, and run as a **service principal** (`run_as`).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise B1 — read the bundle
# MAGIC | Question | Answer |
# MAGIC | --- | --- |
# MAGIC | Which target is used when you don't pass `-t`? | |
# MAGIC | In prod, which `batch` does the job land? Where is that set? | |
# MAGIC | How does the job know the pipeline's ID? | |
# MAGIC | What will the pipeline's schema be called in your dev deployment? | |
# MAGIC | Why does staging set `run_as`? | |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C — Deploy with the CLI (60 min)
# MAGIC Use the **web terminal** (attach to a cluster → Terminal) or your laptop with the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed
# MAGIC and authenticated (`databricks auth login --host <workspace-url>`). Run from the repo root:
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate                       # resolves variables, catches YAML mistakes
# MAGIC databricks bundle deploy                         # dev target by default
# MAGIC databricks bundle summary                        # what was deployed, with links
# MAGIC databricks bundle run northwind_job              # runs the job and streams status
# MAGIC ```
# MAGIC
# MAGIC If `validate` complains about `schema`, your user short name differs from the schema `00_setup` printed. Pass it explicitly:
# MAGIC `databricks bundle deploy --var="schema=<your schema>"`.
# MAGIC
# MAGIC Then open **Jobs & Pipelines**: you should see `[dev <you>] northwind_job` and `[dev <you>] northwind_pipeline`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise C1 — a second environment
# MAGIC Add a target `qa` to `databricks.yml` that:
# MAGIC - uses `mode: development` (no service principal needed for this lab),
# MAGIC - overrides `schema` to `<your schema>_qa` and `batch` to `"2"`.
# MAGIC
# MAGIC Create the landing volume for it (next cell), deploy with `databricks bundle deploy -t qa`, run it, and confirm the two deployments write to different schemas.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

qa_schema = f"{schema}_qa"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{qa_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{qa_schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{qa_schema}.checkpoints")
print(f"ready: {catalog}.{qa_schema}")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}*'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D — What CI does with this (read; built in Phase 2, module B1)
# MAGIC A typical pipeline in GitHub Actions or Azure DevOps:
# MAGIC
# MAGIC | Event | CI step | Identity |
# MAGIC | --- | --- | --- |
# MAGIC | Pull request opened | `databricks bundle validate -t staging` + unit tests | Service principal (OAuth M2M) |
# MAGIC | Merge to `main` | `databricks bundle deploy -t staging` → `bundle run` → integration checks | Service principal |
# MAGIC | Release tag | `databricks bundle deploy -t prod` | Service principal |
# MAGIC
# MAGIC Nobody deploys to prod from a laptop. Humans only deploy to their own `dev` copy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E — Clean up
# MAGIC ```bash
# MAGIC databricks bundle destroy -t qa
# MAGIC ```
# MAGIC Keep your `dev` deployment: the capstone builds on it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check yourself (exam style)
# MAGIC 1. Same code, but prod must write to catalog `prod` and dev to `dev`. Best approach? A) Two copies of the YAML B) A bundle variable overridden per target C) Edit the YAML before each deploy D) An `if` in the notebook
# MAGIC 2. Which command checks the bundle configuration without deploying? A) `bundle deploy --dry` B) `bundle validate` C) `bundle summary` D) `bundle init`
# MAGIC 3. In `mode: development`, a job with a schedule is deployed. The schedule is: A) active B) paused C) deleted D) converted to file arrival
# MAGIC 4. A production job should run as: A) the engineer who deployed it B) a service principal C) the workspace admin D) any account user
# MAGIC
# MAGIC ### See hidden cell for answers

# COMMAND ----------

# 1 B · 2 B · 3 B · 4 B

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (mentors)
# MAGIC **B1:** `dev` (`default: true`) · `"3"`, prod target `variables` · `${resources.pipelines.northwind_pipeline.id}` substitution · `dev_<you>_<schema>_lakeflow` style prefix applied in development mode (check `bundle summary`) · so the job runs with a stable, non-human identity.
# MAGIC
# MAGIC **C1:**
# MAGIC ```yaml
# MAGIC   qa:
# MAGIC     mode: development
# MAGIC     variables:
# MAGIC       schema: <your schema>_qa
# MAGIC       batch: "2"
# MAGIC ```