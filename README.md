# Northwind Labs — Databricks Data Engineer Associate hands-on track

Hands-on material for the intern program's **Phase 1 (certification core)**. There are 11 notebooks, one data generator, one declarative pipeline and one deployable bundle. Everything is built around a single fictional e-commerce dataset, *Northwind Outdoors*, so each lab builds on the previous one.

The labs are mapped to the Data Engineer Associate exam version **live since 4 May 2026** (7 domains). They use the current product names:

| Current name | Old name you'll see online |
| --- | --- |
| Lakeflow Spark Declarative Pipelines | Delta Live Tables (DLT) |
| Lakeflow Jobs | Workflows |
| Declarative Automation Bundles | Databricks Asset Bundles (DABs) |
| `AUTO CDC` | `APPLY CHANGES INTO` |

## Notebook map

| Notebook | Program module | Exam domain (weight) | Time | Main hands-on |
| --- | --- | --- | --- | --- |
| `00_setup` | — | — | 10 min | Schema, volumes, land batch 1 |
| `01_platform_and_compute` | C1 | Databricks Intelligence Platform (6%) | 2–3 h | Delta internals, lazy evaluation, compute choice, Liquid Clustering |
| `02_ingestion` | C2 | Data Ingestion and Loading (21%) | 2 days | `COPY INTO`, Auto Loader, schema evolution, rescued data, nested JSON |
| `03_transformation_and_modeling` | C3 | Data Transformation and Modeling (22%) | 2–3 days | Silver/gold in PySpark + SQL, joins, unions, windows, `MERGE`, MVs, time travel |
| `04_declarative_pipelines` | C2/C3 + B3 | Ingestion + Transformation | 1 day | Pipeline from `pipeline/northwind_pipeline.sql`, expectations, SCD1/SCD2 with `AUTO CDC`, event log |
| `05_lakeflow_jobs` + `05_tasks/` | C4 | Working with Lakeflow Jobs (16%) | 1–2 days | 6-task job, task values, if/else, retries, repair, triggers |
| `06_cicd_bundles` | C5 → B1 | Implementing CI/CD (10%) | 1–2 days | Git folder PR flow, bundle anatomy, CLI deploy to two targets |
| `07_troubleshooting_and_optimization` | C6 | Troubleshooting, Monitoring and Optimization (10%) | 1 day | Skew, spill and OOM in the Spark UI, small files, run history |
| `08_governance_and_security` (SQL) | C7 | Governance and Security (15%) | 1 day | Grants with a buddy, managed vs external, row filters, column masks, tags/ABAC, audit |
| `09_exam_review` | Week 8 | All | 2 h / pass | 12 predict-then-prove exam-style questions |
| `99_cleanup` | — | — | 5 min | Drop all lab schemas |

Every lab notebook has the same shape: learning goals → guided cells → **TODO exercises** → **Check yourself** (exam-style questions with hidden answers) → **Solutions (mentors)**.

## The dataset

`notebooks/_data_generator.py` writes raw files into a Unity Catalog volume in three batches. A fixed seed means every intern gets the same data and the same answers.

| Batch | Lands | Seeded problems |
| --- | --- | --- |
| 1 | 50 products (CSV), 510 customer rows (CSV), 2,000 orders (nested JSON) | 15 missing e-mails, 10 duplicate customer rows |
| 2 | 100 customer rows (60 updates + 40 new), 800 orders | New column `loyalty_tier` (schema drift), country changes (SCD2) |
| 3 | 830 orders | 30 duplicates, 25 null customers, 20 prices as text (`"12,50"`), 40 late orders |

Expected results mentors can check against: `silver_customers` = 540 rows; in the pipeline, `bad_rows` = 0 after batches 1–2 and 55 after batch 3 (30 duplicates + 25 null customers).

## Setup

**Workspace requirements (one-time, by an admin):**
- Unity Catalog enabled. A catalog `interns` (or another name; change the `catalog` widget) where interns have `USE CATALOG` and `CREATE SCHEMA`.
- Serverless compute for notebooks, jobs and pipelines.
- A small **classic** cluster policy for notebook 07 (the Spark UI labs can't run on serverless).
- An account group `interns_analysts` for notebook 08.
- Read access to `system.lakeflow` and `system.access` system tables (optional; used in 07 and 08).

**Each intern:**
1. **Workspace → Create → Git folder** and clone this repository: https://github.com/peter-pencov-ds/bull_academy_2026.
2. Create your own branch (feature_{your_name}). You will work on this branch until lab 06. Do not make pull requests to master from this branch.
2. Open `notebooks/00_setup` and run all. Note the schema it prints.
3. Work through the notebooks in order. Each one says what must already have run.

**Deploying the bundle (notebook 06):**
```bash
databricks auth login --host https://<your-workspace>
databricks bundle validate
databricks bundle deploy                        # dev target; add --var="schema=<schema>" if validate complains
databricks bundle run northwind_job
```

## Repository layout

```
databricks.yml                  bundle: variables + dev / staging / prod targets
resources/
  pipeline.yml                  UC schema + declarative pipeline
  job.yml                       6-task orchestration job
pipeline/
  northwind_pipeline.sql        bronze → silver (expectations, AUTO CDC) → gold
notebooks/
  _config.py                    shared: catalog/schema widgets, volumes
  _data_generator.py            3-batch messy data generator
  00_setup.py … 09_exam_review.py, 99_cleanup.py
  05_tasks/                     notebooks the job runs
```

## Notes for mentors

- **Before the cohort starts:** run all notebooks once end to end in the target workspace. Some features (materialized views, `AUTO CDC` syntax, ABAC policies, `ALTER TABLE ... SET MANAGED`) depend on workspace configuration and runtime version.
- Solutions are inline at the bottom of each notebook. For a student version, delete every cell after the `## Solutions (mentors)` heading.
- Exercise answers that need a judgment call (compute choice, trade-offs) are discussion prompts for the Friday session, not auto-graded.
- The "Check yourself" and `09_exam_review` questions are original practice items, not real exam questions. Pair them with the official exam guide and Databricks Academy's free Data Engineering learning path.
- `08_governance_and_security` says Unity Catalog has no `DENY`. Re-check this against the current official exam guide, which reportedly lists `DENY` among the principal operations.
