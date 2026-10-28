### 07_troubleshooting_and_optimization

## Lab 6 — Broken job clinic (mentor-led, 45 min)
Your mentor deploys a copy of the job with three seeded faults. For each: find it, fix it, and write a 3-line root-cause note (symptom → cause → fix).

| # | Seeded fault (mentor eyes only) | Where interns should find it |
| --- | --- | --- |
| 1 | `validate` reads `pipeline_schema = wrong_schema` | Task output: `TABLE_OR_VIEW_NOT_FOUND` |
| 2 | add job timeout of 30s | Job output: Cancelled (timeout) |
| 3 | `land_files` batch parameter is set to `10` | Task output: `ValueError` |

#### These are just some ideas, feel free to freestyle ;)