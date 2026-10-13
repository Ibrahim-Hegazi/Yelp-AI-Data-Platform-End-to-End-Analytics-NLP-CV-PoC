# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 02 — Yelp Bronze Staging Validator
# MAGIC
# MAGIC This notebook inspects the **error tables** in `yelp_poc_uc.bronze_staging` produced by `01_Yelp_Bronze_JSON_Validator` and decides whether the clean tables can be promoted to `yelp_poc_uc.bronze_final`.
# MAGIC
# MAGIC ### Flow
# MAGIC 1. **Scan** all five `_errors` tables and report row counts alongside their `_clean` counterparts
# MAGIC 2. **If all error tables are empty** → automatically promote the `_clean` tables to `bronze_final`
# MAGIC 3. **If any error table has rows** → display error details and halt for human review
# MAGIC
# MAGIC ### Datasets
# MAGIC
# MAGIC | Staging Clean Table | Staging Errors Table | Final Table |
# MAGIC | --- | --- | --- |
# MAGIC | `yelp_business_clean` | `yelp_business_errors` | `yelp_business` |
# MAGIC | `yelp_checkin_clean` | `yelp_checkin_errors` | `yelp_checkin` |
# MAGIC | `yelp_review_clean` | `yelp_review_errors` | `yelp_review` |
# MAGIC | `yelp_tip_clean` | `yelp_tip_errors` | `yelp_tip` |
# MAGIC | `yelp_user_clean` | `yelp_user_errors` | `yelp_user` |
# MAGIC
# MAGIC ### When to Run
# MAGIC - After `01_Yelp_Bronze_JSON_Validator` completes successfully
# MAGIC - Re-run after fixing source data issues and re-running notebook 01
# MAGIC
# MAGIC ### Human Intervention
# MAGIC When errors exist, execution stops at a checkpoint cell. You have two options:
# MAGIC - **Fix and re-run**: correct the source data, re-run notebook 01, then re-run this notebook
# MAGIC - **Proceed anyway**: set the `action` widget to `PROCEED` to promote only the clean rows (error rows are excluded)

# COMMAND ----------

# DBTITLE 1,Configuration and error table scan
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================
STAGING_SCHEMA = "yelp_poc_uc.bronze_staging"
FINAL_SCHEMA = "yelp_poc_uc.bronze_final"

DATASETS = ["yelp_business", "yelp_checkin", "yelp_review", "yelp_tip", "yelp_user"]

# Widget: controls behaviour when errors exist
# Default is STOP — change to PROCEED to promote only clean rows
dbutils.widgets.dropdown("action", "STOP", ["STOP", "PROCEED"], "Action when errors exist")

# ============================================================
# SCAN — read row counts from all staging tables
# ============================================================
summary = []
for ds in DATASETS:
    clean_table = f"{STAGING_SCHEMA}.{ds}_clean"
    errors_table = f"{STAGING_SCHEMA}.{ds}_errors"

    clean_count = spark.table(clean_table).count()
    error_count = spark.table(errors_table).count()

    summary.append({
        "dataset": ds,
        "clean_table": clean_table,
        "errors_table": errors_table,
        "clean_rows": clean_count,
        "error_rows": error_count,
        "status": "PASS" if error_count == 0 else "NEEDS REVIEW",
    })

display(spark.createDataFrame(summary).select(
    "dataset", "clean_rows", "error_rows", "status"
).orderBy("dataset"))

# Summary flags used by downstream cells
total_errors = sum(s["error_rows"] for s in summary)
datasets_with_errors = [s["dataset"] for s in summary if s["error_rows"] > 0]

if total_errors == 0:
    print(f"\nAll error tables are empty. Clean tables are ready for promotion to bronze_final.")
else:
    print(f"\n{total_errors} error row(s) found across {len(datasets_with_errors)} dataset(s): {', '.join(datasets_with_errors)}")
    print("Review the error details in the next cell before proceeding.")

# COMMAND ----------

# DBTITLE 1,Error details
# ============================================================
# ERROR DETAILS — display row-level diagnostics for each
# dataset that has validation errors
# ============================================================

if datasets_with_errors:
    for ds in datasets_with_errors:
        errors_table = f"{STAGING_SCHEMA}.{ds}_errors"
        error_count = spark.table(errors_table).count()
        print(f"\n{'='*60}")
        print(f"{ds}  ({error_count} error row(s))")
        print(f"{'='*60}")
        display(spark.table(errors_table))
else:
    print("No errors to display. All five datasets passed schema validation.")

# COMMAND ----------

# DBTITLE 1,Human checkpoint
# MAGIC %md
# MAGIC ### Human Checkpoint
# MAGIC
# MAGIC If errors were reported above, review the diagnostic columns:
# MAGIC - `missing_columns` — which expected top-level fields are absent
# MAGIC - `actual_field_count` vs `expected_field_count` — structural mismatch
# MAGIC - `record_data` — the raw JSON line that failed validation
# MAGIC
# MAGIC **Options:**
# MAGIC 1. **Fix and re-run** — correct the source data or update the schema expectations in `01_Yelp_Bronze_JSON_Validator`, re-run it, then re-run this notebook
# MAGIC 2. **Proceed anyway** — set the `action` widget (top of the notebook) to **PROCEED** and run the next cell. Only clean rows will be promoted to `bronze_final`; error rows are excluded
# MAGIC
# MAGIC If all error tables were empty, the next cell will promote automatically regardless of the widget value.

# COMMAND ----------

# DBTITLE 1,Promote clean tables to bronze_final
# ============================================================
# PROMOTE — copy clean staging tables to bronze_final
# ============================================================
# If errors exist, this cell respects the widget:
#   STOP    → exits the notebook without promoting
#   PROCEED → promotes only clean rows (errors excluded)
# If no errors exist, promotion runs unconditionally.
# ============================================================

if total_errors > 0:
    action = dbutils.widgets.get("action")
    if action == "STOP":
        msg = (
            f"STOPPED: {total_errors} error row(s) found across "
            f"{len(datasets_with_errors)} dataset(s). "
            f"Review errors above, then either fix and re-run or set "
            f"the 'action' widget to PROCEED."
        )
        print(msg)
        dbutils.notebook.exit(msg)
    else:
        print(f"Proceeding despite {total_errors} error row(s). Only clean rows will be promoted.\n")

print(f"Promoting clean tables from {STAGING_SCHEMA} to {FINAL_SCHEMA}:\n")

for ds in DATASETS:
    clean_table = f"{STAGING_SCHEMA}.{ds}_clean"
    final_table = f"{FINAL_SCHEMA}.{ds}"
    clean_count = spark.table(clean_table).count()

    spark.sql(f"CREATE OR REPLACE TABLE {final_table} AS SELECT * FROM {clean_table}")
    print(f"  [OK] {final_table} <- {clean_table}  ({clean_count:,} rows)")

print(f"\nPromotion complete. {len(DATASETS)} table(s) written to {FINAL_SCHEMA}.")