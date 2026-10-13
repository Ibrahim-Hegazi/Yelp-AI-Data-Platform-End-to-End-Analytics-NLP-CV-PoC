# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## Yelp Bronze JSON Schema Consistency Check
# MAGIC
# MAGIC This notebook checks whether the five Yelp JSON source files in the landing zone volume (`/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset`) have a consistent schema across all rows. It does **not** clean or transform any data — it only separates rows whose top-level fields match the expected template from rows that do not.
# MAGIC
# MAGIC Results are written as managed Delta tables in the `yelp_poc_uc.bronze_staging` Unity Catalog schema.
# MAGIC
# MAGIC ### Source Files
# MAGIC
# MAGIC - `yelp_academic_dataset_business.json`
# MAGIC - `yelp_academic_dataset_checkin.json`
# MAGIC - `yelp_academic_dataset_review.json`
# MAGIC - `yelp_academic_dataset_tip.json`
# MAGIC - `yelp_academic_dataset_user.json`
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. The first non-empty JSON line in each file is used as the expected top-level field template.
# MAGIC 2. Every subsequent row is checked for:
# MAGIC    - Valid JSON syntax
# MAGIC    - All expected top-level fields present (no missing)
# MAGIC    - No unexpected top-level fields (no extras)
# MAGIC 3. Rows that pass are written to a **clean** Delta table with original column names.
# MAGIC 4. Rows that fail are written to an **errors** Delta table with three diagnostic columns:
# MAGIC    - `actual_field_count` — how many top-level fields the row actually has
# MAGIC    - `expected_field_count` — how many it should have
# MAGIC    - `missing_columns` — which expected fields are absent
# MAGIC
# MAGIC ### Output Delta Tables in `yelp_poc_uc.bronze_staging`
# MAGIC
# MAGIC | Clean Table | Errors Table |
# MAGIC | --- | --- |
# MAGIC | `yelp_business_clean` | `yelp_business_errors` |
# MAGIC | `yelp_checkin_clean` | `yelp_checkin_errors` |
# MAGIC | `yelp_review_clean` | `yelp_review_errors` |
# MAGIC | `yelp_tip_clean` | `yelp_tip_errors` |
# MAGIC | `yelp_user_clean` | `yelp_user_errors` |
# MAGIC

# COMMAND ----------

# DBTITLE 1,Schema consistency check
import json
from pyspark.sql import functions as F

LANDING_ZONE_DIR = "/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset"
PREVALIDATION_SCHEMA = "yelp_poc_uc.bronze_staging"

FILE_CONFIGS = [
    {"file_name": "yelp_academic_dataset_business.json", "dataset_name": "yelp_business"},
    {"file_name": "yelp_academic_dataset_checkin.json", "dataset_name": "yelp_checkin"},
    {"file_name": "yelp_academic_dataset_review.json", "dataset_name": "yelp_review"},
    {"file_name": "yelp_academic_dataset_tip.json", "dataset_name": "yelp_tip"},
    {"file_name": "yelp_academic_dataset_user.json", "dataset_name": "yelp_user"},
]


def get_expected_keys(file_path):
    """Use the first non-empty JSON line as the expected top-level field template."""
    sample_rows = (
        spark.read.text(file_path)
        .where(F.trim("value") != "")
        .limit(1)
        .collect()
    )
    if not sample_rows:
        raise ValueError(f"No JSON records found in {file_path}")
    return list(json.loads(sample_rows[0]["value"]).keys())


def validate_json_file(file_name, dataset_name):
    """
    Check schema consistency for one landing-zone JSON file.
    Writes clean rows and error rows as separate Delta tables in the
    bronze_staging schema.  No data cleaning is performed.
    """
    file_path = f"{LANDING_ZONE_DIR}/{file_name}"
    clean_table = f"{PREVALIDATION_SCHEMA}.{dataset_name}_clean"
    errors_table = f"{PREVALIDATION_SCHEMA}.{dataset_name}_errors"

    # Derive the expected template from the first record
    expected_keys = get_expected_keys(file_path)
    expected_keys_array = F.array(*[F.lit(k) for k in expected_keys])
    inferred_schema = spark.read.json(file_path).schema

    # Read every line as raw text and compute validation columns
    raw_df = spark.read.text(file_path).where(F.trim("value") != "")

    validated_df = raw_df.select(
        "value",
        F.coalesce(F.json_object_keys("value"), F.expr("array()")).alias("actual_keys"),
        F.sort_array(
            F.array_except(
                expected_keys_array,
                F.coalesce(F.json_object_keys("value"), F.expr("array()")),
            )
        ).alias("missing_columns"),
        F.lit(len(expected_keys)).alias("expected_field_count"),
        F.size(F.coalesce(F.json_object_keys("value"), F.expr("array()"))).alias("actual_field_count"),
        F.from_json("value", inferred_schema).alias("parsed"),
    ).select(
        "*",
        (
            F.col("parsed").isNotNull()
            & (F.size("missing_columns") == 0)
            & (F.col("actual_field_count") == F.col("expected_field_count"))
        ).alias("is_clean"),
    )

    # --- Clean rows: original column names, no transformation ---------
    clean_columns = [
        F.col(f"parsed.`{name}`").alias(name) for name in expected_keys
    ]
    clean_df = validated_df.filter(F.col("is_clean")).select(*clean_columns)

    # --- Error rows: raw record + the three diagnostic columns --------
    errors_df = validated_df.filter(~F.col("is_clean")).select(
        F.col("value").alias("record_data"),
        "actual_field_count",
        "expected_field_count",
        F.concat_ws(", ", "missing_columns").alias("missing_columns"),
    )

    # --- Collect summary before writing -------------------------------
    summary_row = validated_df.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("is_clean"), 1).otherwise(0)).alias("clean_rows"),
        F.sum(F.when(~F.col("is_clean"), 1).otherwise(0)).alias("error_rows"),
    ).collect()[0]

    # --- Write managed Delta tables to yelp_poc_uc.bronze_staging -----
    clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(clean_table)
    errors_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(errors_table)

    return {
        "source_file": file_name,
        "clean_delta_table": clean_table,
        "errors_delta_table": errors_table,
        "expected_fields": len(expected_keys),
        "total_rows": summary_row["total_rows"],
        "clean_rows": summary_row["clean_rows"],
        "error_rows": summary_row["error_rows"],
    }


# ---- Run validation for all five files ----
summary_rows = []
for cfg in FILE_CONFIGS:
    print(f"Validating {cfg['file_name']} ...")
    summary_rows.append(validate_json_file(cfg["file_name"], cfg["dataset_name"]))

display(spark.createDataFrame(summary_rows).orderBy("source_file"))