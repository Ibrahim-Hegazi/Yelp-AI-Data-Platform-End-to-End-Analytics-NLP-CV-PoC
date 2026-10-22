# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # 🧹 Business Data Cleaning & Silver Promotion
# MAGIC
# MAGIC > **Layer Transition:** Bronze Final → Silver Cleaned  
# MAGIC > **Source Table:** `yelp_poc_uc.bronze_final.yelp_business`  
# MAGIC > **Target Table:** `yelp_poc_uc.silver_cleaned.yelp_business`  
# MAGIC > **DQ Reference:** _(pending — Deep EDA not yet complete)_
# MAGIC
# MAGIC This notebook implements the **Bronze → Silver Cleaned data population** for the Yelp Business dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > ## 🚨 IMPORTANT — THIS NOTEBOOK IS NOT COMPLETE
# MAGIC >
# MAGIC > **Deep EDA has not been performed on the Business dataset yet.** Without a thorough data-quality assessment, the full set of cleaning transformations, DQ issue resolutions, and schema decisions cannot be finalized.
# MAGIC >
# MAGIC > **Current scope (temporary):** This notebook exists solely to **propagate `business_id` values from Bronze Final into the Silver Cleaned `yelp_business` table** so that downstream tables with a foreign-key dependency on `business_id` — specifically **`yelp_checkin`** — can be fully loaded into the Silver Cleaned layer.
# MAGIC >
# MAGIC > Once the Business Deep EDA is complete, this notebook will be expanded with the full cleaning logic, column-level transformations, and DQ issue resolutions — following the same pattern as `02_Checkin_Cleaning`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **⚠️ Prerequisites — Run DDL Queries First:**  
# MAGIC > Before running this notebook, execute the DDL query **`01_Business_Silver_Cleaned`** to create the `yelp_poc_uc.silver_cleaned` schema and the empty `yelp_business` table with PK constraints.  
# MAGIC > This notebook then **populates** the table created by the DDL. The DDL is idempotent and can be re-run safely.

# COMMAND ----------

# DBTITLE 1,Pipeline Context
# MAGIC %md
# MAGIC ## 📍 Pipeline Context
# MAGIC
# MAGIC | Step | Notebook | UC Location | Layer |
# MAGIC | ---: | --- | --- | --- |
# MAGIC | 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
# MAGIC | 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` | Bronze Staging |
# MAGIC | 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_business` | Bronze Final |
# MAGIC | 4 | `4.1.1_Business_Basic_Profile` | _(reads bronze_final)_ | Basic EDA |
# MAGIC | 5 | _Business Deep EDA_ | _(not yet created)_ | Deep EDA / DQ Assessment |
# MAGIC | 5.1 | `01_Business_Silver_Cleaned` _(DDL query)_ | `yelp_poc_uc.silver_cleaned.yelp_business` + 10 child tables | Schema & Table Creation |
# MAGIC | **6** | **`01_Business_Cleaning` ◄ THIS NOTEBOOK** | **`yelp_poc_uc.silver_cleaned.yelp_business`** | **Silver** |
# MAGIC | 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

# COMMAND ----------

# DBTITLE 1,Environment Setup — Prerequisite Check
# ============================================================
# 🔧 Environment Setup — Prerequisite Check
# ============================================================
# PREREQUISITES (run this DDL query BEFORE this notebook):
#   1. 01_Business_Silver_Cleaned — creates silver_cleaned schema
#      and the yelp_business parent table (+ 10 child tables)
#
# This notebook POPULATES the parent table only.
# No schema creation is needed here.

# Verify the target table exists (created by DDL query)
try:
    tbl_info = spark.sql("DESCRIBE TABLE yelp_poc_uc.silver_cleaned.yelp_business").collect()
    col_count = len([r for r in tbl_info if r['col_name'] and not r['col_name'].startswith('#')])
    print("✅ Target table exists: yelp_poc_uc.silver_cleaned.yelp_business")
    print(f"   Columns: {col_count}")
except Exception as e:
    raise RuntimeError(
        "❌ Target table yelp_poc_uc.silver_cleaned.yelp_business does not exist!\n"
        "   Run the DDL query first:\n"
        "   1. 01_Business_Silver_Cleaned\n"
        f"   Error: {e}"
    )

# COMMAND ----------

# DBTITLE 1,Temporary Scope — business_id Passthrough (No Cleaning)
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Temporary Population — business_id Only
# MAGIC
# MAGIC > **🚨 Only the `business_id` primary key is populated.** All other columns remain NULL. This is the minimum needed to satisfy the `yelp_checkin` FK dependency on `business_id`.
# MAGIC
# MAGIC Once the Business Deep EDA is complete, this section will be replaced with the full cleaning pipeline that populates all columns with type corrections (e.g. `is_open` BIGINT→BOOLEAN, `stars` DOUBLE→DECIMAL).

# COMMAND ----------

# DBTITLE 1,Populate Silver Cleaned — business_id Only
from pyspark.sql.functions import col, lit

# ============================================================
# Populate Silver Cleaned — business_id Only
# ============================================================
# Temporary: only the PK column is populated.
# All other columns are explicitly set to NULL.
# This satisfies the FK dependency for yelp_checkin.
# Full column population will follow after Deep EDA.

df_bronze = spark.read.table("yelp_poc_uc.bronze_final.yelp_business")

bronze_count = df_bronze.count()
print(f"📥 Bronze source: {bronze_count:>12,} rows")

# Build DataFrame matching the full silver DDL schema:
# business_id populated, all other columns NULL
df_silver = (
    df_bronze
    .select(
        col("business_id"),
        lit(None).cast("string").alias("name"),
        lit(None).cast("string").alias("address"),
        lit(None).cast("string").alias("city"),
        lit(None).cast("string").alias("state"),
        lit(None).cast("string").alias("postal_code"),
        lit(None).cast("double").alias("latitude"),
        lit(None).cast("double").alias("longitude"),
        lit(None).cast("decimal(2,1)").alias("stars"),
        lit(None).cast("bigint").alias("review_count"),
        lit(None).cast("boolean").alias("is_open"),
    )
)

# Overwrite to make this notebook idempotent (re-runnable)
(
    df_silver
    .write
    .mode("overwrite")
    .insertInto("yelp_poc_uc.silver_cleaned.yelp_business")
)
print(f"✅ Silver Cleaned table populated (business_id only): yelp_poc_uc.silver_cleaned.yelp_business")

# COMMAND ----------

# DBTITLE 1,Post-Write Validation
# ============================================================
# ✅ Post-Write Validation
# ============================================================
# Read back from the persisted table to verify correctness.

silver_table = spark.read.table("yelp_poc_uc.silver_cleaned.yelp_business")
sv_count = silver_table.count()

# 1. Row count reconciliation
print("🔍 Validation Results:")
print(f"   Bronze rows:  {bronze_count:>12,}")
print(f"   Silver rows:  {sv_count:>12,}")
assert sv_count == bronze_count, f"❌ Row count mismatch! Bronze={bronze_count}, Silver={sv_count}"
print("   ✅ Row counts match")

# 2. PK uniqueness check
distinct_ids = silver_table.select("business_id").distinct().count()
assert distinct_ids == sv_count, f"❌ Duplicate business_ids! Distinct={distinct_ids}, Total={sv_count}"
print(f"   ✅ business_id is unique ({distinct_ids:,} distinct = {sv_count:,} total)")

# 3. Null check on PK
null_ids = silver_table.filter(col("business_id").isNull()).count()
assert null_ids == 0, f"❌ {null_ids} null business_ids found!"
print(f"   ✅ No null business_ids")

print(f"\n🎉 Validation passed — {sv_count:,} businesses ready in silver_cleaned")