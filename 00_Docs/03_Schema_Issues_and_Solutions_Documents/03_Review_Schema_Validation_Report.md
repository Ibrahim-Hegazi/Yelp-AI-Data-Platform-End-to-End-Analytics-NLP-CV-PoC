# Yelp Review Dataset — Schema Validation Report

## Source File

`yelp_academic_dataset_review.json`

**Location:** `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_review.json`

---

## Pipeline Context

This report was produced during the **Schema Validation & Staging** step of the Yelp PoC pipeline. The table below traces the full data lineage, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| **2** | **`01_Landing_to_Staging_Schema_Validator`** | **`yelp_poc_uc.bronze_staging`** (`yelp_review_clean` / `yelp_review_errors`) | **◄ YOU ARE HERE — Schema Validation & Staging** |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_review` | Bronze Final |
| 4 | `4.1.3_Review_Basic_Profile` | _(reads `bronze_final.yelp_review`)_ | Basic EDA |
| 5 | `4.2.3_Review_Deep_EDA` | _(reads `bronze_final.yelp_review`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## Validation Summary

| Metric | Value |
| --- | --- |
| Total Rows | 6,990,280 |
| Clean Rows | 6,990,280 |
| Error Rows | 0 |
| Pass Rate | 100% |
| Schema Status | **PASS — No Issues Found** |

## Schema Issues Found

**None.** All 6,990,280 rows passed schema validation successfully. Every row contains the expected 9 top-level fields with no missing or extra columns detected.

## Validated Schema (9 Fields)

| # | Column Name | Data Type |
| --- | --- | --- |
| 1 | `review_id` | STRING |
| 2 | `user_id` | STRING |
| 3 | `business_id` | STRING |
| 4 | `stars` | DOUBLE |
| 5 | `useful` | LONG |
| 6 | `funny` | LONG |
| 7 | `cool` | LONG |
| 8 | `text` | STRING |
| 9 | `date` | STRING |

## Promotion Status

* **Staging Clean Table:** `yelp_poc_uc.bronze_staging.yelp_review_clean` (6,990,280 rows)
* **Staging Errors Table:** `yelp_poc_uc.bronze_staging.yelp_review_errors` (0 rows)
* **Final Table:** `yelp_poc_uc.bronze_final.yelp_review` (6,990,280 rows)
* **Promoted:** Yes — all clean rows promoted to `bronze_final` with zero data loss

## Validation Method

Schema validation was performed by notebook `01_Landing_to_Staging_Schema_Validator` in the `03_Schema_Validation_and_Promotion` folder. The first non-empty JSON line was used as the expected top-level field template. Each row was checked for:

1. Valid JSON syntax
2. All expected top-level fields present (no missing)
3. No unexpected top-level fields (no extras)

## Solutions Applied

No solutions were required — the dataset passed validation without any schema issues.
