# Yelp Business Dataset — Schema Validation Report

## Source File

`yelp_academic_dataset_business.json`

**Location:** `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_business.json`

---

## Pipeline Context

| Property | Value |
| --- | --- |
| Notebooks Executed **Before** (Upstream) | `01_Landing_to_Staging_Schema_Validator` — Validates JSON schema consistency and splits rows into clean/error Delta tables in `bronze_staging`. | 
| | `02_Staging_Quality_Gate_and_Promotion` — Reviews error tables, reports quality summary, and promotes clean tables to `bronze_final`. |
| Notebooks Executed **After** (Downstream) | `04_Bronze_Data_Profiling_and_EDA/4.1_Basic_Data_Profiling` — Basic data profiling of the promoted `bronze_final` tables. |

---

## Validation Summary

| Metric | Value |
| --- | --- |
| Total Rows | 150,346 |
| Clean Rows | 150,346 |
| Error Rows | 0 |
| Pass Rate | 100% |
| Schema Status | **PASS — No Issues Found** |

## Schema Issues Found

**None.** All 150,346 rows passed schema validation successfully. Every row contains the expected 14 top-level fields with no missing or extra columns detected.

## Validated Schema (14 Fields)

| # | Column Name | Data Type |
| --- | --- | --- |
| 1 | `business_id` | STRING |
| 2 | `name` | STRING |
| 3 | `address` | STRING |
| 4 | `city` | STRING |
| 5 | `state` | STRING |
| 6 | `postal_code` | STRING |
| 7 | `latitude` | DOUBLE |
| 8 | `longitude` | DOUBLE |
| 9 | `stars` | DOUBLE |
| 10 | `review_count` | LONG |
| 11 | `is_open` | LONG |
| 12 | `attributes` | STRUCT |
| 13 | `categories` | STRING |
| 14 | `hours` | STRUCT |

## Promotion Status

* **Staging Clean Table:** `yelp_poc_uc.bronze_staging.yelp_business_clean` (150,346 rows)
* **Staging Errors Table:** `yelp_poc_uc.bronze_staging.yelp_business_errors` (0 rows)
* **Final Table:** `yelp_poc_uc.bronze_final.yelp_business` (150,346 rows)
* **Promoted:** Yes — all clean rows promoted to `bronze_final` with zero data loss

## Validation Method

Schema validation was performed by notebook `01_Landing_to_Staging_Schema_Validator` in the `03_Schema_Validation_and_Promotion` folder. The first non-empty JSON line was used as the expected top-level field template. Each row was checked for:

1. Valid JSON syntax
2. All expected top-level fields present (no missing)
3. No unexpected top-level fields (no extras)

## Solutions Applied

No solutions were required — the dataset passed validation without any schema issues.
