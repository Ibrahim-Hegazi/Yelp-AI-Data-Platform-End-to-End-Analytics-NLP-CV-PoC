# Yelp User Dataset — Schema Validation Report

## Source File

`yelp_academic_dataset_user.json`

**Location:** `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_user.json`

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
| Total Rows | 1,987,897 |
| Clean Rows | 1,987,897 |
| Error Rows | 0 |
| Pass Rate | 100% |
| Schema Status | **PASS — No Issues Found** |

## Schema Issues Found

**None.** All 1,987,897 rows passed schema validation successfully. Every row contains the expected 22 top-level fields with no missing or extra columns detected.

## Validated Schema (22 Fields)

| # | Column Name | Data Type |
| --- | --- | --- |
| 1 | `user_id` | STRING |
| 2 | `name` | STRING |
| 3 | `review_count` | LONG |
| 4 | `yelping_since` | STRING |
| 5 | `useful` | LONG |
| 6 | `funny` | LONG |
| 7 | `cool` | LONG |
| 8 | `elite` | STRING |
| 9 | `friends` | STRING |
| 10 | `fans` | LONG |
| 11 | `average_stars` | DOUBLE |
| 12 | `compliment_hot` | LONG |
| 13 | `compliment_more` | LONG |
| 14 | `compliment_profile` | LONG |
| 15 | `compliment_cute` | LONG |
| 16 | `compliment_list` | LONG |
| 17 | `compliment_note` | LONG |
| 18 | `compliment_plain` | LONG |
| 19 | `compliment_cool` | LONG |
| 20 | `compliment_funny` | LONG |
| 21 | `compliment_writer` | LONG |
| 22 | `compliment_photos` | LONG |

## Promotion Status

* **Staging Clean Table:** `yelp_poc_uc.bronze_staging.yelp_user_clean` (1,987,897 rows)
* **Staging Errors Table:** `yelp_poc_uc.bronze_staging.yelp_user_errors` (0 rows)
* **Final Table:** `yelp_poc_uc.bronze_final.yelp_user` (1,987,897 rows)
* **Promoted:** Yes — all clean rows promoted to `bronze_final` with zero data loss

## Validation Method

Schema validation was performed by notebook `01_Landing_to_Staging_Schema_Validator` in the `03_Schema_Validation_and_Promotion` folder. The first non-empty JSON line was used as the expected top-level field template. Each row was checked for:

1. Valid JSON syntax
2. All expected top-level fields present (no missing)
3. No unexpected top-level fields (no extras)

## Solutions Applied

No solutions were required — the dataset passed validation without any schema issues.
