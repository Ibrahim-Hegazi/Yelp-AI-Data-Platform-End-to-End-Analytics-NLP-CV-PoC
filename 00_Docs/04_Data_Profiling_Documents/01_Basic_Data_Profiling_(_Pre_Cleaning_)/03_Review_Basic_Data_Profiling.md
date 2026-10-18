# Basic Data Profiling — Review Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_review`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This document presents the **Basic Exploratory Data Analysis (EDA)** results for the `yelp_poc_uc.bronze_final.yelp_review` table. It covers schema overview, null/empty analysis, distinct value counts, descriptive statistics for numeric columns, and key initial observations.

> **\u26a0\ufe0f Accuracy Disclaimer:** These profiling results are based on the **Bronze Final** layer where data types and constraints have **not yet been applied**. The `date` column is stored as `string` rather than `TimestampType`, and feedback columns (`useful`, `funny`, `cool`) contain negative values that may indicate data quality issues. Accurate, constraint-aware profiling will follow the Bronze \u2192 Silver transformation.

> **Source Notebook:** `4.2.3_Review_Deep_EDA` (planned)

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_review` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_review.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count | 6,990,280 |
| Column Count | 9 |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_review_clean` / `yelp_review_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_review` | Bronze Final |
| **4** | **`4.1.3_Review_Basic_Profile`** | _(reads `bronze_final.yelp_review`)_ | **YOU ARE HERE - Basic EDA** |
| 5 | `4.2.3_Review_Deep_EDA` | _(reads `bronze_final.yelp_review`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## 3. Schema Overview

| # | Column | Data Type | Notes |
| --- | --- | --- | --- |
| 1 | `review_id` | `string` | Unique review identifier (22-char hash) |
| 2 | `user_id` | `string` | Reviewer’s user identifier |
| 3 | `business_id` | `string` | Reviewed business identifier |
| 4 | `stars` | `double` | Star rating (1.0\u20135.0, integer values only) |
| 5 | `useful` | `bigint` | Count of "useful" votes on the review |
| 6 | `funny` | `bigint` | Count of "funny" votes on the review |
| 7 | `cool` | `bigint` | Count of "cool" votes on the review |
| 8 | `text` | `string` | Full review text body |
| 9 | `date` | `string` | Review timestamp (stored as string: `"YYYY-MM-DD HH:MM:SS"`) |

---

## 4. Null & Empty Value Analysis

| Column | Null/Empty Count | Null % | Notes |
| --- | ---: | ---: | --- |
| `review_id` | 0 | 0.00% | Fully populated |
| `user_id` | 0 | 0.00% | Fully populated |
| `business_id` | 0 | 0.00% | Fully populated |
| `stars` | 0 | 0.00% | Fully populated |
| `useful` | 0 | 0.00% | Fully populated |
| `funny` | 0 | 0.00% | Fully populated |
| `cool` | 0 | 0.00% | Fully populated |
| `text` | 0 | 0.00% | Fully populated |
| `date` | 0 | 0.00% | Fully populated |

**No NULLs or empty values detected in any column.**

---

## 5. Distinct Value Analysis

| Column | Distinct Count | Uniqueness % | Notes |
| --- | ---: | ---: | --- |
| `review_id` | 6,990,280 | 100.00% | Candidate primary key |
| `user_id` | 1,987,929 | 28.44% | ~2M unique reviewers across ~7M reviews |
| `business_id` | 150,346 | 2.15% | Matches business table count exactly |
| `stars` | 5 | 0.00% | 5 discrete values: 1.0, 2.0, 3.0, 4.0, 5.0 |
| `useful` | 259 | 0.00% | Wide range of vote counts |
| `funny` | 201 | 0.00% | Wide range of vote counts |
| `cool` | 210 | 0.00% | Wide range of vote counts |
| `text` | 6,974,127 | 99.77% | Near-unique (some duplicate review texts) |
| `date` | 6,885,665 | 98.50% | Near-unique (some same-second submissions) |

---

## 6. Descriptive Statistics (Numeric Columns)

| Column | Min | Max | Mean | Std Dev | Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `stars` | 1.0 | 5.0 | 3.7486 | 1.4787 | 4.0 |
| `useful` | -1 | 1,182 | 1.1846 | 3.2538 | 0 |
| `funny` | -1 | 792 | 0.3266 | 1.6887 | 0 |
| `cool` | -1 | 404 | 0.4986 | 2.1725 | 0 |

---

## 7. Key Distributions

### 7.1 Star Rating Distribution

| Stars | Distinct Values |
| --- | --- |
| 1.0, 2.0, 3.0, 4.0, 5.0 | Integer-only ratings (no half-stars unlike business-level averages) |

### 7.2 Review Text Length

| Metric | Value |
| --- | ---: |
| Min length | 1 character |
| Max length | 5,000 characters |
| Average length | ~568 characters |

### 7.3 Date Range

| Metric | Value |
| --- | --- |
| Earliest review | 2005-02-16 03:23:22 |
| Latest review | 2022-01-19 19:48:45 |
| Span | ~17 years |

---

## 8. Initial Observations

1. **`review_id` is 100% unique** — confirmed primary key candidate.
2. **Zero NULLs across all 9 columns** — the cleanest table in the dataset at Bronze level.
3. **`useful`, `funny`, `cool` contain minimum value of -1** — negative vote counts are semantically invalid and indicate a data quality issue to investigate during Deep EDA.
4. **`date` stored as `string`** instead of `TimestampType` — type correction needed at Silver layer.
5. **`stars` stored as `double`** but only contains integer values (1\u20135) — could be cast to `IntegerType` at Silver.
6. **`text` max length is exactly 5,000** — suggests a hard truncation limit was applied upstream.
7. **`user_id` has 1,987,929 distinct values** vs. 1,987,897 in the user table — 32 orphan reviewer IDs exist in reviews but not in the user table (referential integrity gap).
8. **`business_id` distinct count (150,346) matches the business table exactly** — every business has at least one review (expected given the `review_count >= 5` filter observed in business data).
9. **Review dates span 2005\u20132022** — a 17-year window of review activity.
10. **~99.77% unique review texts** — small number of duplicate texts worth investigating (copy-paste reviews or spam).
