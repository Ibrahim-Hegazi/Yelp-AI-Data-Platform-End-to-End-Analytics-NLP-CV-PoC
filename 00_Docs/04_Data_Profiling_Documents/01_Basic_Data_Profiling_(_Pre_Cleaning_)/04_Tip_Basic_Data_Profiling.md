# Basic Data Profiling — Tip Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_tip`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This document presents the **Basic Exploratory Data Analysis (EDA)** results for the `yelp_poc_uc.bronze_final.yelp_tip` table. It covers schema overview, null/empty analysis, distinct value counts, descriptive statistics, and key initial observations.

> **\u26a0\ufe0f Accuracy Disclaimer:** These profiling results are based on the **Bronze Final** layer where data types and constraints have **not yet been applied**. The `date` column is stored as `string` rather than `TimestampType`, and no primary key or foreign key constraints are enforced. Accurate, constraint-aware profiling will follow the Bronze \u2192 Silver transformation.

> **Source Notebook:** `4.2.4_Tip_Deep_EDA` (planned)

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_tip` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_tip.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count | 908,915 |
| Column Count | 5 |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_tip_clean` / `yelp_tip_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_tip` | Bronze Final |
| **4** | **`4.1.4_Tip_Basic_Profile`** | _(reads `bronze_final.yelp_tip`)_ | **YOU ARE HERE - Basic EDA** |
| 5 | `4.2.4_Tip_Deep_EDA` | _(reads `bronze_final.yelp_tip`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## 3. Schema Overview

| # | Column | Data Type | Notes |
| --- | --- | --- | --- |
| 1 | `user_id` | `string` | Tip author’s user identifier |
| 2 | `business_id` | `string` | Business being tipped |
| 3 | `text` | `string` | Tip text body |
| 4 | `date` | `string` | Tip timestamp (stored as string: `"YYYY-MM-DD HH:MM:SS"`) |
| 5 | `compliment_count` | `bigint` | Number of compliments received on the tip |

---

## 4. Null & Empty Value Analysis

| Column | Null/Empty Count | Null % | Notes |
| --- | ---: | ---: | --- |
| `user_id` | 0 | 0.00% | Fully populated |
| `business_id` | 0 | 0.00% | Fully populated |
| `text` | 0 | 0.00% | Fully populated |
| `date` | 0 | 0.00% | Fully populated |
| `compliment_count` | 0 | 0.00% | Fully populated |

**No NULLs or empty values detected in any column.**

---

## 5. Distinct Value Analysis

| Column | Distinct Count | Uniqueness % | Notes |
| --- | ---: | ---: | --- |
| `user_id` | 301,758 | 33.20% | ~302K unique tip authors across ~909K tips |
| `business_id` | 106,193 | 11.68% | ~106K businesses received tips |
| `text` | 851,033 | 93.63% | High uniqueness; some duplicate tip texts |
| `date` | 906,694 | 99.76% | Near-unique timestamps |
| `compliment_count` | 7 | 0.00% | Values: 0, 1, 2, 3, 4, 5, 6 |

---

## 6. Descriptive Statistics (Numeric Columns)

| Column | Min | Max | Mean | Std Dev | Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `compliment_count` | 0 | 6 | 0.0125 | 0.1208 | 0 |

---

## 7. Key Distributions

### 7.1 Compliment Count Distribution

| Compliment Count | Count | % of Total |
| ---: | ---: | ---: |
| 0 | 898,376 | 98.84% |
| 1 | 9,821 | 1.08% |
| 2 | 617 | 0.07% |
| 3 | 82 | 0.01% |
| 4 | 14 | <0.01% |
| 5 | 3 | <0.01% |
| 6 | 2 | <0.01% |

### 7.2 Tip Text Length

| Metric | Value |
| --- | ---: |
| Min length | 1 character |
| Max length | 500 characters |
| Average length | ~63 characters |

### 7.3 Date Range

| Metric | Value |
| --- | --- |
| Earliest tip | 2009-04-16 13:11:49 |
| Latest tip | 2022-01-19 20:38:55 |
| Span | ~13 years |

---

## 8. Initial Observations

1. **No single-column primary key** — unlike review (`review_id`) or business (`business_id`), tips have no unique identifier column. A composite key (`user_id`, `business_id`, `date`, `text`) would need to be evaluated.
2. **Zero NULLs across all 5 columns** — fully populated at Bronze level.
3. **98.84% of tips have zero compliments** — the `compliment_count` column is extremely sparse, with only 1.16% of tips receiving any compliments.
4. **`compliment_count` has max of 6** — a very small range, potentially capped by the platform.
5. **`date` stored as `string`** instead of `TimestampType` — type correction needed at Silver layer.
6. **Tip text max length is exactly 500** — suggests a hard character limit enforced by the Yelp platform (much shorter than the 5,000-char review limit).
7. **106,193 businesses have tips** vs. 150,346 total businesses — ~44,153 businesses (29.4%) have zero tips.
8. **301,758 unique tip authors** vs. 1,987,897 total users — ~84.8% of users have never left a tip.
9. **Tips span 2009\u20132022** — a shorter window than reviews (2005\u20132022), suggesting the tip feature was introduced later.
10. **93.63% unique tip texts** — some duplicate texts exist (possibly template/automated tips).
