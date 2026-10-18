# Basic Data Profiling — Checkin Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_checkin`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This document presents the **Basic Exploratory Data Analysis (EDA)** results for the `yelp_poc_uc.bronze_final.yelp_checkin` table. It covers schema overview, null/empty analysis, distinct value counts, and key initial observations.

> **\u26a0\ufe0f Accuracy Disclaimer:** These profiling results are based on the **Bronze Final** layer where data types and constraints have **not yet been applied**. The `date` column stores multiple comma-separated timestamps as a single flat string — all statistics reflect this raw representation. Accurate, constraint-aware profiling will follow the Bronze \u2192 Silver transformation.

> **Source Notebook:** `4.2.2_Checkin_Deep_EDA`

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_checkin` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_checkin.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count (Bronze) | 131,930 (1 row per business) |
| Row Count (Post-Explode, estimated) | ~13,356,875 (1 row per checkin event) |
| Column Count | 2 |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_checkin_clean` / `yelp_checkin_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_checkin` | Bronze Final |
| **4** | **`4.1.2_Checkin_Basic_Profile`** | _(reads `bronze_final.yelp_checkin`)_ | **YOU ARE HERE - Basic EDA** |
| 5 | `4.2.2_Checkin_Deep_EDA` | _(reads `bronze_final.yelp_checkin`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## 3. Schema Overview

| # | Column | Data Type | Notes |
| --- | --- | --- | --- |
| 1 | `business_id` | `string` | Business identifier (22-char hash) |
| 2 | `date` | `string` | Comma-separated timestamps (e.g., `"2020-03-13 21:10:56, 2020-06-02 22:18:06, ..."`) |

---

## 4. Null & Empty Value Analysis

| Column | Null/Empty Count | Null % | Notes |
| --- | ---: | ---: | --- |
| `business_id` | 0 | 0.00% | Fully populated |
| `date` | 0 | 0.00% | Fully populated (but each value is a packed multi-value string) |

---

## 5. Distinct Value Analysis

| Column | Distinct Count | Uniqueness % | Notes |
| --- | ---: | ---: | --- |
| `business_id` | 131,930 | 100.00% | 1:1 mapping — one row per business |
| `date` | 131,930 | 100.00% | Every business has a unique timestamp combination |

---

## 6. Descriptive Statistics

This table has **no numeric columns** in its Bronze form. The only quantifiable metric is the number of timestamps packed into each `date` string.

| Metric | Value |
| --- | ---: |
| Average timestamps per row | ~101.2 |
| Total checkin events (estimated) | ~13,356,875 |

---

## 7. Key Observations

1. **`business_id` is 100% unique** — the table grain is one row per business.
2. **`date` column is a denormalized multi-value string** — each row contains an average of ~101 comma-separated timestamps packed into a single field. This violates First Normal Form (1NF).
3. **No numeric columns exist** at the Bronze level — all meaningful numeric analysis (event counts, temporal distributions) requires first exploding the `date` string.
4. **No NULLs in either column** — every business in this table has at least one checkin event.
5. **The true event count is ~13.4M** — hidden within the 131,930 rows. The Silver layer should explode these into individual rows with proper `TimestampType` columns.
6. **`date` stored as `string`** instead of `array<timestamp>` — this is the primary data type issue to resolve at the Silver layer.
7. **Not all businesses have checkins** — the 131,930 rows here vs. 150,346 businesses means ~18,416 businesses (12.2%) have zero checkin records.
