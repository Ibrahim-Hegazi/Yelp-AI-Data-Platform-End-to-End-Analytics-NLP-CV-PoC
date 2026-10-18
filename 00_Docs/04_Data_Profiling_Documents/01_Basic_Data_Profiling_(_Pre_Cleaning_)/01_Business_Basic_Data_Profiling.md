# Basic Data Profiling — Business Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_business`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This document presents the **Basic Exploratory Data Analysis (EDA)** results for the `yelp_poc_uc.bronze_final.yelp_business` table. It covers schema overview, null/empty analysis, distinct value counts, descriptive statistics for numeric columns, and key initial observations.

> **\u26a0\ufe0f Accuracy Disclaimer:** These profiling results are based on the **Bronze Final** layer where data types and constraints have **not yet been applied**. Statistics should be treated as approximate baselines. Accurate, constraint-aware profiling will follow the Bronze \u2192 Silver transformation.

> **Source Notebook:** `4.2.1_Business_Deep_EDA` (planned)

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_business` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_business.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count | 150,346 |
| Column Count | 14 |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_business_clean` / `yelp_business_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_business` | Bronze Final |
| **4** | **`4.1.1_Business_Basic_Profile`** | _(reads `bronze_final.yelp_business`)_ | **YOU ARE HERE - Basic EDA** |
| 5 | `4.2.1_Business_Deep_EDA` _(planned)_ | _(reads `bronze_final.yelp_business`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## 3. Schema Overview

| # | Column | Data Type | Notes |
| --- | --- | --- | --- |
| 1 | `business_id` | `string` | Unique business identifier (22-char hash) |
| 2 | `name` | `string` | Business name |
| 3 | `address` | `string` | Street address |
| 4 | `city` | `string` | City name |
| 5 | `state` | `string` | State/province abbreviation |
| 6 | `postal_code` | `string` | Postal/ZIP code |
| 7 | `latitude` | `double` | Geographic latitude |
| 8 | `longitude` | `double` | Geographic longitude |
| 9 | `stars` | `double` | Average star rating (1.0\u20135.0 in 0.5 increments) |
| 10 | `review_count` | `bigint` | Total number of reviews |
| 11 | `is_open` | `bigint` | Open (1) or closed (0) — stored as integer, not boolean |
| 12 | `attributes` | `struct<39 fields>` | Business attributes (parking, WiFi, ambience, etc.) — all sub-fields are strings |
| 13 | `categories` | `string` | Comma-separated category labels |
| 14 | `hours` | `struct<7 fields>` | Operating hours per day of week — all sub-fields are strings |

---

## 4. Null & Empty Value Analysis

| Column | Null/Empty Count | Null % | Notes |
| --- | ---: | ---: | --- |
| `business_id` | 0 | 0.00% | Fully populated |
| `name` | 0 | 0.00% | Fully populated |
| `address` | 5,127 | 3.41% | Missing addresses (P.O. boxes, virtual businesses?) |
| `city` | 0 | 0.00% | Fully populated |
| `state` | 0 | 0.00% | Fully populated |
| `postal_code` | 73 | 0.05% | Near-complete |
| `latitude` | 0 | 0.00% | Fully populated |
| `longitude` | 0 | 0.00% | Fully populated |
| `stars` | 0 | 0.00% | Fully populated |
| `review_count` | 0 | 0.00% | Fully populated |
| `is_open` | 0 | 0.00% | Fully populated |
| `attributes` | 13,744 | 9.14% | Struct-level NULLs — no attribute data for these businesses |
| `categories` | 103 | 0.07% | Near-complete |
| `hours` | 23,223 | 15.45% | Struct-level NULLs — no hours data (may be by-appointment-only businesses) |

---

## 5. Distinct Value Analysis

| Column | Distinct Count | Uniqueness % | Notes |
| --- | ---: | ---: | --- |
| `business_id` | 150,346 | 100.00% | Candidate primary key |
| `name` | 114,117 | 75.90% | Many shared names (chains, common names) |
| `address` | 122,844 | 81.71% | Some co-located businesses |
| `city` | 1,416 | 0.94% | Low cardinality — categorical |
| `state` | 27 | 0.02% | Low cardinality — US states + Canadian provinces |
| `postal_code` | 3,362 | 2.24% | Moderate cardinality |
| `latitude` | 135,593 | 90.19% | Near-unique (geographic precision) |
| `longitude` | 131,918 | 87.74% | Near-unique (geographic precision) |
| `stars` | 9 | 0.01% | 9 distinct values: 1.0, 1.5, 2.0, ... 5.0 |
| `review_count` | 1,158 | 0.77% | Wide range of review volumes |
| `is_open` | 2 | 0.00% | Binary: 0 or 1 |
| `categories` | 83,160 | 55.31% | High cardinality (many unique category combos) |

---

## 6. Descriptive Statistics (Numeric Columns)

| Column | Min | Max | Mean | Std Dev | Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `latitude` | 27.5551 | 53.6792 | 36.6712 | 5.8728 | 38.7776 |
| `longitude` | -120.0951 | -73.2005 | -89.3573 | 14.9185 | -86.1212 |
| `stars` | 1.0 | 5.0 | 3.5967 | 0.9744 | 3.5 |
| `review_count` | 5 | 7,568 | 44.87 | 121.12 | 15 |
| `is_open` | 0 | 1 | 0.7962 | 0.4029 | 1 |

---

## 7. Key Distributions

### 7.1 Star Rating Distribution

| Stars | Count | % of Total |
| ---: | ---: | ---: |
| 1.0 | 1,986 | 1.32% |
| 1.5 | 4,932 | 3.28% |
| 2.0 | 9,527 | 6.34% |
| 2.5 | 14,316 | 9.52% |
| 3.0 | 18,453 | 12.27% |
| 3.5 | 26,519 | 17.64% |
| 4.0 | 31,125 | 20.70% |
| 4.5 | 27,181 | 18.08% |
| 5.0 | 16,307 | 10.85% |

### 7.2 Open/Closed Distribution

| Status | Count | % of Total |
| --- | ---: | ---: |
| Open (`is_open = 1`) | 119,698 | 79.62% |
| Closed (`is_open = 0`) | 30,648 | 20.38% |

### 7.3 Top 10 Cities

| City | Count |
| --- | ---: |
| Philadelphia | 14,569 |
| Tucson | 9,250 |
| Tampa | 9,050 |
| Indianapolis | 7,540 |
| Nashville | 6,971 |
| New Orleans | 6,209 |
| Reno | 5,935 |
| Edmonton | 5,054 |
| Saint Louis | 4,827 |
| Santa Barbara | 3,829 |

### 7.4 Top 10 States

| State | Count |
| --- | ---: |
| PA | 34,039 |
| FL | 26,330 |
| TN | 12,056 |
| IN | 11,247 |
| MO | 10,913 |
| LA | 9,924 |
| AZ | 9,912 |
| NJ | 8,536 |
| NV | 7,715 |
| AB | 5,573 |

---

## 8. Initial Observations

1. **`business_id` is 100% unique** — strong candidate for primary key.
2. **`address` has 3.41% missing values** — likely virtual/mobile businesses or P.O. boxes.
3. **`attributes` struct is NULL for 9.14% of rows** — these businesses have no attribute metadata at all (not just empty sub-fields).
4. **`hours` struct is NULL for 15.45% of rows** — significant portion of businesses lack operating hours data.
5. **`is_open` is stored as `bigint`** instead of `boolean` — type correction needed at Silver layer.
6. **`stars` is stored as `double`** with 9 discrete values (0.5 increments) — could be modeled as a constrained decimal or categorical.
7. **`categories` is a comma-separated string** — will need parsing/exploding for category-level analysis at Silver.
8. **`attributes` sub-fields are all strings** — many contain serialized Python-style dicts (e.g., `"{'garage': True, 'street': False}"`), requiring parsing at Silver.
9. **Geographic coverage** spans US and parts of Canada (AB province present), with concentration in PA, FL, TN, IN.
10. **`review_count` minimum is 5** — dataset may have been pre-filtered to exclude very-low-activity businesses.
