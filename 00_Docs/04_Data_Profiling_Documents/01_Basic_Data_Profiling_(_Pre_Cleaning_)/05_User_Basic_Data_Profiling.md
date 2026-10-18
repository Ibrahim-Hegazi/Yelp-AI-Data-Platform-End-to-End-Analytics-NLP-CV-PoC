# Basic Data Profiling — User Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_user`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This document presents the **Basic Exploratory Data Analysis (EDA)** results for the `yelp_poc_uc.bronze_final.yelp_user` table. It covers schema overview, null/empty analysis, distinct value counts, descriptive statistics for numeric columns, and key initial observations.

> **\u26a0\ufe0f Accuracy Disclaimer:** These profiling results are based on the **Bronze Final** layer where data types and constraints have **not yet been applied**. The `yelping_since` column is stored as `string` rather than `TimestampType`, `elite` is a comma-separated string of years, and `friends` is a comma-separated string of user IDs. Accurate, constraint-aware profiling will follow the Bronze \u2192 Silver transformation.

> **Source Notebook:** `4.2.5_User_Deep_EDA` (planned)

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_user` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_user.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count | 1,987,897 |
| Column Count | 22 |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_user_clean` / `yelp_user_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_user` | Bronze Final |
| **4** | **`4.1.5_User_Basic_Profile`** | _(reads `bronze_final.yelp_user`)_ | **YOU ARE HERE - Basic EDA** |
| 5 | `4.2.5_User_Deep_EDA` _(planned)_ | _(reads `bronze_final.yelp_user`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

---

## 3. Schema Overview

| # | Column | Data Type | Notes |
| --- | --- | --- | --- |
| 1 | `user_id` | `string` | Unique user identifier (22-char hash) |
| 2 | `name` | `string` | User’s display name |
| 3 | `review_count` | `bigint` | Total number of reviews written |
| 4 | `yelping_since` | `string` | Registration timestamp (stored as string: `"YYYY-MM-DD HH:MM:SS"`) |
| 5 | `useful` | `bigint` | Total "useful" votes received across all reviews |
| 6 | `funny` | `bigint` | Total "funny" votes received across all reviews |
| 7 | `cool` | `bigint` | Total "cool" votes received across all reviews |
| 8 | `elite` | `string` | Comma-separated years of elite status (e.g., `"2015,2016,2017"`) or empty string |
| 9 | `friends` | `string` | Comma-separated user_id hashes of friends, or `"None"` |
| 10 | `fans` | `bigint` | Number of fans/followers |
| 11 | `average_stars` | `double` | Average star rating given by this user |
| 12 | `compliment_hot` | `bigint` | "Hot" compliments received |
| 13 | `compliment_more` | `bigint` | "More" compliments received |
| 14 | `compliment_profile` | `bigint` | "Profile" compliments received |
| 15 | `compliment_cute` | `bigint` | "Cute" compliments received |
| 16 | `compliment_list` | `bigint` | "List" compliments received |
| 17 | `compliment_note` | `bigint` | "Note" compliments received |
| 18 | `compliment_plain` | `bigint` | "Plain" compliments received |
| 19 | `compliment_cool` | `bigint` | "Cool" compliments received |
| 20 | `compliment_funny` | `bigint` | "Funny" compliments received |
| 21 | `compliment_writer` | `bigint` | "Writer" compliments received |
| 22 | `compliment_photos` | `bigint` | "Photos" compliments received |

---

## 4. Null & Empty Value Analysis

| Column | Null/Empty Count | Null % | Notes |
| --- | ---: | ---: | --- |
| `user_id` | 0 | 0.00% | Fully populated |
| `name` | 0 | 0.00% | Fully populated |
| `review_count` | 0 | 0.00% | Fully populated |
| `yelping_since` | 0 | 0.00% | Fully populated |
| `useful` | 0 | 0.00% | Fully populated |
| `funny` | 0 | 0.00% | Fully populated |
| `cool` | 0 | 0.00% | Fully populated |
| `elite` | 1,896,699 | 95.41% | Empty string for non-elite users |
| `friends` | 0 | 0.00% | Populated (but many contain `"None"` as a string literal) |
| `fans` | 0 | 0.00% | Fully populated |
| `average_stars` | 0 | 0.00% | Fully populated |
| `compliment_hot` | 0 | 0.00% | Fully populated |
| `compliment_more` | 0 | 0.00% | Fully populated |
| `compliment_profile` | 0 | 0.00% | Fully populated |
| `compliment_cute` | 0 | 0.00% | Fully populated |
| `compliment_list` | 0 | 0.00% | Fully populated |
| `compliment_note` | 0 | 0.00% | Fully populated |
| `compliment_plain` | 0 | 0.00% | Fully populated |
| `compliment_cool` | 0 | 0.00% | Fully populated |
| `compliment_funny` | 0 | 0.00% | Fully populated |
| `compliment_writer` | 0 | 0.00% | Fully populated |
| `compliment_photos` | 0 | 0.00% | Fully populated |

---

## 5. Distinct Value Analysis

| Column | Distinct Count | Uniqueness % | Notes |
| --- | ---: | ---: | --- |
| `user_id` | 1,987,897 | 100.00% | Confirmed primary key |
| `name` | 132,361 | 6.66% | Many shared display names |
| `review_count` | 1,973 | 0.10% | Wide range of activity levels |
| `yelping_since` | 1,980,183 | 99.61% | Near-unique registration timestamps |
| `useful` | 5,349 | 0.27% | Wide range |
| `funny` | 3,757 | 0.19% | Wide range |
| `cool` | 4,532 | 0.23% | Wide range |
| `elite` | 1,356 | 0.07% | 1,356 distinct year-combination patterns |
| `friends` | 1,091,999 | 54.93% | Moderate uniqueness (many unique friend lists) |
| `fans` | 691 | 0.03% | Low cardinality relative to row count |
| `average_stars` | 401 | 0.02% | Discrete range of average ratings |
| `compliment_hot` | 1,312 | 0.07% | Low cardinality, heavy zero-skew |
| `compliment_more` | 352 | 0.02% | Low cardinality |
| `compliment_profile` | 345 | 0.02% | Low cardinality |
| `compliment_cute` | 308 | 0.02% | Low cardinality |
| `compliment_list` | 193 | 0.01% | Low cardinality |
| `compliment_note` | 1,010 | 0.05% | Low cardinality |
| `compliment_plain` | 1,663 | 0.08% | Highest cardinality among compliments |
| `compliment_cool` | 1,604 | 0.08% | Same as `compliment_funny` |
| `compliment_funny` | 1,604 | 0.08% | Same as `compliment_cool` |
| `compliment_writer` | 822 | 0.04% | Low cardinality |
| `compliment_photos` | 1,018 | 0.05% | Low cardinality |

---

## 6. Descriptive Statistics (Numeric Columns)

| Column | Min | Max | Mean | Std Dev | Median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `review_count` | 0 | 17,473 | 23.39 | 82.57 | 5 |
| `useful` | 0 | 206,296 | 42.30 | 641.48 | 3 |
| `funny` | 0 | 185,823 | 16.97 | 407.80 | 0 |
| `cool` | 0 | 199,878 | 23.79 | 565.35 | 0 |
| `fans` | 0 | 12,497 | 1.47 | 18.13 | 0 |
| `average_stars` | 1.0 | 5.0 | 3.6305 | 1.1833 | 3.88 |
| `compliment_hot` | 0 | 25,784 | 1.81 | 73.60 | 0 |
| `compliment_more` | 0 | 13,501 | 0.29 | 12.82 | 0 |
| `compliment_profile` | 0 | 14,180 | 0.18 | 15.16 | 0 |
| `compliment_cute` | 0 | 13,654 | 0.13 | 11.36 | 0 |
| `compliment_list` | 0 | 12,669 | 0.06 | 10.04 | 0 |
| `compliment_note` | 0 | 59,031 | 1.44 | 60.17 | 0 |
| `compliment_plain` | 0 | 101,097 | 3.01 | 119.39 | 0 |
| `compliment_cool` | 0 | 49,967 | 2.83 | 96.66 | 0 |
| `compliment_funny` | 0 | 49,967 | 2.83 | 96.66 | 0 |
| `compliment_writer` | 0 | 15,934 | 1.06 | 32.18 | 0 |
| `compliment_photos` | 0 | 82,630 | 1.23 | 95.16 | 0 |

---

## 7. Key Distributions

### 7.1 Elite Status

| Metric | Value |
| --- | ---: |
| Elite users (non-empty `elite`) | 91,198 |
| Non-elite users | 1,896,699 |
| Elite percentage | 4.59% |

### 7.2 Registration Date Range

| Metric | Value |
| --- | --- |
| Earliest registration | 2004-10-12 08:46:11 |
| Latest registration | 2022-01-19 17:15:47 |
| Span | ~17.3 years |

---

## 8. Initial Observations

1. **`user_id` is 100% unique** — confirmed primary key.
2. **`elite` is empty for 95.41% of users** — only 91,198 users (4.59%) have ever held elite status. The column stores comma-separated years as a string, requiring parsing at Silver.
3. **`friends` contains the string literal `"None"`** for users with no friends — not a true NULL. This sentinel value needs standardisation at Silver.
4. **`yelping_since` stored as `string`** instead of `TimestampType` — type correction needed at Silver layer.
5. **All 11 compliment columns are heavily zero-skewed** — medians are 0 across the board, with extreme outliers (max values in the tens of thousands). The standard deviations far exceed the means, indicating highly right-skewed distributions.
6. **`compliment_cool` and `compliment_funny` have identical statistics** — same distinct count (1,604), same min/max/mean/stddev/median. This suggests they may be mechanically linked or mirrored on the platform.
7. **`review_count` has minimum of 0** — unlike the business table (min 5), some users exist with zero reviews. These may be users who only left tips or were created but never posted.
8. **132,361 distinct display names** across ~2M users — names are not unique and cannot serve as identifiers.
9. **`friends` has 54.93% uniqueness** — over a million distinct friend-list patterns, stored as comma-separated user_id hashes in a single string field. This will need exploding for social-graph analysis.
10. **Registrations span 2004\u20132022** — the earliest users predate the review window (2005), indicating the platform had users before the dataset’s review coverage begins.
