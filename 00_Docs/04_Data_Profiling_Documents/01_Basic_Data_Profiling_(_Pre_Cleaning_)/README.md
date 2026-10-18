# Basic Data Profiling (Pre-Cleaning) — Yelp PoC Datasets

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Profiling Stage:** Pre-Cleaning (Raw Bronze)  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-18  

---

## 0. Document Description

This folder contains the results of the **Basic Exploratory Data Analysis (EDA)** performed on the five Yelp datasets residing in the Bronze Final layer (`yelp_poc_uc.bronze_final`). Each dataset has its own dedicated Markdown file summarising schema overview, row/column counts, null distributions, basic descriptive statistics, and initial observations.

The five profiling reports are:

| # | File | Target Table |
| --- | --- | --- |
| 1 | `01_Business_Basic_Data_Profiling.md` | `yelp_poc_uc.bronze_final.yelp_business` |
| 2 | `02_Checkin_Basic_Data_Profiling.md` | `yelp_poc_uc.bronze_final.yelp_checkin` |
| 3 | `03_Review_Basic_Data_Profiling.md` | `yelp_poc_uc.bronze_final.yelp_review` |
| 4 | `04_Tip_Basic_Data_Profiling.md` | `yelp_poc_uc.bronze_final.yelp_tip` |
| 5 | `05_User_Basic_Data_Profiling.md` | `yelp_poc_uc.bronze_final.yelp_user` |

> **⚠️ Accuracy Disclaimer:**  
> These profiling results should be interpreted with caution. The Bronze Final layer data **does not yet have the correct data types or constraints applied**. All columns are stored in their raw ingested types (predominantly `string` and loosely typed numerics), and no primary keys, foreign keys, NOT NULL constraints, or check constraints have been enforced at this stage. As a result:
>
> - **Descriptive statistics** (min, max, mean, stddev) on numeric-looking columns may be computed on string representations, leading to lexicographic rather than numeric ordering.
> - **Null/missing value counts** may undercount issues because empty strings, whitespace, and sentinel values (e.g., `"None"`, `"N/A"`) have not yet been standardised to true NULLs.
> - **Uniqueness and cardinality** metrics reflect raw data without deduplication or constraint validation.
> - **Date/time analysis** is limited since temporal columns are stored as strings and have not been cast to `TimestampType` or `DateType`.
>
> Accurate, constraint-aware profiling will be performed after the **Bronze → Silver** transformation, where proper data types, constraints, and cleaning logic are applied.

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog | `yelp_poc_uc` |
| Schema | `bronze_final` |
| Source Files | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` |
| Storage Format | Delta (managed tables) |
| Environment | Azure Databricks |
| Tables Profiled | `yelp_business`, `yelp_checkin`, `yelp_review`, `yelp_tip`, `yelp_user` |

---

## 2. Pipeline Context

These tables sit in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`*_clean` / `*_errors` per dataset) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final` (5 tables) | Bronze Final |
| **4** | **`4.1.*_<Dataset>_Basic_Profile`** | _(reads `bronze_final.*`)_ | **◄ YOU ARE HERE — Basic EDA** |
| 5 | `4.2.*_<Dataset>_Deep_EDA` | _(reads `bronze_final.*`)_ | Deep EDA / DQ Assessment |
| 6 | Silver Transformation _(planned)_ | `yelp_poc_uc.silver` _(planned)_ | Silver |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

The profiling documented in this folder was performed **before any cleaning or type-casting transformations** (Step 4). Its purpose is to establish a raw-state baseline for each dataset — capturing the data as-is so that downstream cleaning decisions (applied during the Bronze → Silver step) are grounded in observed evidence rather than assumptions.

For the deeper, issue-specific data quality analysis and chosen resolutions (Step 5), refer to the companion folder:

> `00_Docs/06_Data_Quality_Issues_and_Solutions_Document/`
