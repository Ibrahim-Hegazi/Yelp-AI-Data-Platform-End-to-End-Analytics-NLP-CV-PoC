# Schema Specification Document — Bronze Landing Layer (V1)

> **Layer:** Bronze Landing  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_landing`  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-17  

---

## 0. Document Description

This Schema Specification Document describes the **Bronze Landing** layer of the Yelp AI Data Platform PoC. It captures the current state of raw data as it lands in Unity Catalog before any transformation or validation.

> **Important:** In a production setting, this document should be provided or reviewed by a **real business stakeholder / data owner** who can supply authoritative definitions for column semantics, expected value ranges, business rules, and data quality requirements. The current version is auto-generated from file inspection and is **not a complete specification** — a thorough Exploratory Data Analysis (EDA) has not yet been performed. Once EDA is completed in downstream layers, this document (or a subsequent version) will be updated with validated datatypes, nullability rules, uniqueness constraints, key designations, and default values.

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Path | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset` |
| Storage Type | Unity Catalog External Volume |
| Environment | Azure Databricks |

---

## 2. Pipeline Context

| Property | Value |
| --- | --- |
| Notebook Executed **Before** (Upstream) | `03_Extract_Yelp_Dataset_from_Kaggle` — Downloads the raw Yelp academic dataset from Kaggle and lands JSON files into the volume. |
| Notebook Executed **After** (Downstream) | `01_Landing_to_Staging_Schema_Validator` — Reads raw JSON files, applies schema validation, and writes clean/error rows into `bronze_staging`. |

---

## 3. File Formats

| Property | Value |
| --- | --- |
| Format | JSON (newline-delimited / JSON Lines — one JSON object per line) |
| Encoding | UTF-8 |
| Compression | None |

> **Note:** A `Dataset_User_Agreement.pdf` also resides in the same volume directory (non-data asset).

---

## 4. Tables / Files Inventory

| # | File Name | Dataset |
| --- | --- | --- |
| 1 | `yelp_academic_dataset_business.json` | Business |
| 2 | `yelp_academic_dataset_checkin.json` | Check-in |
| 3 | `yelp_academic_dataset_review.json` | Review |
| 4 | `yelp_academic_dataset_tip.json` | Tip |
| 5 | `yelp_academic_dataset_user.json` | User |

---

## 5. Column-Level Schema Specification

### 5.1 `yelp_academic_dataset_business.json` — 14 Columns

| # | Column Name | Datatype | Nullable | Uniqueness | Key | Default Value | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `business_id` | TBD | TBD | TBD | TBD | N/A | 22-char hash identifier |
| 2 | `name` | TBD | TBD | TBD | N/A | N/A | Business display name |
| 3 | `address` | TBD | TBD | TBD | N/A | N/A | Street address |
| 4 | `city` | TBD | TBD | TBD | N/A | N/A | City name |
| 5 | `state` | TBD | TBD | TBD | N/A | N/A | Two-letter state code |
| 6 | `postal_code` | TBD | TBD | TBD | N/A | N/A | ZIP / postal code |
| 7 | `latitude` | TBD | TBD | TBD | N/A | N/A | Geographic latitude |
| 8 | `longitude` | TBD | TBD | TBD | N/A | N/A | Geographic longitude |
| 9 | `stars` | TBD | TBD | TBD | N/A | N/A | Star rating (1.0–5.0) |
| 10 | `review_count` | TBD | TBD | TBD | N/A | N/A | Total review count |
| 11 | `is_open` | TBD | TBD | TBD | N/A | N/A | 0 = closed, 1 = open |
| 12 | `attributes` | TBD | TBD | TBD | N/A | N/A | Variable-key dict of business attributes (e.g. BikeParking, WiFi, BusinessParking). Key set varies per record. |
| 13 | `categories` | TBD | TBD | TBD | N/A | N/A | Comma-separated category list |
| 14 | `hours` | TBD | TBD | TBD | N/A | N/A | Keys are day names (Monday–Sunday), values are "HH:MM-HH:MM" ranges. Can be null. |

---

### 5.2 `yelp_academic_dataset_checkin.json` — 2 Columns

| # | Column Name | Datatype | Nullable | Uniqueness | Key | Default Value | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `business_id` | TBD | TBD | TBD | TBD | N/A | 22-char hash identifier |
| 2 | `date` | TBD | TBD | TBD | N/A | N/A | Comma-separated list of check-in timestamps |

---

### 5.3 `yelp_academic_dataset_review.json` — 9 Columns

| # | Column Name | Datatype | Nullable | Uniqueness | Key | Default Value | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `review_id` | TBD | TBD | TBD | TBD | N/A | 22-char hash identifier |
| 2 | `user_id` | TBD | TBD | TBD | TBD | N/A | References `yelp_user.user_id` |
| 3 | `business_id` | TBD | TBD | TBD | TBD | N/A | References `yelp_business.business_id` |
| 4 | `stars` | TBD | TBD | TBD | N/A | N/A | Rating given (1.0–5.0) |
| 5 | `useful` | TBD | TBD | TBD | N/A | N/A | Count of "useful" votes |
| 6 | `funny` | TBD | TBD | TBD | N/A | N/A | Count of "funny" votes |
| 7 | `cool` | TBD | TBD | TBD | N/A | N/A | Count of "cool" votes |
| 8 | `text` | TBD | TBD | TBD | N/A | N/A | Full review text |
| 9 | `date` | TBD | TBD | TBD | N/A | N/A | Review timestamp ("YYYY-MM-DD HH:MM:SS") |

---

### 5.4 `yelp_academic_dataset_tip.json` — 5 Columns

| # | Column Name | Datatype | Nullable | Uniqueness | Key | Default Value | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `user_id` | TBD | TBD | TBD | TBD | N/A | References `yelp_user.user_id` |
| 2 | `business_id` | TBD | TBD | TBD | TBD | N/A | References `yelp_business.business_id` |
| 3 | `text` | TBD | TBD | TBD | N/A | N/A | Tip text content |
| 4 | `date` | TBD | TBD | TBD | N/A | N/A | Tip timestamp ("YYYY-MM-DD HH:MM:SS") |
| 5 | `compliment_count` | TBD | TBD | TBD | N/A | N/A | Number of compliments on the tip |

---

### 5.5 `yelp_academic_dataset_user.json` — 22 Columns

| # | Column Name | Datatype | Nullable | Uniqueness | Key | Default Value | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `user_id` | TBD | TBD | TBD | TBD | N/A | 22-char hash identifier |
| 2 | `name` | TBD | TBD | TBD | N/A | N/A | User display name |
| 3 | `review_count` | TBD | TBD | TBD | N/A | N/A | Total reviews written |
| 4 | `yelping_since` | TBD | TBD | TBD | N/A | N/A | Registration timestamp ("YYYY-MM-DD HH:MM:SS") |
| 5 | `useful` | TBD | TBD | TBD | N/A | N/A | Total "useful" votes received |
| 6 | `funny` | TBD | TBD | TBD | N/A | N/A | Total "funny" votes received |
| 7 | `cool` | TBD | TBD | TBD | N/A | N/A | Total "cool" votes received |
| 8 | `elite` | TBD | TBD | TBD | N/A | N/A | Comma-separated years of elite status (or empty) |
| 9 | `friends` | TBD | TBD | TBD | N/A | N/A | Comma-separated list of friend user_id hashes |
| 10 | `fans` | TBD | TBD | TBD | N/A | N/A | Number of fans |
| 11 | `average_stars` | TBD | TBD | TBD | N/A | N/A | Average star rating given |
| 12 | `compliment_hot` | TBD | TBD | TBD | N/A | N/A | "Hot" compliments received |
| 13 | `compliment_more` | TBD | TBD | TBD | N/A | N/A | "More" compliments received |
| 14 | `compliment_profile` | TBD | TBD | TBD | N/A | N/A | "Profile" compliments received |
| 15 | `compliment_cute` | TBD | TBD | TBD | N/A | N/A | "Cute" compliments received |
| 16 | `compliment_list` | TBD | TBD | TBD | N/A | N/A | "List" compliments received |
| 17 | `compliment_note` | TBD | TBD | TBD | N/A | N/A | "Note" compliments received |
| 18 | `compliment_plain` | TBD | TBD | TBD | N/A | N/A | "Plain" compliments received |
| 19 | `compliment_cool` | TBD | TBD | TBD | N/A | N/A | "Cool" compliments received |
| 20 | `compliment_funny` | TBD | TBD | TBD | N/A | N/A | "Funny" compliments received |
| 21 | `compliment_writer` | TBD | TBD | TBD | N/A | N/A | "Writer" compliments received |
| 22 | `compliment_photos` | TBD | TBD | TBD | N/A | N/A | "Photos" compliments received |

---

## 6. TBD Fields — Rationale

The following properties are marked **TBD** (To Be Determined) across all tables because they **cannot be reliably inferred at the Bronze Landing stage** where data remains in raw JSON Lines format:

| Property | Reason |
| --- | --- |
| **Datatypes** | JSON is loosely typed. Formal Spark/Delta schema types cannot be determined at the raw file level and will only be assigned during the `Landing_to_Staging` validation step after EDA is completed. |
| **Nullable** | JSON files have no NOT NULL constraint. Nullability rules will be defined and enforced in `bronze_staging` via the schema validator notebook. |
| **Uniqueness** | Cannot be guaranteed at the raw file level. Uniqueness constraints will be profiled and enforced downstream. |
| **Keys (PK / FK)** | Primary and foreign key relationships are logical design decisions applied in `bronze_staging` and `bronze_final`. At the landing layer, these are simply raw field values with no referential integrity enforcement. |
| **Default Values** | JSON source files do not define defaults. Default/fallback values will be applied during schema validation in the staging layer. |

---

## 7. Cross-Reference: Expected Key Relationships (Logical — Not Yet Enforced)

The following key relationships are **anticipated** based on the Yelp data model and will be formally validated in downstream layers:

| Dataset | Candidate PK | FK References |
| --- | --- | --- |
| Business | `business_id` | — |
| Check-in | `business_id` (one row per business) | `business_id` → Business |
| Review | `review_id` | `user_id` → User, `business_id` → Business |
| Tip | Composite: (`user_id`, `business_id`, `date`) | `user_id` → User, `business_id` → Business |
| User | `user_id` | — |

> **Disclaimer:** These are logical expectations only. Actual uniqueness and referential integrity will be validated in `bronze_staging` / `bronze_final`.

---

## Document History

| Version | Date | Author | Changes |
| --- | --- | --- | --- |
| 1.0 | 2026-10-17 | Auto-generated | Initial Bronze Landing schema specification |
