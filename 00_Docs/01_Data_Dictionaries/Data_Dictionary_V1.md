# Data Dictionary V1 — Bronze Landing (Textual Data)

This document serves as the **data dictionary** for the Bronze Landing layer of the Yelp AI Data Platform PoC. It catalogues every raw file landed in the `yelp_textual_dataset` volume — including file locations, sizes, row counts, column definitions, and logical relationships — before any schema validation, type enforcement, or transformation has been applied. Use it as the single source of truth when building downstream Bronze Staging validations or onboarding new team members to the data pipeline.

> **Layer:** Bronze Landing  
> **Last Updated:** October 2026  
> **Maintained By:** Ibrahim Hegazi

> **Important:** In a production setting, this document should be provided or reviewed by a **real business stakeholder / data owner** who can verify that file descriptions, column names, and column definitions accurately reflect the intended meaning of the data. The current version is auto-generated from file inspection and is **not a substitute for domain expertise** — a thorough Exploratory Data Analysis (EDA) has not yet been performed at this layer. Once EDA is completed during the Bronze Data Profiling and EDA phase (folder `04`), and formal schema validation is applied in the Bronze Staging layer, this document (or a subsequent version) will be updated with validated data types and corrected column definitions where needed.

---

## 1. Data Location

| Property | Value |
| --- | --- |
| **Unity Catalog** | `yelp_poc_uc` |
| **Schema** | `bronze_landing` |
| **Volume** | `landing_zone` |
| **Full Volume Path** | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset` |

---

## 2. Pipeline Context

| Step | Notebook | Description |
| --- | --- | --- |
| **Executed Before** | `02_Extract_the_data_from_Kaggle/03_Extract_Yelp_Dataset_from_Kaggle` | Downloads raw JSON files from Kaggle and lands them into the landing zone volume |
| **Executed After** | `03_Schema_Validator_and_Normalizer/01_Landing_to_Staging_Schema_Validator` | Validates JSON schema consistency and splits clean/error rows into `bronze_staging` |

---

## 3. File Inventory

**Total Files:** 6 (5 JSON datasets + 1 PDF agreement)  
**Total Size:** ~8.67 GB  
**File Format:** Newline-delimited JSON (`.json`)

| # | File Name | Format | Size | Rows | Columns |
| --- | --- | --- | --- | --- | --- |
| 1 | `yelp_academic_dataset_business.json` | JSON | 113.4 MB | 150,346 | 14 |
| 2 | `yelp_academic_dataset_checkin.json` | JSON | 273.7 MB | 131,930 | 2 |
| 3 | `yelp_academic_dataset_review.json` | JSON | 4.98 GB | 6,990,280 | 9 |
| 4 | `yelp_academic_dataset_tip.json` | JSON | 172.2 MB | 908,915 | 5 |
| 5 | `yelp_academic_dataset_user.json` | JSON | 3.13 GB | 1,987,897 | 22 |
| 6 | `Dataset_User_Agreement.pdf` | PDF | 0.1 MB | — | — |

---

## 4. File Descriptions

| File | Description |
| --- | --- |
| `yelp_academic_dataset_business.json` | Business listings including name, location (address, city, state, postal code, coordinates), categories, star rating, review count, operating hours, and 39 detailed business attributes (parking, ambience, dietary restrictions, noise level, Wi-Fi, price range, etc.) |
| `yelp_academic_dataset_checkin.json` | Aggregated check-in records per business — each row contains a business ID and a comma-separated string of check-in timestamps |
| `yelp_academic_dataset_review.json` | Individual user reviews for businesses containing the review text, star rating, posting date, and community feedback votes (useful, funny, cool) |
| `yelp_academic_dataset_tip.json` | Short user tips (brief recommendations or notes) for businesses, with a compliment count indicating community endorsement |
| `yelp_academic_dataset_user.json` | User profiles with registration date, review statistics, social graph (friends list), elite status history, and a detailed breakdown of 11 compliment categories |
| `Dataset_User_Agreement.pdf` | Yelp Dataset license terms and usage agreement |

---

## 5. Column Definitions

> **Note:** Data types are **not yet defined** at this layer. Raw JSON files carry no enforced schema metadata — field types, constraints, and keys do not exist in the source files. Formal data type definitions and enforcement begin at the Bronze Staging layer, where data is written to managed Delta tables.

### 5.1 Business (`yelp_academic_dataset_business.json`)

**Rows:** 150,346 | **Columns:** 14

| Column | Description |
| --- | --- |
| `business_id` | Unique 22-character identifier for the business |
| `name` | Business display name |
| `address` | Street address of the business |
| `city` | City where the business is located |
| `state` | Two-letter state or province code (e.g., "AZ", "ON") |
| `postal_code` | Postal or ZIP code |
| `latitude` | Latitude coordinate of the business location |
| `longitude` | Longitude coordinate of the business location |
| `stars` | Average star rating, rounded to half-stars (range: 1.0 – 5.0) |
| `review_count` | Total number of reviews received by the business |
| `is_open` | Whether the business is currently open (1) or permanently closed (0) |
| `categories` | Comma-separated list of business categories (e.g., "Restaurants, Mexican, Bars") |
| `hours` | Operating hours per day of the week — nested JSON object with keys `Monday` through `Sunday`, each containing a time range string (e.g., "11:00-23:00") or null if closed |
| `attributes` | Nested JSON object containing 39 business attribute fields — includes `AcceptsInsurance`, `Alcohol`, `Ambience`, `BikeParking`, `BusinessAcceptsCreditCards`, `BusinessParking`, `Caters`, `DogsAllowed`, `DriveThru`, `GoodForKids`, `GoodForMeal`, `HappyHour`, `HasTV`, `NoiseLevel`, `OutdoorSeating`, `RestaurantsAttire`, `RestaurantsDelivery`, `RestaurantsGoodForGroups`, `RestaurantsPriceRange2`, `RestaurantsReservations`, `RestaurantsTakeOut`, `WheelchairAccessible`, `WiFi`, and more — all stored as string representations |

---

### 5.2 Checkin (`yelp_academic_dataset_checkin.json`)

**Rows:** 131,930 | **Columns:** 2

| Column | Description |
| --- | --- |
| `business_id` | Unique identifier for the business (references Business) |
| `date` | Comma-separated list of check-in timestamps in `YYYY-MM-DD HH:MM:SS` format — represents every check-in event recorded for the business |

---

### 5.3 Review (`yelp_academic_dataset_review.json`)

**Rows:** 6,990,280 | **Columns:** 9

| Column | Description |
| --- | --- |
| `review_id` | Unique 22-character identifier for the review |
| `user_id` | Unique identifier of the reviewing user (references User) |
| `business_id` | Unique identifier of the reviewed business (references Business) |
| `stars` | Star rating given in this review (range: 1.0 – 5.0, whole stars only) |
| `date` | Date and time the review was posted (`YYYY-MM-DD HH:MM:SS`) |
| `text` | Full text body of the review |
| `useful` | Number of "useful" votes this review received from other users |
| `funny` | Number of "funny" votes this review received from other users |
| `cool` | Number of "cool" votes this review received from other users |

---

### 5.4 Tip (`yelp_academic_dataset_tip.json`)

**Rows:** 908,915 | **Columns:** 5

| Column | Description |
| --- | --- |
| `user_id` | Unique identifier of the user who wrote the tip (references User) |
| `business_id` | Unique identifier of the business the tip is about (references Business) |
| `text` | Short tip or recommendation text |
| `date` | Date and time the tip was posted (`YYYY-MM-DD HH:MM:SS`) |
| `compliment_count` | Number of compliments (endorsements) received on this tip from other users |

---

### 5.5 User (`yelp_academic_dataset_user.json`)

**Rows:** 1,987,897 | **Columns:** 22

| Column | Description |
| --- | --- |
| `user_id` | Unique 22-character identifier for the user |
| `name` | User's first name or display alias |
| `review_count` | Total number of reviews written by this user |
| `yelping_since` | Date and time the user created their Yelp account (`YYYY-MM-DD HH:MM:SS`) |
| `friends` | Comma-separated list of friend user IDs — value is `"None"` if the user has no friends |
| `useful` | Cumulative number of "useful" votes received across all the user's reviews |
| `funny` | Cumulative number of "funny" votes received across all the user's reviews |
| `cool` | Cumulative number of "cool" votes received across all the user's reviews |
| `fans` | Number of fans (followers) the user has |
| `elite` | Comma-separated list of years the user held Yelp Elite status (e.g., "2015,2016,2017") — empty string if never Elite |
| `average_stars` | Average star rating across all the user's reviews |
| `compliment_hot` | Number of "hot" compliments received on the user's profile |
| `compliment_more` | Number of "more" compliments received |
| `compliment_profile` | Number of "profile" compliments received |
| `compliment_cute` | Number of "cute" compliments received |
| `compliment_list` | Number of "list" compliments received |
| `compliment_note` | Number of "note" compliments received |
| `compliment_plain` | Number of "plain" compliments received |
| `compliment_cool` | Number of "cool" compliments received on the user's profile |
| `compliment_funny` | Number of "funny" compliments received on the user's profile |
| `compliment_writer` | Number of "writer" compliments received |
| `compliment_photos` | Number of "photos" compliments received |

---

## 6. Relationships (Logical References)

```
Business.business_id  ←——  Review.business_id
Business.business_id  ←——  Tip.business_id
Business.business_id  ←——  Checkin.business_id
User.user_id          ←——  Review.user_id
User.user_id          ←——  Tip.user_id
User.user_id          ←——  User.friends  (self-referencing, comma-separated)
```

---

## 7. Notes

- All five JSON files use **newline-delimited JSON** format (one JSON object per line, no enclosing array).
- The `attributes` column in Business is a **deeply nested struct** — all 39 sub-fields are stored as string representations (including booleans like `"True"`/`"False"` and nested JSON strings like `"{'garage': False, 'street': True}'"`).
- The `hours` column in Business contains `null` for days the business is closed.
- The `date` column in Checkin is a **single string** containing all check-in timestamps separated by commas — not an array. Splitting is required during Silver transformation.
- The `friends` column in User follows the same pattern — a single comma-separated string of user IDs, or the literal string `"None"`.
- The `elite` column in User is a comma-separated string of years, or an empty string.
- **Data types are not yet known** at this layer. Raw JSON files carry no schema metadata — there are no enforced types, constraints, or keys in the source files. Formal data type definitions and enforcement begin at the Bronze Staging layer, where data is written to managed Delta tables.
- **No primary or foreign key constraints exist** at this layer. Columns like `business_id`, `review_id`, and `user_id` are expected to serve as natural identifiers and join keys, but uniqueness and referential integrity are not enforced until downstream processing.
- No data cleaning or transformation has been applied at this layer — all values are raw as received from the Yelp Academic Dataset via Kaggle.
