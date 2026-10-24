# 01 — Business Data Quality Issues & Solutions

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Table:** `yelp_business`  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-24  

---

## 0. Document Description

This document catalogues every data quality (DQ) issue discovered during the **Deep EDA & Quality Assessment** of the **Yelp Business** dataset (`yelp_poc_uc.bronze_final.yelp_business`). Each issue includes its error ID, severity, detection phase, affected columns and rows, domain rule, description, suggested solutions and the best logical solution. After each issue, the **chosen cleaning approach** — approved after human review — is recorded. These decisions will guide the Silver-layer cleaning pipeline implementation.

The findings originate from notebook **4.2.1 — Business Deep EDA** and follow the standardised DQ assessment framework (Phases 6.1–6.9). The contract for every check is the Silver Cleaned business DDL (11 tables: parent `yelp_business`, 3 level-1 children and 7 parsed-dict children). Issues were first detected in the phase shown in **Detected In**; later re-detections are cross-referenced inside each issue rather than given new IDs.

> **Source Notebook:** `4.2.1_Business_Deep_EDA`  
> **Issue Range:** `DQ-BUSINESS-001` … `DQ-BUSINESS-062`

---

## 1. Data Location

| Property | Value |
| --- | --- |
| Unity Catalog Table | `yelp_poc_uc.bronze_final.yelp_business` |
| Source File | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/yelp_academic_dataset_business.json` |
| Storage Format | Delta (managed table) |
| Environment | Azure Databricks |
| Row Count (Bronze) | 150,346 (1 row per business) |
| Columns (14) | `business_id`, `name`, `address`, `city`, `state`, `postal_code`, `latitude`, `longitude`, `stars`, `review_count`, `is_open`, `attributes` (STRUCT of 39 fields, 7 of them Python-dict strings), `categories` (comma-separated string), `hours` (STRUCT of 7 days) |
| EDA DataFrames | `df_business` (parent), `df_attributes` (32 single-value attributes), `df_hours` (7 days × raw / start / end), `df_categories` (long), 7 nested-attribute frames in long form (`business_id`, `key`, `value`) |
| Linked Tables (read-only) | `bronze_final.yelp_review`, `bronze_final.yelp_tip`, `bronze_final.yelp_checkin`, `bronze_final.yelp_user` |
| Inferred As-Of Date | 2022-01-19 (latest linked review / tip / check-in) |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline. The table below traces the full data lineage from raw extraction through to the planned Gold layer, showing which notebooks produced each stage and where the data lands in Unity Catalog.

| Step | Notebook | UC Location | Layer |
| ---: | --- | --- | --- |
| 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
| 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_business_clean` / `yelp_business_errors`) | Bronze Staging |
| 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_business` | Bronze Final |
| 4 | `4.1.1_Business_Basic_Profile` | _(reads `bronze_final.yelp_business`)_ | Basic EDA |
| **5** | **`4.2.1_Business_Deep_EDA`** | _(reads `bronze_final.yelp_business` + linked bronze tables)_ | **◄ YOU ARE HERE — Deep EDA / DQ Assessment** |
| 6a | `01_Business_Silver_Cleaned` (DDL) | `yelp_poc_uc.silver_cleaned.yelp_business` + 10 child tables (schema) | Silver Cleaned DDL |
| 6b | `01_Business_Cleaning` | `yelp_poc_uc.silver_cleaned.yelp_business` + 10 child tables (data) | Silver Cleaned |
| 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

The DQ findings documented below were discovered during the Deep EDA phase (Step 5). All remediations marked as **"Chosen"** are applied during the **Bronze → Silver Cleaned** transformation (Steps 6a–6b). DDL queries create empty table structures; cleaning notebooks populate them. Rows removed during cleaning are preserved in `yelp_poc_uc.quarantine` for audit.

---

## 3. Data Quality Issues & Resolutions

---
#### 🚨 DQ-BUSINESS-001 — Storage Type Non-Conformance vs Silver Contract

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-001` |
| **Severity** | FINDING (schema / data type conformance violation) |
| **Detected In** | Phase 6.1.2 — Data Type Conformance (Cell 27) |
| **Also Confirmed In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29): the values support every target type (58/58 leaves contract-aligned) |
| **Affected Columns** | 84 of 101 contract columns: `stars` (DOUBLE → DECIMAL(2,1)), `is_open` (BIGINT → BOOLEAN), 24 `attributes.*` STRING → BOOLEAN, `attributes.RestaurantsPriceRange2` STRING → INT, 7 enum attributes (Python-repr quoting), 49 flags packed in 7 dict-strings, `categories` (CSV string → 1:N rows) |
| **Total Affected Rows** | **150,346** out of 150,346 (100%). The issue is in the schema, so every row carries it |
| **Invalid Value** | Wrong storage types, e.g. `stars = 3.5` stored as DOUBLE, `is_open = 1` as BIGINT, `"True"` as STRING, `"{'lot': True, ...}"` as STRING |
| **Domain Rule** | Silver DDL rule R5 (Semantic Data Types): column types must reflect real-world meaning, not raw ingestion storage |

**Description:** Bronze keeps the Yelp JSON almost exactly as received. Ratings are floating-point, the open/closed flag is a 0/1 integer, and nearly all business attributes are free-form strings. Some of those strings are Python-repr booleans (`"True"`), some are quoted enums (`u'free'`) and some are whole Python dictionaries (`"{'romantic': False, ...}"`). Only 16 contract columns (identity, location text, coordinates, `review_count`, hours) can be used as-is. Left this way, analysts would have to compare `attr_wifi = "u'free'"` instead of `'free'`, filter on `"True"` strings instead of booleans, and they would hit floating-point drift on star averages. The 6.1.3 value-based inference confirms the problem is the storage types, not the contract: every target type in the DDL matches the values.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Typed cast + parse in the Silver loading script** (the TE.1/TE.2 logic: `CAST(stars AS DECIMAL(2,1))`, 0/1 → BOOLEAN, `'True'/'False'` → BOOLEAN, unquote enums, decode dict literals into MAP<STRING,BOOLEAN>, explode CSV) | Fixes every column once, at the bronze → silver boundary. Bronze stays a faithful raw copy (R1), and the TE.1 validation already showed 0 lossy casts and 0 out-of-domain enums. |
| 2 | **Fix types in bronze** (`ALTER` / rewrite `bronze_final.yelp_business`) | Downstream code gets simpler, but bronze would no longer be a raw replica of the source. That breaks the medallion contract and makes replays and audits harder. |
| 3 | **Expose typed views on top of bronze** | Nothing extra to store, but the parse and cast run on every query (dict regex parsing is costly), and you can't declare PK/FK constraints on views. |
| 4 | **Leave as STRING and cast at query time** | No engineering effort, but every consumer repeats the parsing logic, results drift between teams, and the DDL's R5 rule is ignored. |

**Best Logical Solution:** **Option 1: typed cast and parse in the Silver loading script.** The DDL already defines the target types, the 6.1.3 inference confirms the data supports them, and TE.1/TE.2 show the transformations work with no data loss. Doing this once at the silver boundary keeps bronze immutable and gives every consumer correctly typed, constraint-documented tables.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-002 — Literal `'None'` Strings Masquerading as NULL

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-002` |
| **Severity** | FINDING (completeness masking: false non-null values) |
| **Detected In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29) |
| **Also Confirmed In** | Phase TE.1 — Data Contract & Fix Data Types (Cell 34): all 12,827 tokens decoded as Python `None` → NULL (9,625 in single-value attributes + 3,202 whole-dict values), 0 values lost in decode; Phase TE.2 (Cell 35): inner `None` dict values kept as rows with `value = NULL` (e.g. 33,655 in `df_ambience`, 18,141 in `df_good_for_meal`, 9,654 in `df_business_parking`, 3,202 in `df_music`); Phase 6.1.3 dict-key inventory: inner `None` values inside 4 dict attributes |
| **Affected Columns** | **Level 1** (whole value = `'None'`): 37 of 39 `attributes.*` fields (all except `AgesAllowed` and `GoodForDancing`). Largest: `RestaurantsDelivery` (3,948), `BusinessParking` (2,271), `RestaurantsTakeOut` (2,105), `OutdoorSeating` (1,882), `DriveThru` (755), `Ambience` (551). **Level 2** (key value = `None` inside a dict): `Ambience` (9,754 dicts), `GoodForMeal` (7,337), `BusinessParking` (5,153), `Music` (1,632) |
| **Total Affected Rows** | **9,653** out of 150,346 (6.42%) with at least one level-1 `'None'` string, totalling 12,827 tokens (9,625 in typed columns + 3,202 in dict-strings) |
| **Invalid Value** | The 4-character string `'None'` (Python's `None` serialized by `str()`), stored where a SQL `NULL` belongs |
| **Domain Rule** | Missing information must be a SQL `NULL`. A non-null string must hold a real member of the attribute's domain (`True`/`False`, an enum value, or a dict) |

**Description:** The Yelp export was built by calling Python `str()` on attribute values, so Python's `None` became the literal text `'None'` instead of a JSON null. To Spark these values look present: `IS NOT NULL` is true, `COUNT(col)` includes them, and completeness metrics are overstated. A naive `CAST(... AS BOOLEAN)` sends them to NULL with no trace, and a naive enum cleaner could turn them into a fake `'none'` category. That collision matters here because `Alcohol` has a valid lowercase `'none'` value (no alcohol served). The same pattern repeats inside dict-strings, where single keys carry `None` (e.g. `{'lot': None, 'street': True}`), meaning "unknown", not `False`.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Normalize to SQL NULL during silver load** (`CASE WHEN trim(col) = 'None' THEN NULL`, and map inner `None` dict values to NULL booleans) | Gives the data its true meaning (unknown), restores accurate completeness metrics, and keeps "unknown" separate from `False`. This is what TE.1/TE.2 implement, with 0 lossy casts. |
| 2 | **Map `'None'` to `False`** | Simple, and boolean columns end up fully populated, but it invents information (e.g. "no delivery" when the business never answered) and biases every rate and ratio. |
| 3 | **Keep the token and add a `_is_unknown` flag column** | Fully traceable, but it widens tables and every consumer must remember to filter on the flag. That is redundant when NULL already carries the meaning natively. |
| 4 | **Drop the affected rows** | Removes 6.42% of businesses, along with their valid identity, location and rating data, over a minor attribute gap. Disproportionate. |

**Best Logical Solution:** **Option 1: normalize to SQL NULL.** `'None'` is a serialization artefact, not a business value, so NULL is the only faithful representation. Matching case-sensitively on `'None'` (not lowercase `'none'`) keeps the valid `Alcohol = 'none'` category, and treating inner dict `None` as NULL keeps "unknown" apart from an explicit `False`. Downstream SQL (`AVG`, `COUNT(col)`, `LEFT JOIN`) then handles these values correctly by default.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-003 — Inconsistent Python-Repr Quoting in Enum Attributes

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-003` |
| **Severity** | FINDING (format consistency violation) |
| **Detected In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29) |
| **Also Confirmed In** | Phase 6.1.2 — Data Type Conformance (Cell 27): 7 enum columns flagged "value cleaning needed"; Phase TE.1 — Data Contract & Fix Data Types (Cell 34): 0 out-of-domain and 0 lost values after unquoting |
| **Affected Columns** | `attributes.Alcohol` (33,825 `u'x'` vs 9,326 `'x'`), `WiFi` (42,736 vs 14,128), `NoiseLevel` (31,555 vs 6,399), `RestaurantsAttire` (23,752 vs 15,465), `Smoking` (4,506 vs 47), `BYOBCorkage` (44 vs 1,395), `AgesAllowed` (129 vs 0, consistent but still quoted) |
| **Total Affected Rows** | **27,610** out of 150,346 (18.36%) hold at least one plain-quoted `'x'` enum alongside the dominant `u'x'` form. All 183,307 non-null enum values carry quote wrapping |
| **Invalid Value** | The same category in two spellings, e.g. `u'free'` and `'free'`, or `u'full_bar'` and `'full_bar'` |
| **Domain Rule** | A categorical attribute must use exactly one canonical value per category, from the closed domains documented in the DDL (e.g. WiFi ∈ {free, no, paid}) |

**Description:** Enum attributes were serialized with Python `repr()` in two Python versions: Python 2 unicode literals (`u'free'`) and Python 3 strings (`'free'`). So one category appears as two different strings. A `GROUP BY attr_wifi` gives six buckets instead of three, `WHERE wifi = 'free'` matches nothing, and the counts per category come out split, for example about 21% of WiFi = free businesses (7,385 of 34,414) land in a separate `'free'` bucket. Once the quoting is removed, the 6.1.3 / TE.1 checks show every value falls inside its documented domain, so the content is clean and only the wrapping differs.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Strip the repr wrapper with a regex** (`regexp_replace(col, "^u?'(.*)'$", "$1")`) | One deterministic rule handles both spellings, keeps every value, and lands on the canonical domain. TE.1 confirmed 0 lossy casts and 0 out-of-domain results. |
| 2 | **Explicit mapping table** (`u'free'` → `free`, `'free'` → `free`, ...) | Fully controlled and auditable, but it needs about 40 hand-maintained entries and silently misses any new spelling. |
| 3 | **Normalize only at query/report time** | No pipeline change, but every consumer has to re-implement the fix and results drift between teams. |
| 4 | **Convert enums to integer codes** | Compact storage, but you lose readability, and it contradicts the DDL, which declares these columns as cleaned STRING enums. |

**Best Logical Solution:** **Option 1: regex unquote during silver load, followed by an enum-domain check.** The variation is purely syntactic (Python 2 vs 3 repr), so a single anchored regex resolves it without losing anything. Lowercase `'none'` must stay a valid value (Alcohol = no alcohol); only Python `None` becomes NULL (see DQ-BUSINESS-002). A domain assertion against the DDL value sets then catches any future drift.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-004 — Leading/Trailing Whitespace in Identity Text Fields

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-004` |
| **Severity** | FINDING (format hygiene, low impact) |
| **Detected In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29) |
| **Also Confirmed In** | **Not remediated in this notebook**: TE.1 (Cell 34) keeps `name`, `address`, `city` as-is so the padding stays visible to later checks. `trim()` is the recommended fix for the Silver load |
| **Affected Columns** | `name` (150 rows), `city` (49 rows), `address` (3 rows) |
| **Total Affected Rows** | **≤ 202** out of 150,346 (≤ 0.13%). Counted per column, so a row with several affected fields counts more than once |
| **Invalid Value** | Trailing spaces, e.g. `'The Mad Griddle '`, `'Temple University Hospital '`, `'Reno '`, `'Audubon '` |
| **Domain Rule** | Business-identity text must be trimmed: the displayed value and the matching key must be identical with no invisible padding |

**Description:** A small number of businesses have trailing spaces in their name, city or address, probably from manual entry on the Yelp business portal. They look the same on screen, but they break exact-match logic: `city = 'Reno'` misses `'Reno '`, `GROUP BY city` splits one city into two buckets, and joins or deduplication on name and address (e.g. entity resolution, geo-matching) silently fail. The `city` case matters most because city is a main grouping dimension in Yelp analytics.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **`trim()` during silver load** | Removes only invisible padding, keeps all real content, can be repeated safely, and costs essentially nothing. Recommended for the Silver loading script (TE.1 deliberately keeps the padding for exploration). |
| 2 | **Full normalization** (trim + collapse internal spaces + case-fold) | Better for matching, but case-folding changes how the business name displays (`McDonald's` → `mcdonald's`). That belongs in a derived match key, not the canonical column. |
| 3 | **Fix only `city`** | Covers the highest-impact grouping column, but leaves the same defect in `name` and `address` for entity-resolution use cases. |
| 4 | **Leave as-is and trim at query time** | No pipeline change, but every consumer has to remember it, and forgotten trims produce silently wrong groupings. |

**Best Logical Solution:** **Option 1: `trim()` on `name`, `address` and `city` during silver load.** The defect affects at most 0.13% of rows, and the fix only removes whitespace that carries no meaning. If case-insensitive matching is needed later, add a separate derived normalized key in Silver Enriched rather than altering the canonical display values.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-005 — Duplicate Categories Within a Business

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-005` |
| **Severity** | FINDING (uniqueness / primary-key violation in normalized child) |
| **Detected In** | Phase TE.2 — Remodel Data (Cell 35): structural findings (43 repeated rows across 43 businesses) |
| **Also Confirmed In** | **Not remediated in this notebook**: `df_categories` keeps every token (668,592 rows = raw token count), keyed by (`business_id`, `category_position`), so the repeats stay visible. Against the Silver DDL PK (`business_id`, `category_name`) they are 43 duplicates |
| **Affected Columns** | `categories` → `yelp_business_categories.category_name` |
| **Total Affected Rows** | **43** out of 150,346 businesses (0.029%), each with exactly 1 category repeated twice. 43 surplus tokens out of 668,592 exploded tokens |
| **Invalid Value** | A label repeated in the same comma-separated list, e.g. `"Gas Stations, Automotive, ..., Gas Stations"` |
| **Domain Rule** | A category is either assigned to a business or not, so it can appear at most once per business (DDL PK `pk_sc_business_categories` = `business_id, category_name`) |

**Description:** In bronze, `categories` is a single comma-separated string. For 43 businesses the same label appears twice in that string, and the repeated labels seen include `Gas Stations`. This probably comes from Yelp merging category taxonomies (a parent and a child mapping to the same label). In bronze the repetition is invisible, but once exploded into the 1:N `yelp_business_categories` table (rule R3) it breaks the declared composite primary key. It would also double-count those businesses in category-level metrics such as "number of gas stations per city" or category-share analyses.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Deduplicate on (`business_id`, `category_name`), keeping the first position** (`GROUP BY ... MIN(category_position)`) | Satisfies the PK, loses no information (the duplicate adds nothing), and keeps the original ordinal of the first mention. Recommended for the Silver load (TE.2 keeps the repeats for exploration). |
| 2 | **`array_distinct(split(categories, ','))` before exploding** | Same outcome with simpler logic, but positions after the removed item shift, so `category_position` no longer matches the original string. |
| 3 | **Keep duplicates and add a surrogate key** | Avoids the PK conflict technically, but keeps the double-counting and contradicts the DDL's declared composite key. |
| 4 | **Quarantine affected businesses** | Fully auditable, but it removes 43 valid businesses' categories over a harmless repeat. Disproportionate. |

**Best Logical Solution:** **Option 1: deduplicate on the composite PK and keep `MIN(category_position)`.** The duplicate carries no extra meaning, so collapsing it is lossless. Keeping the first position preserves the original ordering semantics, and the result satisfies the informational PK the DDL declares, which the query optimizer and downstream joins rely on.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-006 — Empty (`{}`) and All-`None` Nested Attribute Dicts

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-006` |
| **Severity** | FINDING (empty nested payload, semantic null) |
| **Detected In** | Phase TE.2 — Remodel Data (Cell 35): structural findings "Empty dict {}" and "Non-empty dict whose values are ALL None" |
| **Also Confirmed In** | Phase TE.1 (Cell 34): the 81 empty dicts decode losslessly to empty MAPs (0 lost). The earlier wide-format remodel reported the same 83 businesses as "All-flags-NULL" rows (Ambience 6 = 4 `{}` + 2 all-`None`) |
| **Affected Columns** | `attributes.Music` → `df_music` (69 `{}`), `attributes.BusinessParking` → `df_business_parking` (8 `{}`), `attributes.Ambience` → `df_ambience` (4 `{}` + 2 dicts whose keys are all `None`) |
| **Total Affected Rows** | **83** out of 150,346 businesses (0.055%): 81 empty dicts + 2 all-`None` dicts. For Music this is 69 of 7,486 dict values (0.92%) |
| **Invalid Value** | `'{}'`: syntactically a dict, but with no keys. `{'romantic': None, ...}`: every key present but unknown |
| **Domain Rule** | Parsed-dict tables are 0:1 with `yelp_business` and should contain a row only when the business actually reported at least one flag (DDL: "only businesses with the dict") |

**Description:** For 81 businesses the dict attribute exists but is empty (`{}`). The business owner opened the Music, Parking or Ambience section and saved it without ticking anything. For 2 more businesses the Ambience dict lists keys whose values are all `None`. This notebook leaves both as they are:
- **Long DataFrames (TE.2):** an empty dict has no entries, so it produces **no rows**. The business then looks the same as one that never had the attribute, although bronze tells them apart (`'{}'` vs `NULL` / `'None'`). The 2 all-`None` dicts do produce rows, all with `value = NULL`.
- **Wide Silver DDL shape:** keeping `dict IS NOT NULL` turns all 83 into child rows where every flag is NULL. `COUNT(*)` on `yelp_business_music` would then overstate "businesses with music information" by about 0.9%, the 0:1 meaning "a row exists, so data exists" no longer holds, and `LEFT JOIN` consumers can't tell "no dict" from "empty dict".

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Treat `'{}'` as absent: exclude it from the parsed-dict child tables** (filter `trim(dict) NOT IN ('{}')` alongside the `'None'` handling) | Matches the DDL's 0:1 rule ("only businesses with the dict"). An empty dict and a missing dict carry the same information (none), and the business stays fully present in the parent and other children. |
| 2 | **Keep the row and set all flags to `False`** | Every row then has values, but it asserts "no live music, no DJ, ..." when the owner said nothing. That is invented information. |
| 3 | **Keep all-NULL rows (current behaviour)** | Records that the section was opened, but the meaning is unclear, counts are inflated, and every consumer has to filter them. |
| 4 | **Keep the row with an `is_empty_dict` flag** | Fully traceable, but it adds a column the DDL doesn't define (schema drift vs contract) for 83 rows of marginal analytical value. |

**Best Logical Solution:** **Option 1: treat `'{}'` the same as a missing dict and exclude it from the level-2 tables.** It follows the DDL's 0:1 rule without changing the schema, keeps row counts meaningful (a row means at least one reported flag), and invents nothing. The fix is a one-line filter added to the TE.2 dict loop and to the Silver loading script.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-007 — Ambiguous and Cross-Midnight Opening Hours

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-007` |
| **Severity** | FINDING (semantic ambiguity in time ranges) |
| **Detected In** | Phase TE.2 — Remodel Data (Cell 35): structural findings on the new `df_hours` start / end columns |
| **Also Confirmed In** | Phase TE.1 (Cell 34): all 801,015 non-null day values match `H:M-H:M` with hours 0–23 and minutes 0–59 (0 lost, 0 out of range). The format is clean; the problem is what the values mean |
| **Affected Columns** | `hours.Monday` … `hours.Sunday` → `df_hours.hours_<day>`, `hours_<day>_start_time`, `hours_<day>_end_time` |
| **Total Affected Rows** | **133,901** of 801,015 non-null day values (16.7%): **73,908** `'0:0-0:0'` (9.23%), **58,501** with end < start (7.30%), **1,492** with end = start other than `0:0-0:0` (0.19%). Separately, 23,223 businesses (15.4%) have no hours on any day |
| **Invalid Value** | `'0:0-0:0'` (closed? open 24 h? placeholder?), ranges whose end is before the start (e.g. a bar open 17:00 → 02:00), and zero-length ranges such as `'9:0-9:0'` |
| **Domain Rule** | A daily opening range must have a single interpretation: end after start on the same day, or explicitly marked as overnight, 24 h or closed |

**Description:** Yelp stores each day as a bare `start-end` string, with no flag for overnight, 24-hour or closed days. Once the range is split into `start_time` / `end_time`, three patterns stand out:
- `'0:0-0:0'` is the single most common value. The TE.2 sample shows a business open `8:0-18:30` Tuesday–Friday with Monday `'0:0-0:0'`, which reads as "closed", while 24-hour businesses use the same encoding.
- About 7% of ranges cross midnight, so a naive `end - start` duration comes out negative.
- A small set of ranges have zero length.

Opening-hours analytics (duration, "open now", late-night coverage) are wrong unless each pattern is handled explicitly. `attributes.Open24Hours` can't disambiguate, because only 39 businesses report it.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the raw string and derive explicit flags** (`is_overnight` = end < start; `is_zero_range` = `0:0-0:0`; duration computed modulo 24 h, NULL for ambiguous ranges) | Nothing is lost or reinterpreted. Overnight ranges become computable, and the `0:0-0:0` ambiguity stays visible instead of being guessed. |
| 2 | **Interpret `0:0-0:0` as open 24 h** | Simple, but it labels businesses that are probably closed that day as open all day, which inflates late-night coverage. |
| 3 | **Interpret `0:0-0:0` as closed** | Matches the sample pattern, but it silently drops genuinely 24-hour businesses. |
| 4 | **Leave as strings only** | No effort, but every consumer re-implements the parsing and the overnight arithmetic differently. |

**Best Logical Solution:** **Option 1: keep the raw range and add derived flags plus a modulo-24 h duration.** The overnight case has one correct interpretation and can be computed. The `0:0-0:0` case has no source rule that resolves it, so it should stay flagged and unknown rather than be guessed, until a reliable signal (e.g. review timestamps or a provider rule) is available.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-008 — Partial Key Sets in Nested Attribute Dicts

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-008` |
| **Severity** | FINDING (structural completeness at nesting level 2) |
| **Detected In** | Phase TE.2 — Remodel Data (Cell 35): structural finding "Dict missing ≥1 contract key" and the key-level profile |
| **Also Confirmed In** | Phase 6.1.3 dict-key inventory (Cell 29): no unexpected keys, so only contract keys are absent. TE.1 (Cell 34): raw key tokens = MAP entries for every dict (0 lost) |
| **Affected Columns** | `attributes.Ambience` (1,337 dicts; `divey` absent 1,222, `upscale` 118, `hipster` 68, the other 6 keys 6 each), `attributes.Music` (559; `no_music` absent 559, `background_music` 349, `video` / `karaoke` 347, `jukebox` 333, `live` 315, `dj` 46), `attributes.HairSpecializesIn` (33; `straightperms`, `africanamerican`, `asian`), `attributes.BusinessParking` (13; `street` / `validated` / `lot` 13, `valet` 11, `garage` 2) |
| **Total Affected Rows** | **1,942** of 175,479 non-empty dicts (1.11%), i.e. at most 1,942 of 150,346 businesses (1.29%). `BestNights`, `DietaryRestrictions` and `GoodForMeal` always carry their full key set |
| **Invalid Value** | A dict that omits a contract key, e.g. a `Music` dict without `no_music` |
| **Domain Rule** | Each nested attribute has a fixed key set (`DICT_KEY_CONTRACT`). Every key should be present, with `True`, `False` or `None` |

**Description:** A nested attribute can hold a key in three states: an explicit `True`/`False`, an explicit `None` (unknown), or absent. In the long DataFrames an absent key is simply a missing row. When the data is pivoted to the wide Silver shape, it becomes NULL and can no longer be told apart from an explicit `None`. The pattern is concentrated in specific keys (`Ambience.divey`, `Music.no_music`), which points to keys added to the Yelp form later and never back-filled. Per-key rates (e.g. "% of businesses with a divey ambience") therefore use different denominators per key unless the analysis accounts for it.

The key-level profile shows two more points:
- `Music.no_music` is **never `True`** (0 of 6,858 present).
- `DietaryRestrictions.kosher` is never `True` (0 of 30).

These keys currently carry no positive signal.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Pivot against the contract key list in Silver, document "absent ≡ unknown" (NULL), and use per-key denominators in reporting** | Matches the existing wide DDL, invents nothing, and states clearly that absent and `None` carry the same information (unknown). |
| 2 | **Long format with a `key_present` flag (contract keys cross-joined, missing ones filled with NULL and flagged)** | Fully traceable at row level, but it adds rows and a column the Silver DDL doesn't define. |
| 3 | **Fill absent keys with `False`** | Every row is complete, but it asserts "not divey" / "has music" when nothing was reported, which biases rates. |
| 4 | **Drop dicts with partial key sets** | Removes the inconsistency, but throws away 1,942 dicts of valid reported flags. |

**Best Logical Solution:** **Option 1: pivot against the contract key list and treat absent keys as unknown (NULL).** Absence carries no more information than `None`, the wide DDL already handles it, and per-key denominators keep the rates honest. The long DataFrames in this notebook keep the distinction for exploration.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-009 — Non-Existent State Code `XMS` (UK Business in a US/CA Dataset)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-009` |
| **Severity** | FINDING (domain violation, record outside dataset scope) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.1 Allowed Value Conformance** (Cell 43): `state` not in the USPS / Canadian jurisdiction list |
| **Also Confirmed In** | 6.2.3 Pattern Conformance (Cell 49): `state` fails `^[A-Z]{2}$`, `postal_code` classified MALFORMED. 6.2.4 Impossible Value Detection (Cell 57): non-existent jurisdiction + foreign (UK-style) postcode |
| **Affected Columns** | `state`, `postal_code`, `address`, `city` (and, by contradiction, `latitude` / `longitude`) |
| **Total Affected Rows** | **1** out of 150,346 businesses (0.001%) |
| **Invalid Value** | `Pumpernickles Catering`, `95a Mount Pleasant`, `Liverpool`, state `XMS`, postal `L3 5TB`, coordinates `38.673, -90.423` |
| **Domain Rule** | `state` must be a real US state / territory or Canadian province code, and the business must lie inside the dataset's 11 metro areas |

**Description:** `XMS` is not a USPS or Canada Post jurisdiction code. Read together with the rest of the record (Liverpool, `L3 5TB`, "95a Mount Pleasant"), it is a UK business in Merseyside: the state field holds a county code and the postal code is a valid UK postcode. The coordinates, however, point to the St. Louis metro. The address fields and the coordinates contradict each other, so at least one of them is wrong, and nothing in the record tells us which. The record also falls outside the dataset's US/Canada scope. It is harmless in volume, but it would create a phantom `XMS` member in any state-level dimension and break a FK/CHECK constraint on a state lookup table.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Quarantine the business** (`quarantine.yelp_business_out_of_scope`, with reason `NON_EXISTENT_STATE`), and exclude it and its child rows from Silver | The record is internally contradictory and out of scope. Quarantine keeps it auditable without letting a fake jurisdiction into the model. |
| 2 | **Re-assign `state = 'MO'` from the coordinates** | Fits the geo dimension, but it trusts the coordinates over four consistent address fields and the city would still read "Liverpool". |
| 3 | **Keep the row, set `state` to NULL** | Satisfies the domain check, but keeps a UK address inside a US/CA dataset and hides the problem from consumers. |
| 4 | **Keep as-is** | No effort, but the invalid code flows into every state-level aggregate and violates the domain. |

**Best Logical Solution:** **Option 1: quarantine.** Correcting any field would mean guessing which half of a self-contradictory record is true. One business is a negligible loss, and quarantine keeps the evidence for audit.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-010 — Real State Codes Outside the Yelp Footprint (Mislabeled or Out-of-Scope Businesses)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-010` |
| **Severity** | FINDING (domain / accuracy, record-level inconsistency) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.1 Allowed Value Conformance** (Cell 43): INFO check "state ∈ the 14 states/provinces of the 11 Yelp metros" |
| **Also Confirmed In** | 6.2.2 Range Conformance (Cell 46): 1 of these businesses (Bennington, VT) also lies > 100 km from every metro (see DQ-BUSINESS-011). The state-vs-coordinates relation is a cross-column check and should be re-tested in 6.6 |
| **Affected Columns** | `state` (and `city`, `address`, `postal_code` vs `latitude` / `longitude`) |
| **Total Affected Rows** | **20** out of 150,346 businesses (0.013%): TX 4, CO 3, WA 2, HI 2, MA 2, NC 1, UT 1, MT 1, MI 1, SD 1, VI 1, VT 1 |
| **Invalid Value** | Three patterns (see below), e.g. `K Soho Salon, Tampa, SD, 33606` or `Wildlife Wonders, Austin, TX` placed at Tampa coordinates |
| **Domain Rule** | The Yelp Open Dataset covers 11 metros in 14 states/provinces (PA, NJ, DE, FL, IN, TN, AZ, LA, NV, AB, CA, ID, MO, IL). A valid but out-of-footprint state must be explained by the rest of the record |

**Description:** The 20 codes are real jurisdictions, so a plain domain check passes them. Looking at each full record shows three different problems:

| Pattern | Rows | Examples | Meaning |
| --- | --- | --- | --- |
| **(a) Only the state is wrong** (city, ZIP and coordinates agree on a footprint metro) | 6 | `Tampa, SD, 33606` · `Mount Juliet, TX, 37122` · `Nashville, TX, 37217` · `Tucson, HI, 85641` · `Eagle, UT, 83616` (Eagle ID) · `Boston, MA, 19103` (Philadelphia ZIP + coords) | data-entry error in `state` |
| **(b) Address is out of footprint, coordinates are inside a metro** | 13 | `Houston, TX` at Nashville coords · `Vail, CO` at Tucson · `Spokane, WA` at Wilmington DE · `Kalispell, MT` at Tampa · `Tortola, VI` at Tampa | address and coordinates contradict, so one of them is wrong |
| **(c) Fully outside the footprint** | 1 | `All Service Citgo, Bennington, VT` (coords also in Vermont) | consistent but out of dataset scope |

Left as-is, pattern (a) assigns businesses to the wrong state in every state-level aggregate, and patterns (b) and (c) bring addresses the dataset doesn't cover into geo analytics.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Rule-based by pattern**: (a) correct `state` from the USPS ZIP-prefix → state mapping when ZIP, city and coordinates agree; (b) and (c) quarantine with reasons `ADDRESS_COORD_CONFLICT` / `OUT_OF_FOOTPRINT` | Fixes only what three independent fields agree on, and isolates records where no single truth can be established. |
| 2 | **Re-derive `state` for all 20 from the coordinates (reverse geocoding)** | Uniform, but for pattern (b) it would label a Houston address as Tennessee. It trusts the field we can't verify. |
| 3 | **Quarantine all 20** | Simple and safe, but it throws away 6 businesses whose only defect is a typo with a provable correction. |
| 4 | **Keep as-is** | Valid codes, but wrong state attribution and contradictory geography remain. |

**Best Logical Solution:** **Option 1: correct pattern (a) from the ZIP prefix, quarantine patterns (b) and (c).** A correction is applied only when ZIP, city and coordinates agree on the answer. Everything else is contradictory or out of scope, so it is isolated rather than guessed.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-011 — Coordinates Outside Every Yelp Metro (Geocoding Error)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-011` |
| **Severity** | FINDING (accuracy, geolocation) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.2 Range Conformance** (Cell 46): `latitude/longitude` more than 100 km (haversine) from all 11 Yelp metro centres |
| **Also Confirmed In** | 6.2.2 global [-90, 90] / [-180, 180] and North-America envelope checks: 0 violations, so these are plausible-looking coordinates in the wrong place, not malformed values. 6.2.4 (Cell 57): 0 Null-Island / zero-axis coordinates |
| **Affected Columns** | `latitude`, `longitude` |
| **Total Affected Rows** | **3** out of 150,346 flagged (0.002%). After review: **1 genuine error**, 1 out-of-scope business, 1 false positive |
| **Invalid Value** | `Broadway Veterinary Hospital, 350 East Linden St, Boise, ID 83706` at `43.994, -92.462` (~Rochester, Minnesota, 626 km from Boise) |
| **Domain Rule** | A business's coordinates must locate its address and fall inside the dataset footprint. Median distance to the nearest metro is 13 km, p99.9 is 60 km |

**Description:** The three businesses beyond the 100 km threshold have different causes:
- **Broadway Veterinary Hospital (Boise, ID):** the address, city, state and ZIP are all consistent Boise values, but the coordinates are in south-east Minnesota. This is a **geocoding error**. Any map, distance or "nearby" analysis puts this vet ~1,000 km away.
- **All Service Citgo (Bennington, VT, 365 km):** the coordinates match its Vermont address. The record is consistent but out of scope, and it is already covered by DQ-BUSINESS-010 pattern (c).
- **Champion Exteriors (Stone Harbor, NJ 08247, 106 km):** Stone Harbor is a real Cape May County shore town in the Philadelphia market. The coordinates are correct, so this is a **false positive** of the 100 km threshold, not a data error.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Fix the genuine error only**: replace the Boise vet's coordinates with the centroid of ZIP 83706 from a reference table (or set them to NULL if none is available); handle Bennington via DQ-BUSINESS-010; keep Stone Harbor unchanged | Treats each case by its real cause. A ZIP centroid is a defensible approximation, and NULL is honest when no reference exists. |
| 2 | **Set coordinates to NULL for all 3** | Simple, but it deletes two sets of correct coordinates. |
| 3 | **Quarantine all 3 businesses** | Over-reaction: two of the three have a valid address and an otherwise complete record. |
| 4 | **Raise the threshold until nothing is flagged** | Hides the real Minnesota geocode, which is 626 km out. The rule should be reviewed per case, not tuned to pass. |

**Best Logical Solution:** **Option 1: re-geocode the Boise vet to its ZIP centroid (NULL if no reference), route Bennington to the DQ-BUSINESS-010 quarantine, and keep Stone Harbor.** Only provably wrong coordinates change, and the threshold stays as a review trigger.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-012 — Implausibly Short Daily Opening Windows (15 / 30 minutes)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-012` |
| **Severity** | WARNING (plausibility, probable data-entry error) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.2 Range Conformance** (Cell 46): INFO check "open duration > 30 min for a real opening window" |
| **Also Confirmed In** | 6.2.2 hour / minute component checks and 6.2.3 format checks (Cell 49): 0 violations, so the values are well-formed. 6.2.4 (Cell 57) covers the separate zero-length case (DQ-BUSINESS-007) |
| **Affected Columns** | `hours.<Day>` → `df_hours.hours_<day>`, `hours_<day>_start_time`, `hours_<day>_end_time` |
| **Total Affected Rows** | **321** of 725,615 measurable business-day ranges (0.044%), across **238** businesses. Exactly two durations occur: 15 min (147) and 30 min (174) |
| **Invalid Value** | `Arrow Electric` Saturday `8:0-8:15`, `Melt` Sunday `11:0-11:15`, `Oriental Kitchen` `12:0-12:30` Wednesday–Sunday, `Jaramillo Hair salon` Tuesday `17:30-18:0` |
| **Domain Rule** | A daily opening window for a restaurant, salon or service business is expected to last hours, not minutes. Median duration is ~10 h, p1 is 3 h |

**Description:** All other opening ranges are at least about 3 hours long, so these windows sit far outside the normal distribution. Only two durations occur, exactly 15 and 30 minutes, which points to the Yelp hours picker: the owner selected the same (or the next) time slot in both the "from" and "to" dropdowns. A restaurant open 12:00–12:30 every day is not credible. A few could be genuine (a pick-up slot, a short service window), so the values can't be proven wrong one by one. Used as-is, they drag down average opening hours and "open now" coverage for those businesses.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the raw range and treat it as low-confidence in analytics** (exclude windows of 30 min or less from duration and "open now" metrics, in the same derived-flag framework as DQ-BUSINESS-007) | Invents nothing and keeps the lineage, while stopping a probable entry error from distorting metrics. |
| 2 | **Set those day values to NULL** | Removes the noise, but it destroys information that is occasionally genuine and can't be told apart from a missing day. |
| 3 | **Replace with the business's modal opening hours from other days** | Plausible-looking output, but fabricated: the owner never reported those hours. |
| 4 | **Keep and use as-is** | No effort, but known-implausible values bias opening-hours analytics. |

**Best Logical Solution:** **Option 1: keep the raw value and exclude it from duration-based metrics.** Without a ground-truth source, the honest choice is to keep what was reported and stop it from skewing aggregates.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-013 — Category Labels Containing `, ` Split into Fake Categories

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-013` |
| **Severity** | FINDING (parsing / delimiter collision, semantic corruption of a dimension) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): "category_name (label split by ', ' delimiter)" fused-label detection, plus the "initial capital" check (`'& Probates'`, 38 rows) |
| **Also Confirmed In** | 6.2.1 (Cell 43): the vocabulary of 1,311 labels includes the 10 fragments. TE.2 (Cell 35): `split(categories, ', ')` is the source of the split |
| **Affected Columns** | `categories` → `df_categories.category_name` / `yelp_business_categories.category_name` |
| **Total Affected Rows** | **4,150** of 150,243 businesses with categories (2.76%); **9,371** of 668,592 category rows (1.40%) are fragments |
| **Invalid Value** | 4 Yelp labels that contain `, `: `Beer, Wine & Spirits` (2,413 businesses) → `Beer` + `Wine & Spirits`; `Books, Mags, Music & Video` (975) → 3 tokens; `Used, Vintage & Consignment` (753) → 2 tokens; `Wills, Trusts, & Probates` (38) → `Wills` + `Trusts` + `& Probates` |
| **Domain Rule** | Each `category_name` must be one complete Yelp taxonomy label |

**Description:** Bronze `categories` is a `, `-separated list, but four official Yelp labels contain `, ` themselves, so the separator is ambiguous. The split was detected generically: a pair of tokens is a split label when every occurrence of the first is immediately followed by the second, and vice versa. All 10 fragments meet this 100% (none ever appears on its own), which proves they are pieces of one label and not independent categories. The effects:
- 10 fake labels (`Beer`, `Mags`, `Used`, `Wills`, ...) replace 4 real ones in the category dimension.
- "Number of categories per business" is inflated by 1–2 for 4,150 businesses.
- Category searches give wrong results. A liquor store tagged `Beer, Wine & Spirits` shows up under a `Beer` category that doesn't exist on Yelp.
- After the split, `category_position` no longer reflects the label order.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Protect known comma-bearing labels before splitting**: maintain a whitelist (the 4 labels found here, regenerated by the adjacency rule on each load), replace them with a placeholder, split on `, `, then restore | Deterministic and cheap. It fixes the root cause in the parser, the result is lossless, and positions stay consistent. |
| 2 | **Split first, then merge adjacent fragment pairs** using the pair table from 6.2.3 | Same outcome, but it needs a window and a recursive merge for the 3-token labels, and positions have to be renumbered afterwards. |
| 3 | **Tokenize against the official Yelp `categories.json` taxonomy (longest match)** | The most authoritative source, but it adds an external dependency that isn't in the landing zone today. |
| 4 | **Keep the fragments** | No effort, but the category dimension stays corrupted for 2.8% of businesses. |

**Best Logical Solution:** **Option 1: whitelist-protected splitting, with the whitelist re-validated by the adjacency rule (and by the official taxonomy if it is ever ingested).** It removes the ambiguity at the source and loses nothing. Apply the DQ-BUSINESS-005 deduplication after the reassembly.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-014 — Truncated Canadian Postal Codes (FSA Only)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-014` |
| **Severity** | FINDING (pattern / precision loss) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): `postal_code` class `CA_FSA_ONLY (truncated)` |
| **Also Confirmed In** | 6.2.4 (Cell 57): none of the 196 contains a letter Canada Post never issues, so the 3-character prefixes are valid Forward Sortation Areas |
| **Affected Columns** | `postal_code` |
| **Total Affected Rows** | **196** of 5,573 Alberta businesses (3.5%), 0.13% of all businesses. 38 distinct FSAs (`T5J`, `T6E`, `T5T`, ...), 7 of them also have an empty `address` |
| **Invalid Value** | `T5J` instead of a full `A1A 1A1` code such as `T5J 1B9` |
| **Domain Rule** | A Canadian postal code is 6 characters, `A1A 1A1` (FSA + Local Delivery Unit) |

**Description:** Every one of the 196 truncated codes is in the Edmonton metro, where 5,356 other businesses have the full code. The 3 characters present are the **Forward Sortation Area**, a real and correct prefix that identifies a district. The 3-character Local Delivery Unit (block or building level) is missing. The value isn't wrong, just imprecise. A strict `A1A 1A1` CHECK constraint would reject these rows, and joins to a full postal-code reference table would fail for them.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the FSA as-is and document that Canadian codes can be FSA-level** (validate with `^[A-Z]\d[A-Z]( \d[A-Z]\d)?$`) | The value is true and useful for district-level analysis. Nothing is invented or thrown away. |
| 2 | **Complete the code by reverse-geocoding the coordinates** | Restores precision, but needs an external geocoder, and the inferred code might not be the business's mailing code. |
| 3 | **Set to NULL** | Satisfies a strict pattern, but deletes valid district information. |
| 4 | **Pad to 6 characters (`T5J 0A0`)** | Passes the format check with a fake code. |

**Best Logical Solution:** **Option 1: keep the FSA and widen the pattern rule to accept FSA-only codes.** It is accurate partial information, and Option 2 can enrich it later if a geocoder becomes available.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-015 — Empty-String Postal Codes (Masked NULL)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-015` |
| **Severity** | FINDING (masked null / completeness) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): `postal_code` class `EMPTY_STRING` |
| **Also Confirmed In** | 4.6 Missing values summary: `postal_code` reports 0 NULLs, because `''` is not counted as missing. Re-test in 6.3.6 Null Representation and 6.4 Completeness |
| **Affected Columns** | `postal_code` |
| **Total Affected Rows** | **73** of 150,346 businesses (0.049%), spread over 11 states. **56** of the 73 also have an empty `address` |
| **Invalid Value** | `''` |
| **Domain Rule** | A missing postal code must be NULL, and a present one must match a postal format |

**Description:** The 73 values are blank strings, not NULLs, so `IS NULL`-based completeness metrics report `postal_code` as 100% populated. Most of them (56) also have a blank address. That is the signature of **service-area businesses** (mobile cleaners, photographers, contractors) that hide their street address on Yelp. They still have coordinates, at a district or city centroid. The value means "unknown", but it is stored as a value.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Convert `''` → NULL in Silver** (`NULLIF(trim(postal_code), '')`) | Standard masked-null handling. Completeness metrics become correct and nothing is invented. |
| 2 | **Infer the ZIP from the coordinates** | Fills the gap, but for service-area businesses the coordinates are a centroid, so the ZIP would be a guess. |
| 3 | **Quarantine the 73 businesses** | Over-reaction: the rest of each record is valid. |
| 4 | **Keep `''`** | Breaks completeness metrics and pattern constraints. |

**Best Logical Solution:** **Option 1: `''` → NULL.** An empty string carries no information beyond "unknown", and NULL is how the model states that. Apply the same rule to the 5,127 empty `address` values (6.3.6).

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-016 — Malformed Postal Codes (Typos in US / Canadian Codes)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-016` |
| **Severity** | FINDING (pattern violation, data-entry error) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): `postal_code` class `MALFORMED` (5 rows) |
| **Also Confirmed In** | 6.2.4 Impossible Value Detection (Cell 57): `336140` flagged as a 6-digit ZIP that no authority issues. The 5th malformed value, `L3 5TB`, is the UK postcode of DQ-BUSINESS-009 and is handled there |
| **Affected Columns** | `postal_code` |
| **Total Affected Rows** | **4** of 150,346 businesses (excluding the DQ-BUSINESS-009 record) |
| **Invalid Value** | `336140` (Apna Kabab House, Tampa FL: 6 digits); `T6x 2H4` (Oodle Noodle, Edmonton: lower-case letter); `T6T 0G` (Q Nails, Edmonton: missing last digit); `T8A 5WA` (Hunter's Social Grill, Sherwood Park: letter where a digit belongs) |
| **Domain Rule** | US: `^\d{5}(-\d{4})?$`. Canada: `^[A-Z]\d[A-Z] \d[A-Z]\d$` |

**Description:** Four isolated typos, each with a different cause:
- `T6x 2H4` only differs in case, and upper-casing gives a valid code.
- `T6T 0G` and `T8A 5WA` have a valid FSA (`T6T`, `T8A`) but a broken Local Delivery Unit.
- `336140` has one digit too many, and we can't tell which digit is the extra one.

They fail any strict pattern constraint and won't join to a postal reference table.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Per-case deterministic rules**: upper-case Canadian codes (`T6X 2H4`); truncate codes with a valid FSA but a broken LDU to the FSA (`T6T`, `T8A`, consistent with DQ-BUSINESS-014); set non-recoverable US codes (`336140`) to NULL | Each fix is provably lossless or only discards the broken part. No digit is guessed. |
| 2 | **Set all 4 to NULL** | Simple, but it throws away a fully recoverable code and two valid FSAs. |
| 3 | **Correct `336140` → `33614`** (drop the last digit) | Plausible, since 33614 is a Tampa ZIP, but it is a guess: the address (Bruce B Downs Blvd) suggests another ZIP. |
| 4 | **Keep as-is** | Pattern violations reach Silver. |

**Best Logical Solution:** **Option 1: upper-case, truncate to the valid FSA, NULL what can't be recovered.** It keeps every provably correct character and invents none.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-017 — Invisible Unicode Characters (Zero-Width Space / NBSP) in Name, Address and City

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-017` |
| **Severity** | FINDING (encoding / hidden characters) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): "invisible chars" checks on `name`, `address`, `city` |
| **Also Confirmed In** | The control-character checks in the same cell return 0, so these are Unicode format/space characters, not ASCII control codes. Related to DQ-BUSINESS-004 (edge whitespace) |
| **Affected Columns** | `name`, `address`, `city` |
| **Total Affected Rows** | **29** values: `name` 15 (5 U+200B zero-width space, 10 U+00A0 no-break space), `address` 10 (1 U+200B, 9 U+00A0), `city` 4 (3 U+200B, 1 U+00A0) |
| **Invalid Value** | `'\u200bClayton'`, `'\u200bLargo'`, `'\u200bLithia'`, `'Saint\xa0Louis'`, `'Club Philadelphia\u200b'`, `'8438 N Lindbergh\xa0Blvd'` |
| **Domain Rule** | Identity text may contain any printable Unicode but no invisible format characters. Spaces must be U+0020 |

**Description:** These characters come from copy-paste out of web pages and word processors. They are invisible when displayed but change the value. `'\u200bClayton'` and `'Clayton'` are two different cities in a `GROUP BY`, a join to a city reference, or a search, and `'Saint\xa0Louis'` doesn't match `'Saint Louis'`. Tokenizers and string-matching (deduplication, entity resolution) treat NBSP and ZWSP inconsistently. This is separate from the 1,381 names with legitimate non-ASCII characters (accents, `’`, `®`), which must be kept.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Targeted normalization**: replace U+00A0 with a regular space, remove U+200B–U+200D, U+2060 and U+FEFF, then trim and collapse repeated spaces | Fixes exactly the invisible characters, keeps legitimate Unicode, and is deterministic and lossless in meaning. |
| 2 | **Apply Unicode NFKC normalization** | Also maps NBSP to a space, but leaves U+200B in place and changes other characters (ligatures, full-width forms), which is more than needed. |
| 3 | **Strip all non-ASCII** | Removes the problem, but destroys 1,381 correct names (`Café`, `Joe’s`). |
| 4 | **Keep as-is** | Silent grouping and join mismatches stay. |

**Best Logical Solution:** **Option 1: targeted removal / replacement of invisible characters.** It is the smallest change that fixes the problem, and it touches nothing a human reader would see.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-018 — Polluted City Values (Trailing Punctuation, Embedded State, Qualifiers, Census Names)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-018` |
| **Severity** | FINDING (pattern / standardization) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): `city` trailing punctuation, embedded state/region and parenthetical-qualifier checks |
| **Also Confirmed In** | 6.2.2 Range Conformance (Cell 46): `city` length > 50 for the 2 `Nashville-Davidson metropolitan government (balance)` rows. Case variants (1,416 raw vs 1,238 case-folded distinct cities) belong to 6.3 |
| **Affected Columns** | `city` |
| **Total Affected Rows** | **32** of 150,346 businesses: **17** trailing comma (`Nashville,`, `Saint Louis,` ×2, `Tampa,`, ...), **9** embedded state/region (`Tucson, Arizona` ×2, `Palm Harbor, FL`, `Havertown, PA`, `Mt Laurel Twp, NJ`, `Maryland Height, MO`, `Tampa,Fl`, ...), **6** parenthetical / census names (`Nashville-Davidson metropolitan government (balance)` ×2, `Indianapolis city (balance)`, `Boise (Meridian)`, `Largo (Walsingham)`, `Philadelphia (Northeast Philly)`) |
| **Invalid Value** | See above |
| **Domain Rule** | `city` holds only the locality name. State, punctuation and neighbourhood qualifiers go elsewhere or nowhere |

**Description:** Owners type the city in free text, so it picks up the state (`, PA`), stray punctuation and neighbourhood notes. A few rows carry U.S. Census place names (`... (balance)`), probably from a geocoder. Each variant creates its own city member: Nashville appears as `Nashville`, `Nashville,` and `Nashville-Davidson metropolitan government (balance)`. That fragments city-level counts and breaks joins to a city/metro dimension. `Maryland Height` is also misspelled (Maryland Heights).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Deterministic rules plus a small curated map**: strip trailing `,;`; remove a `, <state>` / `, <state name>` suffix when it equals the record's state; map census `(balance)` names to the city (`Nashville`, `Indianapolis`); resolve the parenthetical rows and the misspelling through a reviewed mapping table (the ZIP decides `Boise` vs `Meridian`) | Fixes all 32 transparently, with every change traceable to a rule or a reviewed mapping entry. |
| 2 | **Standardize against a gazetteer (USPS / GeoNames) by ZIP** | The most authoritative option, but it adds an external dependency and may rename legitimate local names. |
| 3 | **Keep the raw value and add a cleaned `city_std` column** | Keeps the original, but adds a column the Silver DDL doesn't define (contract drift). |
| 4 | **Keep as-is** | Fragmented city dimension. |

**Best Logical Solution:** **Option 1: rule-based cleanup plus a reviewed mapping for the 7 non-rule cases.** At this volume the curated map is cheap and auditable, and it gives one city member per locality without an external dependency.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-019 — Degenerate Street Addresses (No Street Name)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-019` |
| **Severity** | FINDING (validity, placeholder / misplaced value) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.3 Pattern Conformance** (Cell 49): "address (no letters)" |
| **Also Confirmed In** | 6.2.2 Range Conformance (Cell 46): `address` length < 3 for `'.'` and `'54'` |
| **Affected Columns** | `address` |
| **Total Affected Rows** | **11** of 145,219 non-empty addresses (0.008%) |
| **Invalid Value** | **House number only (6):** `3070`, `5348` (Coast Dental), `850`, `22930`, `11964`, `54`. **ZIP+4 typed into the address (4):** `63088-1828`, `08080-4554`, `08096-3558`, `62025-2812`, each equal to the record's own `postal_code` + 4. **Placeholder (1):** `.` (Anthony David Photography, Nashville) |
| **Domain Rule** | A street address must contain a street name. A bare number, a postal code or punctuation is not an address |

**Description:** None of the 11 values can locate the business. The ZIP+4 values are the postal code entered in the wrong field, so they only repeat `postal_code`. The `.` is a placeholder used to get past a required field. The bare house numbers lack the street they belong to. Geocoding, address matching and deduplication by address all fail or mismatch on them. They are different from the 620 addresses without a house number (malls, landmarks, intersections such as `Citizens Bank Park`), which are legitimate and are kept.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Set all 11 to NULL** (missing street address), keep postal code and coordinates | None of them carries usable street information: the ZIP+4 duplicates `postal_code`, and a house number without a street can't be used. NULL states the truth. |
| 2 | **Move the ZIP+4 extension into `postal_code`** (`63088` → `63088-1828`), NULL the others | Keeps the +4 precision, but changes the postal-code format for 4 rows only, which is inconsistent with the other 144,716 five-digit ZIPs. |
| 3 | **Reverse-geocode an address from the coordinates** | Fills the field, but the inferred address may not be the business's real one. |
| 4 | **Keep as-is** | Invalid addresses reach Silver and break geocoding / matching. |

**Best Logical Solution:** **Option 1: NULL all 11.** It turns misleading values into explicit unknowns, loses no information that isn't already in `postal_code` / coordinates, and matches the masked-null handling of DQ-BUSINESS-015.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-020 — Contradictory Dietary Flags (`vegan = True`, `vegetarian = False`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-020` |
| **Severity** | WARNING (logical implication violated inside one nested attribute) |
| **Detected In** | Phase 6.2 — Domain & Range Checks → **6.2.4 Impossible Value Detection** (Cell 57): "vegan vs vegetarian" (vegan ⊂ vegetarian) |
| **Also Confirmed In** | 6.2.1 (Cell 43): every key and raw token of `DietaryRestrictions` is in the contract domain, so the values are well-formed and only their combination is contradictory. The other intra-dict rule, `Music.no_music` vs other music flags, has 0 violations |
| **Affected Columns** | `attributes.DietaryRestrictions` → `df_dietary_restrictions` (`vegan`, `vegetarian`) |
| **Total Affected Rows** | **3** of 30 businesses with the dict (10%): Rolled Mountain Creamery (Reno; also `dairy-free = True`), Greenheart Bakery (Tucson), The DeLuxe (Reno) |
| **Invalid Value** | `{'vegan': True, ..., 'vegetarian': False}` |
| **Domain Rule** | Vegan food is a strict subset of vegetarian food, so offering vegan options implies offering vegetarian options |

**Description:** Read as a taxonomy, the combination is impossible. Read as owner-entered checkboxes, it is understandable: an owner may tick only the most specific option ("vegan") and leave "vegetarian" unticked, which `False` then records. The base is tiny (30 businesses), so 3 rows make a 10% contradiction rate for this attribute. Any "vegetarian-friendly" metric built on `vegetarian` alone would miss these businesses.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the flags as reported in Silver, document them as independent self-reported options, and derive `vegetarian_friendly = vegan OR vegetarian` in Gold** | Silver stays faithful to the source. The domain logic is applied once, explicitly, where metrics are built. |
| 2 | **Set `vegetarian = TRUE` when `vegan = TRUE` in Silver** | Logically sound, but it overwrites a reported value with an inferred one, so lineage to the source is lost. |
| 3 | **Set both flags to NULL** | Removes the contradiction by deleting a probably-true signal (the vegan option). |
| 4 | **Drop the 3 dicts** | Loses all 7 dietary flags for 10% of the attribute's population. |

**Best Logical Solution:** **Option 1: keep the reported values in Silver and resolve the implication in a Gold derived measure.** The source is preserved, the business rule is explicit and reusable, and metrics come out correct.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.2.4 (already documented, no new ID):** zero-length opening windows (1,492) → **DQ-BUSINESS-007**. Repeated (`business_id`, `category_name`) (43) → **DQ-BUSINESS-005**. Non-existent state `XMS` and UK postcode → **DQ-BUSINESS-009**. 6-digit ZIP `336140` → **DQ-BUSINESS-016**.
>
> **Reviewed and not raised as errors (INFO only):** `review_count` heavy tail (2,362 above the log-IQR fence: popular venues, which are valid); 56 single-business category labels (an open taxonomy, valid long tail); 12 businesses with more than 20 categories (large multi-service venues, max 36); 1,381 names with legitimate non-ASCII characters; 620 landmark / intersection addresses without a house number; unpadded `H:M` source convention and `x:59` closing times (format conventions, not errors).

---
#### 🚨 DQ-BUSINESS-021 — One City Written in Many Spellings (`Saint Louis` / `St. Louis` / `St Louis` / `ST LOUIS` …)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-021` |
| **Severity** | FINDING (representation consistency; directly breaks GROUP BY city / metro analytics) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.1 Categorical Representation Consistency** (Cell 60), check A1 "city (spelling variants, per state)" |
| **Also Confirmed In** | 6.3.5 (Cell 72): 176 ALL-CAPS / all-lower cities and 3 odd interior capitals (DQ-BUSINESS-030), 9 double-spaced cities (DQ-BUSINESS-029). Trailing-comma / embedded-state cities are already DQ-BUSINESS-018; trailing spaces DQ-BUSINESS-004; NBSP / zero-width DQ-BUSINESS-017 |
| **Affected Columns** | `city` (grouped within `state`) |
| **Total Affected Rows** | **4,576** of 150,346 businesses (3.04%) carry a non-canonical spelling, across **153** city clusters with more than one spelling |
| **Invalid Value** | `Saint Louis` (4,827) vs `St. Louis` (1,254) / `St Louis` (356) / `St.Louis` (9) / `SAINT LOUIS` (3) … 14 spellings; `Saint Petersburg` (1,663) vs `St. Petersburg` (1,185) / `St Petersburg` (387) … 8; `Mount Juliet` vs `Mt. Juliet` / `Mt Juliet`; `King of Prussia` vs `King Of Prussia` (134); `O'Fallon` vs `O Fallon` / `OFallon`; `Feasterville Trevose` vs `Feasterville-Trevose`; `Land O Lakes` vs `Land O' Lakes` |
| **Domain Rule** | A city is a controlled geographic entity: one place = one canonical name per state (USPS / Canada Post preferred place name) |

**Description:** `city` is free text typed by owners, so the same municipality is spelled with and without the `Saint→St.` / `Mount→Mt.` / `Fort→Ft.` abbreviation, with or without the period, with different casing, hyphens and apostrophes. The two largest clusters alone (St. Louis 1,634 and St. Petersburg 1,586 non-canonical rows) split two of the 11 Yelp metros into 8-14 groups each. Any city-level KPI (business density, avg stars per city, top cities) is wrong until this is resolved. Note that the *modal* spelling is not always the official USPS form (USPS uses `SAINT LOUIS`, `SAINT PETERSBURG`, `MOUNT JULIET`), so a reference list beats a frequency vote.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw `city`; add `city_normalized` in Silver built from (a) trim + whitespace collapse + invisible-char removal, (b) the folding key used in 6.3.1 (case, punctuation, Saint/Mount/Fort), (c) a reviewed `city_key → canonical name` mapping table per state** | Fixes every cluster deterministically, keeps lineage to the source value, and the mapping table (153 rows) is small enough to review once and reuse for checkin/tip joins |
| 2 | **Overwrite `city` with the most frequent spelling of its cluster** | Fast, but loses the original value and can pick a non-official form (`St. Pete Beach` vs `Saint Pete Beach`); a frequency vote may also merge genuinely different places with similar keys |
| 3 | **Reverse-geocode `latitude`/`longitude` to an official place name** | Most authoritative, but needs an external gazetteer / API, and coordinates have their own issues (DQ-BUSINESS-011, DQ-BUSINESS-027) |
| 4 | **Leave as-is and normalise only in Gold queries** | No Silver change, but every consumer must re-implement the same logic, and results will drift between teams |

**Best Logical Solution:** **Option 1 — raw `city` + derived `city_normalized` from a rule-based key and a curated per-state mapping.** It resolves all 153 clusters reproducibly, uses official spellings rather than the modal one, and keeps the raw value for audit. Option 3 can later validate the mapping for the few ambiguous clusters.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-022 — Same Brand / Chain Written in Several Spellings (`AutoZone` / `Autozone` / `Auto Zone`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-022` |
| **Severity** | LOW / INFO (owner-entered display name; impacts chain-level analytics and entity resolution, not record validity) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.1 Categorical Representation Consistency** (Cell 60), check A2 "name (chain / brand spelling variants)" |
| **Also Confirmed In** | 6.3.5 (Cell 72): 701 names use a curly apostrophe `’` vs 21k+ straight `'` (`David's Bridal` 12 vs `David’s Bridal` 10), 855 ALL-CAPS / all-lower names (`SEPHORA` vs `Sephora`, `ALDI` vs `Aldi`), 263 names with double spaces |
| **Affected Columns** | `name` |
| **Total Affected Rows** | **1,821** of 150,346 businesses (1.21%) carry a non-modal spelling, across **1,252** brand clusters |
| **Invalid Value** | `Autozone` (35) / `AutoZone` (28) / `Auto Zone` (5) / `autozone` (1); `Winn-Dixie` (33) / `Winn Dixie` (32); `TJ Maxx` / `T J Maxx` / `Tj Maxx` / `T.J. Maxx` / `T.J.Maxx`; `Little Caesars Pizza` / `Little Caesar's Pizza`; `HH Gregg` / `hhgregg` / `h h  gregg` |
| **Domain Rule** | One brand should map to one brand key so chain counts, market share and chain-vs-independent features are correct |

**Description:** Unlike `city`, a business name is not a controlled vocabulary; stylisation is sometimes intentional (`lululemon`, `hhgregg`, `SEPHORA` are official brand marks). The issue is therefore not that individual names are "wrong" but that the same chain cannot be grouped reliably: e.g. Winn-Dixie is split exactly 50/50 between two spellings. This affects chain detection, duplicate-listing detection and any "top chains" analysis.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw `name`; add a derived `name_match_key` in Silver (NFC, curly→straight quotes, case-fold, strip punctuation/whitespace, `&`→`and`)**, and build `brand` in Gold from that key | Display names stay as the owner wrote them; the key groups all 1,252 clusters without editing any value; cheap and deterministic |
| 2 | **Overwrite `name` with the modal spelling of its cluster** | Destroys legitimate stylisation and may merge distinct businesses whose keys collide (e.g. two different `Joe's` shops) |
| 3 | **Fuzzy matching (Jaro-Winkler / embeddings) + category + geography to build a brand dimension** | Catches deeper variants (`Little Caesars` vs `Little Caesars Pizza`), but is costly, probabilistic, and belongs in Gold enrichment rather than Silver cleaning |
| 4 | **Do nothing** | Valid for record display, but chain metrics stay fragmented |

**Best Logical Solution:** **Option 1 — raw `name` + deterministic `name_match_key` in Silver**, with Option 3 as a later Gold enhancement. It keeps source fidelity and brand stylisation while making chain grouping correct.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-023 — `state` Contradicts the USPS ZIP Prefix of `postal_code`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-023` |
| **Severity** | FINDING (cross-column category mapping; one of the two geographic fields is wrong) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.1 Categorical Representation Consistency** (Cell 60), check C2 "state ↔ postal_code (USPS ZIP prefix)" |
| **Also Confirmed In** | 6.3.1 C4 (Cell 60): 48 businesses filed under a minority state for their city (`Eagle` UT/ID, `Franklin` IN/TN). Partial overlap with DQ-BUSINESS-010 (out-of-footprint states such as SD/TX/HI/UT). Country-level checks pass: 0 US-state / CA-postcode mismatches (C1) and 0 Canada Post letter mismatches (C3, 5,555 rows) |
| **Affected Columns** | `state`, `postal_code` (and `city`) |
| **Total Affected Rows** | **71** of 144,716 US businesses with a 5-digit ZIP (0.049%), in 62 (state, ZIP3) groups |
| **Invalid Value** | `CA / 836xx Eagle` (Idaho ZIP), `SD / 336xx Tampa` (Florida ZIP), `TX / 372xx Nashville` (Tennessee ZIP), `TN / 28801 White Duck Taco Shop, Nashville` (Asheville NC ZIP), `PA / 08102 Benjamin Franklin Bridge` (Camden NJ ZIP), `NV / 88431 Sparks` (NM ZIP) |
| **Domain Rule** | Every USPS 3-digit ZIP prefix is allocated to exactly one state (USPS National ZIP prefix allocation); `state` must match it |

**Description:** Three patterns explain the 71 rows:

| Pattern | Examples | Likely truth |
| --- | --- | --- |
| Wrong `state`, ZIP and city agree | `CA/UT · 836 · Eagle`, `SD · 336 · Tampa`, `TX · 372 · Nashville`, `HI · 856 · Tucson` | State typo / wrong drop-down value (overlaps DQ-BUSINESS-010) |
| Border metro, ZIP of the neighbouring state | `NJ · 191 · Philadelphia`, `PA · 080-083 · Cherry Hill`, `DE · 190/193 · Media / Glen Mills`, `IL · 631 · St. Louis`, `MO · 622`, `CA · 895 · Reno` | Business sits near the state line; either state or ZIP was taken from the wrong side |
| ZIP typo, state and city agree | `PA · 432 · Philadelphia`, `TN · 928 · Nashville`, `IN · 922 · Indianapolis`, `AZ · 875 · Tuscon`, `PA · 104/117 · Collegeville` | Digits transposed / mistyped ZIP |

Wrong state assignments mis-route businesses to the wrong metro, tax region and time zone (DQ-BUSINESS-025 note), and wrong ZIPs break joins to census / demographic data.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag the 71 rows (`geo_conflict_flag`) and resolve with coordinates in 6.6: point-in-polygon of `latitude`/`longitude` against state and ZIP boundaries decides which field is wrong; correct only that field in Silver, record the rule in lineage** | Coordinates are an independent third witness; it fixes the field that is actually wrong instead of guessing, and is auditable |
| 2 | **Always trust the ZIP and overwrite `state` from the ZIP prefix** | Correct for pattern 1, but wrong for pattern 3 (ZIP typos) — would move a Philadelphia business to Ohio |
| 3 | **Always trust `state` and NULL the ZIP** | Safe for pattern 3, wrong for pattern 1; loses a likely-correct ZIP |
| 4 | **Quarantine the 71 rows** | Clean, but removes valid businesses (with reviews) from all downstream analysis for a fixable error |

**Best Logical Solution:** **Option 1 — flag now, adjudicate with coordinates in 6.6, correct only the losing field.** At 71 rows the cost is negligible and it is the only option that is correct for all three patterns.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-024 — One BYOB Policy Encoded in Three Columns That Contradict Each Other

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-024` |
| **Severity** | FINDING (cross-column category mapping; the same fact has two different values) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.1 Categorical Representation Consistency** (Cell 60), check C5 "BYOB policy" (5 rules + contingency table) |
| **Affected Columns** | `attributes.BYOB` (BOOLEAN), `attributes.BYOBCorkage` (enum `no` / `yes_free` / `yes_corkage`), `attributes.Corkage` (BOOLEAN) → `df_attributes.attr_byob`, `attr_byob_corkage`, `attr_corkage` |
| **Total Affected Rows** | **94** distinct businesses of 1,439 with a known `BYOBCorkage` (6.5%), **100** rule violations (validated with an independent SQL query) |
| **Invalid Value** | Rule breakdown below |
| **Domain Rule** | `BYOBCorkage` is the most specific field: `no` ⇒ BYOB = FALSE and no corkage fee; `yes_free` ⇒ BYOB = TRUE, Corkage = FALSE; `yes_corkage` ⇒ BYOB = TRUE, Corkage = TRUE |

| Rule | Violations | Of (both known) |
| --- | --- | --- |
| `BYOBCorkage yes_*` but `BYOB = False` | 49 | 163 |
| `BYOBCorkage no` but `BYOB = True` | 10 | 163 |
| `BYOBCorkage yes_free` but `Corkage = True` | 11 | 600 |
| `BYOBCorkage yes_corkage` but `Corkage = False` | 1 | 600 |
| `BYOBCorkage no` but `Corkage = True` (fee without BYOB) | 29 | 600 |

**Description:** Yelp collected the BYOB policy through three separate questions at different times, and owners answered them independently. The largest group (38 rows: `yes_free` + `BYOB = False` + `Corkage = False`) suggests `BYOB = False` was read as "we don't *require* BYOB". `no` + `Corkage = True` (29) is semantically impossible: a corkage fee exists only when outside bottles are allowed. Filters such as "BYOB restaurants" return different sets depending on which column is used.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the 3 raw columns in Silver; derive one `byob_policy` (`not_allowed` / `free` / `with_fee` / `unknown`) with precedence `BYOBCorkage` > `BYOB` + `Corkage`, and add `byob_conflict_flag`** | `BYOBCorkage` is the only field that encodes the full policy, so it is the most reliable witness; raw values stay auditable; analysts get one consistent field |
| 2 | **Overwrite `BYOB` / `Corkage` in Silver to agree with `BYOBCorkage`** | Consistent, but replaces reported values with inferred ones and hides the conflict |
| 3 | **NULL all three fields for the 94 conflicting businesses** | Removes contradictions but discards mostly-correct information (e.g. `BYOBCorkage` itself) |
| 4 | **Majority vote among the three columns** | Only two columns are comparable per rule; a vote is undefined when one is NULL (the most common case) |

**Best Logical Solution:** **Option 1 — derived `byob_policy` with `BYOBCorkage` precedence plus a conflict flag.** It produces one trustworthy answer, documents the precedence rule, and keeps the source data intact.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.3.1 (already documented, no new ID):** enum quoting style `u'x'` vs `'x'` (Alcohol 9,326, WiFi 14,128, NoiseLevel 6,399, RestaurantsAttire 15,465, Smoking 47, BYOBCorkage 44 minority values) → **DQ-BUSINESS-003**. `Alcohol 'none'` (20,910) is a valid category and differs from the `None` literal (38) only by case → null-cleaning must be case-sensitive (**DQ-BUSINESS-002**). City ↔ state minority filings (48) → **DQ-BUSINESS-023** / **DQ-BUSINESS-010**.
>
> **Reviewed and not raised:** `category_name` has 0 spelling variants (Yelp-controlled taxonomy); 0 US/CA country mismatches between `state` and `postal_code`; 0 Canada Post first-letter mismatches; `AgesAllowed` uses a single quoting style.

---
#### 🚨 DQ-BUSINESS-025 — Midnight Close and "Open All Day" Written in Several Spellings (`-0:0` vs `-23:59`, `0:0-0:0` vs `0:0-23:59`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-025` |
| **Severity** | FINDING (time-format consistency; same instant / schedule has several representations) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.2 Date & Time Format Consistency** (Cell 65), checks D "midnight close spelling" and E "open all day spelling" |
| **Affected Columns** | `hours.Monday` … `hours.Sunday` → `df_hours.hours_<day>` and derived `hours_<day>_end_time` |
| **Total Affected Rows** | **1,384** day-slots of 801,015 (0.17%): **959** closing at `23:59` instead of `0:0` (3.9% of 24,746 midnight closes), **425** all-day slots written `0:0-23:59` (412) or `0:0-23:45` (13) instead of the dominant `0:0-0:0` (73,908) |
| **Invalid Value** | `11:0-23:59`, `0:0-23:59`, `0:0-23:45` |
| **Domain Rule** | One instant = one representation: midnight close = `00:00` of the next day; a 24-hour day = one canonical encoding |

**Description:** Everything else in the schedule format is clean: 16 raw shapes, all 24-hour `H:M-H:M`, no AM/PM, no `24:00`, no seconds, no split shifts, one consistent unpadded convention. The only representation conflict is at the end of the day. `23:59` silently drops one minute and makes duration maths and "open now at 23:59:30" checks disagree with `-0:0` rows; `0:0-23:59` is a second spelling of "open 24 h" alongside `0:0-0:0`, which itself is ambiguous (DQ-BUSINESS-007). `0:0-23:45` (13) may be genuine (closes 15 min before midnight) and should not be force-mapped to 24 h.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw `hours_<day>`; in the Silver derived columns map `end = 23:59` → `00:00` with `closes_next_day = TRUE`, map `0:0-23:59` → `is_open_24h = TRUE`, keep `0:0-23:45` as a real window, and compute `open_minutes` with modulo-24 h arithmetic** | Unifies the two spellings of the same instant without inventing data; duration and "open now" logic become consistent; consistent with the DQ-BUSINESS-007 derived-flag design |
| 2 | **Rewrite the raw strings to `-0:0` / `0:0-0:0`** | Simple, but alters source values and pushes the 425 rows into the already-ambiguous `0:0-0:0` bucket |
| 3 | **Treat `23:59` as a separate closing time** | No change needed, but every downstream duration is 1 minute short and midnight-close counts are under-reported by 959 |
| 4 | **NULL the non-canonical slots** | Loses valid opening information for 1,384 slots |

**Best Logical Solution:** **Option 1 — canonicalise in derived Silver columns (`end_time`, `closes_next_day`, `is_open_24h`, `open_minutes`) and keep the raw string.** It resolves the representation conflict at the semantic level while preserving the source.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-026 — `Open24Hours = True` Contradicts the Weekly Schedule

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-026` |
| **Severity** | LOW (redundant representation of one fact disagrees; tiny population) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.2 Date & Time Format Consistency** (Cell 65), check E "attr_open_24_hours ↔ hours" |
| **Also Confirmed In** | 6.3.4 (Cell 70): `Open24Hours` is the sparsest boolean attribute (19 True / 20 False / 150,307 NULL) |
| **Affected Columns** | `attributes.Open24Hours` → `df_attributes.attr_open_24_hours`; `hours.<Day>` |
| **Total Affected Rows** | **15** of 39 businesses with a known `Open24Hours` (38.5%); all 15 are `True` with fewer than 7 all-day slots (10 have none, 5 have only Monday = `0:0-0:0`) |
| **Invalid Value** | `Open24Hours = True` with `hours` = `9:0-17:0` every day (`aG-CtKPYEJ8ZR5xdnqg5Qw`), or `0:0-0:0` Monday + `9:0-0:0` other days (`mGVZZXjH1SyLIuxpcNNIqw`) |
| **Domain Rule** | "Open 24 hours" is stated twice (attribute and schedule); both must agree |

**Description:** The attribute was answered once and never maintained, while the schedule is edited more often (seasonal hours). A pattern of `Monday = 0:0-0:0` with normal hours on other days also shows that `0:0-0:0` is sometimes a placeholder rather than "24 h" — which is exactly the DQ-BUSINESS-007 ambiguity. With only 39 populated values the attribute is too sparse to be used to resolve `0:0-0:0` for the other 73,908 slots.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Treat `hours` as the source of truth; keep `attr_open_24_hours` raw but derive `is_open_24h` from the schedule (DQ-BUSINESS-025), and add `open24_conflict_flag` for the 15** | The schedule is richer and more recently maintained; one authoritative derived field avoids two answers to the same question |
| 2 | **Trust the attribute and overwrite hours to `0:0-0:0`** | Would erase detailed and probably correct opening hours for 15 businesses |
| 3 | **NULL `attr_open_24_hours` where it conflicts** | Removes the contradiction but also the signal; acceptable but loses lineage |
| 4 | **Drop the attribute from Silver** | 99.97% NULL already; defensible, but the DDL contract includes it, so it must be kept for schema conformance |

**Best Logical Solution:** **Option 1 — schedule wins, attribute kept raw with a conflict flag.** It is the most reliable evidence, needs no destructive change and keeps the DDL contract.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Reviewed and not raised (INFO only):** no DATE/TIMESTAMP column in the business table and no date literals in `name` / `address` / `city` / `postal_code`; the unpadded `H:M` convention is used consistently (0 padded components) and TE.1 derives uniform `HH:mm`; `hours` struct fields come in alphabetical order and use Title-case names vs lower-case `BestNights` keys (map both to ISO day numbers in Silver). **Time zone:** schedules are local wall-clock time and the zone is not stored — 7 IANA zones are implied by `state` (AZ without DST), 13 businesses map to no zone (out-of-footprint states, DQ-BUSINESS-010). Recommendation: derive a `time_zone` column from `state` in Silver before comparing hours across metros.

---
#### 🚨 DQ-BUSINESS-027 — Coarse (City / ZIP-Centroid) and Mixed-Precision Coordinates

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-027` |
| **Severity** | LOW (numeric format / accuracy; values are valid but not equally precise) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.3 Numeric Format Consistency** (Cell 68), check B "latitude/longitude precision" (decimal-places distribution) |
| **Also Confirmed In** | 6.2.2 (Cell 46): 3 coordinates > 100 km from every Yelp metro (DQ-BUSINESS-011). Sign convention, swapped axes and scientific notation all PASS |
| **Affected Columns** | `latitude`, `longitude` |
| **Total Affected Rows** | **348** of 150,346 businesses (0.23%) with ≤ 3 decimals on the least precise axis (1 dp: 41, 2 dp: 119, 3 dp: 188); in addition **29,229** (19.4%) carry ≥ 9 decimals |
| **Invalid Value** | `Nothing But Family (Nashville) 36.06, -86.67`; `Reno Oriental Rug Wash (Sparks) 39.54, -119.75`; `CVS Pharmacy (Tampa) 28.0604, -82.541` |
| **Domain Rule** | A storefront needs ≥ 4 decimals (≈ 11 m); 2 decimals ≈ 1.1 km, 1 decimal ≈ 11 km |

**Description:** The decimal-place profile (mode 7 dp = 46.8%, 6 dp = 26.8%, 10 dp = 16.1%) shows coordinates coming from several geocoders. The 348 coarse points are city- or ZIP-centroid geocodes (134 of them belong to businesses with an empty `address`, i.e. service-area businesses that hide their street location): they plot several unrelated businesses on the same spot and can be off by up to ~1-11 km, which distorts distance features, nearest-competitor counts and map clustering. The 9-10 dp values are a float-to-string artefact of another source; they are harmless but confirm the mixed provenance.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep values; add `geo_precision_dp` and `geo_quality` (`rooftop` ≥ 4 dp / `approximate` ≤ 3 dp) in Silver** | No value is invented; downstream models can down-weight or exclude approximate points; cheap and transparent |
| 2 | **Re-geocode the 348 rows from `address` + `city` + `postal_code`** | Gives true precision, but needs an external geocoder, and 134 of the 348 (38.5%) have an empty address (DQ-BUSINESS-032), so they cannot be re-geocoded |
| 3 | **Round all coordinates to a common precision (e.g. 6 dp)** | Removes the format inconsistency for the 9-10 dp rows, but cannot add precision to coarse ones |
| 4 | **NULL the coarse coordinates** | Loses metro-level location that is still useful for city-level analytics |

**Best Logical Solution:** **Option 1 — keep values and expose a precision / quality flag**, optionally followed by Option 2 for the rows that do have a full address. Precision is metadata about the value, so it should be recorded, not hidden.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.3.3 (already documented, no new ID):** `stars` stored as DOUBLE instead of DECIMAL(2,1) → **DQ-BUSINESS-001** (no float residue found). Lower-case Canadian postal code `T6x 2H4` (1) → **DQ-BUSINESS-016**. `RestaurantsPriceRange2` `'None'` → **DQ-BUSINESS-002**. Purely numeric name with trailing space (`'2300 '`) → **DQ-BUSINESS-004**.
>
> **Reviewed and not raised (valid real-world forms):** `review_count` is BIGINT with no sentinels (range 5 - 7,568); `RestaurantsPriceRange2` uses only bare digits `1`-`4`; no float / scientific / lost-leading-zero / ZIP+4 / separator artefacts in `postal_code`; house-number ranges (1,007), fractions (61), alphanumeric (463) and PO Boxes (4) are legitimate USPS forms (never cast house numbers to INT); ordinal street words vs digits (132 vs 12,975) is part of address standardisation (DQ-BUSINESS-031); 10 purely numeric names are genuine brand names (`24`, `1984`, `360`).

---
#### 🚨 DQ-BUSINESS-028 — Yes/No Facts Stored in Four Physical Encodings (BIGINT 0/1, `'True'`/`'False'` Strings, Dict Tokens with `u'k'` / `'k'` Keys, Negative Enum Words)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-028` |
| **Severity** | MEDIUM (format / encoding; no wrong values, but no native BOOLEAN anywhere and parsing is mandatory before any use) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.4 Boolean Representation Consistency** (Cell 70), checks A "encoding inventory", C "nested-dict key quoting" and D "negation as category word" |
| **Affected Columns** | `is_open` (BIGINT), 24 boolean `attributes.*` (STRING), 7 dict-string attributes (`Ambience`, `BestNights`, `BusinessParking`, `DietaryRestrictions`, `GoodForMeal`, `HairSpecializesIn`, `Music`), 4 enums (`WiFi`, `Smoking`, `BYOBCorkage`, `Alcohol`) |
| **Total Affected Rows** | 78 yes/no facts in **0** native BOOLEAN columns. Dict key-quoting split: **12,255** dicts use Python-2 `u'k'` keys vs `'k'` in the rest (Ambience 4,713 / 43,728; BusinessParking 4,314 / 88,814; GoodForMeal 1,816 / 28,784; Music 771 / 7,486; BestNights 506 / 5,683; HairSpecializesIn 135 / 1,035; DietaryRestrictions 0 / 30). Negation as category word: WiFi `no` 21,831, Alcohol `none` 20,910, Smoking `no` 2,405, BYOBCorkage `no` 747 |
| **Invalid Value** | `is_open = 1`; `RestaurantsTakeOut = 'True'`; `BusinessParking = "{u'garage': False, u'street': True, ...}"` vs `"{'garage': False, 'street': True, ...}"` |
| **Domain Rule** | A yes/no fact is stored as BOOLEAN (TRUE / FALSE / NULL), with one encoding for the whole table (R5 Semantic Data Types of the Silver DDL) |

**Description:** Inside each encoding the data is clean: the 24 boolean attributes use only exact-case `'True'` / `'False'` / `'None'` (0 alternative spellings, 0 case variants, both TRUE and FALSE observed in every column); every dict value is a Python `True` / `False` / `None` token, and no dict mixes `u'k'` and `'k'` keys internally. The problem is the *mix across columns*: four parsers are required, the dict strings are Python `repr()` rather than JSON (so `from_json` fails on `u'` prefixes and on `True` / `None`), and the 2/3 key-quoting split shows two source export versions (same root cause as DQ-BUSINESS-003). Negation-as-enum (`WiFi = 'no'`) is a valid category but must be mapped explicitly when a boolean flag (`has_wifi`) is derived.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Decode once in Silver: `is_open` → BOOLEAN; 24 attrs via an exact map `'True'→TRUE`, `'False'→FALSE`, `'None'`/NULL→NULL; dict strings via one anchored parser that strips the `u` prefix and maps Python tokens, landing in the 7 child tables (BOOLEAN flags); enums kept as enums, with derived boolean flags documented (`has_wifi = WiFi IN ('free','paid')`)** | Matches the Silver DDL (R3/R5), one deterministic rule per encoding, already prototyped in TE.1 (`dec_bool`, `dec_dict`); no value changes |
| 2 | **Convert dict strings to JSON (`replace("'", '"')`, `True→true`) and use `from_json`** | Standard tooling, but naive quote replacement breaks on apostrophes inside keys/values and is fragile |
| 3 | **Python `ast.literal_eval` UDF** | Exact Python semantics, but row-by-row UDF is slow on 150k × 7 dicts and not Spark-Connect friendly at scale |
| 4 | **Keep STRING columns in Silver** | No work, but violates the contract and pushes parsing to every consumer |

**Best Logical Solution:** **Option 1 — encoding-specific deterministic decoders into native BOOLEAN columns / child tables**, validated with the lossless-decode check from TE.1. It is exact, fast (native Spark functions) and satisfies the Silver contract.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.3.4 (already documented, no new ID):** 21 of 24 boolean attributes mix SQL NULL and the `'None'` literal (largest: RestaurantsDelivery 3,948, RestaurantsTakeOut 2,105, OutdoorSeating 1,882) → **DQ-BUSINESS-002**. `Open24Hours` sparsity / conflict → **DQ-BUSINESS-026**. BYOB policy conflicts → **DQ-BUSINESS-024**.
>
> **Reviewed and not raised:** no alternative boolean spellings (`true`, `TRUE`, `1`, `yes`, `Y`, `u'True'`); no one-sided boolean columns; all dict value tokens in the {True, False, None} domain; `is_open` 0/1 decodes losslessly.

---
#### 🚨 DQ-BUSINESS-029 — Multiple Consecutive Spaces and Dangling Separators in `name`, `address`, `city`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-029` |
| **Severity** | LOW (text normalization; invisible to readers, breaks exact matching and dedup) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.5 Text Normalization Issues** (Cell 72), checks A "multiple consecutive spaces" and D "trailing , ; punctuation" |
| **Also Confirmed In** | 6.3.1 (Cell 60): double-spaced spellings appear inside city clusters (`St  Louis`, `Mt  Juliet`, `Santa  Barbara`) and brand clusters (`h h  gregg`) |
| **Affected Columns** | `name`, `address`, `city` |
| **Total Affected Rows** | Multiple spaces: **name 263**, **address 245**, **city 9**. Dangling trailing separator: **name 1**, **address 6** (city 17 already in DQ-BUSINESS-018). ≈ **524** values in total (0.35% of rows) |
| **Invalid Value** | `'PAWS  Plaquemines Animal Welfare Society'`, `'David Gower, Jr.  - Coldwell Banker Preferred'`, `'10303  108 Street NW'`, `'14350 Mundy Dr, Ste  700'`, `'West  Deptford'`, `"Mom's Kitchen,"`, `'2810 Limekiln Pike,'`, `'150 2nd Ave N, 970,'` |
| **Domain Rule** | Words are separated by exactly one space; a value does not end with a list separator |

**Description:** These are keying artefacts (double-tap of the space bar, copy/paste of a multi-line address, a removed suite number leaving its comma). No control characters (`\t \n \r \x00`) were found in any field and `state` / `category_name` are clean. The impact is on equality-based logic: `'8330  82 Avenue'` and `'8330 82 Avenue'` are two addresses for an exact-match dedup or a join to a geocoder cache.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **In Silver apply `regexp_replace(x, '\\s+', ' ')` then strip a trailing `[,;]` (after the trim of DQ-BUSINESS-004 and invisible-char removal of DQ-BUSINESS-017)** | The change is meaning-preserving (whitespace / a dangling comma carry no information), deterministic and cheap; it is the industry-standard canonical text form |
| 2 | **Normalise only in derived match keys (`name_match_key`, `address_match_key`)** | Keeps raw text byte-identical, but displays and exports still show the artefacts |
| 3 | **Leave as-is** | No effort, but dedup and joins stay unreliable |

**Best Logical Solution:** **Option 1 — collapse whitespace and strip dangling separators in the cleaned Silver columns**, applied in one ordered text-hygiene step together with DQ-BUSINESS-004 and DQ-BUSINESS-017. It is lossless in meaning and benefits every consumer.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-030 — Inconsistent Capitalisation in `city` and `address` (`TAMPA`, `tampa`, `EdMonton`, `4774 PARIS AVE`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-030` |
| **Severity** | LOW (text normalization; feeds DQ-BUSINESS-021 city fragmentation) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.5 Text Normalization Issues** (Cell 72), check B "capitalisation consistency" |
| **Also Confirmed In** | 6.3.1 (Cell 60): case variants inside city clusters (`Tampa` 9,048 vs `TAMPA` 16 / `tampa` 2; `Tucson` vs `TUCSON` 6; `Philadelphia` vs `PHILADELPHIA` 4) |
| **Affected Columns** | `city`, `address` (and `name`, INFO only) |
| **Total Affected Rows** | **city 179** (120 ALL CAPS, 56 all lower, 3 odd interior capitals); **address 62** ALL CAPS / all lower containing at least one 3-letter word (the raw case table shows 564 ALL CAPS + 29 all lower, but most of those contain only numbers plus 1-2-letter tokens such as directionals or unit letters, which have no case to normalise); **name** 855 ALL CAPS / all lower and 4,018 interior caps — *informational only* |
| **Invalid Value** | `'TAMPA'`, `'NEW PORT RICHEY'`, `'abington'`, `'phoenixville'`, `'EdMonton'`, `'ClearWater'`, `'20 mullanphy gardens'`, `'593 s lowry st'`, `'4774 PARIS AVE'` |
| **Domain Rule** | Place names and street addresses use one case convention (Title case for display, upper case for USPS matching) |

**Description:** City and address casing is an entry habit, not information. For `name`, casing is frequently a deliberate brand mark (`ALDI`, `YMCA`, `CAVA`, `west elm`, `lululemon`, `GameStop`), so changing it would corrupt the display name; it is therefore not counted as an error here and is handled by the case-folded match key of DQ-BUSINESS-022.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **`city`: resolve through the canonical mapping of DQ-BUSINESS-021 (case-folded key → official name). `address`: case-fold only in `address_match_key`; keep raw display. `name`: never re-case** | The mapping gives correct official casing (`McKees Rocks`, `O'Fallon`) that `initcap()` cannot; addresses and names keep the owner's text while matching becomes case-insensitive |
| 2 | **`initcap()` on city and address** | Simple, but produces errors: `Mckees Rocks`, `O'fallon`, `Nw`, `Ste 2b`, `Us-19` |
| 3 | **`upper()` everything (USPS style)** | Good for postal matching, poor for display, and destroys brand casing if applied to `name` |
| 4 | **Leave as-is** | City groupings stay split (DQ-BUSINESS-021) |

**Best Logical Solution:** **Option 1 — dictionary-based canonical city, case-insensitive match keys for address/name.** It fixes the analytical impact without introducing new casing errors.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-031 — Address Tokens Not in USPS Pub. 28 Standard Form (`Street` / `St.` vs `St`, `Suite` vs `Ste`, `North` vs `N`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-031` |
| **Severity** | LOW / INFO (address standardisation; affects dedup and geocoding joins, not validity) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.5 Text Normalization Issues** (Cell 72), check E "non-standard suffix / unit / directional" |
| **Also Confirmed In** | 6.3.3 (Cell 68): ordinal street names written as words vs digits (`Second Ave` 132 vs `2nd Ave` 12,975) |
| **Affected Columns** | `address` |
| **Total Affected Rows** | **11,165** of 145,219 non-empty addresses (7.69%) contain at least one non-standard token |
| **Invalid Value** | Street: `street` 2,670 / `st.` 181 / `str` 6; Avenue: `avenue` 1,882 / `ave.` 99 / `av` 18; Road 1,283; West 1,157; South 1,078; East 896; North 852; Highway 696; Suite 564 (`suite` 533 / `ste.` 31); Drive 430; Boulevard 370; Unit `#` 189 |
| **Domain Rule** | USPS Publication 28 standard abbreviations (ST, AVE, BLVD, RD, DR, HWY, PKWY, LN, STE, UNIT, N/S/E/W); Canada Post equivalents for AB |

**Description:** The same physical address can be written `1616 North Chapala Street, Suite 2` or `1616 N Chapala St, Ste 2`. This is legitimate free-text variation, but it defeats exact-match deduplication (two listings of the same business), joins to external address / census datasets, and geocoding caches. Standard forms already dominate (e.g. `ST` 36,053 vs 2,857 variants).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw `address`; add `address_standardized` (and `address_match_key`) in Silver using a token-level USPS Pub. 28 dictionary applied to whole words only (suffix in final street position, directional as prefix/suffix, unit designators), plus `5th`↔`Fifth` ordinal mapping** | Deterministic and auditable; only whole-word tokens are touched (avoids `Main St` → `Main Saint` errors); raw text preserved for display |
| 2 | **Use an address-parsing library (libpostal / usaddress) to split into components** | Most robust, gives house number / street / unit columns, but adds a native dependency and UDF cost |
| 3 | **Overwrite `address` with the standardised form** | Consistent, but loses what the owner entered and makes mistakes irreversible |
| 4 | **Leave as-is** | Valid for display; dedup stays weak |

**Best Logical Solution:** **Option 1 — derived standardised address column with whole-word USPS dictionary**, with Option 2 as a later enhancement if address-level matching becomes a core use case.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.3.5 (already documented, no new ID):** leading / trailing whitespace (name 150, address 3, city 49) → **DQ-BUSINESS-004**. NBSP / zero-width characters (name 15, address 10, city 4) → **DQ-BUSINESS-017**. Empty-after-trim `address` (5,127) → **DQ-BUSINESS-032**; `postal_code` (73) → **DQ-BUSINESS-015**. City trailing commas (17) → **DQ-BUSINESS-018**. `category_name` fragment `& Probates` (38) → **DQ-BUSINESS-013**. Curly apostrophes in names (701) → **DQ-BUSINESS-022**.
>
> **Reviewed and not raised:** 0 tab / newline / carriage-return / other control characters in any field; 0 HTML entities, 0 mojibake, 0 decomposed (NFD) accents; 621 names and 8 addresses with non-ASCII letters are legitimate (`Café`, `Poké`, `La Cañada Dr`) — keep, NFC-normalise.

---
#### 🚨 DQ-BUSINESS-032 — Empty-String `address` Masquerading as a Value (Masked NULL, Not Missing at Random)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-032` |
| **Severity** | FINDING (null representation; 0 SQL NULLs exist, so every completeness metric under-reports missing addresses) |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.6 Null Representation Consistency** (Cell 76), check A "address (masked nulls)" and the category profile of empty addresses |
| **Also Confirmed In** | 6.3.5 (Cell 72): "address (empty after trim)" = 5,127. 6.3.3 (Cell 68): 134 of the 348 coarse coordinates belong to these businesses (DQ-BUSINESS-027). Same pattern as empty `postal_code` (DQ-BUSINESS-015); 56 businesses have both empty. The single `'.'` address is part of DQ-BUSINESS-019 |
| **Affected Columns** | `address` |
| **Total Affected Rows** | **5,128** of 150,346 businesses (3.41%): **5,127** `''` + **1** punctuation-only `'.'`; **0** SQL NULL |
| **Invalid Value** | `''`, `'.'` |
| **Domain Rule** | Unknown / not disclosed = SQL NULL only |

**Description:** `address` has two representations of "no address" (`''`, `'.'`) and none of them is NULL, so `count(address)` and `isNull()` report 100% completeness. The empty values are **not random**: Home Services make up 45.6% of them, Local Services 20.1%, Event Planning 14.7%, Food Trucks 7.8%, Contractors 8.2%, Home Cleaning 5.8%, Landscaping 5.1%. These are **service-area businesses** (plumbers, cleaners, caterers, food trucks) that travel to the customer and deliberately hide their street address on Yelp. So the missingness is MNAR / structurally explained, and the empty value actually carries a business meaning ("no public storefront").

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Convert `''` and `'.'` → SQL NULL in Silver and add a derived `is_service_area_business` (address NULL) flag; document the MNAR reason in the column comment** | One null representation (consistent with DQ-BUSINESS-002 / -015); completeness metrics in 6.4 become correct; the business meaning of the missing address is kept explicitly instead of hidden in an empty string |
| 2 | **Convert to NULL only** | Correct representation, but loses the easy signal that these are mobile / service-area businesses |
| 3 | **Impute the address by reverse-geocoding coordinates** | Fabricates a storefront address that the business intentionally does not have, and coordinates are often coarse for these rows |
| 4 | **Drop the 5,128 businesses** | Removes a whole business segment (home / local services) and biases every category analysis |

**Best Logical Solution:** **Option 1 — `''` / `'.'` → NULL plus an explicit service-area flag.** It standardises the null representation and preserves the real-world meaning of the missingness for 6.4 pattern analysis.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-033 — "Closed" Days Encoded Implicitly as NULL Inside a Listed Week

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-033` |
| **Severity** | MEDIUM (null representation; one NULL carries two meanings, "closed" and "not entered") |
| **Detected In** | Phase 6.3 — Format & Encoding Standardization → **6.3.6 Null Representation Consistency** (Cell 76), check E "hours_<day> (NULL inside a listed week)" |
| **Also Confirmed In** | Independent SQL check on `bronze_final.yelp_business` (same count). 6.3.2 (Cell 65): `0:0-0:0` (73,908 slots) is the other ambiguous schedule value (DQ-BUSINESS-007) |
| **Affected Columns** | `hours.Monday` … `hours.Sunday` → `df_hours.hours_<day>` |
| **Total Affected Rows** | **55,837** of 127,123 businesses with a schedule (43.9%) have at least one NULL day; 88,846 NULL day-slots in total (889,861 possible slots - 801,015 filled). Separately, 23,223 businesses have no `hours` struct at all (single, consistent representation) |
| **Invalid Value** | `hours = {Monday: '9:0-17:0', ..., Friday: '9:0-17:0', Saturday: NULL, Sunday: NULL}` |
| **Domain Rule** | Within a published schedule every day should state its status explicitly: open window, closed, or unknown |

**Description:** Yelp shows a missing day as "Closed", so in most cases a NULL day inside a listed week means *closed* (weekend closures of offices, Monday closures of restaurants). But the same NULL is also produced when an owner simply never entered a day, and the data cannot distinguish the two. Downstream, a naive `hours_saturday IS NULL` is read as "unknown" by SQL semantics but as "closed" by Yelp semantics; "open on weekends" or "days open per week" features will differ depending on which reading is used.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw NULLs; add per-day `is_closed_<day>` = (hours struct present AND day NULL), `days_open_per_week`, and a table-level comment stating the Yelp convention "NULL day in a listed week = closed"** | Applies the source platform's own documented semantics explicitly, leaves NULL for the truly unknown case (no struct), and makes features reproducible |
| 2 | **Replace NULL days with a sentinel string `'closed'`** | Explicit, but mixes text into a time-range column and breaks the `H:M-H:M` parser |
| 3 | **Treat NULL days as unknown everywhere** | SQL-pure, but contradicts what Yelp users see and under-counts closures for 55,837 businesses |
| 4 | **Impute the day from neighbouring days** | Invents opening hours that the business never published |

**Best Logical Solution:** **Option 1 — derived explicit `is_closed_<day>` flags following the Yelp convention, raw NULL preserved.** It removes the ambiguity for analytics without altering source values, and pairs with the DQ-BUSINESS-007 / -025 derived schedule columns.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.3.6 (already documented, no new ID):** 9,625 `'None'` literals across 32 scalar attributes (RestaurantsDelivery 3,948, RestaurantsTakeOut 2,105, OutdoorSeating 1,882, DriveThru 755 …) → **DQ-BUSINESS-002**. Nested dicts with masked "no information" (`'None'`, `'{}'`, all-None): BusinessParking 2,279, Ambience 557, GoodForMeal 303, Music 104, HairSpecializesIn 30, BestNights 11, DietaryRestrictions 1 → **DQ-BUSINESS-002** / **DQ-BUSINESS-006**. Empty `postal_code` (73) → **DQ-BUSINESS-015**. `'0:0-0:0'` (73,908) → **DQ-BUSINESS-007**. `Alcohol 'none'` (20,910) must survive null-cleaning (valid category, see 6.3.1).
>
> **Reviewed and not raised:** `name`, `city`, `state`, `category_name` have no masked nulls; no placeholder names / addresses (`TBD`, `N/A`, `Unknown`); `attributes` has one representation for "no attributes" (13,744 NULL structs, 0 all-NULL structs); `hours` has one representation for "no schedule" (23,223 NULL structs); `categories` uses SQL NULL only (103); no NaN in DOUBLE columns; no 0 placeholders in coordinates, `stars` or `review_count`. Missingness volumes and MCAR / MAR / MNAR classification continue in 6.4.

---
#### 🚨 DQ-BUSINESS-034 — Structural "Not Applicable" NULLs Dominate the Wide Attribute Model

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-034` |
| **Severity** | MEDIUM (modelling / completeness-semantics issue; no value is wrong, but NULL carries two meanings) |
| **Detected In** | Phase 6.4 — Completeness → **6.4.3 Per-Cell Null Rate** (Cell 82), "effective vs applicable" breakdown |
| **Also Confirmed In** | 6.4.1 (Cell 80): 5 near-empty attributes (< 5 % in-segment coverage: `OpenTwentyFourHours` 0.03 %, `RestaurantsCounterService` 0.03 %, `DietaryRestrictions` 0.04 %, `AgesAllowed` 0.5 %, `BYOBCorkage` 2.13 %). 6.4.4 (Cell 84): 0 MCAR-consistent attributes outside these near-empty ones |
| **Affected Columns** | 32 scalar attributes in `df_attributes`, 7 nested dicts |
| **Total Affected Rows** | Wide model: **8,419,376** cells; raw NULL **58.30 %**, effective **58.51 %**, but only **36.47 %** once cells that do not apply to the business type are excluded. `df_attributes` alone: 78.8 % effective vs 62.7 % applicable |
| **Invalid Value** | `attr_restaurants_reservations = NULL` for a dentist (not applicable) is stored identically to `NULL` for a restaurant (unknown) |
| **Domain Rule** | Missingness metrics and Silver columns must distinguish *unknown* from *not applicable to this business type* |

**Description:** Yelp asks a different questionnaire per business type (restaurants, nightlife, salons, health). Flattening every attribute into one wide row gives every business ~40 attribute columns, most of which do not apply to it. Around 22 percentage points of the wide-model null rate are this structural "not applicable" NULL, not missing data. A completeness KPI or an ML feature matrix built on the raw wide table overstates missingness, and imputation would invent answers to questions that were never asked. The near-empty attributes add almost-all-NULL columns with no analytic value.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Store attributes in the long (business_id, attribute, value) Silver table, plus an applicability map (attribute → segment) used by completeness metrics; archive the 5 near-empty attributes** | Long form stores only answered questions; applicability makes "unknown" measurable; removes dead columns |
| 2 | **Keep wide table but add a `<attr>_applicable` flag per attribute** | Correct semantics, but doubles column count |
| 3 | **Fill non-applicable cells with a sentinel (`'N/A'`)** | Breaks boolean / enum types |
| 4 | **Impute all NULLs** | Fabricates answers for non-applicable questions |

**Best Logical Solution:** **Option 1 — long attribute model plus an explicit applicability map; drop / archive near-empty attributes.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-035 — Missingness Is Not Random: It Follows Popularity and Business Type, and Attributes Go Missing as a Block

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-035` |
| **Severity** | HIGH for analytics (complete-case analysis and naive imputation are biased) |
| **Detected In** | Phase 6.4 — Completeness → **6.4.4 MCAR / MAR / MNAR** (Cell 84) and **6.4.5 Missing Value Correlation** (Cell 85) |
| **Also Confirmed In** | 6.4.2 (Cell 81): 28,570 rows > 25 % profile missing, 102 skeleton listings. 6.4.7 (Cell 89): 1,543 businesses not analytics-ready |
| **Affected Columns** | `attributes` block, `hours` block, 38 of 44 tested fields classified MAR (Cramér's V >= 0.10) |
| **Total Affected Rows** | attributes 13,960 / schedule 23,223 missing; **4,298** businesses miss both; 36 of 45 expected-attribute pairs co-missing with phi >= 0.3 |
| **Invalid Value** | n/a (pattern issue) |
| **Domain Rule** | Silver features and KPIs must not assume missing attributes / hours are a random sample |

**Description:**
* **Popularity driven (MAR):** missing schedule falls from **23.7 %** (5–9 reviews) to **0.8 %** (500+); missing attributes from 17.4 % to 0 %. Crowd-sourced attributes such as `NoiseLevel` are missing for 76.9 % of 5–9-review restaurants against 1.3 % of 500+.
* **Business-type driven:** "all-business" attributes are in practice category-specific (`GoodForKids` missing 36.7 % for food against 95.4 % for service-area; `ByAppointmentOnly` 9.1 % for hair against 92.1 % for food).
* **Regional sourcing gap:** `BusinessAcceptsCreditCards` is missing for **95.8 %** of Alberta (Edmonton) businesses against 15.5 % in Missouri. This is a market-level collection difference, not business behaviour.
* **Block-wise:** restaurant attributes vanish together (`RestaurantsAttire` ↔ `RestaurantsGoodForGroups` phi 0.80; P(GoodForGroups missing | Attire missing) = 96.5 %). A business without hours is 2.4× as likely to also lack attributes (18.5 % vs 7.6 %). A listing is enriched as a whole questionnaire or not at all.
* **Structural MNAR:** missing address is concentrated in service-area businesses (11.6 % against 0.9–1.4 % for storefront segments), see DQ-BUSINESS-032.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep NULLs, add per-business `profile_completeness_pct`, `has_attributes`, `has_hours`, `is_low_information` flags and document MAR drivers; analyses stratify / weight by `review_count` band and segment** | Preserves all rows, makes the bias visible and controllable downstream |
| 2 | **Impute attributes from other attributes / similar businesses** | Block-wise missingness means there is usually no observed attribute to impute from; would fabricate answers mostly for low-traffic businesses |
| 3 | **Drop incomplete businesses (complete-case)** | Removes 24 % of businesses, skewed towards small / new / non-restaurant businesses |
| 4 | **Treat missing as `False`** | Wrong for crowd-sourced attributes: absence means "not asked / not answered", not "no" |

**Best Logical Solution:** **Option 1 — flag and stratify, never complete-case or blanket-impute.** For the Alberta credit-card gap, report the metric per market or exclude AB from that attribute's KPIs.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-036 — `is_open = TRUE` for Businesses Silent for Years (Status Without an As-Of Date)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-036` |
| **Severity** | MEDIUM (temporal completeness; the status field is probably out of date for a large share of "open" businesses) |
| **Detected In** | Phase 6.4 — Completeness → **6.4.6 Temporal Gap Analysis** (Cell 87), part B, business review timeline |
| **Also Confirmed In** | Independent SQL check (same counts). Review month grid 2005-02 → 2022-01 has **0** missing months (204 present); the 2020-03 → 2020-07 drop (-46 % to -72 % YoY) matches COVID lockdowns and is not a collection gap |
| **Affected Columns** | `is_open` (with `yelp_review.date` as evidence) |
| **Total Affected Rows** | **34,047** of 119,698 open businesses (28.4 %) have no review in the last 12 months before the snapshot end (2022-01-19); **7,608** are silent > 3 years (longest: 4,536 days, last review 2009) |
| **Invalid Value** | `is_open = 1`, `last_review = 2009-08-19` |
| **Domain Rule** | An activity status should have an as-of date, and "open" should be supported by recent activity |

**Description:** `is_open` is the only lifecycle signal in the business table and it has no timestamp. Many "open" businesses have had no customer activity for years, which on Yelp usually means the business closed without anyone flagging it. Long silent periods *inside* a history (46,784 businesses with a > 2-year gap) are expected for low-volume businesses and are not data loss. The review history itself is complete: every business has reviews, and only **9** businesses have one review fewer loaded than their `review_count`. The opposite direction (more reviews loaded than `review_count`) is a consistency issue for 6.6.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep `is_open` as published; derive `last_review_date`, `days_since_last_activity` (reviews + tips + check-ins) and `is_active_12m` in Silver; document `is_open` as "Yelp status at snapshot, not verified"** | Source value preserved; analysts get an evidence-based activity flag |
| 2 | **Overwrite `is_open = 0` when silent > N years** | Changes source data on a heuristic; some businesses are open but simply not reviewed |
| 3 | **Drop silent businesses** | Loses valid history for time-series analysis |

**Best Logical Solution:** **Option 1 — derived activity recency flags next to the unchanged `is_open`.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-037 — `business_id` Values Starting With `-` (Export / CLI Interoperability Hazard)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-037` |
| **Severity** | LOW (the key is valid and unique, but it can be silently corrupted outside Databricks) |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.2 Key-Level Uniqueness** (Cell 93), check A "starts with '-' (CLI / spreadsheet hazard)" |
| **Also Confirmed In** | 6.5.2 (Cell 93): the same key passes NOT NULL, exact uniqueness, uniqueness after case-folding (0 collisions), no padding, and the `^[A-Za-z0-9_-]{22}$` format. So the issue is only the leading character |
| **Affected Columns** | `business_id` (PK of `yelp_business`; FK in all 10 business child tables, and in `yelp_review`, `yelp_tip`, `yelp_checkin`) |
| **Total Affected Rows** | **2,386** of 150,346 businesses (1.59%) |
| **Invalid Value** | `-4dYswJy7SPcbcERvitmIg`, `-ZXYcCyDjj1QimnJyGtYtg`, `-aeZuatjCDMV1X4gCTz9Ug`, `-7GDqSUaXrpC8Ql7nDBxWA` |
| **Domain Rule** | An identifier must survive every export and tool round-trip byte-for-byte. Yelp ids are 22-char URL-safe base64, so `-` and `_` are legal anywhere, including position 1 |

**Description:** These ids are correct inside Delta and Spark SQL. The risk comes from consumers outside the lakehouse:
* Excel / Google Sheets read a cell starting with `-` as a formula or negative number, giving `#NAME?` or a rewritten value. The same rule is behind CSV-injection warnings.
* Shell tools and the Databricks / Azure CLIs read `-4dYs...` as a command-line option.
* Some BI tools trim or re-type the value.

Because `business_id` is also the join key to reviews, tips and check-ins, a single corrupted export breaks joins for about 1.6% of businesses and their reviews without any error. This is an interoperability issue, not a data error, and the value must not be changed.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep `business_id` unchanged; document the hazard in the DDL column comment and enforce quoting in every export path** (`quoteAll=true` for CSV, text-typed columns for Excel, `--` before positional ids in CLI calls) | No value change, so lineage and FK joins to review / tip / checkin stay intact. The fix sits where the problem happens (the consumer) |
| 2 | **Add an integer surrogate `business_sk` in Gold dimensions** for BI tools, keeping `business_id` as the natural key | Integer keys are immune to formula parsing and join faster in BI. Costs one extra mapping, and the problem still exists for anyone exporting `business_id` |
| 3 | **Prefix or rewrite the ids (e.g. `b_-4dYs...`)** | Removes the hazard, but changes the key: every review / tip / checkin FK has to be rewritten the same way, and the ids no longer match the Yelp source |
| 4 | **Do nothing** | Correct inside Databricks, but the first spreadsheet or CLI hand-off can silently break 2,386 businesses |

**Best Logical Solution:** **Option 1 — keep the id as-is, document it, and enforce quoting on export**, with Option 2 as an optional Gold improvement. The source key stays authoritative and joinable, and the risk is handled at the boundary where it occurs.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-038 — Same Storefront Listed Under Several `business_id`s (Natural-Key Duplicate Listings)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-038` |
| **Severity** | HIGH (entity-level duplication: reviews, ratings and check-ins of one place are split across ids) |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.3 Composite Key Uniqueness** (Cell 95), check A `(name, address, city, state, postal_code)` exact and check C `(name_key, addr_key, zip_key)` normalised |
| **Also Confirmed In** | 6.5.3 B (Cell 95): 36 surplus rows share name + identical lat/lon. 6.5.1 C (Cell 92): 0 static-profile duplicates, so the copies are **not** byte copies (categories, attributes, stars and review counts differ). The 51 extra duplicates found only after normalisation come from the spelling issues DQ-BUSINESS-021 / 022 / 029 / 031. 6.5.2 (Cell 93) shows `business_id` itself is unique, so a PK check cannot catch this |
| **Affected Columns** | `name`, `address`, `city`, `state`, `postal_code` (key); impact on `stars`, `review_count`, `is_open`, and every FK in review / tip / checkin |
| **Total Affected Rows** | **Exact:** 121 groups / 248 rows / **127 surplus** (0.084%). **Normalised:** 172 groups / 350 rows / **178 surplus** (0.118%), 41 groups only visible after normalisation |
| **Invalid Value** | `AMC Indianapolis 17 @ 4325 S Meridian St`: 2 ids, one closed (69 reviews, 4.0 stars), one open (72 reviews, 3.5 stars). `Bourbon Orleans Hotel @ 717 Orleans St`: 45 vs 306 reviews. `Fat Hen Grocery`: `7457 St Charles Ave` vs `7457 St. Charles Ave`. `Telus World of Science`, `Whitts Barbecue`, `Joe’s Cafe` / `Joe's Cafe` (name spelling only) |
| **Domain Rule** | One physical business location = one entity. Yelp keeps one business page per location, so the natural key (name + street address + postal code) must identify at most one active listing |

**Description:** The same storefront exists under two or more `business_id`s. By `is_open` status, the exact groups split as:

| Pattern | Groups | Rows | Meaning |
| --- | --- | --- | --- |
| all open | 52 | 109 | **Live duplicates**: two active pages for one place |
| open + closed | 56 | 112 | **Re-listing**: Yelp opened a new page and closed the old one, so the history is split |
| all closed | 13 | 27 | Historical duplicates, low impact |

The normalised groups split 82 / 71 / 19 the same way.

The impact is measurable:
* **Reviews:** the 121 exact groups hold 13,616 reviews, and **3,346 (24.6%) sit on the smaller copy**, invisible to anyone reading the main page.
* **Ratings:** stars differ inside 90 of 121 groups, by **1 star or more in 48 groups** (max 2.0).
* **Analytics:** per-business ratings, review counts, "top-rated" rankings and business counts per city / category are biased, and check-ins and tips are split the same way.

**Caveat:** only 3 groups have 3+ rows, and they are legitimate multi-unit venues: several Starbucks units inside airport terminals (STL `10701 Natural Bridge Rd`, TPA `4100 George J Bean Pkwy`) and XpresSpa at PHL `8000 Essington Ave`. They need a unit / terminal qualifier, not a merge.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep every `business_id`; add a deterministic `canonical_business_id` crosswalk in Silver** built from the normalised natural key (`name_key`, `addr_key`, `zip_key`). Survivorship order: open > most reviews > highest id. Send groups with 3+ rows or airport / mall addresses to manual review | Reviews, tips and check-ins keep their original FK (no cross-table rewrite). Gold can aggregate by `canonical_business_id` to re-join split reviews and recompute stars from them. Deterministic and auditable, and reversible if a merge is wrong |
| 2 | **Physically merge: keep one row, move the others to `quarantine`, and repoint review / tip / checkin FKs** | Gives the cleanest Silver table, but it is destructive and touches 4 tables. Bronze-to-Silver lineage of the dropped ids is lost, and a false merge (airport chains) is hard to undo |
| 3 | **Probabilistic entity resolution (e.g. Splink / Jaro-Winkler + geo + category)** | Higher recall (catches "Little Caesars" vs "Little Caesars Pizza"), but non-deterministic, needs tuning and labelled pairs. Better suited to a later Gold enrichment |
| 4 | **Do nothing** | Ratings and review counts stay split for 248–350 listings; every per-business KPI is wrong for them |

**Best Logical Solution:** **Option 1 — keep all ids and add a deterministic `canonical_business_id` crosswalk (normalised natural key + survivorship rules), with multi-unit venues routed to manual review.** It fixes the analytics without rewriting any FK, stays reproducible from the 6.5.1 match keys, and leaves room for Option 3 later.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-039 — Same-Name Listings Within 100 m (Spatial Near-Duplicates, Re-Listings and Copied Geocodes)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-039` |
| **Severity** | MEDIUM (candidate duplicates; needs confidence tiers before any action) |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.3 Composite Key Uniqueness** (Cell 95), check D "same `name_key` within 100 m (haversine)" |
| **Also Confirmed In** | 6.5.3 A/C (Cell 95): the 144 same-address pairs are the DQ-BUSINESS-038 groups seen spatially. 6.5.3 B (Cell 95): 13 same-name groups share an identical geocode but a **different** address text. Coordinate quality issues DQ-BUSINESS-011 / 027 explain part of the jitter |
| **Affected Columns** | `name`, `latitude`, `longitude`, `address`, `is_open` |
| **Total Affected Rows** | **397** candidate pairs involving **782** businesses (0.52%). 50 pairs have differently spelled names |
| **Invalid Value** | `IHOP` (Boone): `702 Blowing Rock Rd` vs `825 Nashville Pike, Ste D` at **0 m**. `Union Taco`: `4161 Main St` vs `4229 Main St` at 0.2 m. `Fox and Hound Pub & Grille`: `14490 Lowe Way` vs `14490 Lowes Way`. `East End Alehouse` vs `East End Ale House` (`, 712 Main St` vs `712 Main St`) |
| **Domain Rule** | Two listings of the same brand at (almost) the same point are one location, unless the venue is multi-unit (airport, mall, hotel + its restaurant). Two different street addresses cannot share the exact same rooftop geocode |

**Description:** Pairs by match basis and open status:

| Match basis | all open | open + closed | all closed | Total |
| --- | --- | --- | --- | --- |
| same address (normalised) | 67 | 58 | 19 | 144 |
| ≤ 25 m, address differs | 60 | 49 | 19 | 128 |
| 25–100 m, address differs | 47 | 53 | 21 | 121 |
| no address (service-area) | 3 | 1 | 0 | 4 |
| **Total** | **177** | **161** | **59** | **397** |

High-confidence pairs (same address or ≤ 25 m): 128 live, 108 re-listing, 38 historical. The 249 "address differs" pairs are not caught by the address-based check (DQ-BUSINESS-038), and they mix three different situations:
* **(a) Address variants of one place:** typos or suffix changes (`Lowe Way` / `Lowes Way`).
* **(b) Relocations:** one id closed, a new one opened nearby. These are two real places in time, so they should be linked rather than merged.
* **(c) Copied geocodes:** different addresses carrying an identical lat/lon (IHOP 0 m apart with two different streets). This is a coordinate error, not a duplicate.

A plain distance rule cannot tell these apart, so the list is a candidate set, not a delete list.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Tiered candidate list:** auto-link **high-confidence** pairs (same normalised address, or ≤ 25 m with the same `zip_key`) into the DQ-BUSINESS-038 `canonical_business_id` crosswalk, and flag **medium** pairs (25–100 m) with `is_possible_duplicate` for review | Uses the evidence already computed. High tier has very low false-merge risk, medium tier is surfaced without being acted on |
| 2 | **Link relocations instead of merging:** for open + closed pairs with different addresses, store `successor_business_id` on the closed listing | Keeps two real locations distinct, while letting Gold follow a business's history across the move |
| 3 | **Flag copied geocodes:** same point but different address → `geo_quality = 'shared_point'`, and re-geocode from the address where possible | Treats (c) as a coordinate problem; stops wrong coordinates feeding distance features and later dedupe runs |
| 4 | **Auto-merge every pair within 100 m** | Simple, but merges chain units in airports / malls and relocations, and the damage is hard to undo |
| 5 | **Tighten to ≤ 10 m only** | Fewer false positives, but misses address-variant duplicates whose geocodes jitter (DQ-BUSINESS-027) |

**Best Logical Solution:** **Option 1 combined with Options 2 and 3.** Auto-link the high-confidence tier, flag the medium tier, link relocations with `successor_business_id`, and mark copied geocodes. Each situation gets the treatment that matches what it really is, and nothing is merged on distance alone.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-040 — Placeholder Centroid Coordinates Shared by Many Address-Less Businesses

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-040` |
| **Severity** | MEDIUM (lat/lon is not a real location for these rows; geo analytics are distorted) |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.3 Composite Key Uniqueness** (Cell 95), check E "(latitude, longitude) placeholder centroid" |
| **Also Confirmed In** | 6.5.4 A (Cell 99): only 137,248 distinct (lat, lon) pairs for 150,346 businesses, so 13,098 rows reuse another business's geocode. 6.3.6 / DQ-BUSINESS-032: 5,127 empty-string addresses (service-area businesses). DQ-BUSINESS-027 (coarse coordinates) and DQ-BUSINESS-011 (coordinates outside every metro) are other coordinate-quality issues |
| **Affected Columns** | `latitude`, `longitude` (with `address = ''`) |
| **Total Affected Rows** | **2,238** businesses (1.49%) on **123** hotspot points (≥ 10 businesses, ≥ 50% address-less). **2,143** of them (95.8%) have no address, i.e. **41.8% of all address-less businesses** sit on these 123 points. 1,976 of the 2,238 are open |
| **Invalid Value** | `(34.420334, -119.7107494)` Santa Barbara: **146** businesses, 141 without address. `(34.4208305, -119.6981901)` Santa Barbara: 65 / 60. `(39.4835647, -119.7310213)` Reno: 51 / 48. `(27.9552692, -82.4563199)` "Tampa Bay": 35 / 33. `(39.5401545, -119.7483949)` Sparks: 34 / 34 (100%) |
| **Domain Rule** | `latitude` / `longitude` should locate the business premises. A business with no street address has no premises, so a coordinate shared by dozens of unrelated businesses is a city / ZIP centroid, not a location |

**Description:** Service-area businesses (movers, plumbers, mobile services, photographers) hide their address on Yelp. Yelp then geocodes them to a city or ZIP centroid, so up to 146 unrelated businesses sit on exactly the same point. Multi-tenant buildings also share geocodes, but they have addresses. The hotspots here are 95.8% address-less overall (the 15 largest are 89–100%), which separates the two cases. The effects:
* **Heatmaps and density maps:** artificial spikes downtown.
* **Distance and "nearby" features:** for example, distance to the closest competitor comes out as 0 m.
* **Geo-clustering:** clusters form around the fake points.
* **Other uniqueness checks:** these points create false same-place matches (`(latitude, longitude)` cannot be used as part of a location key for these rows).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the coordinates; add `geo_precision` in Silver** (`'rooftop'` / `'shared_point'` / `'placeholder_centroid'`), where placeholder = point shared by ≥ 10 businesses with ≥ 50% address-less, or `address` empty. Gold geo features filter or down-weight non-rooftop rows | No value is lost (the city-level position is roughly correct), the rule is deterministic and reproducible from this check, and each consumer decides how to use it |
| 2 | **Set lat/lon to NULL for placeholder rows** | Stops misuse, but throws away still-useful city-level location and turns 2,238 rows into missing coordinates for any map |
| 3 | **Re-geocode from the street address** | Gives correct points for the 95 hotspot rows that *do* have an address, but cannot work for the 2,143 address-less ones, and needs an external geocoder |
| 4 | **Add random jitter around the centroid** | Removes the visual spike, but fabricates locations. Not acceptable for a cleaned layer |

**Best Logical Solution:** **Option 1 — keep the coordinates and add a `geo_precision` flag**, plus Option 3 for the 95 hotspot rows that have an address. Coordinates are preserved, the fake precision is made explicit, and every downstream geo feature gets a reliable filter.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-041 — `Music.no_music` Is Never TRUE (Dead Flag That Contradicts All-False Music Dicts)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-041` |
| **Severity** | LOW–MEDIUM (semantic: the flag carries no information and makes all-False dicts ambiguous) |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.4 Distribution Uniqueness** (Cell 99), check C "value per key: each key takes both TRUE and FALSE" on `df_music` |
| **Also Confirmed In** | Same check flags `DietaryRestrictions.kosher` as constant (never TRUE), but that dict exists for only 30 businesses (low support, DQ-BUSINESS-034), so it is documented, not treated as an error. TE.2 key profile (Cell 35) shows `no_music` present in the Music dicts like every other key |
| **Affected Columns** | `attributes.Music['no_music']` → `df_music` (`key = 'no_music'`) / `yelp_business_music.no_music` |
| **Total Affected Rows** | `no_music = TRUE` in **0** of the 6,858 bronze Music dicts that contain the key. **4,622** Music dicts contain no TRUE value at all (every music type FALSE / None *and* `no_music` FALSE) |
| **Invalid Value** | `{'dj': False, 'background_music': False, 'no_music': False, 'jukebox': False, 'live': False, 'video': False, 'karaoke': False}` says neither "some music" nor "no music" |
| **Domain Rule** | `no_music` should be the complement of the music types: `no_music = TRUE` ⇔ every other music flag is FALSE. A flag that is always FALSE is either never collected or defaulted |

**Description:** The distinct-value check shows `no_music` has a cardinality of 1 (always FALSE). This most likely means the key is never collected as TRUE upstream and only appears with a default value when the Music block is saved. As a result, 4,622 businesses (62% of the 7,417 with a Music dict) have a dict that is logically contradictory: no DJ, no background music, no jukebox, no live music, no karaoke, and also *not* "no music".

Any model or report that reads `no_music` literally learns a constant. Any rule that derives "has music" from it is wrong. The all-False pattern also suggests that, for this dict, FALSE often means "not answered" rather than a confirmed no.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep `no_music` as-is but document it as non-informative in the DDL / data dictionary; in Silver Enriched derive `has_music = dj OR background_music OR jukebox OR live OR video OR karaoke`** and use that instead | Source values are untouched (lineage kept), the derived flag is built from keys that do vary, and consumers are warned off the dead column |
| 2 | **Overwrite `no_music = NOT has_music`** | Makes the dict internally consistent, but asserts facts the source never gave: an all-False dict may mean "not answered", not "no music" |
| 3 | **Drop `no_music` from `yelp_business_music`** | Removes the noise, but changes the agreed DDL contract and the bronze-to-silver 1:1 key mapping (6.1.1 coverage check would then report an unmapped leaf) |
| 4 | **Do nothing** | Constant feature in every model; the contradiction stays hidden |

**Best Logical Solution:** **Option 1 — keep the raw flag, document it as non-informative, and derive `has_music` from the other six keys.** It respects the contract and the source values, and gives analysts a meaningful music indicator. `DietaryRestrictions.kosher` gets the same "never TRUE, low support" note in the data dictionary.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 📝 6.6.2 Derived-Column Logic — Re-Detected Errors (no new ID)

The two FINDINGs in **6.6.2 Derived-Column Logic Checks (Cell 104)** are errors that are already documented. The 6.6 view adds detail, so the existing notes are updated here instead of being given new numbers.

| Check (Cell 104) | Violations | Existing Error | What 6.6 adds |
| --- | --- | --- | --- |
| C) `same non-midnight start/end` (start == end, not `0:0-0:0`) | **1,492** business-days, 238 businesses | **DQ-BUSINESS-007** (zero-length ranges) | Most of these values are **not random**. `6:0-6:0` (600 days / 93 businesses), `5:0-5:0` (396 / 58) and `9:0-9:0` (334 / 49) make up 89%. This is the pattern a 24-hour business produces when its operating day is entered as starting at 05:00 or 06:00 (e.g. White Castle `5:0-5:0` every day, flagged `latenight`). The derived duration is undefined (0 h or 24 h) |
| E) `no_music == NOT(any music type)` | **2,876** of 5,226 Music dicts where all six music types are answered | **DQ-BUSINESS-041** | The derivation rule a Silver `has_music` / `no_music` indicator would rely on fails for 55% of fully answered dicts. This confirms that `no_music` cannot be used as an indicator |

**Updated recommendation for DQ-BUSINESS-007 (same-time ranges):**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add `is_same_time_range` in Silver; set the derived duration to NULL; additionally set `likely_24h = TRUE` when the same start = end value is repeated on ≥ 5 days of the week** | The repeated-boundary pattern is a strong but not certain 24 h signal. Flagging keeps the raw value and makes the inference explicit and reversible |
| 2 | Treat every start = end as 24 h | Correct for most of the 5:00 / 6:00 cases, but wrong for isolated values such as `0:30-0:30` or `12:0-12:0`, which look like data-entry errors |
| 3 | Treat every start = end as closed | Removes 24 h businesses from "open now" and late-night analyses |

**Best Logical Solution:** **Option 1.** It keeps the source value, adds a documented heuristic only where the weekly pattern supports it, and leaves single-day cases unknown.

> **Reviewed and not raised in 6.6.1 / 6.6.2:** all 81 logical-relationship checks in Cell 103 pass. Every TE.2 value (scalars, 32 attributes, 21 hour columns, category list and order, and key/TRUE/FALSE/NULL counts of the 7 nested dicts) equals an independent re-derivation from bronze, so the restructuring is lossless row by row. `business_id` is a 22-char hash in all 11 frames, `stars` is always on the half-star grid, and all contract naming rules hold. The 73,908 `0:0-0:0` days (INFO) are the existing DQ-BUSINESS-007 / 025 ambiguity.

---
#### 🚨 DQ-BUSINESS-042 — `RestaurantsAttire = 'formal'` on Fast-Food and Casual Venues

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-042` |
| **Severity** | MEDIUM (logical contradiction; invalidates the top tier of an ordinal attribute) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.3 Logical Contradiction Detection** (Cell 106): checks `attire formal ∧ category Fast Food` (24) and `attire formal ∧ ambience casual/divey` (3) |
| **Also Confirmed In** | Same cell, INFO `attire formal ∧ NOT table_service` (11 of 70, incl. Domino's and Taco Bell). Independent SQL on bronze: 25 distinct businesses meet either rule |
| **Affected Columns** | `attributes.RestaurantsAttire` → `df_attributes.attr_restaurants_attire`; cross-checked with `df_categories.category_name` and `df_ambience` (`casual`, `divey`) |
| **Total Affected Rows** | **25** of the **70** businesses with `formal` attire (**36%** of the whole `formal` class) |
| **Invalid Value** | `Subway` / `McDonald's` / `Church's Chicken` / `Culver's` with `attire = formal`. `Subway` and `Culver's` also have `Ambience.casual = True` |
| **Domain Rule** | `formal` is the highest dress-code tier (jacket required). A fast-food counter or a casual / divey place cannot also require formal dress |

**Description:** `formal` is a rare value (70 businesses), and more than a third of it is contradicted by the business's own category or ambience. Most hits are units of national fast-food chains. This suggests a wrong drop-down choice by the owner, or a bulk chain-level default, rather than a real dress code. Because the class is so small, these errors noticeably distort any "formal dining" segment and the ordinal `casual < dressy < formal` feature.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the raw value; add `attire_conflict_flag` in Silver; in Silver Enriched expose `attr_restaurants_attire_validated` = NULL when the flag is set** | The source stays auditable, analysts get a consistent field, and no unsupported value (e.g. `casual`) is invented |
| 2 | **Recode the 25 to `casual`** | Probably right for fast food, but it asserts a value the source never gave |
| 3 | **Set the attribute to NULL in Silver Cleaned** | Consistent, but loses lineage of what the owner reported |
| 4 | **Do nothing** | 36% of the `formal` tier stays wrong |

**Best Logical Solution:** **Option 1 — flag and expose a validated column.** The contradiction is certain, but the correct value is not, so NULL-on-conflict in the enriched layer is the safest outcome.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-043 — Adults-Only Venues (`AgesAllowed` 21plus / 18plus) Marked `GoodForKids = True`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-043` |
| **Severity** | LOW–MEDIUM (logical contradiction; small volume but affects family-suitability filters) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.3 Logical Contradiction Detection** (Cell 106): check `ages_allowed 21plus/18plus ∧ good_for_kids` |
| **Also Confirmed In** | Independent SQL on bronze: 3 rows with `21plus` + `GoodForKids = True` (+1 with `18plus` = 4). The reverse soft rule (`allages` ∧ NOT good_for_kids, 5) is INFO only |
| **Affected Columns** | `attributes.AgesAllowed` → `attr_ages_allowed`; `attributes.GoodForKids` → `attr_good_for_kids` |
| **Total Affected Rows** | **4** of **52** adults-only businesses (7.7%) |
| **Invalid Value** | `Morgan's Pier` (21plus, kids = True), `Bollywood Beats` (18plus, kids = True), `Red Door Chinese Eatery` (21plus, kids = True) |
| **Domain Rule** | A venue that admits only adults (18+ / 21+) cannot be suitable for children |

**Description:** Both attributes describe the audience. `AgesAllowed` is an admission rule, so it is the stronger statement. A Chinese restaurant tagged `21plus` suggests the *age* answer is the wrong one in some cases, so neither field can be trusted on its own.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag (`audience_conflict_flag`); in Silver Enriched set a derived `family_friendly` = NULL for flagged rows** | The contradiction is certain, the true side is not (restaurant with `21plus` vs pier bar with kids = True) |
| 2 | **Give `AgesAllowed` precedence and set `GoodForKids = FALSE`** | An admission rule normally beats a suitability opinion, but this is wrong for misentered ages (`Red Door Chinese Eatery`) |
| 3 | **Do nothing** | 4 adults-only venues appear in "good for kids" searches |

**Best Logical Solution:** **Option 1 — flag and NULL the derived audience field.** At 4 rows it costs nothing and avoids guessing.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-044 — `Ambience` Simultaneously `divey` and `classy`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-044` |
| **Severity** | LOW–MEDIUM (mutually exclusive flags inside one nested dict) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.3 Logical Contradiction Detection** (Cell 106): check `divey ∧ (classy ∨ upscale)` |
| **Also Confirmed In** | Independent SQL on the raw `attributes.Ambience` string: 108 with `divey` + `classy`, **0** with `divey` + `upscale`. INFO checks in the same cell: `divey ∧ $$$$` (3) and `upscale ∧ $` (3) |
| **Affected Columns** | `attributes.Ambience['divey']`, `['classy']` → `df_ambience` (`key` = divey / classy) / `yelp_business_ambience.divey`, `.classy` |
| **Total Affected Rows** | **108** of **1,468** businesses with `divey = True` (7.4%) |
| **Invalid Value** | `The Howlin' Wolf` and `The Blarney Stone`: `{divey: True, classy: True, casual: True/False, upscale: False}` |
| **Domain Rule** | "Divey" (rough, cheap, unpretentious) and "classy" (refined, elegant) are opposite ends of the same scale |

**Description:** Ambience flags come from crowd votes, and Yelp shows a key as TRUE once enough reviewers pick it, so opposing tags can both pass the threshold. The pattern is concentrated in bars and music venues. Because `upscale` never co-occurs with `divey`, reviewers seem to use "classy" loosely ("a classy dive"), so this is a weak signal rather than corrupted data. It still breaks a one-hot or "dominant ambience" feature.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep both flags; add `ambience_conflict_flag`; when Silver Enriched derives one `primary_ambience`, return `mixed` for flagged rows** | Crowd-sourced flags are opinions, so both are real observations. A derived single label must not pick one arbitrarily |
| 2 | **Drop `classy` when `divey` is TRUE (or vice versa)** | Consistent, but an arbitrary precedence between two crowd opinions |
| 3 | **Set both to NULL** | Removes valid information about the 108 venues |

**Best Logical Solution:** **Option 1 — keep, flag, and resolve only in the derived label.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-045 — Food Drive-Thrus With `RestaurantsTakeOut = False` (Chain-Level Default)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-045` |
| **Severity** | MEDIUM (logical contradiction; systematic, chain-wide) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.3 Logical Contradiction Detection** (Cell 106): check `drive_thru ∧ NOT take_out (food businesses)` |
| **Also Confirmed In** | The unscoped version of the rule gave 536 hits, mostly pharmacy drive-thrus (e.g. CVS Pharmacy). Those are legitimate and were removed by restricting the rule to restaurant / coffee / fast-food categories. Independent SQL by business name confirms the chain concentration below |
| **Affected Columns** | `attributes.DriveThru` → `attr_drive_thru`; `attributes.RestaurantsTakeOut` → `attr_restaurants_take_out`; scope from `df_categories` |
| **Total Affected Rows** | **162** of **3,674** food businesses with a drive-thru (4.4%). **Jack in the Box (93)** and **Burger King (34)** make up 78% |
| **Invalid Value** | `Jack in the Box` / `Burger King` / `Chick-fil-A` / `Starbucks`: `DriveThru = True`, `RestaurantsTakeOut = False` |
| **Domain Rule** | Ordering at a food drive-thru *is* take-out, so `DriveThru = True` ⇒ `RestaurantsTakeOut ≠ False` for food businesses |

**Description:** The concentration in two chains points to a bulk / chain-level attribute load in which take-out was defaulted to FALSE, not to individual owner mistakes. Any "offers take-out" analysis under-counts these chains, and models learn a false negative correlation between the two flags.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Silver Enriched: derive `offers_take_out = TRUE` when `DriveThru = TRUE` for food categories; keep the raw `attr_restaurants_take_out`; add `takeout_inferred_flag`** | The implication is a business-logic certainty (drive-thru ⊆ take-out), so the inference is safe. The flag keeps it transparent |
| 2 | **Overwrite `RestaurantsTakeOut = TRUE` in Silver Cleaned** | Same result, but loses the record of what the source said |
| 3 | **NULL `RestaurantsTakeOut` for the 162** | Safe, but discards a value that can be inferred with certainty |
| 4 | **Do nothing** | Major chains appear as "no take-out" |

**Best Logical Solution:** **Option 1 — rule-based inference in the enriched layer with a flag.** This is one of the few contradictions where the correct value follows logically.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.6.3 (already documented, no new ID):** `vegan ∧ NOT vegetarian` (3 of 14) → **DQ-BUSINESS-020** (count confirmed across frames). `all music keys FALSE incl. no_music` (2,876 of 7,417) → **DQ-BUSINESS-041**.
>
> **Reviewed and not raised (plausible business reality, INFO):** `delivery ∧ NOT take_out` 1,586 (Walmart / CVS delivery, ghost kitchens), `smoking outdoor ∧ NOT outdoor_seating` 293 (patio without seating), `allages ∧ NOT good_for_kids` 5, `upscale ∧ $` 3, `divey ∧ $$$$` 3 (gentlemen's clubs), `name == address` 6 / `name == city` 11 (e.g. a business literally named after its street or town). Passed: 0 ratings without reviews, 0 `no_music = TRUE` with music, 0 appointment-only + 24 h.

---
#### 🚨 DQ-BUSINESS-046 — Address Block (city / state / ZIP) Contradicts the Coordinates

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-046` |
| **Severity** | HIGH (accuracy; the business is placed in a different state or metro than its address) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.4 Cross-Column Range Logic** (Cell 111): A) `(lat, lon) ∈ bbox(state)` (18) and B) `(lat, lon) ≤ 50 km from ZIP median` (4) |
| **Also Confirmed In** | 6.6.6 D) `metro(lat, lon) → state` (Cell 121): 15 in-footprint conflicts such as `Los Angeles CA 90041` and `Tampa FL 33609` with Philadelphia coordinates, or `Chattanooga TN` / `Salinas CA` with Tampa coordinates. Independent SQL with the same boxes and ZIP medians: **20** distinct businesses (18 ∪ 4, 2 in both). Most of them passed the metro-footprint check in 6.2.2 because the point does lie inside a Yelp metro. Only the far-away cases (e.g. `Broadway Veterinary Hospital`, 626 km from any metro) overlap DQ-BUSINESS-011 |
| **Affected Columns** | `latitude`, `longitude` vs `state`, `postal_code`, `city` (`df_business`) |
| **Total Affected Rows** | **20** businesses (18 of 150,325 outside the bounding box of their own state; 4 of 149,839 more than 50 km from the median point of their ZIP, up to 1,899 km) |
| **Invalid Value** | `Hellz Bellz · Los Angeles, CA 90041 · 39.661, -75.570` (Delaware); `Loft 17 · Tampa, FL 33609 · 40.042, -75.481` (Philadelphia suburbs, 1,492 km from its ZIP); `Intensity Fit Club · Tampa, CA 33613 · 28.076, -82.416` (correct Tampa point, wrong state) |
| **Domain Rule** | Coordinates, state and ZIP describe one location. The point must lie in the labelled state and near the other businesses with the same ZIP |

**Description:** Two patterns:

| Pattern | Example | Which field is wrong |
| --- | --- | --- |
| Coordinates in a Yelp metro, the whole address block from somewhere else | `Los Angeles CA 90041` / `Tampa FL 33609` at Philadelphia coordinates | Usually the **address** (Yelp's footprint is defined by the metros, so the geocode is the more reliable witness). Possibly a chain's HQ / default address copied into the listing |
| Address and coordinates agree, only `state` differs | `Tampa, CA 33613` at a Tampa point | **`state`** (overlaps DQ-BUSINESS-023) |

These rows put businesses into the wrong state for every state-level aggregate, and into the wrong metro for distance or "nearby" features.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag `geo_address_conflict`; adjudicate by majority of three witnesses (coordinates, ZIP prefix, city): correct `state` when ZIP + city + coordinates agree against it; otherwise keep values and set the flag** | Uses independent evidence, fixes only the field that is outvoted, and leaves unresolved cases visible |
| 2 | **Reverse-geocode the coordinates and overwrite city / state / ZIP** | Most complete fix, but needs an external service and assumes the geocode is always right (not true for DQ-BUSINESS-040 placeholder centroids) |
| 3 | **Quarantine the 20 businesses** | Clean, but removes reviewed businesses for an error that is often fixable |
| 4 | **Do nothing** | 20 businesses in the wrong state / metro |

**Best Logical Solution:** **Option 1 — flag, then correct only by a 2-of-3 witness rule.** It is deterministic, auditable and needs no external dependency. Reverse geocoding (Option 2) can be added later for the rows still unresolved.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-047 — False "Overnight" Windows: Inverted Start/End and 12-Hour-Clock Entries

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-047` |
| **Severity** | MEDIUM (wrong opening hours; 18–23 h windows computed for normal businesses) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.4 Cross-Column Range Logic** (Cell 111): C) `overnight end ≤ 06:00` |
| **Also Confirmed In** | Independent SQL split on `[-:]` over the raw bronze strings: the same 339 business-days. DQ-BUSINESS-007 treated every end < start range as a genuine overnight window that "can be computed". This check shows a subset that is not overnight at all |
| **Affected Columns** | `hours.<Day>` → `df_hours.hours_<day>`, `_start_time`, `_end_time` |
| **Total Affected Rows** | **339** of 58,501 overnight business-days (0.58%), at most **102 businesses** (70 + 31 + 1 by pattern) |
| **Invalid Value** | See breakdown below |
| **Domain Rule** | A range that wraps midnight belongs to a late-night business and ends in the early morning (≤ 06:00). An "overnight" range ending at 07:00–22:00 implies an 18–23 h opening |

| Pattern | Business-days | Businesses | Examples | Likely truth |
| --- | --- | --- | --- | --- |
| Start/end **swapped** (start ≥ 15:00) | 240 | 70 | `22:0-17:0`, `20:0-18:0`, `22:0-21:0`, `23:0-22:0` | `17:00-22:00`, `18:00-20:00` … |
| **12-hour clock** entry (both ≤ 12:00) | 98 | 31 | `9:0-7:0`, `11:0-10:0`, `10:0-9:0`, `11:30-8:0` | `09:00-19:00`, `11:00-22:00` … (PM end entered without +12) |
| Other | 1 | 1 | — | manual review |

**Description:** Naive modulo-24 h arithmetic (DQ-BUSINESS-007 Option 1) turns `22:0-17:0` into a 19-hour day and `9:0-7:0` into 22 hours. That inflates opening-hours features and "open now" / late-night coverage for up to 102 ordinary businesses. Because the pattern usually repeats on every day of the week for a business, it is a systematic entry error, not a real schedule.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag `hours_suspect_inverted` / `hours_suspect_12h`; in Silver Enriched compute duration and "open at" only from a corrected range (swap for pattern 1, end + 12 h for pattern 2) and keep the raw range** | Both corrections are deterministic and reversible. The flags let consumers exclude the rows instead |
| 2 | **Set duration to NULL for these rows** | Safe, but loses a correction that is almost certainly right |
| 3 | **Overwrite the raw string in Silver Cleaned** | Changes source values with no record of the original |
| 4 | **Treat as genuine overnight** (current DQ-007 rule) | Produces 18–23 h days for ordinary businesses |

**Best Logical Solution:** **Option 1 — flag and correct in the derived layer only.** Raw hours stay untouched, which keeps lineage and the EDA rule "never change values".

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-048 — Overnight Window Overlaps the Next Day's Opening

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-048` |
| **Severity** | LOW (small volume; double-counted open time) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.4 Cross-Column Range Logic** (Cell 111): D) `overnight end ≤ next day's start` |
| **Also Confirmed In** | Partly overlaps DQ-BUSINESS-047 (e.g. `Monday 20:0-17:0 → Tuesday 8:0-17:0` is an inverted window that also overlaps) |
| **Affected Columns** | `hours.<Day>` and `hours.<Day+1>` (Sunday wraps to Monday) → `df_hours` start / end columns |
| **Total Affected Rows** | **20** of 56,338 overnight business-days followed by a real opening the next day (0.04%) |
| **Invalid Value** | `Thursday 17:0-1:0 → Friday 0:0-3:0`; `Friday 15:0-2:0 → Saturday 1:30-2:0`; `Monday 20:0-17:0 → Tuesday 8:0-17:0` |
| **Domain Rule** | If a window on day *d* runs past midnight, it must end before (or exactly when) day *d+1* opens. Two windows cannot cover the same clock time |

**Description:** The overlaps come either from late-night windows also recorded as the next day's early-morning window (`0:0-3:0` after `17:0-1:0`), or from inverted ranges (DQ-BUSINESS-047). Weekly opening hours and "open now" counts are double-counted for those hours.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **In Silver Enriched build a weekly interval table (business_id, open_ts, close_ts on a 7-day clock) and merge overlapping intervals; flag `hours_overlap_flag`** | Interval merge is the standard, lossless way to represent schedules; raw values stay unchanged |
| 2 | **Truncate the earlier window at the next day's opening** | Simple, but assumes which of the two windows is right |
| 3 | **Do nothing** | 20 business-days double-count opening time |

**Best Logical Solution:** **Option 1 — merged weekly intervals plus a flag.** It also gives a clean base for all opening-hours analytics (DQ-007, 012, 025, 047).

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Reviewed and not raised in 6.6.4 (INFO):** 778 same-day windows longer than 20 h are almost all `0:0-23:59` / `0:0-23:0`, which are alternative all-day spellings already in **DQ-BUSINESS-025**. 307 businesses with ≥ 100 reviews and an extreme rating (1.0 / 5.0) are statistically possible (a 5.0 needs a ≥ 4.75 average). The bucket table shows the 5.0 share falls from 18.4% (< 10 reviews) to 0.7% (500+), which is the expected small-sample effect, not a data error.

---
#### 🚨 DQ-BUSINESS-049 — `BestNights` Recommends Days on Which the Business Is Closed

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-049` |
| **Severity** | MEDIUM (cross-frame dependency violation between `df_best_nights` and `df_hours`) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.5 Dependency Rule Validation** (Cell 115): B) `BestNights.<day> TRUE ⇒ open that day` (only businesses that publish hours) |
| **Also Confirmed In** | Independent SQL on the raw strings: 235 (business, day) pairs. Monday 58, Tuesday 54, Sunday 40, Saturday 30, Wednesday 23, Thursday 18, Friday 12 |
| **Affected Columns** | `attributes.BestNights` → `df_best_nights` (`key` = weekday, `value` = TRUE) vs `hours.<Day>` → `df_hours.hours_<day>` (NULL) |
| **Total Affected Rows** | **208** of 5,387 businesses with both BestNights and hours (3.9%), 235 business-days |
| **Invalid Value** | `Visual Arts Collective`: only Saturday hours, BestNights `friday = True`; `21st Amendment at La Louisiane`: open Thu–Sat, BestNights `tuesday = True` |
| **Domain Rule** | A "best night" must be a night the venue is open: `BestNights[d] = TRUE ⇒ hours[d] IS NOT NULL` |

**Description:** Two snapshots taken at different times disagree. BestNights is a crowd vote that accumulates over years, while hours is the current schedule. The Mon/Tue/Sun skew matches typical closing days, so the most likely cause is that the venue stopped opening on those days after the votes were cast. A second possible cause is that a NULL day in `hours` means "not provided" rather than "closed" (DQ-BUSINESS-033).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep both; in Silver Enriched derive `best_night_open_<day> = best_night AND hours_<day> IS NOT NULL` and add `bestnight_hours_conflict_flag`** | The current schedule is the operational truth for "go tonight" use cases, while the raw vote is preserved for history |
| 2 | **Set the conflicting BestNights keys to FALSE** | Asserts the crowd was wrong; loses the historical signal |
| 3 | **Treat the NULL hours day as open** | Contradicts DQ-BUSINESS-033 (NULL day = closed for 55,837 business-days) |

**Best Logical Solution:** **Option 1 — derived, schedule-aware flag plus conflict flag.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-050 — `GoodForMeal` Flags That the Opening Hours Make Impossible

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-050` |
| **Severity** | MEDIUM (cross-frame dependency violation between `df_good_for_meal` and `df_hours`) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.5 Dependency Rule Validation** (Cell 115): B) four meal ⇒ schedule rules |
| **Also Confirmed In** | The breakfast rule was first tested with a 10:00 cut-off and loosened to 10:30 so that 10:30 openers (e.g. QDOBA) are not counted. All counts below use the tolerant rule |
| **Affected Columns** | `attributes.GoodForMeal` → `df_good_for_meal` (`breakfast`, `lunch`, `dinner`, `latenight`) vs `df_hours` start / end times |
| **Total Affected Rows** | **762** rule violations among businesses that publish hours (see table) |
| **Invalid Value** | See table |
| **Domain Rule** | A meal flag must fall inside at least one opening window of the week |

| Rule | Violations | Of | Example |
| --- | --- | --- | --- |
| `lunch = TRUE` ⇒ open at 12:30 on some day | **553** | 13,627 (4.1%) | `Sal DeForte's Ristorante` opens 16:00 every day |
| `dinner = TRUE` ⇒ open at 19:00 on some day | **116** | 12,678 (0.9%) | `Corner Bakery` 6:30–15:00; `Sweet Soulfood` 11:00–17:00 |
| `latenight = TRUE` ⇒ closes ≥ 22:00 / overnight / all-day | **58** | 1,289 (4.5%) | `Khyber Pass Pub` 12:00–21:00; `White Castle` `5:0-5:0` (see DQ-007) |
| `breakfast = TRUE` ⇒ opens ≤ 10:30 on some day | **35** | 2,904 (1.2%) | `Roundabout Grill` opens 16:00–17:00 |

**Description:** Like DQ-BUSINESS-049, GoodForMeal is a long-run crowd opinion, while hours is the current schedule. A restaurant that stops serving lunch keeps its old `lunch = TRUE` tag. Some hits are caused by other hours errors (`5:0-5:0` same-time ranges, DQ-BUSINESS-007; inverted ranges, DQ-BUSINESS-047), so fixing those first reduces this count.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the raw meal flags; in Silver Enriched derive `serves_<meal>_now = good_for_<meal> AND open during the meal window`, and add `meal_hours_conflict_flag`; evaluate after the DQ-047 / DQ-007 hour fixes** | Separates crowd opinion from current service, so meal-time recommendations use the current schedule; raw values stay intact |
| 2 | **Set the conflicting meal flags to FALSE** | Probably right for most lunch cases, but assumes the hours are always correct (not true for DQ-047 rows) |
| 3 | **Do nothing** | 553 restaurants are recommended for lunch while closed at lunch time |

**Best Logical Solution:** **Option 1 — schedule-aware derived flags, evaluated after the hours corrections.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-051 — `HappyHour = True` at Businesses With `Alcohol = 'none'`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-051` |
| **Severity** | LOW–MEDIUM (dependency violation; partly explained by BYOB) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.5 Dependency Rule Validation** (Cell 115): C) `HappyHour TRUE ⇒ Alcohol ≠ 'none'` |
| **Also Confirmed In** | Independent SQL on bronze (both quoting styles of `'none'`, DQ-BUSINESS-003): **438**. Split by BYOB: NULL 314, True 61, False 62, `'None'` literal 1 |
| **Affected Columns** | `attributes.HappyHour` → `attr_happy_hour`; `attributes.Alcohol` → `attr_alcohol` (`'none'` is a valid category, DQ-BUSINESS-002); `attr_byob` |
| **Total Affected Rows** | **438** of 9,721 businesses with a happy hour (4.5%) |
| **Invalid Value** | `HappyHour = True`, `Alcohol = 'none'` |
| **Domain Rule** | A happy hour is a discounted-drinks period, so it implies alcohol is served: `HappyHour = TRUE ⇒ Alcohol ∈ {beer_and_wine, full_bar}` |

**Description:** Only the 61 BYOB = True rows have a plausible explanation (food specials at an unlicensed BYOB place). The other 377 contradict each other: either the venue obtained a licence and `Alcohol` is stale, or `HappyHour` is used for food / coffee discounts. Any "bars with happy hour" or alcohol-licence analysis is affected.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep both raw values; add `happyhour_alcohol_conflict_flag` for `Alcohol = 'none'` AND `BYOB IS NOT TRUE` (377); treat the 61 BYOB rows as valid** | Separates the explainable case from the real conflicts, and no value is guessed |
| 2 | **Set `HappyHour = FALSE` when `Alcohol = 'none'`** | Wrong for venues whose `Alcohol` answer is out of date |
| 3 | **Upgrade `Alcohol` to `beer_and_wine`** | Invents a licence level |

**Best Logical Solution:** **Option 1 — BYOB-aware conflict flag.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-052 — Ambiguous Meaning of `BYOB`: Corkage Fee With `BYOB = False`

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-052` |
| **Severity** | MEDIUM (semantic ambiguity in the attribute definition; 56% of corkage businesses affected) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.5 Dependency Rule Validation** (Cell 115): C) `Corkage TRUE ⇒ BYOB ≠ FALSE` |
| **Also Confirmed In** | Independent SQL on bronze: 612, split by Alcohol: `full_bar` 457, `beer_and_wine` 138, `none` 16, NULL 1. For comparison, `Corkage = True` + `BYOB = True`: 272. This rule was **not** part of DQ-BUSINESS-024 (which only tested rules anchored on `BYOBCorkage`) |
| **Affected Columns** | `attributes.BYOB` → `attr_byob`; `attributes.Corkage` → `attr_corkage`; `attr_alcohol` |
| **Total Affected Rows** | **612** of 1,096 businesses with `Corkage = True` (55.8%) |
| **Invalid Value** | `Top Shelf Sports Lounge`: full_bar, `BYOB = False`, `Corkage = True` |
| **Domain Rule** | A corkage fee is charged on bottles brought in by guests, so outside bottles must be allowed |

**Description:** This is mainly a **definition** problem, not 612 typos. In 595 of the 612 cases (97%), the venue is licensed (`full_bar` / `beer_and_wine`). Licensed restaurants often accept a guest's wine for a fee while not being a "BYOB restaurant". So in practice `BYOB = False` means "not a BYOB-concept (unlicensed) venue", **not** "outside bottles forbidden". Only the **16** unlicensed rows (`Alcohol = 'none'`, BYOB = False, Corkage = True) are true contradictions. This also changes how DQ-BUSINESS-024 should be read: its Option 1 precedence rule assumes BYOB means "outside bottles allowed".

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Document in the DDL / data dictionary that `attr_byob` = "BYOB-concept venue"; in the DQ-024 `byob_policy` derivation treat `Corkage = TRUE` as "outside bottles allowed with fee" regardless of BYOB; flag only the 16 unlicensed contradictions** | Matches observed behaviour in 97% of cases, prevents a false 612-row error count, and keeps a single consistent policy field |
| 2 | **Treat all 612 as conflicts and NULL BYOB** | Destroys valid information for licensed restaurants |
| 3 | **Overwrite `BYOB = TRUE` when Corkage = TRUE** | Mislabels full-bar restaurants as BYOB venues |

**Best Logical Solution:** **Option 1 — fix the semantics in the data dictionary and in the `byob_policy` derivation, and flag only the 16 real contradictions.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-053 — Attributes Answered Outside Their Business-Type Scope

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-053` |
| **Severity** | LOW (INFO-level: misapplied attributes or incomplete category lists) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.5 Dependency Rule Validation** (Cell 115): A) attribute scope ⇒ business type (9 INFO checks) |
| **Also Confirmed In** | Related to DQ-BUSINESS-034 (structural "not applicable" NULLs): there the attributes are missing where they don't apply; here they are *present* where they don't apply |
| **Affected Columns** | `attr_restaurants_table_service`, `attr_restaurants_attire`, `attr_restaurants_reservations`, `attr_alcohol`, `GoodForMeal`, `attr_accepts_insurance`, `BestNights`, `attr_good_for_dancing` / `attr_coat_check` vs `df_categories` |
| **Total Affected Rows** | **743** rule hits: dancing / coat check 333, reservations 237, table service 119, attire 25, alcohol 24, GoodForMeal 2, AcceptsInsurance 2, BestNights 1; HairSpecializesIn 0 |
| **Invalid Value** | `Target` / `Petco` with `GoodForDancing` / `CoatCheck` and `RestaurantsReservations`; `U.S. Bank Branch` with `Alcohol`; `The Children's Museum of Indianapolis` with `RestaurantsAttire` |
| **Domain Rule** | Yelp shows each attribute question only to the business types it applies to, so an answered attribute implies a matching category |

**Description:** Two causes. (a) Chain-level bulk attribute templates applied to every unit (Target, Petco, European Wax Center), which fill restaurant / nightlife questions with (usually FALSE) answers. (b) Real but uncategorised services, e.g. a stadium or museum with an on-site restaurant (`Citizens Bank Park` with attire). Case (a) adds a misleading FALSE where the honest value is "not applicable", and it biases attribute prevalence statistics (e.g. share of venues without coat check).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw values; add a category-scope map in Silver Enriched and expose scoped attributes as NULL ("not applicable") outside their scope, with `attr_out_of_scope_flag`** | Not-applicable is semantically different from FALSE; the source stays auditable; prevalence metrics use the right denominator |
| 2 | **Delete out-of-scope answers in Silver Cleaned** | Same analytical effect, but loses lineage and the cases where the business genuinely offers the service |
| 3 | **Do nothing** | Template FALSEs distort attribute rates for restaurants and nightlife |

**Best Logical Solution:** **Option 1 — scope-aware enriched view with a flag.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.6.5 (already documented, no new ID):** `Open24Hours TRUE ⇒ every listed day all-day` 15 of 19 → **DQ-BUSINESS-026** (count identical). BYOB trio: `BYOBCorkage yes_* ⇒ BYOB ≠ FALSE` 49, `yes_free ⇒ Corkage ≠ TRUE` 11, `no ⇒ BYOB ≠ TRUE` 10 → **DQ-BUSINESS-024** (counts identical to Cell 60). `is_open TRUE ⇒ published schedule` (INFO) → **DQ-BUSINESS-032 / 033**.
>
> **Passed:** 0 reviewed businesses without a rating; 0 seven-day all-day schedules with `Open24Hours = FALSE`; 0 HairSpecializesIn outside hair / beauty categories.

---
#### 🚨 DQ-BUSINESS-054 — One ZIP Code Mapped to Several City Names (Township / Neighbourhood / Mall Names Used as `city`)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-054` |
| **Severity** | LOW (standardization; breaks city-level grouping, not location accuracy) |
| **Detected In** | Phase 6.6 — Cross-Column Consistency Checks → **6.6.6 Category Mapping Consistency** (Cell 121): B) `postal_code (ZIP5) → city (folded)` |
| **Also Confirmed In** | The city key is already folded for case, punctuation and Saint/St, Mount/Mt (DQ-BUSINESS-021), so these are different *names*, not spelling variants. Related to DQ-BUSINESS-018 (polluted city values) |
| **Affected Columns** | `city` vs `postal_code` (`df_business`) |
| **Total Affected Rows** | **8,385** of 144,716 US businesses (5.8%) are filed under a non-majority city name for their ZIP5 |
| **Invalid Value** | `08002 → Cherry Hill (445) / Cherry Hill Mall (1) / W Cherry Hill (1)`; `08012 → Blackwood (137) / Turnersville (125) / Gloucester Township (18) / Washington Township (5) / Hilltop (1) / Gloucester (1)`; `08009 → Berlin (92) / Berlin Boro / West Berlin / Winslow` |
| **Domain Rule** | A city dimension should hold one canonical place name per locality. A ZIP has one USPS default city, even if other names are "acceptable" |

**Description:** Many of these are legitimate USPS "acceptable" names (Blackwood vs Turnersville, both valid for 08012), so not every row is wrong. But the dimension mixes three levels: postal city, municipality / township (`Gloucester Township`) and points of interest (`Cherry Hill Mall`). City-level counts and rankings therefore split one locality into several groups.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep raw `city`; add `city_canonical` = the USPS default city for the ZIP (or the modal city per ZIP within the dataset when no reference is available), plus `city_is_alias`** | Gives one consistent grouping key and preserves the owner-entered locality |
| 2 | **Overwrite `city` with the ZIP's modal city** | Consistent, but removes valid locality detail (Turnersville is a real place) |
| 3 | **Do nothing** | City-level aggregates stay fragmented |

**Best Logical Solution:** **Option 1 — add a canonical city from a USPS ZIP reference table (fall back to the modal city).**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **Cross-references from 6.6.6 (already documented, no new ID):** `postal_code (ZIP5) → state`: 31 minority-state rows in 29 ZIPs (e.g. `08054 → NJ 411 / NV 1`, `19014 → PA 138 / DE 1`). Independent SQL shows **all 31** are among the 71 state ↔ ZIP-prefix conflicts of **DQ-BUSINESS-023**. `metro(lat, lon) → state`: 35 conflicts = 20 off-footprint state codes (CO, MT, WA, MA, NC, …) → **DQ-BUSINESS-010**, plus 15 in-footprint ones (`Eagle CA 83616` in Boise, `Mount Laurel NV 08054` in Philadelphia, `New Orleans FL 70122`) → **DQ-BUSINESS-023** / **DQ-BUSINESS-046**. `point → some metro` (2 > 150 km) → **DQ-BUSINESS-011**.
>
> **Passed:** 0 Canadian FSA ↔ province mismatches (5,555); every enum raw literal decodes to exactly one value and spellings differ only by the `u'…'` prefix (183,307 values); all nested-dict keys belong to the contract key map; BestNights keys ↔ hours days match 1:1.

---

> **6.6 Register note:** Phase 6.6 raised DQ-BUSINESS-042 … 054 (13 new IDs). Re-detected existing errors (counts updated, no new ID): DQ-BUSINESS-007 (1,492 same-time ranges, 89% at 5:00 / 6:00 / 9:00 → likely 24 h, Cell 104), DQ-BUSINESS-020 (vegan without vegetarian, 3), DQ-BUSINESS-023 (ZIP → state, 31 ⊆ 71), DQ-BUSINESS-024 (49 / 11 / 10, identical), DQ-BUSINESS-026 (Open24Hours vs schedule, 15), DQ-BUSINESS-041 (no_music derivation fails on 2,876 dicts), DQ-BUSINESS-010 / 011 (off-footprint states and remote points in the metro mapping). All 81 logical-relationship checks of 6.6.1 (Cell 103) pass, so the TE.1 / TE.2 restructuring is proven lossless row by row. The full consolidated register is in Section 4.

---
#### 🚨 DQ-BUSINESS-055 — `review_count` / `stars` Count Only Each User's Current Review (Undocumented Aggregate Definition)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-055` |
| **Severity** | MEDIUM (semantic: a naive re-derivation in Silver / Gold silently changes 45% of counters and 5.6% of ratings) |
| **Detected In** | Phase 6.7 — Referential Integrity → **6.7.3 Cross-Table Aggregate Reconciliation**: A) `review_count = COUNT(*)`, B) `stars = round½(AVG all reviews)` |
| **Also Confirmed In** | Independent SQL over `bronze_final.yelp_review`. `review_count = COUNT(DISTINCT user_id)` for 150,037 / 150,346 businesses (99.79%). `stars = half-up round½(AVG of each user's latest review)` for 150,327 (99.99%) |
| **Affected Columns** | `yelp_business.review_count`, `yelp_business.stars` ↔ `yelp_review (business_id, user_id, stars, date)` |
| **Total Affected Rows** | **244,520** superseded review rows (same user, same business, not the latest) in **67,301** businesses. `review_count ≠ COUNT(*)` for **67,362** businesses. `stars ≠ round½(AVG all)` for **8,456** businesses |
| **Invalid Value** | `T-Mobile (mGFsPRHdE6SMV1gYQ6GzgQ)`: stored 2.0, mean of current reviews 2.167 → 2.0 ✔, mean of all reviews 2.429 → 2.5 ✘. Max per business: 289 more review rows than the stored counter |
| **Domain Rule** | On Yelp, a user's newer review of a business **replaces** the older one in the business's count and rating. Older reviews stay visible as "previous reviews" but do not count. The review extract includes these superseded rows |

**Description:** The stored counters are correct. The definition behind them is not written down anywhere. A Silver / Gold job that recomputes `review_count` with `COUNT(*)` or `stars` with `AVG(stars)` over `yelp_review` will disagree with Yelp for tens of thousands of businesses. Each reconciliation run will then raise false alarms. Review-level analytics (sentiment, rating trends) also double-count users who reviewed the same business more than once.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add `is_current_review` to Silver `yelp_review` (latest review per `(business_id, user_id)`, tie-break on `review_id`). Derive every business aggregate from current reviews only, and document the definition in the DDL column comments of `review_count` / `stars`** | Reproduces Yelp's numbers exactly and keeps the history |
| 2 | **Delete superseded reviews** | Matches the counters, but destroys rating-history data (244k rows) |
| 3 | **Keep the stored values and never recompute** | Correct today, but drifts the moment new reviews land (see DQ-BUSINESS-056) |

**Best Logical Solution:** **Option 1 — flag current reviews and derive aggregates from them, using the documented formula `stars = floor(2·AVG + 0.5) / 2`.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-056 — `review_count` / `stars` Taken at a Different Moment Than the Review Extract (Snapshot Drift)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-056` |
| **Severity** | LOW (309 businesses, ± 1–2 reviews, max half a star) |
| **Detected In** | Phase 6.7 — Referential Integrity → **6.7.3 Cross-Table Aggregate Reconciliation**: `review_count = COUNT(DISTINCT user_id)`, `stars = round½(AVG current)`, `\|stars − AVG current\| ≤ 0.25` |
| **Also Confirmed In** | Independent SQL: 279 businesses have stored < computed and 30 have stored > computed, with a maximum gap of 2 |
| **Affected Columns** | `yelp_business.review_count`, `yelp_business.stars` |
| **Total Affected Rows** | **309** businesses with a counter mismatch (276 at −1, 3 at −2, 30 at +1). **19** of them also have a rating mismatch, 16 of which fall outside the ± 0.25 rounding bound |
| **Invalid Value** | `Roy's Appliance Service`: stored 5, distinct reviewers 6. `Mr.Smoke`: stored 2.5, mean of current reviews 3.0 |
| **Domain Rule** | A derived counter has to reflect the same snapshot as the fact table it summarises |

**Description:** The residual is a timing effect, not random corruption. In 59.5% of the mismatched businesses the last review is ≤ 30 days before the snapshot (2022-01-19), against 17.2% of businesses that match. The median gap is 13 days vs 236 days. Every one of the 19 rating mismatches is inside the 309 counter mismatches. So the business file was frozen slightly before (stored < computed) or after (stored > computed: reviews later removed) the review file.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Recompute `review_count` / `stars` in Silver Enriched from current reviews (DQ-BUSINESS-055). Keep the Yelp values as `review_count_source` / `stars_source` with a `aggregates_in_sync` flag** | The aggregates become consistent with the fact table, and the source value stays auditable |
| 2 | **Overwrite the stored values with the recomputed ones** | Consistent, but loses the source value |
| 3 | **Do nothing** | 309 businesses disagree between the dimension and the fact table |

**Best Logical Solution:** **Option 1 — recompute from current reviews and keep the source values with an in-sync flag.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **6.7 Register note:** Phase 6.7 raised DQ-BUSINESS-055 … 056.
>
> **Clean results (PASS):** `business_id` is unique, NOT NULL, a well-formed 22-char id and case-insensitive unique. All 10 TE.2 child frames have 0 orphan / malformed FKs and 0 PK duplicates, and the 1:1 frames hold exactly one row per business. Review (6,990,280), tip (908,915) and check-in (131,930) rows have 0 orphan, NULL or malformed `business_id`. Every business has reviews, and check-in is 0:1. All 7 enum attributes conform to `ENUM_DOMAINS`. The category vocabulary (1,311 labels) has no blank / padded labels and no case / space variants. Tip / check-in coverage shows no state-level gap.
>
> **Re-detected existing errors (no new ID):** DQ-BUSINESS-005 (43 duplicate `(business_id, category_name)` pairs break the Silver category PK), DQ-BUSINESS-009 (`XMS`) / -010 (21 off-footprint states) in the state lookup, DQ-BUSINESS-013 (10 delimiter-fragment labels, 9,371 rows in 4,150 businesses, would become fake category-dimension members, including `& Probates`), DQ-BUSINESS-036 (28,038 open businesses with no review / tip / check-in in the final year, plus 130 closed businesses active in the final 30 days).
>
> **INFO (expected, documented):** businesses without tips (44,153) or check-ins (18,416) are low-activity listings (median 7–9 reviews vs 23). Dict-frame coverage ranges from 0.02% (DietaryRestrictions) to 59% (BusinessParking), which is structural not-applicable (DQ-BUSINESS-034 / 053). 33 reviews in 32 businesses come from users missing from `yelp_user` and are owned by the review / user EDA.

---
#### 🚨 DQ-BUSINESS-057 — Yelp Event Pages Listed as Businesses

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-057` |
| **Severity** | MEDIUM (few rows, but they are not businesses, and 79% are marked open) |
| **Detected In** | Phase 6.8 — Business Rule & Accuracy → **6.8.1 A** — `listing is a Yelp event, not a business`, `Yelp event flagged is_open = TRUE` |
| **Also Confirmed In** | 6.8.5 C: compressed lifecycles and single-day review bursts (`Elites Go Free: Jurassic Quest`: 12 reviews in 4 days; `Elite Experience: Raising Cane's Holiday Soiree`: 15 of 25 reviews on one day) |
| **Affected Columns** | `yelp_business.categories` (`Yelp Events`, always with `Local Flavor`), `name`, `is_open` |
| **Total Affected Rows** | **48** event listings (1,668 reviews). **38** of them have `is_open = TRUE`, 23 have no hours and 6 have no address |
| **Invalid Value** | `Elite Event: Elite Week at Pig Shack Eatery` (Reno, open, 57 reviews), `Yelp's Totally Bazaar: Freakshakes at TeeJay's Sweet Tooth` (Indianapolis, open, 93 reviews) |
| **Domain Rule** | The business dimension holds places that operate over time. A one-off community / Elite event is a different entity type |

**Description:** Yelp hosts Elite and community events as pages that look like business listings. Their "reviews" are attendee feedback for one evening, so their rating, review counts and lifecycle (span of days, single-day bursts) are not comparable to a real business. Because events are never formally "closed", 38 of 48 still say `is_open = TRUE`. They inflate open-business counts and add noise to popularity and rating models.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add `is_yelp_event = array_contains(categories, 'Yelp Events')` in Silver. Keep the rows, but exclude them from business-level aggregates, open-business KPIs and model training by default** | Data stays complete and auditable; consumers get a clean business population |
| 2 | **Route the rows to a separate `yelp_event` table** | Cleaner model, but needs a new entity and FK handling for their reviews / tips |
| 3 | **Delete the rows** | Loses 1,668 reviews and breaks review → business FKs |

**Best Logical Solution:** **Option 1 — flag with `is_yelp_event` and exclude by default.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-058 — `name` Carries Operational Status or a Copy of the Address / City

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-058` |
| **Severity** | LOW (25 businesses) |
| **Detected In** | Phase 6.8 — Business Rule & Accuracy → **6.8.1 A** — `name carries operational status`, `name = address or city` |
| **Also Confirmed In** | Overlaps DQ-BUSINESS-019 / 032 / 040 (empty or generic addresses) for the name = location rows |
| **Affected Columns** | `yelp_business.name` |
| **Total Affected Rows** | **8** names with status text (`- Closed`, `- CLOSED`, `moved`, `coming soon`; all 7 "closed" names are correctly `is_open = FALSE`). **17** names equal to their address or city |
| **Invalid Value** | `Reginelli's Pizzeria - Closed`, `MedExpress Urgent Care - CLOSED`, name `Philadelphia` with empty address in city `Philadelphia`, name `King of Prussia` |
| **Domain Rule** | `name` holds the trading name only. Status belongs in `is_open`, location in `address` / `city` |

**Description:** Two kinds of contamination of the identity field. (a) Status suffixes added by editors when a place closed: the flag already holds the information, but the suffix breaks name matching, chain grouping (DQ-022) and dedup (DQ-038 / 039). (b) Place names used as the business name: most have empty or generic addresses (overlap with DQ-019 / 032 / 040) and look like placeholder listings. A few are real venues named after their location (e.g. `Blackett's Ridge`).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **In Silver add `name_clean` with the status suffix removed (regex on `- closed` / `(closed)` / `moved` / `coming soon`), and a flag `name_is_location`. Keep the original `name`** | Matching and chain logic use `name_clean`; the source value stays auditable |
| 2 | **Overwrite `name`** | Simpler, but loses the editor's signal |
| 3 | **Do nothing** | Duplicate / chain detection misses these rows |

**Best Logical Solution:** **Option 1 — `name_clean` + `name_is_location` flag.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-059 — `RestaurantsPriceRange2` Contradicts the Business Type

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-059` |
| **Severity** | LOW (32 businesses) |
| **Detected In** | Phase 6.8 — Business Rule & Accuracy → **6.8.1 B** — `$$$$ on a budget format`, `upscale ambience at price $` |
| **Also Confirmed In** | Related to DQ-BUSINESS-044 INFO checks (`upscale ∧ $` 3, `divey ∧ $$$$` 3) in 6.6.3 |
| **Affected Columns** | `attr_restaurants_price_range_2`, `categories`, `Ambience.upscale` |
| **Total Affected Rows** | **29** of 8,892 budget-format businesses (Fast Food, Food Trucks, Hot Dogs, Food Stands, Convenience Stores, Street Vendors) are `$$$$`. **3** of 428 upscale venues are `$` |
| **Invalid Value** | `CVS Pharmacy` (Spring Hill) and `Wawa` (Palmyra) at price level 4. `Savaya Coffee Market` upscale at price level 1 |
| **Domain Rule** | Price level is consistent with the format: budget formats are `$` / `$$`, upscale venues are at least `$$` |

**Description:** `RestaurantsPriceRange2` is crowd-sourced and the 1–4 scale is easy to invert (1 = cheap). The `$$$$` budget formats are mostly chain units (CVS, Wawa, convenience stores) where luxury pricing is not credible, which points to inverted or misclicked answers. Price level is a key feature for recommendation and segmentation, so a few inverted values on high-volume chains have a large impact.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the value and add a `price_range_suspect` flag in Silver (budget format × 4, or upscale × 1). Optionally impute the chain's modal price level into `price_range_imputed`** | Contradictions are visible; models can use the flag or the imputed value |
| 2 | **Set suspect values to NULL** | Removes noise, but loses data that may be correct (e.g. a premium food truck) |
| 3 | **Do nothing** | Inverted values bias price-level features |

**Best Logical Solution:** **Option 1 — flag, with optional chain-mode imputation.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **6.8 Register note:** Phase 6.8 raised DQ-BUSINESS-057 … 059.
>
> **Clean results (PASS):** no placeholder or phone-number names, no restaurant sub-category without `Restaurants` (hierarchy complete), no week longer than 168 h, no closed-name listing marked open. All review (6.99 M), tip (909 K) and check-in (13.36 M) timestamps parse and fall inside [2004-10-01, snapshot]. No state deviates by more than 10 pp from the global open rate (79.6%).
>
> **Re-detected existing errors (no new ID):** DQ-BUSINESS-036 (stale `is_open`: 5,542 open businesses silent > 3 years, 130 closed businesses active in the last 30 days), DQ-BUSINESS-021 / 018 (868 of 1,467 `(city, state)` values hold < 5 businesses, mostly suburbs and spelling variants), DQ-BUSINESS-022 (chain name variants affect brand grouping), DQ-BUSINESS-007 / 026 (168 h/week schedules on non-24h types, e.g. a law office, are consistent with the ambiguous `0:0-0:0` encoding), DQ-BUSINESS-055 (stored `review_count` used for concentration).
>
> **INFO (expected, documented — analytics caveats for Silver / Gold):**
> * **Small-sample ratings:** 24.8% of businesses with 5–9 reviews have an extreme rating (≤ 1.5 or 5.0), vs 1.6% at 200+ reviews (× 15.5). 279 perfect-5.0 and 116 ≤ 1.5-star businesses still have ≥ 100 reviews (the low ones are banks, fast food, insurers). Use a Bayesian / shrunk rating for ranking.
> * **Inclusion bias:** `review_count` is left-truncated at 5 (mode 5 = 9.9% of businesses), so averages are not representative of all Yelp listings. Recent entry cohorts are right-censored (2021: 2,220, 2022: 19), and their open rate rises to 98% (survivorship, cohort ↔ open % correlation + 0.96). The 2010 cohort jump (+ 119%) coincides with check-ins starting on 2009-12-30, so `first_activity` mixes sources.
> * **Concentration:** reviews Gini 0.678 (top 1% of businesses = 19.6% of reviews), check-ins 0.814, tips 0.764. LA is the most concentrated state (Gini 0.753, French Quarter landmarks). Brands with ≥ 10 units hold 16.0% of listings but only 10.0% of reviews. Top 10 cities hold 48.7% of businesses. Stratify / weight samples by city and brand.
> * **Outliers (extreme, not invalid):** 32 review-count outliers on the log scale (Acme Oyster House 7,568), 909 businesses far above their city median, 232 chain units ≥ 2 stars off their chain median (163 of them with only 5–9 reviews), 38 opening minutes off the quarter-hour grid, 236 categories used by < 10 businesses.
> * **Lifecycle shapes:** 8 businesses with ≥ 10 reviews inside ≤ 7 days and single-day bursts (mostly events / pop-ups, see DQ-057), businesses reactivated after a ≥ 3-year silence (possible new owner under the same listing), newcomers with ≥ 5 reviews within 90 days.

---
#### 🚨 DQ-BUSINESS-060 — Tip Feed Halves in the Final Weeks Before the Cut-Off (Source Regime Shift)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-060` |
| **Severity** | MEDIUM (timeliness / boundary bias on tip-based business features) |
| **Detected In** | Phase 6.9 — Timeliness → **6.9.3 Snapshot Lag & Currency**, section G (source regime shift) |
| **Also Confirmed In** | Independent weekly tip / review ratio query (same step change); 6.9.3 E monthly table: Dec-2021 tips 1,777 vs Nov-2021 2,785 (−36 %) while reviews rose |
| **Affected Columns** | business-level tip features derived from `yelp_tip.date` (`last_tip`, tips per period, tip recency) |
| **Total Affected Rows** | Tips per 100 reviews fall from **5.75** (18 weeks before) to **3.60** (final 8 weeks), ×0.63; same windows one year earlier ×0.97 → seasonality-adjusted ×0.65. Ratio stays < 70 % of baseline from the week of **2021-12-06** to the cut-off (2022-01-19). Check-ins stay stable (×0.99 adjusted) |
| **Invalid Value** | week 2022-01-10: 346 tips vs 11,725 reviews (2.95 per 100), baseline ≈ 6 per 100 |
| **Domain Rule** | All sources of one snapshot should be complete up to the same cut-off; a feed-specific step change right before the cut-off indicates a collection / product change, not customer behaviour |

**Description:** Reviews and check-ins keep their usual level until 2022-01-19, but tips drop to about half of their normal rate relative to reviews from early December 2021. The drop is a step, not a slow trend, and it does not appear in the same weeks of the previous year, so it is not seasonal. Most likely the tip feed was captured with a lag (late-arriving tips not yet in the extract) or Yelp changed how tips were collected. For the business table this means tip-based recency ("tips in the last 90 days", `last_tip`) is biased low for every business in the final weeks, and a trend chart would show a false collapse of tip engagement.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep all tips; set a tip-specific effective cut-off (`tip_complete_until = 2021-11-30`) in Silver TBLPROPERTIES and exclude later weeks from tip trends / recency features (or flag them `is_partial_period`)** | Source data untouched; analyses use only the complete window |
| 2 | **Re-scale late-period tip counts by the review ratio** | Invents volume; hides the issue |
| 3 | **Drop tips after 2021-11-30** | Loses valid rows; not needed if the cut-off is documented |

**Best Logical Solution:** **Option 1 — document a tip-specific completeness date and apply it in recency features and trends.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-061 — Alberta (Edmonton) Market Coverage Decay: Business State Systematically Older

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-061` |
| **Severity** | MEDIUM (timeliness differs by market → biased cross-market comparisons) |
| **Detected In** | Phase 6.9 — Timeliness → **6.9.3 Snapshot Lag & Currency**, section A (metro activity freshness) |
| **Also Confirmed In** | Independent SQL on `yelp_review ⋈ yelp_business` (2019 = 10,302, 2021 = 3,895 reviews, ratio 0.38; PA 0.57, FL 0.79) |
| **Affected Columns** | `is_open`, `stars`, `review_count`, `hours`, `attributes` of `state = 'AB'` businesses |
| **Total Affected Rows** | **5,573** AB businesses. Only **18.5 %** had activity in the last 90 days (metro median 37.9 %); median silence **592 days** (other metros 135–237 d); 2021 / 2019 review volume **0.38** (metro median 0.69). The market still has events up to the cut-off (no truncated extract) |
| **Invalid Value** | `state = 'AB'`, `pct_active_90d = 18.5`, `reviews_2021_vs_2019 = 0.38` |
| **Domain Rule** | Markets of one snapshot should be observed with comparable freshness before comparing their current-state fields |

**Description:** Alberta is the only Canadian metro and its coverage has thinned much faster than all US metros. The extract was not cut early (the last AB event is on 2022-01-19), but far fewer AB businesses are still observed. Their `is_open`, hours and attributes therefore reflect an older state than in other markets, while the open rate (78 %) looks normal — which suggests closures are flagged less in AB. Comparing open rates, ratings or amenity shares between AB and US states mixes different snapshot ages.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep the rows; add market-level freshness metrics (`pct_active_90d`, `median_days_silent`) to a Silver / Gold market dimension and use the business-level `days_since_last_activity` (DQ-BUSINESS-036) as a filter in cross-market analysis** | Transparent; no values changed |
| 2 | **Exclude AB from cross-market comparisons** | Simple, but loses a whole metro |
| 3 | **Re-weight AB metrics** | Model-dependent; hard to justify in Silver |

**Best Logical Solution:** **Option 1 — expose market freshness and filter on activity recency, rather than removing the market.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---
#### 🚨 DQ-BUSINESS-062 — Business-Linked Timestamps Are UTC but Carry No Time Zone

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-BUSINESS-062` |
| **Severity** | MEDIUM (wrong calendar day / weekday / hour for about 1 in 4 events if read as local time) |
| **Detected In** | Phase 6.9 — Timeliness → **6.9.4 Temporal Consistency**, section D (time-zone interpretation) |
| **Also Confirmed In** | Three independent tests: (1) quiet 3-hour window per state lies at 08–11 h on the raw clock but at 03–04 h in **14 / 14** states after UTC→local conversion; (2) DST spring-forward test: **1,440** reviews at raw 02:xx on the 14 DST-start Sundays (a local hour that does not exist) vs ~1,267 expected — impossible for local wall-clock time; (3) separate exploratory query gave the same 1,440 |
| **Affected Columns** | `yelp_review.date`, `yelp_tip.date`, `yelp_checkin.date` (and every business-level date feature derived from them) |
| **Total Affected Rows** | **2,004,782** of 6,989,984 reviews in the 14 metro states (**28.7 %**) fall on a different local calendar day after conversion (mostly the previous day) |
| **Invalid Value** | e.g. a review stored as `2021-03-14 02:30:00` for a Philadelphia business (that local time does not exist) |
| **Domain Rule** | Event timestamps must be local business time, or the time zone must be declared |

**Description:** The raw timestamp strings have no offset, and the dataset has no time-zone column. Their behaviour is that of UTC: activity is lowest at 03–04 h local time only after conversion, and reviews exist in the hour that daylight-saving skips. Reading them as local time shifts every evening review (after 19–20 h local in the US East) to the next day. For the business table this distorts `last_activity`, days-since-last-activity, weekday / hour-of-week patterns compared with `hours`, and "active in opening hours" checks (e.g. DQ-BUSINESS-050).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Silver: parse as `TIMESTAMP` in UTC (declared in the column comment / TBLPROPERTIES), add a business time-zone attribute derived from `state` (plus city for split states) and compute local-time columns only where needed (`event_local_ts`, `event_local_date`)** | Keeps the source instant; local features become correct and explicit |
| 2 | **Convert and store only local time** | Loses the original instant; DST ambiguity in the fall-back hour |
| 3 | **Leave as-is, document only** | Analysts will keep using the wrong day / hour |

**Best Logical Solution:** **Option 1 — store UTC explicitly and derive local-time columns from the business's time zone.**

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver Cleaned |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |

---

> **6.9 Register note:** Phase 6.9 raised DQ-BUSINESS-060 … 062 (next free ID: **DQ-BUSINESS-063**). Totals from 6.9.5: **68 checks — 3 FINDING, 30 INFO, 35 PASS**.
>
> **Clean results (PASS):** one shared cut-off (2022-01-19) for review / tip / check-in in all 14 metros; tables loaded in one batch with no later data change and unchanged row counts; every review, tip, check-in token and `yelping_since` value has 19 characters, parses, round-trips, and lies in [2004-10-01, as-of]; check-in lists are sorted; no time-of-day heaping (full second precision); no empty month in any source; check-in feed stable up to the cut-off.
>
> **Snapshot properties documented (INFO, no new ID):** snapshot is **1,739 days** old (as-of 2022-01-19); no row-level ingestion metadata and no lineage TBLPROPERTIES (add `source.snapshot_date` in Silver, R6); tips start 2009-04 and check-ins 2009-12 (left-censored); 2005 and 2022 are partial years and 2022-01 a partial month; last full year volume vs peak: reviews 0.68, tips 0.32, check-ins 0.28; 41,328 businesses last observed before 2020-03; 2,846 established open businesses have < 10 % of reviews from the last 3 years and 36,314 open businesses a median review age > 5 years (stars without recency); hours / attributes still published for 18,013 / 23,747 businesses silent > 3 years; 117 reviews by 72 users predate `yelping_since` (median 648 days — account reset, user-table issue); 2,664 check-in lists contain duplicate tokens (DQ-CHECKIN-005).
>
> **Re-detected existing errors (no new ID):** DQ-BUSINESS-036 (stale `is_open`: 22,496 + 5,542 open businesses silent 1–3 y / > 3 y; 130 closed businesses active in the last 30 days), DQ-BUSINESS-056 (counter drift: 279 businesses with `review_count` below and 30 above the distinct reviewers in the extract, mostly reviewed in the final weeks).

---

## 4. Consolidated Error Register

All **62** business data-quality errors raised in `4.2.1_Business_Deep_EDA` (next free ID: **DQ-BUSINESS-063**). Severity is copied from each issue block; the Chosen Cleaning Approach is still pending human review for every one.

| Error ID | Title | Severity | Phase |
| --- | --- | --- | --- |
| DQ-BUSINESS-001 | Storage Type Non-Conformance vs Silver Contract | FINDING | 6.1.2 |
| DQ-BUSINESS-002 | Literal `'None'` Strings Masquerading as NULL | FINDING | 6.1.3 |
| DQ-BUSINESS-003 | Inconsistent Python-Repr Quoting in Enum Attributes | FINDING | 6.1.3 |
| DQ-BUSINESS-004 | Leading/Trailing Whitespace in Identity Text Fields | FINDING | 6.1.3 |
| DQ-BUSINESS-005 | Duplicate Categories Within a Business | FINDING | TE.2 |
| DQ-BUSINESS-006 | Empty (`{}`) and All-`None` Nested Attribute Dicts | FINDING | TE.2 |
| DQ-BUSINESS-007 | Ambiguous and Cross-Midnight Opening Hours | FINDING | TE.2 |
| DQ-BUSINESS-008 | Partial Key Sets in Nested Attribute Dicts | FINDING | TE.2 |
| DQ-BUSINESS-009 | Non-Existent State Code `XMS` | FINDING | 6.2 |
| DQ-BUSINESS-010 | Real State Codes Outside the Yelp Footprint | FINDING | 6.2 |
| DQ-BUSINESS-011 | Coordinates Outside Every Yelp Metro | FINDING | 6.2 |
| DQ-BUSINESS-012 | Implausibly Short Daily Opening Windows | WARNING | 6.2 |
| DQ-BUSINESS-013 | Category Labels Containing `, ` Split into Fake Categories | FINDING | 6.2 |
| DQ-BUSINESS-014 | Truncated Canadian Postal Codes (FSA Only) | FINDING | 6.2 |
| DQ-BUSINESS-015 | Empty-String Postal Codes (Masked NULL) | FINDING | 6.2 |
| DQ-BUSINESS-016 | Malformed Postal Codes | FINDING | 6.2 |
| DQ-BUSINESS-017 | Invisible Unicode Characters in Name, Address and City | FINDING | 6.2 |
| DQ-BUSINESS-018 | Polluted City Values | FINDING | 6.2 |
| DQ-BUSINESS-019 | Degenerate Street Addresses | FINDING | 6.2 |
| DQ-BUSINESS-020 | Contradictory Dietary Flags (vegan without vegetarian) | WARNING | 6.2 |
| DQ-BUSINESS-021 | One City Written in Many Spellings | FINDING | 6.3 |
| DQ-BUSINESS-022 | Same Brand / Chain Written in Several Spellings | LOW / INFO | 6.3 |
| DQ-BUSINESS-023 | `state` Contradicts the USPS ZIP Prefix | FINDING | 6.3 |
| DQ-BUSINESS-024 | BYOB Policy Encoded in Three Contradicting Columns | FINDING | 6.3 |
| DQ-BUSINESS-025 | Midnight Close / "Open All Day" in Several Spellings | FINDING | 6.3 |
| DQ-BUSINESS-026 | `Open24Hours = True` Contradicts the Weekly Schedule | LOW | 6.3 |
| DQ-BUSINESS-027 | Coarse and Mixed-Precision Coordinates | LOW | 6.3 |
| DQ-BUSINESS-028 | Yes/No Facts Stored in Four Physical Encodings | MEDIUM | 6.3 |
| DQ-BUSINESS-029 | Multiple Consecutive Spaces and Dangling Separators | LOW | 6.3 |
| DQ-BUSINESS-030 | Inconsistent Capitalisation in `city` and `address` | LOW | 6.3 |
| DQ-BUSINESS-031 | Address Tokens Not in USPS Pub. 28 Standard Form | LOW / INFO | 6.3 |
| DQ-BUSINESS-032 | Empty-String `address` (Masked NULL, Not Missing at Random) | FINDING | 6.3 |
| DQ-BUSINESS-033 | "Closed" Days Encoded Implicitly as NULL | MEDIUM | 6.3 |
| DQ-BUSINESS-034 | Structural "Not Applicable" NULLs in the Wide Attribute Model | MEDIUM | 6.4 |
| DQ-BUSINESS-035 | Non-Random, Block-Wise Missingness | HIGH (analytics) | 6.4 |
| DQ-BUSINESS-036 | `is_open = TRUE` for Businesses Silent for Years | MEDIUM | 6.4 |
| DQ-BUSINESS-037 | `business_id` Values Starting With `-` | LOW | 6.5 |
| DQ-BUSINESS-038 | Same Storefront Under Several `business_id`s | HIGH | 6.5 |
| DQ-BUSINESS-039 | Same-Name Listings Within 100 m | MEDIUM | 6.5 |
| DQ-BUSINESS-040 | Placeholder Centroid Coordinates | MEDIUM | 6.5 |
| DQ-BUSINESS-041 | `Music.no_music` Is Never TRUE | LOW–MEDIUM | 6.5 |
| DQ-BUSINESS-042 | `RestaurantsAttire = 'formal'` on Fast-Food / Casual Venues | MEDIUM | 6.6 |
| DQ-BUSINESS-043 | Adults-Only Venues Marked `GoodForKids` | LOW–MEDIUM | 6.6 |
| DQ-BUSINESS-044 | `Ambience` Both `divey` and `classy` | LOW–MEDIUM | 6.6 |
| DQ-BUSINESS-045 | Food Drive-Thrus With `RestaurantsTakeOut = False` | MEDIUM | 6.6 |
| DQ-BUSINESS-046 | Address Block Contradicts the Coordinates | HIGH | 6.6 |
| DQ-BUSINESS-047 | False "Overnight" Windows (Inverted / 12-Hour Clock) | MEDIUM | 6.6 |
| DQ-BUSINESS-048 | Overnight Window Overlaps the Next Day | LOW | 6.6 |
| DQ-BUSINESS-049 | `BestNights` on Closed Days | MEDIUM | 6.6 |
| DQ-BUSINESS-050 | `GoodForMeal` Outside Opening Hours | MEDIUM | 6.6 |
| DQ-BUSINESS-051 | `HappyHour = True` With `Alcohol = 'none'` | LOW–MEDIUM | 6.6 |
| DQ-BUSINESS-052 | Ambiguous `BYOB` Meaning (Corkage Without BYOB) | MEDIUM | 6.6 |
| DQ-BUSINESS-053 | Attributes Outside Their Business-Type Scope | LOW | 6.6 |
| DQ-BUSINESS-054 | One ZIP Code, Several City Names | LOW | 6.6 |
| DQ-BUSINESS-055 | Stored Aggregates Count Only Current Reviews | MEDIUM | 6.7 |
| DQ-BUSINESS-056 | Counter Snapshot Drift | LOW | 6.7 |
| DQ-BUSINESS-057 | Yelp Event Pages Listed as Businesses | MEDIUM | 6.8 |
| DQ-BUSINESS-058 | `name` Carries Status or Location Text | LOW | 6.8 |
| DQ-BUSINESS-059 | Price Range Contradicts Business Type | LOW | 6.8 |
| DQ-BUSINESS-060 | Tip Feed Halves Before the Cut-Off | MEDIUM | 6.9 |
| DQ-BUSINESS-061 | Alberta Market Coverage Decay | MEDIUM | 6.9 |
| DQ-BUSINESS-062 | Timestamps Are UTC Without a Time Zone | MEDIUM | 6.9 |

