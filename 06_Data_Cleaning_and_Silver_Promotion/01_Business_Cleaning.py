# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC # 🧹 Business Data Cleaning & Silver Promotion
# MAGIC
# MAGIC > **Layer Transition:** Bronze Final → Silver Cleaned  
# MAGIC > **Source Table:** `yelp_poc_uc.bronze_final.yelp_business` (150,346 rows — 1 row per business; nested `attributes` STRUCT with 7 Python-dict strings, `hours` STRUCT, comma-packed `categories`)  
# MAGIC > **Target Tables:** `yelp_poc_uc.silver_cleaned.yelp_business` (parent) + 10 child tables (3 level-1: attributes, hours, categories · 7 parsed-dict: ambience, best_nights, parking, dietary_restrictions, good_for_meal, hair_specializes_in, music)  
# MAGIC > **Quarantine Tables:** `yelp_poc_uc.quarantine.yelp_business_out_of_scope` (whole businesses removed, MS-01.4) · `yelp_review_orphan_business` / `yelp_checkin_orphan_business` / `yelp_tip_orphan_business` (downstream rows of removed businesses)  
# MAGIC > **DQ Reference:** `01_Business_DQ_Issues_and_Solutions.md` (`DQ-BUSINESS-001` … `DQ-BUSINESS-063`)
# MAGIC
# MAGIC This notebook implements the **Bronze → Silver Cleaned data population** for the Yelp Business dataset. It applies the chosen resolutions from the Data Quality Issues & Solutions document to produce clean, type-corrected and normalized Silver Cleaned tables that follow the `01_Business_Silver_Cleaned` DDL contract.
# MAGIC
# MAGIC > **⚠️ Prerequisites — Run These First (in order):**  
# MAGIC > 1. **`01_Business_Silver_Cleaned`** (DDL query) — creates the `yelp_poc_uc.silver_cleaned` schema and the 11 empty business tables with PK/FK constraints  
# MAGIC > 2. **`yelp_poc_uc.quarantine`** schema must exist — rows removed during cleaning are preserved there for audit  
# MAGIC >  
# MAGIC > This notebook then **populates** the tables created by step 1. The DDL query is idempotent and can be re-run safely.
# MAGIC
# MAGIC > **🔁 Downstream Re-Run Required:**  
# MAGIC > `yelp_business` is the FK parent of `yelp_checkin`, `yelp_review` and `yelp_tip`. This notebook quarantines out-of-scope businesses (DQ-BUSINESS-009 / 010), so the dependent cleaning notebooks (**`02_Checkin_Cleaning`**, **`03_Review_Cleaning`**, and the tip notebook once built) must be re-run afterwards. Their child rows for quarantined businesses must be routed to quarantine as well, otherwise the FK is broken.

# COMMAND ----------

# DBTITLE 1,Cleaning Rules — How Every DQ Issue Is Handled
# MAGIC %md
# MAGIC ## 📏 Cleaning Rules — How Every DQ Issue Is Handled
# MAGIC
# MAGIC ### Global rules (apply to every issue)
# MAGIC
# MAGIC | # | Rule |
# MAGIC | --- | --- |
# MAGIC | R-1 | **Never delete a row because a value is NULL, `None`, `{}` or empty.** Replace the invalid value with a real SQL / Delta `NULL` and keep the row. |
# MAGIC | R-2 | Missing information is always a real `NULL` — never the text `"None"`, `"null"`, `""`, and never an invented value such as `false` or `0`. |
# MAGIC | R-3 | Fixes are applied to the **same working DataFrames** built in the Working Split cells (`df_business`, `df_attributes`, `df_hours`, `df_categories`, the 7 dict DataFrames). No new DataFrames are created for later steps; a `_before...` copy is kept only so the verify cell can compare. |
# MAGIC | R-4 | Bronze is never modified. Everything happens in memory until the final write to Silver. |
# MAGIC | R-5 | Every nested dict is standardized to its **full contract key list**: a missing key, an empty dict `{}`, the word `None` or a bronze NULL all become a row whose key columns are `NULL`. |
# MAGIC | R-6 | The DQ document's "Best Logical Solution" is a suggestion (AI-generated). When the human review picks a different solution, both this notebook and the DQ document are updated. |
# MAGIC
# MAGIC > Accepted row exclusions: (1) **review** table — reviews whose `text` is NULL (required by the ML model); (2) **this notebook, MS-01.4** — the 15 out-of-scope businesses of DQ-BUSINESS-009 / 010 (decision Ibrahim Mohamed, 2026-10-25), moved with all their rows to `quarantine.yelp_business_out_of_scope`.
# MAGIC
# MAGIC > **Invalid values (row kept):** when a value is *invalid* (not just empty) and can't be corrected without guessing, it becomes SQL NULL and the row is kept. It is not repaired, truncated or filled from neighbours, and it is not quarantined; the original stays in bronze. First used for `postal_code` in MS-01.5 (decision Ibrahim Mohamed, 2026-10-26).
# MAGIC
# MAGIC ### The 4 cells of every DQ issue
# MAGIC
# MAGIC | Cell | Title pattern | What it must contain |
# MAGIC | --- | --- | --- |
# MAGIC | **1 — Markdown** | `DQ-BUSINESS-xxx — Issue, Solutions & Decision` | • The issue metadata table copied from the DQ document<br>• **The Problem in Plain Words** (short table with real examples)<br>• The suggested solutions from the DQ document<br>• **Solution Taken** (what changes, in which DataFrame, row counts kept)<br>• **Not Done in This Block** (what is handled by a later issue, and which one)<br>• **Why This Is the Best Solution** |
# MAGIC | **2 — Show the Issue** | `DQ-BUSINESS-xxx — Show the Issue` | • **Actual sample rows** of the erroneous data from the working DataFrames, first<br>• Then **statistics**: counts per column / DataFrame, and a total reconciled with the DQ document<br>• Each output has a numbered heading and a `Purpose:` line |
# MAGIC | **3 — Apply the Solution** | `DQ-BUSINESS-xxx — Apply the Solution` | • Keeps a `_before...` copy of the frames it changes<br>• Replaces the working DataFrames with the fixed version<br>• Prints row counts before → after (they must be equal, rule R-1) |
# MAGIC | **4 — Verify the Issue Is Solved** | `DQ-BUSINESS-xxx — Verify the Issue Is Solved` | • The issue count is now 0<br>• A before → after table showing only the intended values changed<br>• Row counts unchanged<br>• **Actual sample rows** after the fix<br>• `assert` statements so the notebook stops if any check fails |
# MAGIC
# MAGIC ### Readability
# MAGIC
# MAGIC * Use plain, readable column headers in outputs (e.g. `Value before`, `Rows`), not internal names.
# MAGIC * Keep markdown short and in plain words; long DQ-document text goes inside a collapsible `<details>` block.

# COMMAND ----------

# DBTITLE 1,Pipeline Context
# MAGIC %md
# MAGIC ## 📍 Pipeline Context
# MAGIC
# MAGIC | Step | Notebook | UC Location | Layer |
# MAGIC | ---: | --- | --- | --- |
# MAGIC | 1 | `03_Extract_Yelp_Dataset_from_Kaggle` | `/Volumes/yelp_poc_uc/bronze_landing/landing_zone/yelp_textual_dataset/` | Landing Zone |
# MAGIC | 2 | `01_Landing_to_Staging_Schema_Validator` | `yelp_poc_uc.bronze_staging` (`yelp_business_clean` / `yelp_business_errors`) | Bronze Staging |
# MAGIC | 3 | `02_Staging_Quality_Gate_and_Promotion` | `yelp_poc_uc.bronze_final.yelp_business` | Bronze Final |
# MAGIC | 4 | `4.1.1_Business_Basic_Profile` | _(reads bronze_final)_ | Basic EDA |
# MAGIC | 5 | `4.2.1_Business_Deep_EDA` | _(reads bronze_final + linked review / tip / check-in / user)_ | Deep EDA / DQ Assessment |
# MAGIC | 6a | `01_Business_Silver_Cleaned` (DDL) | `yelp_poc_uc.silver_cleaned.yelp_business` + 10 child tables (schema) | Silver Cleaned DDL |
# MAGIC | **6b** | **`01_Business_Cleaning` ◄ THIS NOTEBOOK** | **`yelp_poc_uc.silver_cleaned.yelp_business` + 10 child tables (data)**, `yelp_poc_uc.quarantine.yelp_business_out_of_scope` | **Silver Cleaned** |
# MAGIC | 6c | `02_Checkin_Silver_Cleaned` (DDL) | `yelp_poc_uc.silver_cleaned.yelp_checkin` (schema) | Silver Cleaned DDL |
# MAGIC | 6d | `02_Checkin_Cleaning` _(FK child — re-run after 6b)_ | `yelp_poc_uc.silver_cleaned.yelp_checkin` (data) | Silver Cleaned |
# MAGIC | 6e | `03_Review_Silver_Cleaned` (DDL) | `yelp_poc_uc.silver_cleaned.yelp_review` (schema) | Silver Cleaned DDL |
# MAGIC | 6f | `03_Review_Cleaning` _(FK child — re-run after 6b)_ | `yelp_poc_uc.silver_cleaned.yelp_review` (data) | Silver Cleaned |
# MAGIC | 7 | Gold Aggregation _(planned)_ | `yelp_poc_uc.gold` _(planned)_ | Gold |

# COMMAND ----------

# DBTITLE 1,Scope — Core DQ Issues Resolved Here
# MAGIC %md
# MAGIC ## ✅ Scope — Core DQ Issues Resolved in This Notebook
# MAGIC
# MAGIC This notebook addresses the **structural, type, NULL-semantics, key-integrity and text-hygiene** issues that must be fixed to produce valid Silver Cleaned tables. The classification rule is:
# MAGIC
# MAGIC * **In scope** — the fix is required to satisfy the `01_Business_Silver_Cleaned` DDL contract (types, keys, FK, domains, NULL semantics), is deterministic, needs no external reference data, and fits the existing columns (no new flag / derived columns).
# MAGIC * **Out of scope** — the recommended fix adds derived or flag columns, needs an external reference (geocoder, USPS tables), changes the data model, or is an analytical caveat. These are listed in the next cell.
# MAGIC
# MAGIC ### Output Tables
# MAGIC
# MAGIC | # | Table | Grain | Built From |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | `yelp_business` | 1 row per business (PK `business_id`) | identity, location, `stars`, `review_count`, `is_open` |
# MAGIC | 2 | `yelp_business_attributes` | 0:1 per business | 32 single-value attributes (24 BOOLEAN, 1 INT, 7 enum STRING) |
# MAGIC | 3 | `yelp_business_hours` | 0:1 per business | 7 day columns (raw `H:M-H:M` range) |
# MAGIC | 4 | `yelp_business_categories` | 1:N per business (PK `business_id`, `category_name`) | exploded `categories` |
# MAGIC | 5–11 | `yelp_business_ambience`, `_best_nights`, `_parking`, `_dietary_restrictions`, `_good_for_meal`, `_hair_specializes_in`, `_music` | 1 row per business (all key columns NULL when the dict has no information) | parsed Python-dict strings → BOOLEAN flags |
# MAGIC
# MAGIC No `_ingestion_timestamp` column — lineage is tracked via TBLPROPERTIES metadata (see DDL rule R6).
# MAGIC
# MAGIC ### DQ Issues Addressed
# MAGIC
# MAGIC | DQ Issue | Summary | Resolution Applied |
# MAGIC | --- | --- | --- |
# MAGIC | **DQ-BUSINESS-001 / 028** | Storage types do not match the Silver contract; yes/no facts stored in 4 physical encodings | Typed cast and parse: `is_open` BIGINT → BOOLEAN, `stars` DOUBLE → DECIMAL(2,1), `'True'`/`'False'` → BOOLEAN, price range → INT, dict strings → BOOLEAN child columns. Lossless-decode check as in TE.1 |
# MAGIC | **DQ-BUSINESS-002** | Literal `'None'` strings used as NULL (9,653 businesses, 12,827 tokens) | Case-sensitive `'None'` → NULL; lowercase `'none'` is handled later by DQ-063 |
# MAGIC | **DQ-BUSINESS-003** | Mixed `u'x'` / `'x'` quoting in enum attributes (27,610 businesses) | Anchored regex unquote, multi-word values split with `_` (`18plus` → `18_plus`), then check against the DDL comment list |
# MAGIC | **DQ-BUSINESS-006** | Dicts with no information: bronze NULL, `'None'`, empty `{}`, all-`None` | Row kept; every contract key column = SQL NULL (e.g. Music `dj` … `karaoke` all NULL). No rows deleted |
# MAGIC | **DQ-BUSINESS-008** | Partial key sets in nested dicts (1,942 dicts) | Standardize the columns: expand to the full contract key list; absent key → NULL (unknown) |
# MAGIC | **DQ-BUSINESS-063** | Null-like negative words `'none'` / `'no'` in Alcohol, WiFi, Smoking, BYOBCorkage (45,893 values) | Whole value `no` → `false` (24,983); null-like words `none` / `unknown` / `n/a` / `null` / … → NULL (20,910 Alcohol `none`). Rows kept |
# MAGIC | **DQ-BUSINESS-004 / 017 / 029** | Leading/trailing whitespace, invisible Unicode (U+200B, U+00A0), repeated spaces and dangling separators in `name`, `address`, `city` | One ordered text-hygiene step: remove/replace invisible chars → collapse spaces → strip dangling separators → `trim()` |
# MAGIC | **DQ-BUSINESS-005** | Same category repeated within a business (43 businesses) | Deduplicate on the composite PK, keep `MIN(category_position)` |
# MAGIC | **DQ-BUSINESS-013** | 4 Yelp labels containing `, ` split into fake categories (4,150 businesses, 9,371 fragment rows) | Whitelist-protected split (`Beer, Wine & Spirits`, `Books, Mags, Music & Video`, `Used, Vintage & Consignment`, `Wills, Trusts, & Probates`), then DQ-005 dedupe |
# MAGIC | **DQ-BUSINESS-009** | Non-existent state `XMS` (UK business, 1 row) | Quarantine to `quarantine.yelp_business_out_of_scope` (reason `NON_EXISTENT_STATE`) |
# MAGIC | **DQ-BUSINESS-010** | Real state codes outside the Yelp footprint (20 rows) | (a) 6 rows: correct `state` from the ZIP prefix when ZIP, city and coordinates agree. (b) 13 + (c) 1 rows: quarantine (`OUT_OF_FOOTPRINT`) |
# MAGIC | **DQ-BUSINESS-023 / 046** | `state` contradicts the ZIP prefix (71) / address block contradicts the coordinates (20) | Correct `state` only when the 2-of-3 witness rule (ZIP prefix, coordinates, city) is unanimous on the answer. Unresolved rows stay raw and are deferred (see next cell) |
# MAGIC | **DQ-BUSINESS-011** | Coordinates outside every Yelp metro (3 flagged) | 1 proven geocoding error → `latitude`/`longitude` set to NULL (no ZIP-centroid reference available); Bennington, VT goes to DQ-010 quarantine; Stone Harbor kept (false positive) |
# MAGIC | **DQ-BUSINESS-015 / 032** | Empty-string `postal_code` (73) and `address` (5,127 `''` + 1 `'.'`) | Masked NULL → SQL NULL |
# MAGIC | **DQ-BUSINESS-016** | Malformed postal codes (4 rows) | Upper-case, truncate to the valid FSA, NULL what cannot be recovered |
# MAGIC | **DQ-BUSINESS-014** | Canadian postal codes truncated to the FSA (196 AB rows) | Kept as-is — accurate partial value; validation pattern widened to accept FSA-only codes |
# MAGIC | **DQ-BUSINESS-018** | Polluted `city` values: trailing comma, embedded state, qualifiers (32 rows) | Rule-based cleanup plus a reviewed mapping for the 7 non-rule cases |
# MAGIC | **DQ-BUSINESS-019** | Degenerate street addresses with no street name (11 rows) | Set to NULL |
# MAGIC
# MAGIC > **⚠️ Note on quarantine (DQ-009 / 010):** 15 businesses (1 + 13 + 1) leave Silver. Their reviews, tips and check-ins must follow them into quarantine in the child notebooks, otherwise `yelp_checkin` / `yelp_review` / `yelp_tip` hold FK values without a parent.
# MAGIC
# MAGIC > **📝 Review status:** the *Chosen Cleaning Approach* block of every issue in the DQ document is still _TBD — pending human review_. This classification applies the documented **Best Logical Solution** and should be confirmed in the document before the pipeline is finalised.

# COMMAND ----------

# DBTITLE 1,Out of Scope — Deferred to Enrichment / Analytics Phase
# MAGIC %md
# MAGIC ## 🔜 Out of Scope — Deferred to Silver Enriched / Analytics Phase
# MAGIC
# MAGIC The DQ document recommends many **flag, derived, canonical and conflict columns** (e.g. `city_normalized`, `name_match_key`, `canonical_business_id`, `geo_precision`, `byob_policy`, `is_closed_<day>`, `open_minutes`, `is_yelp_event`, activity-recency flags). These are **not added in this notebook** — the Silver Cleaned tables keep exactly the DDL columns, and raw source values that are only *suspicious* (not provably wrong) are kept unchanged.
# MAGIC
# MAGIC They belong to the **Silver Enriched / Gold phase**, where the analytical use case decides which flags and features are needed. Keeping Silver Cleaned lean keeps it a general-purpose, reusable foundation without premature analytical assumptions.
# MAGIC
# MAGIC | Deferred DQ Issue | What It Recommends | Why Deferred |
# MAGIC | --- | --- | --- |
# MAGIC | DQ-BUSINESS-007 / 025 / 047 / 048 | Overnight / `0:0-0:0` / midnight-spelling flags, modulo-24 h duration, `closes_next_day`, `is_open_24h`, merged weekly intervals, corrected inverted windows | Derived schedule columns — raw range is kept in `yelp_business_hours` |
# MAGIC | DQ-BUSINESS-012 | Exclude 15/30-min windows from duration metrics | Metric-level filter — raw value kept |
# MAGIC | DQ-BUSINESS-033 | Explicit `is_closed_<day>` flags | Derived flags — raw NULL day preserved |
# MAGIC | DQ-BUSINESS-020 / 041 | Vegan ⇒ vegetarian implication; `has_music` derived from the other music keys (`no_music` never TRUE) | Gold derived measure — reported values kept |
# MAGIC | DQ-BUSINESS-024 / 052 | `byob_policy` with `BYOBCorkage` precedence + conflict flag | Derived semantic column |
# MAGIC | DQ-BUSINESS-026 / 042 / 043 / 044 / 045 / 049 / 050 / 051 / 053 / 059 | Conflict / contradiction flags across attributes, schedule and business type (Open24Hours, formal attire, adults-only + kids, divey + classy, drive-thru take-out, BestNights / GoodForMeal vs hours, HappyHour vs alcohol, out-of-type attributes, price range) | Correct value is not certain — flag and validated column in the enriched layer |
# MAGIC | DQ-BUSINESS-021 / 030 / 054 | `city_normalized` from a curated per-state map / USPS ZIP reference | Derived canonical column; USPS reference not ingested |
# MAGIC | DQ-BUSINESS-022 / 058 | `name_match_key`, `name_clean`, `name_is_location` | Derived matching columns — display name kept |
# MAGIC | DQ-BUSINESS-031 | USPS Pub. 28 standardised address column | Derived column; address-level matching not a current use case |
# MAGIC | DQ-BUSINESS-023 / 046 _(unresolved rows)_ | `geo_conflict_flag` / `geo_address_conflict`, reverse geocoding | Needs flag column or external geocoder |
# MAGIC | DQ-BUSINESS-027 / 040 | `geo_precision` flag for coarse and placeholder-centroid coordinates | Precision metadata — coordinates kept |
# MAGIC | DQ-BUSINESS-034 / 035 | Long attribute model + applicability map, drop near-empty attributes; flag and stratify non-random missingness | Data-model change (Silver Normalized) and analytical handling |
# MAGIC | DQ-BUSINESS-036 / 061 | Activity-recency flags next to `is_open`; per-market freshness (Alberta coverage decay) | Requires linked review / tip / check-in facts — Gold feature |
# MAGIC | DQ-BUSINESS-038 / 039 | `canonical_business_id` crosswalk, `successor_business_id`, near-duplicate tiers | Entity resolution — no FK rewrite in Silver Cleaned |
# MAGIC | DQ-BUSINESS-055 / 056 | Recompute `review_count` / `stars` from current reviews + in-sync flag | Cross-table aggregate — source counters kept as delivered |
# MAGIC | DQ-BUSINESS-057 | `is_yelp_event` flag, exclude by default (48 event pages) | Analytical filter — rows are valid FK parents for 1,668 reviews |
# MAGIC | DQ-BUSINESS-037 | Keep `business_id` starting with `-`, enforce quoting on export | No transformation — export-side concern |
# MAGIC | DQ-BUSINESS-060 | Tip-specific completeness date for recency features | Metadata caveat — no row-level transformation |
# MAGIC | DQ-BUSINESS-062 | Store UTC explicitly, derive local-time columns from the business time zone | Applies to review / tip / check-in timestamps — no timestamp in the business table |

# COMMAND ----------

# DBTITLE 1,Environment Setup — Prerequisite Check
# ============================================================
# 🔧 Environment Setup — Prerequisite Check
# ============================================================
# PREREQUISITES (run this DDL query BEFORE this notebook):
#   1. 01_Business_Silver_Cleaned — creates silver_cleaned schema
#      and the yelp_business parent table (+ 10 child tables)
#
# This notebook POPULATES the parent table only.
# No schema creation is needed here.

# Verify the target table exists (created by DDL query)
try:
    tbl_info = spark.sql("DESCRIBE TABLE yelp_poc_uc.silver_cleaned.yelp_business").collect()
    col_count = len([r for r in tbl_info if r['col_name'] and not r['col_name'].startswith('#')])
    print("✅ Target table exists: yelp_poc_uc.silver_cleaned.yelp_business")
    print(f"   Columns: {col_count}")
except Exception as e:
    raise RuntimeError(
        "❌ Target table yelp_poc_uc.silver_cleaned.yelp_business does not exist!\n"
        "   Run the DDL query first:\n"
        "   1. 01_Business_Silver_Cleaned\n"
        f"   Error: {e}"
    )

# COMMAND ----------

# DBTITLE 1,Working Split — Remodel Bronze into Output Domains
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Working Split — Remodel Bronze into Output Domains
# MAGIC
# MAGIC Before handling the in-scope DQ issues one by one, the bronze business table is first divided into the working DataFrames that mirror the final Silver-Cleaned output domains.
# MAGIC
# MAGIC The next two code cells:
# MAGIC
# MAGIC * read `yelp_poc_uc.bronze_final.yelp_business`
# MAGIC * split it into the parent / child working DataFrames: `df_business`, `df_attributes` (32 single-value attributes), `df_hours`, `df_categories`, and one DataFrame per nested dict (`df_ambience`, `df_best_nights`, `df_business_parking`, `df_dietary_restrictions`, `df_good_for_meal`, `df_hair_specializes_in`, `df_music`)
# MAGIC * every dict DataFrame has **one row per business (150,346)**; a business without the dict keeps `raw_dict = NULL` (rule R-1, DQ-BUSINESS-006)
# MAGIC * the keys of the 7 dicts are **detected from the data** and mapped to the Silver DDL columns; each dict DataFrame is then reshaped to **wide** form (`business_id` + one STRING column per Silver key + `raw_dict` for lineage). A key not written in a dict is NULL; values stay `'True'` / `'False'` / `'None'` until they are typed later
# MAGIC * create reusable constants for attributes, hours, categories and nested dict domains
# MAGIC * initialize the in-memory quarantine frame `df_quarantine_business`
# MAGIC * keep everything **in memory only** for now — nothing is written back to Unity Catalog yet
# MAGIC
# MAGIC This makes the later issue-by-issue work easier, because each DQ concern can be queried and fixed in the DataFrame where it actually belongs.

# COMMAND ----------

# DBTITLE 1,Build Working DataFrames from Bronze Business
from pyspark.sql import functions as F, types as T

# ============================================================
# Build Working DataFrames from Bronze Business
# ============================================================
# This cell does NOT write to Silver. It only prepares the
# in-memory DataFrames that the issue-by-issue cleaning cells
# will use.

BRONZE_TABLE = "yelp_poc_uc.bronze_final.yelp_business"
FOOTPRINT_STATES = ["PA", "NJ", "DE", "FL", "IN", "TN", "AZ", "LA", "NV", "AB", "CA", "ID", "MO", "IL"]
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

BOOL_ATTRS = [
    "AcceptsInsurance", "BikeParking", "BusinessAcceptsBitcoin", "BusinessAcceptsCreditCards",
    "ByAppointmentOnly", "BYOB", "Caters", "CoatCheck", "Corkage", "DogsAllowed", "DriveThru",
    "GoodForDancing", "GoodForKids", "HappyHour", "HasTV", "Open24Hours", "OutdoorSeating",
    "RestaurantsCounterService", "RestaurantsDelivery", "RestaurantsGoodForGroups",
    "RestaurantsReservations", "RestaurantsTableService", "RestaurantsTakeOut",
    "WheelchairAccessible"
]
ENUM_ATTRS = ["AgesAllowed", "Alcohol", "BYOBCorkage", "NoiseLevel", "RestaurantsAttire", "Smoking", "WiFi"]
INT_ATTRS = ["RestaurantsPriceRange2"]
DICT_ATTRS = {
    "Ambience": ["touristy", "hipster", "romantic", "divey", "intimate", "trendy", "upscale", "classy", "casual"],
    "BestNights": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
    "BusinessParking": ["garage", "street", "validated", "lot", "valet"],
    "DietaryRestrictions": ["dairy-free", "gluten-free", "vegan", "kosher", "halal", "soy-free", "vegetarian"],
    "GoodForMeal": ["dessert", "latenight", "lunch", "dinner", "brunch", "breakfast"],
    "HairSpecializesIn": ["coloring", "africanamerican", "curly", "kids", "perms", "asian", "straightperms", "extensions"],
    "Music": ["dj", "background_music", "no_music", "jukebox", "live", "video", "karaoke"]
}
PROTECTED_CATEGORY_LABELS = {
    "Beer, Wine & Spirits": "__CAT_BEER_WINE_SPIRITS__",
    "Books, Mags, Music & Video": "__CAT_BOOKS_MAGS_MUSIC_VIDEO__",
    "Used, Vintage & Consignment": "__CAT_USED_VINTAGE_CONSIGNMENT__",
    "Wills, Trusts, & Probates": "__CAT_WILLS_TRUSTS_PROBATES__",
}

# Read bronze once
_df_bronze = spark.table(BRONZE_TABLE)
bronze_count = _df_bronze.count()

# Split into working domains
TOP_LEVEL_COLS = [
    "business_id", "name", "address", "city", "state", "postal_code",
    "latitude", "longitude", "stars", "review_count", "is_open"
]

df_bronze = _df_bronze
# Parent working frame
# Raw values are kept initially; issue cells will progressively fix them.
df_business = df_bronze.select(*TOP_LEVEL_COLS)

# 1:1 child working frames
# Keep the raw bronze encodings for now so each issue can be shown before it is fixed.
# df_attributes holds only the 32 single-value attributes; the 7 dict-string
# attributes get their own DataFrames below.
SINGLE_ATTRS = BOOL_ATTRS + INT_ATTRS + ENUM_ATTRS
df_attributes = df_bronze.select("business_id", *[F.col(f"attributes.{a}").alias(a) for a in SINGLE_ATTRS])
df_hours = df_bronze.select("business_id", *[F.col(f"hours.{d}").alias(d) for d in DAYS])

# Categories are split naively first so DQ-BUSINESS-013 and 005 can be shown and then fixed.
df_categories_source = df_bronze.select("business_id", "categories")
df_categories = (
    df_categories_source
    .where(F.col("categories").isNotNull())
    .select("business_id", F.posexplode(F.split(F.col("categories"), ", ")).alias("category_position", "category_name"))
)

# Level-2 nested-dict frames: one DataFrame per Silver dict table.
# Raw dict strings are kept as-is; they are decoded issue-by-issue later
# (DQ-001/028 parse, DQ-002 None, DQ-006 empty dicts, DQ-008 partial keys).
# Grain 1:1 - every business gets a row (rule R-1: never drop a row because it is NULL).
# A business without the dict (bronze NULL) keeps raw_dict = NULL; later its key columns become NULL (DQ-006).
def _dict_df(attr):
    return df_bronze.select("business_id", F.col(f"attributes.{attr}").alias("raw_dict"))

df_ambience             = _dict_df("Ambience")
df_best_nights          = _dict_df("BestNights")
df_business_parking     = _dict_df("BusinessParking")
df_dietary_restrictions = _dict_df("DietaryRestrictions")
df_good_for_meal        = _dict_df("GoodForMeal")
df_hair_specializes_in  = _dict_df("HairSpecializesIn")
df_music                = _dict_df("Music")

# Bronze attribute -> (working DataFrame variable, Silver target table)
DICT_DF_MAP = {
    "Ambience":            ("df_ambience",             "yelp_business_ambience"),
    "BestNights":          ("df_best_nights",          "yelp_business_best_nights"),
    "BusinessParking":     ("df_business_parking",     "yelp_business_parking"),
    "DietaryRestrictions": ("df_dietary_restrictions", "yelp_business_dietary_restrictions"),
    "GoodForMeal":         ("df_good_for_meal",        "yelp_business_good_for_meal"),
    "HairSpecializesIn":   ("df_hair_specializes_in",  "yelp_business_hair_specializes_in"),
    "Music":               ("df_music",                "yelp_business_music"),
}

# Empty in-memory quarantine frame for later issue cells
quarantine_schema = T.StructType([
    T.StructField("business_id", T.StringType(), False),
    T.StructField("quarantine_reason", T.StringType(), False),
    T.StructField("dq_issue", T.StringType(), False),
    T.StructField("note", T.StringType(), True),
])

df_quarantine_business = spark.createDataFrame([], quarantine_schema)

print("📥 Bronze business source loaded")
print(f"   Bronze rows:            {bronze_count:>12,}")
print("\n🧱 Working DataFrames initialised (in memory only)")
print("   df_business            → parent business domain")
print("   df_attributes          → level-1 attributes domain (32 single-value attributes)")
print("   df_hours               → level-1 hours domain")
print("   df_categories          → category tokens (naive first split)")
for attr, (var, tbl) in DICT_DF_MAP.items():
    print(f"   {var:<22} → level-2 {tbl} (raw dict string)")
print("   df_quarantine_business → in-memory quarantine accumulator")

# COMMAND ----------

# DBTITLE 1,Detect All Keys in the 7 Dict DataFrames
# ============================================================
# 🔎 Detect All Keys in the 7 Dict DataFrames
# ============================================================
# Read every key actually written in the dict text (data-driven, not the
# hard-coded DICT_ATTRS list), clean it with a regex (u'casual' / 'casual'
# -> casual) and map it to its Silver DDL column (read live from the empty
# Silver tables): dairy-free -> dairy_free.
# Fail-fast: a key with no Silver column, or an unknown value, stops the cell.
from functools import reduce

SILVER_SCHEMA     = "yelp_poc_uc.silver_cleaned"
DICT_PAIR_RE      = r"(u?'[^']*')\s*:\s*([^,}]+)"   # group 1 = key AS WRITTEN, group 2 = value
DICT_KEY_CLEAN_RE = r"^u?'|'$"                      # removes the leading u' or ' and the closing '
DICT_VALUE_TOKENS = ["True", "False", "None"]

# Silver DDL key columns per dict, in DDL order (business_id excluded)
DICT_KEY_COLS = {attr: [c for c in spark.table(f"{SILVER_SCHEMA}.{tbl}").columns if c != "business_id"]
                 for attr, (_var, tbl) in DICT_DF_MAP.items()}

# One row per (business, key) pair written in the text
_pairs_det = reduce(lambda a, b: a.unionByName(b), [
    globals()[var].where(F.col("raw_dict").isNotNull())
    .select(F.lit(attr).alias("Attribute"), F.lit(var).alias("DataFrame"), "business_id",
            F.explode(F.zip_with(F.regexp_extract_all("raw_dict", F.lit(DICT_PAIR_RE), 1),
                                 F.regexp_extract_all("raw_dict", F.lit(DICT_PAIR_RE), 2),
                                 lambda k, v: F.struct(k.alias("written"), F.trim(v).alias("value")))).alias("p"))
    .select("Attribute", "DataFrame", "business_id", F.col("p.written").alias("Key as written"),
            F.col("p.value").alias("Value"))
    for attr, (var, _tbl) in DICT_DF_MAP.items()])
_pairs_det = (_pairs_det
              .withColumn("Key", F.regexp_replace("Key as written", DICT_KEY_CLEAN_RE, ""))
              .withColumn("Silver column", F.regexp_replace("Key", "-", "_")))

_inv = (_pairs_det.groupBy("Attribute", "DataFrame", "Key", "Silver column")
        .agg(F.array_sort(F.collect_set("Key as written")).alias("Written as"),
             F.countDistinct("business_id").alias("Dicts with this key"),
             F.array_sort(F.collect_set("Value")).alias("Values found"))
        .collect())

# Data-driven key list and key -> Silver column map
DETECTED_DICT_KEYS = {attr: sorted(r["Key"] for r in _inv if r["Attribute"] == attr) for attr in DICT_DF_MAP}
DICT_KEY_TO_COL    = {attr: {r["Key"]: r["Silver column"] for r in _inv if r["Attribute"] == attr} for attr in DICT_DF_MAP}

# ---- 1) Key inventory -------------------------------------------------
print("1) Every key found in the dict text, and its Silver column")
print("   Purpose: the keys come from the data itself; 'In Silver DDL' must be 'yes' for every key.")
display(spark.createDataFrame(
    [(r["DataFrame"], r["Key"], ", ".join(r["Written as"]), r["Silver column"],
      "yes" if r["Silver column"] in DICT_KEY_COLS[r["Attribute"]] else "NO",
      int(r["Dicts with this key"]), ", ".join(r["Values found"])) for r in _inv],
    ["DataFrame", "Key", "Written as", "Silver column", "In Silver DDL", "Dicts with this key", "Values found"])
    .orderBy("DataFrame", "Key"))

# ---- 2) Detected keys vs Silver DDL -----------------------------------
_cmp = []
for attr, (var, tbl) in DICT_DF_MAP.items():
    _found = set(DICT_KEY_TO_COL[attr].values())
    _ddl   = DICT_KEY_COLS[attr]
    _cmp.append((var, tbl, len(_found), len(_ddl),
                 ", ".join(sorted(_found - set(_ddl))), ", ".join(c for c in _ddl if c not in _found)))
print("2) Detected keys vs Silver DDL columns")
print("   Purpose: 'Keys not in DDL' must be empty; 'DDL columns never found' is allowed (they stay NULL).")
display(spark.createDataFrame(_cmp, ["DataFrame", "Silver table", "Keys found", "DDL key columns",
                                     "Keys not in DDL", "DDL columns never found"]))

# ---- Fail-fast --------------------------------------------------------
_not_in_ddl = [(c[0], c[4]) for c in _cmp if c[4]]
assert not _not_in_ddl, f"Keys with no Silver column: {_not_in_ddl}"
_bad_vals = sorted({v for r in _inv for v in r["Values found"]} - set(DICT_VALUE_TOKENS))
assert not _bad_vals, f"Unexpected dict values: {_bad_vals}"

print(f"✅ {sum(len(v) for v in DETECTED_DICT_KEYS.values())} distinct keys detected across 7 dicts; "
      f"all map to a Silver DDL column; values are only {DICT_VALUE_TOKENS}")

# COMMAND ----------

# DBTITLE 1,Reshape the 7 Dict DataFrames to Wide (Silver Schema)
# ============================================================
# 🔧 Reshape the 7 Dict DataFrames to Wide (Silver Schema)
# ============================================================
# Every dict DataFrame becomes WIDE: one row per business, one column per
# Silver DDL key column (DDL order), plus raw_dict kept for lineage
# (needed by DQ-002 / DQ-006; dropped before the Silver write).
#   * keys are cleaned with a regex: u'casual' / 'casual' -> casual, dairy-free -> dairy_free
#   * values are kept AS WRITTEN ('True' / 'False' / 'None', STRING); typing is done later
#   * a key that is NOT written in the dict -> NULL
#   * no dict at all (bronze NULL, 'None', '{}') -> every key column NULL
# Fail-fast: an unreadable pair, a key outside the DDL, a repeated key or an
# unknown value stops the cell. Rule R-1: no business is dropped.

# Keep a reference to the 'before' version, only so the next cell can compare before vs after.
_before_wide_dicts = {var: globals()[var] for var, _ in DICT_DF_MAP.values()}

def _to_wide(df, attr):
    raw     = F.col("raw_dict")
    written = F.regexp_extract_all(raw, F.lit(DICT_PAIR_RE), 1)                              # e.g. u'dairy-free'
    cols    = F.transform(written, lambda k: F.regexp_replace(F.regexp_replace(k, DICT_KEY_CLEAN_RE, ""), "-", "_"))
    vals    = F.transform(F.regexp_extract_all(raw, F.lit(DICT_PAIR_RE), 2), lambda v: F.trim(v))
    ddl     = F.array(*[F.lit(c) for c in DICT_KEY_COLS[attr]])
    kv = (F.when(raw.isNotNull() & ((F.size(F.split(raw, ":")) - 1) != F.size(written)),
                 F.raise_error(F.concat(F.lit(f"WIDE: unreadable pair in {attr}: "), raw)))
           .when(F.size(F.array_except(cols, ddl)) > 0,
                 F.raise_error(F.concat(F.lit(f"WIDE: key not in Silver DDL in {attr}: "), raw)))
           .when(F.size(F.array_distinct(cols)) != F.size(cols),
                 F.raise_error(F.concat(F.lit(f"WIDE: repeated key in {attr}: "), raw)))
           .when(F.exists(vals, lambda v: ~v.isin(DICT_VALUE_TOKENS)),
                 F.raise_error(F.concat(F.lit(f"WIDE: unexpected value in {attr}: "), raw)))
           .otherwise(F.map_from_arrays(cols, vals)))
    return (df.withColumn("_kv", kv)
              .select("business_id",
                      *[F.element_at("_kv", F.lit(c)).alias(c) for c in DICT_KEY_COLS[attr]],   # absent key -> NULL
                      "raw_dict"))

df_ambience             = _to_wide(df_ambience,             "Ambience")
df_best_nights          = _to_wide(df_best_nights,          "BestNights")
df_business_parking     = _to_wide(df_business_parking,     "BusinessParking")
df_dietary_restrictions = _to_wide(df_dietary_restrictions, "DietaryRestrictions")
df_good_for_meal        = _to_wide(df_good_for_meal,        "GoodForMeal")
df_hair_specializes_in  = _to_wide(df_hair_specializes_in,  "HairSpecializesIn")
df_music                = _to_wide(df_music,                "Music")

# Count once so that a fail-fast error is raised here, not in a later cell.
print("🔧 7 dict DataFrames are now wide (business_id + one column per Silver key + raw_dict); no business dropped")
for attr, (var, tbl) in DICT_DF_MAP.items():
    _b, _a = _before_wide_dicts[var], globals()[var]
    _nb, _na, _nbiz = _b.count(), _a.count(), _a.select("business_id").distinct().count()
    _n_pairs = F.size(F.regexp_extract_all("raw_dict", F.lit(DICT_PAIR_RE), 1))
    _r = _b.agg(F.sum(F.when(_n_pairs > 0, 1).otherwise(0)).alias("info"),
                F.sum(F.when((_n_pairs > 0) & (_n_pairs < len(DICT_KEY_COLS[attr])), 1).otherwise(0)).alias("partial")).first()
    print(f"   {var:<24}: {_nb:>9,} → {_na:>9,} rows | {len(DICT_KEY_COLS[attr])} key columns"
          f" | dicts with keys: {int(_r['info'] or 0):,} (of which with missing keys → NULL: {int(_r['partial'] or 0):,})")
    assert _nb == _na == _nbiz, f"{var}: a business was dropped or repeated ({_nb} -> {_na}, {_nbiz} distinct)"

# COMMAND ----------

# DBTITLE 1,The 7 Wide Dict DataFrames — What You Are Working With
# ============================================================
# 👀 The 7 Wide Dict DataFrames — What You Are Working With
# ============================================================
# These 7 Python variables are the working DataFrames for the dict cleaning.
# Each one: 1 row per business (150,346)
#   business_id | one STRING column per Silver DDL key (DDL order) | raw_dict (lineage only)
#   values are as written: 'True' / 'False' / 'None'; a key absent from the dict -> NULL
# The 5 sample rows are businesses that actually have the dict (raw_dict IS NOT NULL).

_has_dict = F.col("raw_dict").isNotNull()

print("=" * 100)
print("df_ambience  →  yelp_business_ambience")
print("=" * 100)
df_ambience.printSchema()
display(df_ambience.where(_has_dict).limit(5))

print("=" * 100)
print("df_best_nights  →  yelp_business_best_nights")
print("=" * 100)
df_best_nights.printSchema()
display(df_best_nights.where(_has_dict).limit(5))

print("=" * 100)
print("df_business_parking  →  yelp_business_parking")
print("=" * 100)
df_business_parking.printSchema()
display(df_business_parking.where(_has_dict).limit(5))

print("=" * 100)
print("df_dietary_restrictions  →  yelp_business_dietary_restrictions")
print("=" * 100)
df_dietary_restrictions.printSchema()
display(df_dietary_restrictions.where(_has_dict).limit(5))

print("=" * 100)
print("df_good_for_meal  →  yelp_business_good_for_meal")
print("=" * 100)
df_good_for_meal.printSchema()
display(df_good_for_meal.where(_has_dict).limit(5))

print("=" * 100)
print("df_hair_specializes_in  →  yelp_business_hair_specializes_in")
print("=" * 100)
df_hair_specializes_in.printSchema()
display(df_hair_specializes_in.where(_has_dict).limit(5))

print("=" * 100)
print("df_music  →  yelp_business_music")
print("=" * 100)
df_music.printSchema()
display(df_music.where(_has_dict).limit(5))

# COMMAND ----------

# DBTITLE 1,Audit the Working Split
# ============================================================
# ✅ Audit the Working Split
# ============================================================
# Validate the in-memory split before the issue-by-issue cells
# start mutating the working DataFrames.

split_summary = spark.createDataFrame([
    ("df_business", int(df_business.count()), "1 row per business"),
    ("df_attributes", int(df_attributes.count()), "1 row per business"),
    ("df_hours", int(df_hours.count()), "1 row per business"),
    ("df_categories", int(df_categories.count()), "1 row per category token after the initial split"),
] + [
    (var, int(globals()[var].count()), "1 row per business (wide: one STRING column per Silver key + raw_dict; absent key = NULL)")
    for var, _ in DICT_DF_MAP.values()
], ["working_df", "rows", "grain"])

display(split_summary)

# Basic integrity checks on the split
assert df_business.count() == bronze_count, "df_business should retain one row per bronze business"
assert df_attributes.count() == bronze_count, "df_attributes should retain one row per bronze business"
assert df_hours.count() == bronze_count, "df_hours should retain one row per bronze business"
assert df_business.select("business_id").distinct().count() == bronze_count, "business_id must remain unique in df_business"
assert df_business.filter(F.col("business_id").isNull()).count() == 0, "business_id cannot be null"
# Attribute lists must cover every bronze attributes field exactly once (32 single-value + 7 dicts = 39)
_bronze_attr_fields = set(df_bronze.schema["attributes"].dataType.fieldNames())
_listed = set(SINGLE_ATTRS) | set(DICT_ATTRS)
assert _listed == _bronze_attr_fields, f"Attribute lists differ from bronze: missing {_bronze_attr_fields - _listed}, extra {_listed - _bronze_attr_fields}"
assert len(SINGLE_ATTRS) == 32 and len(df_attributes.columns) == 33, "df_attributes must hold business_id + 32 single-value attributes"

# Each dict DataFrame: its non-null dicts must match bronze
# (rows = bronze rows and unique business_id are already asserted in the Reshape cell)
_exp = df_bronze.agg(*[F.count(f"attributes.{a}").alias(a) for a in DICT_DF_MAP]).first().asDict()
for attr, (var, _) in DICT_DF_MAP.items():
    n_non_null = globals()[var].where(F.col("raw_dict").isNotNull()).count()
    assert n_non_null == _exp[attr], f"{var}: {n_non_null} non-null dicts vs {_exp[attr]} in bronze"

print("🔍 Working split validation passed")
print(f"   Parent rows:           {df_business.count():>12,}")
print(f"   Category tokens:       {df_categories.count():>12,}")
print(f"   In-memory quarantine:  {df_quarantine_business.count():>12,}")
print("\nNext step: add per-issue metadata, issue-query, solution, and solved-query cells on top of these working DataFrames.")

# COMMAND ----------

# DBTITLE 1,Quick Preview — 5 Rows from Each Working DataFrame
# ============================================================
# 👀 Quick Preview — 5 Rows from Each Working DataFrame
# ============================================================

print("=" * 80)
print("df_business (parent)")
print("=" * 80)
display(df_business.limit(5))

print("=" * 80)
print("df_attributes")
print("=" * 80)
display(df_attributes.limit(5))

print("=" * 80)
print("df_hours")
print("=" * 80)
display(df_hours.limit(5))

print("=" * 80)
print("df_categories")
print("=" * 80)
display(df_categories.limit(5))

# The 7 dict DataFrames are previewed in the 'The 7 Wide Dict DataFrames' cell above.

print("=" * 80)
print("df_quarantine_business")
print("=" * 80)
display(df_quarantine_business.limit(5))

# COMMAND ----------

# DBTITLE 1,All Working DataFrames — 5 Rows Each
# ============================================================
# 📋 All Working DataFrames — 5 Rows Each
# ============================================================
# Every working DataFrame built so far (parent, level-1 children,
# 7 wide dict DataFrames, quarantine), shown in pipeline order.

_all_working_dfs = [
    ("df_business",             df_business),
    ("df_attributes",           df_attributes),
    ("df_hours",                df_hours),
    ("df_categories",           df_categories),
    ("df_ambience",             df_ambience),
    ("df_best_nights",          df_best_nights),
    ("df_business_parking",     df_business_parking),
    ("df_dietary_restrictions", df_dietary_restrictions),
    ("df_good_for_meal",        df_good_for_meal),
    ("df_hair_specializes_in",  df_hair_specializes_in),
    ("df_music",                df_music),
    ("df_quarantine_business",  df_quarantine_business),
]

for _name, _df in _all_working_dfs:
    print("=" * 80)
    print(f"{_name}  ({len(_df.columns)} columns)")
    print("=" * 80)
    display(_df.limit(5))

# COMMAND ----------

# DBTITLE 1,VI-00 · Value Inventory — Purpose & Numbering
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📚 VI-00 · Value Inventory (Detection) — Every Unique Value in Every Column
# MAGIC
# MAGIC **Numbering used from here on** — detection and correction share the same number per DataFrame:
# MAGIC
# MAGIC | Code | Meaning | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `VI-nn` | **V**alue **I**nventory (detection, read-only) for DataFrame `nn` | `VI-01` = `df_business` |
# MAGIC | `MS-nn` | **M**apping & **S**tandardization (correction) for the same DataFrame | `MS-01` = `df_business` |
# MAGIC | `MS-nn.k` | Column / issue step `k` inside that DataFrame | `MS-01.1` = `df_business.name` |
# MAGIC | `· Explain / Detect / Fix / Verify` | The 4 cells of every step | `MS-01.1 · name · Fix` |
# MAGIC
# MAGIC | nn | DataFrame | nn | DataFrame |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 01 | `df_business` | 07 | `df_business_parking` |
# MAGIC | 02 | `df_attributes` | 08 | `df_dietary_restrictions` |
# MAGIC | 03 | `df_hours` | 09 | `df_good_for_meal` |
# MAGIC | 04 | `df_categories` | 10 | `df_hair_specializes_in` |
# MAGIC | 05 | `df_ambience` | 11 | `df_music` |
# MAGIC | 06 | `df_best_nights` | 12 | `df_quarantine_business` |
# MAGIC
# MAGIC **What we do:** cells `VI-01` … `VI-12` list (one cell per DataFrame) the unique values of every column in all working DataFrames (`df_business`, `df_attributes`, `df_hours`, `df_categories`, the 7 dict DataFrames and `df_quarantine_business`), with the number of rows for each value.
# MAGIC
# MAGIC **Why:** before fixing anything, we need to see exactly how each value is written in the data. This shows which values need a mapping rule, for example:
# MAGIC
# MAGIC | Value found | Mapping needed |
# MAGIC | --- | --- |
# MAGIC | `'None'` (text) | → SQL `NULL` |
# MAGIC | `u'free'` and `'free'` | → `free` |
# MAGIC | `allages`, `21plus` | → `all_ages`, `21_plus` |
# MAGIC | `'none'`, `'no'` in enum attributes | → `NULL` / `false` |
# MAGIC
# MAGIC The cell only reads the data — nothing is changed. A real SQL `NULL` is shown as `(SQL NULL)`, and values are also shown inside `[ ]` so hidden spaces are visible. Identifier and free-text columns (`business_id`, `name`, `address`, ...) show only their 25 most frequent values.

# COMMAND ----------

# DBTITLE 1,VI-01 · Value Inventory — df_business
# ============================================================
# 📚 VI-01 · Value Inventory — df_business
# ============================================================
# Read-only: lists the unique values of every df_business column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_business"
_df      = df_business
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-02 · Value Inventory — df_attributes
# ============================================================
# 📚 VI-02 · Value Inventory — df_attributes
# ============================================================
# Read-only: lists the unique values of every df_attributes column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_attributes"
_df      = df_attributes
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-03 · Value Inventory — df_hours
# ============================================================
# 📚 VI-03 · Value Inventory — df_hours
# ============================================================
# Read-only: lists the unique values of every df_hours column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_hours"
_df      = df_hours
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-04 · Value Inventory — df_categories
# ============================================================
# 📚 VI-04 · Value Inventory — df_categories
# ============================================================
# Read-only: lists the unique values of every df_categories column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the FK to the parent, so it is only counted, not listed

_df_name = "df_categories"
_df      = df_categories
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-05 · Value Inventory — df_ambience
# ============================================================
# 📚 VI-05 · Value Inventory — df_ambience
# ============================================================
# Read-only: lists the unique values of every df_ambience column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_ambience"
_df      = df_ambience
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-06 · Value Inventory — df_best_nights
# ============================================================
# 📚 VI-06 · Value Inventory — df_best_nights
# ============================================================
# Read-only: lists the unique values of every df_best_nights column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_best_nights"
_df      = df_best_nights
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-07 · Value Inventory — df_business_parking
# ============================================================
# 📚 VI-07 · Value Inventory — df_business_parking
# ============================================================
# Read-only: lists the unique values of every df_business_parking column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_business_parking"
_df      = df_business_parking
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-08 · Value Inventory — df_dietary_restrictions
# ============================================================
# 📚 VI-08 · Value Inventory — df_dietary_restrictions
# ============================================================
# Read-only: lists the unique values of every df_dietary_restrictions column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_dietary_restrictions"
_df      = df_dietary_restrictions
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-09 · Value Inventory — df_good_for_meal
# ============================================================
# 📚 VI-09 · Value Inventory — df_good_for_meal
# ============================================================
# Read-only: lists the unique values of every df_good_for_meal column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_good_for_meal"
_df      = df_good_for_meal
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-10 · Value Inventory — df_hair_specializes_in
# ============================================================
# 📚 VI-10 · Value Inventory — df_hair_specializes_in
# ============================================================
# Read-only: lists the unique values of every df_hair_specializes_in column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_hair_specializes_in"
_df      = df_hair_specializes_in
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-11 · Value Inventory — df_music
# ============================================================
# 📚 VI-11 · Value Inventory — df_music
# ============================================================
# Read-only: lists the unique values of every df_music column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key (one row per business), so it is only counted, not listed

_df_name = "df_music"
_df      = df_music
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,VI-12 · Value Inventory — df_quarantine_business
# ============================================================
# 📚 VI-12 · Value Inventory — df_quarantine_business
# ============================================================
# Read-only: lists the unique values of every df_quarantine_business column, with row counts.
#   * values are read as text, exactly as stored (no trim, no lower-case)
#   * a real SQL NULL is shown as (SQL NULL); the text 'None' stays 'None'
#   * 'Value in brackets' wraps the value in [ ] so leading/trailing spaces are visible
#   * business_id is the key, so it is only counted, not listed
#   * the quarantine is filled later by DQ-009 / DQ-010, so it may still be empty here

_df_name = "df_quarantine_business"
_df      = df_quarantine_business
_cols    = _df.columns          # read once (avoids one schema call per column)
_skip    = ["business_id"]      # key column: unique per row, not useful for mapping

print(f"{_df_name}: {_df.count():,} rows")

# One row per (column, value) with its row count
_long = _df.select(F.explode(F.array(*[
    F.struct(F.lit(i).alias("col_pos"), F.lit(c).alias("Column"), F.col(c).cast("string").alias("val"))
    for i, c in enumerate(_cols)
])).alias("kv")).select("kv.*")

_values = (_long.groupBy("col_pos", "Column", "val").count()
    .select("col_pos", "Column",
            F.coalesce("val", F.lit("(SQL NULL)")).alias("Value"),
            F.when(F.col("val").isNull(), F.lit("(SQL NULL)"))
             .otherwise(F.concat(F.lit("["), F.col("val"), F.lit("]"))).alias("Value in brackets"),
            F.col("count").alias("Rows")))

# ---- 1) Overview: one row per column ----------------------------------
print(f"1) {_df_name} — overview, one row per column")
print("   Purpose: how many unique values each column has and how many rows are SQL NULL.")
display(_values.groupBy("col_pos", "Column")
        .agg(F.count(F.lit(1)).alias("Unique values"),
             F.sum("Rows").alias("Total rows"),
             F.sum(F.when(F.col("Value") == "(SQL NULL)", F.col("Rows")).otherwise(0)).alias("SQL NULL rows"))
        .orderBy("col_pos").drop("col_pos"))

# ---- 2) Every unique value, one table per column ------------------------
for n, c in enumerate([c for c in _cols if c not in _skip], start=2):
    print("=" * 80)
    print(f"{n}) {_df_name}.{c} — every unique value")
    print("   Purpose: see how each value is written, to decide which values need a mapping rule.")
    print("=" * 80)
    display(_values.where(F.col("Column") == c)
            .orderBy("Value")
            .select("Value", "Value in brackets", "Rows"))


# COMMAND ----------

# DBTITLE 1,MS-01 · df_business — Mapping & Standardization
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🧭 MS-01 · `df_business` — Mapping & Standardization (Correction)
# MAGIC
# MAGIC The next cells fix the **mapping and standardization issues** found in the `df_business` value inventory (`VI-01`): values that mean the same thing but are written differently, masked NULLs, and storage types that do not match the Silver contract.
# MAGIC
# MAGIC **Approach — one column at a time:** all issues of one column are fixed and verified before moving to the next column (order below). For each column: **Explain → Detect → Fix → Verify** (cell titles `MS-01.k · <column> · Explain / Detect / Fix / Verify`). Fixes are applied to `df_business` in place. No row is deleted.
# MAGIC
# MAGIC > 📄 **Detailed findings:** `00_Docs/06_Data_Quality_Issues_and_Solutions_Document/01_Business_Value_Inventory_Findings.md` (section 3.1 *df_business*) — every value, row count and the DQ issue that owns the fix.
# MAGIC
# MAGIC | Step ID | Column | Issues found (rows) | Mapping / fix | DQ issue |
# MAGIC | ---: | --- | --- | --- | --- |
# MAGIC | `MS-01.1` | `name` | invisible chars (15), repeated spaces (263), trailing `,` (1), outer spaces (150) | remove invisible chars → collapse spaces → strip end separator → `trim()` | DQ-017 / 029 / 004 |
# MAGIC | `MS-01.2` | `address` | invisible chars (10), repeated spaces (245), trailing `,` (6), outer spaces (3), empty `''` (5,127), `'.'` (1) | same text steps, then `''` / `'.'` → NULL | DQ-017 / 029 / 004 / 032 / 019 |
# MAGIC | `MS-01.3` | `city` | invisible chars (4), repeated spaces (9), trailing `,` (17), outer spaces (49), embedded state / qualifiers (`Tampa,Fl`, `Philadelphia PA`, `… (balance)`, ~32) | same text steps, then rules + reviewed map | DQ-017 / 029 / 004 / 018 |
# MAGIC | `MS-01.4` | `state` | `XMS` (1), real states outside the Yelp footprint (20) | quarantine / correct from ZIP | DQ-009 / 010 |
# MAGIC | `MS-01.5` | `postal_code` | empty `''` (73), malformed (5: `T6x 2H4`, `T6T 0G`, `336140`, …), FSA only (196) | `''` → NULL; upper-case / truncate to FSA / NULL; FSA kept as-is | DQ-015 / 016 / 014 |
# MAGIC | `MS-01.6` | `stars` | stored as DOUBLE (150,346) | → DECIMAL(2,1) | DQ-001 |
# MAGIC | `MS-01.7` | `is_open` | stored as BIGINT 0/1 (150,346) | → BOOLEAN | DQ-001 |
# MAGIC
# MAGIC **Not fixed here (Silver Enriched):** one city written in many spellings or casings (`St. Louis` / `Saint Louis` / `ST LOUIS`, 154 groups, DQ-021 / 030) and the misspelled cities found in the inventory (`Tuscon`, `St. Loius`, `St Petersberg`, … — new item **N1**, pending decision). Raw values are kept unchanged.
# MAGIC
# MAGIC **Order inside a text column matters:** invisible characters → repeated spaces / separators → trim → empty string to NULL. Cleaning text first makes sure values such as `' '` or `'\u200b'` also end up as NULL.

# COMMAND ----------

# DBTITLE 1,MS-01.1 · name · Explain
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 MS-01.1 · `df_business.name` — Text Hygiene (DQ-BUSINESS-017 / 029 / 004)
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **DQ Issues** | `DQ-BUSINESS-017` Invisible Unicode characters · `DQ-BUSINESS-029` Repeated spaces and dangling separators · `DQ-BUSINESS-004` Leading / trailing whitespace |
# MAGIC | **Column** | `df_business.name` |
# MAGIC | **Rows affected** | invisible chars **15** (5 × U+200B zero-width space, 10 × U+00A0 no-break space) · repeated spaces **263** · trailing `,` **1** · leading / trailing spaces **150** |
# MAGIC | **Examples** | `'The Bug \u200bWhisperer'`, `"Mamita's\xa0Hot Tamales"`, `'PAWS  Plaquemines Animal Welfare Society'`, `"Mom's Kitchen,"`, `'The Mad Griddle '` |
# MAGIC | **Details** | `01_Business_Value_Inventory_Findings.md` → section 3.1 (`name`) |
# MAGIC
# MAGIC **The problem in plain words:** some business names contain characters you cannot see (zero-width space, no-break space), double spaces, a comma at the end, or spaces at the start / end. They look the same as the clean name on screen, but `'Starbucks '` and `'Starbucks'` are different values for SQL, so grouping, joins and duplicate checks give wrong results.
# MAGIC
# MAGIC **Suggested solutions (DQ document):**
# MAGIC
# MAGIC | # | Solution | Comment |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | One ordered text-hygiene step in Silver Cleaned (remove invisible chars → collapse spaces → strip dangling separator → `trim()`) | Smallest change, meaning is not changed |
# MAGIC | 2 | Keep raw `name` and add a `name_clean` column | Adds a column the DDL does not have |
# MAGIC | 3 | Leave as-is, clean at query time | Every consumer repeats the logic |
# MAGIC
# MAGIC **Solution taken:** **Option 1**, in this order:
# MAGIC
# MAGIC 1. U+200B / U+200C / U+200D / U+FEFF (zero-width) → removed; U+00A0 (no-break space) and tab → normal space
# MAGIC 2. 2 or more spaces → 1 space
# MAGIC 3. `,` / `;` at the end of the name → removed
# MAGIC 4. `trim()` leading / trailing spaces
# MAGIC 5. If nothing is left (empty string) → SQL `NULL` (safety rule R-2; no such row exists today)
# MAGIC
# MAGIC **Not done here:** no change of upper / lower case, no removal of accents or other valid non-ASCII letters (`Café`, `Pho`), no brand-spelling merge (`AutoZone` / `Auto Zone`, DQ-022) and no removal of status text in names (DQ-058) — those change the meaning or belong to Silver Enriched.
# MAGIC
# MAGIC **Why this is best:** the fix only removes characters that carry no meaning, so the name a person reads stays the same, while exact matching, grouping and dedup start working. It needs no new column and no row is deleted.

# COMMAND ----------

# DBTITLE 1,MS-01.1 · name · Detect
# ============================================================
# 🔎 MS-01.1 · name · Detect — df_business.name
# ============================================================
# Read-only. Shows real rows for each name problem, then the counts.
# 'Name made visible' replaces hidden characters with a readable tag:
#   ⟨ZWSP⟩ = zero-width space (U+200B), ⟨NBSP⟩ = no-break space (U+00A0), ⟨TAB⟩ = tab
# Normal spaces (U+0020) are part of the name and are shown as they are (not tagged).
# Section 3 lists the names that contain a REAL middle dot '·' (U+00B7), for review.

from functools import reduce

ZERO_WIDTH_RE = "[\u200b\u200c\u200d\ufeff]"   # remove
SPACE_LIKE_RE = "[\u00a0\t]"                    # replace with a normal space

def _visible(col):
    """Make hidden characters visible (display only)."""
    c = F.regexp_replace(col, "\u200b", "⟨ZWSP⟩")
    c = F.regexp_replace(c, "\u00a0", "⟨NBSP⟩")
    c = F.regexp_replace(c, "[\u200c\u200d\ufeff]", "⟨ZW⟩")
    return F.regexp_replace(c, "\t", "⟨TAB⟩")

_n = F.col("name")
NAME_CHECKS = [
    ("Invisible characters (U+200B / U+00A0 / ...)", _n.rlike(f"{ZERO_WIDTH_RE}|{SPACE_LIKE_RE}")),
    ("Repeated spaces",                              _n.rlike("  ")),
    ("Trailing , or ;",                              _n.rlike("[,;]\\s*$")),
    ("Leading / trailing spaces",                    _n != F.trim(_n)),
    ("Empty string",                                 _n == ""),
]

# ---- 1) Real rows per problem ------------------------------------------
for i, (label, cond) in enumerate(NAME_CHECKS, start=1):
    print("=" * 80)
    print(f"1.{i}) Sample rows — {label}")
    print("   Purpose: see the real names with this problem (hidden characters shown as tags).")
    print("=" * 80)
    display(df_business.where(cond)
            .select("business_id",
                    F.col("name").alias("Name (as stored)"),
                    _visible(_n).alias("Name made visible"),
                    "city", "state")
            .orderBy("name").limit(20))

# ---- 2) Counts --------------------------------------------------------
print("=" * 80)
print("2) How many names have each problem")
print("   Purpose: size of each problem; a name can have more than one problem.")
print("=" * 80)
_name_counts = df_business.select(
    *[F.sum(cond.cast("int")).alias(label) for label, cond in NAME_CHECKS])
_any_issue = reduce(lambda a, b: a | b, [cond for _, cond in NAME_CHECKS])
_name_issue_rows = df_business.where(_any_issue).count()

display(spark.createDataFrame(
    [(label, int(v)) for label, v in _name_counts.first().asDict().items()]
    + [("Rows with at least one problem", int(_name_issue_rows))],
    "Problem string, Rows long"))

print(f"Total names: {df_business.count():,} | SQL NULL names: {df_business.where(_n.isNull()).count():,}")

# ---- 3) Names with a real middle dot (U+00B7) -------------------------
print("=" * 80)
print("3) Names that contain a real middle dot '·' (U+00B7)")
print("   Purpose: check these by eye; the dot is part of the stored name (e.g. a brand), not a space marker.")
print("=" * 80)
display(df_business.where(_n.contains("\u00b7"))
        .select("business_id",
                F.col("name").alias("Name (as stored)"),
                F.regexp_replace(_n, "\u00b7", "⟨MIDDOT⟩").alias("Name with · tagged"),
                "city", "state")
        .orderBy("name"))


# COMMAND ----------

# DBTITLE 1,MS-01.1 · name · Fix
# ============================================================
# 🔧 MS-01.1 · name · Fix — df_business.name
# ============================================================
# Ordered text hygiene, applied to df_business in place. No row is deleted.
#   1. zero-width chars removed; no-break space / tab -> normal space   (DQ-017)
#   2. 2+ spaces -> 1 space                                             (DQ-029)
#   3. ',' / ';' at the end removed                                     (DQ-029)
#   4. trim()                                                           (DQ-004)
#   5. empty string -> SQL NULL                                         (rule R-2)
# The steps are idempotent: running the cell again changes nothing.

def clean_text(col):
    c = F.regexp_replace(col, ZERO_WIDTH_RE, "")
    c = F.regexp_replace(c, SPACE_LIKE_RE, " ")
    c = F.regexp_replace(c, " {2,}", " ")
    c = F.regexp_replace(c, "[\\s,;]+$", "")
    c = F.trim(c)
    return F.when(c == "", F.lit(None).cast("string")).otherwise(c)

# Before copy, kept only for the verify cell
_name_before = df_business.select("business_id", F.col("name").alias("name_before"))

df_business = df_business.withColumn("name", clean_text(F.col("name")))

_name_changed = (df_business.select("business_id", F.col("name").alias("name_after"))
                 .join(_name_before, "business_id")
                 .where(~F.col("name_before").eqNullSafe(F.col("name_after"))))
print(f"🔧 MS-01.1 df_business.name cleaned | names changed: {_name_changed.count():,} "
      f"(expected {_name_issue_rows:,} from the Detect cell) | rows: {df_business.count():,}")


# COMMAND ----------

# DBTITLE 1,MS-01.1 · name · Verify
# ============================================================
# ✅ MS-01.1 · name · Verify — df_business.name
# ============================================================
# Read-only checks:
#   * every problem from the Detect cell is now 0
#   * row count unchanged, no new NULL names
#   * only the expected names changed
#   * valid non-ASCII letters (accents etc.) were not removed

# ---- 1) Before / after for the changed names ---------------------------
print("1) Before / after — changed names")
print("   Purpose: see that only hidden characters, extra spaces and end separators were removed.")
display(_name_changed
        .select("business_id",
                _visible(F.col("name_before")).alias("Before (made visible)"),
                _visible(F.col("name_after")).alias("After (made visible)"))
        .orderBy("After (made visible)"))

# ---- 2) Checks ----------------------------------------------------------
_total_after   = df_business.count()
_null_after    = df_business.where(F.col("name").isNull()).count()
_null_before   = _name_before.where(F.col("name_before").isNull()).count()
_changed_rows  = _name_changed.count()
_left = {label: df_business.where(cond).count() for label, cond in NAME_CHECKS}

# Non-ASCII letters other than the removed hidden characters must be unchanged
_real_non_ascii = "[^\\x00-\\x7F\u200b\u200c\u200d\ufeff\u00a0]"
_na_before = _name_before.where(F.col("name_before").rlike(_real_non_ascii)).count()
_na_after  = df_business.where(F.col("name").rlike(_real_non_ascii)).count()

_checks = [(f"No names left with: {label}", 0, v) for label, v in _left.items()] + [
    ("Row count unchanged",                         _name_before.count(), _total_after),
    ("No new SQL NULL names",                       _null_before,         _null_after),
    ("Changed names = rows flagged in Detect cell",   _name_issue_rows,     _changed_rows),
    ("Names with real non-ASCII letters unchanged", _na_before,           _na_after),
]

print("2) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.1 (name) verification failed"
print("✅ MS-01.1 · df_business.name: all checks passed")


# COMMAND ----------

# DBTITLE 1,MS-01.2 · address · Explain
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 MS-01.2 · `df_business.address` — Text Hygiene + Masked NULLs (DQ-BUSINESS-017 / 029 / 004 / 032 / 019)
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **DQ Issues** | `DQ-BUSINESS-017` Invisible Unicode characters · `DQ-BUSINESS-029` Repeated spaces and dangling separators · `DQ-BUSINESS-004` Leading / trailing whitespace · `DQ-BUSINESS-032` Empty-string address (masked NULL) · `DQ-BUSINESS-019` Degenerate street addresses (no street name) |
# MAGIC | **Column** | `df_business.address` |
# MAGIC | **Rows affected** | invisible chars **10** (1 × U+200B, 9 × U+00A0) · repeated spaces **245** · trailing `,` **6** · leading / trailing spaces **3** · empty `''` **5,127** · no street name **11** (6 house number only, 4 ZIP+4, 1 `'.'`) |
# MAGIC | **Examples** | `'8438 N Lindbergh\xa0Blvd'`, `'10303  108 Street NW'`, `'2810 Limekiln Pike,'`, `'150 2nd Ave N, 970,'`, `''`, `'3070'`, `'63088-1828'`, `'.'` |
# MAGIC | **Details** | `01_Business_Value_Inventory_Findings.md` → section 3.1 (`address`) |
# MAGIC
# MAGIC **The problem in plain words:** like `name`, some addresses have hidden characters, double spaces, a comma at the end or spaces at the edges. On top of that, **5,127 businesses have an empty text `''` instead of a real NULL**, so `count(address)` says every business has an address. Another **11 addresses contain no street name at all** — only a house number (`3070`), the business's own ZIP+4 typed into the wrong field (`63088-1828`) or a placeholder dot (`.`). None of these can locate the business.
# MAGIC
# MAGIC **Suggested solutions (DQ document):**
# MAGIC
# MAGIC | # | Solution | Comment |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Same ordered text hygiene as `name`, then `''` / no-street-name values → SQL NULL | One NULL representation; nothing invented |
# MAGIC | 2 | Option 1 plus a derived `is_service_area_business` flag (DQ-032) | Useful, but adds a column the DDL does not have |
# MAGIC | 3 | Move the ZIP+4 extension into `postal_code` (DQ-019) | Only 4 rows would get a different postal-code format |
# MAGIC | 4 | Reverse-geocode an address from the coordinates | Invents a storefront that many of these (mobile) businesses do not have |
# MAGIC | 5 | Drop the rows | ❌ Rejected — cleaning never deletes rows (rule R-1) |
# MAGIC
# MAGIC **Solution taken:** **Option 1**, in this order:
# MAGIC
# MAGIC 1. Text hygiene with the same `clean_text()` as `MS-01.1`: zero-width chars removed, no-break space / tab → space, 2+ spaces → 1, `,` / `;` at the end removed, `trim()`, empty → NULL
# MAGIC 2. An address with **no letter at all** after step 1 (`3070`, `63088-1828`, `.`) → SQL `NULL`
# MAGIC
# MAGIC **Not done here:** no service-area flag (DQ-032, no new columns), no USPS abbreviation standardisation (`Street` → `St`, DQ-031), no change of upper / lower case (DQ-030), no reverse geocoding. Addresses without a house number (malls, landmarks like `Citizens Bank Park`) are valid and kept.
# MAGIC
# MAGIC **Why this is best:** missing addresses get one honest representation (SQL `NULL`), so completeness metrics become correct, and the values that remain are real street addresses. The postal code and coordinates of these rows are untouched, so no information is lost. No row is deleted.

# COMMAND ----------

# DBTITLE 1,MS-01.2 · address · Detect
# ============================================================
# 🔎 MS-01.2 · address · Detect — df_business.address
# ============================================================
# Read-only. Shows real rows for each address problem, then the counts.
# Uses ZERO_WIDTH_RE / SPACE_LIKE_RE / _visible() from MS-01.1 · Detect.
#   'Address made visible'  : hidden characters shown as ⟨ZWSP⟩ / ⟨NBSP⟩ / ⟨TAB⟩ (normal spaces untouched)
#   'Address in brackets'   : value wrapped in [ ] so leading / trailing spaces can be seen

_a = F.col("address")
_a_trim = F.trim(F.regexp_replace(F.regexp_replace(_a, ZERO_WIDTH_RE, ""), SPACE_LIKE_RE, " "))

ADDRESS_CHECKS = [
    ("Invisible characters (U+200B / U+00A0 / ...)", _a.rlike(f"{ZERO_WIDTH_RE}|{SPACE_LIKE_RE}")),
    ("Repeated spaces",                              _a.rlike("  ")),
    ("Trailing , or ;",                              _a.rlike("[,;]\\s*$")),
    ("Leading / trailing spaces",                    _a != F.trim(_a)),
    ("Empty string ''",                              _a_trim == ""),
    ("No street name (no letter at all)",            (_a_trim != "") & ~_a_trim.rlike("[A-Za-z]")),
]

# ---- 1) Real rows per problem ------------------------------------------
for i, (label, cond) in enumerate(ADDRESS_CHECKS, start=1):
    print("=" * 80)
    print(f"1.{i}) Sample rows — {label}")
    print("   Purpose: see the real addresses with this problem.")
    print("=" * 80)
    display(df_business.where(cond)
            .select("business_id", "name",
                    F.col("address").alias("Address (as stored)"),
                    _visible(_a).alias("Address made visible"),
                    F.concat(F.lit("["), _a, F.lit("]")).alias("Address in brackets"),
                    "city", "state", "postal_code")
            .orderBy("address").limit(20))

# ---- 2) Counts --------------------------------------------------------
print("=" * 80)
print("2) How many addresses have each problem")
print("   Purpose: size of each problem; an address can have more than one problem.")
print("=" * 80)
_addr_counts = df_business.select(
    *[F.sum(cond.cast("int")).alias(label) for label, cond in ADDRESS_CHECKS]).first().asDict()
_addr_any_issue  = reduce(lambda a, b: a | b, [cond for _, cond in ADDRESS_CHECKS])
_addr_issue_rows = df_business.where(_addr_any_issue).count()
# Rows that must end up as SQL NULL after the fix
_addr_to_null    = _addr_counts["Empty string ''"] + _addr_counts["No street name (no letter at all)"]

display(spark.createDataFrame(
    [(label, int(v)) for label, v in _addr_counts.items()]
    + [("Rows with at least one problem", int(_addr_issue_rows)),
       ("Rows that will become SQL NULL", int(_addr_to_null))],
    "Problem string, Rows long"))

print(f"Total addresses: {df_business.count():,} | SQL NULL addresses: {df_business.where(_a.isNull()).count():,}")

# ---- 3) All no-street-name values ---------------------------------------
print("=" * 80)
print("3) Every address with no street name (will become NULL)")
print("   Purpose: check by eye that none of them is a real address; ZIP+4 values repeat the row's postal_code.")
print("=" * 80)
display(df_business.where(ADDRESS_CHECKS[5][1])
        .select("business_id", "name", F.col("address").alias("Address (as stored)"),
                "postal_code", "city", "state")
        .orderBy("address"))


# COMMAND ----------

# DBTITLE 1,MS-01.2 · address · Fix
# ============================================================
# 🔧 MS-01.2 · address · Fix — df_business.address
# ============================================================
# Applied to df_business in place. No row is deleted.
#   1. clean_text() from MS-01.1 · Fix:
#        zero-width chars removed; no-break space / tab -> space   (DQ-017)
#        2+ spaces -> 1 space; ',' / ';' at the end removed        (DQ-029)
#        trim(); empty string -> SQL NULL                          (DQ-004 / DQ-032)
#   2. no letter at all ('3070', '63088-1828', '.') -> SQL NULL    (DQ-019)
# Idempotent: running the cell again changes nothing.

def clean_address(col):
    c = clean_text(col)
    return F.when(~c.rlike("[A-Za-z]"), F.lit(None).cast("string")).otherwise(c)

# Before copy, kept only for the verify cell
_addr_before = df_business.select("business_id", F.col("address").alias("address_before"))

df_business = df_business.withColumn("address", clean_address(F.col("address")))

_addr_changed = (df_business.select("business_id", F.col("address").alias("address_after"))
                 .join(_addr_before, "business_id")
                 .where(~F.col("address_before").eqNullSafe(F.col("address_after"))))
print(f"🔧 MS-01.2 df_business.address cleaned | addresses changed: {_addr_changed.count():,} "
      f"(expected {_addr_issue_rows:,} from the Detect cell) | "
      f"now SQL NULL: {df_business.where(F.col('address').isNull()).count():,} "
      f"(expected {_addr_to_null:,}) | rows: {df_business.count():,}")


# COMMAND ----------

# DBTITLE 1,MS-01.2 · address · Verify
# ============================================================
# ✅ MS-01.2 · address · Verify — df_business.address
# ============================================================
# Read-only checks:
#   * every problem from the Detect cell is now 0
#   * row count unchanged
#   * SQL NULL addresses = empty + no-street-name rows from the Detect cell
#   * only the expected addresses changed
#   * valid non-ASCII letters were not removed

# ---- 1) Before / after for the changed addresses (excluding '' -> NULL) --
print("1) Before / after — changed addresses (the 5,127 '' -> NULL rows are counted in section 2, not listed)")
print("   Purpose: see that only hidden characters, extra spaces, end separators and no-street-name values changed.")
display(_addr_changed.where(F.trim(F.col("address_before")) != "")
        .select("business_id",
                F.concat(F.lit("["), _visible(F.col("address_before")), F.lit("]")).alias("Before (in brackets)"),
                F.coalesce(F.concat(F.lit("["), F.col("address_after"), F.lit("]")), F.lit("(SQL NULL)")).alias("After (in brackets)"))
        .orderBy("After (in brackets)"))

# ---- 2) Checks ----------------------------------------------------------
_a = F.col("address")
_left = {label: df_business.where(cond).count() for label, cond in ADDRESS_CHECKS}

_real_non_ascii = "[^\\x00-\\x7F\u200b\u200c\u200d\ufeff\u00a0]"
_na_before = _addr_before.where(F.col("address_before").rlike(_real_non_ascii)).count()
_na_after  = df_business.where(_a.rlike(_real_non_ascii)).count()

_checks = [(f"No addresses left with: {label}", 0, v) for label, v in _left.items()] + [
    ("Row count unchanged",                              _addr_before.count(), df_business.count()),
    ("SQL NULL addresses = '' + no-street-name rows",    _addr_to_null,        df_business.where(_a.isNull()).count()),
    ("Changed addresses = rows flagged in Detect cell",  _addr_issue_rows,     _addr_changed.count()),
    ("Addresses with real non-ASCII letters unchanged",  _na_before,           _na_after),
    ("Every remaining address contains a letter",        0,                    df_business.where(_a.isNotNull() & ~_a.rlike("[A-Za-z]")).count()),
]

print("2) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.2 (address) verification failed"
print("✅ MS-01.2 · df_business.address: all checks passed")


# COMMAND ----------

# DBTITLE 1,MS-01.3 · city · Explain
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 MS-01.3 · `df_business.city` — Text Hygiene + Polluted City Values (DQ-BUSINESS-017 / 029 / 004 / 018)
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **DQ Issues** | `DQ-BUSINESS-017` Invisible Unicode characters · `DQ-BUSINESS-029` Repeated spaces and dangling separators · `DQ-BUSINESS-004` Leading / trailing whitespace · `DQ-BUSINESS-018` Polluted city values (trailing punctuation, embedded state, qualifiers, census names) |
# MAGIC | **Column** | `df_business.city` (uses `state` to decide whether a state text in the city is the row's own state) |
# MAGIC | **Rows affected** | invisible chars **4** · repeated spaces **9** · trailing `,` **17** · leading / trailing spaces **49** · own state inside the city **26** · reviewed-map values **10** · **111** rows with at least one problem |
# MAGIC | **Examples** | `'Nashville,'`, `'Boise '`, `'Tucson, Arizona'`, `'Tampa,Fl'`, `'Philadelphia PA'`, `'AB Edmonton'`, `'Nashville-Davidson metropolitan government (balance)'`, `'Boise (Meridian)'`, `'Maryland Height'` |
# MAGIC | **Details** | `01_Business_Value_Inventory_Findings.md` → section 3.1 (`city`) |
# MAGIC
# MAGIC **The problem in plain words:** the city is typed by hand, so it picks up the same hidden characters and spaces as `name` and `address`, plus extra text: the state (`Tucson, Arizona`, `Philadelphia PA`, `AB Edmonton`), a neighbourhood in brackets (`Largo (Walsingham)`), U.S. Census place names (`Indianapolis city (balance)`) and a typo (`Maryland Height`). Each variant becomes a separate city, so Nashville is counted as `Nashville`, `Nashville,` and `Nashville-Davidson metropolitan government (balance)`.
# MAGIC
# MAGIC **Suggested solutions (DQ document):**
# MAGIC
# MAGIC | # | Solution | Comment |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Rules + a small reviewed map | Every change comes from a rule or a reviewed map entry; no external data |
# MAGIC | 2 | Standardise against a gazetteer (USPS / GeoNames) by ZIP | Most authoritative, but an external dependency that may rename real local names |
# MAGIC | 3 | Keep raw `city` and add `city_std` | Adds a column the Silver DDL does not have |
# MAGIC | 4 | Keep as-is | Fragmented city dimension |
# MAGIC | 5 | Drop the rows | ❌ Rejected — cleaning never deletes rows (rule R-1) |
# MAGIC
# MAGIC **Solution taken:** **Option 1**, in this order:
# MAGIC
# MAGIC 1. Text hygiene with `clean_text()` from `MS-01.1` (hidden chars, repeated spaces, trailing `,` / `;`, trim, empty → NULL)
# MAGIC 2. **State rule:** remove the state at the end (`, PA`, ` PA`, `,Fl`, `, Arizona`, ` Florida`) or at the start (`AB Edmonton`) — **only when it is the row's own `state`**, as code or full name, any upper / lower case
# MAGIC 3. **Reviewed map** (`CITY_REVIEWED_MAP`), applied last:
# MAGIC
# MAGIC | Value after steps 1–2 | Becomes | Reason |
# MAGIC | --- | --- | --- |
# MAGIC | `Nashville-Davidson metropolitan government (balance)` | `Nashville` | Census place name |
# MAGIC | `Indianapolis city (balance)` | `Indianapolis` | Census place name |
# MAGIC | `Boise (Meridian)` | `Boise` | ZIP `83713` is a Boise ZIP |
# MAGIC | `Largo (Walsingham)` | `Largo` | Neighbourhood in brackets |
# MAGIC | `Philadelphia (Northeast Philly)` | `Philadelphia` | Neighbourhood in brackets |
# MAGIC | `Maryland Height` | `Maryland Heights` | Typo (MO, ZIP 63043) |
# MAGIC | `Bensalem.` | `Bensalem` | Stray dot left after `. Pa` |
# MAGIC | `Indian Rocks Beach.` | `Indian Rocks Beach` | Stray dot at the end |
# MAGIC
# MAGIC **Not done here:** no upper / lower case change (`maple shade`, `ST. LOUIS` — DQ-030), no `St.` / `Saint` / `Mt` / `Twp` unification (DQ-021), no fix of the misspellings found in the value inventory (`Tuscon`, `St. Loius` — new finding N1, pending your decision), no change to dash / slash qualifiers such as `Tampa - North` or `Wayne/Radnor` (not part of DQ-018). Abbreviation dots (`Mt. Juliet`, `Fairview Hts.`) are kept.
# MAGIC
# MAGIC **Why this is best:** each locality ends up as one city value, every change can be traced to a rule or a reviewed map entry, and the state is only removed when it matches the row's own state, so no real city name is cut. No row is deleted and no city becomes NULL.

# COMMAND ----------

# DBTITLE 1,MS-01.3 · city · Detect
# ============================================================
# 🔎 MS-01.3 · city · Detect — df_business.city
# ============================================================
# Read-only. Shows real rows for each city problem, then the counts.
# Uses ZERO_WIDTH_RE / SPACE_LIKE_RE / _visible() from MS-01.1 · Detect and clean_text() from MS-01.1 · Fix.
# Defines the rules used by MS-01.3 · Fix:
#   STATE_NAMES        : state code -> full name (for 'Tucson, Arizona', 'Reno Nevada', ...)
#   _city_strip_state(): removes the row's OWN state at the end / start of the city
#   CITY_REVIEWED_MAP  : reviewed one-to-one replacements (census names, brackets, typo, stray dot)

STATE_NAMES = {
    "AB": "Alberta",  "AZ": "Arizona",  "CA": "California", "DE": "Delaware", "FL": "Florida",
    "ID": "Idaho",    "IL": "Illinois", "IN": "Indiana",    "LA": "Louisiana", "MO": "Missouri",
    "NJ": "New Jersey", "NV": "Nevada", "PA": "Pennsylvania", "TN": "Tennessee",
}
CITY_REVIEWED_MAP = {
    "Nashville-Davidson metropolitan government (balance)": "Nashville",
    "Indianapolis city (balance)":                          "Indianapolis",
    "Boise (Meridian)":                                     "Boise",          # ZIP 83713 = Boise
    "Largo (Walsingham)":                                   "Largo",
    "Philadelphia (Northeast Philly)":                      "Philadelphia",
    "Maryland Height":                                      "Maryland Heights",
    "Bensalem.":                                            "Bensalem",
    "Indian Rocks Beach.":                                  "Indian Rocks Beach",
}
_STATE_NAME_MAP = F.create_map(*[F.lit(x) for kv in STATE_NAMES.items() for x in kv])
_CITY_MAP       = F.create_map(*[F.lit(x) for kv in CITY_REVIEWED_MAP.items() for x in kv])

def _city_strip_state(c, state=F.col("state")):
    """Remove the row's own state (code or full name, any case) at the end or the start of the city."""
    name = F.coalesce(_STATE_NAME_MAP[state], state)
    suffix = F.concat(F.lit("(?i)[ ,]+("), state, F.lit("|"), name, F.lit(")$"))   # ', PA' / ' Florida' / ',Fl'
    prefix = F.concat(F.lit("^("), state, F.lit(") +"))                           # 'AB Edmonton'
    stripped = F.trim(F.regexp_replace(F.regexp_replace(c, suffix, F.lit("")), prefix, F.lit("")))
    return F.when(state.isNull() | c.isNull(), c).otherwise(stripped)

_c       = F.col("city")
_c_hyg   = clean_text(_c)                       # city after text hygiene only
_c_state = _city_strip_state(_c_hyg)            # ... and after the state rule

CITY_CHECKS = [
    ("Invisible characters (U+200B / U+00A0 / ...)", _c.rlike(f"{ZERO_WIDTH_RE}|{SPACE_LIKE_RE}")),
    ("Repeated spaces",                              _c.rlike("  ")),
    ("Trailing , or ;",                              _c.rlike("[,;]\\s*$")),
    ("Leading / trailing spaces",                    _c != F.trim(_c)),
    ("Empty string",                                 _c_hyg.isNull() & _c.isNotNull()),
    ("Own state inside the city",                    _c_state != _c_hyg),
    ("Value in the reviewed map",                    _c_state.isin(list(CITY_REVIEWED_MAP))),
]

# ---- 1) Real rows per problem ------------------------------------------
for i, (label, cond) in enumerate(CITY_CHECKS, start=1):
    print("=" * 80)
    print(f"1.{i}) Sample rows — {label}")
    print("   Purpose: see the real cities with this problem.")
    print("=" * 80)
    display(df_business.where(cond)
            .select("business_id", "name",
                    F.col("city").alias("City (as stored)"),
                    _visible(_c).alias("City made visible"),
                    F.concat(F.lit("["), _c, F.lit("]")).alias("City in brackets"),
                    "state", "postal_code")
            .orderBy("city").limit(30))

# ---- 2) Counts --------------------------------------------------------
print("=" * 80)
print("2) How many cities have each problem")
print("   Purpose: size of each problem; a city can have more than one problem.")
print("=" * 80)
_city_counts = df_business.select(
    *[F.sum(cond.cast("int")).alias(label) for label, cond in CITY_CHECKS]).first().asDict()
_city_any_issue  = reduce(lambda a, b: a | b, [cond for _, cond in CITY_CHECKS])
_city_issue_rows = df_business.where(_city_any_issue).count()
_city_distinct_before = df_business.select("state", "city").distinct().count()

display(spark.createDataFrame(
    [(label, int(v)) for label, v in _city_counts.items()]
    + [("Rows with at least one problem", int(_city_issue_rows))],
    "Problem string, Rows long"))
print(f"Total cities: {df_business.count():,} | SQL NULL cities: {df_business.where(_c.isNull()).count():,} "
      f"| distinct (state, city) pairs: {_city_distinct_before:,}")

# ---- 3) Every state-inside-city value --------------------------------------
print("=" * 80)
print("3) Every city that contains its own state (the state part will be removed)")
print("   Purpose: check by eye that only the state text is cut and the real city stays.")
print("=" * 80)
display(df_business.where(CITY_CHECKS[5][1])
        .groupBy(F.col("city").alias("City (as stored)"), "state",
                 _c_state.alias("City after the state rule"))
        .count().withColumnRenamed("count", "Rows").orderBy("City (as stored)"))

# ---- 4) Reviewed map: entries and rows found --------------------------------
print("=" * 80)
print("4) Reviewed map — every entry and how many rows it matches")
print("   Purpose: each entry must match at least 1 row; 'Rows' = rows that will be replaced.")
print("=" * 80)
_map_hits = (df_business.where(CITY_CHECKS[6][1])
             .groupBy(_c_state.alias("From")).count().withColumnRenamed("count", "Rows"))
display(spark.createDataFrame(list(CITY_REVIEWED_MAP.items()), "From string, To string")
        .join(_map_hits, "From", "left").fillna(0, ["Rows"]).orderBy("From"))


# COMMAND ----------

# DBTITLE 1,MS-01.3 · city · Fix
# ============================================================
# 🔧 MS-01.3 · city · Fix — df_business.city
# ============================================================
# Applied to df_business in place. No row is deleted.
#   1. clean_text() from MS-01.1 · Fix                                   (DQ-017 / 029 / 004)
#   2. _city_strip_state(): remove the row's own state at the end/start  (DQ-018)
#   3. CITY_REVIEWED_MAP: reviewed one-to-one replacements               (DQ-018)
#   4. empty -> SQL NULL (none expected)
# Uses STATE_NAMES / _city_strip_state / CITY_REVIEWED_MAP from MS-01.3 · Detect.
# Idempotent: running the cell again changes nothing.

def clean_city(col, state=F.col("state")):
    c = _city_strip_state(clean_text(col), state)
    c = F.coalesce(_CITY_MAP[c], c)
    return F.when(c == "", F.lit(None).cast("string")).otherwise(c)

# Before copy, kept only for the verify cell
_city_before = df_business.select("business_id", "state", F.col("city").alias("city_before"))

df_business = df_business.withColumn("city", clean_city(F.col("city")))

_city_changed = (df_business.select("business_id", F.col("city").alias("city_after"))
                 .join(_city_before, "business_id")
                 .where(~F.col("city_before").eqNullSafe(F.col("city_after"))))
print(f"🔧 MS-01.3 df_business.city cleaned | cities changed: {_city_changed.count():,} "
      f"(expected {_city_issue_rows:,} from the Detect cell) | rows: {df_business.count():,}")


# COMMAND ----------

# DBTITLE 1,MS-01.3 · city · Verify
# ============================================================
# ✅ MS-01.3 · city · Verify — df_business.city
# ============================================================
# Read-only checks:
#   * every problem from the Detect cell is now 0 (re-detected on the cleaned city)
#   * row count unchanged, no new NULL cities
#   * only the expected cities changed; every reviewed-map target exists
#   * valid non-ASCII letters were not removed
#   * fewer distinct (state, city) pairs than before

# ---- 1) Before / after for the changed cities ---------------------------
print("1) Before / after — every changed city value")
print("   Purpose: see that only hidden characters, spaces, end separators, the own state and reviewed values changed.")
display(_city_changed
        .groupBy(F.concat(F.lit("["), _visible(F.col("city_before")), F.lit("]")).alias("Before (in brackets)"),
                 F.concat(F.lit("["), F.col("city_after"), F.lit("]")).alias("After (in brackets)"),
                 "state")
        .count().withColumnRenamed("count", "Rows")
        .orderBy("After (in brackets)", "Before (in brackets)"))

# ---- 2) Checks ----------------------------------------------------------
_c = F.col("city")
_left = {label: df_business.where(cond).count() for label, cond in CITY_CHECKS}   # CITY_CHECKS read the current 'city'

_real_non_ascii = "[^\\x00-\\x7F\u200b\u200c\u200d\ufeff\u00a0]"
_na_before = _city_before.where(F.col("city_before").rlike(_real_non_ascii)).count()
_na_after  = df_business.where(_c.rlike(_real_non_ascii)).count()
_null_before = _city_before.where(F.col("city_before").isNull()).count()
_city_distinct_after = df_business.select("state", "city").distinct().count()
_map_targets_missing = (spark.createDataFrame([(v,) for v in set(CITY_REVIEWED_MAP.values())], "city string")
                        .join(df_business.select("city").distinct(), "city", "left_anti").count())

_checks = [(f"No cities left with: {label}", 0, v) for label, v in _left.items()] + [
    ("Row count unchanged",                          _city_before.count(), df_business.count()),
    ("No new SQL NULL cities",                       _null_before,         df_business.where(_c.isNull()).count()),
    ("Changed cities = rows flagged in Detect cell", _city_issue_rows,     _city_changed.count()),
    ("Cities with real non-ASCII letters unchanged", _na_before,           _na_after),
    ("Every reviewed-map target exists as a city",   0,                    _map_targets_missing),
]

print("2) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))
print(f"Distinct (state, city) pairs: {_city_distinct_before:,} before -> {_city_distinct_after:,} after "
      f"({_city_distinct_before - _city_distinct_after:,} variants merged)")

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.3 (city) verification failed"
assert _city_distinct_after < _city_distinct_before, "❌ MS-01.3: no city variants were merged"
print("✅ MS-01.3 · df_business.city: all checks passed")


# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Explain
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 MS-01.4 · `df_business.state` — Invalid and Out-of-Footprint States (DQ-BUSINESS-009 / 010)
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **DQ Issues** | `DQ-BUSINESS-009` Non-existent state code `XMS` · `DQ-BUSINESS-010` Real state codes outside the Yelp footprint |
# MAGIC | **Column** | `df_business.state` (uses `postal_code`, `latitude`, `longitude`, `city` as witnesses) |
# MAGIC | **Rows affected** | **21** of 150,346: `XMS` **1** · out-of-footprint codes **20** (TX 4, CO 3, HI 2, MA 2, WA 2, MI 1, MT 1, NC 1, SD 1, UT 1, VI 1, VT 1) |
# MAGIC | **Examples** | `Pumpernickles Catering, Liverpool, XMS, L3 5TB` · `K Soho Salon, Tampa, SD, 33606` · `Eagle, UT, 83616` · `Houston, TX, 77077` at Nashville coordinates · `Bennington, VT, 05201` |
# MAGIC | **Footprint** | The Yelp Open Dataset covers 14 states / provinces: PA, NJ, DE, FL, IN, TN, AZ, LA, NV, AB, CA, ID, MO, IL (`FOOTPRINT_STATES`) |
# MAGIC
# MAGIC **The problem in plain words:** 21 businesses have a state outside the 14 states of the dataset. They are not all the same problem:
# MAGIC
# MAGIC | Pattern | Rows | What the record says | Meaning |
# MAGIC | --- | ---: | --- | --- |
# MAGIC | **(a) Only the state is wrong** | 6 | ZIP and coordinates both point to the same footprint state (`Tampa, SD, 33606` → FL) | Typo in `state`, provable fix |
# MAGIC | **(b) Address and coordinates disagree** | 13 | Address is outside the footprint, coordinates are inside it (`Houston, TX` at Nashville) | One half of the record is wrong, we cannot tell which |
# MAGIC | **(c) Fully outside the footprint** | 1 | Bennington, VT — address and coordinates agree on Vermont | Real business, but outside the dataset scope |
# MAGIC | **XMS** | 1 | UK county code + UK postcode (`L3 5TB`), coordinates in St. Louis | Not a US / Canadian state at all |
# MAGIC
# MAGIC **Suggested solutions (DQ document):**
# MAGIC
# MAGIC | # | Solution | Comment |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | (a) correct `state` from the ZIP prefix when ZIP and coordinates agree; (b), (c), XMS → quarantine with a reason | Fixes only what two independent fields prove; isolates the rest |
# MAGIC | 2 | Re-derive `state` for all 21 from the coordinates | Would label a Houston address as Tennessee |
# MAGIC | 3 | Quarantine all 21 | Loses 6 businesses whose only defect is a provable typo |
# MAGIC | 4 | Set `state` to NULL for all 21 | Hides the problem; keeps contradictory addresses |
# MAGIC | 5 | Keep as-is | Wrong states in every state-level count |
# MAGIC
# MAGIC **Solution taken:** **Option 1**, applied as follows:
# MAGIC
# MAGIC 1. **ZIP witness:** the first 3 digits of a 5-digit US ZIP give the state (USPS prefix ranges for the 13 US footprint states, `ZIP3_STATE_RANGES`).
# MAGIC 2. **Coordinate witness:** the coordinates must lie inside the **data-derived box** of that state (0.1 % – 99.9 % latitude / longitude of all businesses already in that state).
# MAGIC 3. **Pattern (a)** = both witnesses agree on a footprint state → `state` is replaced by that state. The `city` check is shown for information.
# MAGIC 4. **Patterns (b), (c) and XMS (15 businesses)** → **moved to quarantine**: one detailed row each in `df_quarantine_business`, then the business is removed from `df_business` **and all its child rows are removed from the 10 child DataFrames** (`df_attributes`, `df_hours`, `df_categories` and the 7 dict DataFrames), so **no orphan row is left**. Their values are not changed (no guessing).
# MAGIC
# MAGIC **What each quarantine row contains:**
# MAGIC
# MAGIC | Column(s) | Content |
# MAGIC | --- | --- |
# MAGIC | `quarantine_reason` | `ADDRESS_COORD_CONFLICT` (b) · `OUT_OF_FOOTPRINT` (c) · `NON_EXISTENT_STATE` (XMS) |
# MAGIC | `dq_issue`, `pattern`, `detected_in` | `DQ-BUSINESS-009` / `010`, the pattern, and the notebook step that moved it |
# MAGIC | `reason_detail` | Full plain-text explanation built from the record itself: why the state is invalid, what the address block says, what the ZIP says, which footprint state box the coordinates fall in, why nothing can be corrected, which child rows were removed, and how many reviews / tips / check-ins must follow in the downstream notebooks |
# MAGIC | `note`, `zip_state`, `coords_in_states` | Short machine-readable evidence |
# MAGIC | `name` … `is_open` | The full business record (values as cleaned up to MS-01.4) |
# MAGIC | `bronze_attributes_json`, `bronze_hours_json`, `bronze_categories` | The raw bronze child data, so the business can be audited or restored completely |
# MAGIC | `child_rows_removed`, `child_rows_removed_total` | Rows removed per child DataFrame (JSON) and in total |
# MAGIC | `downstream_reviews`, `downstream_tips`, `downstream_checkins` | Rows in `bronze_final` that the review / tip / check-in notebooks must also quarantine |
# MAGIC | `action`, `quarantined_at` | `MOVED_TO_QUARANTINE` and the timestamp |
# MAGIC
# MAGIC > ⚠️ **Approved exception to rule R-1 (decision by Ibrahim Mohamed, 2026-10-25):** businesses whose location cannot be trusted (b, XMS) or that lie outside the dataset scope (c) leave the working DataFrames together with all their child rows. Nothing is lost: the complete record is kept in `df_quarantine_business`, which is written to the Unity Catalog quarantine schema (`yelp_poc_uc.quarantine`) at the end of the notebook.
# MAGIC
# MAGIC **Not done here:** the reviews / tips / check-ins of the 15 businesses live in other notebooks — each of them must quarantine its rows whose `business_id` is not in `silver_cleaned.yelp_business`. `state` vs ZIP contradictions inside the footprint (DQ-023, 71 rows) and address vs coordinates conflicts of footprint businesses (DQ-046) — separate steps. No coordinates and no other column are changed.
# MAGIC
# MAGIC **Why this is best:** a state is only corrected when two independent fields prove the right answer, so no value is invented. Records that contradict themselves or lie outside the dataset are isolated together with all their child rows, so no orphan is created, and the detailed reason plus the full record keep every decision auditable and reversible.

# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Detect
# ============================================================
# 🔎 MS-01.4 · state · Detect — df_business.state
# ============================================================
# Read-only. Shows every business whose state is outside FOOTPRINT_STATES, with the two witnesses:
#   ZIP state          : state of the USPS 3-digit ZIP prefix (only the 13 US footprint states are listed)
#   Coords in ZIP state: lat/lon inside the data-derived box of the ZIP state (0.1%-99.9% of its businesses)
#   City in ZIP state  : the city name is used by other businesses of the ZIP state (information only)
# Defines _state_review (one row per out-of-footprint business + its pattern), used by MS-01.4 · Fix.

# USPS 3-digit ZIP prefix ranges for the US footprint states (AB uses Canadian postal codes)
ZIP3_STATE_RANGES = [
    ("NJ",  70,  89), ("PA", 150, 196), ("DE", 197, 199), ("FL", 320, 349),
    ("TN", 370, 385), ("IN", 460, 479), ("IL", 600, 629), ("MO", 630, 658),
    ("LA", 700, 714), ("ID", 832, 838), ("AZ", 850, 865), ("NV", 889, 898),
    ("CA", 900, 961),
]

def _zip_state(pc=F.col("postal_code")):
    z3 = F.when(pc.rlike("^[0-9]{5}$"), F.substring(pc, 1, 3).cast("int"))
    expr = F.lit(None).cast("string")
    for st, lo, hi in ZIP3_STATE_RANGES:
        expr = F.when(z3.between(lo, hi), F.lit(st)).otherwise(expr)
    return expr

# Data-derived coordinate box per footprint state (robust to the few bad rows)
_state_box = (df_business.where(F.col("state").isin(FOOTPRINT_STATES))
    .groupBy("state").agg(
        F.expr("percentile_approx(latitude, 0.001)").alias("lat_min"),
        F.expr("percentile_approx(latitude, 0.999)").alias("lat_max"),
        F.expr("percentile_approx(longitude, 0.001)").alias("lon_min"),
        F.expr("percentile_approx(longitude, 0.999)").alias("lon_max"))
    .withColumnRenamed("state", "zip_state"))

_state_cities = (df_business.where(F.col("state").isin(FOOTPRINT_STATES))
    .select(F.col("state").alias("zip_state"), F.lower("city").alias("city_lc")).distinct()
    .withColumn("city_in_zip_state", F.lit(True)))

_out = (df_business.where(~F.col("state").isin(FOOTPRINT_STATES) | F.col("state").isNull())
        .withColumn("zip_state", _zip_state())
        .withColumn("city_lc", F.lower("city"))
        .join(_state_box, "zip_state", "left")
        .join(_state_cities, ["zip_state", "city_lc"], "left"))

_coords_ok = (F.col("zip_state").isNotNull()
              & F.col("latitude").between(F.col("lat_min"), F.col("lat_max"))
              & F.col("longitude").between(F.col("lon_min"), F.col("lon_max")))
_out = (_out.withColumn("coords_in_zip_state", F.coalesce(_coords_ok, F.lit(False)))
            .withColumn("city_in_zip_state", F.coalesce("city_in_zip_state", F.lit(False))))

# (b) vs (c): which footprint state boxes contain the coordinates?
#   at least one -> (b) address and coordinates disagree; none -> (c) fully outside the footprint
_b = _state_box.alias("b")
_coords_states = (_out.alias("r")
    .join(_b, F.col("r.latitude").between(F.col("b.lat_min"), F.col("b.lat_max"))
              & F.col("r.longitude").between(F.col("b.lon_min"), F.col("b.lon_max")), "inner")
    .groupBy(F.col("r.business_id").alias("business_id"))
    .agg(F.array_sort(F.collect_set(F.col("b.zip_state"))).alias("coords_in_states")))

_state_review = (_out.join(_coords_states, "business_id", "left")
    .withColumn("coords_in_footprint", F.coalesce(F.size("coords_in_states") > 0, F.lit(False)))
    .withColumn("pattern",
        F.when(F.col("state") == "XMS", "XMS")
         .when(F.col("coords_in_zip_state"), "(a) only the state is wrong")
         .when(F.col("coords_in_footprint"), "(b) address and coordinates disagree")
         .otherwise("(c) fully outside the footprint"))
    .withColumn("new_state", F.when(F.col("pattern").startswith("(a)"), F.col("zip_state")))
    .withColumn("quarantine_reason", F.when(F.col("pattern") == "XMS", "NON_EXISTENT_STATE")
                .when(F.col("pattern").startswith("(b)"), "ADDRESS_COORD_CONFLICT")
                .when(F.col("pattern").startswith("(c)"), "OUT_OF_FOOTPRINT"))
    .withColumn("dq_issue", F.when(F.col("pattern") == "XMS", "DQ-BUSINESS-009").otherwise("DQ-BUSINESS-010"))
    .select("business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude",
            "stars", "review_count", "is_open",
            "zip_state", "coords_in_zip_state", "city_in_zip_state", "coords_in_footprint", "coords_in_states",
            "pattern", "new_state", "quarantine_reason", "dq_issue"))

# ---- 1) Every out-of-footprint business with its witnesses ---------------
print("=" * 80)
print("1) Every business with a state outside the 14 footprint states")
print("   Purpose: see each record and why it gets its pattern. (a) = ZIP and coordinates agree -> state corrected.")
print("=" * 80)
display(_state_review.select(
            "business_id", "name", "city", F.col("state").alias("State (as stored)"), "postal_code",
            "latitude", "longitude",
            F.col("zip_state").alias("ZIP state"),
            F.col("coords_in_zip_state").alias("Coords in ZIP state"),
            F.col("city_in_zip_state").alias("City in ZIP state (info)"),
            F.col("coords_in_footprint").alias("Coords in footprint"),
            F.concat_ws(", ", "coords_in_states").alias("Coords inside the box of"),
            F.col("pattern").alias("Pattern"),
            F.coalesce("new_state", F.lit("(unchanged)")).alias("New state"),
            F.coalesce("quarantine_reason", F.lit("-")).alias("Quarantine reason"))
        .orderBy("Pattern", "State (as stored)", "city"))

# ---- 2) Counts --------------------------------------------------------
print("=" * 80)
print("2) How many businesses per pattern")
print("   Purpose: size of each pattern; expected 6 (a), 13 (b), 1 (c), 1 XMS.")
print("=" * 80)
_state_pattern_counts = {r["pattern"]: r["count"] for r in _state_review.groupBy("pattern").count().collect()}
_state_fix_rows        = _state_review.where(F.col("new_state").isNotNull()).count()
_state_quarantine_rows = _state_review.where(F.col("quarantine_reason").isNotNull()).count()
display(spark.createDataFrame(
    sorted(_state_pattern_counts.items())
    + [("Rows whose state will be corrected (a)", _state_fix_rows),
       ("Businesses moved to quarantine with all child rows (b) + (c) + XMS", _state_quarantine_rows)],
    "Pattern string, Rows long"))

print(f"Total businesses: {df_business.count():,} | outside footprint: {_state_review.count():,} "
      f"| SQL NULL states: {df_business.where(F.col('state').isNull()).count():,}")

# ---- 3) Coordinate boxes used as witness ---------------------------------
print("=" * 80)
print("3) Data-derived coordinate box per footprint state")
print("   Purpose: the area each state's businesses cover; a pattern (a) row must lie inside its ZIP state's box.")
print("=" * 80)
display(_state_box.withColumnRenamed("zip_state", "state").orderBy("state"))


# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Fix
# ============================================================
# 🔧 MS-01.4 · state · Fix — df_business.state
# ============================================================
# Uses _state_review from MS-01.4 · Detect (run Detect right before this cell).
#   1. pattern (a): state <- ZIP state (ZIP and coordinates agree)                     (DQ-010 a)
#   2. pattern (b), (c) and XMS (15 businesses):                                       (DQ-009 / DQ-010 b, c)
#      * one detailed row each in df_quarantine_business: reason code, a full plain-text
#        reason (reason_detail), the evidence, the full business record, the raw bronze
#        attributes / hours / categories, the child rows removed and the downstream rows
#      * the business is REMOVED from df_business AND all its child rows are removed from the
#        10 child DataFrames (no orphans)  -> approved exception to rule R-1 (2026-10-25)
# df_quarantine_business is written to yelp_poc_uc.quarantine at the end of the notebook.
# Idempotent: re-running replaces this step's quarantine rows instead of adding duplicates.

import re, json

MOVED_TO_QUARANTINE = "MOVED_TO_QUARANTINE"
CHILD_DF_NAMES = ["df_attributes", "df_hours", "df_categories",
                  "df_ambience", "df_best_nights", "df_business_parking", "df_dietary_restrictions",
                  "df_good_for_meal", "df_hair_specializes_in", "df_music"]
DOWNSTREAM_TABLES = {"reviews": "yelp_poc_uc.bronze_final.yelp_review",
                     "tips": "yelp_poc_uc.bronze_final.yelp_tip",
                     "checkins": "yelp_poc_uc.bronze_final.yelp_checkin"}
UK_POSTCODE_RE = r"^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$"

# ---- 0) Snapshots for the verify cell -------------------------------------
_state_before      = df_business.select("business_id", F.col("state").alias("state_before"))
_biz_rows_before   = df_business.count()
_child_rows_before = {n: globals()[n].count() for n in CHILD_DF_NAMES}

_review_rows = [r.asDict() for r in _state_review.collect()]
_q_rows_src  = [r for r in _review_rows if r["quarantine_reason"]]
_q_ids_df    = spark.createDataFrame([(r["business_id"],) for r in _q_rows_src], "business_id string")

# ---- 1) Evidence: child rows, downstream rows, raw bronze record -----------
_child_counts = {}                       # business_id -> {child DataFrame: rows}
for n in CHILD_DF_NAMES:
    for r in (globals()[n].join(F.broadcast(_q_ids_df), "business_id", "left_semi")
              .groupBy("business_id").count().collect()):
        _child_counts.setdefault(r["business_id"], {})[n] = r["count"]

_downstream = {}                         # business_id -> {reviews / tips / checkins: rows}
for label, tbl in DOWNSTREAM_TABLES.items():
    for r in (spark.table(tbl).join(F.broadcast(_q_ids_df), "business_id", "left_semi")
              .groupBy("business_id").count().collect()):
        _downstream.setdefault(r["business_id"], {})[label] = r["count"]

_bronze_raw = {r["business_id"]: r for r in
               _df_bronze.join(F.broadcast(_q_ids_df), "business_id", "left_semi")
               .select("business_id", F.to_json("attributes").alias("attributes_json"),
                       F.to_json("hours").alias("hours_json"), "categories").collect()}

# ---- 2) Detailed plain-text reason per business ---------------------------
def _q(v):
    return "NULL" if v is None else f"'{v}'"

def _reason_detail(r):
    bid, st, pc = r["business_id"], r["state"], r["postal_code"] or ""
    addr   = (f"street {_q(r['address'])}, city {_q(r['city'])}, state {_q(st)}, "
              f"postal code {_q(r['postal_code'])}")
    coords = f"({r['latitude']:.5f}, {r['longitude']:.5f})"
    boxes  = ", ".join(r["coords_in_states"] or []) or "none"
    fp     = ", ".join(FOOTPRINT_STATES)

    # what the postal code says
    if r["zip_state"]:
        zip_txt = (f"The ZIP prefix '{pc[:3]}' belongs to {r['zip_state']}, not to the stored state {st}"
                   + (", and the coordinates are not inside {0} either, so the ZIP does not confirm the coordinates."
                      .format(r["zip_state"]) if not r["coords_in_zip_state"] else "."))
    elif re.match(r"^[0-9]{5}$", pc):
        zip_txt = (f"The ZIP prefix '{pc[:3]}' does not belong to any of the 13 US footprint states, "
                   f"so the ZIP agrees with the out-of-footprint state {st}.")
    elif re.match(UK_POSTCODE_RE, pc):
        zip_txt = (f"The postal code '{pc}' has the UK postcode format (outward code + inward code); "
                   f"it is neither a US ZIP nor a Canadian postal code.")
    else:
        zip_txt = f"The postal code {_q(r['postal_code'])} is not a 5-digit US ZIP code."

    if r["pattern"] == "XMS":
        head = (f"NON_EXISTENT_STATE (DQ-BUSINESS-009). The stored state {_q(st)} is not a USPS (United States) "
                f"or Canada Post (Canada) jurisdiction code, and not one of the 14 footprint states / provinces "
                f"of the Yelp Open Dataset ({fp}). Address block: {addr}. {zip_txt} The address reads as a "
                f"foreign (United Kingdom) address, while the coordinates {coords} lie inside the footprint "
                f"coordinate box of {boxes}. The state code itself is invalid and the address contradicts the "
                f"coordinates, so no field can be corrected without guessing.")
    elif r["pattern"].startswith("(b)"):
        head = (f"ADDRESS_COORD_CONFLICT (DQ-BUSINESS-010, pattern b). The stored state {_q(st)} is a real "
                f"jurisdiction but outside the 14 footprint states / provinces ({fp}). Address block: {addr}. "
                f"{zip_txt} The coordinates {coords} lie inside the footprint coordinate box of {boxes} "
                f"(data-derived 0.1%-99.9% latitude / longitude range of the businesses of that state). The "
                f"address points outside the footprint and the coordinates point inside it: one half of the "
                f"record is wrong and nothing in the record proves which one, so neither the state nor the "
                f"coordinates can be corrected without guessing.")
    else:
        head = (f"OUT_OF_FOOTPRINT (DQ-BUSINESS-010, pattern c). The stored state {_q(st)} is a real "
                f"jurisdiction but outside the 14 footprint states / provinces ({fp}). Address block: {addr}. "
                f"{zip_txt} The coordinates {coords} are outside the coordinate box of every footprint state, "
                f"so address and coordinates agree with each other: the record is consistent, but it describes "
                f"a business outside the scope of the Yelp Open Dataset (11 metro areas).")

    cc = _child_counts.get(bid, {})
    ds = _downstream.get(bid, {})
    child_txt = ", ".join(f"{n} {cc.get(n, 0)}" for n in CHILD_DF_NAMES)
    tail = (f" Action taken (MS-01.4, 01_Business_Cleaning): the business was removed from df_business and "
            f"moved to quarantine with its full record (business columns as cleaned up to MS-01.4, plus the raw "
            f"bronze attributes, hours and categories). Child rows removed from the working DataFrames so that "
            f"no orphan remains: {child_txt} (total {sum(cc.values())}). Downstream rows in bronze_final that "
            f"must follow this business into quarantine in their own notebooks: reviews {ds.get('reviews', 0)}, "
            f"tips {ds.get('tips', 0)}, check-ins {ds.get('checkins', 0)}. Business record at quarantine time: "
            f"review_count {r['review_count']}, stars {r['stars']}, is_open {r['is_open']}.")
    return head + tail

# ---- 3) Quarantine rows ---------------------------------------------------
_Q_SCHEMA = ("business_id string, quarantine_reason string, dq_issue string, pattern string, "
             "reason_detail string, note string, action string, detected_in string, "
             "zip_state string, coords_in_states string, "
             "child_rows_removed string, child_rows_removed_total long, "
             "downstream_reviews long, downstream_tips long, downstream_checkins long, "
             "name string, address string, city string, state string, postal_code string, "
             "latitude double, longitude double, stars double, review_count long, is_open long, "
             "bronze_attributes_json string, bronze_hours_json string, bronze_categories string")

_q_records = []
for r in _q_rows_src:
    bid = r["business_id"]
    cc, ds, raw = _child_counts.get(bid, {}), _downstream.get(bid, {}), _bronze_raw.get(bid)
    _q_records.append((
        bid, r["quarantine_reason"], r["dq_issue"], r["pattern"], _reason_detail(r),
        f"state={r['state']} | city={r['city']} | postal_code={r['postal_code']} | "
        f"zip_state={r['zip_state'] or 'none'} | coords_in={','.join(r['coords_in_states'] or []) or 'none'}",
        MOVED_TO_QUARANTINE, "01_Business_Cleaning · MS-01.4 · state",
        r["zip_state"], ", ".join(r["coords_in_states"] or []) or None,
        json.dumps({n: cc.get(n, 0) for n in CHILD_DF_NAMES}), int(sum(cc.values())),
        int(ds.get("reviews", 0)), int(ds.get("tips", 0)), int(ds.get("checkins", 0)),
        r["name"], r["address"], r["city"], r["state"], r["postal_code"],
        r["latitude"], r["longitude"], r["stars"], r["review_count"], r["is_open"],
        raw["attributes_json"] if raw else None, raw["hours_json"] if raw else None,
        raw["categories"] if raw else None))

_state_q = (spark.createDataFrame(_q_records, _Q_SCHEMA)
            .withColumn("quarantined_at", F.current_timestamp()))

df_quarantine_business = (df_quarantine_business
    .where(~F.col("dq_issue").isin("DQ-BUSINESS-009", "DQ-BUSINESS-010"))   # idempotent re-run
    .unionByName(_state_q, allowMissingColumns=True))

# ---- 4) df_business: correct (a), remove the 15 ----------------------------
_state_fix = _state_review.where(F.col("new_state").isNotNull()).select("business_id", "new_state")
df_business = (df_business
               .join(F.broadcast(_q_ids_df), "business_id", "left_anti")
               .join(F.broadcast(_state_fix), "business_id", "left")
               .withColumn("state", F.coalesce("new_state", "state"))
               .drop("new_state")
               .select(*TOP_LEVEL_COLS))                       # keep the original column order

# ---- 5) Child DataFrames: remove the orphaned rows -------------------------
df_attributes           = df_attributes.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_hours                = df_hours.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_categories           = df_categories.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_ambience             = df_ambience.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_best_nights          = df_best_nights.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_business_parking     = df_business_parking.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_dietary_restrictions = df_dietary_restrictions.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_good_for_meal        = df_good_for_meal.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_hair_specializes_in  = df_hair_specializes_in.join(F.broadcast(_q_ids_df), "business_id", "left_anti")
df_music                = df_music.join(F.broadcast(_q_ids_df), "business_id", "left_anti")

_state_changed = (df_business.select("business_id", F.col("state").alias("state_after"))
                  .join(_state_before, "business_id")
                  .where(~F.col("state_before").eqNullSafe(F.col("state_after"))))
print(f"🔧 MS-01.4 df_business.state | corrected: {_state_changed.count():,} (expected {_state_fix_rows:,}) "
      f"| moved to quarantine: {_state_q.count():,} (expected {_state_quarantine_rows:,}) "
      f"| df_business: {_biz_rows_before:,} -> {df_business.count():,}")
for n in CHILD_DF_NAMES:
    print(f"   {n:<24}: {_child_rows_before[n]:>9,} -> {globals()[n].count():>9,} rows")


# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Verify
# ============================================================
# ✅ MS-01.4 · state · Verify — df_business.state
# ============================================================
# Read-only checks:
#   * the pattern (a) rows now carry their ZIP state; no other state changed
#   * the 15 quarantined businesses are gone from df_business and from all 10 child DataFrames
#   * no orphan: every business_id of every child DataFrame exists in df_business
#   * quarantine: one detailed row per business, full record equal to the Detect snapshot
#   * no business outside the footprint is left, no NULL states, kept rows untouched

# ---- 1) Before / after for the corrected states -------------------------
print("1) Before / after — corrected states")
print("   Purpose: see that only the state changed and that it matches the ZIP code and the city.")
display(_state_changed.join(df_business.select("business_id", "name", "city", "postal_code"), "business_id")
        .select("business_id", "name", "city", "postal_code",
                F.col("state_before").alias("State before"), F.col("state_after").alias("State after"))
        .orderBy("State after", "city"))

_q_state = df_quarantine_business.where(F.col("dq_issue").isin("DQ-BUSINESS-009", "DQ-BUSINESS-010"))

print("2) Quarantine register — one row per moved business, with the full reason")
print("   Purpose: read why each business was moved; reason_detail holds the complete explanation.")
display(_q_state.select("business_id", "name", "quarantine_reason", "dq_issue", "pattern", "reason_detail",
                        "child_rows_removed_total", "downstream_reviews", "downstream_tips", "downstream_checkins")
        .orderBy("quarantine_reason", "name"))

print("3) Quarantine register — evidence and full record (all columns)")
print("   Purpose: everything needed to audit or restore the business is kept here.")
display(_q_state.orderBy("quarantine_reason", "name"))

# ---- 4) Checks ----------------------------------------------------------
_still_out = df_business.where(~F.col("state").isin(FOOTPRINT_STATES)).select("business_id")
_q_ids     = _q_state.select("business_id")
_expected_fix = _state_review.where(F.col("new_state").isNotNull()).select("business_id", F.col("new_state").alias("state_after"))

# kept rows: every column except state equals the Detect snapshot
_other_cols = [c for c in TOP_LEVEL_COLS if c not in ("business_id", "state")]
_row_unchanged = (df_business.join(_state_review.select("business_id", *[F.col(c).alias(f"{c}_b") for c in _other_cols]), "business_id")
                  .where(reduce(lambda a, b: a | b, [~F.col(c).eqNullSafe(F.col(f"{c}_b")) for c in _other_cols])).count())

# moved rows: the quarantine record equals the Detect snapshot in every business column
_rec_cols = [c for c in TOP_LEVEL_COLS if c != "business_id"]
_full_record_ok = (_q_state.select("business_id", *_rec_cols)
    .join(_state_review.select("business_id", *[F.col(c).alias(f"{c}_b") for c in _rec_cols]), "business_id")
    .where(reduce(lambda a, b: a & b, [F.col(c).eqNullSafe(F.col(f"{c}_b")) for c in _rec_cols])).count())

_recorded_child = {n: sum(_child_counts.get(b, {}).get(n, 0) for b in _child_counts) for n in CHILD_DF_NAMES}
_biz_ids = df_business.select("business_id")

_checks = [
    ("Changed states = pattern (a) rows",                     _state_fix_rows,        _state_changed.count()),
    ("Every changed state equals its ZIP state",              _state_fix_rows,        _state_changed.join(_expected_fix, ["business_id", "state_after"]).count()),
    ("df_business rows removed = quarantined businesses",     _state_quarantine_rows, _biz_rows_before - df_business.count()),
    ("Quarantined businesses still in df_business",           0,                      _q_ids.join(_biz_ids, "business_id", "left_semi").count()),
    ("Businesses outside the footprint left in df_business",  0,                      _still_out.count()),
]
for n in CHILD_DF_NAMES:
    _d = globals()[n]
    _checks += [
        (f"{n}: rows removed = child rows recorded in quarantine", _recorded_child[n], _child_rows_before[n] - _d.count()),
        (f"{n}: quarantined businesses left",                     0,                  _d.join(_q_ids, "business_id", "left_semi").count()),
        (f"{n}: orphan rows (business_id not in df_business)",    0,                  _d.join(_biz_ids, "business_id", "left_anti").count()),
    ]
_checks += [
    ("Quarantine rows = (b) + (c) + XMS",                     _state_quarantine_rows, _q_state.count()),
    ("Quarantine has one row per business",                   _q_state.count(),       _q_ids.distinct().count()),
    ("Quarantine rows without reason / detail / action",      0,                      _q_state.where(F.col("quarantine_reason").isNull() | F.col("action").isNull()
                                                                                         | (F.length(F.coalesce("reason_detail", F.lit(""))) < 300)).count()),
    ("reason_detail mentions the business's own state",       _q_state.count(),       _q_state.where(F.col("reason_detail").contains(F.concat(F.lit("'"), F.col("state"), F.lit("'")))).count()),
    ("Full business record kept in quarantine",               _state_quarantine_rows, _full_record_ok),
    ("Raw bronze record kept (found in bronze)",              _state_quarantine_rows, _q_state.where(F.col("bronze_hours_json").isNotNull() | F.col("bronze_attributes_json").isNotNull() | F.col("bronze_categories").isNotNull()).count()),
    ("No SQL NULL states",                                    0,                      df_business.where(F.col("state").isNull()).count()),
    ("Other columns of the kept rows unchanged",              0,                      _row_unchanged),
]

print("4) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.4 (state) verification failed"
print("✅ MS-01.4 · df_business.state: all checks passed")


# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Explain — downstream orphans
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC #### 🔗 MS-01.4 · Downstream orphans — reviews, tips and check-ins of the 15 quarantined businesses
# MAGIC
# MAGIC **The problem in plain words:** the 15 businesses moved to quarantine still have activity rows in other tables. If those rows stay, they point to a `business_id` that no longer exists in `silver_cleaned.yelp_business` (orphan / foreign-key violation), and their stars, reviews and visits would still be counted for a business whose location cannot be trusted or is outside the dataset.
# MAGIC
# MAGIC | Table | Where the rows live today | Rows of the 15 businesses |
# MAGIC | --- | --- | ---: |
# MAGIC | reviews | `yelp_poc_uc.silver_cleaned.yelp_review` (already built) | 199 |
# MAGIC | check-ins | `yelp_poc_uc.silver_cleaned.yelp_checkin` (already built, one row per visit) | 99 |
# MAGIC | tips | `yelp_poc_uc.bronze_final.yelp_tip` (no Silver tip table yet) | 6 |
# MAGIC
# MAGIC **Solution taken:** for each table, every row of the 15 businesses is copied **unchanged** into a quarantine DataFrame together with a detailed reason:
# MAGIC
# MAGIC | DataFrame | Target table at the end of the notebook |
# MAGIC | --- | --- |
# MAGIC | `df_quarantine_review_business` | `yelp_poc_uc.quarantine.yelp_review_orphan_business` |
# MAGIC | `df_quarantine_checkin_business` | `yelp_poc_uc.quarantine.yelp_checkin_orphan_business` |
# MAGIC | `df_quarantine_tip_business` | `yelp_poc_uc.quarantine.yelp_tip_orphan_business` |
# MAGIC
# MAGIC Each row keeps all its source columns and adds: `quarantine_reason` (`PARENT_BUSINESS_QUARANTINED`), `reason_detail` (full plain-text explanation: which business, why the business was quarantined, the parent's evidence, why the row cannot stay), the parent's reason / DQ issue / pattern / evidence, `source_table`, `detected_in`, `action` and `quarantined_at`. The source is the layer where the rows live today (Silver if the table exists, otherwise bronze).
# MAGIC
# MAGIC **Not done here (nothing is written or deleted in this cell):** removing the rows from `silver_cleaned.yelp_review` / `yelp_checkin` and writing the quarantine tables happen in the final write step. The review / check-in / tip notebooks must also exclude these business_ids when they are re-run (foreign-key check against `silver_cleaned.yelp_business`).
# MAGIC
# MAGIC **Why this is best:** the orphans are isolated with the same level of detail as their parent business, nothing is lost, and every row can be restored together with its business.

# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Fix — downstream orphans
# ============================================================
# 🔧 MS-01.4 · state · Fix — downstream orphans (reviews / check-ins / tips)
# ============================================================
# In memory only: builds one quarantine DataFrame per downstream table with every row of the
# businesses moved to quarantine in MS-01.4 · Fix. Nothing is written or deleted here.
# Source = the layer where the rows live today (Silver if the table exists, otherwise bronze_final).

DOWNSTREAM_SOURCES = {          # kind -> (Silver table, bronze table)
    "review":  ("yelp_poc_uc.silver_cleaned.yelp_review",  "yelp_poc_uc.bronze_final.yelp_review"),
    "checkin": ("yelp_poc_uc.silver_cleaned.yelp_checkin", "yelp_poc_uc.bronze_final.yelp_checkin"),
    "tip":     ("yelp_poc_uc.silver_cleaned.yelp_tip",     "yelp_poc_uc.bronze_final.yelp_tip"),
}

# Parent = the businesses moved to quarantine, with their reason and evidence
_parent = (df_quarantine_business.where(F.col("action") == MOVED_TO_QUARANTINE)
           .select("business_id",
                   F.col("name").alias("parent_business_name"),
                   F.col("quarantine_reason").alias("parent_quarantine_reason"),
                   F.col("dq_issue").alias("parent_dq_issue"),
                   F.col("pattern").alias("parent_pattern"),
                   F.col("note").alias("parent_evidence")))

def _orphan_quarantine(kind):
    silver_tbl, bronze_tbl = DOWNSTREAM_SOURCES[kind]
    src_tbl  = silver_tbl if spark.catalog.tableExists(silver_tbl) else bronze_tbl
    in_silver = src_tbl == silver_tbl
    src = spark.table(src_tbl)
    reason = F.concat(
        F.lit(f"PARENT_BUSINESS_QUARANTINED (orphan {kind}). This {kind} row (source {src_tbl}) belongs to "
              f"business_id '"), F.col("business_id"), F.lit("' ('"), F.col("parent_business_name"),
        F.lit("'), which 01_Business_Cleaning · MS-01.4 moved to yelp_poc_uc.quarantine "
              "(table yelp_business_out_of_scope) with reason "), F.col("parent_quarantine_reason"),
        F.lit(" ("), F.col("parent_dq_issue"), F.lit(", "), F.col("parent_pattern"),
        F.lit("; parent evidence: "), F.col("parent_evidence"),
        F.lit(f"). The business is no longer in silver_cleaned.yelp_business, so keeping this {kind} in Silver "
              f"would leave a business_id without a parent row (orphan / foreign-key violation) and would count "
              f"activity for a business whose location cannot be trusted or lies outside the dataset scope. "
              f"The row is kept here unchanged so it can be restored together with its business. "
              + (f"Action: remove it from {silver_tbl} in the final write step." if in_silver else
                 f"Action: exclude it when {silver_tbl} is built.")))
    return (src.join(F.broadcast(_parent), "business_id", "inner")
            .withColumn("quarantine_reason", F.lit("PARENT_BUSINESS_QUARANTINED"))
            .withColumn("reason_detail", reason)
            .withColumn("dq_issue", F.col("parent_dq_issue"))
            .withColumn("source_table", F.lit(src_tbl))
            .withColumn("detected_in", F.lit("01_Business_Cleaning · MS-01.4 · state (downstream orphans)"))
            .withColumn("action", F.lit(f"TO_BE_REMOVED_FROM_{silver_tbl.split('.')[-1].upper()}" if in_silver
                                        else f"EXCLUDE_WHEN_BUILDING_{silver_tbl.split('.')[-1].upper()}"))
            .withColumn("quarantined_at", F.current_timestamp())
            .select(*src.columns, "quarantine_reason", "reason_detail", "dq_issue",
                    "parent_business_name", "parent_quarantine_reason", "parent_dq_issue",
                    "parent_pattern", "parent_evidence", "source_table", "detected_in",
                    "action", "quarantined_at"))

df_quarantine_review_business  = _orphan_quarantine("review")
df_quarantine_checkin_business = _orphan_quarantine("checkin")
df_quarantine_tip_business     = _orphan_quarantine("tip")

DOWNSTREAM_QUARANTINE = {   # DataFrame variable -> target quarantine table (written at the end)
    "df_quarantine_review_business":  "yelp_poc_uc.quarantine.yelp_review_orphan_business",
    "df_quarantine_checkin_business": "yelp_poc_uc.quarantine.yelp_checkin_orphan_business",
    "df_quarantine_tip_business":     "yelp_poc_uc.quarantine.yelp_tip_orphan_business",
}

for var, tgt in DOWNSTREAM_QUARANTINE.items():
    _d = globals()[var]
    _n = _d.count()
    print(f"🔧 {var:<32}: {_n:>4,} rows from {_d.select('source_table').first()[0] if _n else '-'} -> {tgt}")


# COMMAND ----------

# DBTITLE 1,MS-01.4 · state · Verify — downstream orphans
# ============================================================
# ✅ MS-01.4 · state · Verify — downstream orphans
# ============================================================
# Read-only checks:
#   * every row of the 15 businesses in each source table is in its quarantine DataFrame (nothing missed)
#   * every quarantined row belongs to a moved business and none of them is in df_business
#   * every row has a reason and a detailed reason_detail
#   * counts agree with the downstream counts recorded on the business quarantine rows

# ---- 1) Real rows -------------------------------------------------------
for var in DOWNSTREAM_QUARANTINE:
    _d = globals()[var]
    print("=" * 80)
    print(f"1) {var} — sample rows (text columns hidden)")
    print("   Purpose: see the real orphan rows and their reason.")
    print("=" * 80)
    display(_d.drop(*[c for c in ["text"] if c in _d.columns]).orderBy("parent_business_name").limit(20))

# ---- 2) Rows per business --------------------------------------------
print("=" * 80)
print("2) Orphan rows per quarantined business")
print("   Purpose: compare with the downstream counts on the business quarantine row (bronze rows).")
print("=" * 80)
_per_biz = df_quarantine_business.where(F.col("action") == MOVED_TO_QUARANTINE).select(
    "business_id", "name", "quarantine_reason", "downstream_reviews", "downstream_tips", "downstream_checkins")
for var, label in [("df_quarantine_review_business", "reviews_quarantined"),
                   ("df_quarantine_tip_business", "tips_quarantined"),
                   ("df_quarantine_checkin_business", "checkin_rows_quarantined")]:
    _per_biz = _per_biz.join(globals()[var].groupBy("business_id").count().withColumnRenamed("count", label),
                             "business_id", "left").fillna(0, [label])
display(_per_biz.orderBy("quarantine_reason", "name"))

# ---- 3) Checks ----------------------------------------------------------
_moved_ids = df_quarantine_business.where(F.col("action") == MOVED_TO_QUARANTINE).select("business_id")
_sum = lambda c: int(df_quarantine_business.agg(F.sum(c)).first()[0] or 0)
_checks = []
for var in DOWNSTREAM_QUARANTINE:
    _d   = globals()[var]
    _src = spark.table(_d.select("source_table").first()[0]) if _d.count() else None
    _checks += [
        (f"{var}: rows = all source rows of the moved businesses",
         _src.join(_moved_ids, "business_id", "left_semi").count() if _src is not None else 0, _d.count()),
        (f"{var}: rows whose business is not a moved business", 0, _d.join(_moved_ids, "business_id", "left_anti").count()),
        (f"{var}: rows whose business is still in df_business", 0, _d.join(df_business, "business_id", "left_semi").count()),
        (f"{var}: rows without reason / short reason_detail",  0,
         _d.where(F.col("quarantine_reason").isNull() | (F.length(F.coalesce("reason_detail", F.lit(""))) < 300)).count()),
    ]
_checks += [
    ("Reviews quarantined = downstream_reviews recorded on the businesses", _sum("downstream_reviews"), df_quarantine_review_business.count()),
    ("Tips quarantined = downstream_tips recorded on the businesses",       _sum("downstream_tips"),    df_quarantine_tip_business.count()),
    ("Businesses with check-ins: bronze = quarantined",
     df_quarantine_business.where(F.col("downstream_checkins") > 0).count(),
     df_quarantine_checkin_business.select("business_id").distinct().count()),
]

print("3) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.4 (downstream orphans) verification failed"
print("✅ MS-01.4 · downstream orphans: all checks passed")


# COMMAND ----------

# MAGIC %md
# MAGIC # Summary for what happened above:
# MAGIC
# MAGIC MS-01.4 handles the `state` column of `df_business` and the businesses whose state is outside the dataset's area. It has 6 cells.
# MAGIC
# MAGIC **The problem**
# MAGIC
# MAGIC The Yelp dataset covers 14 states and provinces (`FOOTPRINT_STATES`), but 21 businesses have a state outside that list. The step uses three pieces of evidence to decide each case:
# MAGIC * **The ZIP code:** the first 3 digits identify a US state (`ZIP3_STATE_RANGES`).
# MAGIC * **The coordinates:** each state has a box built from the data (the 0.1%–99.9% latitude and longitude range of its businesses), and the step checks which box the point falls in.
# MAGIC * **The city.**
# MAGIC
# MAGIC **How each business is classified**
# MAGIC
# MAGIC | Pattern | Rule | Rows | Action |
# MAGIC | --- | --- | ---: | --- |
# MAGIC | (a) Only the state is wrong | the ZIP points to a footprint state and the coordinates are inside that state's box | 6 | fix `state` from the ZIP (SD→FL, HI→AZ, UT→ID, 2×TX→TN, MA→PA) |
# MAGIC | (b) `ADDRESS_COORD_CONFLICT` | the address is outside the footprint, the coordinates are inside it | 13 | quarantine |
# MAGIC | (c) `OUT_OF_FOOTPRINT` | the address and coordinates agree, but both are outside (Bennington, VT) | 1 | quarantine |
# MAGIC | `NON_EXISTENT_STATE` | `XMS` is not a real state code; it's a UK address with St. Louis coordinates | 1 | quarantine |
# MAGIC
# MAGIC A value is corrected only when two independent sources agree on the answer. Everything else contradicts itself or is out of scope, so it is isolated instead of guessed.
# MAGIC
# MAGIC **What each cell does**
# MAGIC
# MAGIC 1. **Explain (md):** the problem, the options and the chosen solution.
# MAGIC 2. **Detect:** builds `_state_review`, one row per out-of-footprint business with its ZIP state, the boxes its coordinates fall in, its pattern and its new state or quarantine reason.
# MAGIC 3. **Fix:**
# MAGIC    * Takes snapshots (`_state_before`, `_biz_rows_before`, `_child_rows_before`) for the Verify cell.
# MAGIC    * Applies the 6 pattern (a) corrections.
# MAGIC    * For the 15 quarantined businesses, it counts their rows in the 10 child DataFrames and their bronze reviews, tips and check-ins, and saves their raw bronze attributes, hours and categories.
# MAGIC    * Writes a long plain-text `reason_detail` for each one (`_reason_detail()`): what's wrong, the address, what the ZIP says, where the coordinates fall, why it can't be corrected, and what was removed.
# MAGIC    * Builds `df_quarantine_business` (26 columns, `_Q_SCHEMA`) with the full record and `action = MOVED_TO_QUARANTINE`.
# MAGIC    * Removes the 15 businesses from `df_business` and from all 10 child DataFrames, leaving no orphan rows. This is the approved exception to your "never delete a row" rule.
# MAGIC    * Re-running replaces this step's quarantine rows instead of adding duplicates.
# MAGIC 4. **Verify:** 43 checks, including:
# MAGIC    * only the 6 states changed, and each now equals its ZIP state;
# MAGIC    * the 15 businesses are gone from `df_business` and from every child DataFrame, with removed counts matching the recorded counts;
# MAGIC    * no child row is left without a business;
# MAGIC    * the quarantine has one row per business, a full reason and a record identical to the Detect snapshot;
# MAGIC    * no out-of-footprint or NULL state is left, and the kept rows are otherwise untouched.
# MAGIC 5. **Downstream orphans (Explain, Fix, Verify):**
# MAGIC    * Builds three in-memory quarantine DataFrames with every row of the 15 businesses: 199 reviews and 99 check-ins from Silver, and 6 tips from bronze because there is no Silver tip table yet.
# MAGIC    * Each row gets `PARENT_BUSINESS_QUARANTINED`, a detailed reason and the parent business's evidence.
# MAGIC    * The Verify cell checks that nothing was missed and the counts match.
# MAGIC
# MAGIC **What MS-01.4 does not do**
# MAGIC
# MAGIC Nothing is written to any table. The quarantine tables are written, and the 199 reviews and 99 check-ins deleted from Silver, only in FW-02 at the end of the notebook.
# MAGIC
# MAGIC **Key variables used later**
# MAGIC
# MAGIC * `df_quarantine_business`, `MOVED_TO_QUARANTINE`, `CHILD_DF_NAMES`
# MAGIC * `df_quarantine_review_business`, `df_quarantine_checkin_business`, `df_quarantine_tip_business`
# MAGIC * `DOWNSTREAM_QUARANTINE`, `DOWNSTREAM_SOURCES`
# MAGIC
# MAGIC **Result:** `df_business` goes from 150,346 to 150,331 rows (6 states corrected, 15 businesses quarantined).

# COMMAND ----------

# DBTITLE 1,MS-01.5 · postal_code · Explain
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 MS-01.5 · `df_business.postal_code` — Empty, Malformed and FSA-Only Postal Codes (DQ-BUSINESS-015 / 016 / 014)
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **DQ Issues** | `DQ-BUSINESS-015` Empty-string postal codes (masked NULL) · `DQ-BUSINESS-016` Malformed postal codes (typos) · `DQ-BUSINESS-014` Truncated Canadian codes (FSA only) |
# MAGIC | **Column** | `df_business.postal_code` (uses `state` to know the country: `AB` = Canada, every other state = USA) |
# MAGIC | **Rows affected** | empty `''` **73** · malformed **4** (`T6x 2H4`, `T6T 0G`, `T8A 5WA`, `336140`; the 5th, `L3 5TB`, left with the XMS business in MS-01.4) · FSA only **196** (kept) |
# MAGIC | **Decision** | Ibrahim Mohamed, 2026-10-26: invalid codes → SQL NULL; no repair, no truncation, no quarantine. The **business row stays** in `df_business`; only the value becomes NULL |
# MAGIC | **Valid formats** | USA `12345` or `12345-6789` · Canada `A1A 1A1` or the FSA `A1A` alone. Canada Post never uses `D F I O Q U`, and `W Z` never as the first letter |
# MAGIC | **Details** | `01_Business_Value_Inventory_Findings.md` → `postal_code` |
# MAGIC
# MAGIC The Detect cell counts on the current `df_business`, i.e. after the 15 businesses moved to quarantine in MS-01.4.
# MAGIC
# MAGIC **The problem in plain words:** 73 businesses have a blank postal code. It looks like a value, so completeness reports say 100 % filled, but it means "unknown" (56 of them also have a blank address: service-area businesses that hide their location). 4 codes are typos: a lower-case letter, a Canadian code missing its last digit, a letter where a digit belongs, and a 6-digit US ZIP. 196 Edmonton businesses have only the first 3 characters of the Canadian code (the district, called FSA): that is correct, just less precise.
# MAGIC
# MAGIC **Suggested solutions (DQ document):**
# MAGIC
# MAGIC | # | Solution | Comment |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | `''` → NULL; upper-case; truncate broken Canadian codes to their FSA; NULL the rest | Keeps the readable district, but the kept characters may themselves hold the typo |
# MAGIC | 2 | Fill / complete codes from the coordinates or from nearby businesses | Needs a geocoder or a neighbour rule; the filled code may not be the business's mailing code |
# MAGIC | 3 | **Upper-case what is only a case problem; every other invalid code → SQL NULL** | Nothing invented or partly kept; the original value is still in bronze |
# MAGIC | 4 | Pad FSA-only codes (`T5J 0A0`) or guess the 6-digit ZIP (`33614`) | Invents values |
# MAGIC | 5 | Drop the rows | ❌ Rejected — cleaning never deletes rows (rule R-1) |
# MAGIC
# MAGIC **Solution taken:** **Option 3** (your decision), one rule per case:
# MAGIC
# MAGIC | Value | Becomes | Rule |
# MAGIC | --- | --- | --- |
# MAGIC | `''` (73) | SQL NULL | empty string is a masked NULL (DQ-015) |
# MAGIC | `T6x 2H4` | `T6X 2H4` | only the case is wrong; 2 other units in the same building have `T6X 2H4` (DQ-016) |
# MAGIC | `T6T 0G` | SQL NULL | 5 characters instead of 6; nearby codes `T6T 0G9` and `T6T 0G6` both fit, so the code can't be known (DQ-016) |
# MAGIC | `T8A 5WA` | SQL NULL | character 6 is a letter where a digit is required, so no real code has this form (DQ-016) |
# MAGIC | `336140` | SQL NULL | 6 digits; `33614` is a real Tampa ZIP, but the businesses at the same address use `33647`, so dropping a digit is a guess (DQ-016) |
# MAGIC | FSA only, e.g. `T5J` (196) | unchanged | correct district code, accepted as a valid Canadian format (DQ-014) |
# MAGIC
# MAGIC The rule is general, not a list of values: trim, upper-case, empty → NULL; Canada (`AB`): keep a valid full code or FSA, else NULL; USA: keep a valid ZIP, else NULL. The Detect cell shows, for each invalid code, why it is invalid (`_postal_why()`).
# MAGIC
# MAGIC **Not done here:** no check of the ZIP against the state (DQ-023, Silver Enriched), no completion of FSA-only codes, no code filled from coordinates or neighbours.
# MAGIC
# MAGIC **Why this is best:** every remaining code is valid for its country and fully true; nothing is guessed or partly kept; the 3 invalid originals stay available in bronze. No row is deleted.

# COMMAND ----------

# DBTITLE 1,MS-01.5 · postal_code · Detect
# ============================================================
# 🔎 MS-01.5 · postal_code · Detect — df_business.postal_code
# ============================================================
# Read-only. Classifies every postal code, shows real rows per problem and what the Fix will do.
# Defines the rules used by MS-01.5 · Fix:
#   CA_STATES                       : states whose codes are Canadian (all others are US ZIP codes)
#   CA_FULL_RE / CA_FSA_RE / US_ZIP_RE : the valid formats
#   _postal_class()                 : class of each value (valid / problem)
#   _postal_rule()                  : the cleaned value (used by clean_postal_code() in the Fix cell)
#   POSTAL_INVALID_CLASSES          : invalid codes -> SQL NULL (business row kept)
#   _postal_why()                   : plain-text reason why a code is invalid (shown in section 3)
# Decision (Ibrahim Mohamed, 2026-10-26): an invalid code is NOT repaired or truncated (no 'T6T 0G' -> 'T6T');
# it becomes SQL NULL and the business row is kept. No quarantine for invalid values.

CA_STATES  = ["AB"]
_CA_FSA    = r"[ABCEGHJ-NPRSTVXY][0-9][ABCEGHJ-NPRSTV-Z]"     # no D F I O Q U; no W Z as 1st letter
_CA_LDU    = r"[0-9][ABCEGHJ-NPRSTV-Z][0-9]"
CA_FULL_RE = f"^{_CA_FSA} {_CA_LDU}$"                          # T5J 1B9
CA_FSA_RE  = f"^{_CA_FSA}$"                                    # T5J
US_ZIP_RE  = r"^[0-9]{5}(-[0-9]{4})?$"                         # 33606 / 33606-1234

def _postal_class(p=F.col("postal_code"), state=F.col("state")):
    t, is_ca = F.trim(p), state.isin(CA_STATES)
    u = F.upper(t)
    return (F.when(p.isNull(),                                   F.lit("SQL NULL"))
             .when(t == "",                                      F.lit("Empty string"))
             .when(~is_ca & t.rlike(US_ZIP_RE),                  F.lit("US ZIP (valid)"))
             .when(~is_ca,                                       F.lit("US ZIP malformed"))
             .when(t.rlike(CA_FULL_RE),                          F.lit("CA full code (valid)"))
             .when(t.rlike(CA_FSA_RE),                           F.lit("CA FSA only (valid, kept)"))
             .when(u.rlike(CA_FULL_RE) | u.rlike(CA_FSA_RE),     F.lit("CA lower-case letters"))
             .otherwise(                                         F.lit("CA malformed")))

def _postal_rule(p=F.col("postal_code"), state=F.col("state")):
    u, is_ca = F.upper(F.trim(p)), state.isin(CA_STATES)
    ca = F.when(u.rlike(CA_FULL_RE) | u.rlike(CA_FSA_RE), u)           # valid full code / FSA (upper-cased), else NULL
    us = F.when(u.rlike(US_ZIP_RE), u)                                 # valid ZIP, else NULL
    return F.when(u == "", F.lit(None).cast("string")).when(is_ca, ca).otherwise(us)

POSTAL_PROBLEM_CLASSES    = ["Empty string", "US ZIP malformed", "CA lower-case letters", "CA malformed"]
POSTAL_INVALID_CLASSES    = ["US ZIP malformed", "CA malformed"]      # -> SQL NULL
_pc_class = _postal_class()
_pc_new   = _postal_rule()

def _postal_why(v, is_ca):
    """Plain-text reason why a postal code is invalid (built from the value, not from a list)."""
    u = (v or "").strip().upper()
    if is_ca:
        s, pattern, why = u.replace(" ", ""), "LNLNLN", []
        if len(s) not in (3, 6):
            why.append(f"it has {len(s)} letters/digits instead of 6 (A1A 1A1), so part of the code is missing or extra")
        for i, ch in enumerate(s[:6]):
            want = "letter" if pattern[i] == "L" else "digit"
            if (want == "letter" and not ch.isalpha()) or (want == "digit" and not ch.isdigit()):
                why.append(f"character {i + 1} is '{ch}' where a {want} is required")
            elif want == "letter" and ch in "DFIOQU":
                why.append(f"character {i + 1} is '{ch}', a letter Canada Post never uses")
        return "; ".join(why) or "it does not match A1A 1A1 or the district code A1A"
    if not u.replace("-", "").isdigit():
        return f"it contains characters other than digits"
    return (f"it has {len(u.replace('-', ''))} digits instead of 5 (or 5+4 written 12345-6789), "
            f"and it is not possible to tell which digit is wrong or extra")

# ---- 1) Class counts -----------------------------------------------------
print("=" * 80)
print("1) Every postal code by class")
print("   Purpose: size of each problem; the valid classes are left unchanged.")
print("=" * 80)
_pc_counts = {r["class"]: r["count"] for r in
              df_business.groupBy(_pc_class.alias("class")).count().collect()}
display(spark.createDataFrame(
    [(c, int(n), "problem" if c in POSTAL_PROBLEM_CLASSES else "ok") for c, n in sorted(_pc_counts.items())],
    "Class string, Rows long, Status string"))

# ---- 2) Real rows per problem ----------------------------------------------
for i, label in enumerate(POSTAL_PROBLEM_CLASSES + ["CA FSA only (valid, kept)"], start=1):
    print("=" * 80)
    print(f"2.{i}) Sample rows — {label}")
    print("   Purpose: see the real postal codes and the value the Fix will write.")
    print("=" * 80)
    display(df_business.where(_pc_class == label)
            .select("business_id", "name", "address", "city", "state",
                    F.concat(F.lit("["), F.col("postal_code"), F.lit("]")).alias("Postal code in brackets"),
                    _pc_new.alias("After the Fix"))
            .orderBy("state", "city").limit(30))

# ---- 3) Every non-empty problem value --------------------------------------
print("=" * 80)
print("3) Every malformed postal code, why it is invalid and its planned value")
print("   Purpose: check by eye that only the case changes or the value becomes NULL; nothing is repaired or guessed.")
print("=" * 80)
_pc_bad = (df_business.where(_pc_class.isin(POSTAL_PROBLEM_CLASSES) & (_pc_class != "Empty string"))
           .select("business_id", "name", "city", "state", "postal_code",
                   _pc_class.alias("Class"), _pc_new.alias("After the Fix")))
display(spark.createDataFrame(
    [(r["business_id"], r["name"], r["city"], r["state"], r["postal_code"], r["Class"], r["After the Fix"],
      _postal_why(r["postal_code"], r["state"] in CA_STATES) if r["Class"] in POSTAL_INVALID_CLASSES else "only lower-case letters")
     for r in _pc_bad.collect()],
    "business_id string, name string, city string, state string, `Postal code (as stored)` string, Class string, "
    "`After the Fix` string, `Why` string"))

# ---- 4) Numbers used by Fix / Verify ----------------------------------------
_pc_issue_rows   = df_business.where(_pc_class.isin(POSTAL_PROBLEM_CLASSES)).count()
_pc_to_null      = df_business.where(F.col("postal_code").isNotNull() & _pc_new.isNull()).count()
_pc_null_before  = _pc_counts.get("SQL NULL", 0)
_pc_fsa_before   = _pc_counts.get("CA FSA only (valid, kept)", 0)
_pc_invalid      = sum(_pc_counts.get(c, 0) for c in POSTAL_INVALID_CLASSES)
_pc_unexpected   = df_business.where(F.col("postal_code").isNotNull() & ~_pc_class.isin(POSTAL_PROBLEM_CLASSES)
                                     & ~F.col("postal_code").eqNullSafe(_pc_new)).count()
print(f"Total postal codes: {df_business.count():,} | rows with a problem: {_pc_issue_rows:,} "
      f"| will become SQL NULL: {_pc_to_null:,} | invalid -> NULL: {_pc_invalid:,} | FSA kept: {_pc_fsa_before:,} "
      f"| valid codes the rule would change (must be 0): {_pc_unexpected:,}")


# COMMAND ----------

# DBTITLE 1,MS-01.5 · postal_code · Fix
# ============================================================
# 🔧 MS-01.5 · postal_code · Fix — df_business.postal_code
# ============================================================
# Applied to df_business in place. No row is deleted.
#   1. trim + upper-case; '' -> SQL NULL                                     (DQ-015)
#   2. Canada (state in CA_STATES): valid full code / FSA kept (T6x 2H4 -> T6X 2H4), FSA-only kept (DQ-014);
#      invalid (T6T 0G, T8A 5WA) -> SQL NULL, no repair / truncation       (DQ-016)
#   3. USA: valid ZIP kept; invalid (336140) -> SQL NULL                     (DQ-016)
# The business row always stays in df_business; only the value becomes NULL. No quarantine.
# Uses CA_STATES / regexes / _postal_rule() from MS-01.5 · Detect.
# Idempotent: running the cell again changes nothing.

def clean_postal_code(col=F.col("postal_code"), state=F.col("state")):
    return _postal_rule(col, state)

# Before copy, kept only for the verify cell
_pc_before = df_business.select("business_id", "state", F.col("postal_code").alias("postal_before"))

df_business = df_business.withColumn("postal_code", clean_postal_code())

_pc_changed = (df_business.select("business_id", F.col("postal_code").alias("postal_after"))
               .join(_pc_before, "business_id")
               .where(~F.col("postal_before").eqNullSafe(F.col("postal_after"))))
print(f"🔧 MS-01.5 df_business.postal_code cleaned | codes changed: {_pc_changed.count():,} "
      f"(expected {_pc_issue_rows:,} from the Detect cell) | now SQL NULL: "
      f"{df_business.where(F.col('postal_code').isNull()).count():,} (expected {_pc_null_before + _pc_to_null:,}) "
      f"| rows: {df_business.count():,}")


# COMMAND ----------

# DBTITLE 1,MS-01.5 · postal_code · Verify
# ============================================================
# ✅ MS-01.5 · postal_code · Verify — df_business.postal_code
# ============================================================
# Read-only checks:
#   * every problem class from the Detect cell is now 0 (re-classified on the cleaned value)
#   * every remaining code is valid for its country (US ZIP / CA full code / CA FSA)
#   * row count unchanged; SQL NULLs = before + the planned ones
#   * only the problem rows changed; FSA-only codes kept; a kept value only changed its case (nothing repaired)
#   * every invalid code is now SQL NULL and its business is still in df_business (row kept)
#   * the rule is idempotent

# ---- 1) Before / after for the changed non-empty codes ------------------------
print("1) Before / after — changed postal codes (the '' -> NULL rows are counted in section 2, not listed)")
print("   Purpose: see that a code only changed its case or became SQL NULL (invalid, not repaired).")
display(_pc_changed.where(F.trim(F.col("postal_before")) != "")
        .join(df_business.select("business_id", "name", "city"), "business_id")
        .select("business_id", "name", "city", "state",
                F.col("postal_before").alias("Postal code before"), F.col("postal_after").alias("Postal code after"))
        .orderBy("state", "city"))

# ---- 2) Class counts after -------------------------------------------------
_pc_after_counts = {r["class"]: r["count"] for r in
                    df_business.groupBy(_postal_class().alias("class")).count().collect()}
print("2) Postal code classes — before vs after")
print("   Purpose: problem classes go to 0; their rows move to SQL NULL or to a valid class.")
display(spark.createDataFrame(
    [(c, int(_pc_counts.get(c, 0)), int(_pc_after_counts.get(c, 0)))
     for c in sorted(set(_pc_counts) | set(_pc_after_counts))],
    "Class string, Rows_before long, Rows_after long"))

# ---- 3) Checks ----------------------------------------------------------
_pc_now = F.col("postal_code")
_is_ca  = F.col("state").isin(CA_STATES)
_invalid_now = df_business.where(_pc_now.isNotNull() & ~(
    (_is_ca & (_pc_now.rlike(CA_FULL_RE) | _pc_now.rlike(CA_FSA_RE))) | (~_is_ca & _pc_now.rlike(US_ZIP_RE))))
_pc_join = _pc_before.join(df_business.select("business_id", F.col("postal_code").alias("postal_after")), "business_id")
_repaired = _pc_join.where(F.col("postal_after").isNotNull() &
                           (F.upper(F.trim(F.col("postal_before"))) != F.col("postal_after")))

# The invalid codes from the Detect cell (before values), looked up again in df_business after the Fix
_pc_inv_ids = (_pc_before.where(_postal_class(F.col("postal_before")).isin(POSTAL_INVALID_CLASSES))
               .select("business_id"))
_pc_inv_now = _pc_inv_ids.join(df_business.select("business_id", "postal_code"), "business_id")

_checks = [(f"Problem class left: {c}", 0, _pc_after_counts.get(c, 0)) for c in POSTAL_PROBLEM_CLASSES]
_checks += [
    ("Codes not valid for their country",                          0,                              _invalid_now.count()),
    ("Row count unchanged",                                        _pc_before.count(),             df_business.count()),
    ("Changed codes = problem rows from Detect",                   _pc_issue_rows,                 _pc_changed.count()),
    ("SQL NULL codes = before + planned",                          _pc_null_before + _pc_to_null,  df_business.where(_pc_now.isNull()).count()),
    ("No empty-string codes",                                      0,                              df_business.where(F.trim(_pc_now) == "").count()),
    ("FSA-only codes kept unchanged",                              _pc_fsa_before,                 _pc_after_counts.get("CA FSA only (valid, kept)", 0)),
    ("Kept values differ from the old one by more than case",      0,                              _repaired.count()),
    ("Invalid codes found = invalid codes from Detect",            _pc_invalid,                    _pc_inv_ids.count()),
    ("Invalid codes: business still in df_business (row kept)",    _pc_invalid,                    _pc_inv_now.count()),
    ("Invalid codes: postal_code not NULL after the Fix",          0,                              _pc_inv_now.where(F.col("postal_code").isNotNull()).count()),
    ("Valid codes changed by the rule (Detect)",                   0,                              _pc_unexpected),
    ("Idempotent: running the rule again changes nothing",         0,                              df_business.where(~_pc_now.eqNullSafe(clean_postal_code())).count()),
]

print("3) Verification checks")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ MS-01.5 (postal_code) verification failed"
print("✅ MS-01.5 · df_business.postal_code: all checks passed")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,FW-00 · Final Write — What Happens
# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC
# MAGIC # 🏁 FINAL WRITE — Silver Business Tables, Quarantine Tables and Silver Orphan Deletes
# MAGIC
# MAGIC > ⚠️ **This is the only part of the notebook that writes to Unity Catalog.** Every cell above works in memory only. Keep all new cleaning steps (MS-01.5 …) **above** the empty separator cells, so the final write always runs last.
# MAGIC
# MAGIC **When to run it:** only after all cleaning steps are finished and verified, and after the Silver DDL query `01_Business_Silver_Cleaned` has been run (it creates the target tables with the contract schema). The gate in FW-01 checks this and blocks the write if anything is missing.
# MAGIC
# MAGIC | Cell | Writes? | What it does |
# MAGIC | --- | --- | --- |
# MAGIC | **FW-01 · Pre-write gate** | No (read-only) | Checks every DataFrame against its target table and sets `FINAL_WRITE_GATE_PASSED`. Shows the FAIL rows first. |
# MAGIC | **FW-02 · Write** | **Yes: writes and deletes** | Runs only when `FINAL_WRITE_GATE_PASSED` is `True`. Three steps in a fixed order (below). |
# MAGIC | **FW-03 · Post-write verification** | No (read-only) | Reads the tables back and proves the result. |
# MAGIC
# MAGIC **What the gate (FW-01) checks**
# MAGIC
# MAGIC | Area | Checks |
# MAGIC | --- | --- |
# MAGIC | 1 Silver business tables (11) | the target table exists; no target column is missing in the DataFrame; no extra column (except the working-only `raw_dict`); every column already has the **target type** (no silent cast at write time); primary key unique and not NULL; none of the 15 quarantined businesses is present; no child row without a parent in `df_business` |
# MAGIC | 2 Quarantine tables (4) | one row per moved business; every row has `quarantine_reason` and `reason_detail`; every row belongs to a moved business |
# MAGIC | 3 Silver orphan deletes | for `silver_cleaned.yelp_review`, `yelp_checkin` (and `yelp_tip` once it exists): the rows that will be deleted are exactly the rows already captured in the quarantine DataFrames |
# MAGIC
# MAGIC **What the write (FW-02) does — the order matters**
# MAGIC
# MAGIC | Step | Action | Tables | Why this order |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **1** | Write the quarantine tables (overwrite) | `quarantine.yelp_business_out_of_scope` (15) · `yelp_review_orphan_business` (199) · `yelp_checkin_orphan_business` (99) · `yelp_tip_orphan_business` (6) | Every row is safely stored **before** anything is deleted |
# MAGIC | **2** | `DELETE` the orphan rows from Silver | `silver_cleaned.yelp_review` (199 rows) · `silver_cleaned.yelp_checkin` (99 rows) | The rows of the 15 businesses leave Silver; the delete is driven by the stored quarantine table, and the deleted count must equal the planned count |
# MAGIC | **3** | Write the 11 Silver business tables (`INSERT OVERWRITE`) | `yelp_business` · `yelp_business_attributes` · `yelp_business_hours` · `yelp_business_categories` · the 7 dict tables | One atomic Delta commit per table that keeps the DDL schema, constraints and table properties (no `overwriteSchema`) |
# MAGIC
# MAGIC The counts in brackets are today's values; the cells compute them at run time.
# MAGIC
# MAGIC **Re-running is safe:** if the Silver orphan rows were already deleted by an earlier run, the downstream quarantine DataFrames are empty, and FW-02 **keeps** the stored quarantine table instead of replacing it with nothing; the delete then removes 0 rows.
# MAGIC
# MAGIC **What the post-write verification (FW-03) proves:** the row counts in every table equal the DataFrames; no quarantined business is left in any Silver table; no review / check-in / tip row and no child row points to a business that is not in `silver_cleaned.yelp_business` (FK integrity); bronze businesses = Silver businesses + quarantined businesses.
# MAGIC
# MAGIC **Not done here:** `03_Review_Cleaning` and `02_Checkin_Cleaning` rebuild their Silver tables from bronze. Their business FK check (Phase 4.5 / Phase 2.5) quarantines the same rows again, so **re-run them after every final write of this notebook**. The tip notebook does not exist yet; it must apply the same check.

# COMMAND ----------

# DBTITLE 1,FW-01 · Final Write · Pre-write gate (read-only)
# ============================================================
# 🏁 FW-01 · Final Write · Pre-write gate (READ-ONLY)
# ============================================================
# Nothing is written here. Checks that every DataFrame is ready for its Silver / quarantine table,
# and that the Silver orphan rows to delete are exactly the rows already captured in quarantine.
# Sets FINAL_WRITE_GATE_PASSED (True / False). FW-02 refuses to write unless it is True.
from functools import reduce

SILVER_SCHEMA     = "yelp_poc_uc.silver_cleaned"
QUARANTINE_SCHEMA = "yelp_poc_uc.quarantine"

SILVER_TARGETS = {   # DataFrame -> (Silver table, primary key)
    "df_business":             ("yelp_business",                      ["business_id"]),
    "df_attributes":           ("yelp_business_attributes",           ["business_id"]),
    "df_hours":                ("yelp_business_hours",                ["business_id"]),
    "df_categories":           ("yelp_business_categories",           ["business_id", "category_name"]),
    "df_ambience":             ("yelp_business_ambience",             ["business_id"]),
    "df_best_nights":          ("yelp_business_best_nights",          ["business_id"]),
    "df_business_parking":     ("yelp_business_parking",              ["business_id"]),
    "df_dietary_restrictions": ("yelp_business_dietary_restrictions", ["business_id"]),
    "df_good_for_meal":        ("yelp_business_good_for_meal",        ["business_id"]),
    "df_hair_specializes_in":  ("yelp_business_hair_specializes_in",  ["business_id"]),
    "df_music":                ("yelp_business_music",                ["business_id"]),
}
HELPER_COLS = {"raw_dict"}      # working-only columns, never written to Silver

QUARANTINE_TARGETS = {"df_quarantine_business": f"{QUARANTINE_SCHEMA}.yelp_business_out_of_scope",
                      **DOWNSTREAM_QUARANTINE}
ORPHAN_SILVER_TABLES = [DOWNSTREAM_SOURCES[k][0] for k in ("review", "checkin", "tip")]

_gate = []   # (area, object, check, expected, actual, result)
def _g(area, obj, check, expected, actual):
    _gate.append((area, obj, check, str(expected), str(actual), "PASS" if str(expected) == str(actual) else "FAIL"))

_moved   = df_quarantine_business.where(F.col("action") == MOVED_TO_QUARANTINE).select("business_id").distinct()
_moved_n = _moved.count()
_biz_ids = df_business.select("business_id")

# ---- 1) Silver business tables --------------------------------------------
for var, (tbl, pk) in SILVER_TARGETS.items():
    fq = f"{SILVER_SCHEMA}.{tbl}"
    d  = globals()[var]
    exists = spark.catalog.tableExists(fq)
    _g("1 Silver", fq, "target table exists (DDL applied)", True, exists)
    if exists:
        tgt, src = dict(spark.table(fq).dtypes), dict(d.dtypes)
        _g("1 Silver", fq, "target columns missing in the DataFrame", "-",
           ", ".join(sorted(set(tgt) - set(src))) or "-")
        _g("1 Silver", fq, "DataFrame columns not in the target", "-",
           ", ".join(sorted(set(src) - set(tgt) - HELPER_COLS)) or "-")
        _g("1 Silver", fq, "columns whose type differs (DataFrame -> target)", "-",
           ", ".join(f"{c}: {src[c]} -> {tgt[c]}" for c in tgt if c in src and src[c] != tgt[c]) or "-")
    if all(c in d.columns for c in pk):
        _g("1 Silver", fq, f"duplicate primary keys ({', '.join(pk)})", 0,
           d.groupBy(*pk).count().where("count > 1").count())
        _g("1 Silver", fq, "NULL primary key values", 0,
           d.where(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in pk])).count())
    else:
        _g("1 Silver", fq, f"primary key columns present ({', '.join(pk)})", "yes", "no")
    _g("1 Silver", fq, "quarantined businesses present", 0, d.join(_moved, "business_id", "left_semi").count())
    if var != "df_business":
        _g("1 Silver", fq, "orphan rows (business_id not in df_business)", 0,
           d.join(_biz_ids, "business_id", "left_anti").count())

# ---- 2) Quarantine tables -----------------------------------------------
_g("2 Quarantine", QUARANTINE_TARGETS["df_quarantine_business"], "one row per moved business",
   _moved_n, df_quarantine_business.count())
for var, tbl in QUARANTINE_TARGETS.items():
    d = globals()[var]
    _g("2 Quarantine", tbl, "rows without quarantine_reason / reason_detail", 0,
       d.where(F.col("quarantine_reason").isNull() | F.col("reason_detail").isNull()).count())
    _g("2 Quarantine", tbl, "rows whose business is not a moved business", 0,
       d.join(_moved, "business_id", "left_anti").count())

# ---- 3) Silver orphan rows that FW-02 will delete --------------------------
_delete_plan = {}    # Silver table -> rows of the moved businesses to delete
for t in ORPHAN_SILVER_TABLES:
    if not spark.catalog.tableExists(t):
        _g("3 Silver delete", t, "table not built yet -> nothing to delete", "-", "-")
        continue
    _delete_plan[t] = spark.table(t).join(_moved, "business_id", "left_semi").count()
    _captured = sum(globals()[v].where(F.col("source_table") == t).count() for v in DOWNSTREAM_QUARANTINE)
    _g("3 Silver delete", t, "rows to delete = rows captured in quarantine", _captured, _delete_plan[t])

# ---- 4) Result --------------------------------------------------------------
print("FW-01) Pre-write gate — FAIL rows first")
print("   Purpose: every row must be PASS before FW-02 is allowed to write.")
display(spark.createDataFrame(_gate, "area string, object string, check string, expected string, actual string, result string")
        .orderBy("result", "area", "object"))

FINAL_WRITE_GATE_PASSED = all(r[5] == "PASS" for r in _gate)
_n_fail = sum(r[5] == "FAIL" for r in _gate)
print(f"Silver orphan rows planned for delete: {_delete_plan}")
print("✅ FW-01 gate PASSED — FW-02 may write" if FINAL_WRITE_GATE_PASSED else
      f"⛔ FW-01 gate FAILED ({_n_fail} checks) — FW-02 will not write. Fix the FAIL rows above.")


# COMMAND ----------

# DBTITLE 1,FW-02 · Final Write · Write quarantine, delete Silver orphans, write Silver
# ============================================================
# 🏁 FW-02 · Final Write · WRITE  (⚠️ writes and deletes)
# ============================================================
#   Step 1  write the 4 quarantine tables FIRST  (nothing is deleted before it is safely stored)
#   Step 2  DELETE the rows of the quarantined businesses from Silver review / check-in / tip
#   Step 3  INSERT OVERWRITE the 11 Silver business tables (keeps DDL schema, constraints, properties)
# Runs only when FW-01 passed in this session.

assert globals().get("FINAL_WRITE_GATE_PASSED") is True, (
    "❌ FW-01 gate has not passed — nothing was written. Fix the FAIL rows of FW-01 and re-run it.")

_fw_log = []   # (step, table, operation, expected rows, actual rows)

# ---- Step 1: quarantine tables ---------------------------------------------
for var, tbl in QUARANTINE_TARGETS.items():
    d = globals()[var]
    n = d.count()
    existing = spark.table(tbl).count() if spark.catalog.tableExists(tbl) else 0
    if n == 0 and existing > 0:
        # Re-run after an earlier final write: the Silver source rows are already deleted,
        # so the DataFrame is empty. Keep the stored rows instead of replacing them with nothing.
        _fw_log.append(("1 quarantine", tbl, "kept (already stored by an earlier run)", existing, existing))
        continue
    d.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl)
    w = spark.table(tbl).count()
    assert w == n, f"❌ {tbl}: {w:,} rows stored, expected {n:,}"
    _fw_log.append(("1 quarantine", tbl, "overwrite", n, w))

# ---- Step 2: delete the orphan rows from Silver -------------------------------
_moved_tbl = QUARANTINE_TARGETS["df_quarantine_business"]
for t, n_plan in _delete_plan.items():
    before = spark.table(t).count()
    spark.sql(f"""
        DELETE FROM {t}
        WHERE business_id IN (SELECT business_id FROM {_moved_tbl} WHERE action = '{MOVED_TO_QUARANTINE}')
    """)
    deleted = before - spark.table(t).count()
    assert deleted == n_plan, f"❌ {t}: deleted {deleted:,} rows, planned {n_plan:,}"
    _fw_log.append(("2 delete Silver orphans", t, "DELETE (business in quarantine)", n_plan, deleted))

# ---- Step 3: Silver business tables -----------------------------------------
for var, (tbl, _) in SILVER_TARGETS.items():
    fq   = f"{SILVER_SCHEMA}.{tbl}"
    cols = spark.table(fq).columns                       # target column order (insertInto is positional)
    d    = globals()[var].select(*cols)
    n    = d.count()
    d.write.insertInto(fq, overwrite=True)               # one atomic Delta commit, DDL kept
    w = spark.table(fq).count()
    assert w == n, f"❌ {fq}: {w:,} rows stored, expected {n:,}"
    _fw_log.append(("3 Silver", fq, "INSERT OVERWRITE", n, w))

SILVER_WRITE_DONE = True
print("FW-02) Write log")
print("   Purpose: every step with the rows expected and the rows actually stored / deleted.")
display(spark.createDataFrame(_fw_log, "step string, table string, operation string, expected_rows long, actual_rows long"))
print("✅ FW-02 final write complete — run FW-03 to verify")


# COMMAND ----------

# DBTITLE 1,FW-03 · Final Write · Post-write verification
# ============================================================
# ✅ FW-03 · Final Write · Post-write verification (READ-ONLY)
# ============================================================
# Reads the stored tables back (not the DataFrames) and proves:
#   * every Silver business table holds exactly the rows of its DataFrame
#   * no quarantined business is left in any Silver table (business, children, review, check-in, tip)
#   * FK integrity: every child / review / check-in / tip row has a parent in silver_cleaned.yelp_business
#   * quarantine tables are complete; bronze businesses = Silver + quarantined businesses

assert globals().get("SILVER_WRITE_DONE") is True, "❌ Run FW-02 first"

_sv_biz   = spark.table(f"{SILVER_SCHEMA}.yelp_business").select("business_id")
_q_biz    = spark.table(QUARANTINE_TARGETS["df_quarantine_business"])
_q_moved  = _q_biz.where(F.col("action") == MOVED_TO_QUARANTINE).select("business_id")
_checks = []

for var, (tbl, _) in SILVER_TARGETS.items():
    t = spark.table(f"{SILVER_SCHEMA}.{tbl}")
    _checks += [
        (f"{tbl}: rows stored = rows of {var}",   globals()[var].count(), t.count()),
        (f"{tbl}: quarantined businesses left",  0, t.join(_q_moved, "business_id", "left_semi").count()),
    ]
    if tbl != "yelp_business":
        _checks.append((f"{tbl}: rows without a parent business (FK)", 0, t.join(_sv_biz, "business_id", "left_anti").count()))

for t in ORPHAN_SILVER_TABLES:
    if spark.catalog.tableExists(t):
        _d = spark.table(t)
        _checks += [
            (f"{t.split('.')[-1]}: quarantined businesses left",         0, _d.join(_q_moved, "business_id", "left_semi").count()),
            (f"{t.split('.')[-1]}: rows without a parent business (FK)", 0, _d.join(_sv_biz, "business_id", "left_anti").count()),
        ]

for step, tbl, op, exp, act in _fw_log:
    if step == "1 quarantine":
        _qt = spark.table(tbl)
        _checks += [
            (f"{tbl.split('.')[-1]}: rows stored = rows written", exp, _qt.count()),
            (f"{tbl.split('.')[-1]}: rows without reason / reason_detail", 0,
             _qt.where(F.col("quarantine_reason").isNull() | F.col("reason_detail").isNull()).count()),
        ]

_bronze_biz = spark.table("yelp_poc_uc.bronze_final.yelp_business").count()
_checks.append(("bronze businesses = Silver businesses + quarantined businesses",
                _bronze_biz, _sv_biz.count() + _q_moved.count()))

print("FW-03) Post-write verification")
print("   Purpose: every check must be PASS.")
display(spark.createDataFrame(
    [(c, int(e), int(a), "PASS" if int(e) == int(a) else "FAIL") for c, e, a in _checks],
    "Check string, Expected long, Actual long, Result string"))

assert all(int(e) == int(a) for _, e, a in _checks), "❌ FW-03 post-write verification failed"
print("✅ FW-03 · final write verified — re-run 02_Checkin_Cleaning and 03_Review_Cleaning next")
