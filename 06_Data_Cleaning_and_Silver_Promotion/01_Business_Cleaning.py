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
# MAGIC > **Quarantine Table:** `yelp_poc_uc.quarantine.yelp_business_out_of_scope`  
# MAGIC > **DQ Reference:** `01_Business_DQ_Issues_and_Solutions.md` (`DQ-BUSINESS-001` … `DQ-BUSINESS-062`)
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
# MAGIC > The only accepted row exclusion in the whole project is in the **review** table: reviews whose `text` is NULL, because the text is required by the ML model. It does not apply to this notebook.
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
# MAGIC | **DQ-BUSINESS-003** | Mixed `u'x'` / `'x'` quoting in enum attributes (27,610 businesses) | Anchored regex unquote, then enum-domain check |
# MAGIC | **DQ-BUSINESS-004 / 017 / 029** | Leading/trailing whitespace, invisible Unicode (U+200B, U+00A0), repeated spaces and dangling separators in `name`, `address`, `city` | One ordered text-hygiene step: remove/replace invisible chars → collapse spaces → strip dangling separators → `trim()` |
# MAGIC | **DQ-BUSINESS-005** | Same category repeated within a business (43 businesses) | Deduplicate on the composite PK, keep `MIN(category_position)` |
# MAGIC | **DQ-BUSINESS-013** | 4 Yelp labels containing `, ` split into fake categories (4,150 businesses, 9,371 fragment rows) | Whitelist-protected split (`Beer, Wine & Spirits`, `Books, Mags, Music & Video`, `Used, Vintage & Consignment`, `Wills, Trusts, & Probates`), then DQ-005 dedupe |
# MAGIC | **DQ-BUSINESS-006** | Dicts with no information: bronze NULL, `'None'`, empty `{}`, all-`None` | Row kept; every contract key column = SQL NULL (e.g. Music `dj` … `karaoke` all NULL). No rows deleted |
# MAGIC | **DQ-BUSINESS-008** | Partial key sets in nested dicts (1,942 dicts) | Standardize the columns: expand to the full contract key list; absent key → NULL (unknown) |
# MAGIC | **DQ-BUSINESS-009** | Non-existent state `XMS` (UK business, 1 row) | Quarantine to `quarantine.yelp_business_out_of_scope` (reason `NON_EXISTENT_STATE`) |
# MAGIC | **DQ-BUSINESS-010** | Real state codes outside the Yelp footprint (20 rows) | (a) 6 rows: correct `state` from the ZIP prefix when ZIP, city and coordinates agree. (b) 13 + (c) 1 rows: quarantine (`OUT_OF_FOOTPRINT`) |
# MAGIC | **DQ-BUSINESS-023 / 046** | `state` contradicts the ZIP prefix (71) / address block contradicts the coordinates (20) | Correct `state` only when the 2-of-3 witness rule (ZIP prefix, coordinates, city) is unanimous on the answer. Unresolved rows stay raw and are deferred (see next cell) |
# MAGIC | **DQ-BUSINESS-011** | Coordinates outside every Yelp metro (3 flagged) | 1 proven geocoding error → `latitude`/`longitude` set to NULL (no ZIP-centroid reference available); Bennington, VT goes to DQ-010 quarantine; Stone Harbor kept (false positive) |
# MAGIC | **DQ-BUSINESS-015 / 032** | Empty-string `postal_code` (73) and `address` (5,127 `''` + 1 `'.'`) | Masked NULL → SQL NULL |
# MAGIC | **DQ-BUSINESS-016** | Malformed postal codes (4 rows) | Upper-case, truncate to the valid FSA, NULL what cannot be recovered |
# MAGIC | **DQ-BUSINESS-014** | Canadian postal codes truncated to the FSA (196 AB rows) | Kept as-is — accurate partial value; validation pattern widened to accept FSA-only codes |
# MAGIC | **DQ-BUSINESS-018** | Polluted `city` values: trailing comma, embedded state, qualifiers (32 rows) | Rule-based cleanup plus a reviewed mapping for the 7 non-rule cases |
# MAGIC | **DQ-BUSINESS-019** | Degenerate street addresses with no street name (11 rows) | Set to NULL |
# MAGIC | **DQ-BUSINESS-063** | Null-like negative words `'none'` / `'no'` in Alcohol, WiFi, Smoking, BYOBCorkage (45,893 values) | Whole value `no` → `false` (24,983); null-like words `none` / `unknown` / `n/a` / `null` / … → NULL (20,910 Alcohol `none`). Rows kept |
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

# DBTITLE 1,Audit the Working Split
# ============================================================
# ✅ Audit the Working Split
# ============================================================
# Validate the in-memory split before the issue-by-issue cells
# start mutating the working DataFrames.

from pyspark.sql import functions as F

split_summary = spark.createDataFrame([
    ("df_business", int(df_business.count()), "1 row per business"),
    ("df_attributes", int(df_attributes.count()), "1 row per business"),
    ("df_hours", int(df_hours.count()), "1 row per business"),
    ("df_categories", int(df_categories.count()), "1 row per category token after the initial split"),
] + [
    (var, int(globals()[var].count()), "1 row per business (raw dict string; bronze NULL kept as NULL)")
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

# Each dict DataFrame must keep every business (1:1) and its non-null values must match bronze
_exp = df_bronze.agg(*[F.count(f"attributes.{a}").alias(a) for a in DICT_DF_MAP]).first().asDict()
for attr, (var, _) in DICT_DF_MAP.items():
    n = globals()[var].count()
    n_non_null = globals()[var].where(F.col("raw_dict").isNotNull()).count()
    assert n == bronze_count, f"{var}: {n} rows vs {bronze_count} bronze businesses (no row may be dropped)"
    assert n_non_null == _exp[attr], f"{var}: {n_non_null} non-null dicts vs {_exp[attr]} in bronze"
    assert globals()[var].select("business_id").distinct().count() == n, f"{var}: business_id must be unique (1:1)"

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

for attr_name, (var, tbl) in DICT_DF_MAP.items():
    print("=" * 80)
    print(f"{var}  →  {tbl}")
    print("=" * 80)
    display(globals()[var].limit(5))

print("=" * 80)
print("df_quarantine_business")
print("=" * 80)
display(df_quarantine_business.limit(5))

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-001 — Issue, Solutions & Decision
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 DQ-BUSINESS-001 — Storage Type Non-Conformance vs Silver Contract
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **Error ID** | `DQ-BUSINESS-001` |
# MAGIC | **Severity** | FINDING (schema / data type conformance violation) |
# MAGIC | **Detected In** | Phase 6.1.2 — Data Type Conformance (Cell 27) |
# MAGIC | **Also Confirmed In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29): the values support every target type (58/58 leaves contract-aligned) |
# MAGIC | **Affected Columns** | 84 of 101 contract columns: `stars` (DOUBLE → DECIMAL(2,1)), `is_open` (BIGINT → BOOLEAN), 24 `attributes.*` STRING → BOOLEAN, `attributes.RestaurantsPriceRange2` STRING → INT, 7 enum attributes (Python-repr quoting), 49 flags packed in 7 dict-strings, `categories` (CSV string → 1:N rows) |
# MAGIC | **Total Affected Rows** | **150,346** out of 150,346 (100%). The issue is in the schema, so every row carries it |
# MAGIC | **Invalid Value** | Wrong storage types, e.g. `stars = 3.5` stored as DOUBLE, `is_open = 1` as BIGINT, `"True"` as STRING, `"{'lot': True, ...}"` as STRING |
# MAGIC | **Domain Rule** | Silver DDL rule R5 (Semantic Data Types): column types must reflect real-world meaning, not raw ingestion storage |
# MAGIC
# MAGIC ### 🧩 The Problem in Plain Words
# MAGIC
# MAGIC Bronze stores many values in the **wrong data type**. The value itself is fine, but the way it is stored is not:
# MAGIC
# MAGIC | Column | Stored in bronze as | Should be | Why it matters |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `stars` | `3.5` as DOUBLE (floating point) | DECIMAL(2,1) | Averages of doubles can drift (e.g. 3.4999999) |
# MAGIC | `is_open` | `1` / `0` as BIGINT (number) | BOOLEAN (`true` / `false`) | It is a yes/no fact, not a number |
# MAGIC | 24 yes/no attributes (e.g. `GoodForKids`) | the text `"True"` / `"False"` | BOOLEAN | You would have to filter on text instead of `true` / `false` |
# MAGIC | `RestaurantsPriceRange2` | the text `"2"` | INT | You cannot do math or ranges on text |
# MAGIC
# MAGIC The column **names** in the working frames also still use the bronze spelling (`GoodForKids`, `Monday`) instead of the Silver spelling (`attr_good_for_kids`, `hours_monday`).
# MAGIC
# MAGIC <details><summary>Full description from the DQ document</summary>
# MAGIC
# MAGIC Bronze keeps the Yelp JSON almost exactly as received. Ratings are floating-point, the open/closed flag is a 0/1 integer, and nearly all business attributes are free-form strings. Some of those strings are Python-repr booleans (`"True"`), some are quoted enums (`u'free'`) and some are whole Python dictionaries (`"{'romantic': False, ...}"`). Only 16 contract columns (identity, location text, coordinates, `review_count`, hours) can be used as-is. Left this way, analysts would have to compare `attr_wifi = "u'free'"` instead of `'free'`, filter on `"True"` strings instead of booleans, and they would hit floating-point drift on star averages. The 6.1.3 value-based inference confirms the problem is the storage types, not the contract: every target type in the DDL matches the values.
# MAGIC
# MAGIC </details>
# MAGIC
# MAGIC ### Suggested Solutions
# MAGIC
# MAGIC | # | Solution | Justification |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | **Typed cast + parse in the Silver loading script** (the TE.1/TE.2 logic: `CAST(stars AS DECIMAL(2,1))`, 0/1 → BOOLEAN, `'True'/'False'` → BOOLEAN, unquote enums, decode dict literals into MAP<STRING,BOOLEAN>, explode CSV) | Fixes every column once, at the bronze → silver boundary. Bronze stays a faithful raw copy (R1), and the TE.1 validation already showed 0 lossy casts and 0 out-of-domain enums. |
# MAGIC | 2 | **Fix types in bronze** (`ALTER` / rewrite `bronze_final.yelp_business`) | Downstream code gets simpler, but bronze would no longer be a raw replica of the source. That breaks the medallion contract and makes replays and audits harder. |
# MAGIC | 3 | **Expose typed views on top of bronze** | Nothing extra to store, but the parse and cast run on every query (dict regex parsing is costly), and you can't declare PK/FK constraints on views. |
# MAGIC | 4 | **Leave as STRING and cast at query time** | No engineering effort, but every consumer repeats the parsing logic, results drift between teams, and the DDL's R5 rule is ignored. |
# MAGIC
# MAGIC ### ✅ Solution Taken — Option 1
# MAGIC
# MAGIC Convert the types here, in this notebook, while loading Silver. Bronze is not changed.
# MAGIC
# MAGIC The fix is applied to the **same working DataFrames** built above (`df_business`, `df_attributes`, `df_hours`). They are replaced by their converted version; no new DataFrames are created for later steps.
# MAGIC
# MAGIC | DataFrame | Column(s) | Conversion |
# MAGIC | --- | --- | --- |
# MAGIC | `df_business` | `stars` | `3.5` (double) → `3.5` (decimal) |
# MAGIC | `df_business` | `is_open` | `1` → `true`, `0` → `false` |
# MAGIC | `df_attributes` | 24 yes/no attributes | `"True"` → `true`, `"False"` → `false`, `"None"` → empty (NULL) |
# MAGIC | `df_attributes` | `RestaurantsPriceRange2` | `"1"`…`"4"` → `1`…`4`, `"None"` → empty (NULL) |
# MAGIC | `df_attributes`, `df_hours` | all columns | renamed to the Silver names (`attr_*`, `hours_*`); values unchanged |
# MAGIC
# MAGIC If a value appears that is not in this list, the cell **stops with an error** instead of guessing.
# MAGIC
# MAGIC **Rows kept:** 150,346 before and after in `df_business`, `df_attributes` and `df_hours` — no row is deleted (rule R-1).
# MAGIC
# MAGIC ### ⏭️ Not Done in This Block (each has its own block later)
# MAGIC
# MAGIC | What | Where |
# MAGIC | --- | --- |
# MAGIC | 7 text attributes such as `WiFi = "u'free'"` → `free` (type is already text; only the value needs cleaning) | DQ-003 |
# MAGIC | The text `"None"` in those 7 text attributes and inside the dicts | DQ-002 |
# MAGIC | The 7 dict DataFrames (`df_ambience`, …) → one yes/no column per key | DQ-028 / 006 / 008 |
# MAGIC | `df_categories` | DQ-013 / 005 |
# MAGIC | `df_hours` values | no change needed — Silver keeps them as text |
# MAGIC
# MAGIC > The text `"None"` in the yes/no and price columns (9,441 values) is turned into NULL **here**, because a BOOLEAN or INT column cannot hold the word "None".
# MAGIC
# MAGIC ### 💡 Why This Is the Best Solution
# MAGIC
# MAGIC * **The target types are already defined and the data fits them.** Nothing is lost; only the storage format changes.
# MAGIC * **Bronze stays an exact copy of the source** (Option 2 would break that).
# MAGIC * **Done once.** Views or query-time casts (Options 3 and 4) repeat the work on every query, and each team could convert differently.

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-001 — Show the Issue
# ============================================================
# 🔍 DQ-BUSINESS-001 — Show the Issue
# ============================================================
# Compare the current working DataFrames against the Silver DDL
# (read live from the empty target tables, independent of this
# notebook) and show the raw encodings that cause the mismatch.
import re

SILVER = "yelp_poc_uc.silver_cleaned"

# Bronze field -> Silver DDL column name (only irregular names are listed explicitly)
_NAME_OVERRIDES = {"BYOB": "byob", "BYOBCorkage": "byob_corkage", "HasTV": "has_tv",
                   "Open24Hours": "open_24_hours", "WiFi": "wifi",
                   "RestaurantsPriceRange2": "restaurants_price_range_2"}
ATTR_COL_MAP = {a: "attr_" + _NAME_OVERRIDES.get(a, re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", a).lower())
                for a in SINGLE_ATTRS}
HOURS_COL_MAP = {d: f"hours_{d.lower()}" for d in DAYS}

TARGETS_001 = {  # target table -> (working DataFrame, bronze-name -> DDL-name map)
    "yelp_business":            (df_business,   {c: c for c in TOP_LEVEL_COLS}),
    "yelp_business_attributes": (df_attributes, {"business_id": "business_id", **ATTR_COL_MAP}),
    "yelp_business_hours":      (df_hours,      {"business_id": "business_id", **HOURS_COL_MAP}),
}

rows = []
for tbl, (wdf, name_map) in TARGETS_001.items():
    required = {f.name: f.dataType.simpleString() for f in spark.table(f"{SILVER}.{tbl}").schema}
    now = {f.name: f.dataType.simpleString() for f in wdf.schema}
    assert set(name_map.values()) == set(required), f"{tbl}: name map does not cover the Silver columns"
    df_name = {"yelp_business": "df_business", "yelp_business_attributes": "df_attributes",
               "yelp_business_hours": "df_hours"}[tbl]
    for src, tgt in name_map.items():
        wrong_type, wrong_name = now[src] != required[tgt], src != tgt
        if wrong_type or wrong_name:
            problem = "wrong type and name" if (wrong_type and wrong_name) else ("wrong type" if wrong_type else "wrong name")
            rows.append((df_name, src, tgt, now[src], required[tgt], problem))

issue_001 = spark.createDataFrame(rows, ["DataFrame", "Column now", "Column required",
                                         "Type now", "Type required", "Problem"])

# ---- 1) Actual sample rows (the erroneous data itself) ----------
_t = {r["Column now"]: (r["Type now"], r["Type required"], r["Column required"]) for r in issue_001.collect()}
def _hdr(c):
    """Readable header: column now -> what Silver requires."""
    tn, tr, cr = _t[c]
    return f"{c} ({tn}) -> {cr} ({tr})" if tn != tr else f"{c} -> {cr}"

_sample_cols = ["stars", "is_open", "GoodForKids", "RestaurantsTakeOut", "RestaurantsPriceRange2", "Monday"]
_sample_001 = (df_business.select("business_id", "name", "stars", "is_open")
               .join(df_attributes.select("business_id", "GoodForKids", "RestaurantsTakeOut", "RestaurantsPriceRange2"), "business_id")
               .join(df_hours.select("business_id", "Monday"), "business_id"))
# 5 ordinary rows + 3 rows that hold the word 'None' in a yes/no or price column
_sample_001 = (_sample_001.where(F.col("RestaurantsPriceRange2").rlike("^[1-4]$") & F.col("GoodForKids").isin("True", "False")).orderBy("business_id").limit(5)
               .unionByName(_sample_001.where((F.col("GoodForKids") == "None") | (F.col("RestaurantsPriceRange2") == "None")).orderBy("business_id").limit(3)))

print("1) Sample rows as they are stored now")
print("   Purpose: shows the real values with the wrong type / name. Each header reads 'column now (type now) -> Silver column (Silver type)'.")
display(_sample_001.select("business_id", "name", *[F.col(c).alias(_hdr(c)) for c in _sample_cols]))

# ---- 2) Statistics ---------------------------------------------
print("2) Every column whose name or data type does not match Silver")
print("   Purpose: the full list of columns this block must fix, with the type it has now and the type Silver needs.")
display(issue_001.orderBy("DataFrame", "Problem", "Column now"))

print("3) How many columns have each problem")
print("   Purpose: summary of the list above per DataFrame.")
display(issue_001.groupBy("DataFrame", "Problem").count().withColumnRenamed("count", "Columns")
        .orderBy("DataFrame", "Problem"))

# The values that will be converted
print("4) The values currently stored in the columns with a wrong type")
print("   Purpose: every distinct value that the conversion must handle; anything not listed here would stop the next cell.")
_bool_vals = (df_attributes
              .select(F.explode(F.array(*[
                  F.struct(F.lit(a).alias("col"), F.col(a).alias("val")) for a in BOOL_ATTRS
              ])).alias("p"))
              .select(F.col("p.col").alias("Column"), F.col("p.val").alias("Value now"))
              .groupBy("Column", "Value now").count())
_price_vals = df_attributes.groupBy(F.col("RestaurantsPriceRange2").alias("Value now")).count().withColumn("Column", F.lit("RestaurantsPriceRange2"))
_open_vals  = df_business.groupBy(F.col("is_open").cast("string").alias("Value now")).count().withColumn("Column", F.lit("is_open"))
_star_vals  = df_business.groupBy(F.col("stars").cast("string").alias("Value now")).count().withColumn("Column", F.lit("stars"))
display(_bool_vals.unionByName(_price_vals).unionByName(_open_vals).unionByName(_star_vals)
        .withColumnRenamed("count", "Rows").select("Column", "Value now", "Rows").orderBy("Column", "Value now"))

# ---- 5) Reconciliation with the DQ document ---------------------
# DQ document: 84 of 101 contract columns have a wrong storage type, on all 150,346 rows.
_wrong_type_here = issue_001.where(F.col("Problem").contains("type")).count()
_wrong_name_here = issue_001.where(F.col("Problem").contains("name")).count()
_dict_keys = sum(len(v) for v in DICT_ATTRS.values())
_recon_001 = spark.createDataFrame([
    ("Wrong type, fixed in this block (stars, is_open, 24 yes/no, price range)", _wrong_type_here, "DQ-001"),
    ("7 text attributes with quoted values (u'free')", len(ENUM_ATTRS), "DQ-003"),
    ("Yes/no flags packed inside the 7 dict strings", _dict_keys, "DQ-028 / 006 / 008"),
    ("categories packed in one text value", 1, "DQ-013 / 005"),
    ("TOTAL (DQ document says 84)", _wrong_type_here + len(ENUM_ATTRS) + _dict_keys + 1, ""),
], ["What", "Columns", "Fixed in block"])
print("5) Reconciliation with the DQ document")
print(f"   Purpose: proves the counts add up to the 84 columns in the DQ document. Rows affected: {df_business.count():,} of {bronze_count:,} (the issue is in the schema).")
print(f"   Columns with a wrong name (renamed in this block): {_wrong_name_here}")
display(_recon_001)
assert _wrong_type_here + len(ENUM_ATTRS) + _dict_keys + 1 == 84, "Column counts do not reconcile with the DQ document (84)"

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-001 — Apply the Solution
# ============================================================
# 🔧 DQ-BUSINESS-001 — Apply the Solution
# ============================================================
# Fail-fast decoders: only the documented tokens are accepted.
# Any other value raises an error instead of silently becoming NULL.

def _decode_bool(c, name):
    v = F.col(c)
    return (F.when(v.isNull(), F.lit(None).cast("boolean"))
             .when(v == "True", F.lit(True))
             .when(v == "False", F.lit(False))
             .when(v == "None", F.lit(None).cast("boolean"))   # Python None literal (DQ-002)
             .otherwise(F.raise_error(F.concat(F.lit(f"DQ-001: unexpected value in {name}: "), v)).cast("boolean")))

def _decode_price(c):
    v = F.col(c)
    return (F.when(v.isNull() | (v == "None"), F.lit(None).cast("int"))
             .when(v.rlike("^[1-4]$"), v.cast("int"))
             .otherwise(F.raise_error(F.concat(F.lit("DQ-001: unexpected price range: "), v)).cast("int")))

# Keep a reference to the 'before' version, only so the next cell can compare before vs after.
# The working DataFrames themselves (df_business, df_attributes, df_hours) are replaced below.
_before_business, _before_attributes, _before_hours = df_business, df_attributes, df_hours

# 1) Parent: stars DOUBLE -> DECIMAL(2,1), is_open BIGINT 0/1 -> BOOLEAN
df_business = df_business.select(
    "business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude",
    F.col("stars").cast("decimal(2,1)").alias("stars"),
    F.col("review_count"),
    (F.when(F.col("is_open") == 1, F.lit(True))
      .when(F.col("is_open") == 0, F.lit(False))
      .when(F.col("is_open").isNull(), F.lit(None).cast("boolean"))
      .otherwise(F.raise_error(F.concat(F.lit("DQ-001: unexpected is_open: "), F.col("is_open").cast("string"))).cast("boolean"))
    ).alias("is_open"),
)

# 2) Attributes: 24 BOOLEAN + 1 INT decoded, 7 enums renamed only (values -> DQ-003)
df_attributes = df_attributes.select(
    "business_id",
    *[_decode_bool(a, a).alias(ATTR_COL_MAP[a]) for a in BOOL_ATTRS],
    *[_decode_price(a).alias(ATTR_COL_MAP[a]) for a in INT_ATTRS],
    *[F.col(a).alias(ATTR_COL_MAP[a]) for a in ENUM_ATTRS],
)

# 3) Hours: rename only (DDL keeps the raw 'H:M-H:M' range as STRING)
df_hours = df_hours.select("business_id", *[F.col(d).alias(HOURS_COL_MAP[d]) for d in DAYS])

# Count once so that an unexpected value raises the error here, not in a later cell.
# Rule R-1: row counts before and after must be equal.
print("🔧 DQ-001 applied to the working DataFrames (no rows deleted)")
for _name, _bf, _af in [("df_business", _before_business, df_business),
                        ("df_attributes", _before_attributes, df_attributes),
                        ("df_hours", _before_hours, df_hours)]:
    _nb, _na = _bf.count(), _af.count()
    print(f"   {_name:<14}: {_nb:>9,} → {_na:>9,} rows")
    assert _nb == _na, f"{_name}: row count changed ({_nb} -> {_na})"

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-001 — Verify the Issue Is Solved
# ============================================================
# ✅ DQ-BUSINESS-001 — Verify the Issue Is Solved
# ============================================================
# Check 1: every column now has the Silver name and data type.
# Check 2: every old value was converted to exactly one new value,
#          and no row was lost.

# ---- Check 0: the issue count is now 0 ------------------------
# Re-run the same comparison as the 'Show the Issue' cell on the fixed frames.
_left = []
for tbl, wdf in [("yelp_business", df_business), ("yelp_business_attributes", df_attributes), ("yelp_business_hours", df_hours)]:
    _req = {f.name: f.dataType.simpleString() for f in spark.table(f"{SILVER}.{tbl}").schema}
    _now = {f.name: f.dataType.simpleString() for f in wdf.schema}
    _left += [c for c in _req if _now.get(c) != _req[c]]
print("0) Columns still wrong (was", issue_001.count(), "in the 'Show the Issue' cell):", len(_left))
print("   Purpose: the issue count must now be 0.")
assert len(_left) == 0, f"Columns still wrong: {_left}"

# ---- Check 1: names and types ---------------------------------
check_rows = []
for tbl, df_name, wdf in [("yelp_business", "df_business", df_business),
                          ("yelp_business_attributes", "df_attributes", df_attributes),
                          ("yelp_business_hours", "df_hours", df_hours)]:
    required = [(f.name, f.dataType.simpleString()) for f in spark.table(f"{SILVER}.{tbl}").schema]
    now = [(f.name, f.dataType.simpleString()) for f in wdf.schema]
    wrong = sum(1 for r, n in zip(required, now) if r != n) + abs(len(required) - len(now))
    check_rows.append((df_name, len(required), len(now), wrong, "✅ yes" if wrong == 0 else "❌ no"))

print("1) Do the column names and data types now match Silver?")
print("   Purpose: confirms every column in each working DataFrame has the correct Silver name and type.")
display(spark.createDataFrame(check_rows, ["DataFrame", "Columns required", "Columns now",
                                           "Columns still wrong", "Matches Silver"]))
assert all(r[3] == 0 for r in check_rows), "Some columns still differ from Silver"

# ---- Check 2: how each old value was converted ------------------
# Join the 'before' and 'after' versions of each row on business_id.
_b = _before_attributes.alias("b").join(df_attributes.alias("a"), "business_id")
_bool_map = (_b.select(F.explode(F.array(*[
                 F.struct(F.col(f"b.{a}").alias("before"), F.col(f"a.{ATTR_COL_MAP[a]}").cast("string").alias("after"))
                 for a in BOOL_ATTRS])).alias("p"))
               .select(F.lit("24 yes/no attributes (all together)").alias("Column"),
                       F.col("p.before").alias("Value before"), F.col("p.after").alias("Value after")))
_price_map = _b.select(F.lit("RestaurantsPriceRange2").alias("Column"),
                       F.col("b.RestaurantsPriceRange2").alias("Value before"),
                       F.col("a.attr_restaurants_price_range_2").cast("string").alias("Value after"))
_p = _before_business.alias("b").join(df_business.alias("a"), "business_id")
_open_map = _p.select(F.lit("is_open").alias("Column"), F.col("b.is_open").cast("string").alias("Value before"),
                      F.col("a.is_open").cast("string").alias("Value after"))
_star_map = _p.select(F.lit("stars").alias("Column"), F.col("b.stars").cast("string").alias("Value before"),
                      F.col("a.stars").cast("string").alias("Value after"))

conversion_001 = (_bool_map.unionByName(_price_map).unionByName(_open_map).unionByName(_star_map)
                  .groupBy("Column", "Value before", "Value after").count()
                  .withColumnRenamed("count", "Rows"))

print("2) How each old value was converted (NULL = empty):")
print("   Purpose: shows every distinct before→after mapping so you can verify no value was silently changed or lost.")
display(conversion_001.orderBy("Column", "Value before"))

# Every old value must map to exactly one new value
_multi = conversion_001.groupBy("Column", "Value before").count().where("count > 1").count()
assert _multi == 0, "An old value was converted to more than one new value"
# Stars must keep the same number (3.5 stays 3.5)
assert _p.where(F.col("a.stars").cast("double") != F.col("b.stars")).count() == 0, "A stars value changed"
# ---- Check 3: row counts unchanged -------------------------------
_rc = [("df_business", _before_business.count(), df_business.count()),
       ("df_attributes", _before_attributes.count(), df_attributes.count()),
       ("df_hours", _before_hours.count(), df_hours.count())]
print("3) Row counts before → after")
print("   Purpose: rule R-1 — no row may be deleted.")
display(spark.createDataFrame(_rc, ["DataFrame", "Rows before", "Rows after"]))
assert all(b == a == bronze_count for _, b, a in _rc), "Rows were lost"

# ---- Check 4: the same sample rows, before and after -------------
_ids = [r.business_id for r in _sample_001.select("business_id").collect()]
_after_001 = (df_business.select("business_id", "name", "stars", "is_open")
              .join(df_attributes.select("business_id", "attr_good_for_kids", "attr_restaurants_take_out",
                                         "attr_restaurants_price_range_2"), "business_id")
              .join(df_hours.select("business_id", "hours_monday"), "business_id")
              .where(F.col("business_id").isin(_ids)))
print("4) The sample rows from 'Show the Issue', after the fix")
print("   Purpose: the same businesses now carry Silver names and types ('None' is a real NULL).")
display(_after_001.select("business_id", "name",
                          *[F.col(c).alias(f"{c} ({t})") for c, t in
                            [(f.name, f.dataType.simpleString()) for f in _after_001.schema if f.name not in ("business_id", "name")]]))

print("✅ DQ-001 solved: names and types match Silver, each old value maps to one new value, no rows lost.")

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-002 — Issue, Solutions & Decision
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🚨 DQ-BUSINESS-002 — Literal `'None'` Strings Masquerading as NULL
# MAGIC
# MAGIC | Attribute | Detail |
# MAGIC | --- | --- |
# MAGIC | **Error ID** | `DQ-BUSINESS-002` |
# MAGIC | **Severity** | FINDING (completeness masking: false non-null values) |
# MAGIC | **Detected In** | Phase 6.1.3 — Semantic Data Type Violations (Cell 29) |
# MAGIC | **Also Confirmed In** | Phase TE.1 — Data Contract & Fix Data Types (Cell 34): all 12,827 tokens decoded as Python `None` → NULL (9,625 in single-value attributes + 3,202 whole-dict values), 0 values lost in decode; Phase TE.2 (Cell 35): inner `None` dict values kept as rows with `value = NULL` (e.g. 33,655 in `df_ambience`, 18,141 in `df_good_for_meal`, 9,654 in `df_business_parking`, 3,202 in `df_music`); Phase 6.1.3 dict-key inventory: inner `None` values inside 4 dict attributes |
# MAGIC | **Affected Columns** | **Level 1** (whole value = `'None'`): 37 of 39 `attributes.*` fields (all except `AgesAllowed` and `GoodForDancing`). Largest: `RestaurantsDelivery` (3,948), `BusinessParking` (2,271), `RestaurantsTakeOut` (2,105), `OutdoorSeating` (1,882), `DriveThru` (755), `Ambience` (551). **Level 2** (key value = `None` inside a dict): `Ambience` (9,754 dicts), `GoodForMeal` (7,337), `BusinessParking` (5,153), `Music` (1,632) |
# MAGIC | **Total Affected Rows** | **9,653** out of 150,346 (6.42%) with at least one level-1 `'None'` string, totalling 12,827 tokens (9,625 in typed columns + 3,202 in dict-strings) |
# MAGIC | **Invalid Value** | The 4-character string `'None'` (Python's `None` serialized by `str()`), stored where a SQL `NULL` belongs |
# MAGIC | **Domain Rule** | Missing information must be a SQL `NULL`. A non-null string must hold a real member of the attribute's domain (`True`/`False`, an enum value, or a dict) |
# MAGIC
# MAGIC ### 🧩 The Problem in Plain Words
# MAGIC
# MAGIC When Yelp exported the data, a missing answer was written as the **word** `"None"` instead of being left empty. Spark therefore thinks a value is there:
# MAGIC
# MAGIC | Where the word `"None"` appears | Example | Why it matters |
# MAGIC | --- | --- | --- |
# MAGIC | As the whole value of a single attribute | `NoiseLevel = "None"` | It counts as "filled in", so completeness looks better than it is |
# MAGIC | As the whole value of a dict attribute | `BusinessParking = "None"` | It looks like the business has parking info, but it has none |
# MAGIC | Inside a dict, for one key | `{'lot': None, 'street': True}` | `lot` is unknown — it must not become `false` |
# MAGIC
# MAGIC ⚠️ Careful: `Alcohol` has a **real** value `'none'` (lowercase, in quotes = "no alcohol served"). Only the exact word `None` (capital N, no quotes) is a missing value.
# MAGIC
# MAGIC <details><summary>Full description from the DQ document</summary>
# MAGIC
# MAGIC The Yelp export was built by calling Python `str()` on attribute values, so Python's `None` became the literal text `'None'` instead of a JSON null. To Spark these values look present: `IS NOT NULL` is true, `COUNT(col)` includes them, and completeness metrics are overstated. A naive `CAST(... AS BOOLEAN)` sends them to NULL with no trace, and a naive enum cleaner could turn them into a fake `'none'` category. That collision matters here because `Alcohol` has a valid lowercase `'none'` value (no alcohol served). The same pattern repeats inside dict-strings, where single keys carry `None` (e.g. `{'lot': None, 'street': True}`), meaning "unknown", not `False`.
# MAGIC
# MAGIC </details>
# MAGIC
# MAGIC ### Suggested Solutions
# MAGIC
# MAGIC | # | Solution | Justification |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | **Normalize to SQL NULL during silver load, keeping every row** (`CASE WHEN col = 'None' THEN NULL`, applied to single-value attributes **and** to whole-dict values; inner `None` dict values become NULL booleans when the dict is decoded) | Gives the data its true meaning (unknown), restores accurate completeness metrics, and keeps "unknown" separate from `False`. No row is deleted, so row counts always reconcile with bronze. |
# MAGIC | 2 | **Map `'None'` to `False`** | Simple, and boolean columns end up fully populated, but it invents information (e.g. "no delivery" when the business never answered) and biases every rate and ratio. |
# MAGIC | 3 | **Keep the token and add a `_is_unknown` flag column** | Fully traceable, but it widens tables and every consumer must remember to filter on the flag. That is redundant when NULL already carries the meaning natively. |
# MAGIC | 4 | **Drop the affected rows** (whole businesses, or the child dict rows whose value is `'None'`) | ❌ Rejected. Breaks the rule that cleaning never deletes rows, and loses valid identity, location and rating data. |
# MAGIC
# MAGIC ### ✅ Solution Taken — Option 1
# MAGIC
# MAGIC Turn the word `"None"` into a real empty value (SQL / Delta `NULL`). The match is **exact and case-sensitive**, so `Alcohol = 'none'` is kept.
# MAGIC
# MAGIC 🛑 **No row is deleted.** Every row stays in its DataFrame; only the value changes from the word `"None"` to `NULL`.
# MAGIC
# MAGIC The fix is applied to the **same working DataFrames** (`df_attributes` and the 7 dict DataFrames). No new DataFrames are created.
# MAGIC
# MAGIC | Where | Rows with `"None"` | What happens |
# MAGIC | --- | --- | --- |
# MAGIC | 24 yes/no columns + price range in `df_attributes` | 9,441 | **Already done in DQ-001** (a BOOLEAN / INT column cannot hold the word) |
# MAGIC | 7 text columns in `df_attributes` (e.g. `attr_noise_level`) | 184 | `"None"` → NULL. The business row stays. |
# MAGIC | 7 dict DataFrames (e.g. `df_business_parking`) | 3,202 | `raw_dict = "None"` → `raw_dict = NULL`. The row stays. |
# MAGIC | **Total** | **12,827** | matches the DQ document |
# MAGIC
# MAGIC ### ⏭️ Not Done in This Block
# MAGIC
# MAGIC | What | Where |
# MAGIC | --- | --- |
# MAGIC | `None` **inside** a dict (e.g. `'lot': None`). The dict is still one text value here, so a single key cannot be NULL yet | DQ-028, when each dict is split into one yes/no column per key; that key becomes NULL (not `false`) |
# MAGIC | Quotes around text values (e.g. `u'free'` → `free`) | DQ-003 |
# MAGIC
# MAGIC ### 💡 Why This Is the Best Solution
# MAGIC
# MAGIC * **NULL is the true meaning** — the business never answered. NULL is skipped by `COUNT(col)` and `AVG`, so completeness and rates become correct automatically.
# MAGIC * **Option 2 invents facts** ("no delivery" for a business that never said).
# MAGIC * **Option 3 adds an extra column** that every user must remember; NULL already says "unknown".
# MAGIC * **Option 4 deletes rows**, which breaks the rule that cleaning never deletes data, and throws away good businesses (name, location, rating) because of one missing attribute.
# MAGIC * **Keeping every row** means row counts always reconcile with bronze, and nothing has to be recovered later.

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-002 — Show the Issue
# ============================================================
# 🔍 DQ-BUSINESS-002 — Show the Issue
# ============================================================
# Count the exact word "None" (capital N, no quotes) in every place
# it can still appear after DQ-001.

NONE_TOKEN = "None"
ENUM_COLS = [ATTR_COL_MAP[a] for a in ENUM_ATTRS]   # e.g. attr_noise_level

# ---- 1) Actual rows: text columns in df_attributes that contain the word "None" ----
print("1) Sample rows from df_attributes where a text column is the word 'None':")
print("   Purpose: shows the actual data so you can see what 'None' looks like next to real values.")
_none_in_attrs = df_attributes.where(
    F.greatest(*[(F.col(c) == NONE_TOKEN).cast("int") for c in ENUM_COLS]) == 1
).select("business_id", *ENUM_COLS)
display(_none_in_attrs.limit(15))

# ---- 2) Actual rows: dict DataFrames whose whole value is the word "None" ----
print("2) Sample rows from the dict DataFrames where raw_dict = 'None':")
print("   Purpose: shows actual business rows that have the word 'None' instead of a real dict.")
_dict_none_samples = []
for attr, (var, tbl) in DICT_DF_MAP.items():
    _df = globals()[var].where(F.col("raw_dict") == NONE_TOKEN).withColumn("DataFrame", F.lit(var)).limit(3)
    _dict_none_samples.append(_df.select("DataFrame", "business_id", "raw_dict"))
from functools import reduce
display(reduce(lambda a, b: a.unionByName(b), _dict_none_samples))

# ---- 3) Alcohol: real 'none' vs missing None — actual rows ----
print("3) Alcohol column — the word 'None' vs the real value 'none' / u'none':")
print("   Purpose: shows side by side that 'none' (lowercase, in quotes) is a real value meaning 'no alcohol served',")
print("            while 'None' (capital N, no quotes) is a missing value that should be NULL.")
_alc_none   = df_attributes.where(F.col("attr_alcohol") == "None").select("business_id", "attr_alcohol").limit(3)
_alc_real_1 = df_attributes.where(F.col("attr_alcohol") == "u'none'").select("business_id", "attr_alcohol").limit(3)
_alc_real_2 = df_attributes.where(F.col("attr_alcohol") == "'none'").select("business_id", "attr_alcohol").limit(3)
_alc_bar    = df_attributes.where(F.col("attr_alcohol") == "u'full_bar'").select("business_id", "attr_alcohol").limit(3)
display(_alc_none.unionByName(_alc_real_1).unionByName(_alc_real_2).unionByName(_alc_bar))

# ---- 4) Per-column counts + summary (statistics) ----
_enum_counts = df_attributes.agg(*[F.sum((F.col(c) == NONE_TOKEN).cast("int")).alias(c) for c in ENUM_COLS]).first()
rows_002 = [("df_attributes", c, int(_enum_counts[c] or 0)) for c in ENUM_COLS]
for attr, (var, tbl) in DICT_DF_MAP.items():
    n = globals()[var].where(F.col("raw_dict") == NONE_TOKEN).count()
    rows_002.append((var, "raw_dict", n))

issue_002 = spark.createDataFrame(rows_002, ["DataFrame", "Column", "Rows with 'None'"])
print("4) Count of 'None' per column:")
print("   Purpose: shows how widespread the issue is in each column.")
display(issue_002.orderBy("DataFrame", "Column"))

_typed_none = (df_bronze
               .select(F.explode(F.array(*[F.col(f"attributes.{a}") for a in BOOL_ATTRS + INT_ATTRS])).alias("v"))
               .where(F.col("v") == NONE_TOKEN).count())
_enum_none = sum(r[2] for r in rows_002 if r[0] == "df_attributes")
_dict_none = sum(r[2] for r in rows_002 if r[0] != "df_attributes")

summary_002 = spark.createDataFrame([
    ("24 yes/no + price columns (df_attributes)", _typed_none, "already NULL (done in DQ-001)"),
    ("7 text columns (df_attributes)",            _enum_none,  "to fix in this block"),
    ("7 dict DataFrames",                          _dict_none,  "to fix in this block"),
    ("Total",                                      _typed_none + _enum_none + _dict_none, "DQ document says 12,827"),
], ["Where", "Rows with 'None'", "Status"])

print("5) Summary of all 'None' values:")
print("   Purpose: shows the total matches the DQ document and what is still left to fix.")
display(summary_002)

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-002 — Apply the Solution
# ============================================================
# 🔧 DQ-BUSINESS-002 — Apply the Solution
# ============================================================
# Exact, case-sensitive match on "None": Alcohol = 'none' is NOT touched.
# Rule: no row is deleted. "None" is replaced by a real NULL and the row stays.

# Keep a reference to the 'before' version, only so the next cell can compare before vs after.
_before002_attributes = df_attributes
_before002_dicts = {var: globals()[var] for var, _ in DICT_DF_MAP.values()}

# 1) 7 text columns in df_attributes: "None" -> NULL (business row stays)
df_attributes = df_attributes.select(*[
    F.when(F.col(c) == NONE_TOKEN, F.lit(None).cast("string")).otherwise(F.col(c)).alias(c)
    if c in ENUM_COLS else F.col(c)
    for c in df_attributes.columns
])

# 2) 7 dict DataFrames: whole dict "None" -> NULL (row stays)
def _none_to_null(df):
    return df.withColumn("raw_dict",
                         F.when(F.col("raw_dict") == NONE_TOKEN, F.lit(None).cast("string"))
                          .otherwise(F.col("raw_dict")))

df_ambience             = _none_to_null(df_ambience)
df_best_nights          = _none_to_null(df_best_nights)
df_business_parking     = _none_to_null(df_business_parking)
df_dietary_restrictions = _none_to_null(df_dietary_restrictions)
df_good_for_meal        = _none_to_null(df_good_for_meal)
df_hair_specializes_in  = _none_to_null(df_hair_specializes_in)
df_music                = _none_to_null(df_music)

print("🔧 DQ-002 applied to the working DataFrames (no rows deleted)")
print(f"   df_attributes : {df_attributes.count():,} rows (same as before; only values changed)")
for var, _ in DICT_DF_MAP.values():
    _b = _before002_dicts[var]
    before_n, after_n = _b.count(), globals()[var].count()
    n_none = _b.where(F.col("raw_dict") == NONE_TOKEN).count()
    print(f"   {var:<24}: {before_n:>7,} → {after_n:>7,} rows (same) | 'None' turned into NULL: {n_none:,}")

# COMMAND ----------

# DBTITLE 1,DQ-BUSINESS-002 — Verify the Issue Is Solved
# ============================================================
# ✅ DQ-BUSINESS-002 — Verify the Issue Is Solved
# ============================================================
# Check 1: the word "None" no longer appears anywhere it was found.
# Check 2: only "None" was changed; every other value stayed the same.
# Check 3: the dict DataFrames kept every row; only 'None' became NULL.

# ---- Check 1: no "None" left ---------------------------------
_left = df_attributes.agg(*[F.sum((F.col(c) == NONE_TOKEN).cast("int")).alias(c) for c in ENUM_COLS]).first()
check1 = [("df_attributes", c, int(_left[c] or 0)) for c in ENUM_COLS]
for var, _ in DICT_DF_MAP.values():
    check1.append((var, "raw_dict", globals()[var].where(F.col("raw_dict") == NONE_TOKEN).count()))

print("1) Rows that still contain the word 'None' (all should be 0):")
print("   Purpose: confirms the word 'None' was removed from every column found in the 'Show the Issue' cell.")
display(spark.createDataFrame(check1, ["DataFrame", "Column", "Rows with 'None' now"]).orderBy("DataFrame", "Column"))
assert all(r[2] == 0 for r in check1), "The word 'None' is still present"

# ---- Check 2: before -> after for the 7 text columns ---------
_j = _before002_attributes.alias("b").join(df_attributes.alias("a"), "business_id")
_pairs = _j.select(F.explode(F.array(*[
    F.struct(F.lit(c).alias("col"), F.col(f"b.{c}").alias("before"), F.col(f"a.{c}").alias("after"))
    for c in ENUM_COLS])).alias("p")).select("p.*")

changed_002 = (_pairs.where(~F.col("before").eqNullSafe(F.col("after")))
               .groupBy(F.col("col").alias("Column"), F.col("before").alias("Value before"),
                        F.col("after").alias("Value after"))
               .count().withColumnRenamed("count", "Rows"))

print("2) Every value that changed in the 7 text columns:")
print("   Purpose: shows that ONLY 'None' became NULL; no other value (e.g. Alcohol 'none') was touched.")
display(changed_002.orderBy("Column"))
assert changed_002.where(F.col("Value before") != NONE_TOKEN).count() == 0, "A value other than 'None' changed"
assert changed_002.where(F.col("Value after").isNotNull()).count() == 0, "'None' was not turned into NULL"

# ---- Check 3: dict DataFrames kept every row ------------------
# Join before and after on business_id: every 'None' row must now be NULL,
# every other row must have exactly the same raw_dict.
check3 = []
for var, _ in DICT_DF_MAP.values():
    before_df, after_df = _before002_dicts[var], globals()[var]
    n_before, n_after = before_df.count(), after_df.count()
    _jd = before_df.alias("b").join(after_df.alias("a"), "business_id")
    n_none_now_null = _jd.where((F.col("b.raw_dict") == NONE_TOKEN) & F.col("a.raw_dict").isNull()).count()
    n_other_changed = _jd.where((F.col("b.raw_dict") != NONE_TOKEN) & ~F.col("b.raw_dict").eqNullSafe(F.col("a.raw_dict"))).count()
    ok = (n_before == n_after) and n_other_changed == 0
    check3.append((var, n_before, n_after, n_none_now_null, n_other_changed, "✅ yes" if ok else "❌ no"))

print("3) Dict DataFrames: rows before vs after, and what changed:")
print("   Purpose: confirms no row was deleted, 'None' became NULL, and every other dict value is unchanged.")
display(spark.createDataFrame(check3, ["DataFrame", "Rows before", "Rows after", "'None' → NULL",
                                       "Other values changed", "Correct"]))
assert all(r[5] == "✅ yes" for r in check3), "A dict row was deleted or a non-'None' value changed"

print("\n4) Example rows that now hold NULL instead of 'None' (rows are kept):")
print("   Purpose: shows the same businesses from the 'Show the Issue' cell, now with a real NULL.")
_ex = []
for var, _ in DICT_DF_MAP.values():
    _ex.append(_before002_dicts[var].alias("b").where(F.col("b.raw_dict") == NONE_TOKEN)
               .join(globals()[var].alias("a"), "business_id")
               .select(F.lit(var).alias("DataFrame"), "business_id",
                       F.col("b.raw_dict").alias("raw_dict before"), F.col("a.raw_dict").alias("raw_dict after"))
               .limit(3))
display(reduce(lambda a, b: a.unionByName(b), _ex))

# ---- Total reconciliation with the DQ document ----------------
_dict_nulled = sum(r[3] for r in check3)
_text_nulled = changed_002.agg(F.sum("Rows")).first()[0]
_total = _typed_none + _text_nulled + _dict_nulled
print(f"\nTotal 'None' values turned into NULL: {_typed_none:,} (DQ-001) + "
      f"{_text_nulled:,} (text columns) + {_dict_nulled:,} (dicts) = {_total:,}")
assert _total == 12_827, "Total does not match the DQ document (12,827)"

print("✅ DQ-002 solved: no 'None' word left, no row deleted, only 'None' changed, totals match the DQ document (12,827).")