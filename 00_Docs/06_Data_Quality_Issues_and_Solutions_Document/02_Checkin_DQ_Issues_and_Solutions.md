# Data Quality Issues & Solutions — Checkin Dataset

> **Layer:** Bronze Final  
> **Catalog:** `yelp_poc_uc`  
> **Schema:** `bronze_final`  
> **Document Version:** 1.0  
> **Last Updated:** 2026-10-17  

---

## 0. Document Description

This document catalogues every data quality issue discovered during the Deep Exploratory Data Analysis (EDA) of the **Yelp Checkin** dataset (`yelp_poc_uc.bronze_final.yelp_checkin`). Each issue includes its error ID, description, detection phase, severity, root cause, and suggested solutions. After each issue, the **chosen resolution** — approved after human review — is recorded. These decisions will guide the Silver-layer cleaning pipeline implementation.

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
| Row Count (Post-Explode EDA) | 13,356,875 (1 row per checkin event) |
| Columns | `business_id` (string), `date` (string — comma-separated timestamps) |

---

## 2. Pipeline Context

This table sits in the **Bronze Final** layer of the Yelp PoC pipeline:

```
Landing Zone (raw JSON)
  └─► bronze_staging (schema-validated clean/error split)
        └─► bronze_final.yelp_review  ◄── YOU ARE HERE
              └─► silver (cleaned, typed, enriched, modeled)
                    └─► gold (aggregated, feature-engineered)
```

The DQ findings documented below were discovered during the Bronze-layer profiling phase. All remediations marked as **"Chosen"** will be applied during the **Bronze → Silver** transformation step.

---

## 3. Data Quality Issues & Resolutions

---
#### 🟠 DQ-CHECKIN-001 — Data Type Mismatch: `date` Column

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-001` |
| **Description** | The `date` column's declared schema type (`string`) does not match the expected data contract type (`array`). The source data stores multiple comma-separated timestamps as a single flat string instead of a structured array type. |
| **Detection Phase** | Phase 6.1 — Structural / Schema Checks → **6.1.2) Data Type Conformance** |
| **Detected In** | Cell 27: `6.1.2) Data Type Conformance` |
| **Severity** | ⚠️ **Medium** — downstream consumers expecting `array<timestamp>` will fail; aggregation queries on individual checkin events are impossible without pre-processing. |

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Split string → `array<string>`, then cast each element to `TimestampType`** | Preserves all timestamps per business in a native array. Enables `explode()` for row-level analysis while keeping the original 1-row-per-business grain available. Minimal data loss risk. |
| 2 | **Explode into individual rows at ingestion (Silver layer)** | Normalizes the data model — each checkin event becomes its own row with a proper `TimestampType` column. Best for analytics but changes the grain (row count increases \~100x). |
| 3 | **Store as-is, parse on read** | Zero transformation cost at ingestion. However, every downstream query must implement its own parsing, leading to duplicated logic, inconsistent handling, and slower queries. |

**✅ Best Logical Solution: Solution 2 — Explode into individual rows at the Silver layer.**

The checkin data is fundamentally event-level (each timestamp = one checkin event), so the natural grain is one row per checkin, not one row per business. Exploding at the Silver layer creates a clean, analytics-ready model that eliminates repeated parsing logic, enables proper temporal joins, and allows standard SQL aggregations (`COUNT`, `GROUP BY date`, time-series analysis) without custom UDFs. The temporary fix in cells TE.1 and TE.2 already validates this approach.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2** — Explode the comma-separated `date` string into individual rows at the Silver layer, yielding one row per checkin event with a native `TimestampType` column. This becomes the canonical checkin grain for all downstream analytics.

---

---

---
#### 🔴 DQ-CHECKIN-002 — Semantic Type Violation: `date` Column Contains Packed Timestamps

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-002` |
| **Description** | The `date` column is stored as `string` but **92.7% of rows** contain multiple comma-separated timestamps (e.g., `"2010-06-28 22:25:18, 2010-07-21 23:03:49, ..."`). The semantic type is `array<timestamp>`, not `string`. This is a compound representation encoding violation — multiple discrete events packed into a single field. |
| **Detection Phase** | Phase 6.1 — Structural / Schema Checks → **6.1.3) Semantic Data Type Violations** |
| **Detected In** | Cell 29: `6.1.3) Semantic Data Type Violations` |
| **Severity** | 🟥 **High** — this is not just a type mismatch but an architectural data modeling issue. The field conflates a collection of events into a single value, preventing direct filtering, indexing, and joining on individual timestamps. |
| **Related Error** | `DQ-CHECKIN-001` (same root cause, detected from a different angle) |

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **`split(date, ', ')` → `array<string>` → cast elements to `timestamp`** | Corrects the type to `array<timestamp>`. Keeps the 1:1 business-to-row grain. Downstream can `explode()` or `size()` as needed. Good for Silver if the business-level grain is the primary use case. |
| 2 | **`split()` + `explode()` + `to_timestamp()` at the Silver layer** | Fully denormalizes: one row per checkin event with a proper `TimestampType`. Best for event-level analytics, temporal aggregations, and time-series work. Validated by the temporary TE.1/TE.2 fix in this notebook. |
| 3 | **Apply Spark's `from_json()` or schema-on-read** | Only viable if the format were JSON-encoded arrays. Since the format is plain comma-separated, this adds unnecessary complexity. Not recommended. |

**✅ Best Logical Solution: Solution 2 — Split, explode, and cast at the Silver layer.**

The Bronze table's `date` field is a denormalized multi-value string that violates First Normal Form (1NF). Exploding creates a proper event-level model with a native `TimestampType`, enabling standard temporal analysis without custom parsing. The 92.7% multi-value rate confirms this is not an edge case — it is the dominant data pattern and demands a structural fix, not a workaround. The temporary fix in cells TE.1 and TE.2 already proves this approach works correctly on this dataset.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2** — Apply `split()` → `explode()` → `to_timestamp()` at the Silver layer to normalize the packed multi-value string into proper 1NF event-level rows. (Same implementation as DQ-CHECKIN-001 — shared root cause.)

---
#### 🟡 DQ-CHECKIN-003 — Expected Schema Drift After Temporary Remodeling

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-003` |
| **Description** | After applying temporary fixes (TE.1: data type correction, TE.2: explode + temporal feature extraction), the schema drifted from the Bronze baseline: **34 columns added** (temporal features from the exploded timestamps) and **1 column removed** (`date` array replaced by exploded timestamp + derived features). This drift is **expected and intentional** — it validates that the remodeling pipeline produces the correct Silver-layer schema. |
| **Detection Phase** | Phase 6.1 — Structural / Schema Checks → **6.1.4) Schema Drift Detection** |
| **Detected In** | Cell 34: `6.1.4) Schema Drift Detection` |
| **Severity** | ℹ️ **Informational (Acknowledged)** — the drift is a deliberate consequence of the temporary remodeling steps (TE.1 and TE.2) applied for EDA purposes. No corrective action is required at this stage. |
| **Root Cause** | Cells TE.1 and TE.2 applied `split` → `explode` → `to_timestamp` and extracted 34 temporal features to enable comprehensive downstream quality checks. |

**Key Drifts Detected:**

| Drift Type | Count | Details |
| --- | --- | --- |
| **ADDED** columns | 34 | Temporal features: `checkin_timestamp_ISO_8601`, `checkin_date`, `checkin_year`, `checkin_month`, `checkin_day`, DOW/quarter/half-year features, time-of-day features, boolean flags (`is_weekend`, `is_weekday`, etc.) |
| **REMOVED** columns | 1 | `date` (original comma-separated string → replaced by exploded individual timestamp and derived features) |
| **RETYPED** columns | 0 | None — no existing columns changed type |

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Formalize the remodeled schema as the Silver-layer contract** | The 34 added columns represent the intended analytical features. Documenting this as the expected Silver schema ensures the cleaning pipeline produces the correct output and future drift checks validate against the right baseline. |
| 2 | **Trim to only essential derived columns** | If 34 temporal features are excessive, select only the most analytically valuable ones (e.g., `timestamp`, `date`, `year`, `month`, `dow_number`, `hour`, `is_weekend`) and drop redundant ones (e.g., `checkin_hour_12`, `checkin_hour_bin_4h`). Reduces storage and complexity. |
| 3 | **No action — acknowledge as expected drift** | Since this check compared the remodeled DataFrame against the original Bronze baseline, the drift is expected. The comparison confirms the remodeling worked correctly. |

**✅ Best Logical Solution: Solution 1 — Formalize the remodeled schema as the Silver-layer contract.**

The drift detected here is exactly the transformation the cleaning pipeline should perform. Rather than treating this as an error, it should be codified as the expected output schema for the Silver checkin table. This turns the drift detection from a warning into a validation that the pipeline correctly transforms Bronze → Silver. Solution 2 (column trimming) can be applied as a secondary optimization during Silver-layer design if some features prove unnecessary for downstream analytics.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 1** — Formalize the remodeled schema (34 added temporal feature columns, 1 removed `date` column) as the Silver-layer data contract. Future drift checks will validate against this baseline.

---
#### 🟠 DQ-CHECKIN-004 — Temporal Gaps: Incomplete Boundary Years (2009 & 2022)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-004` |
| **Description** | The checkin dataset spans 2009–2022 but has **22 missing year-months** concentrated at the temporal boundaries. **2009** is missing months 1–11 (only December present, with just **2 checkins**). **2022** is missing months 2–12 (only January present, with **20,940 checkins**). Both years fall below 10% of the median annual volume (1,096,206), confirming they represent partial data collection periods rather than true seasonal absence. |
| **Detection Phase** | Phase 6.4 — Completeness Checks → **6.4.6) Temporal Gap Analysis** |
| **Detected In** | Cell 54: `6.4.6) Temporal Gap Analysis` |
| **Severity** | 🟠 **Medium** — the core dataset (2010–2021) is fully covered with all 144 expected year-months present; however, including the boundary years in temporal analyses will produce misleading aggregations, distort year-over-year trends, and undercount seasonal patterns. |
| **Affected Records** | **2009:** 2 rows (0.00001%); **2022:** 20,940 rows (0.16%) — combined 20,942 of 13,356,875 total rows (0.16%). |

> **📝 Contextual Note:** A significant volume drop is also observed in **2020–2021** (~474K each vs. ~1M+ in prior years), consistent with the COVID-19 pandemic's impact on restaurant foot traffic. This is **not** a data quality issue but a real-world phenomenon that should be accounted for in temporal trend analyses.

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Filter to complete years only (2010–2021) for temporal analyses** | Removes both partial boundary years entirely, ensuring all year-over-year and seasonal comparisons use complete 12-month periods. Simplest approach — eliminates all boundary distortion with no schema changes. However, discards 20,942 valid checkin records permanently from temporal queries. |
| 2 | **Retain all years but add an `is_complete_year` boolean flag** | Preserves the full dataset (zero data loss) while giving downstream consumers a queryable indicator to filter at will. Temporal trend queries filter on `is_complete_year = True` for reliable comparisons; exploratory or business-level queries retain access to boundary data. Metadata-driven and non-destructive. |
| 3 | **Exclude 2009 entirely; annotate 2022 with a cutoff marker** | 2009 has only 2 records — statistically meaningless and unprintable in any chart. 2022's 20,940 January records still carry analytical value for month-level or business-level questions. This hybrid approach balances data retention with quality by removing only the truly negligible boundary. |
| 4 | **Document as a known limitation — no transformation** | If the cleaning pipeline is scope-limited to structural/value fixes, simply document the boundary-year incompleteness as a known limitation for downstream analysts. Appropriate when the pipeline's mandate does not include temporal-coverage remediation. |

##### ✅ Recommended Solution

**Solution 2 — Add an `is_complete_year` flag column.** This is the most flexible and non-destructive approach: it preserves every record while providing an explicit, queryable indicator of year completeness. Temporal trend analyses can filter to `is_complete_year = True` (yielding the clean 2010–2021 window) for reliable year-over-year and seasonal comparisons, while exploratory, business-level, or month-specific analyses retain full access to boundary data. The decision to include or exclude partial years stays in the hands of the downstream analyst rather than being hard-coded into the pipeline — a critical property for a reusable Bronze → Silver transformation.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2** — Add an `is_complete_year` boolean flag column. Records in 2009 and 2022 flagged `FALSE`; all others `TRUE`. Temporal trend queries filter on `is_complete_year = TRUE` (2010–2021).

---
#### 🟠 DQ-CHECKIN-005 — Exact Duplicate Rows: Composite Key Violations

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-005` |
| **Description** | **3,543 rows** (0.0265%) are exact duplicates — identical on the composite key `(business_id, checkin_timestamp_ISO_8601)` and all derived columns. The functional dependency was confirmed: key-distinct (13,353,332) equals row-distinct (13,353,332), meaning every duplicate originates from a repeated composite key. The duplication follows a long-tail pattern: **3,463** keys appear 2\x, **30** appear 3\x, **4** appear 4\x, and **2** appear 5\x, totaling **7,042 rows** involved across **3,499 key groups**. The worst offender is business `A5ZOxjJUE3wyRFO_Xy92xw` with 100 duplicate keys affecting 212 rows. |
| **Detection Phase** | Phase 6.5 — Uniqueness Checks → **6.5.1) Row-Level Uniqueness** (first detection) and **6.5.3) Composite Key Uniqueness** (detailed analysis) |
| **Detected In** | Cell 59: `6.5.1) Row-Level Uniqueness`, Cell 61: `6.5.3) Composite Key Uniqueness` |
| **Severity** | 🟠 **Medium** — only 0.03% of rows are affected, but their presence indicates a systematic upstream ingestion issue (likely duplicate entries in the original comma-separated `date` string before the `split` + `explode` transformation). If left uncleaned, duplicates inflate checkin counts, skew frequency metrics, and distort business-level popularity rankings. |
| **Affected Records** | 3,543 excess rows across 3,499 composite key groups; 7,042 total rows involved in duplication (including the "original" kept-rows). |
| **Root Cause** | The original Bronze `date` column stored all checkin timestamps as a single comma-separated string per business. Some businesses had the same timestamp repeated within that string (e.g., `"2015-03-28 16:55:38, 2015-03-28 16:55:38, ..."`). After `split(',')` + `explode()` in the remodeling step (TE.1/TE.2), each repeated timestamp became a separate row — surfacing the hidden duplicates. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **`dropDuplicates(["business_id", "checkin_timestamp_ISO_8601"])` at Silver layer** | Simple and deterministic — removes all excess rows in a single transformation. Since all columns are functionally determined by the composite key, no information is lost. Requires no schema change. **Limitation:** silently drops rows without an audit trail. |
| 2 | **Dedup with audit: keep first occurrence + log duplicates to a quarantine table** | Same deduplication as Solution 1, but additionally writes the 3,543 dropped rows to a `yelp_checkin_duplicates` error table for post-pipeline audit and root-cause investigation. Provides traceability — important for data governance and compliance reporting. |
| 3 | **Deduplicate at Bronze ingestion: apply `array_distinct()` on the comma-separated timestamps before writing** | Addresses the root cause by preventing duplicates from entering the pipeline. A `split → array_distinct → join` step in the Bronze layer eliminates repeated timestamps at ingestion time. However, this requires a schema migration of the Bronze table, which may not be feasible retroactively. |

##### ✅ Best Logical Solution: Solution 2 — Dedup with audit trail.

Deduplication is necessary since exact duplicates carry zero informational value and inflate all downstream metrics. Solution 2 combines the simplicity of `dropDuplicates` with the governance benefit of an audit table. The quarantine table enables root-cause analysis (e.g., identifying whether duplicates correlate with specific businesses, time periods, or ingestion batches) and provides documentation for data quality reporting. Long-term, the root cause should also be addressed upstream (Solution 3), but the Silver-layer dedup provides immediate correction without Bronze schema changes.

> **🔧 Resolution:** *Deferred to updated DQ-CHECKIN-005 (post-RCA) below.*


---
#### 🟠 DQ-CHECKIN-005 — Exact Duplicate Rows: Composite Key Violations *(Updated post-RCA)*

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-005` |
| **Description** | **3,543 rows** (0.0265%) are exact duplicates — identical on the composite key `(business_id, checkin_timestamp_ISO_8601)` and all derived columns. Functional dependency confirmed: key-distinct (13,353,332) == row-distinct (13,353,332). Duplication pattern: **3,463** keys appear 2×, **30** appear 3×, **4** appear 4×, **2** appear 5× — totaling 7,042 rows across 3,499 key groups. |
| **Detection Phase** | Phase 6.5 — Uniqueness Checks → **6.5.1) Row-Level Uniqueness** and **6.5.3) Composite Key Uniqueness** |
| **Detected In** | Cell 59: `6.5.1) Row-Level Uniqueness`, Cell 63: `6.5.3) Composite Key Uniqueness` |
| **Severity** | 🟠 **Medium** — only 0.03% of rows affected, but indicates a systematic source-data issue. Inflates checkin counts, skews frequency metrics, and distorts business popularity rankings if left uncleaned. |
| **Affected Records** | 3,543 excess rows across **2,664 businesses** (2.0% of all 131,930 businesses). |

---

##### 🔍 Root Cause Analysis Evidence

| RCA Step | Finding |
| --- | --- |
| **Step 1 — Bronze trace** | Reloaded `yelp_poc_uc.bronze_final.yelp_checkin` (131,930 rows, 1 per business). |
| **Step 2 — In-string duplicates** | Compared `size(split(date))` vs `size(array_distinct(split(date)))` per business. Found **3,543 duplicate timestamps** in raw strings — **EXACT MATCH** with post-explode duplicates. |
| **Step 3 — Casting check** | Compared `array_distinct` on raw strings vs on cast `array<timestamp>`. **Zero businesses** had casting-induced duplicates. |
| **Step 4 — Concentration** | Top offender: `A5ZOxjJUE3wyRFO_Xy92xw` — 842 timestamps in raw string, 730 unique, **112 duplicates**. Runner-up: `5Z7HXqp22lSiN0Q8iF94hQ` — 4,478 raw / 4,430 unique / **48 duplicates**. |
| **Step 5 — Inspection** | Worst offender's duplicated timestamps concentrate in **2017-11 through 2019-01**, appearing 2–3× each (e.g., `'2017-11-09 03:58:07' → 3×`, `'2018-12-07 04:24:59' → 3×`). Timestamps are exact string matches — not rounding artifacts. |

##### ✅ Confirmed Root Cause: **SOURCE DATA**

> All 3,543 duplicate timestamps were **already present** in the original comma-separated `date` strings in the Bronze table. The `split()` → `cast("array<timestamp>")` → `explode()` transformation **did not introduce any duplicates**. Timestamp casting **did not collapse** distinct strings into identical timestamps. The Yelp source platform recorded some checkin timestamps multiple times per business, embedding them as repeated entries in the comma-separated string.

---

##### 💡 Suggested Solutions *(unchanged — confirmed appropriate by RCA)*

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **`dropDuplicates(["business_id", "checkin_timestamp_ISO_8601"])` at Silver layer** | Simple and deterministic. Since all columns are functionally determined by the composite key, no information is lost. **Limitation:** no audit trail. |
| 2 | **Dedup with audit: keep first occurrence + log duplicates to a quarantine table** | Same deduplication + writes 3,543 dropped rows to `yelp_checkin_duplicates` for post-pipeline audit and root-cause investigation. Provides traceability for data governance. |
| 3 | **Deduplicate at Bronze ingestion: apply `array_distinct()` on the split timestamps before writing** | Addresses the root cause at ingestion time. A `split → array_distinct → join` step in the Bronze layer prevents duplicates from entering the pipeline. Requires Bronze schema migration. |

##### ✅ Best Logical Solution: Solution 2 — Dedup with audit trail.

The RCA confirmed these are genuine source-data errors (not pipeline artifacts), so deduplication is safe and necessary. Solution 2 combines `dropDuplicates` simplicity with governance-grade traceability via a quarantine table. Long-term, Solution 3 should also be applied at the Bronze layer to prevent recurrence.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2** — `dropDuplicates(["business_id", "checkin_timestamp_ISO_8601"])` at the Silver layer + log the 3,543 removed rows to a `yelp_checkin_duplicates` quarantine table for audit. Long-term: apply `array_distinct()` at Bronze ingestion.

---
#### 🟠 DQ-CHECKIN-006 — Near-Duplicate Checkins: Suspicious Sub-Minute Timestamps

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-006` |
| **Description** | **229,154 checkin pairs** (1.72% of total rows) share the same `business_id` with timestamps **≤ 60 seconds apart**. The vast majority show a gap of just **1 second** (e.g., business `0WrdiQ4qJBu2Gc7wRcIfdw` at `17:39:06` and `17:39:07`). These sub-minute clusters are suspiciously dense for independent user checkins and likely represent either (a) rapid-fire duplicate API submissions, (b) time-rounding artifacts in the source system, or (c) systematic ingestion issues where near-identical events were not deduplicated upstream. Unlike DQ-CHECKIN-005 (exact duplicates), these are **not** byte-identical — the timestamps differ by 1–60 seconds — so standard `dropDuplicates` would **not** catch them. |
| **Detection Phase** | Phase 6.5 — Uniqueness Checks → **6.5.3) Composite Key Uniqueness** (Near-Duplicate Detection sub-check) |
| **Detected In** | Cell 63: `6.5.3) Composite Key Uniqueness` |
| **Severity** | 🟠 **Medium** — 229,154 rows is a non-trivial volume affecting 1.72% of all checkins. While some may represent legitimate concurrent checkins (e.g., a group of friends checking in simultaneously), the dominant 1-second gap pattern strongly suggests automated/systematic duplication rather than organic user behavior. **Impact:** inflated checkin frequency, distorted peak-hour analysis, and skewed business popularity rankings. |
| **Affected Records** | 229,154 rows flagged as near-duplicates (within a 60-second window of a prior checkin for the same business). |
| **Root Cause** | Most likely originates from the source Yelp platform: when multiple users check in at the same business within seconds, or when a single user's check-in event is recorded with slight timestamp jitter (off-by-one-second rounding). The original comma-separated `date` string already contained these near-identical timestamps, which became separate rows after the `explode()` transformation. |
| **Related Error** | `DQ-CHECKIN-005` — exact duplicates are a subset of the broader duplication pattern; near-duplicates extend the issue to non-identical but suspiciously close timestamps. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add an `is_near_duplicate` boolean flag column (window-based)** | Non-destructive: marks suspected near-duplicates without removing them. Uses a `Window` function partitioned by `business_id`, ordered by timestamp, with a `lag()` comparison (gap ≤ 60s → `True`). Downstream analysts can filter `WHERE is_near_duplicate = FALSE` for deduped analysis or include them for completeness. Preserves data provenance and is consistent with the `is_complete_year` flag approach used in DQ-CHECKIN-004. |
| 2 | **Window-based dedup: keep the first checkin per business within a 60-second session window** | Removes suspected duplicates by assigning a session ID to each cluster of checkins within the window and keeping only the earliest timestamp. Produces cleaner data for analytics but is **irreversible** and assumes all sub-minute checkins are spurious — risky if some are legitimate (e.g., busy restaurants, event venues, group checkins). |
| 3 | **Hybrid: flag + downstream aggregation safeguard** | Add the `is_near_duplicate` flag (Solution 1) **AND** enforce dedup in downstream aggregation queries (e.g., `COUNT(DISTINCT business_id, DATE_TRUNC('minute', timestamp))`) rather than physically removing rows. Provides maximum flexibility: raw data is preserved, but standard aggregate queries produce deduplicated counts automatically. |
| 4 | **Document as known pattern — no transformation** | If deeper investigation confirms these represent genuinely distinct user actions (e.g., a busy restaurant with rapid-fire independent checkins), no cleanup is needed. However, the 1-second gap pattern at scale is not characteristic of organic user behavior — this option should only be selected after explicit validation. |

##### ✅ Best Logical Solution: Solution 1 — Add an `is_near_duplicate` boolean flag.

The ambiguity of near-duplicates (some could be legitimate group checkins) makes non-destructive flagging the safest approach. The flag column enables downstream consumers to include or exclude suspected near-duplicates based on their specific use case — temporal trend analysis would exclude them, while business-level total counts might include them. This is consistent with the approach taken for DQ-CHECKIN-004 (the `is_complete_year` flag pattern) and keeps the decision in the hands of the analyst rather than hard-coding it into the pipeline. If deeper investigation later confirms these are all spurious, the flag can easily be used as a filter for physical dedup in a future pipeline iteration.

> **🔧 Resolution:** *Deferred to updated DQ-CHECKIN-006 (post-RCA) below.*

---
#### 🟡 DQ-CHECKIN-006 — Near-Duplicate Checkins: Concurrent User Activity *(Updated post-RCA)*

> **⚠️ Severity downgraded from 🟠 Medium to 🟡 Low** and title revised from "Suspicious Sub-Minute Timestamps" to "Concurrent User Activity" based on root cause analysis evidence.

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-006` |
| **Description** | **229,154 checkin pairs** (1.72% of total rows) share the same `business_id` with timestamps ≤ 60 seconds apart. ~~The vast majority show a gap of just 1 second~~ — **Corrected:** 1-second gaps account for only **2.3%** (5,350 pairs); the gap distribution follows a **smooth natural decay** from 1s (∼5,350) through 60s (∼2,300), with no artificial spike at any single interval. This pattern is consistent with **genuine concurrent user activity** at popular venues, not systematic duplication. |
| **Detection Phase** | Phase 6.5 — Uniqueness Checks → **6.5.3) Composite Key Uniqueness** (Near-Duplicate Detection sub-check) |
| **Detected In** | Cell 63: `6.5.3) Composite Key Uniqueness` |
| **Severity** | 🟡 **Low** *(downgraded from Medium)* — RCA confirmed these are predominantly real-world concurrent checkins, not data errors. They do not corrupt analytical accuracy when properly understood. The flagging approach remains recommended to give downstream analysts the choice. |
| **Affected Records** | 229,154 rows across **34,894 businesses** (26.4% of all 131,930 businesses). Mean near-dup rate: 3.11% per business; median: 1.99%. |

---

##### 🔍 Root Cause Analysis Evidence

| RCA Step | Finding |
| --- | --- |
| **Step 1 — Bronze trace** | Near-duplicate count in raw data: **229,154** — **exact match** with the remodeled dataframe. The pipeline did not create or remove any near-duplicates. |
| **Step 2 — Casting check** | Pairs with identical raw strings but different cast timestamps: **0**. The 1-second gaps exist as distinct literal strings in the original comma-separated `date` field (e.g., `"2018-09-17 20:09:46"` and `"2018-09-17 20:09:47"`). No casting/rounding artifact. |
| **Step 3 — Gap distribution** | Smooth natural decay: 1s=5,350 → 10s=5,104 → 20s=4,413 → 30s=4,014 → 40s=3,282 → 50s=2,805 → 60s=2,296. **No spike at any single interval** — inconsistent with systematic duplication, consistent with random independent arrivals. |
| **Step 4 — Temporal patterns** | Near-dups peak at **11 PM–midnight** (24K) and **5–7 PM** (15K), mirroring restaurant dinner traffic. Yearly pattern tracks overall Yelp usage: peak in 2015–2016 (∼31K–33K), sharp drop in 2020–2021 (∼5.5K) consistent with COVID-19. These patterns match genuine human behavior, not systematic artifacts. |
| **Step 5 — Popularity correlation** | **Pearson r = 0.78** between total checkins and near-dup pairs. Busier businesses generate proportionally more near-duplicates. Top example: `FEXhWNCMkv22qG04E83Qjg` (40,109 total checkins, 1,789 near-dup pairs, 4.5% rate). |

##### ✅ Confirmed Root Cause: **REAL-WORLD BEHAVIOR (genuine concurrent checkins)**

> The 229,154 near-duplicate pairs are **genuine concurrent checkins** from the source data. Evidence:
> 1. **Smooth gap distribution** — no artificial spike at any interval; natural decay consistent with Poisson-like independent arrivals
> 2. **Temporal alignment** — peaks at dinner hours and late night, drops during COVID lockdowns
> 3. **Strong popularity correlation** (r=0.78) — busier venues produce proportionally more concurrent checkins
> 4. **Zero casting artifacts** — raw string timestamps genuinely differ by 1–60 seconds
>
> **Caveat:** The checkin dataset does NOT contain `user_id`, so we cannot definitively distinguish "two different users checking in 1s apart" from "one user's action recorded twice with a 1s offset". However, all statistical evidence favors the former interpretation.

---

##### 💡 Suggested Solutions *(revised based on RCA findings)*

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add an `is_near_duplicate` boolean flag column (window-based)** | Still the recommended approach. Non-destructive flagging gives downstream analysts the choice. Temporal trend analysis can exclude them; business-level totals can include them. Consistent with `is_complete_year` pattern from DQ-CHECKIN-004. |
| 2 | ~~Window-based dedup~~ **Not recommended** | RCA evidence shows these are predominantly real-world concurrent checkins, not errors. Physically removing them would **discard legitimate user activity** and artificially deflate checkin counts at popular venues. |
| 3 | **Hybrid: flag + downstream aggregation safeguard** | Add `is_near_duplicate` flag + use `COUNT(DISTINCT business_id, DATE_TRUNC('minute', timestamp))` in aggregate queries. Provides maximum flexibility with no data loss. |
| 4 | **Document as known pattern — no transformation** | Now a stronger option given RCA confirmed real-world behavior. If the team agrees these are genuine, documenting the pattern without any flag column is sufficient. Appropriate for teams prioritizing pipeline simplicity. |

##### ✅ Best Logical Solution: Solution 1 — Add `is_near_duplicate` flag *(confirmed by RCA)*.

Although the RCA confirmed these are predominantly genuine, the flag still adds value: it lets analysts **quantify and optionally exclude** concurrent-arrival noise from time-series analyses without permanently discarding real events. The non-destructive approach is the safest middle ground between Solution 4 (do nothing) and the now-deprecated Solution 2 (irreversible deletion).

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 1** — Add an `is_near_duplicate` boolean flag column (window-based: `lag()` on timestamp, gap ≤ 60s → `TRUE`). No physical removal — RCA confirmed these are predominantly genuine concurrent checkins.


---
#### 🟡 DQ-CHECKIN-007 — Incomplete Tip-Checkin Business Overlap

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-007` |
| **Description** | Only **77.2%** of businesses with checkin activity also appear in `yelp_tip`. Out of **131,930** distinct `business_id` values in the checkin dataset, **30,046 businesses (22.8%)** have at least one checkin event but **zero tips**. Conversely, **4,309 businesses** have tips but no checkins. While checkin↔review overlap is a perfect 100%, the checkin↔tip overlap is notably lower, indicating a structural asymmetry in user engagement: users who check in do not always leave tips, and the gap is large enough to affect any analysis that assumes tip coverage mirrors checkin coverage. |
| **Detection Phase** | Phase 6.7 — Referential Integrity Checks → **6.7.2) Lookup Table Conformance** |
| **Detected In** | Cell 80: `6.7.2) Lookup Table Conformance` |
| **Severity** | 🟡 **Low** — This is a **behavioral data gap**, not a data defect. Tips are a voluntary, lower-frequency user action compared to checkins; the 22.8% gap is consistent with expected user engagement patterns. No rows are corrupted or missing — the gap reflects genuine absence of tip activity for those businesses. However, it becomes **medium-severity** if downstream models or dashboards implicitly assume tip coverage equals checkin coverage (e.g., joining checkin and tip tables without handling the 30K unmatched businesses). |
| **Affected Records** | **30,046** distinct `business_id` values in checkin with no corresponding tip records; **4,309** `business_id` values in tip with no corresponding checkin records. |
| **Root Cause** | Tips and checkins represent different user engagement modes. Checkins are a low-effort, high-frequency action (tap to check in), while tips require composing text — naturally resulting in fewer businesses having tip coverage. The 4,309 tip-only businesses (present in tip but absent from checkin) likely represent businesses whose checkin records were removed, predate the checkin feature, or appear only in older dataset snapshots. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Use `LEFT JOIN` (not `INNER JOIN`) when combining checkin and tip tables** | Preserves all 131,930 checkin businesses while surfacing null tip columns for the 30,046 unmatched businesses. Prevents silent data loss from inner joins that would drop 22.8% of businesses. Simple, schema-preserving, and requires no data modification. |
| 2 | **Add a `has_tips` boolean flag to the Silver-layer business dimension** | A computed column (`has_tips = tip_count > 0`) lets downstream consumers explicitly filter or segment by tip availability. Enables analysts to consciously decide whether to include tip-less businesses rather than discovering the gap at query time. |
| 3 | **Document the coverage gap in the data contract / data dictionary** | Formally recording that tip coverage is \~77% of checkin coverage prevents future analysts from assuming 1:1 correspondence. Costs nothing to implement and protects every downstream consumer. |
| 4 | **Create a reconciliation view that surfaces the gap per business** | A view joining all five Yelp tables on `business_id` with `COALESCE`-based presence flags (`has_checkin`, `has_review`, `has_tip`) provides a single-query answer to "which engagement signals exist for each business." Useful for coverage dashboards and data quality monitoring. |

##### ✅ Best Logical Solution: Solution 1 + Solution 3 combined.

The immediate priority is preventing data loss: using `LEFT JOIN` ensures no checkin records are silently dropped when tips are joined. Paired with a documented data contract note, this guards both the technical pipeline (Solution 1) and human consumers (Solution 3) against the 22.8% coverage gap. Solutions 2 and 4 are valuable follow-ups for the Silver layer but are not strictly necessary to avoid incorrect results.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 1 + Solution 3** — Use `LEFT JOIN` (never `INNER JOIN`) for all checkin↔tip joins to preserve the 30,046 unmatched businesses. Document the 77.2% coverage gap in the data contract.

---
#### 🟡 DQ-CHECKIN-008 — Suspicious High-Volume Daily Checkins

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-008` |
| **Description** | **131 business-day combinations** exceed 50 checkins at a single business in a single calendar day. The top offender (`CySqUcNz8oPiQTu4EXTnig`) recorded **465 checkins on 2016-06-25** — nearly 1 checkin every 3 minutes over 24 hours. The top 5 spikes all exceed 250 daily checkins, concentrated in 2015–2017. Patterns this dense could indicate bot-generated activity, bulk data uploads, event-driven spikes (concerts, festivals, grand openings), or loyalty-program check-in incentives. |
| **Detection Phase** | Phase 6.8 — Business Rule & Accuracy Checks → **6.8.1) Business Rule Violations** (Rule 2) |
| **Detected In** | Cell 85: `6.8.1) Business Rule Violations` |
| **Severity** | 🟡 **Moderate** — While 131 business-days is a small fraction of the ~4.8M total business-day combinations, the extreme magnitudes (up to 465 checkins/day) are suspicious. These records could disproportionately skew per-business and per-day aggregations. Severity escalates to **High** if downstream models treat checkin volume as a proxy for popularity without accounting for potential artificial inflation. |
| **Affected Records** | **131** business-day combinations; the exact row count depends on whether duplicates (DQ-CHECKIN-005/006) overlap with these spikes. |
| **Root Cause** | Likely a combination of: (1) Event venues (bars, stadiums) hosting large gatherings where many patrons check in simultaneously; (2) Yelp promotional campaigns or check-in incentive programs during 2015–2017; (3) Possible bot or automated check-in tools used for business promotion. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag but retain** — Add a boolean column `is_high_volume_day` for business-days exceeding the 50-checkin threshold | Preserves all data; downstream consumers can filter or weight accordingly. Non-destructive and reversible. The cheapest solution that prevents silent data quality issues. |
| 2 | **Investigate root cause** — Join with `yelp_business` to identify venue types (bars, event venues, stadiums) and cross-reference with `yelp_review`/`yelp_tip` to check if review/tip volume correlates with the spikes | Determines whether spikes are organic (popular event venues) or artificial (bots). Evidence-based filtering criteria are far more reliable than arbitrary thresholds. |
| 3 | **Cap / Winsorize** — Limit daily checkin count per business to the 99th percentile for modeling purposes | Reduces the impact of extreme spikes on aggregations (mean, variance) without deleting data. Useful for statistical models sensitive to outliers but loses the true signal for legitimately popular venues. |
| 4 | **Remove suspected bot rows** — Delete checkin records from business-days exceeding the threshold | Eliminates potentially artificial data, but risks losing legitimate event-driven checkins. Should only be applied after root-cause investigation confirms artificial origin. |

##### ✅ Best Logical Solution: **Solution 2 (Investigate) → then Solution 1 (Flag)**

First investigate the root cause by joining with `yelp_business` metadata (categories, city, review count) to determine whether high-volume days correspond to event venues, popular chains, or suspicious patterns. Then **flag** rather than remove — this preserves data integrity while enabling downstream analysts to include or exclude these records based on their specific use case. Removal (Solution 4) should only follow confirmed artificial origin.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2 → Solution 1** — First investigate by joining with `yelp_business` metadata to classify spike root causes, then flag with an `is_high_volume_day` boolean column for business-days exceeding 50 checkins. No removal without confirmed artificial origin.


---
#### 🟠 DQ-CHECKIN-009 — Severe Year-over-Year Volume Declines (2020 & 2022)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-009` |
| **Description** | Two years exhibit dramatic year-over-year volume declines that distort temporal analyses: **2020** dropped **−54.2%** (from 1,035,160 to 474,172 checkins) and **2022** dropped **−95.6%** (from 477,467 to only 20,940 checkins). The 2020 decline aligns with the **COVID-19 pandemic** impact on in-person business visits globally. The 2022 decline is far more severe and represents **dataset truncation** — the dataset contains data only through **January 19, 2022** (confirmed in 6.9 Timeliness Checks / DQ-CHECKIN-004), meaning 2022 has barely 19 days of data vs. a full calendar year for others. Together these two anomalous years create a false narrative of declining checkin activity if included naively in trend analyses. |
| **Detection Phase** | Phase 6.8 — Business Rule & Accuracy Checks → **6.8.3) Distribution Anomaly Detection** (Year Distribution) |
| **Detected In** | Cell 88: `6.8.3) Distribution Anomaly Detection` |
| **Severity** | 🟠 **Significant** — Any year-over-year trend, growth-rate calculation, forecasting model, or time-series analysis that includes 2020 or 2022 without adjustment will produce **misleading results**. The 2022 truncation is especially dangerous because it silently appears as a catastrophic decline rather than an incomplete data collection period. Severity is High for temporal analytics pipelines. |
| **Affected Records** | **2020:** 474,172 checkins (pandemic-depressed, partial-effect year) | **2022:** 20,940 checkins (only 19 days of data, January 1–19) | **Combined:** 495,112 records in anomalous years out of 13,356,875 total (3.7%). |
| **Root Cause** | (1) **2020:** COVID-19 lockdowns, social distancing, and temporary business closures reduced in-person visits globally starting March 2020. (2) **2022:** The Yelp Open Dataset release used for this PoC was snapshotted on or shortly after January 19, 2022. No further checkins were captured after that date — this is a **data collection boundary**, not a real-world activity decline. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Exclude 2022 entirely** from year-over-year and trend analyses | 2022 has only 19 days of data — including it in annual aggregations creates a false 95.6% decline. This is the minimum-viable fix for any temporal analysis. Simple and unambiguous. |
| 2 | **Exclude both 2020 and 2022** from baseline/trend modeling | COVID-19 makes 2020 non-representative of normal business patterns. Combined with truncated 2022, these two years should be treated as anomalous periods in any forecasting or trend model. Provides the cleanest baseline for understanding organic growth/decline patterns. |
| 3 | **Add contextual flags** — `is_covid_period` (2020-03 through 2021-06) and `is_truncated_year` (2022) as boolean columns | Preserves all records while enabling analysts to include/exclude these periods contextually. Non-destructive and maximally flexible — different use cases may need different treatments (e.g., a COVID-impact study would specifically *want* 2020 data). |
| 4 | **Normalize 2022 to an annual equivalent** by extrapolating the 19-day volume to 365 days | Risky — 19 days in January is not representative of an entire year due to strong monthly seasonality (January has a seasonality index of 0.949, and summer months index above 1.05). Would create a misleading estimate and false confidence. **Not recommended.** |

##### ✅ Best Logical Solution: **Solution 3 (Flag both periods) + Solution 1 (Exclude 2022 from trends)**

During the Silver-layer cleaning phase, add two boolean columns: `is_covid_period` (TRUE for records between 2020-03-01 and 2021-06-30) and `is_truncated_year` (TRUE for all 2022 records). This non-destructive approach preserves every record while providing explicit metadata for downstream consumers. For any **trend analysis** specifically, apply Solution 1 — exclude 2022 entirely since 19 days cannot represent a year. Solution 2 (also excluding 2020) should be applied when building **baseline models** of normal activity, while 2020 data should be *included* in any COVID-impact or resilience study.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 3 + Solution 1** — Add `is_covid_period` (2020-03 to 2021-06) and `is_truncated_year` (2022) boolean flags. Exclude 2022 entirely from year-over-year trend analyses. Include 2020 selectively based on use case.


---
#### 🟡 DQ-CHECKIN-010 — Extreme Checkin Concentration Inequality

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-010` |
| **Description** | The checkin distribution across businesses exhibits **extreme concentration inequality** with a Gini coefficient of **0.788** (where 0 = perfect equality, 1 = perfect inequality). The top **1% of businesses (1,319)** account for **24.7%** of all checkins, while the top **17.5% (23,051 businesses)** drive **80%** of total checkin volume. Conversely, **9,611 businesses (7.3%)** have only a single checkin, and **32,201 businesses (24.4%)** have 5 or fewer checkins. The distribution is heavily right-skewed (skewness = 47.97) with extreme heavy tails (kurtosis = 4,268). The top single business (`-QI8Qi8XWH3D8y8ethnajA`) alone has **52,144 checkins** — 512× the median of 20. |
| **Detection Phase** | Phase 6.8 — Business Rule & Accuracy Checks → **6.8.2) Statistical Outlier Detection** and **6.8.4) Concentration & Pareto Analysis** |
| **Detected In** | Cell 87: `6.8.2) Statistical Outlier Detection` and Cell 90: `6.8.4) Concentration & Pareto Analysis` |
| **Severity** | 🟡 **Moderate** — This is a **distribution characteristic**, not a data defect. However, it has significant downstream implications: (1) Simple averages are misleading (mean=101 vs. median=20); (2) Models assuming normality will fail; (3) A handful of super-active businesses can dominate aggregate metrics; (4) The 24.4% of businesses with ≤5 checkins may lack sufficient signal for per-business modeling. Severity escalates to **High** if downstream pipelines use untransformed checkin counts in ML features or KPIs. |
| **Affected Records** | All **13,356,875** rows (distribution characteristic affecting the entire dataset). Key subgroups: **1,188 Z-score outlier businesses** (>1,353 checkins), **11,165 extreme IQR outlier businesses** (>270 checkins), **9,611 single-checkin businesses**. |
| **Root Cause** | A natural consequence of the Pareto principle in user-generated platforms: a small number of highly popular businesses (restaurants, bars, entertainment venues) attract disproportionate engagement, while the long tail consists of niche, new, or low-traffic businesses. This is consistent with power-law distributions observed across all Yelp engagement metrics (reviews, tips, photos). |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Log-transform** checkin counts for any model or aggregation that assumes normality | The extreme skewness (47.97) violates normality assumptions. Log transformation (`log1p`) compresses the range and reduces the influence of super-active businesses. Essential for regression, clustering, and distance-based algorithms. |
| 2 | **Segment businesses into activity tiers** using data-driven quartile boundaries: Low (1–5), Medium (6–72), High (73–431), Very High (432+) | Enables tier-specific analysis and prevents high-volume businesses from dominating aggregate metrics. Quartile-based boundaries are reproducible and objective. Allows reporting like "checkin growth among Medium-tier businesses." |
| 3 | **Apply stratified sampling** when creating training datasets to ensure representation of low-activity businesses | Prevents models from being biased toward high-checkin businesses. Ensures minority patterns (single-checkin, zero-span) are adequately represented in training data. Critical for any ML pipeline. |
| 4 | **Investigate single-checkin businesses** (9,611) for data quality issues — are they real businesses with minimal traction or data artifacts? | Single-checkin entries may represent test accounts, defunct businesses, or data import artifacts rather than genuine business activity. Cross-referencing with `yelp_business` can distinguish real low-traffic businesses from noise. |

##### ✅ Best Logical Solution: **Solution 2 (Segment) + Solution 1 (Log-transform)**

During the Silver-layer transformation, create an `activity_tier` categorical column using the quartile boundaries (Q1=6, Median=20, Q3=72, P99=1,277) as natural breakpoints. For any **statistical modeling**, apply `log1p` transformation to the checkin counts to normalize the distribution. This two-pronged approach addresses both **analytical reporting** needs (human-readable tiers) and **modeling requirements** (normalization) without discarding any data. Solution 4 (investigating single-checkin businesses) should also be performed as a follow-up validation step.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2 + Solution 1** — Create an `activity_tier` categorical column (Low/Medium/High/Very High based on quartile boundaries) + apply `log1p` transformation on checkin counts for statistical modeling. Investigate single-checkin businesses as a follow-up.


---
#### 🟡 DQ-CHECKIN-011 — Zero-Span & High-Intensity Business Activity Anomalies

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-011` |
| **Description** | Two complementary anomalies detected in per-business activity patterns: **(1) Zero-Span Businesses:** **9,773 businesses (7.4%)** have all their checkins compressed into a single calendar day (active span = 0 days). These may represent bulk data imports, single-event pop-ups, grand openings, or businesses that appeared and immediately went dormant. **(2) High-Intensity Businesses:** **12 businesses** exceed **100 checkins per active month**, with the maximum reaching **359.5 checkins/month** — indicating abnormally concentrated activity relative to their operating window. The median business spans 1,909 active days (\~5.2 years) with 0.5 checkins/month, making these 12 outliers >200× the typical intensity. Additionally, **100% of businesses** (131,930) have their last checkin more than 3 years ago, which is a dataset staleness artifact already documented in **6.9 Timeliness Checks** (data ends 2022-01-19). |
| **Detection Phase** | Phase 6.8 — Business Rule & Accuracy Checks → **6.8.5) Business Activity Span Analysis** |
| **Detected In** | Cell 92: `6.8.5) Business Activity Span Analysis` |
| **Severity** | 🟡 **Moderate** — Zero-span businesses are not necessarily erroneous (a new business can legitimately receive many checkins on opening day), but the volume (7.4%) is notable and may include data artifacts. High-intensity businesses (12) are a tiny population but could disproportionately skew per-business activity metrics if treated as normal. Severity escalates to **High** if per-business engagement models (churn prediction, popularity scoring) do not account for these edge cases. |
| **Affected Records** | **9,773** zero-span businesses (representing some subset of the 13.3M checkin rows) + **12** high-intensity businesses. Overlap: many zero-span businesses are also the 9,611 single-checkin businesses from DQ-CHECKIN-010. |
| **Root Cause** | (1) **Zero-span businesses:** Combination of genuinely new/flash-in-the-pan businesses, bulk check-in events (opening-day promotions), and possibly data import artifacts where multiple historical checkins were loaded with identical timestamps. (2) **High-intensity businesses:** Likely popular nightlife venues, event spaces, or airport/transit businesses that see extremely high foot traffic in short bursts. (3) **100% inactive >3 years:** Not a business-level issue — the entire dataset was snapshotted in January 2022 and is now 4+ years old. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag zero-span businesses** with a boolean `is_zero_span` column in the Silver layer | Non-destructive flagging allows downstream consumers to decide whether to include or exclude these businesses based on their analytical context. Simple to implement, zero risk of data loss. |
| 2 | **Cross-reference with `yelp_business`** to check whether zero-span businesses have valid profiles (name, address, categories, review counts) | Determines whether zero-span businesses are legitimate businesses with minimal engagement or data quality artifacts (test entries, businesses without real addresses). Evidence-based decision-making. |
| 3 | **Remove zero-span businesses that also have only 1 checkin** (\~9,611 overlap with single-checkin businesses) | These businesses contribute minimal analytical value and may represent noise. However, removal should **only** follow the investigation in Solution 2 to avoid deleting legitimate low-traffic businesses. |
| 4 | **Cap the intensity metric** at the 99th percentile for per-business modeling | Prevents the 12 extreme-intensity businesses from distorting per-business activity metrics in models while preserving them for separate investigation. Standard winsorization technique. |
| 5 | **Create an `activity_profile` composite column** combining span, intensity, and recency into a business health classification (e.g., "flash", "steady", "dormant", "power-user") | Provides a rich, actionable segmentation for downstream analytics. Combines multiple signals (span=0 + count=1 = "flash"; span>3yr + high count = "power-user") into a single interpretable feature. |

##### ✅ Best Logical Solution: **Solution 2 (Investigate) → Solution 1 (Flag) + Solution 5 (Segment)**

First, cross-reference zero-span businesses against the `yelp_business` table to determine whether they have complete business profiles. Businesses with complete profiles (valid address, categories, non-zero review count) are likely legitimate low-engagement venues; those with sparse profiles may be data artifacts. Then apply the `is_zero_span` flag and create the `activity_profile` composite segmentation during the Silver-layer cleaning step. This preserves all records while enabling targeted filtering and richer downstream analytics. The 12 high-intensity outliers should be flagged separately for manual review.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 2 → Solution 1 + Solution 5** — Cross-reference zero-span businesses with `yelp_business` profiles, then add `is_zero_span` boolean flag and create an `activity_profile` composite column (flash/steady/dormant/power-user). Flag the 12 high-intensity outliers for manual review.


---
#### 🟡 DQ-CHECKIN-012 — Significant Data Staleness (\~4 Years Old)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-012` |
| **Description** | The checkin dataset's most recent record dates to **2022-01-19**, making the data approximately **1,731 days (\~4 years, 9 months)** stale as of October 2026. While the dataset spans \~12 years (2009–2022, 4,403 days), the **recency distribution is heavily back-loaded**: the last 3 years of data (2020–2022) contribute only **7.3% of total volume** (972,579 of 13,356,875 rows). The 2020–2022 period shows a sharp volume decline — from \~1.35M checkins/year in 2017 to \~475K in 2020–2021 and just \~21K in January 2022 — consistent with COVID-19's impact on in-person business activity and the Yelp dataset's public-release cutoff date. |
| **Detection Phase** | Phase 6.9 — Timeliness Checks → **6.9.1) Data Freshness** |
| **Detected In** | Cell 96: `6.9.1) Data Freshness` |
| **Severity** | 🟡 **INFO** — not a data defect but a critical contextual constraint |
| **Affected Scope** | Entire dataset (13,356,875 rows) — no records after 2022-01-19 |
| **Impact** | Any analysis, model, or dashboard built on this data will **not reflect current business conditions** (post-2022 trends, new businesses, recovered foot traffic). Time-series forecasting models trained on 2020–2022 data will disproportionately learn pandemic-era patterns rather than normal seasonal behavior. Trend analyses will show a misleading decline that may not represent actual 2023–2026 reality. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Acquire and ingest a newer Yelp dataset release** (if available from Yelp's academic dataset program or API) | Directly resolves the staleness by bringing data up to the current period. Yelp periodically releases updated academic datasets. This is the only solution that actually adds the missing 2022–2026 data. |
| 2 | **Add a data freshness metadata field** (`dataset_release_date` or `data_as_of_date`) to the Bronze layer | Does not fix the gap but makes the limitation **explicit and machine-readable**, so downstream consumers (dashboards, models) can programmatically warn users or filter accordingly. Prevents silent misinterpretation. |
| 3 | **Annotate all downstream analyses with a temporal caveat** (e.g., “Data reflects activity through January 2022 only”) | Ensures any report or model output carries a clear disclaimer. Low-effort safeguard that prevents stakeholders from treating stale results as current. |
| 4 | **Exclude 2020–2022 from trend/forecasting analyses** (or apply pandemic-adjustment weights) | The 2020–2022 period is both sparse (7.3% volume) and structurally different (COVID impact). Including it without adjustment distorts seasonal patterns and growth trends. Excluding or down-weighting it produces more representative historical baselines. |

##### ✅ Best Logical Solution

**Solution 1 (Acquire newer data)** is the ideal long-term fix — the only way to genuinely close the data gap. However, since the Yelp academic dataset release schedule is outside our control, **Solution 3 (temporal caveats)** should be applied **immediately** as a zero-cost safeguard, combined with **Solution 4 (pandemic-era exclusion/adjustment)** for any analytical or modeling work. Solution 2 adds durable metadata governance.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 3 + Solution 4** — Annotate all downstream analyses with a temporal caveat (“Data reflects activity through January 2022 only”). Exclude or adjust 2020–2022 in trend/forecasting analyses. Solution 1 (acquire newer data) is the ideal long-term fix, pending Yelp dataset availability.

---

---

---
#### 🟡 DQ-CHECKIN-013 — Missing Ingestion Metadata (No Load/ETL Timestamps)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-CHECKIN-013` |
| **Description** | The checkin dataset contains **no load timestamp, ETL timestamp, or ingestion metadata columns** of any kind. A systematic search across 13 common patterns (`load_timestamp`, `load_date`, `load_ts`, `etl_timestamp`, `ingestion_timestamp`, `ingested_at`, `created_at`, `_loaded_at`, `_ingestion_time`, `updated_at`, `modified_at`, `last_updated`, `_metadata_timestamp`) returned zero matches. As a direct consequence, **source-to-target latency cannot be computed** — there is no way to measure the delay between when a checkin event occurred at the source (Yelp) and when it became available in the Bronze layer. This means data freshness SLAs cannot be enforced, ingestion anomalies cannot be detected retroactively, and there is no audit trail for when each record entered the pipeline. |
| **Detection Phase** | Phase 6.9 — Timeliness Checks → **6.9.2) Load Timestamp Analysis** and **6.9.3) Source-to-Target Latency** |
| **Detected In** | Cell 98: `6.9.2) Load Timestamp Analysis` and Cell 99: `6.9.3) Source-to-Target Latency` |
| **Severity** | 🟡 **INFO** — data governance gap, not a row-level defect |
| **Affected Scope** | Entire dataset (13,356,875 rows) — 100% of rows lack ingestion metadata |
| **Impact** | **(1) No SLA monitoring:** Cannot verify whether data was loaded within an acceptable time window after generation. **(2) No latency diagnostics:** Cannot identify slow, delayed, or out-of-order ingestion batches. **(3) No audit trail:** Cannot determine when a specific record was first ingested, making root-cause analysis of data issues more difficult. **(4) No incremental load validation:** Cannot distinguish between records from a fresh load vs. historical backfill. This is typical for raw public dataset imports (Yelp academic dataset) but would be a significant gap in a production ETL pipeline. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add `_ingestion_timestamp = current_timestamp()` during Silver layer transformation** | The most standard approach: stamps each record with the time it was processed into the Silver layer. Enables latency measurement (event time vs. ingestion time), SLA monitoring, and incremental load tracking. Uses Spark's built-in `current_timestamp()` so it's zero-cost and deterministic per batch. |
| 2 | **Leverage Delta table metadata** (`_metadata.file_modification_time` or `DESCRIBE HISTORY`) as a proxy | Delta Lake already records commit timestamps. While not a per-row ingestion stamp, `DESCRIBE HISTORY` provides batch-level load times without schema changes. Useful as an interim measure or for retrospective analysis. |
| 3 | **Add `_source_file_name` and `_source_file_timestamp`** using Auto Loader's `cloudFiles.inferColumnTypes` or `input_file_name()` | Captures which source file each record came from and when that file was last modified. Particularly valuable if future data arrives as incremental file drops, enabling file-level lineage and replay. |
| 4 | **Record `_pipeline_run_id` and `_pipeline_run_timestamp`** from the orchestrating job | Ties each record to a specific pipeline execution, enabling traceability from Bronze through Gold. Especially useful for debugging failed or partial loads and for data reconciliation. |

##### ✅ Best Logical Solution

**Solution 1 (Add `_ingestion_timestamp` in Silver)** is the recommended primary fix — it is the industry-standard pattern for Medallion architecture pipelines and directly enables all four blocked capabilities (SLA monitoring, latency diagnostics, audit trail, incremental validation). For this PoC, **Solution 2 (Delta DESCRIBE HISTORY)** can serve as an immediate zero-change proxy until the Silver transformation is built. In production, combine Solutions 1 + 4 for full per-row and per-batch lineage.

> **🔧 Chosen Resolution (Post-Human Review):** **Solution 1** — Add `_ingestion_timestamp = current_timestamp()` during the Silver-layer transformation. Use Delta `DESCRIBE HISTORY` as an interim proxy. In production, combine with `_pipeline_run_id` for full lineage.

---