# 03 — Review Data Quality Issues & Solutions

**Layer:** Bronze  
**Catalog:** `yelp_poc_uc`  
**Schema:** `bronze_final`  
**Table:** `yelp_review`  
**Document Version:** 1.0  
**Last Updated:** 2026-10-17  

---

## 0. Document Description

This document catalogues every data quality (DQ) finding discovered during the **Deep EDA & Quality Assessment** of the `yelp_poc_uc.bronze_final.yelp_review` table. Each finding includes a unique error ID, severity, root cause, suggested solutions, the best logical recommendation, and — after human review — the **chosen cleaning approach** that will be implemented in the Silver-layer cleaning pipeline.

The findings originate from notebook **4.2.3 — Review Deep EDA** and follow the standardised DQ assessment framework (Phases 6.1–6.9).

---

## 1. Data Location

| Attribute | Value |
| --- | --- |
| **Catalog** | `yelp_poc_uc` |
| **Schema** | `bronze_final` |
| **Table** | `yelp_review` |
| **Format** | Delta |
| **Row Count** | 6,990,280 |
| **Columns** | `review_id`, `user_id`, `business_id`, `stars`, `useful`, `funny`, `cool`, `text`, `date` |
| **Source** | Yelp Academic Dataset — static JSON export |

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

## 3. Data Quality Findings


---
#### 🟠 DQ-REVIEW-001 — Data Type Mismatch: `stars` Column

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-001` |
| **Description** | The `stars` column’s declared schema type (`double`) does not match the data contract type (`int`). Star ratings are ordinal values (1–5) with no fractional component, yet the source stores them as 64-bit floating-point. |
| **Detection Phase** | Phase 6.1 — Structural / Schema Checks → **6.1.2) Data Type Conformance** |
| **Detected In** | Cell 27 of this notebook |
| **Confirmed By** | Cell 30 — **6.1.3) Semantic Data Type Violations**: all 6,990,280 values are whole numbers (samples: `1.0`, `4.0`, `5.0`). Also reflected in Cell 35 — **6.1.4) Schema Drift Detection** as a RETYPED finding (`int` → `double`). |
| **Severity** | 🟠 Medium |
| **Affected Rows** | 6,990,280 (100% of the dataset) |
| **Impact** | Wastes 4 bytes/row (double=8B vs int=4B — \~27 MB across the dataset), misleads downstream models into treating a discrete ordinal variable as continuous, and causes floating-point display artefacts (e.g., `4.0` instead of `4`). |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Cast `stars` to `IntegerType`** during the Silver-layer cleaning step | Most direct fix: preserves the 1–5 ordinal semantics, halves storage, prevents continuous-variable misinterpretation, and is universally compatible with Spark, ML pipelines, and BI tools. |
| 2 | Cast to `ShortType` (2 bytes) | Even smaller footprint (2B per row), valid since range 1–5 fits comfortably. However, `ShortType` is less commonly supported across ML frameworks and BI connectors, which may cause downstream compatibility issues. |
| 3 | Leave as `double` and add a `CHECK` constraint (`stars BETWEEN 1 AND 5`) | Avoids a schema change but does **not** fix the type mismatch, wasted storage, or the risk of continuous misinterpretation. Only adds a validation guard. |

##### ✅ Best Logical Solution
**Solution 1 — Cast to `IntegerType`** during the Silver-layer cleaning step. It correctly represents the ordinal domain (1–5), is universally supported, reduces storage by 50%, and is already validated in this notebook’s temporary fix cell (TE.1, Cell 33) with **0 failed casts** across all 6,990,280 rows.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🔴 DQ-REVIEW-002 — Data Type Mismatch: `date` Column

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-002` |
| **Description** | The `date` column’s declared schema type (`string`) does not match the data contract type (`timestamp`). Each row contains a single review timestamp in the format `"YYYY-MM-DD HH:MM:SS"` (e.g., `"2015-08-06 04:21:46"`), but it is stored as a flat string rather than a proper `TimestampType`. |
| **Detection Phase** | Phase 6.1 — Structural / Schema Checks → **6.1.2) Data Type Conformance** |
| **Detected In** | Cell 27 of this notebook |
| **Confirmed By** | Cell 30 — **6.1.3) Semantic Data Type Violations**: 100% of 6,990,280 values are parseable as timestamps (samples: `2015-08-06 04:21:46`, `2014-01-12 02:30:08`, `2019-07-01 03:21:00`). Also reflected in Cell 35 — **6.1.4) Schema Drift Detection** as ADDED (`date:string`) + REMOVED (`review_timestamp:timestamp`). |
| **Severity** | 🔴 High |
| **Affected Rows** | 6,990,280 (100% of the dataset) |
| **Impact** | Prevents native temporal operations (date-range filtering, `year()`/`month()`/`hour()` extraction, window functions), blocks partition pruning and temporal indexing, forces repeated string parsing in every downstream query, and makes time-zone-aware processing impossible. This is the highest-impact finding because temporal analysis is central to review-trend, seasonality, and recency studies. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Cast to `TimestampType`** using `to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")` during the Silver-layer cleaning step | Direct and lossless — the format is uniform across all 6.99M rows (0 null casts validated). Unlocks all temporal operations, enables partition pruning on date ranges, and preserves full datetime precision (hour, minute, second) needed for time-of-day and temporal trend analysis. |
| 2 | Cast to `DateType` (date-only, discard time component) | Simpler type, but **loses the HH:MM:SS component** which is valuable for time-of-day analysis (review hour, AM/PM patterns, peak posting times). Not recommended for a review dataset where time-of-day is analytically significant. |
| 3 | Keep as `string` and parse on-the-fly in downstream queries | Avoids a schema change but forces every downstream consumer to re-parse the same string, creates significant performance overhead on 7M rows, and prevents predicate pushdown on temporal filters. |

##### ✅ Best Logical Solution
**Solution 1 — Cast to `TimestampType`** during the Silver-layer cleaning step. The format is perfectly consistent across all rows (0 failed casts validated in Cell 30), preserves the full datetime precision needed for review-hour and temporal-trend analysis, and is already applied in this notebook’s temporary fix cell (TE.1, Cell 33) as `review_timestamp`.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🚨 DQ-REVIEW-003 — Negative Vote Counts

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-003` |
| **Severity** | FINDING (data integrity violation) |
| **Detected In** | Phase 6.2.1 — Allowed Value Conformance (Cell 38) |
| **Also Confirmed In** | Phase 6.2.2 — Range Conformance (Cell 40); Phase 6.2.4 — Impossible Value Detection (Cell 44) |
| **Affected Columns** | `useful` (4 rows), `funny` (2 rows), `cool` (1 row) |
| **Total Affected Rows** | **7** out of 6,990,280 (0.0001%) |
| **Invalid Value** | `-1` (all three columns share the same offending value) |
| **Domain Rule** | Vote counts are community-generated non-negative integers; users can add votes but cannot subtract them |

**Description:** Seven review records contain a vote count of `-1` across the `useful`, `funny`, and `cool` columns. On the Yelp platform, vote counts represent cumulative community endorsements and are monotonically non-negative — a user can mark a review as "useful" but cannot "un-useful" it into a negative count. The value `-1` is therefore physically impossible under normal platform behaviour.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Clamp to zero** (`SET col = 0 WHERE col < 0`) | Preserves the row and all other fields; treats `-1` as a floor violation and resets to the minimum valid value. Simple, reversible, and safe for downstream aggregations (sums, averages). |
| 2 | **Set to NULL** (`SET col = NULL WHERE col < 0`) | Signals "unknown" rather than asserting a value. Downstream code that already handles NULLs (e.g., `AVG` ignores NULLs) will naturally skip these. Useful if the root cause is a data-pipeline bug and the true value is genuinely unknown. |
| 3 | **Delete the 7 rows entirely** | Removes all trace of the corrupted records. Justified only if the rows are confirmed as test/bot artefacts. Risky because it discards the review text and star rating, which are otherwise valid. |
| 4 | **Flag and quarantine** (add `is_vote_anomaly = True`) | Non-destructive; keeps the data for investigation while letting analysts filter it out. Best for audit-trail workflows. |

**Best Logical Solution:** **Option 1 — Clamp to zero.** The 7 affected rows represent 0.0001% of the dataset, and the only anomalous value is `-1` (exactly one unit below the valid floor). Clamping to `0` is the least disruptive fix: it corrects the domain violation without losing the review's text, star rating, or other valid metadata. The magnitude of the error (`-1` vs `0`) has negligible impact on any aggregate statistic, making this the most proportionate and reversible remediation.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### ⚠️ DQ-REVIEW-004 — Extreme Vote Count Outliers

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-005` |
| **Severity** | INFO (advisory — not a data error, but a quality signal) |
| **Detected In** | Phase 6.2.2 — Range Conformance, Statistical Profiling (Cell 40) |
| **Affected Columns** | `useful` (27,845 rows), `funny` (26,919 rows), `cool` (27,873 rows) |
| **Threshold** | 5σ above the mean for each column |
| **Observed Maximums** | `useful` = 1,182 &nbsp;\|&nbsp; `funny` = 792 &nbsp;\|&nbsp; `cool` = 404 |

**Description:** Approximately 27K reviews per vote column exceed the 5-sigma statistical threshold (`useful` > 17, `funny` > 9, `cool` > 11). While these are not *invalid* values — Yelp does not cap vote counts — such extremes may indicate viral reviews, bot-driven voting, coordinated campaigns, or long-tenured highly-visible reviews that organically accumulated votes over many years.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Retain as-is, document for analysts** | Extreme votes are a legitimate feature of the platform. Analysts can apply their own thresholds during modelling. No data is lost. |
| 2 | **Winsorize at the 99th percentile** during feature engineering | Caps extreme values to reduce skew in ML features (e.g., `useful_capped`). Preserves the original column alongside the capped version. |
| 3 | **Log-transform** vote columns during modelling | Compresses the heavy tail (`log1p(useful)`) without altering the stored data. Standard practice for count features. |
| 4 | **Add a boolean flag** `is_vote_outlier` | Non-destructive labelling for downstream filtering or stratified analysis. |

**Best Logical Solution:** **Option 1 — Retain as-is and document.** At the pre-cleaning discovery stage, these are informational observations, not errors. The correct place to handle vote skew is during feature engineering (Silver/Gold layer), where either winsorization or log-transform can be applied based on the specific modelling task. Altering the Bronze data would lose fidelity.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### ℹ️ DQ-REVIEW-005 — Very Short Reviews (1–9 characters)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-006` |
| **Severity** | INFO (advisory — quality signal for NLP pipelines) |
| **Detected In** | Phase 6.2.2 — Range Conformance, Text Length (Cell 40) |
| **Affected Rows** | **156** out of 6,990,280 (0.002%) |
| **Sample Values** | `*`, `S`, `Au jus`, `-----`, `Thank you` |
| **Text Length Stats** | min = 1 char, max = 5,000 chars, avg = 568 chars |

**Description:** 156 reviews have a text body shorter than 10 characters. While technically non-empty (passing the ≥ 1 char rule), these ultra-short texts carry minimal semantic content for NLP tasks such as sentiment analysis, topic modelling, or review summarisation. Some appear to be single punctuation marks (`*`, `-----`) or fragmentary phrases (`S`, `Au jus`).

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag with `is_short_review = True`** (threshold < 10 chars) | Non-destructive; lets NLP pipelines exclude them without losing the data from aggregation queries. |
| 2 | **Exclude during NLP preprocessing** (Silver layer filter) | Standard NLP practice: minimum token count filters (e.g., ≥ 3 words) are applied during text cleaning. No Bronze modification needed. |
| 3 | **Delete the 156 rows** | Overly aggressive for 0.002% of data. The star rating and vote metadata are still valid even if the text is too short for NLP. |

**Best Logical Solution:** **Option 2 — Exclude during NLP preprocessing at the Silver layer.** These 156 rows are not data errors; they are legitimate but low-information reviews. Minimum token/character filters are a standard NLP preprocessing step and should be applied where text analysis occurs, not at the Bronze/discovery stage.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

---

---
#### ⚠️ DQ-REVIEW-006 — Placeholder/Test-Data Reviews

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-004` |
| **Severity** | FINDING (data quality issue — semi-artificial records) |
| **Detected In** | Phase 6.2.3 — Pattern Conformance (Cell 42) |
| **Affected Rows** | **18** out of 6,990,280 (0.00026%) |
| **Pattern** | Placeholder content or "test" markers in review text |

**Description:** Detected 18 reviews containing placeholder or test data in the `text` column. Examples include generic strings, test artefacts, or boilerplate indicating synthetic or QA-originated reviews. While these pass basic domain and range checks (non-empty, parseable dates, valid IDs), their content is not genuine user-generated feedback and may distort downstream NLP or engagement metrics.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag with `is_placeholder_review = True`** | Non-destructive; lets downstream quality pipelines ignore or stratify without removal. |
| 2 | **Exclude from NLP and engagement modelling** | Treat flagged values as uninformative for analytics. Preserves all linked metadata. |
| 3 | **Delete the 18 rows** | Removes all test-artefact data, but irreversibly discards all review and metadata (votes, stars, business/user linkage).
Risk: overkill at 0.00026%. |

**Best Logical Solution:** **Option 2 — Exclude from downstream analytics.** These placeholder/test records are extremely rare (0.00026%) and may be valuable for audit-trail consistency. Removing them from sentiment/NLP and engagement pipelines prevents quality contamination while preserving a complete record of ingest history. Adding an explicit flag further supports analytical transparency and reporting.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🚨 DQ-REVIEW-007 — Multiple Consecutive Spaces in Review Text

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-007` |
| **Severity** | FINDING (NLP data quality issue) |
| **Detected In** | Phase 6.3.5 — Text Normalization, Part A: Basic Hygiene (Cell 51) |
| **Affected Column** | `text` |
| **Affected Rows** | **2,381,849** out of 6,990,280 (34.07%) |
| **Pattern** | Two or more consecutive space characters within the review body |

**Description:** Over one-third of all reviews contain multiple consecutive space characters (e.g., "Great  place" or "The food   was amazing"). While not a data integrity error, this is the most pervasive text normalization issue in the dataset and directly impacts downstream NLP tasks: tokenizers may produce empty tokens, TF-IDF vectors may be distorted by space-inflated token counts, and text similarity measures may be skewed.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Regex collapse during Silver-layer text preprocessing** (`regexp_replace(text, '  +', ' ')`) | Standard NLP preprocessing step. Preserves all semantic content while normalising whitespace. Non-destructive to the Bronze record. |
| 2 | **Apply in-place on the Bronze table** (`UPDATE ... SET text = regexp_replace(text, '  +', ' ')`) | Faster downstream queries since every consumer gets clean text. Risk: loses the original representation if the extra spaces were intentional formatting. |
| 3 | **Retain as-is, handle in each consumer** | Zero data modification, but every NLP pipeline must independently handle it — error-prone and duplicates effort. |

**Best Logical Solution:** **Option 1 — Regex collapse at the Silver layer.** At 34% prevalence this is a systemic formatting artefact, not an error. Collapsing consecutive spaces is a universal NLP preprocessing step that should happen once during the Bronze→Silver transformation, preserving the raw Bronze record for audit while giving all Silver consumers clean input.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🚨 DQ-REVIEW-008 — Control Characters in Review Text

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-008` |
| **Severity** | FINDING (data integrity / corruption risk) |
| **Detected In** | Phase 6.3.5 — Text Normalization, Part A: Basic Hygiene (Cell 51) |
| **Affected Column** | `text` |
| **Affected Rows** | **18** out of 6,990,280 (0.00026%) |
| **Pattern** | Non-printable control characters (`\x00`–`\x08`, `\x0B`, `\x0C`, `\x0E`–`\x1F`, `\x7F`) excluding normal whitespace (`\n`, `\r`, `\t`) |

**Description:** 18 reviews contain non-printable control characters that are invisible but can corrupt downstream processing: JSON/CSV serialisation may break, tokenizers may produce garbage tokens, and full-text search indexing can fail silently.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Strip control characters during Silver-layer preprocessing** (`regexp_replace(text, '[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]', '')`) | Removes invisible corruption without altering readable content. Standard text-cleaning practice. |
| 2 | **Replace with space** instead of removing | Avoids accidentally merging words (e.g., "word1\x00word2" → "word1 word2" instead of "word1word2"). Marginally safer for NLP tokenisation. |
| 3 | **Flag and quarantine the 18 rows** | Preserves original data for investigation, but at 0.00026% the investigation value is minimal. |

**Best Logical Solution:** **Option 1 — Strip control characters at the Silver layer.** The 18 affected rows are an infinitesimal fraction and the characters carry zero semantic meaning. Stripping them is universally safe and prevents silent downstream failures.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### ⚠️ DQ-REVIEW-009 — HTML Entities in Review Text

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-009` |
| **Severity** | FINDING (encoding issue impacting NLP) |
| **Detected In** | Phase 6.3.5 — Text Normalization, Part B: NLP Checks (Cell 51) |
| **Affected Column** | `text` |
| **Affected Rows** | **97** out of 6,990,280 (0.0014%) |
| **Pattern** | HTML entity sequences: `&amp;`, `&lt;`, `&gt;`, `&nbsp;`, `&quot;`, `&#NNN;`, `&#xHH;` |

**Description:** 97 reviews contain HTML-encoded entities that were not decoded during data ingestion. For example, `&amp;` should be `&`, `&lt;` should be `<`. These artefacts indicate the text was scraped from a web page and the HTML decoding step was incomplete. They corrupt NLP features: a tokenizer sees `&amp;` as a single token instead of the punctuation it represents.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **HTML-unescape during Silver-layer text preprocessing** (Python `html.unescape()` via UDF or `regexp_replace` for common entities) | Restores the text to its intended readable form. Handles both named (`&amp;`) and numeric (`&#38;`) entities. Standard web-data cleaning step. |
| 2 | **Replace only known common entities** (`&amp;`→`&`, `&lt;`→`<`, `&gt;`→`>`, `&nbsp;`→` `) | Simpler regex, covers 95%+ of real-world cases, avoids importing Python’s html module as a UDF. |
| 3 | **Leave as-is, let tokenizers handle it** | Some modern tokenizers (SentencePiece, BPE) can learn entity patterns. But standard Spark NLP and NLTK tokenizers will not. |

**Best Logical Solution:** **Option 1 — Full HTML unescape at the Silver layer.** A comprehensive unescape is trivially implemented and handles all entity forms. At only 97 rows the performance cost is nil, and it ensures all downstream NLP consumers receive semantically correct text.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### ℹ️ DQ-REVIEW-010 — NLP Readiness Advisory Signals

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-010` |
| **Severity** | INFO (advisory — not data errors, quality signals for NLP pipelines) |
| **Detected In** | Phase 6.3.5 — Text Normalization, Part B: NLP Checks (Cell 51) |
| **Affected Column** | `text` |

| Signal | Count | % of Dataset | Description |
| --- | --- | --- | --- |
| Excessive newlines (4+ consecutive) | 4,566 | 0.065% | Reviews with large paragraph gaps; may indicate copy-paste from other sources |
| Carriage returns (`\r`) | 389 | 0.006% | Windows-style line endings (`\r\n`); inconsistent with Unix-style (`\n`) in the rest of the dataset |
| URLs in review text | 12,551 | 0.180% | Reviews containing `http://`, `https://`, or `www.` links |

**Description:** These three signals are **not data errors** but important quality indicators for NLP preprocessing design. Excessive newlines may need collapsing, carriage returns should be normalised to `\n`, and URLs may need removal or replacement with a `<URL>` token depending on the NLP task.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Normalise all three during Silver-layer NLP preprocessing**: collapse `\n{3,}` → `\n\n`, replace `\r\n` → `\n` and `\r` → `\n`, optionally replace URLs with `<URL>` token | One-pass regex chain handles all three. Standard NLP text-cleaning practice. |
| 2 | **Retain as-is at Silver, handle per-task at Gold** | Different NLP tasks may want URLs preserved (e.g., link analysis) vs. removed (sentiment). Flexible but duplicates effort. |

**Best Logical Solution:** **Option 1 — Normalise at the Silver layer** for the newline/CR issues (universally beneficial), but **Option 2 for URLs** (task-dependent, better deferred to Gold-layer feature engineering).

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

---

---
#### ⚠️ DQ-REVIEW-011 — Masked Null Tokens in Review Text

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-011` |
| **Severity** | FINDING (data integrity — non-standard null representations) |
| **Detected In** | Phase 6.3.6 — Null Representation Consistency, Check 1 (Cell 53) |
| **Affected Column** | `text` |
| **Affected Rows** | **11** out of 6,990,280 (0.00016%) |
| **Values Found** | `*`, `Tbd`, `.` |

**Description:** 11 reviews contain text that matches well-known null-masking tokens: a single asterisk (`*`), the abbreviation `Tbd` ("to be determined"), and a lone period (`.`). These are not genuine review content — they represent cases where the review body was left effectively blank but a placeholder character was entered instead of a proper NULL. They will pollute NLP pipelines with noise tokens and inflate non-null row counts, masking the true completeness picture.

**Suggested Solutions:**

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Convert to proper NULL** (`SET text = NULL WHERE LOWER(TRIM(text)) IN ('*', 'tbd', '.')`) | Makes missingness explicit and correctable in section 6.4 Completeness Checks. Standard null-normalisation practice. |
| 2 | **Flag with `is_masked_null = True`** | Non-destructive; preserves the original value for investigation while letting downstream pipelines exclude flagged rows. |
| 3 | **Delete the 11 rows** | Overkill — the star rating and vote metadata on these rows are still valid. |

**Best Logical Solution:** **Option 1 — Convert to proper NULL at the Silver layer.** These are unambiguously non-content values. Converting them to NULL makes the dataset’s completeness semantics honest and allows NLP pipelines to exclude them via a simple `WHERE text IS NOT NULL` filter instead of maintaining a list of masked-null patterns.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🔁 6.3.6 Confirmation: DQ-REVIEW-004 (Placeholder Reviews)

Phase 6.3.6 Null Representation Consistency, Check 2 (Cell 53) independently detected **10 no-comment placeholder reviews** (`-----`, `..`, `...`). These are a **subset** of the 18 placeholder/test-data reviews already fully documented under **DQ-REVIEW-004** after 6.2.3 (Cell 42).

- The 10 rows detected here used the no-comment pattern list (`-----`, `..`, `...`, etc.).
- The original DQ-REVIEW-004 detection used a broader placeholder pattern list and captured all 18.
- No new remediation is needed beyond what DQ-REVIEW-004 already prescribes.

---

---
#### 🟠 DQ-REVIEW-012 — Temporal Gaps: Incomplete Boundary Years (2005 & 2022)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-012` |
| **Severity** | 🟠 **Medium** — the core dataset (2006–2021) has all 192 expected year-months fully covered; however, the two boundary years are incomplete and will distort year-over-year and seasonal trend analyses if included without adjustment. |
| **Detected In** | Phase 6.4 — Completeness Checks → **6.4.6) Temporal Gap Analysis** (Cell 61) |
| **Affected Column** | `date` (parsed to year/month) |
| **Affected Rows** | **2005:** 854 rows (0.012%); **2022:** 31,665 rows (0.453%) — combined 32,519 of 6,990,280 total rows (0.47%) |

**Description:** The review dataset spans **2005–2022** (18 years) but has **12 missing year-months** concentrated at the temporal boundaries:

* **2005:** January is missing — the earliest review is February 2005, with only **3 reviews** that month. The entire year has just **854 reviews**, confirming it represents the Yelp platform’s launch ramp rather than a data collection gap.
* **2022:** February through December are missing — only January 2022 is present, with **31,665 reviews**. This represents a dataset extraction cutoff, not a data quality issue.

Additionally, **4 years** fall below 10% of the median annual volume (335,260):

| Year | Reviews | % of Median | Nature |
| --- | --- | --- | --- |
| 2005 | 854 | 0.25% | Platform launch — negligible volume |
| 2006 | 3,853 | 1.15% | Early growth — statistically fragile |
| 2007 | 15,363 | 4.58% | Growth phase — approaching viability |
| 2022 | 31,665 | 9.45% | Extraction cutoff — January only |

> **📝 Contextual Note — COVID-19 Impact:** A visible volume dip appears in **2020** (554,557 reviews, \~39% drop from the 2019 peak of 907,284) and **2021** (618,189). This is consistent with the pandemic’s impact on restaurant foot traffic and is a **real-world phenomenon**, not a data quality issue. It should be acknowledged in temporal trend analyses but does not require remediation.

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Filter to complete years (2006–2021) for temporal analyses** | Removes both boundary years, ensuring all year-over-year and seasonal comparisons use complete 12-month periods. Simplest approach — eliminates distortion with no schema changes. Discards 32,519 valid review records from temporal queries. |
| 2 | **Retain all years but add an `is_complete_year` boolean flag** | Preserves the full dataset while giving downstream consumers a queryable indicator to filter at will. Temporal trend queries filter on `is_complete_year = True`; exploratory or business-level queries retain full access to boundary data. Non-destructive and metadata-driven. |
| 3 | **Exclude 2005 entirely; annotate 2022 with a cutoff marker** | 2005 has only 854 records — statistically meaningless for trend analysis. 2022’s 31,665 January records carry analytical value for month-level or business-level questions. This hybrid approach removes only the truly negligible boundary. |
| 4 | **Document as a known limitation — no transformation** | If the cleaning pipeline is scope-limited to structural/value fixes, document the boundary-year incompleteness as a known limitation for downstream analysts. |

##### ✅ Recommended Solution

**Solution 2 — Add an `is_complete_year` flag column.** This preserves every record while providing an explicit, queryable indicator of year completeness. Temporal trend analyses filter to `is_complete_year = True` (yielding the clean 2006–2021 window) for reliable year-over-year and seasonal comparisons, while exploratory analyses retain full access. The decision stays in the downstream analyst’s hands — critical for a reusable Bronze → Silver transformation.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### ℹ️ DQ-REVIEW-013 — Sparse Early Years: Platform Growth Phase (2005–2008)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-013` |
| **Severity** | ℹ️ **Info** (advisory — not a data error, platform growth artefact) |
| **Detected In** | Phase 6.4 — Completeness Checks → **6.4.6) Temporal Gap Analysis** (Cell 61) |
| **Affected Rows** | 2005–2008 combined: **68,296 rows** (0.98% of dataset) |

**Description:** The first four years of the dataset (2005–2008) represent Yelp’s early platform growth phase. Monthly volumes are extremely low — 2005 averages **78 reviews/month**, while the mature period (2015–2019) averages **\~63,000 reviews/month**. These years are all present (11/12 months in 2005, all 12 in 2006–2008) but are statistically fragile for any per-month or per-business aggregation.

| Signal | 2005 | 2006 | 2007 | 2008 | 2009+ |
| --- | --- | --- | --- | --- | --- |
| Annual volume | 854 | 3,853 | 15,363 | 48,226 | 74,387+ |
| Avg monthly volume | 78 | 321 | 1,280 | 4,019 | 6,199+ |
| % of median year | 0.25% | 1.15% | 4.58% | 14.39% | 22.19%+ |

**Suggested Handling:** No remediation needed. These years reflect genuine platform adoption and should be retained. However, downstream analyses involving per-month, per-business, or per-user aggregations should consider setting a **minimum-volume threshold** (e.g., 2010+ for reliable trend analysis) or weighting early observations accordingly.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🟠 DQ-REVIEW-014 — Multi-Review User-Business Pairs

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-014` |
| **Severity** | 🟠 **Medium** — 3.50% of total rows are affected; the core review data is sound (`review_id` is 100% unique), but duplicate user-business pairs inflate review counts per business and per user, distorting popularity rankings, rating averages, and recommendation models. |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.3) Composite Key Uniqueness**, Check 1 |
| **Affected Records** | **244,520 excess reviews** across **212,788 user-business pairs** with >1 review (457,308 total rows involved, 6.54% of dataset). |
| **Pattern** | Long-tail distribution: 190,088 pairs have 2 reviews, 17,601 have 3, tapering to 1 pair with 40 reviews. Top offender: user `a0DnfD31lNdiBTY2-YBBFA` — 40 reviews for business `C6YaSrMAzy3jJqinlFVudw`, 38 for `FCCnOgjjvwQJBAy3u2fcLA`. |
| **Root Cause** | On the live Yelp platform, a user may have only one active review per business. When users update their review, the platform replaces the old version. However, the Yelp Academic Dataset appears to retain **historical versions** of each review as separate rows (each with a distinct `review_id`). This is not a data quality defect per se — it is a design characteristic of the export. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep only the latest review per (user_id, business_id)** — `ROW_NUMBER() OVER (PARTITION BY user_id, business_id ORDER BY date DESC) = 1` | Mirrors the live Yelp experience where only the most recent review is visible. Suitable for recommendation engines and rating aggregation. |
| 2 | **Add an `is_latest_review` boolean flag** (window-based, non-destructive) | Preserves all historical versions for temporal analysis (e.g., sentiment drift, rating evolution) while giving downstream consumers a simple filter for current-state views. |
| 3 | **Retain all versions as-is; document the multi-review pattern** | If the analytical goal includes studying how users revise their opinions over time (review update behavior, rating trajectory), all versions are valuable. Downstream aggregations must account for this by deduplicating per (user_id, business_id) when computing per-business metrics. |

##### ✅ Best Logical Solution: **Option 2 — Add `is_latest_review` flag at the Silver layer.**

This non-destructive approach preserves the full review history (valuable for NLP sentiment-over-time analysis and review-update behavior studies) while providing a clean filter (`WHERE is_latest_review = TRUE`) for all standard analytics: rating averages, review counts, business popularity, and recommendation pipelines. It satisfies both the current-state and historical-analysis use cases without data loss.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🟠 DQ-REVIEW-015 — Exact Timestamp Collisions & Near-Duplicate Reviews

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-015` |
| **Severity** | 🟠 **Medium** — the 25 exact collisions are a clear anomaly (a user cannot organically submit two distinct reviews at the same second); the 1,409 near-duplicates (≤24h) represent a broader pattern of rapid re-reviews that may or may not be legitimate. |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.3) Composite Key Uniqueness**, Checks 2 & 3 |
| **Affected Records** | **25 exact timestamp collisions** (0.0004%) + **1,409 near-duplicate pairs** within 24h window (same user, same business). |
| **Near-Dup Breakdown** | Same timestamp: 25 \| Within 1h: 1,144 \| 1–6h: 108 \| 6–24h: 132 |
| **Key Observation** | Each collision/near-dup pair has **different `review_id` values** — these are genuinely distinct records in the source data, not duplicate ingestion artifacts. Combined with DQ-REVIEW-014 (multi-review pairs), these are the most temporally proximate subset of the retained historical review versions. |
| **Root Cause** | The 25 exact-second collisions likely represent rapid review edits captured as separate records with identical timestamps (system-generated timestamp precision). The 1,144 within-1-hour near-duplicates are consistent with review update behavior where a user posts, then quickly revises or replaces their review. |
| **Related Finding** | `DQ-REVIEW-014` — all 1,409 near-duplicate pairs are a subset of the 244,520 multi-review user-business pairs. |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Subsume under DQ-REVIEW-014 resolution** — the `is_latest_review` flag naturally handles these cases | If the latest-review dedup from DQ-REVIEW-014 is applied, near-duplicates are implicitly resolved because only the most recent review survives per (user_id, business_id). No additional transformation needed. |
| 2 | **Add `is_near_duplicate` boolean flag** (window-based, ≤24h gap detection) | Non-destructive flag that marks temporally suspicious pairs. Useful if the analysis retains all historical versions but wants to identify likely rapid-edit pairs. |
| 3 | **Drop exact-second collisions only** (25 rows) | Conservative: removes only the clearly anomalous records while preserving the broader near-duplicate set for investigation. |

##### ✅ Best Logical Solution: **Option 1 — Subsume under DQ-REVIEW-014 resolution.**

The `is_latest_review` flag from DQ-REVIEW-014 already resolves this issue: keeping only the latest review per (user_id, business_id) pair eliminates both exact collisions and near-duplicates by construction. Adding a separate `is_near_duplicate` flag (Option 2) provides additional granularity only if the analysis specifically studies rapid review-edit behavior. For standard analytics pipelines, the DQ-REVIEW-014 solution is sufficient.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |



---
#### 🟡 DQ-REVIEW-016 — Negative Vote Counts (useful, funny, cool)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-016` |
| **Severity** | 🟡 **Low** — only **5 rows** (0.000072% of 6,990,280) are affected. The anomaly is cosmetically minor and has near-zero impact on aggregate metrics, but it violates the domain constraint that vote counts must be ≥ 0 and should be corrected for data integrity. |
| **Detected In** | Phase 6.5 — Uniqueness Checks → **6.5.4) Distribution Uniqueness** (Vote Columns Summary Stats sub-section) |
| **Detected In Cell** | Cell 70: `6.5.4) Distribution Uniqueness` |
| **Description** | All three vote columns (`useful`, `funny`, `cool`) contain a minimum value of **-1**. Vote counts represent how many users found a review useful, funny, or cool — they are by definition non-negative integers. A value of -1 is semantically invalid and likely represents a system-level placeholder or encoding artifact rather than an actual negative vote. |
| **Affected Records** | **5 rows total**: `useful < 0`: 4 rows \| `funny < 0`: 2 rows \| `cool < 0`: 1 row \| all three negative: 1 row. All negative values are exactly **-1** (no -2, -3, etc.). |
| **Pattern** | 3 of the 5 affected rows involve the **same business** (`SJQePJL1bkeEk-6gp-6eeA`) — all with `useful = -1`. The remaining 2 rows target distinct businesses. Dates span 2016–2018. The concentration on one business suggests a business-level flag or system artifact rather than random corruption. |
| **Affected `review_id`s** | `JcWLycg0Hdsz6LBNAqS37A` (all three = -1), `gk_9N_c-7oEgzrXVDaR8-g`, `iYS4pfUCUPT_TEmW6BOnrA`, `plhadSWfXZFKRAhEztb6Dw` (useful = -1), `7tlF2NEDiFqMRjKBgTPJ3g` (funny = -1, but useful = 2, cool = 2) |
| **Root Cause** | The Yelp platform likely uses -1 as a sentinel value to indicate a special state — e.g., a review whose votes were invalidated, a review flagged by moderators, or a vote-count rollback. The Yelp Academic Dataset exports this raw internal representation without mapping it back to a neutral value (0 or NULL). |

---

##### 💡 Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Clamp to zero: `GREATEST(col, 0)`** at the Silver layer | Simple and deterministic — replaces -1 with 0 for all three vote columns. Since -1 is semantically "no votes" or "invalid," mapping to 0 preserves the intended meaning without losing non-negative vote data. Applies universally and requires no conditional logic. |
| 2 | **Replace with NULL** to distinguish from genuine zero-vote reviews | Marks the 5 rows as "vote data unavailable" rather than asserting zero votes. Downstream aggregations (`AVG`, `SUM`) automatically exclude NULLs, preventing even marginal distortion. However, it introduces NULLs into otherwise 100%-complete columns, which may complicate downstream NULL-handling logic. |
| 3 | **Flag and retain: add an `has_invalid_votes` boolean column** | Non-destructive approach that preserves the raw -1 values for investigation while flagging them for downstream consumers. Useful if the root cause needs further upstream investigation before deciding on the correction. |

##### ✅ Best Logical Solution: **Option 1 — Clamp to zero with `GREATEST(col, 0)`.**

With only 5 rows affected and the sentinel meaning of -1 being functionally equivalent to "no valid votes," clamping to 0 is the simplest, most maintainable solution. It eliminates the domain violation without introducing NULLs or extra columns. The transformation `GREATEST(useful, 0)` is also idempotent and safe to apply across all rows — it has no effect on the 6,990,275 rows that already have values ≥ 0.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🟠 DQ-REVIEW-017 — User-ID / Business-ID Namespace Collision

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-017` |
| **Description** | **1 review** has an identical value for both `user_id` and `business_id` (`lJzfEleeQ1uYnYQp37LD9Q`). In Yelp's data model, user identifiers and business identifiers occupy distinct namespaces — a user’s 22-char hash should never equal a business’s 22-char hash under normal circumstances. This collision means either (a) a hash-generation defect mapped two different entities to the same key, (b) the record is a synthetic/test row that was not scrubbed before the academic dataset release, or (c) a data export artifact merged two ID columns. |
| **Detection Phase** | Phase 6.6 → **6.6.3) Logical Contradiction Detection** (Part A: Review Domain Contradictions) |
| **Detected In** | Cell 76: `6.6.3) Logical Contradiction Detection` |
| **Severity** | 🟠 **Medium** — only 1 row (0.00001% of 6,990,280), but the violation is semantically severe: any join, grouping, or aggregation that pivots on `user_id` or `business_id` independently will conflate this user with this business, producing incorrect user-level and business-level metrics |
| **Affected Record** | `review_id = YwEHOUJF51lMefLLD_DO6w`, `user_id = business_id = lJzfEleeQ1uYnYQp37LD9Q`, `stars = 1`, `useful = 0`, `funny = 0`, `cool = 0`, `date = 2021-07-19 17:44:12` |
| **Pattern** | The review is a **1-star review with zero engagement** (no useful, funny, or cool votes) posted in July 2021. The absence of any community interaction and the ID collision together suggest this may be a test or system-generated record rather than a genuine user review. |
| **Impact** | **(1)** User-level aggregations (e.g., average stars per user, reviews per user) will include a business entity as if it were a user. **(2)** Business-level aggregations will include a user entity as if it were a business. **(3)** FK joins from review → yelp_user and review → yelp_business may both succeed for this row, creating a spurious link. **(4)** Any deduplication logic keyed on `(user_id, business_id)` will treat this as a self-referential record. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag and quarantine the row** — add an `is_id_collision` boolean column and exclude the row from analytical queries | Non-destructive: preserves the raw record for investigation while preventing it from distorting downstream aggregations. Simple WHERE clause (`WHERE NOT is_id_collision`) in Silver views. |
| 2 | **Delete the row** at the Silver layer during cleaning | With only 1 affected row out of \~7M, removal has zero statistical impact. Eliminates the contamination source entirely. However, hard deletes reduce auditability. |
| 3 | **NULL out the `business_id`** (or `user_id`) to break the collision while preserving the record | Retains the review for text/NLP analysis while severing the problematic ID link. The row would appear in user-level analyses (with a NULL business) or vice versa, depending on which ID is nullified. Requires a policy decision on which ID to trust. |
| 4 | **Cross-reference against `yelp_user` and `yelp_business`** to determine which ID is valid | Look up `lJzfEleeQ1uYnYQp37LD9Q` in both reference tables. If it exists in one but not the other, the valid table tells us which column is correct and the other can be NULLed. If it exists in both, this confirms a genuine hash collision at the source. |

##### ✅ Best Logical Solution

**Solution 4 (Cross-reference) followed by Solution 1 (Flag and quarantine).** First, determine whether the shared ID `lJzfEleeQ1uYnYQp37LD9Q` exists in `yelp_user`, `yelp_business`, or both — this disambiguates the root cause. Then flag the row with `is_id_collision = true` so downstream queries can exclude it without losing the raw audit trail. If the cross-reference reveals the ID belongs to only one table, additionally correct the invalid column to NULL.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---


---

#### 🟡 DQ-REVIEW-018 — Disproportionate Cool Vote Counts (cool > useful + funny)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-018` |
| **Description** | **915 reviews** (0.013% of 6,990,280) have a `cool` vote count that exceeds the combined `useful + funny` count, with `cool > 10`. On Yelp, `useful`, `funny`, and `cool` are independently cast vote types, so this is not inherently impossible — but the pattern is statistically unusual because `cool` votes are typically correlated with `useful` votes. A review that many people found "cool" but few found "useful" or "funny" deviates from the expected engagement pattern. |
| **Detection Phase** | Phase 6.6 → **6.6.4) Cross-Column Range Logic** (Part A: Review Domain Ranges) |
| **Detected In** | Cell 78: `6.6.4) Cross-Column Range Logic` |
| **Severity** | 🟡 **Low / Informational** — 915 rows (0.013%), not a hard data defect but a plausibility anomaly worth documenting for downstream awareness |
| **Affected Scope** | 915 rows — `cool` range [11, 360] (avg 15.6), `useful` range [0, 359] (avg 9.6), `funny` range [0, 21] (avg 1.7), excess (`cool - useful - funny`) range [1, 27] (avg 4.3) |
| **Pattern** | **96.4% of affected reviews are 4–5 star ratings** (626 five-star, 256 four-star, 24 three-star, 3 two-star, 6 one-star). This strongly suggests the pattern reflects **high-quality positive reviews** that are stylishly written — users click “cool” to acknowledge the writing quality rather than its informational value (“useful”) or humor (“funny”). The top excess cases show reviews with 23–30 cool votes and 0–5 useful/funny votes, consistent with aspirational restaurant or travel reviews. |
| **Impact** | **(1)** Any composite engagement metric that assumes `cool ≤ useful + funny` will incorrectly flag these reviews as anomalous. **(2)** Engagement-weighted models (e.g., review quality scoring) may overweight cool votes if the three vote types are assumed to be balanced. **(3)** If using vote counts as features for NLP or fraud detection, the disproportionate cool pattern could confuse classifiers that expect correlated vote distributions. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **No action required — document as known pattern** | The 915 rows are genuine reviews with legitimate (if disproportionate) community voting behavior. The excess is modest (avg 4.3 extra cool votes) and the affected reviews are overwhelmingly high-quality positive reviews. Removing or modifying them would distort the real engagement signal. |
| 2 | **Add an `is_cool_disproportionate` flag** for downstream awareness | Non-destructive annotation that lets analysts and model builders decide how to handle these rows. Particularly useful if building vote-based feature engineering where balanced vote distributions are assumed. |
| 3 | **Cap cool votes at `useful + funny + N` (e.g., N=10)** during feature engineering (not raw data) | Only relevant in modelling contexts where vote balance matters. Preserves raw data integrity while providing a transformed feature. Should be applied at the Gold/feature-engineering layer, never at Bronze or Silver. |

##### ✅ Best Logical Solution

**Solution 1 (No action — document as known pattern).** The 915 affected reviews are genuine high-quality content with legitimate voting behavior. The "cool" vote type on Yelp captures an aesthetic dimension (well-written, inspiring, aspirational) that is distinct from "useful" (informational) and "funny" (humorous), making an imbalance entirely plausible for certain review styles. The excess is small (avg 4.3) and concentrated in 4–5 star reviews. Capping or removing these values would destroy real engagement signal. Instead, document the pattern so downstream consumers can account for it if building models that assume vote-type correlation.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |



---
#### 🚠 DQ-REVIEW-019 — Orphan User IDs (FK Violation)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-019` |
| **Description** | **32 distinct `user_id` values** in the review table have no matching parent record in `yelp_user`. These 32 orphan user_ids affect **33 review rows** (one user has 2 reviews). All orphan user_ids conform to the valid 22-char hash format, so the values are structurally correct but simply absent from the user table. |
| **Detection Phase** | Phase 6.7 → **6.7.1) FK Validity** (FK-2: review.user_id → yelp_user.user_id, View 1: Orphan Records) |
| **Detected In** | Cell 84: `6.7.1) FK Validity` |
| **Severity** | 🟠 **Medium** — only 33 rows (0.00047% of 6,990,280), but any FK violation means these reviews cannot be joined to user metadata (yelping_since, elite status, friend network, compliments). User-level aggregations will silently exclude these 32 users if using INNER JOIN, or produce NULLs if using LEFT JOIN. |
| **Affected Records** | 32 distinct `user_id`s including: `G0PWeUgNeGDobntevJlJ1g`, `0oMk8hhMqiSt4G1BJjjG6g`, `3N6-acEgosQSbipmBZKoSg`, `qH_QwXBgA4Z7WyCjtwRi2A`, `lzpM_Vf2rKA4ivGtAIOH4w`, `tquAg8GqbhN5k6Hkd23M0A`, `u8cq-5zzD7dPSa3LR8rIMw`, `5iBVQ3OeK8lV4Z_4PXc1Xw`, `5-L9Oo6PyUiBjeqj1GsJyQ`, `OxvEeexqWWeWkPwkKtTy2Q` (10 of 32 shown). |
| **Root Cause** | Likely users who were deleted, suspended, or merged after their reviews were posted. The Yelp academic dataset is a static export; user deletions that occurred between the user-table snapshot and the review-table snapshot would create orphans. |
| **Impact** | **(1)** INNER JOINs between review and yelp_user will silently drop 33 rows. **(2)** Any user-level feature engineering (e.g., user tenure, elite status, friend count) will be NULL for these reviews. **(3)** Deduplication or user-cohort analyses will miss these users entirely. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag with `is_orphan_user = true`** and use LEFT JOINs | Non-destructive. Preserves reviews for text/NLP analysis while making the FK gap explicit. |
| 2 | **Exclude the 33 rows** at the Silver layer | With 0.00047% impact, removal has zero statistical effect. Simplifies downstream joins. |
| 3 | **Create stub user records** with NULL metadata | Ensures INNER JOINs retain these rows. Artificial but complete. |

##### ✅ Best Logical Solution: Solution 1 (flag and LEFT JOIN).

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🟡 DQ-REVIEW-020 — Low Review↔Tip User Overlap

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-020` |
| **Description** | Only **15.2%** of reviewing users (301,758 out of 1,987,929) also appear in `yelp_tip`. The remaining **1,686,171 users (84.8%)** have reviews but zero tips. Conversely, **all** tip users also have reviews (0 tip-only users). The business_id dimension shows better overlap: 87.8% with `yelp_checkin` and 70.6% with `yelp_tip`. |
| **Detection Phase** | Phase 6.7 → **6.7.2) Lookup Table Conformance** (Part B: user_id Cross-Referencing) |
| **Detected In** | Cell 85: `6.7.2) Lookup Table Conformance` |
| **Severity** | 🟡 **Low** — This is a **behavioral data gap**, not a data defect. Tips are a voluntary, lower-frequency user action; most Yelp users write reviews but never leave tips. The 15.2% overlap is consistent with expected engagement asymmetry. However, it becomes medium-severity if downstream models assume tip coverage mirrors review coverage (e.g., using tip sentiment as a feature for all users). |
| **Impact** | **(1)** Joining review and tip tables on `user_id` via INNER JOIN drops 84.8% of review users. **(2)** User-level features derived from tips (tip count, tip text sentiment) will be NULL for the majority. **(3)** Any analysis of "users who both review and tip" is limited to 15.2% of the user base. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Use LEFT JOIN** when combining review and tip tables on `user_id` | Preserves all review users; tip columns are NULL for the 84.8% non-tippers. Prevents silent data loss. |
| 2 | **Add `has_tips` boolean** to the Silver-layer user dimension | Lets downstream consumers explicitly segment by tip availability. |
| 3 | **Document the 15.2% coverage** in the data contract | Prevents future analysts from assuming 1:1 correspondence. |

##### ✅ Best Logical Solution: Solution 1 + Solution 3.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

#### 🟠 DQ-REVIEW-021 — Cross-Table Aggregate Drift (Business & User)

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-021` |
| **Description** | Significant drift between **computed review aggregates** and **stored values** in `yelp_business` and `yelp_user`. The Yelp academic dataset is a geographic subset of Yelp's full platform; user and business tables carry lifetime counters that reflect activity beyond this dataset's scope. Key metrics: |
| **Detection Phase** | Phase 6.7 → **6.7.3) Cross-Table Aggregate Reconciliation** (Checks 1–5) |
| **Detected In** | Cell 87: `6.7.3) Cross-Table Aggregate Reconciliation` |

##### Drift Summary

| Check | Exact Match | Direction | Anomalous Cases |
| --- | --- | --- | --- |
| `review_count` per business | 55.2% (67,362 mismatches) | 67,353 computed > stored | Most businesses have MORE reviews in the dataset than `business.review_count` claims |
| `avg stars` per business | 99.7% within 0.5 | Balanced | Only 408 outliers beyond 0.5 — PASS |
| `review_count` per user | 24.0% (1,511,189 mismatches) | 1,493,524 stored > computed | Expected: users review outside this geographic scope |
| `avg stars` per user | 62.7% within 0.5 | N/A | 741,724 beyond 0.5 tolerance |
| `useful` votes per user | 44.3% exact | 1,995 computed > stored | Anomalous: dataset has more useful votes than user table claims |
| `funny` votes per user | 64.4% exact | 485 computed > stored | Minor anomaly |
| `cool` votes per user | 61.7% exact | 388 computed > stored | Minor anomaly |

| Attribute | Detail |
| --- | --- |
| **Severity** | 🟠 **Medium** — The drift is **structurally expected** for a geographic subset: `yelp_user.review_count` and `average_stars` reflect a user's global Yelp history, while this dataset only contains reviews for businesses in selected metros. However, two anomalies warrant investigation: **(a)** 67,353 businesses where computed review count **exceeds** stored `review_count` (the business table may have been snapshotted before the final review export), and **(b)** 17,665 users and 1,995/485/388 users where computed review counts or vote totals **exceed** stored values (these should never happen if stored values reflect lifetime totals). |
| **Root Cause** | The five Yelp academic dataset tables are exported as **independent snapshots** at potentially different points in time. The `review_count`, `stars`, `average_stars`, and vote columns in `yelp_business` and `yelp_user` are **cached aggregates** that may lag behind the actual review table. Additionally, reviews removed for policy violations after the review-table export but before the business/user-table export would reduce the stored counter without affecting the review-table rows. |
| **Impact** | **(1)** Using `yelp_business.review_count` directly will undercount for 67,353 businesses. **(2)** Using `yelp_user.review_count` will overcount for 1,493,524 users and undercount for 17,665. **(3)** Vote totals from `yelp_user` are unreliable for 36–56% of users. **(4)** `yelp_user.average_stars` differs by >0.5 stars for 37.3% of users. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Always compute aggregates from the review table** rather than using stored counters | Guarantees consistency within the dataset. Use `COUNT(*)` per business/user, `AVG(stars)`, and `SUM(useful/funny/cool)` from the review table directly. |
| 2 | **Add computed columns** to Silver-layer business/user dimensions | Replace `yelp_business.review_count` with `computed_review_count` and flag the delta: `review_count_drift = stored - computed`. |
| 3 | **Flag the 17,665 anomalous users** (computed > stored) for investigation | These violate the invariant that stored lifetime counters should always be ≥ dataset-subset counters. May indicate data export defects. |

##### ✅ Best Logical Solution: Solution 1 + Solution 2.
Never trust pre-computed counters from parent tables in the Yelp academic dataset. Always derive aggregates from the review table at the Silver layer, and carry the drift as a diagnostic column for data quality monitoring.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🟡 DQ-REVIEW-022 — Suspicious Reviewer Behavior Patterns

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-022` |
| **Description** | The revised review-focused rule set surfaces several **behavioral anomaly patterns** rather than hard data-integrity failures: **732 user-day combinations** across **641 users** exceed **20 reviews in a single day** (top case: user `n-lBS02-3yvlY5Q91mmwDA` posted **121 reviews on 2005-07-10**), **145 user-text groups** reuse the same normalized review text across **3+ businesses** affecting **557 reviews**, **7 reviews** are ultra-short (`<15` chars) yet received **10+ votes**, and **3,070 reviews** accumulated **more than 100 total votes**. These patterns are plausible on an open review platform, but they are important for spam detection, reviewer-behavior analysis, and downstream model robustness. |
| **Detection Phase** | Phase 6.8 → **6.8.1) Business Rule Violations** |
| **Detected In** | Cell 90: `6.8.1) Business Rule Violations` |
| **Severity** | 🟡 **Moderate** — These are not schema violations, but they can distort trust, reviewer-level features, engagement models, and anomaly detection if treated as ordinary behavior. Severity increases for fraud/spam use cases. |
| **Affected Records** | 732 high-volume user-days, 557 reused-text review rows, 7 short-but-high-vote reviews, 3,070 highly engaged reviews. |
| **Root Cause** | Likely a mix of genuinely prolific reviewers, cross-posted complaint/praise text, viral reviews with unusual visibility, and a smaller subset of potentially templated or coordinated behavior. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag suspicious reviewer behavior** with booleans such as `is_high_daily_reviewer`, `is_reused_text_pattern`, and `is_highly_engaged_review` | Preserves all records while making risky behavioral patterns explicit. |
| 2 | **Review repeated-text patterns manually** for top users before filtering | Reused wording may represent legitimate copy/paste complaints across related businesses, not just spam. |
| 3 | **Down-weight high-engagement outliers** in reviewer-quality or reputation modeling | Prevents a small set of viral reviews from dominating downstream features. |

##### ✅ Best Logical Solution: Flag first, investigate top offenders second.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |



---

---
#### 🟡 DQ-REVIEW-023 — Strong Rating Polarization & Non-Uniform Posting Cadence

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-023` |
| **Description** | The revised distribution analysis found **no non-boundary year with a >50% YoY decline**, which is a good sign after explicitly excluding the first and last dataset years from anomaly logic. However, the review table remains strongly **behaviorally non-uniform**: **61.5%** of reviews are either **1★ or 5★**, **67.0%** are positive (**4–5★**), **31.6%** arrive on weekends, and **29.84%** are posted between **00:00 and 05:59**. Monthly seasonality is present but moderate (seasonality index ranges from **0.912** to **1.124**). |
| **Detection Phase** | Phase 6.8 → **6.8.3) Distribution Anomaly Detection** |
| **Detected In** | Cell 93: `6.8.3) Distribution Anomaly Detection` |
| **Severity** | 🟡 **Informational / Moderate** — These are not data defects; they describe the behavioral shape of Yelp reviews. They matter because models or KPIs that assume balanced classes, uniform posting cadence, or smooth seasonality will be mis-specified. |
| **Affected Records** | Entire review table (**6,990,280** rows). |
| **Root Cause** | Typical review-platform behavior: users tend to post at emotional extremes, weekends generate more leisure/travel reviews, and overnight posting likely reflects asynchronous app usage plus timezone-normalized timestamps. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Keep explicit temporal features** such as hour, weekday/weekend, and month | These patterns carry real behavioral signal. |
| 2 | **Avoid assuming class balance** in rating models | The star distribution is clearly skewed toward 4★ and 5★ reviews. |
| 3 | **Treat first and last dataset years carefully** in trend analysis | Boundary years are structurally different and should be isolated from anomaly logic. |

##### ✅ Best Logical Solution: Retain these signals as modeled features rather than treating them as defects.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |



---

---
#### 🟡 DQ-REVIEW-024 — High Review Concentration Across Businesses

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-024` |
| **Description** | Review activity is materially concentrated across businesses. The revised business-level concentration analysis shows a **Gini coefficient of 0.6784** (high concentration). The top **1% of businesses (1,503)** account for **19.5%** of all reviews, and **27.9% of businesses (41,906)** account for **80%** of total review volume. At the sparse end, **12,901 businesses (8.6%)** have **5 or fewer reviews**, while the largest business (`_ab50qdWOk0DdB6XOrBitw`) has **7,673 reviews**. |
| **Detection Phase** | Phase 6.8 → **6.8.4) Concentration & Pareto Analysis** |
| **Detected In** | Cell 95: `6.8.4) Concentration & Pareto Analysis` |
| **Severity** | 🟡 **Moderate** — This is a platform-distribution characteristic, not a defect, but it can bias aggregates, evaluation sets, and models toward popular businesses. |
| **Affected Records** | Entire review table (**6,990,280** rows) and all **150,346** reviewed businesses. |
| **Root Cause** | Expected long-tail marketplace behavior: a relatively small share of highly visible businesses attracts a disproportionately large share of reviews. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Create activity tiers** for businesses | Makes reporting and segmentation more interpretable than raw counts alone. |
| 2 | **Use log-transformed review-count features** in modeling | Reduces the dominance of the right tail. |
| 3 | **Sample or stratify by activity band** when building evaluation/training datasets | Prevents popular businesses from overwhelming less-reviewed businesses. |

##### ✅ Best Logical Solution: Segment by activity tier and log-transform review-count features for modeling.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |



---

---
#### 🟡 DQ-REVIEW-025 — Dormant and High-Intensity Business Lifecycle Patterns

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-025` |
| **Description** | The revised lifecycle analysis uses the **dataset end date** rather than the current calendar date, which makes the signal much more meaningful. Only **3 businesses** have a true **zero-span** lifecycle (all reviews on one day), but **62,613 businesses (41.6%)** were already **dormant for more than 1 year before the dataset cutoff** on **2022-01-19**. On the opposite end, only **1 business** exceeded **50 reviews per active month**: `GXFMD0Z4jEVZBCsbPf4CTQ` with **6,160 reviews** over **3,448 active days** (**53.9 reviews/month**). |
| **Detection Phase** | Phase 6.8 → **6.8.5) Business Activity Span Analysis** |
| **Detected In** | Cell 97: `6.8.5) Business Activity Span Analysis` |
| **Severity** | 🟡 **Moderate** — Not erroneous, but highly relevant for lifecycle modeling, churn-style analysis, and any attempt to compare businesses without accounting for review recency and active span. |
| **Affected Records** | 3 zero-span businesses, 62,613 dormant-before-cutoff businesses, 1 very high-intensity business. |
| **Root Cause** | Natural business lifecycle heterogeneity: some businesses peaked early and went quiet long before snapshot end, while a tiny number sustained extremely dense review activity. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Flag lifecycle states** such as `is_zero_span`, `is_dormant_pre_cutoff`, and `is_high_intensity` | Makes lifecycle segmentation explicit and reusable downstream. |
| 2 | **Use dataset-relative recency** instead of today-based recency in historical EDA | Prevents stale snapshots from making every business look inactive. |
| 3 | **Create lifecycle segments** for downstream modeling/reporting | Separates flash, dormant, steady, and hyper-active businesses. |

##### ✅ Best Logical Solution: Use dataset-relative lifecycle flags and segment businesses before downstream modeling.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---
#### 🟡 DQ-REVIEW-026 — Historical Snapshot Staleness and Partial Terminal Year

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-026` |
| **Description** | The review dataset is a **historical snapshot**, not a current feed. The latest review date is **2022-01-19**, which is **1,732 days (~4.7 years)** older than today (2026-10-17). The snapshot spans **2005-02-16 → 2022-01-19** across **6,990,280 rows**, **18 calendar years**, and **6,181 days (~16.9 years)**, but the boundary years are incomplete: **2005** begins in **February** and **2022** contains **January only**. The latest three calendar years (**2020–2022**) contribute **1,204,411 rows (17.2%)**, so trend analyses near the right edge of the series are shaped by both COVID-era behavior and the January-2022 cutoff. |
| **Detection Phase** | Phase 6.9 — Timeliness Checks → **6.9.1) Snapshot Freshness & Coverage** |
| **Detected In** | Cell 101: `6.9.1) Snapshot Freshness & Coverage` |
| **Severity** | 🟡 **INFO** — critical temporal context, not a row-level defect |
| **Affected Scope** | Entire review table (**6,990,280** rows) |
| **Impact** | Any dashboard, model, or narrative framed as “current Yelp behavior” would be temporally misleading. Trend lines near 2020–2022 mix real-world pandemic effects with an extraction cutoff, while year-over-year analysis that includes 2022 will understate annual activity because that year is only one month long in the data. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Carry an explicit `data_as_of_date` / snapshot date in downstream layers** | Makes the dataset’s currency machine-readable and prevents consumers from mistaking a historical snapshot for a live feed. |
| 2 | **Filter temporal trend analysis to complete years (`2006–2021`)** | Removes the partial boundary years while preserving the clean multi-year core of the series for seasonality and YoY comparisons. |
| 3 | **Add a temporal caveat to every downstream artifact** | A clear note such as “Review data available through 2022-01-19 only” is the fastest safeguard against misinterpretation. |
| 4 | **Refresh the source snapshot if the goal is current-state analysis** | The only true fix for present-day relevance is newer source data. |

##### ✅ Best Logical Solution

Use **Solutions 1 + 2 + 3 immediately**: store an explicit snapshot date, restrict time-series comparisons to complete years, and label downstream outputs with the data cutoff. If the business question requires 2023–2026 behavior, then **Solution 4** becomes mandatory.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---

---

---
#### 🟡 DQ-REVIEW-027 — Table-Level History Exists, but Row-Level Ingestion Metadata Is Missing

| Attribute | Detail |
| --- | --- |
| **Error ID** | `DQ-REVIEW-027` |
| **Description** | The review table contains **no row-level load / ingestion timestamp columns** across 13 checked naming patterns (`load_timestamp`, `load_date`, `load_ts`, `etl_timestamp`, `ingestion_timestamp`, `ingested_at`, `created_at`, `_loaded_at`, `_ingestion_time`, `updated_at`, `modified_at`, `last_updated`, `_metadata_timestamp`). However, the Delta table itself *does* expose **table-level history**: **3 commit events** are present in `DESCRIBE HISTORY`, with table creation on **2026-09-22** and the latest commit on **2026-10-16**. This means batch-level observability exists, but **true per-row ingestion timing and source-to-target latency do not**. The gap between the **latest review date (2022-01-19)** and the **latest Delta commit (2026-10-16)** is **1,731 days**, confirming that this table behaves as a recently materialized historical snapshot rather than a near-real-time feed. |
| **Detection Phase** | Phase 6.9 — Timeliness Checks → **6.9.2) Ingestion Observability & Table History** and **6.9.3) Snapshot Lag & Boundary Period Risk** |
| **Detected In** | Cell 103: `6.9.2) Ingestion Observability & Table History` and Cell 104: `6.9.3) Snapshot Lag & Boundary Period Risk` |
| **Severity** | 🟡 **INFO** — observability / governance gap, not a row-level data defect |
| **Affected Scope** | Entire review table (**6,990,280** rows) |
| **Impact** | **(1) No per-record latency measurement:** event-time-to-ingestion-time cannot be computed. **(2) No row-level audit trail:** individual records cannot be tied to a precise ingestion moment. **(3) Snapshot ambiguity:** without an explicit dataset-as-of field, downstream users may confuse a historical snapshot with a live pipeline. **(4) Limited SLA governance:** table history helps at the batch level, but it cannot replace row-level ingestion observability. |

##### Suggested Solutions

| # | Solution | Justification |
| --- | --- | --- |
| 1 | **Add `_ingestion_timestamp = current_timestamp()` during the next downstream transformation** | Standard row-level observability pattern for measuring event-to-ingestion delay and supporting auditability. |
| 2 | **Add an explicit `data_as_of_date` / snapshot release field** | Distinguishes historical-snapshot freshness from pipeline-latency freshness and prevents semantic confusion downstream. |
| 3 | **Retain and monitor `DESCRIBE HISTORY` as a batch-level proxy** | Useful immediately with no schema change; it provides commit timing and operation history even though it is not row-granular. |
| 4 | **Capture pipeline run identifiers and batch timestamps in downstream layers** | Complements row-level ingestion metadata with operational lineage and easier root-cause analysis. |

##### ✅ Best Logical Solution

Use **Solutions 1 + 2** as the primary fix: add a row-level ingestion timestamp for observability and a dataset-as-of field for semantics. Keep **Solution 3** as an immediate interim control, and add **Solution 4** if this PoC evolves into a production pipeline.

##### 🔧 Chosen Cleaning Approach (Post Human Review)

| Attribute | Detail |
| --- | --- |
| **Chosen Solution** | _TBD — pending human review_ |
| **Implementation Layer** | Silver |
| **Reviewer** | — |
| **Review Date** | — |
| **Notes** | — |


---