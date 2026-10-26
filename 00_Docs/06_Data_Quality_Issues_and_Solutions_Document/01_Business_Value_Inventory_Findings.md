# Business Value Inventory — Findings

> **Source:** `01_Business_Cleaning` → *Value Inventory* cells (one cell per working DataFrame: `df_business`, `df_attributes`, `df_hours`, `df_categories`, 7 dict DataFrames, `df_quarantine_business`)
> **Data:** in-memory working split of `yelp_poc_uc.bronze_final.yelp_business` (150,346 businesses), before any cleaning
> **Date:** 2026-10-25
> **Purpose:** list every value problem visible in the inventory, the mapping it needs, and the DQ issue that owns the fix.

---

## 1. How to read this document

* **Rows** = number of rows in the working DataFrame with that value.
* **DQ issue** = the issue in `01_Business_DQ_Issues_and_Solutions.md` that owns the fix.
* **🆕** = not described (or not counted) in the DQ document yet; needs a decision.
* All counts were checked a second way (direct counts on the working DataFrames) and add up to the full row count of each DataFrame (150,346; `df_categories` 668,592).

---

## 2. Summary

| # | DataFrame | Problem | Rows | Mapping / fix needed | DQ issue |
| --- | --- | --- | ---: | --- | --- |
| 1 | `df_attributes` + 7 dicts | Text `'None'` used as NULL | 12,827 tokens (9,625 attr + 3,202 whole dicts) | `'None'` → SQL NULL | DQ-002 |
| 2 | `df_attributes` (24 yes/no) | Yes/no stored as text `'True'` / `'False'` | all non-NULL values | `'True'` → TRUE, `'False'` → FALSE | DQ-001 / 028 |
| 3 | `df_attributes` (7 enums) | Same value quoted two ways: `u'x'` and `'x'` | 183,307 quoted values | strip quotes → `x` | DQ-003 |
| 4 | `df_attributes.AgesAllowed` | Multi-word values written together | 129 | `allages` → `all_ages`, `21plus` → `21_plus`, `18plus` → `18_plus` | DQ-003 |
| 5 | `df_attributes` (4 enums) | Negative words as categories | 45,893 | `no` → `false`; `none` → NULL | DQ-063 |
| 6 | `df_attributes.RestaurantsPriceRange2` | Text `'1'`–`'4'` + `'None'` (34) | 85,314 | `'None'` → NULL, then INT | DQ-001 / 002 |
| 7 | `df_business.is_open`, `stars` | Storage types | 150,346 | BIGINT 0/1 → BOOLEAN; DOUBLE → DECIMAL(2,1) | DQ-001 |
| 8 | `df_business.address` | Empty string `''` (5,127) + `'.'` (1) | 5,128 | → NULL | DQ-032 / 019 |
| 9 | `df_business.postal_code` | Empty string `''` | 73 | → NULL (MS-01.5) | DQ-015 |
| 10 | `df_business.postal_code` | Malformed codes | 5 | `T6x 2H4` → `T6X 2H4`; `T6T 0G`, `T8A 5WA`, `336140` → NULL (row kept, no quarantine); `L3 5TB` left with the XMS business (MS-01.4). Done in MS-01.5, decision Ibrahim Mohamed 2026-10-26 | DQ-016 |
| 11 | `df_business.postal_code` | Canadian FSA only (`T5J`) | 196 | keep (valid partial value) (MS-01.5) | DQ-014 |
| 12 | `df_business.name` / `address` / `city` | Leading / trailing spaces | 150 / 3 / 49 | `trim()` | DQ-004 |
| 13 | `df_business.name` / `address` / `city` | Invisible chars (U+200B, U+00A0) | 15 / 10 / 4 | remove / replace with space | DQ-017 |
| 14 | `df_business.name` / `address` / `city` | Repeated spaces | 263 / 245 / 9 | collapse to one space | DQ-029 |
| 15 | `df_business.name` / `address` / `city` | Dangling `,` / `;` at the end | 1 / 6 / 17 | strip | DQ-029 / 018 |
| 16 | `df_business.city` | Polluted city (state, qualifiers, census names) | ~32 | rule-based + reviewed map | DQ-018 |
| 17 | `df_business.city` | One city in many spellings / casings | 154 groups | canonical city map (deferred) | DQ-021 / 030 |
| 18 | `df_business.city` | **Misspelled city names** | 28 | reviewed map | 🆕 DQ-021 (not listed) |
| 19 | `df_business.state` | `XMS` (UK) | 1 | quarantine | DQ-009 |
| 20 | `df_business.state` | Real states outside Yelp footprint | 20 | correct 6 / quarantine 14 | DQ-010 |
| 21 | `df_hours` | Same time written several ways (`-23:59`, `0:0-23:59`) | 959 values end `23:59` | canonical form | DQ-025 |
| 22 | `df_hours` | Same start and end (`6:0-6:0`), not `0:0-0:0` | 1,492 values | decide 24 h vs unknown | DQ-007 |
| 23 | `df_hours` | **Odd-minute times** (`0:1-0:0`, `6:29-23:0`, `14:30-21:25`) | 89 values / 21 businesses | keep; document | 🆕 |
| 24 | `df_hours` | Times not zero-padded (`9:0` instead of `09:00`) | 771,916 of 801,015 values | format as `HH:mm` | DQ-025 / DDL |
| 25 | `df_categories` | Fake categories from splitting on `, ` | 9,371 fragment rows | whitelist-protected split | DQ-013 |
| 26 | `df_categories` | Same category twice in one business | 43 pairs | dedupe, keep first position | DQ-005 |
| 27 | `df_categories` | **Businesses with no category at all** | 103 (95 open) | keep, no child rows | 🆕 |
| 28 | dict DataFrames | Empty dict `{}` | 81 | row kept, all keys NULL | DQ-006 |
| 29 | dict DataFrames | Dict keys quoted two ways (`u'k'` / `'k'`) | 12,255 dicts | already fixed in the reshape cell | DQ-028 / 008 |
| 30 | `df_dietary_restrictions` | `vegan = True`, `vegetarian = False` | 3 | keep (analytical caveat) | DQ-020 |
| 31 | `df_music` | `no_music` is never `True` | 0 TRUE / 6,858 FALSE | keep (dead flag) | DQ-041 |

---

## 3. Details per DataFrame

### 3.1 `df_business` (11 columns, 150,346 rows)

| Column | Unique values | Finding |
| --- | ---: | --- |
| `business_id` | 150,346 | Unique, no NULL. Not listed (key). |
| `name` | 114,117 | No empty / NULL. 150 with outer spaces, 263 with repeated spaces, 15 with invisible chars (e.g. `'The Bug \u200bWhisperer'`, `"Mamita's\xa0Hot Tamales"`), 1 trailing comma (`"Mom's Kitchen,"`). 1,381 contain non-ASCII (mostly valid accents — do **not** strip). |
| `address` | 122,844 | 5,127 empty `''` + 1 `'.'` (DQ-032 / 019). 6 end with a comma (`'8631 W Ardene St,'`, `'2810 Limekiln Pike,'`). 245 repeated spaces, 10 invisible chars, 3 outer spaces. |
| `city` | 1,416 | No empty. 154 groups of the same city with different case / spacing / punctuation (e.g. `Philadelphia` / `PHILADELPHIA` / `philadelphia` / `'Philadelphia '`; `Saint\xa0Louis`; `\u200bLargo`; `EdMonton`). `St.` / `St` / `Saint` variants (`St. Louis` 1,255 vs `Saint Louis` 4,827). Embedded state / qualifiers: `Tampa,Fl`, `Tucson, Arizona`, `Philadelphia PA`, `Nashville-Davidson metropolitan government (balance)`, `Tampa - North`. 590 cities appear only once. |
| `state` | 27 | 14 footprint states + 12 out-of-footprint codes (`TX` 4, `CO` 3, `HI` 2, `MA` 2, `WA` 2, `MI`, `MT`, `NC`, `SD`, `UT`, `VI`, `VT` 1 each) + `XMS` 1. |
| `postal_code` | 3,362 | 144,716 US 5-digit, 5,356 CA full, 196 CA FSA-only, 73 empty, 5 malformed: `T8A 5WA`, `T6x 2H4` (lower-case), `T6T 0G` (short), `336140` (6 digits), `L3 5TB` (UK, the `XMS` row). |
| `latitude` / `longitude` | 135,593 / 131,918 | Range 27.56…53.68 / −120.10…−73.20; no zeros, no NULL. Geographic checks are covered by DQ-011 / 027 / 040. |
| `stars` | 9 | 1.0 … 5.0 in 0.5 steps — clean. |
| `review_count` | 1,158 | 5 … 7,568 — clean (Yelp minimum is 5). |
| `is_open` | 2 | `1` 119,698 / `0` 30,648 — clean. |

**Misspelled cities (🆕, not named in DQ-021):**

| Value | State | Rows | Should be |
| --- | --- | ---: | --- |
| `Tuscon` | AZ | 18 | Tucson |
| `St. Loius` | MO | 3 | Saint Louis |
| `St Petersberg` / `St. Petersberg` / `St Petersurg` | FL | 3 | Saint Petersburg |
| `EdMonton` | AB | 2 | Edmonton (casing, DQ-030) |
| `ClearWater` | FL | 1 | Clearwater (casing, DQ-030) |
| `St Boise` | ID | 1 | Boise |

### 3.2 `df_attributes` (33 columns)

* **24 yes/no columns:** only `True`, `False`, `None` (text) and SQL NULL — no other spellings. `'None'` counts are small (2 … 3,948; largest `RestaurantsDelivery` 3,948, `RestaurantsTakeOut` 2,105, `OutdoorSeating` 1,882). `GoodForDancing`, `Open24Hours`, `RestaurantsCounterService` have no `'None'`.
* **`RestaurantsPriceRange2`:** `1` 28,840, `2` 48,581, `3` 6,667, `4` 1,192, `'None'` 34.
* **7 enum columns — value after removing the quotes:**

| Column | Values (rows: `u'x'` + `'x'`) | Extra mapping |
| --- | --- | --- |
| `AgesAllowed` | `allages` 77, `21plus` 46, `18plus` 6 (all `u'x'`) | split → `all_ages`, `21_plus`, `18_plus` |
| `Alcohol` | `none` 15,977 + 4,933, `full_bar` 12,968 + 3,024, `beer_and_wine` 4,880 + 1,369, `'None'` 38 | `none` → NULL (DQ-063) |
| `BYOBCorkage` | `no` 17 + 730, `yes_free` 18 + 572, `yes_corkage` 9 + 93, `'None'` 5 | `no` → `false` |
| `NoiseLevel` | `average` 21,581 + 4,607, `quiet` 6,460 + 1,174, `loud` 2,447 + 485, `very_loud` 1,067 + 133, `'None'` 39 | — |
| `RestaurantsAttire` | `casual` 23,190 + 15,154, `dressy` 530 + 273, `formal` 32 + 38, `'None'` 38 | — |
| `Smoking` | `no` 2,375 + 30, `outdoor` 1,800 + 17, `yes` 331 + 0, `'None'` 14 | `no` → `false` |
| `WiFi` | `free` 27,029 + 7,385, `no` 15,221 + 6,610, `paid` 486 + 133, `'None'` 50 | `no` → `false` |

* **14,629 businesses have all 32 attributes NULL** (block missingness, DQ-034 / 035 — row kept, no fix).

### 3.3 `df_hours` (8 columns)

* All 801,015 non-NULL day values match `H:M-H:M`; hours 0–23, minutes 0–59 — no impossible times.
* 73,908 values are `0:0-0:0`; 959 end with `23:59` (`0:0-23:59` 412, `7:0-23:59` 409) — DQ-025.
* 1,492 values have the same start and end (`6:0-6:0` 600, `5:0-5:0` 396, `9:0-9:0` 334) — DQ-007.
* 58,501 values end before they start (overnight / closes at midnight, e.g. `17:0-0:0`) — DQ-007 / 047.
* 🆕 **89 odd-minute values in 21 businesses** (not on a 5/15-minute grid and not a `:59` close): `0:1-0:0` 14, `6:29-23:0` 7, `0:0-23:55` 7, `14:30-21:25` 7, `9:0-19:40` 6. Plausible owner entries; recommend **keep as-is** and document.
* 23,223 businesses have all 7 days NULL (no hours at all); NULL days inside a listed week = DQ-033.
* Values are not zero-padded (`9:0`); the DDL target format `HH:mm` needs padding when parsed.

### 3.4 `df_categories` (3 columns, 668,592 rows)

* 1,311 category names; no empty, no outer spaces, no case variants, no non-ASCII — clean apart from the two issues below.
* **DQ-013 fragments:** `Beer` 2,413 + `Wine & Spirits` 2,413; `Books` / `Mags` / `Music & Video` 975 each; `Used` / `Vintage & Consignment` 753 each; `Wills` / `Trusts` / `& Probates` 38 each.
* **DQ-005:** 43 (business, category) pairs repeated.
* 🆕 **103 businesses (95 open) have no category** — `categories` is NULL / empty in bronze too (e.g. `A A Berlin Glass & Mirror Co`, `Luxury Perfume`). Recommend **keep** (0 child rows; `LEFT JOIN` handles it) and note it in the DQ document.
* 56 categories are used by only one business (information only).

### 3.5 The 7 dict DataFrames

| DataFrame | `raw_dict` NULL | `'None'` | `{}` | `u'k'` dicts | All keys `False` | Values found |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `df_ambience` | 106,067 | 551 | 4 | 4,713 | 15,198 | `True` / `False` / `None` |
| `df_best_nights` | 144,652 | 11 | 0 | 506 | 1,156 | `True` / `False` |
| `df_business_parking` | 59,261 | 2,271 | 8 | 4,314 | 23,964 | `True` / `False` / `None` (`valet` no `None`) |
| `df_dietary_restrictions` | 150,315 | 1 | 0 | 0 | 0 | `True` / `False` |
| `df_good_for_meal` | 121,259 | 303 | 0 | 1,816 | 9,105 | `True` / `False` / `None` |
| `df_hair_specializes_in` | 149,281 | 30 | 0 | 135 | 132 | `True` / `False` |
| `df_music` | 142,825 | 35 | 69 | 771 | 2,876 | `True` / `False` / `None` |

* Totals reconcile with the DQ document: `'None'` dicts 3,202 (DQ-002), `{}` 81 (DQ-006), `u'k'` dicts 12,255 (DQ-028).
* Key values are only `True` / `False` / `None` — no unexpected values; no dict text left with all keys NULL by mistake.
* `df_dietary_restrictions`: 3 rows `vegan = True` and `vegetarian = False` (DQ-020).
* `df_music`: `no_music` is never `True`; 2,876 dicts have every key `False` (DQ-041).

### 3.6 `df_quarantine_business`

* 0 rows — expected; it is filled by the DQ-009 / DQ-010 cells (15 businesses).

---

## 4. New items needing a decision (🆕)

| # | Finding | Rows | Suggested decision |
| --- | --- | ---: | --- |
| N1 | Misspelled city names (`Tuscon`, `St. Loius`, `St Petersberg`, `St Petersurg`, `St Boise`) | 25 (+3 casing) | Add to the DQ-021 reviewed city map (Silver Enriched), or to the DQ-018 map if fixed in Silver Cleaned |
| N2 | Odd-minute opening times | 89 values / 21 businesses | Keep as-is; document under DQ-012 / DQ-025 |
| N3 | Businesses with no category | 103 (95 open) | Keep; document as a valid "no category" case (no child rows) |

---

## 5. Verification

* Every overview table adds up to the full DataFrame row count.
* Counts that match the DQ document: `'None'` 12,827 / 3,202 dicts, `Alcohol none` 20,910, `{}` 81, `u'k'` 12,255, DQ-005 43, DQ-007 1,492, FSA 196, empty postal 73, empty address 5,127, vegan 3, DQ-041 2,876.
* No value was changed — the inventory cells are read-only.
