# 01 — Original Data Models

This folder contains the **Conceptual**, **Logical**, and **Physical** data models
for the five Yelp datasets in their original (Bronze Final) form.

## Datasets Covered

- `yelp_business`
- `yelp_checkin`
- `yelp_review`
- `yelp_tip`
- `yelp_user`

## When These Models Were Created

Each model was produced **after the Basic Data Profiling notebooks**
(`04_Bronze_Data_Profiling_and_EDA/4.1_Basic_Data_Profiling`), using profiling
results to inform entity definitions, relationships, and column-level details.

## How Data Types Were Inferred

The data types documented in these models reflect the **Spark JSON schema
inference** process used during the Bronze pipeline, not manual type assignments.
The inference follows a two-stage chain:

### Stage 1 — Spark JSON Schema Inference (`01_Landing_to_Staging_Schema_Validator`)

The raw JSON files in the landing zone volume are read with:

```python
inferred_schema = spark.read.json(file_path).schema
```

Spark scans the entire file and infers types by examining every value across
all rows. The inference rules are:

| JSON Value Pattern          | Inferred Spark Type |
| --------------------------- | ------------------- |
| Integer numbers (no decimal)| `LongType`          |
| Decimal numbers             | `DoubleType`        |
| Strings                     | `StringType`        |
| Nested `{}` objects         | `StructType`        |
| Arrays `[]`                 | `ArrayType`         |
| `true` / `false`            | `BooleanType`       |

The inferred schema is then used with `from_json("value", inferred_schema)` to
parse each raw JSON line. The parsed columns flow into the `_clean` staging
tables in `yelp_poc_uc.bronze_staging`.

### Stage 2 — Pass-Through Promotion (`02_Staging_Quality_Gate_and_Promotion`)

Clean staging tables are promoted to `yelp_poc_uc.bronze_final` via a straight
CTAS:

```sql
CREATE OR REPLACE TABLE bronze_final.{table} AS SELECT * FROM bronze_staging.{table}_clean
```

This inherits the schema exactly from staging — **no type changes occur** at
this stage.

### Known Type Limitations at Bronze Final

Because no explicit casting is applied, the Bronze Final schemas carry several
inference artifacts that the Silver layer should address:

- **Dates are strings** — `date`, `yelping_since` remain `StringType` (Spark's
  JSON reader does not auto-parse date strings without an explicit
  `timestampFormat` option)
- **Nested JSON stored as strings** — fields like `BusinessParking`, `Ambience`,
  and `GoodForMeal` inside the `attributes` struct are `StringType` because the
  Yelp source encodes them as string-serialised dicts (e.g. `"{'garage': False}"`)
- **Booleans as strings** — fields such as `BikeParking` and `GoodForKids` are
  `StringType` (source uses `"True"` / `"False"` instead of JSON `true` / `false`)
- **`is_open` is `LongType`** — semantically boolean (0/1) but inferred as
  integer because JSON integers always map to `LongType`

## Folder Contents

| Model              | Description                                                                 |
| ------------------ | --------------------------------------------------------------------------- |
| Conceptual Model   | High-level entity-relationship view — entities and their associations       |
| Logical Model      | Attribute-level detail — column names, logical types, nullability, keys     |
| Physical Model     | Implementation-specific — Spark/Delta types, partitioning, struct layouts   |
