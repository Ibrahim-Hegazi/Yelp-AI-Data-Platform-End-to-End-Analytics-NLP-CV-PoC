# 02 — Normalized (Silver) Data Models

This folder contains the **Conceptual**, **Logical**, and **Physical** data models
for the Yelp datasets after normalization, data cleaning, and constraint
application at the Silver layer.

## Datasets Covered

- `yelp_business`
- `yelp_checkin`
- `yelp_review`
- `yelp_tip`
- `yelp_user`

## What Changed From Bronze Final to Silver

Unlike the original Bronze Final models (see `01_Original_Data_Models/`), the
models in this folder reflect data that has been:

- **Normalized** — tables restructured to reduce redundancy and establish clear
  relationships (e.g. extracting nested structs into separate dimension tables)
- **Cleaned** — data type corrections (dates cast from strings to proper
  timestamps, booleans resolved, nested JSON parsed), null handling, and
  deduplication
- **Constrained** — primary keys, foreign keys, NOT NULL constraints, and
  value-range validations applied to enforce data integrity

## Why Normalization Was Applied

This is a **Proof of Concept (PoC)** project designed for onboarding **Data
Engineer interns** and **AI Engineer interns**. The senior team lead requested
normalization specifically to:

- Make the table structures **easier to read and understand** for interns who
  are new to the codebase
- Provide clear, well-defined relationships between entities rather than wide,
  denormalized tables with nested fields
- Serve as a teaching reference for proper relational data modelling practices

## Prerequisites — Read These First

To fully understand the models in this folder, the reader should first consult
the following documents in `00_Docs/`:

| Document | Why |
| --- | --- |
| **Data Dictionary v3** | Defines every column — name, description, allowed values, and business meaning after Silver transformations |
| **Schema Specification v3** | Documents the exact Spark/Delta types, nullability, constraints, and table relationships at the Silver layer |

These two documents provide the column-level detail and constraint definitions
that the models in this folder visualise at the conceptual, logical, and
physical levels.

## Folder Contents

| Model | Description |
| --- | --- |
| Conceptual Model | High-level entity-relationship view — normalized entities and their associations |
| Logical Model | Attribute-level detail — column names, logical types, nullability, primary/foreign keys, and constraints |
| Physical Model | Implementation-specific — Spark/Delta types, partitioning, table properties, and struct layouts after cleaning |
