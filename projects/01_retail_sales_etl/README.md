# Project 1: Retail sales ETL pipeline

## Goal
Build a three-layer batch pipeline (bronze, silver, gold) that takes raw
retail order data and turns it into business-ready aggregates, using only
PySpark and Delta Lake. No orchestration tools yet — that comes later in
the capstone project.

## Dataset
Primary: Databricks built-in sample dataset at
`/databricks-datasets/retail-org/sales_orders/` (available in every
workspace, including Free Edition — no upload needed).

Fallback: Olist Brazilian E-Commerce dataset (Kaggle), uploaded manually
to a Unity Catalog volume or DBFS if the built-in path is unavailable.

## Architecture

| Layer  | Notebook               | Reads from | Writes to             | Responsibility |
|--------|------------------------|------------|------------------------|----------------|
| Bronze | `01_ingest_bronze`     | Raw source | `bronze.sales_raw`     | Land raw data as-is, enforce schema, add ingestion metadata. No business logic. |
| Silver | `02_transform_silver`  | Bronze     | `silver.sales_clean`   | Remove nulls/duplicates, cast types, standardize. Trusted source of truth. |
| Gold   | `03_aggregate_gold`    | Silver     | `gold.sales_by_region` | Business aggregates (revenue, order counts by region/category). |

## Design principles
- **Bronze** should always answer "what did we actually receive, and when."
  No filtering, no deduplication — pure landing zone.
- **Silver** is what analysts and downstream pipelines should trust as
  valid, deduplicated data.
- **Gold** is small, fast, and dashboard-ready — pre-aggregated for
  direct consumption.

## Decisions left to the implementer
- Schema definition strategy (explicit `StructType` vs inferred)
- Write mode per layer (`append` vs `overwrite`) and why
- Null and duplicate handling rules at the silver layer
- Which columns to keep, drop, or rename
- Which aggregations matter for the gold layer

## Out of scope for this project
- Streaming ingestion (see Project 2)
- Unity Catalog governance and grants (see Project 4)
- Performance tuning under load (see Project 3)
- Job scheduling / orchestration (see capstone project)

## Success criteria
- [ ] Three independently runnable notebooks, each writing to its own
      Delta table
- [ ] `DESCRIBE HISTORY` shows a clean, understandable version log per table
- [ ] This README reflects the actual design decisions made, with brief
      reasoning for each