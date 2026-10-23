# Architecture

## Medallion Lakehouse Architecture

This project follows the **medallion architecture** pattern within Databricks, organizing data into three progressively refined layers stored as Delta Lake tables under Unity Catalog.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Databricks Jobs                              │
│                      (Orchestrator / Scheduler)                     │
└──────┬──────────────────────┬───────────────────────┬───────────────┘
       │                      │                       │
       ▼                      ▼                       ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐
│    Bronze     │    │      Silver      │    │       Gold       │
│  Raw Ingest   │───▶│  Cleaned &       │───▶│  Analytics-Ready │
│              │    │  Enriched        │    │                  │
│ • API fetch  │    │ • Type casting   │    │ • daily_listings │
│ • JSON flat  │    │ • Null handling  │    │ • dealers        │
│ • Dedup      │    │ • Derived cols   │    │ • vehicles       │
│ • Delta MERGE│    │ • Complex parse  │    │ • Dedup / Agg    │
└──────────────┘    └──────────────────┘    └──────────────────┘
                                                     │
                                                     ▼
                                            ┌──────────────────┐
                                            │     Power BI     │
                                            │   Reporting &    │
                                            │   Dashboards     │
                                            └──────────────────┘
```

## Layer Details

### Bronze — Raw Ingestion
**Table:** `analytics_lakehouse.bronze.denver_vehicle_listings`

- Fetches vehicle listing data from a third-party vehicle listings API
- Paginates through results (up to 50 pages per run)
- Flattens nested JSON responses using `pd.json_normalize`
- Adds metadata columns: `retrieval_date`, `unique_id` (composite key)
- Writes to Delta Lake via `MERGE` (incremental) or `OVERWRITE` (initial load)

### Silver — Cleaned & Enriched
**Table:** `analytics_lakehouse.silver.denver_vehicle_listings`

- Applies comprehensive SQL transformations via external `.sql` files
- Type casting, null handling, and standardization across 100+ columns
- Parses complex nested arrays: `priceHistory`, `ownerHistory`, `serviceHistory`, `accidentHistory`
- Derives new columns: `vehicle_id` (composite), `days_on_market`, `value_variance`, `dealer_brand`
- Fills missing values using group-level averages (dealer ratings, MPG)
- Supports both full-load and incremental modes via SQL delimiter pattern

### Gold — Analytics-Ready
**Tables:**
- `analytics_lakehouse.gold.daily_listings` — One row per listing per retrieval date (full transaction history)
- `analytics_lakehouse.gold.dealers` — Deduplicated dealer dimension (latest record per dealer)
- `analytics_lakehouse.gold.vehicles` — Deduplicated vehicle dimension (latest record per vehicle)

## Incremental Load Strategy

The pipeline supports two execution modes controlled by notebook parameters:

| Parameter             | Description                                      |
|-----------------------|--------------------------------------------------|
| `initLoad`            | `true` = full overwrite; `false` = incremental   |
| `incrementalInterval` | Number of days to look back for changed records   |
| `run_id`              | Unique identifier for ETL logging                 |

**SQL Delimiter Pattern:** Each `.sql` file uses a `-- << INCREMENTAL >> --` delimiter. The full-load query is everything above the delimiter. The incremental query is the entire file, which appends a `LEFT JOIN` filter to exclude already-processed records.

## ETL Logging

Each layer writes to an `etl_run_log` Delta table tracking:
- Run ID, table name, schema, start/end timestamps
- Row counts (inserted, updated, source, target)
- Execution status (success/failure)

## Tech Stack

| Component        | Technology                            |
|------------------|---------------------------------------|
| Compute          | Databricks (PySpark + SQL)            |
| Storage          | Delta Lake                            |
| Catalog          | Unity Catalog                         |
| Orchestration    | Databricks Jobs                       |
| Notebooks        | Python (pandas, requests, PySpark)    |
| Transformations  | Spark SQL (external `.sql` files)     |
| Visualization    | Power BI                              |
