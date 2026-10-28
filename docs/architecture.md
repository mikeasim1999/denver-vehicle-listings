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

## Shared Utilities

ETL notebooks call reusable functions from a separate [`databricks-shared-utilities`](https://github.com/mikeasim1999/databricks-shared-utilities) repo via `%run`. This keeps common logic (logging, schema enforcement, data quality checks) in one place across projects.

| Function | Used By | Purpose |
|----------|---------|---------|
| `ensure_logging_table_exists` | All three notebooks | Creates the `etl_run_log` table if it doesn't exist |
| `log_etl_run` | All three notebooks | Writes run metadata (row counts, status, timestamps) |
| `load_sql_queries` | Silver, Gold | Reads `.sql` files and splits on the incremental delimiter |
| `ensure_column_exists` | Silver, Gold | Adds missing columns to Delta tables |
| `ensure_table_schema` | Silver, Gold | Validates DataFrame schema against the target table |
| `data_quality_checks` | Silver, Gold | Runs metadata-driven DQ rules; CRITICAL failures halt the pipeline |

## Error Handling

Each notebook wraps its core ETL logic in `try/except/finally` blocks with a consistent pattern:

| Behavior | Detail |
|----------|--------|
| **Per-table isolation** | Silver and gold notebooks process each table independently — a failure in one table is caught and logged without halting the others |
| **Typed exception handling (Bronze)** | API fetch logic catches `HTTPError`, `Timeout`, and `RequestException` separately, logging the error type and page number while continuing to the next page |
| **Validation guards** | Pre-write checks raise `ValueError` for missing merge keys, missing SQL files, and empty API responses — failing fast before bad data reaches the target table |
| **ETL logging on failure** | The `finally` block ensures every run — success or failure — writes to the `etl_run_log` table with status, duration, and error message |
| **Graceful run ID fallback** | If the Job-provided `run_id` widget is missing (e.g. during manual notebook runs), each notebook generates a `manual_YYYYMMDD_HHMMSS` ID instead of failing |
| **Concurrent execution safety** | Silver and gold use `ThreadPoolExecutor` with `as_completed` — each future's result is caught individually so one table's failure doesn't cancel the thread pool |

Critical failures (logging table creation, all pages failing) re-raise to halt the Job and trigger Databricks email notifications.

## Data Quality

Data quality checks are metadata-driven — rules are stored in a `data_quality_checks` Delta table and evaluated against each DataFrame before it is written to the target table.

| Severity | Behavior |
|----------|----------|
| **CRITICAL** | Raises an exception and halts the pipeline |
| **WARNING** | Logged to `etl_run_log` but allows the pipeline to continue |

## Delta Lake Maintenance

A weekly VACUUM job reclaims storage from stale files left by `MERGE` operations. The notebook loops through all managed and external tables across configured schemas, measures directory size before and after `VACUUM`, and reports per-schema GB freed via `dbutils.notebook.exit()`. Databricks Job notifications deliver the summary by email.

## Power BI Connection

Power BI connects via the **Databricks SQL Connector** (Import mode) through a SQL warehouse endpoint, which provides a JDBC/ODBC interface to Unity Catalog without requiring a running cluster.

### Gold Views (Presentation Layer)

Rather than importing the gold tables directly, Power BI queries **views** that sit on top of the gold tables. These views serve a single purpose: aliasing column names from `snake_case` to `Proper Case` for display in the semantic model.

```sql
-- Example: analytics_lakehouse.gold.v_daily_listings
SELECT
  listing_id AS `Listing ID`,
  dealer_id AS `Dealer ID`,
  days_on_market AS `Days on Market`,
  list_price AS `List Price`,
  ...
FROM analytics_lakehouse.gold.daily_listings
```

This keeps the engineering convention (`snake_case`) in the Delta tables and the business-friendly naming (`Proper Case`) in Power BI, without duplicating data or adding transformation logic to the views.

### Date Table

The Date table is built in Power Query M with a dynamic range — start date derived from the earliest `first_listed` value and end date from the latest `retrieval_date`. This ensures the calendar always covers exactly the data's time span without manual updates. Columns include Year, Quarter, Month, Week, Day of Week, and a `Currently Listed` flag that distinguishes today's active listings from historical snapshots.

## CI/CD & Version Control

Notebooks are version-controlled in GitHub with a dev/main branch workflow. GitHub Actions runs Ruff linting and notebook structure validation on every push and pull request. Databricks Jobs pull from the `main` branch via Git provider, so only merged code runs in production. See [ci_cd.md](ci_cd.md) for full details.

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
| CI/CD            | GitHub Actions, Ruff                  |
| Version Control  | Databricks Repos (Git integration)    |
