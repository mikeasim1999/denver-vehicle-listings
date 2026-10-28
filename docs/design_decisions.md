# Design Decisions

Technical choices made during development and the reasoning behind them.

## Why This Project Exists

After the 2025 tariff announcement, I wanted to see how vehicle prices in the Denver metro area would actually respond — whether list prices climbed, how long cars sat on lots, and whether certain makes or body types were hit harder than others. Rather than rely on national aggregates, I built a pipeline to collect local listing data daily so I could track these shifts over time in a way that answered my own questions.

That curiosity shaped the technical decisions below. Daily ingestion captures trends as they develop. Over 100 derived columns let me slice the data by price history, ownership, dealer type, and vehicle segment. The Power BI report at the end is the place where those questions actually get answered.

## Architecture & Pattern Choices

| Decision | Choice | Reasoning |
|----------|--------|-----------|
| **Lakehouse pattern** | Medallion (Bronze → Silver → Gold) | Recommended pattern in Databricks, and the data had a clear use for all three layers — raw ingestion landing zone, a heavy transformation step with 100+ derived columns, and a final analytics layer with deduplication and aggregation for Power BI |
| **SQL file separation** | External `.sql` files in `queries/` | Keeps transformation logic separate from notebook orchestration code. The silver query alone is hundreds of lines of CTEs — embedding that inline would make the notebook difficult to maintain and review |
| **Parallel table processing** | Python `ThreadPoolExecutor` | Already familiar with the package from prior work. Silver and gold layers process multiple independent tables, so running them concurrently cuts wall-clock time without adding orchestration complexity |
| **Incremental loading** | Delta Lake `MERGE` with parameterized full/incremental modes | Daily ingestion of the same listings means heavy duplication. `MERGE` handles upserts efficiently and avoids reprocessing the full dataset on every run. The `initLoad` parameter keeps the option open for backfills or resets. Because `MERGE` matches on a composite key and is parameterized, the pipeline is **idempotent** — re-running it for the same day produces the same result without duplicating data |
| **Schema evolution** | `mergeSchema` option on Delta writes | The source API can add or rename fields without notice. Allowing schema evolution prevents pipeline failures on upstream changes while preserving historical data |
| **Shared utilities repo** | Separate `databricks-shared-utilities` repo with `%run` references | Utility functions (logging, data quality checks, schema enforcement) are not specific to this project. A shared repo avoids copy-pasting them into every new project and keeps a single source of truth |
| **Git integration** | Databricks Repos with GitHub Actions CI | Notebooks are version-controlled in GitHub with Ruff linting on every push. Jobs pull from the `main` branch via Git provider, so only reviewed code runs in production |
| **Delta Lake maintenance** | Weekly `VACUUM` job with storage tracking | Delta Lake accumulates stale files from `MERGE` operations. A scheduled VACUUM notebook reclaims storage and reports per-schema results via job notifications |

## Tradeoffs Accepted

**No dev/staging/prod separation.** This is a single-workspace personal project. The dev/main branch workflow with Databricks Repos establishes the pattern, but there is no promotion pipeline between environments. In a production setting, Databricks Asset Bundles (DABs) would handle that.

**No partitioning.** The gold-layer tables are not large enough to benefit from partition pruning. Adding partitions on small tables can actually hurt performance by creating too many small files. `Z-ORDER` and `OPTIMIZE` are a better fit at this scale and can be added to the maintenance job if query patterns justify it.

**No streaming.** The source API returns batch data and updates once per day. A daily scheduled job is the right fit — streaming would add complexity without a faster data source to justify it.

**Workspace-scoped `%run` paths.** The `%run` calls to the shared utilities repo use workspace-specific paths (`/Repos/<user>/...`). In a team environment, these would use a shared or service-principal path instead.
