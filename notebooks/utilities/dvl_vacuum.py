# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake VACUUM
# MAGIC Loops through all tables in the bronze, silver, and gold schemas
# MAGIC and runs VACUUM to remove stale data files no longer referenced
# MAGIC by the Delta transaction log. Default retention is 168 hours (7 days).
# MAGIC
# MAGIC **Schedule:** Weekly via a standalone Databricks Job.

# COMMAND ----------

CATALOG = "analytics_lakehouse"
SCHEMAS = ["bronze", "silver", "gold"]
RETENTION_HOURS = 168  # 7 days

# COMMAND ----------

from datetime import datetime

def get_storage_size_gb(table_name):
    """Returns the total storage size (including stale files) in GB."""
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    location = detail["location"]
    files = dbutils.fs.ls(location)
    total_bytes = sum(f.size for f in files if not f.name.startswith("_"))
    return total_bytes / (1024 ** 3)

results = []

for schema in SCHEMAS:
    tables = spark.catalog.listTables(f"{CATALOG}.{schema}")

    if not tables:
        print(f"No tables found in {CATALOG}.{schema}")
        continue

    for table in tables:
        if table.tableType != "MANAGED" and table.tableType != "EXTERNAL":
            continue

        full_table_name = f"{CATALOG}.{schema}.{table.name}"

        try:
            size_before = get_storage_size_gb(full_table_name)

            start = datetime.now()
            spark.sql(f"VACUUM {full_table_name} RETAIN {RETENTION_HOURS} HOURS")
            duration = (datetime.now() - start).total_seconds()

            size_after = get_storage_size_gb(full_table_name)
            freed_gb = size_before - size_after

            results.append({
                "schema": schema,
                "table": table.name,
                "full_name": full_table_name,
                "status": "SUCCESS",
                "duration_seconds": round(duration, 2),
                "size_before_gb": round(size_before, 4),
                "size_after_gb": round(size_after, 4),
                "freed_gb": round(freed_gb, 4)
            })
            print(f"VACUUMED  {full_table_name} ({duration:.2f}s) — freed {freed_gb:.4f} GB")

        except Exception as e:
            results.append({
                "schema": schema,
                "table": table.name,
                "full_name": full_table_name,
                "status": "FAILED",
                "duration_seconds": 0,
                "size_before_gb": 0,
                "size_after_gb": 0,
                "freed_gb": 0,
                "error": str(e)
            })
            print(f"FAILED    {full_table_name} — {str(e)}")

# COMMAND ----------

total_duration = sum(r["duration_seconds"] for r in results)
total_freed = sum(r["freed_gb"] for r in results)
succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
failed = sum(1 for r in results if r["status"] == "FAILED")

# COMMAND ----------

# Exit with summary
if failed:
    failed_tables = ", ".join([r["full_name"] for r in results if r["status"] == "FAILED"])
    raise Exception(f"VACUUM failed on {failed} table(s): {failed_tables}")

schema_summary = " | ".join(
    f"{schema}: {sum(1 for r in results if r['schema'] == schema and r['status'] == 'SUCCESS')}/{sum(1 for r in results if r['schema'] == schema)} tables, "
    f"{sum(r['freed_gb'] for r in results if r['schema'] == schema):.4f} GB freed"
    for schema in SCHEMAS
    if any(r["schema"] == schema for r in results)
)

dbutils.notebook.exit(
    f"VACUUM complete — {succeeded}/{len(results)} tables | {total_freed:.4f} GB freed | {round(total_duration, 2)}s\n"
    f"{schema_summary}"
)
