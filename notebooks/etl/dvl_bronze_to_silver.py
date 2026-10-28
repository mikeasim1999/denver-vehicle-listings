# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver Transformation
# MAGIC
# MAGIC Reads raw data from the bronze layer, applies SQL-based transformations
# MAGIC (loaded from external .sql files), and writes cleaned/enriched data
# MAGIC to the silver schema using MERGE (incremental) or OVERWRITE (full load).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import requests
import pandas as pd
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, LongType, BooleanType
)
import logging
from pyspark.sql import SparkSession
import traceback
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

logger = logging.getLogger(__name__)

dbutils.widgets.text("initLoad", "false", "Initial Load Flag")
dbutils.widgets.text("incrementalInterval", "3", "Incremental Interval (days)")
dbutils.widgets.text("run_id", "", "Job Run ID")

# Get Job run ID
try:
    run_id = dbutils.widgets.get('run_id')
    if not run_id:
        run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
except Exception:
    run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Get parameters
init_load = dbutils.widgets.get("initLoad")
if init_load.lower() in ["true", "t", "yes", "y", "1"]:
    init_load = True
else:
    init_load = False

if init_load:
    incremental_interval = 9999  # Effectively all data
else:
    incremental_interval = int(dbutils.widgets.get("incrementalInterval"))

logger.info(f"Job Run ID: {run_id}")
logger.info(f"Init Load Parameter: {init_load}")
logger.info(f"Load Type: {'Initial Load' if init_load else 'Incremental Load'}")

# Capture start time
start_time = datetime.now()
start_timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]
logger.info(f"Start Time: {start_timestamp}")

run_concurrent = False
max_workers = 4

CATALOG = 'analytics_lakehouse'
TARGET_SCHEMA = 'silver'
SOURCE_SCHEMA = 'bronze'
QUERY_PATH = '/Workspace/Denver_Vehicle_Listings/Queries/silver'

# COMMAND ----------

TABLES = [
    "denver_vehicle_listings"
]

KEYS = {
    "denver_vehicle_listings": ["transaction_listing_id"]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/ensure_logging_table_exists"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/log_etl_run"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/load_sql_queries"

# COMMAND ----------

sql_queries_df = load_sql_queries()

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/ensure_column_exists"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/ensure_table_schema"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/mike.asim1999@gmail.com/main/databricks-shared-utilities/functions/data_quality_checks"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main ETL Function

# COMMAND ----------

def main_etl(table_name):
    """
    Main ETL function that processes a single table from Bronze to Silver.

    Logic Flow:
    1. Determine load type (full vs incremental)
    2. Select appropriate SQL (before or after delimiter)
    3. Execute SQL and get data
    4. Write data (overwrite for full load, merge for incremental)
    5. Log results
    """
    result = {"table": table_name, "success": False, "skipped": False}
    start_time = datetime.now()
    duration = 0.0
    read_duration = 0.0
    write_duration = 0.0
    effective_mode = "unknown"
    was_forced_full_load = False
    num_source = num_target = num_inserted = num_updated = num_deleted = num_copied = 0

    target_table = f"{CATALOG}.{TARGET_SCHEMA}.{table_name}"
    table_exists = spark.catalog.tableExists(target_table)
    pk = KEYS.get(table_name, [])
    force_full_load = not pk

    try:
        if not table_exists:
            load_type = "CREATE"
        elif init_load:
            load_type = "OVERWRITE"
        elif force_full_load:
            load_type = "OVERWRITE"
            was_forced_full_load = True
            logger.info(f"{target_table} - No merge key provided, forcing full load")
        else:
            load_type = "MERGE"

        query_row = sql_queries_df.filter(F.col("table_name") == table_name).select("query").first()
        if not query_row:
            raise ValueError(f"SQL file not found for table: {table_name}")

        raw_sql = query_row[0]
        delimiter = "-- << INCREMENTAL >> --"

        # Select appropriate SQL based on load type
        if load_type in ["CREATE", "OVERWRITE"]:
            if delimiter in raw_sql:
                sql_query = raw_sql.split(delimiter)[0].strip()
                logger.info(f"Using FULL LOAD SQL (before delimiter)")
            else:
                sql_query = raw_sql.strip()
                logger.warning(f"No delimiter found - using entire file for FULL LOAD")
        else:
            sql_query = raw_sql.strip()
            logger.info(f"Using INCREMENTAL SQL (includes LEFT JOIN filter logic)")

        # Format SQL with parameters
        sql_query = sql_query.format(
            CATALOG=CATALOG,
            SOURCE_SCHEMA=SOURCE_SCHEMA,
            TARGET_SCHEMA=TARGET_SCHEMA,
            incrementalInterval=incremental_interval,
            initLoad="true" if init_load else "false",
        )

        # Execute SQL and read data
        logger.info(f"Executing SQL query")
        read_start = datetime.now()
        df = spark.sql(sql_query)

        read_duration = (datetime.now() - read_start).total_seconds()
        logger.info(f"Read completed in {read_duration:.2f}s")

        data_quality_checks(table_name, df)

        has_data = bool(df.take(1))
        if not has_data:
            logger.info(f"No source rows found for {table_name} - skipping write")
            target_row_count = spark.table(target_table).count() if table_exists else 0
            duration = (datetime.now() - start_time).total_seconds()
            result.update({
                "success": True, "skipped": True,
                "source_row_count": 0, "target_row_count": target_row_count,
                "num_inserted": 0, "num_updated": 0, "num_deleted": 0, "num_copied": 0,
                "duration": duration, "read_duration": read_duration,
                "write_duration": 0.0, "effective_mode": "skip",
            })
            return result

        # Write data to target table
        write_start = datetime.now()

        if load_type in ["CREATE", "OVERWRITE"]:
            logger.info(f"Writing data using {load_type} mode")
            num_source = df.count()
            df.write.mode("overwrite") \
                .option("overwriteSchema", "true") \
                .format("delta") \
                .saveAsTable(target_table)
            num_target = num_source
            num_inserted = num_source
            num_updated = num_deleted = num_copied = 0
            effective_mode = load_type.lower()
            logger.info(f"{load_type} completed with {num_source:,} rows")
        else:
            logger.info(f"Writing data using MERGE mode")
            source_missing_keys = [c for c in pk if c not in df.columns]
            if source_missing_keys:
                raise ValueError(f"Merge key(s) missing from source data for {table_name}: {source_missing_keys}")
            delta_table = DeltaTable.forName(spark, target_table)
            merge_condition = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk])

            delta_table.alias("target") \
                .merge(df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

            effective_mode = "merge"
            history_df = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
            merge_row = history_df.collect()

            if merge_row and merge_row[0].operationMetrics:
                m = merge_row[0].operationMetrics
                num_source = int(m.get("numSourceRows", 0))
                num_inserted = int(m.get("numTargetRowsInserted", 0))
                num_updated = int(m.get("numTargetRowsUpdated", 0))
                num_deleted = int(m.get("numTargetRowsDeleted", 0))
                num_copied = int(m.get("numTargetRowsCopied", 0))
                num_target = int(m.get("numOutputRows", num_source))
                logger.info(
                    f"Merge completed: Source={num_source:,}, "
                    f"Inserted={num_inserted:,}, Updated={num_updated:,}, "
                    f"Deleted={num_deleted:,}, Copied={num_copied:,}"
                )
            else:
                logger.warning("Could not extract merge metrics")
                num_source = df.count()
                num_target = spark.table(target_table).count()

        write_duration = (datetime.now() - write_start).total_seconds()
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Completed in {duration:.2f}s (Read: {read_duration:.2f}s, Write: {write_duration:.2f}s)")

        result.update({
            "success": True, "skipped": False,
            "source_row_count": num_source, "target_row_count": num_target,
            "num_inserted": num_inserted, "num_updated": num_updated,
            "num_deleted": num_deleted, "num_copied": num_copied,
            "duration": duration, "read_duration": read_duration,
            "write_duration": write_duration, "effective_mode": effective_mode,
        })

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        result["error"] = str(e)
        result["success"] = False
        effective_mode = "error"
        logger.error(f"Failed: {table_name}")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())

    finally:
        result.setdefault("duration", duration)
        result.setdefault("effective_mode", effective_mode)
        log_run(
            catalog=CATALOG,
            schema_name=TARGET_SCHEMA,
            run_context={
                "run_id": run_id,
                "table_name": table_name,
                "log_datetime": datetime.now(),
                "status": "SKIPPED" if result.get("skipped", False) else ("SUCCESS" if result["success"] else "FAILED"),
                "load_type": result.get("effective_mode", "unknown"),
                "error_message": result.get("error"),
                "duration_seconds": result.get("duration", 0.0),
                "duration_minutes": result.get("duration", 0.0) / 60.0,
                "read_duration_seconds": result.get("read_duration", 0.0),
                "write_duration_seconds": result.get("write_duration", 0.0),
                "source_row_count": result.get("source_row_count"),
                "target_row_count": result.get("target_row_count"),
                "inserted": result.get("num_inserted"),
                "updated": result.get("num_updated"),
                "deleted": result.get("num_deleted"),
                "copied": result.get("num_copied"),
                "incremental_interval": incremental_interval,
                "was_forced_full_load": was_forced_full_load,
            },
        )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute ETL

# COMMAND ----------

def execute_etl(run_concurrent, max_workers):
    """Orchestrates the ETL process for all configured tables."""
    job_start_time = datetime.now()
    results = []

    try:
        ensure_logging_table(CATALOG, TARGET_SCHEMA)
    except Exception as e:
        logger.error(f"Failed to create logging table - job cannot continue: {e}")
        raise

    if run_concurrent:
        logger.info(f"Running ETL concurrently with max_workers={max_workers}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(main_etl, t): t for t in TABLES}
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    res = future.result()
                    results.append(res)
                except Exception as e:
                    logger.error(f"{table_name} - CRITICAL FAILURE: {e}")
                    results.append({"table_name": table_name, "success": False, "error": str(e)})
    else:
        logger.info("Running ETL sequentially")
        for table_name in TABLES:
            try:
                res = main_etl(table_name)
                results.append(res)
            except Exception as e:
                logger.error(f"{table_name} - CRITICAL FAILURE: {e}")
                results.append({"table_name": table_name, "success": False, "error": str(e)})

    job_duration = (datetime.now() - job_start_time).total_seconds()
    total_tables = len(results)
    successful = sum(1 for r in results if r.get("success") and not r.get("skipped"))
    skipped = sum(1 for r in results if r.get("skipped"))
    failed = sum(1 for r in results if not r.get("success"))

    logger.info(f"Run ID: {run_id}")
    logger.info(f"Job Duration: {job_duration:.2f}s ({job_duration/60:.2f}min)")
    logger.info(f"Tables: {total_tables} | Success: {successful} | Skipped: {skipped} | Failed: {failed}")

    return results


table_results = execute_etl(run_concurrent, max_workers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary_rows = []
for r in table_results:
    table_name = r.get("table", "Unknown")
    success = r.get("success", False)
    skipped = r.get("skipped", False)

    if skipped:
        status = "Skipped"
    elif success:
        status = "Success"
    else:
        status = "Failed"

    if success and not skipped:
        row = {
            "Table": table_name, "Status": status,
            "Source": f"{r.get('source_row_count', 0):,}",
            "Target": f"{r.get('target_row_count', 0):,}",
            "Ins": f"{r.get('num_inserted', 0):,}",
            "Upd": f"{r.get('num_updated', 0):,}",
            "Del": f"{r.get('num_deleted', 0):,}",
            "Cpy": f"{r.get('num_copied', 0):,}",
            "Duration (s)": f"{r.get('duration', 0.0):.2f}",
            "Mode": r.get("effective_mode", "N/A"),
            "Error": "",
        }
    elif skipped:
        row = {
            "Table": table_name, "Status": status,
            "Source": "0", "Target": f"{r.get('target_row_count', 0):,}",
            "Ins": "0", "Upd": "0", "Del": "0", "Cpy": "0",
            "Duration (s)": f"{r.get('duration', 0.0):.2f}",
            "Mode": "skip", "Error": "",
        }
    else:
        row = {
            "Table": table_name, "Status": status,
            "Source": "-", "Target": "-",
            "Ins": "-", "Upd": "-", "Del": "-", "Cpy": "-",
            "Duration (s)": f"{r.get('duration', 0.0):.2f}",
            "Mode": r.get("effective_mode", "error"),
            "Error": r.get("error", "Unknown"),
        }
    summary_rows.append(row)

summary_df = pd.DataFrame(summary_rows)
display(summary_df)