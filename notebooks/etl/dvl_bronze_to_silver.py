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
except:
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

def ensure_logging_table(catalog, schema_name):
    """
    Creates ETL run log table if it doesn't exist with comprehensive metrics.
    """
    full_table_name = f"{catalog}.{schema_name}.etl_run_log"

    try:
        table_exists = spark.catalog.tableExists(full_table_name)

        if not table_exists:
            logger.info(f"Creating comprehensive logging table: {full_table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    run_id STRING NOT NULL COMMENT 'Unique identifier for the ETL run',
                    table_name STRING NOT NULL COMMENT 'Name of the table being processed',
                    log_datetime TIMESTAMP NOT NULL COMMENT 'Timestamp when the run started',
                    status STRING NOT NULL COMMENT 'Run status: SUCCESS, FAILED, or SKIPPED',
                    load_type STRING COMMENT 'Type of load: merge, overwrite, skip, or error',
                    error_message STRING COMMENT 'Error details if failed',
                    duration_seconds DOUBLE COMMENT 'Total execution time in seconds',
                    duration_minutes DOUBLE COMMENT 'Total execution time in minutes',
                    read_duration_seconds DOUBLE COMMENT 'Time spent reading source data',
                    write_duration_seconds DOUBLE COMMENT 'Time spent writing to target',
                    source_row_count BIGINT COMMENT 'Number of rows in source query',
                    target_row_count BIGINT COMMENT 'Number of rows in target after operation',
                    inserted INT COMMENT 'Number of rows inserted',
                    updated INT COMMENT 'Number of rows updated',
                    deleted INT COMMENT 'Number of rows deleted',
                    copied INT COMMENT 'Number of rows copied (unchanged in merge)',
                    incremental_interval INT COMMENT 'Incremental interval in days (if applicable)',
                    was_forced_full_load BOOLEAN COMMENT 'Whether this was forced to full load due to missing table'
                )
                USING delta
                COMMENT 'Comprehensive ETL run log for tracking pipeline execution and performance'
            """)
            logger.info(f"Successfully created {full_table_name}")
        else:
            logger.info(f"Logging table already exists: {full_table_name}")

    except Exception as e:
        logger.error(f"Failed to ensure logging table: {e}")
        raise


def log_run(catalog, schema_name, run_context):
    """
    Logs comprehensive ETL run information to the logging table.
    """
    full_table_name = f"{catalog}.{schema_name}.etl_run_log"

    try:
        log_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("log_datetime", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("load_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("duration_minutes", DoubleType(), True),
            StructField("read_duration_seconds", DoubleType(), True),
            StructField("write_duration_seconds", DoubleType(), True),
            StructField("source_row_count", LongType(), True),
            StructField("target_row_count", LongType(), True),
            StructField("inserted", IntegerType(), True),
            StructField("updated", IntegerType(), True),
            StructField("deleted", IntegerType(), True),
            StructField("copied", IntegerType(), True),
            StructField("incremental_interval", IntegerType(), True),
            StructField("was_forced_full_load", BooleanType(), True),
        ])

        def to_int(value):
            return int(value) if value is not None else None

        def to_float(value):
            return float(value) if value is not None else None

        log_data = {
            "run_id": str(run_context.get("run_id", "unknown")),
            "table_name": str(run_context.get("table_name", "unknown")),
            "log_datetime": run_context.get("log_datetime", datetime.now()),
            "status": str(run_context.get("status", "UNKNOWN")),
            "load_type": str(run_context.get("load_type")) if run_context.get("load_type") else None,
            "error_message": str(run_context.get("error_message"))[:1000] if run_context.get("error_message") else None,
            "duration_seconds": to_float(run_context.get("duration_seconds")),
            "duration_minutes": to_float(run_context.get("duration_minutes")),
            "read_duration_seconds": to_float(run_context.get("read_duration_seconds")),
            "write_duration_seconds": to_float(run_context.get("write_duration_seconds")),
            "source_row_count": to_int(run_context.get("source_row_count")),
            "target_row_count": to_int(run_context.get("target_row_count")),
            "inserted": to_int(run_context.get("inserted")),
            "updated": to_int(run_context.get("updated")),
            "deleted": to_int(run_context.get("deleted")),
            "copied": to_int(run_context.get("copied")),
            "incremental_interval": to_int(run_context.get("incremental_interval")),
            "was_forced_full_load": bool(run_context.get("was_forced_full_load", False)),
        }

        log_df = spark.createDataFrame([log_data], schema=log_schema)
        log_df.write.format("delta").mode("append").saveAsTable(full_table_name)

        if log_data["status"] == "SUCCESS":
            duration_text = (
                f"{log_data['duration_minutes']:.2f}min"
                if log_data["duration_minutes"] is not None
                else "duration unknown"
            )
            timing_parts = []
            if log_data["read_duration_seconds"] is not None:
                timing_parts.append(f"R:{log_data['read_duration_seconds']:.1f}s")
            if log_data["write_duration_seconds"] is not None:
                timing_parts.append(f"W:{log_data['write_duration_seconds']:.1f}s")
            timing_text = f" ({' + '.join(timing_parts)})" if timing_parts else ""

            logger.info(
                f"Run logged successfully. "
                f"Source: {log_data['source_row_count'] if log_data['source_row_count'] is not None else 'N/A'} | "
                f"Target: {log_data['target_row_count'] if log_data['target_row_count'] is not None else 'N/A'} | "
                f"Inserted: {log_data['inserted'] if log_data['inserted'] is not None else 'N/A'} | "
                f"Updated: {log_data['updated'] if log_data['updated'] is not None else 'N/A'} | "
                f"Deleted: {log_data['deleted'] if log_data['deleted'] is not None else 'N/A'} | "
                f"{duration_text}{timing_text}"
            )
        elif log_data["status"] == "SKIPPED":
            logger.info("Run logged as skipped.")
        else:
            logger.error(
                f"Run logged as failed. "
                f"Error: {log_data['error_message'][:100] if log_data['error_message'] else 'Unknown'}"
            )

    except Exception as e:
        logger.error(f"Failed to log run to {full_table_name}: {e}")
        logger.error(traceback.format_exc())

# COMMAND ----------

def load_sql_queries():
    """
    Loads SQL query files from workspace path.
    Returns a Spark DataFrame with columns: table_name, query
    """
    try:
        files = dbutils.fs.ls(QUERY_PATH)
        sql_files = [file.path for file in files if file.path.endswith('.sql')]

        if not sql_files:
            logger.warning(f"No SQL files found in {QUERY_PATH}")
            return None

        logger.info(f"Found {len(sql_files)} SQL files in {QUERY_PATH}")

        queries = []
        for file_path in sql_files:
            try:
                table_name = file_path.split('/')[-1].replace('.sql', '')
                local_path = file_path.replace('dbfs:', '')

                with open(local_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                if content.strip():
                    queries.append((table_name, content.strip()))
                    logger.info(f"Loaded query: {table_name} ({len(content)} chars)")
                else:
                    logger.warning(f"Skipped empty file: {table_name}")
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                continue

        if not queries:
            return None

        df = spark.createDataFrame(queries, ["table_name", "query"])
        logger.info(f"Successfully loaded {df.count()} SQL queries")
        return df

    except Exception as e:
        logger.error(f"Failed to load SQL files: {e}")
        logger.error(traceback.format_exc())
        raise


sql_queries_df = load_sql_queries()

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
