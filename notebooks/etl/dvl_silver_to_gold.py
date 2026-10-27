# Databricks notebook source

# MAGIC %md
# MAGIC ## Silver to Gold Transformation
# MAGIC
# MAGIC Reads cleaned data from the silver layer, applies SQL-based transformations
# MAGIC to build analytics-ready dimension and fact tables in the gold schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import pandas as pd
import requests
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
    incremental_interval = 9999
else:
    incremental_interval = int(dbutils.widgets.get("incrementalInterval"))

logger.info(f"Job Run ID: {run_id}")
logger.info(f"Init Load Parameter: {init_load}")
logger.info(f"Load Type: {'Initial Load' if init_load else 'Incremental Load'}")

start_time = datetime.now()
start_timestamp = spark.sql("SELECT current_timestamp()").collect()[0][0]
logger.info(f"Start Time: {start_timestamp}")

CATALOG = 'analytics_lakehouse'
TARGET_SCHEMA = 'gold'
SOURCE_SCHEMA = 'silver'
QUERY_PATH = '/Workspace/Denver_Vehicle_Listings/Queries/gold'

# COMMAND ----------

tables = {
    "daily_listings",
    "dealers",
    "vehicles",
}

keys = {
    "daily_listings": ["transaction_listing_id"],
    "vehicles": ["vehicle_id"],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def ensure_logging_table(catalog, schema_name):
    """Creates ETL run log table if it doesn't exist."""
    full_table_name = f"{catalog}.{schema_name}.etl_run_log"
    try:
        table_exists = spark.catalog.tableExists(full_table_name)
        if not table_exists:
            logger.info(f"Creating logging table: {full_table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    run_id STRING NOT NULL,
                    table_name STRING NOT NULL,
                    log_datetime TIMESTAMP NOT NULL,
                    status STRING NOT NULL,
                    load_type STRING,
                    error_message STRING,
                    duration_seconds DOUBLE,
                    duration_minutes DOUBLE,
                    read_duration_seconds DOUBLE,
                    write_duration_seconds DOUBLE,
                    source_row_count BIGINT,
                    target_row_count BIGINT,
                    inserted INT,
                    updated INT,
                    deleted INT,
                    copied INT,
                    incremental_interval INT,
                    was_forced_full_load BOOLEAN
                )
                USING delta
                COMMENT 'ETL run log for tracking pipeline execution'
            """)
            logger.info(f"Successfully created {full_table_name}")
    except Exception as e:
        logger.error(f"Failed to ensure logging table: {e}")
        raise


def log_run(catalog, schema_name, run_context):
    """Logs ETL run information to the logging table."""
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

        log_data = {
            "run_id": str(run_context.get("run_id", "unknown")),
            "table_name": str(run_context.get("table_name", "unknown")),
            "log_datetime": run_context.get("log_datetime", datetime.now()),
            "status": str(run_context.get("status", "UNKNOWN")),
            "load_type": str(run_context.get("load_type")) if run_context.get("load_type") else None,
            "error_message": str(run_context.get("error_message"))[:1000] if run_context.get("error_message") else None,
            "duration_seconds": float(run_context.get("duration_seconds", 0.0)),
            "duration_minutes": float(run_context.get("duration_minutes", 0.0)),
            "read_duration_seconds": float(run_context.get("read_duration_seconds", 0.0)),
            "write_duration_seconds": float(run_context.get("write_duration_seconds", 0.0)),
            "source_row_count": int(run_context.get("source_row_count", 0)),
            "target_row_count": int(run_context.get("target_row_count", 0)),
            "inserted": int(run_context.get("inserted", 0)),
            "updated": int(run_context.get("updated", 0)),
            "deleted": int(run_context.get("deleted", 0)),
            "copied": int(run_context.get("copied", 0)),
            "incremental_interval": int(run_context.get("incremental_interval", 0)) if run_context.get("incremental_interval") else None,
            "was_forced_full_load": bool(run_context.get("was_forced_full_load", False)),
        }

        log_df = spark.createDataFrame([log_data], schema=log_schema)
        log_df.write.format("delta").mode("append").saveAsTable(full_table_name)

        if log_data['status'] == 'SUCCESS':
            logger.info(
                f"Run logged: Source: {log_data['source_row_count']:,} | "
                f"Target: {log_data['target_row_count']:,} | "
                f"+{log_data['inserted']:,} | ~{log_data['updated']:,} | "
                f"-{log_data['deleted']:,} | {log_data['duration_minutes']:.2f}min"
            )
        elif log_data['status'] == 'SKIPPED':
            logger.info(f"Run logged: SKIPPED (no source data)")
        else:
            logger.error(
                f"Run logged: FAILED | "
                f"Error: {log_data['error_message'][:100] if log_data['error_message'] else 'Unknown'}"
            )
    except Exception as e:
        logger.error(f"Failed to log run to {full_table_name}: {e}")
        logger.error(traceback.format_exc())

# COMMAND ----------

def load_sql_queries():
    """Loads SQL query files from workspace path."""
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
            except Exception as e:
                logger.error(f"Failed to read {file_path}: {e}")
                continue

        if not queries:
            return None

        df = spark.createDataFrame(queries, ["table_name", "query"])
        return df

    except Exception as e:
        logger.error(f"Failed to load SQL files: {e}")
        raise


sql_queries_df = load_sql_queries()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main ETL Function

# COMMAND ----------

def main_etl(table_name):
    """
    Processes a single table from Silver to Gold.
    Supports full overwrite and incremental merge via SQL delimiter pattern.
    """
    result = {"table": table_name, "success": False, "skipped": False}
    start_time = datetime.now()
    duration = 0.0
    read_duration = 0.0
    write_duration = 0.0
    effective_mode = "unknown"
    num_source = num_target = num_inserted = num_updated = num_deleted = num_copied = 0

    target_table = f"{CATALOG}.{TARGET_SCHEMA}.{table_name}"
    effective_initLoad = init_load
    table_exists = spark.catalog.tableExists(target_table)

    try:
        query_row = sql_queries_df.filter(F.col("table_name") == table_name).collect()
        if not query_row:
            raise ValueError(f"SQL file not found for table: {table_name}")

        raw_sql = query_row[0]["query"]
        delimiter = "-- << INCREMENTAL >> --"

        if effective_initLoad:
            if delimiter in raw_sql:
                sql_query = raw_sql.split(delimiter)[0].strip()
                logger.info(f"Using FULL LOAD SQL (before delimiter)")
            else:
                sql_query = raw_sql.strip()
                logger.warning(f"No delimiter found - using entire file for FULL LOAD")
        else:
            sql_query = raw_sql.strip()
            logger.info(f"Using INCREMENTAL SQL (includes LEFT JOIN filter logic)")

        sql_query = sql_query.format(
            CATALOG=CATALOG,
            SOURCE_SCHEMA=SOURCE_SCHEMA,
            TARGET_SCHEMA=TARGET_SCHEMA,
            incrementalInterval=incremental_interval,
            initLoad="true" if effective_initLoad else "false",
        )

        logger.info(f"Executing SQL query")
        read_start = datetime.now()
        df = spark.sql(sql_query)
        has_data = bool(df.take(1))
        read_duration = (datetime.now() - read_start).total_seconds()
        logger.info(f"Read completed in {read_duration:.2f}s")

        if not has_data:
            logger.info(f"No source rows found -> skipping write")
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

        write_start = datetime.now()
        pk = keys.get(table_name, [])

        if effective_initLoad or not pk:
            logger.info(f"Writing data using OVERWRITE mode")
            df.write.mode("overwrite") \
                .option("overwriteSchema", "true") \
                .format("delta") \
                .saveAsTable(target_table)
            num_source = df.count()
            num_target = num_source
            num_inserted = num_source
            effective_mode = "overwrite"
            logger.info(f"Overwrote table with {num_source:,} rows")
        else:
            logger.info(f"Writing data using MERGE mode")
            if not table_exists:
                df.limit(0).write.format("delta").mode("overwrite").saveAsTable(target_table)
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
            else:
                num_source = df.count()
                num_target = spark.table(target_table).count()

        write_duration = (datetime.now() - write_start).total_seconds()
        duration = (datetime.now() - start_time).total_seconds()

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
        logger.error(f"Failed: {table_name} - {str(e)}")
        logger.error(traceback.format_exc())

    finally:
        result.setdefault("duration", duration)
        result.setdefault("effective_mode", effective_mode)
        log_run(
            catalog=CATALOG,
            schema_name=TARGET_SCHEMA,
            run_context={
                "run_id": run_id, "table_name": table_name,
                "log_datetime": start_time,
                "status": "SKIPPED" if result.get("skipped") else ("SUCCESS" if result["success"] else "FAILED"),
                "load_type": result.get("effective_mode", "unknown"),
                "error_message": result.get("error"),
                "duration_seconds": result.get("duration", 0.0),
                "duration_minutes": result.get("duration", 0.0) / 60.0,
                "read_duration_seconds": result.get("read_duration", 0.0),
                "write_duration_seconds": result.get("write_duration", 0.0),
                "source_row_count": result.get("source_row_count", 0),
                "target_row_count": result.get("target_row_count", 0),
                "inserted": result.get("num_inserted", 0),
                "updated": result.get("num_updated", 0),
                "deleted": result.get("num_deleted", 0),
                "copied": result.get("num_copied", 0),
                "incremental_interval": incremental_interval,
                "was_forced_full_load": True,
            },
        )

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute ETL

# COMMAND ----------

def execute_etl():
    """Orchestrates the ETL process for all configured tables."""
    job_start_time = datetime.now()
    results = []

    try:
        ensure_logging_table(CATALOG, TARGET_SCHEMA)
    except Exception as e:
        logger.error(f"Failed to create logging table: {e}")
        raise

    for idx, tbl in enumerate(tables, 1):
        try:
            res = main_etl(tbl)
            results.append(res)
        except Exception as e:
            logger.error(f"{tbl} - CRITICAL FAILURE: {e}")
            results.append({"table": tbl, "success": False, "error": str(e)})

    job_duration = (datetime.now() - job_start_time).total_seconds()
    successful = sum(1 for r in results if r.get("success") and not r.get("skipped"))
    skipped = sum(1 for r in results if r.get("skipped"))
    failed = sum(1 for r in results if not r.get("success"))

    logger.info(f"Run ID: {run_id}")
    logger.info(f"Job Duration: {job_duration:.2f}s ({job_duration/60:.2f}min)")
    logger.info(f"Tables: {len(results)} | Success: {successful} | Skipped: {skipped} | Failed: {failed}")

    return results


results = execute_etl()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary_rows = []
for r in results:
    table_name = r.get("table", "Unknown")
    success = r.get("success", False)
    skipped = r.get("skipped", False)

    status = "Skipped" if skipped else ("Success" if success else "Failed")

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
            "Mode": r.get("effective_mode", "N/A"), "Error": "",
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

failed_tables = [r.get("table") for r in results if not r.get("success")]
if failed_tables:
    logger.error(f"Job completed with failures: {', '.join(failed_tables)}")
else:
    logger.info("All tables processed successfully")
