# Databricks notebook source

# MAGIC %md
# MAGIC ### Denver Vehicle Listing Bronze Load
# MAGIC
# MAGIC #### Process
# MAGIC - Get data from a third-party vehicle listings API
# MAGIC - Perform light transformations such as assigning data types and creating a unique id (primary key)
# MAGIC - Load data into Lakehouse and implement schema change handling

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

# COMMAND ----------

logger = logging.getLogger(__name__)

dbutils.widgets.text("initLoad", "false", "Initial Load Flag")

# Get Job run ID
try:
    run_id = dbutils.widgets.get('run_id')
    if not run_id:
        run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
except:
    run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Get init_load parameter
init_load = dbutils.widgets.get("initLoad")
if init_load.lower() in ["true", "t", "yes", "y", "1"]:
    init_load = True
else:
    init_load = False

logger.info(f"Job Run ID: {run_id}")
logger.info(f"Init Load Parameter: {init_load}")

TARGET_CATALOG = 'analytics_lakehouse'
TARGET_SCHEMA = 'bronze'

logger.info(f"Load Type: {'Initial Load' if init_load else 'Incremental Load'}")

# COMMAND ----------

TABLES = [
    "denver_vehicle_listings"
]

KEYS = {
    "denver_vehicle_listings": "unique_id"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def ensure_logging_table(catalog, schema_name):
    """
    Creates ETL run log table if it doesn't exist with comprehensive metrics.

    Args:
        catalog: Catalog name
        schema_name: Schema name for the log table
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

                    -- Timing metrics
                    duration_seconds DOUBLE COMMENT 'Total execution time in seconds',
                    duration_minutes DOUBLE COMMENT 'Total execution time in minutes',
                    read_duration_seconds DOUBLE COMMENT 'Time spent reading source data',
                    write_duration_seconds DOUBLE COMMENT 'Time spent writing to target',

                    -- Row count metrics
                    source_row_count BIGINT COMMENT 'Number of rows in source query',
                    target_row_count BIGINT COMMENT 'Number of rows in target after operation',
                    inserted INT COMMENT 'Number of rows inserted',
                    updated INT COMMENT 'Number of rows updated',
                    deleted INT COMMENT 'Number of rows deleted',
                    copied INT COMMENT 'Number of rows copied (unchanged in merge)',

                    -- Configuration
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

        log_data = {
            "run_id": str(run_context.get("run_id", "unknown")),
            "table_name": str(run_context.get("table_name", "unknown")),
            "log_datetime": run_context.get("log_datetime", datetime.now()),
            "status": str(run_context.get("status", "UNKNOWN")),
            "load_type": str(run_context.get("load_type")) if run_context.get("load_type") else None,
            "error_message": str(run_context.get("error_message"))[:1000] if run_context.get("error_message") else None,
            # Timing
            "duration_seconds": float(run_context.get("duration_seconds")) if run_context.get("duration_seconds") is not None else None,
            "duration_minutes": float(run_context.get("duration_minutes")) if run_context.get("duration_minutes") is not None else None,
            "read_duration_seconds": None,
            "write_duration_seconds": None,
            # Counts
            "source_row_count": int(run_context.get("source_row_count", 0)),
            "target_row_count": int(run_context.get("target_row_count", 0)),
            "inserted": int(run_context.get("inserted", 0)),
            "updated": int(run_context.get("updated", 0)),
            "deleted": int(run_context.get("deleted", 0)),
            "copied": int(run_context.get("copied", 0)),
            # Config
            "incremental_interval": int(run_context.get("incremental_interval", 0)) if run_context.get("incremental_interval") else None,
            "was_forced_full_load": bool(run_context.get("was_forced_full_load", False)),
        }

        log_df = spark.createDataFrame([log_data], schema=log_schema)
        log_df.write.format("delta").mode("append").saveAsTable(full_table_name)

        # Enhanced logging
        if log_data['status'] == 'SUCCESS':
            logger.info(
                f"Run logged was Successful. Quick stats: "
                f"Source: {log_data['source_row_count']:,} | "
                f"Target: {log_data['target_row_count']:,} | "
                f"{log_data['inserted']:,} | "
                f"{log_data['updated']:,} | "
                f"{log_data['deleted']:,} | "
                f"{log_data['duration_minutes']:.2f}min "
            )
        elif log_data['status'] == 'SKIPPED':
            logger.info(f"Run logged was Skipped. (no source data)")
        else:
            logger.error(
                f"Run logged was Failed.  "
                f"Error: {log_data['error_message'][:100] if log_data['error_message'] else 'Unknown'}"
            )

    except Exception as e:
        logger.error(f"Failed to log run to {full_table_name}: {e}")
        logger.error(traceback.format_exc())

# COMMAND ----------

def ensure_schema(dataframe):
    """Convert void columns to string type"""
    for name, dtype in dataframe.dtypes:
        if dtype.lower() == "void":
            logger.info(f"Converting void column to string: {name}")
            dataframe = dataframe.withColumn(name, F.lit(None).cast(StringType()))
    return dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## API Call

# COMMAND ----------

def fetch_data():
    """
    Fetches vehicle listing data from a third-party vehicle listings API.
    Paginates through results, deduplicates, and returns a Pandas DataFrame.
    """
    pages_attempted = 0
    pages_succeeded = 0
    pages_failed = 0
    raw_listings_retrieved = 0

    all_data = []
    url = "<VEHICLE_LISTINGS_API_ENDPOINT>"

    headers = {
        "User-Agent": "insomnia/9.2.0"
    }

    for page in range(1, 51):
        params = {
            "zip": "80205",
            "radius": "250",
            "sort": "BEST",
            "vehicleCondition": "USED",
            "page": str(page),
            "dynamicRadius": "false",
            "fetchImageLimit": "6",
        }
        pages_attempted += 1
        try:
            response = requests.get(url, headers=headers, params=params, timeout=120)
            response.raise_for_status()

            data = response.json()
            pages_succeeded += 1

            listings = data.get("listings", [])
            raw_listings_retrieved += len(listings)
            all_data.extend(listings)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error on page {page}. HTTP Error: {e}")
            pages_failed += 1
        except requests.exceptions.Timeout:
            logger.error(f"Error on page {page}. Request Timeout")
            pages_failed += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error on page {page}. Request Exception: {e}")
            pages_failed += 1
        except Exception as e:
            logger.error(f"Error on page {page}. Unknown Error: {e}")
            pages_failed += 1

    df = pd.json_normalize(all_data)
    if df.empty:
        logger.error("No data found in the response.")
        raise ValueError("No data returned from vehicle listings API")

    df['retrieval_date'] = datetime.now().strftime('%Y-%m-%d')
    df['retrieval_date'] = pd.to_datetime(df['retrieval_date'], errors='coerce')
    logger.info('retrieval_date added to dataframe')

    if 'firstSeen' in df.columns:
        logger.info("firstSeen column found in data. Column data type changed to datetime")
        df['firstSeen'] = pd.to_datetime(df['firstSeen'], errors='coerce')
    else:
        logger.warning("firstSeen column not found in data")
        df['firstSeen'] = None

    if 'id' in df.columns:
        logger.info("id column found in data. Creating unique_id")
        df['unique_id'] = df['id'].astype(str) + '|' + df['retrieval_date'].astype(str).str.replace('-', '', regex=False)
    else:
        logger.error("id column not found in data")
        raise ValueError("Required column 'id' missing from API response")

    before_drop_duplicate_count = len(df)
    df = df.drop_duplicates(subset=['id'], keep='first').reset_index(drop=True)
    after_drop_duplicate_count = len(df)
    deduplicated_rows_removed = before_drop_duplicate_count - after_drop_duplicate_count
    logger.info(f"Removed {deduplicated_rows_removed} rows.")

    fetch_metrics = {
        "pages_attempted": pages_attempted,
        "pages_succeeded": pages_succeeded,
        "pages_failed": pages_failed,
        "raw_listings_retrieved": raw_listings_retrieved,
        "deduplicated_rows_removed": deduplicated_rows_removed,
        "final_row_count": after_drop_duplicate_count,
    }

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main ETL Process

# COMMAND ----------

def main_etl(df, catalog, schema_name, table_name, init_load, merge_key):
    """
    Writes DataFrame to warehouse with upsert logic.
    Creates table if it doesn't exist, otherwise merges on merge key.
    Logs run results to the etl_run_log.
    """
    start_time = datetime.now()
    full_table_name = f"{catalog}.{schema_name}.{table_name}"
    force_full_load = merge_key is None or str(merge_key).strip() == ""
    num_source_rows = None

    try:
        # Create Spark DataFrame and ensure valid datatypes
        spark_df = spark.createDataFrame(df)
        spark_df = ensure_schema(spark_df)

        # Check if table exists
        table_exists = spark.catalog.tableExists(full_table_name)

        ## FULL LOAD
        if not table_exists or init_load or force_full_load:
            if not table_exists:
                load_type = "CREATE"
            elif force_full_load:
                load_type = "OVERWRITE"
                logger.info(f"{full_table_name} - No merge key provided, forcing full load")
            else:
                load_type = "OVERWRITE"
            logger.info(f"{full_table_name} - Starting {load_type} load")
            num_source_rows = spark_df.count()

            spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(full_table_name)

            logger.info(f"{table_name} {load_type.lower()} completed with {num_source_rows} rows")

            result = {
                "status": "SUCCESS",
                "load_type": load_type,
                "error_message": None,
                "source_row_count": num_source_rows,
                "inserted": num_source_rows,
                "updated": 0,
                "deleted": 0,
            }

        ## INCREMENTAL LOAD
        else:
            logger.info(f"{full_table_name} - Starting MERGE load")
            delta_table = DeltaTable.forName(spark, full_table_name)

            if merge_key not in spark_df.columns:
                raise ValueError(f"Merge key '{merge_key}' not found in source data for {table_name}")

            if merge_key not in delta_table.toDF().columns:
                raise ValueError(f"Merge key '{merge_key}' not found in target data for {table_name}")

            delta_table.alias("target").merge(
                spark_df.alias("source"),
                f"target.{merge_key} = source.{merge_key}"
            ) \
            .withSchemaEvolution() \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

            logger.info("Merge completed successfully")

            # Get metrics for logging
            history_df = delta_table.history(1)
            metrics_row = history_df.select("operationMetrics").first()

            metrics = {}
            if metrics_row and metrics_row[0]:
                metrics = metrics_row[0]
            else:
                logger.warning(f"No operationMetrics returned for merge on {full_table_name}")

            num_updated_rows = int(metrics.get('numTargetRowsUpdated', 0))
            num_inserted_rows = int(metrics.get('numTargetRowsInserted', 0))
            num_deleted_rows = int(metrics.get('numTargetRowsDeleted', 0))
            num_source_rows = spark_df.count()

            logger.info(f"{full_table_name} merged: {num_inserted_rows} inserted, {num_updated_rows} updated, {num_deleted_rows} deleted")

            result = {
                "status": "SUCCESS",
                "load_type": "MERGE",
                "error_message": None,
                "source_row_count": num_source_rows,
                "inserted": num_inserted_rows,
                "updated": num_updated_rows,
                "deleted": num_deleted_rows,
            }

        # Calculate duration
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = duration_seconds / 60

        result.update({
            "duration_seconds": duration_seconds,
            "duration_minutes": duration_minutes,
            "end_time": end_time,
        })

        return result

    except Exception as e:
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = duration_seconds / 60
        logger.error(f"Error writing to {full_table_name}: {str(e)}")

        result = {
            "status": "FAILED",
            "load_type": None,
            "error_message": str(e),
            "source_row_count": num_source_rows,
            "inserted": None,
            "updated": None,
            "deleted": None,
            "duration_seconds": duration_seconds,
            "duration_minutes": duration_minutes,
            "end_time": end_time,
        }

        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write To Bronze

# COMMAND ----------

def run_bronze_pipeline(df, catalog, schema_name, table_name, init_load, merge_key):
    """
    Main ETL pipeline orchestration function.
    """
    start_time = datetime.now()
    try:
        logger.info("STARTING BRONZE PIPELINE")

        # Ensure logging table exists
        logger.info("Ensuring logging table exists")
        ensure_logging_table(catalog, schema_name)

        # Run main ETL function
        logger.info("Executing main ETL process")
        write_result = main_etl(
            df=df,
            catalog=catalog,
            schema_name=schema_name,
            table_name=table_name,
            init_load=init_load,
            merge_key=merge_key,
        )
        logger.info(f"Main ETL completed - Status: {write_result['status']}")

        end_time = datetime.now()

        # Build context for logging
        logger.info("Building run context")
        run_context = {
            "run_id": run_id,
            "table_name": table_name,
            "log_datetime": end_time,
            "status": write_result["status"],
            "load_type": write_result["load_type"],
            "error_message": write_result["error_message"],
            "duration_seconds": write_result["duration_seconds"],
            "duration_minutes": write_result["duration_minutes"],
            "source_row_count": write_result.get("source_row_count"),
            "inserted": write_result.get("inserted"),
            "updated": write_result.get("updated"),
            "deleted": write_result.get("deleted"),
        }
        logger.info("Run context built")

        # Log run
        logger.info("Logging run to etl_run_log")
        log_run(catalog, schema_name, run_context)

        # Check if pipeline succeeded
        if write_result["status"] == "FAILED":
            logger.error("PIPELINE FAILED")
            return run_context

        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        return run_context

    except Exception as e:
        logger.error("CRITICAL PIPELINE FAILURE")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Full Traceback:\n{traceback.format_exc()}")
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        failure_context = {
            "run_id": run_id,
            "table_name": table_name,
            "log_datetime": end_time,
            "status": "FAILED",
            "load_type": "PIPELINE_ERROR",
            "error_message": str(e)[:1000],
            "duration_seconds": duration,
            "duration_minutes": duration / 60,
            "source_row_count": None,
            "inserted": None,
            "updated": None,
            "deleted": None,
        }

        log_run(catalog, schema_name, failure_context)
        return failure_context

# COMMAND ----------

# Pipeline Execution
API_CALLS = {
    "denver_vehicle_listings": fetch_data
}

pipeline_results = []
for table_name in TABLES:
    merge_key = KEYS.get(table_name)
    api_function = API_CALLS[table_name]

    df = api_function()
    logger.info(f"Passing through table {table_name} and key {merge_key}")

    pipeline_result = run_bronze_pipeline(
        df=df,
        catalog=TARGET_CATALOG,
        schema_name=TARGET_SCHEMA,
        table_name=table_name,
        init_load=init_load,
        merge_key=merge_key,
    )
    pipeline_results.append(pipeline_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

if pipeline_results:
    summary_data = [{
        "Table": result["table_name"],
        "Status": result["status"],
        "Load Type": result["load_type"],
        "Duration (min)": round(result["duration_minutes"], 2) if result["duration_minutes"] is not None else None,
        "Rows Inserted": result["inserted"],
        "Rows Updated": result["updated"],
        "Rows Deleted": result["deleted"],
        "Error": result["error_message"] if result["error_message"] else "None",
    } for result in pipeline_results]

    summary_df = spark.createDataFrame(summary_data)
    summary_df = summary_df.select("Table", "Status", "Load Type", "Duration (min)", "Rows Inserted", "Rows Updated", "Rows Deleted", "Error")
    display(summary_df)
else:
    logger.warning("No pipeline results to summarize")
