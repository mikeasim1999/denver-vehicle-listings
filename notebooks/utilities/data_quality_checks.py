# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC Metadata-driven data quality validation. Reads check definitions from the
# MAGIC `data_quality_checks` table and evaluates them against a DataFrame
# MAGIC before data is written to the target table.
# MAGIC
# MAGIC **CRITICAL** failures raise an exception and halt the pipeline.
# MAGIC **WARNING** failures are logged but allow the pipeline to continue.

# COMMAND ----------

def data_quality_checks(table_name, dataframe):
    """
    Runs data quality checks for a target table against a DataFrame.

    Parameters
    ----------
    table_name  : str – Target table name to pull rules for
    dataframe   : DataFrame – The transformed data to validate

    Returns
    -------
    Number of WARNING failures (0 if all checks passed)

    Raises
    ------
    Exception if any CRITICAL check fails, halting the pipeline
    """

    # Register DataFrame as a temp view so subquery-based checks
    # (e.g. duplicate detection) can reference it by name
    dataframe.createOrReplaceTempView(table_name)

    # Fetch rules for the target table
    rules_df = spark.sql(f"""
        SELECT rule_id, rule_name, sql_expression, severity
        FROM {CATALOG}.{TARGET_SCHEMA}.data_quality_checks
        WHERE target_table = '{table_name}'
        ORDER BY rule_id
    """).collect()

    if not rules_df:
        print(f"No data quality rules found for {table_name} in {CATALOG}.{TARGET_SCHEMA}.data_quality_checks")
        return 0

    critical_failures = []
    warning_failures  = []

    print(f"  Data Quality Checks — {CATALOG}.{TARGET_SCHEMA}.{table_name}")

    # Evaluate each rule
    for rule in rules_df:
        fail_count = dataframe.where(rule['sql_expression']).count()

        if fail_count > 0 and rule['severity'] == 'CRITICAL':
            critical_failures.append({
                'rule_id':   rule['rule_id'],
                'rule_name': rule['rule_name'],
                'fail_count': fail_count
            })
            print(f"  CRITICAL  {rule['rule_name']} — {fail_count} records failed")

        elif fail_count > 0 and rule['severity'] == 'WARNING':
            warning_failures.append({
                'rule_id':   rule['rule_id'],
                'rule_name': rule['rule_name'],
                'fail_count': fail_count
            })
            print(f"  WARNING   {rule['rule_name']} — {fail_count} records failed")

        else:
            print(f"  PASSED    {rule['rule_name']}")

    # Summary
    total = len(rules_df)
    passed = total - len(critical_failures) - len(warning_failures)

    print(f"{'=' * 65}")
    print(f"  {total} checks | {passed} passed | {len(warning_failures)} warnings | {len(critical_failures)} critical")
    print(f"{'=' * 65}\n")

    # Halt pipeline on CRITICAL failures
    if critical_failures:
        failure_details = ', '.join(
            [f"{f['rule_name']} ({f['fail_count']} rows)" for f in critical_failures]
        )
        raise Exception(
            f"Data quality validation FAILED for {table_name}. "
            f"Critical failures: {failure_details}"
        )

    return len(warning_failures)
