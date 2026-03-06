"""
Grouping Measure Implementation

This module implements the grouping measure functionality that allows executing measures
based on grouped data. The grouping measure takes a list of attributes and measures,
groups the data by specified attributes, and executes measures for each group.

Example:
    Selected Attributes: Make, Color
    Group By Attribute: VIN
    Selected Measures: null_count, Length, blank_count
    
    This will:
    1. Group data by VIN
    2. For each group, execute null_count, Length, and blank_count measures
    3. Calculate scores at measure level, attribute level, and source measure level
"""

import json
from uuid import uuid4
from datetime import datetime
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection, execute_query, delete_metrics
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import calculate_weightage_score, check_measure_result
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_constants.dq_constants import PASSED, FAILED, PENDING

MEASURES = None
QUERIES = None

def execute_cost_performance_measure(measure: dict, config: dict, default_queries: dict) -> None:
    """
    Executes a cost or performance measure for the given configuration and measure definition.

    This function retrieves the appropriate query for the measure, executes it to obtain the measure value,
    updates the measure dictionary with the result, and saves the measure details to the database.

    Args:
        measure (dict): The measure definition containing metadata and configuration.
        config (dict): The configuration dictionary for the current run.
        default_queries (dict): A dictionary of default queries for various measure categories.

    Raises:
        Exception: If any error occurs during execution or saving of the measure.
    """
    try:
        measure = measure if measure else {}
        measure_name = measure.get("name", "")
        measure_category = measure.get("category", "")
        measure_relation = f"{measure_category}_measure"
        queries = default_queries.get(measure_relation, {}) if default_queries else {}
        measure_query = queries.get(measure_name, "")
        measure_value, query_string = get_measure_value(config, measure_query)
        measure.update({"value": measure_value, "processed_query": query_string})
        save_measure_detail(config, measure)
    except Exception as e:
        log_error(f"Failed to execute grouping measure: {str(e)}", e)
        raise e

def get_measure_value(config: dict, query: dict):
    """
    Executes the provided query to retrieve the value for a cost or performance measure.

    The function replaces the <date> placeholder in the query with the current or configured task date,
    executes the query, and returns the resulting value and the processed query string.

    Args:
        config (dict): The configuration dictionary, possibly containing 'task_date'.
        query (str): The SQL query string with a possible <date> placeholder.

    Returns:
        tuple: A tuple containing the value (from the query result) and the processed query string.
    """
    task_date = config.get("task_date", "")
    if task_date:
        task_date = task_date.strftime("%Y-%m-%d")
    else:
        task_date = datetime.now().strftime("%Y-%m-%d")
    query_string = query.replace("<date>", task_date)
    response, _ = execute_native_query(config, query_string)
    response = response if response else {}
    value = response.get("value", 0)
    return value, query_string

def get_selected_columns(measure_name: str):
    """
    Returns the column name to be selected based on the measure name.

    Args:
        measure_name (str): The name of the measure.

    Returns:
        str: The column name to be used in queries for this measure.
    """
    if measure_name == "data_spillage":
        return "total_spill_gb"
    else:
        return "query_count"

def save_measure_detail(config: dict, measure: dict):
    """
    Saves the measure result and related metadata to the database.

    This function deletes any existing metrics for the current run and measure,
    then inserts the new metric result into the database. It also updates the measure's
    score and result in the core.measure table.

    Args:
        config (dict): The configuration dictionary for the current run.
        measure (dict): The measure dictionary containing result and metadata.

    Raises:
        Exception: If any error occurs during the database operations.
    """
    measure_id = measure.get("id", "")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id", "")
    airflow_run_id = config.get("airflow_run_id")
    weightage = measure.get("weightage", 100)
    weightage = int(weightage) if weightage else 100
    is_archived = measure.get("is_archived", False)
    allow_score = measure.get("allow_score", True)
    is_drift_enabled = measure.get("is_drift_enabled", False)
    measure_name = measure.get("name", "")
    value = measure.get("value", "")
    total_records = measure.get("total_records", 0)
    valid_count = measure.get("valid_count", 0)
    invalid_count = measure.get("invalid_count", 0)
    valid_percentage = measure.get("valid_percentage", 0)
    invalid_percentage = measure.get("invalid_percentage", 0)
    score = measure.get("score", None)
    if not allow_score:
        score = None
    processed_query = measure.get("processed_query", "")
    delete_metrics(
        config,
        run_id=run_id,
        measure_id=measure_id,
        level="measure"
    )
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            measure_results = []
            query_input = (
                str(uuid4()),
                organization_id,
                connection_id,
                None,
                None,
                measure_id,
                run_id,
                airflow_run_id,
                None,
                measure_name,
                "measure",
                str(value),
                weightage,
                total_records,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                PASSED,
                is_archived,
                processed_query,
                allow_score,
                is_drift_enabled,
                True,
                True,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            measure_results.append(query_param)

            try:
                pass_criteria_result = check_measure_result(measure, score)
                score = "null" if score is None else score
                update_measure_score_query = f"""
                    update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count} , result = '{pass_criteria_result}'
                    where id='{measure_id}'
                """
                cursor = execute_query(
                    connection, cursor, update_measure_score_query)

            except Exception as e:
                pass

            measures_input = split_queries(measure_results)
            for input_values in measures_input:
                try:
                    input_value = ",".join(input_values)
                    attribute_insert_query = f"""
                        insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                        measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                        valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                        query, allow_score, is_drift_enabled, is_measure,is_active,is_delete, created_date)
                        values {input_value}
                    """
                    cursor = execute_query(
                        connection, cursor, attribute_insert_query)
                except Exception as e:
                    log_error("Save Measures: inserting new metric", e)
            
    except Exception as e:
        log_error(f"Failed to save metrics: {str(e)}", e)
        raise e