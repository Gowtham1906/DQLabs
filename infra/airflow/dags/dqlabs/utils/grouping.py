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
from typing import Dict, List, Any, Tuple
from copy import deepcopy
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection, execute_query, delete_metrics
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import calculate_weightage_score, check_measure_result
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.connection_helper import format_attribute_name
from dqlabs.app_constants.dq_constants import PASSED, FAILED, PENDING
from dqlabs.utils.extract_workflow import get_queries, get_merge_grouping_measure_queries, prepare_grouping_measure_query, prepare_custom_grouping_measure_query
from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query, extract_attribute_ids_from_rules
from dqlabs.enums.connection_types import ConnectionType

MEASURES = None
QUERIES = None

def execute_grouping_measure(measure: dict, config: dict, default_queries: dict) -> None:
    """
    Executes the grouping measure by:
    1. Extracting grouping configuration from measure properties
    2. Building the grouping query with pagination
    3. Executing measures for each group with chunking
    4. Calculating scores at different levels
    
    Args:
        measure (dict): The measure configuration
        config (dict): The execution configuration
        default_queries (dict): Default queries for the connection type
    """
    try:
        
        # Extract grouping configuration from measure properties
        measure_detail = deepcopy(measure)
        properties = measure.get("properties", {})
        connection_type = config.get("connection_type", "")
        
        if connection_type == ConnectionType.MongoDB.value:
            return
        
        asset_id = properties.get("asset", "")
        if isinstance(properties, str):
            properties = json.loads(properties)
        global MEASURES
        global QUERIES
        
        selected_attributes = properties.get("attributes", [])
        group_by_attributes = properties.get("group_by_attributes", [])
        selected_measures = properties.get("measures", [])
        is_custom_measure = properties.get("custom_measure", False)
        group_measure_type = properties.get("group_measure_type", "")
        is_sql_measure = group_measure_type == "sql_measure"
        attribute_aggregation_pairs = properties.get("attribute_aggregation_pairs", [])
        MEASURES = properties.get("measures", [])
        group_by_clause = group_by_attributes[0].get("name", "")
        group_by_clause = format_attribute_name(connection_type, group_by_clause)

        if is_custom_measure and is_sql_measure:
            if (not attribute_aggregation_pairs or not group_by_attributes):
                raise Exception("Invalid grouping configuration: missing required parameters")
        if is_custom_measure and not is_sql_measure:
            if not selected_measures or not group_by_attributes:
                raise Exception("Invalid grouping configuration: missing required parameters")

        if (not selected_attributes or not group_by_attributes or not selected_measures) and not is_custom_measure:
            raise Exception("Invalid grouping configuration: missing required parameters")
    

        asset = get_asset_detail(config, asset_id)
        config.update({"asset": asset})
        config.update({"table_name": format_table_name(config)})
        total_records = 0
        overall_score = 0
        overall_count = 0
        processed_query = ""

        # Prepare Default Queries
        default_queries_result = get_queries(config)
        default_queries = default_queries_result if isinstance(default_queries_result, dict) else {}
        queries = default_queries if default_queries else {}
        queries = get_merge_grouping_measure_queries(queries)
        QUERIES = default_queries.get("grouping", {}) if default_queries else {}

        if is_custom_measure:
            if group_measure_type == "sql_measure":
                results, processed_query = get_grouping_sql_query_result(config, measure)
                measure_stats = process_group_result(config, results, measure_detail, [], [], group_by_attributes)
                total_records = measure_stats.get("total_records", 0)
                overall_score = measure_stats.get("overall_score", 0)
                overall_count = measure_stats.get("overall_count", 0)
            else:
                measures_list = [measure.get("id") for measure in selected_measures]
                measures = get_measures(config, measures_list)
                results, processed_query = get_grouping_custom_query_result(config, measures, measure, group_by_clause, default_queries)
                measure_stats = process_group_result(config, results, measure_detail, measures, [], group_by_attributes)
                total_records = measure_stats.get("total_records", 0)
                overall_score = measure_stats.get("overall_score", 0)
                overall_count = measure_stats.get("overall_count", 0)
        else:
            # Get all measures from the database
            measures = [measure.get("id") for measure in selected_measures]
            attribute_queries = []
            for attribute in selected_attributes:
                attribute_type = attribute.get("type", "")
                attribute_name = attribute.get("name", "")
                for measure in measures:
                    query = queries.get(measure, "")
                    if not query:
                        continue
                    query = prepare_grouping_measure_query(query, str(measure), attribute_type, attribute_name)
                    attribute_queries.append(query)
            attribute_queries = ",".join(attribute_queries)
            group_count = get_group_result_count(config, [*selected_attributes, *group_by_attributes], group_by_clause)

            if group_count > 0:
                delete_existing_table(config, measure_detail.get("id", ""))

            chunk_size = 25000
            start = 1

            while start <= group_count:
                end = min(start + chunk_size - 1, group_count)
                results, processed_query = get_group_results(config, [*selected_attributes, *group_by_attributes], group_by_clause, attribute_queries, start, end)
                if results:
                    chunk_stats = process_group_result(config, results, measure_detail, selected_measures, selected_attributes, group_by_attributes)
                    total_records += chunk_stats.get("total_records", 0)
                    overall_score += chunk_stats.get("overall_score", 0)
                    overall_count += chunk_stats.get("overall_count", 0)
                start = end + 1

            # Calculate overall score for the grouping measure
        calculate_grouping_measure_score(total_records, overall_count, overall_score, measure_detail)

        # Update processed query
        measure_detail.update({"processed_query": processed_query})

        # Save the measure details
        save_measure_detail(config, measure_detail)

    except Exception as e:
        log_error(f"Failed to execute grouping measure: {str(e)}", e)
        raise e

def get_group_result_count(config: dict, attributes: list, group_by_clause: str) -> int:
    """
    Get the count of grouping results from the database
    """
    try:
        asset = config.get("asset", {})
        properties = asset.get("properties", {})
        table_name = asset.get("name", "")
        schema_name = properties.get("schema", "")
        database = properties.get("database", "")
        query = asset.get("query", "")
        if query:
            query_string = QUERIES.get("group_count_query", "") if QUERIES else ""
            query_string = query_string.replace("<group_by_clause>", group_by_clause).replace("<query>", query)
        else:
            query_string = QUERIES.get("group_count", "") if QUERIES else ""

            # Prepare the query for JSON attributes
            attributes, is_json_attributes = extract_attribute_ids_from_rules(config, attributes) 
            if is_json_attributes:
                query_string = prepare_json_attribute_flatten_query(config, None, query_string,"sub_query", attributes)

            query_string = (query_string.replace("<group_by_clause>", group_by_clause)
                            .replace("<database_name>", database)
                            .replace("<schema_name>", schema_name)
                            .replace("<table_name>", table_name))
        counts, _ = execute_native_query(config, query_string, is_count_only=True)
        return int(counts) if counts is not None else 0
    except Exception as e:
        log_error(f"Failed to get group result count: {str(e)}", e)
        raise e

def get_group_results(config: dict, attributes: list, group_by_clause: str, attribute_queries: str, offset: int, chunk_size: int) -> Tuple[List[Dict[str, Any]], str]:      
    """
    Get group results with streaming approach to prevent memory accumulation.
    Processes and yields results immediately without storing in memory.
    """
    try:
        # Use smaller chunk size for memory efficiency
        asset = config.get("asset", {})
        properties = asset.get("properties", {})
        table_name = asset.get("name", "")
        schema_name = properties.get("schema", "")
        database = properties.get("database", "")
        query = asset.get("query", "")
        query_string = QUERIES.get("group_result_query", "") if query else QUERIES.get("group_result", "")
        if not query_string:
            return []

        # Prepare the query for JSON attributes
        attributes, is_json_attributes = extract_attribute_ids_from_rules(config, attributes) 
        if is_json_attributes:
            query_string = prepare_json_attribute_flatten_query(config, None, query_string,"sub_query", attributes)

        # Replace placeholders in query string
        query_string = (query_string.replace("<group_by_clause>", group_by_clause)
                        .replace("<query>", query)
                        .replace("<measure_query>", attribute_queries)
                        .replace("<database_name>", database)
                        .replace("<schema_name>", schema_name)
                        .replace("<table_name>", table_name)
                        .replace("<limit>", str(chunk_size))
                        .replace("<offset>", str(offset)))
        results, _ = execute_native_query(config, query_string, is_list=True)
        return results if isinstance(results, list) else [] , query_string
    except Exception as e:
        log_error(f"Failed to get group results: {str(e)}", e)
        raise e

def create_and_insert_table(config: dict, table_name: str, data: list):
    """
    Creates a table and inserts records into it.
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            columns = []
            sample_data = data[0]
            # Prepare columns for Postgres table creation based on sample data
            for column_name, sample_value in sample_data.items():
                # Infer Postgres column type
                if isinstance(sample_value, int):
                    column_type = "INTEGER"
                elif isinstance(sample_value, float):
                    column_type = "DOUBLE PRECISION"
                elif isinstance(sample_value, bool):
                    column_type = "BOOLEAN"
                elif isinstance(sample_value, str):
                    column_type = "TEXT"
                elif sample_value is None:
                    column_type = "TEXT"
                else:
                    column_type = "TEXT"
                columns.append(f'"{column_name}" {column_type}')
            
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS grouping."{table_name}" (
                    {', '.join(columns)}
                )
            """
            cursor.execute(create_table_query)

            detail_insert_objects = []
            for report in data:
                query_input = tuple(report.values())
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input
                ).decode("utf-8")
                detail_insert_objects.append(query_param)
            
            detail_insert_objects = split_queries(detail_insert_objects)
            for input_values in detail_insert_objects:
                try:
                    columns = [f'"{column}"' for column in sample_data.keys()]
                    columns_string = ", ".join(columns)
                    query_input = ",".join(input_values)
                    insert_query = f"""
                        INSERT INTO grouping."{table_name}" (
                            {columns_string}
                        ) VALUES {query_input}
                    """
                    cursor = execute_query(connection, cursor, insert_query)
                except Exception as e:
                    log_error(f"Detail records insert failed for {table_name}: {str(e)}", e)
    except Exception as e:
        log_error(f"Failed to create table: {str(e)}", e)
        raise e

def delete_existing_table(config: dict, measure_id: str):
    """
    Delete existing table
    """
    try:
        table_name = measure_id.replace("-", "_")
        detail_table_name = f"{table_name}_detail"
        summary_table_name = f"{table_name}_summary"
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                DROP TABLE IF EXISTS grouping."{detail_table_name}"
            """
            cursor.execute(query_string)
            query_string = f"""
                DROP TABLE IF EXISTS grouping."{summary_table_name}"
            """
            cursor.execute(query_string)
    except Exception as e:
        log_error(f"Failed to delete table: {str(e)}", e)
        raise e

def get_grouping_custom_query_result(config: dict, measures: list, measure: dict, group_by_clause: str, default_queries: dict):
    """
    Get the grouping custom query result
    """
    try:
        asset = config.get("asset", {})
        properties = asset.get("properties", {})
        table_name = asset.get("name", "")
        schema_name = properties.get("schema", "")
        database = properties.get("database", "")
        connection_type = config.get("connection_type", "")
        query = asset.get("query", "")

        asset_query_string = QUERIES.get("group_custom_measure_asset_query", "") if query else QUERIES.get("group_custom_measure_query", "")
        if not asset_query_string:
            return []

        asset_query_string = (asset_query_string.replace("<group_by_clause>", group_by_clause)
                    .replace("<query>", query)
                    .replace("<database_name>", database)
                    .replace("<schema_name>", schema_name)
                    .replace("<table_name>", table_name))   
        
        # Prepare Custom Measure Grouping Query
        query_string = prepare_custom_grouping_measure_query(config, measures, default_queries, group_by_clause, asset_query_string)
        
        results, _ = execute_native_query(config, query_string, is_list=True)
        processed_query = query_string

        if results:
            delete_existing_table(config, measure.get("id", ""))
        return results if isinstance(results, list) else [] , processed_query
    except Exception as e:
        log_error(f"Failed to get grouping custom query result: {str(e)}", e)
        raise e

def prepare_grouping_sql_query(sql_query: str, attribute_aggregations: list, group_by_clause: str) -> str:
    """
    Prepare the SQL query
    """
    try:
        agg_attributes = ", ".join(attribute_aggregations)
        sql_query = sql_query.strip().replace(";", "")
        sql_query = f"""
        WITH user_sql_query AS (
            {sql_query}
        )
        SELECT {group_by_clause}, {agg_attributes}
        FROM user_sql_query
        GROUP BY {group_by_clause}
        """
        return sql_query
    except Exception as e:
        log_error(f"Failed to prepare SQL query: {str(e)}", e)

def prepare_attribute_aggregation_pairs(attribute_aggregation_pairs: list) -> list:
    """
    Prepare attribute aggregation pairs for SQL query.
    Maps UI aggregation types to SQL-compatible functions.
    """
    # Map UI aggregation types to SQL functions
    aggregation_mapping = {
        "AVERAGE": "AVG",
        "COUNT": "COUNT",
        "SUM": "SUM",
        "MIN": "MIN",
        "MAX": "MAX"
    }
    
    attribute_aggregations = []
    agg_attributes = []
    for attribute_aggregation_pair in attribute_aggregation_pairs:
        attribute_name = attribute_aggregation_pair.get("attribute", {}).get("name", "")
        aggregation_type = attribute_aggregation_pair.get("aggregation", {}).get("value")
        
        if not aggregation_type:
            continue
        
        # Map aggregation type to SQL-compatible function
        sql_aggregation = aggregation_mapping.get(aggregation_type.upper(), aggregation_type.upper())
        
        # Use original aggregation_type for alias to maintain consistency with results
        attbute_agg = f"{sql_aggregation}({attribute_name}) AS {attribute_name}__{aggregation_type}" if aggregation_type else attribute_name
        agg_attributes.append(f"{attribute_name}__{aggregation_type}")
        attribute_aggregations.append(attbute_agg)
    return attribute_aggregations, agg_attributes

def get_grouping_sql_query_result(config: dict, measure: dict) -> Tuple[List[Dict[str, Any]], str]:
    """
    Get the grouping SQL query result
    """
    try:
        
        properties = measure.get("properties", {})
        sql_query = properties.get("sql_query", "")
        group_by_attributes = properties.get("group_by_attributes", "")
        attribute_aggregation_pairs = properties.get("attribute_aggregation_pairs", "")

        group_by_clause = group_by_attributes[0].get("name", "")
        # prepare the attribute aggregation pairs
        attribute_aggregations, _ = prepare_attribute_aggregation_pairs(attribute_aggregation_pairs)
        
        # prepare the sql query
        query_string = prepare_grouping_sql_query(sql_query, attribute_aggregations, group_by_clause)

        results, _ = execute_native_query(config, query_string, is_list=True)
        if results:
            delete_existing_table(config, measure.get("id", ""))
        return results if isinstance(results, list) else [] , query_string
    except Exception as e:
        log_error(f"Failed to get grouping SQL query result: {str(e)}", e)
        raise e

def process_group_result(config: dict, results: List[Dict[str, Any]], measure: dict, measures: list, selected_attributes: list, group_by_attributes: list) -> Dict[str, int]:
    """
    Process group results and return statistics for score calculation
    """
    group_by_attributes = group_by_attributes[0]
    group_by_clause = group_by_attributes.get("name", "") if isinstance(group_by_attributes, dict) else ""
    weightage = measure.get("weightage", 100)
    weightage = int(weightage) if weightage else 100
    is_positive = measure.get("is_positive", False)
    measure_id = measure.get("id", "")
    measure_properties = measure.get("properties", {})
    is_custom_measure = measure_properties.get("custom_measure", False)
    group_measure_type = measure_properties.get("group_measure_type", "")
    is_sql_measure = group_measure_type == "sql_measure"
    detail_report_list = []
    summary_report_list = []
    
    total_records = 0
    overall_score = 0
    overall_count = 0
    if is_sql_measure:
        _, agg_attributes = prepare_attribute_aggregation_pairs(measure_properties.get("attribute_aggregation_pairs", ""))
    for result in results:
        group_by_name = group_by_clause.lower()
        group_by_value = result.get(group_by_name, "")
        group_total_records = result.get("count", 0)
        total_records += group_total_records
        
        detail_report = {group_by_clause: group_by_value}
        summary_report = {group_by_clause: group_by_value}
        if is_sql_measure:
            for agg_attribute in agg_attributes:
                summary_report[agg_attribute] = result.get(agg_attribute.lower(), 0)
                detail_report[agg_attribute] = result.get(agg_attribute.lower(), 0)
        group_scores = []
        if is_custom_measure:
            for measure in measures:
                measure_technical_name = measure.get("id", "")
                name = f"{measure_technical_name}".lower()
                measure_name = measure.get("name", "").lower()
                valid_count = result.get(measure_name, 0)
                if valid_count is None:
                        valid_count = 0
                elif isinstance(valid_count, str):
                    if valid_count.lower() in ("true", "false"):
                        valid_count = 1 if valid_count.lower() == "true" else 0
                    else:
                        try:
                            valid_count = int(valid_count)
                        except ValueError:
                            valid_count = 0
                elif not isinstance(valid_count, int):
                    try:
                        valid_count = int(valid_count)
                    except Exception:
                        valid_count = 0

                valid_count = valid_count if is_positive else (group_total_records - valid_count)
                # Ensure group_total_records is not None or zero before division
                if group_total_records is None or group_total_records == 0:
                    valid_percentage = 0
                else:
                    try:
                        valid_percentage = float(valid_count) / float(group_total_records) * 100
                    except Exception:
                        valid_percentage = 0
                score = calculate_weightage_score(valid_percentage, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                group_scores.append(score)
                report_measure_name = f"{measure_technical_name}".replace("-", "_")
                detail_report.update({report_measure_name: score})
        else:
            for attribute in selected_attributes:
                attribute_name = attribute.get("name", "").replace(".", "_")
                attribute_id = attribute.get("id", "").replace("-", "_")
                
                attribute_scores = []
                for measure in measures:
                    measure_technical_name = measure.get("id", "")
                    name = f"{attribute_name}___{measure_technical_name}".lower()
                    valid_count = result.get(name, 0)
                    # Safely convert valid_count to int, handling boolean-like strings
                    # Ensure valid_count is not None and is an int
                    if valid_count is None:
                        valid_count = 0
                    elif isinstance(valid_count, str):
                        if valid_count.lower() in ("true", "false"):
                            valid_count = 1 if valid_count.lower() == "true" else 0
                        else:
                            try:
                                valid_count = int(valid_count)
                            except ValueError:
                                valid_count = 0
                    elif not isinstance(valid_count, int):
                        try:
                            valid_count = int(valid_count)
                        except Exception:
                            valid_count = 0

                    valid_count = valid_count if is_positive else (group_total_records - valid_count)
                    # Ensure group_total_records is not None or zero before division
                    if group_total_records is None or group_total_records == 0:
                        valid_percentage = 0
                    else:
                        try:
                            valid_percentage = float(valid_count) / float(group_total_records) * 100
                        except Exception:
                            valid_percentage = 0
                    score = calculate_weightage_score(valid_percentage, weightage)
                    score = 100 if score > 100 else score
                    score = 0 if score < 0 else score
                    attribute_scores.append(score)
                    report_measure_name = f"{attribute_id}___{measure_technical_name}".replace("-", "_")
                    detail_report.update({report_measure_name: score})

                attribute_score = sum(attribute_scores) / len(attribute_scores) if attribute_scores else 0
                attribute_score = attribute_score if attribute_score else 0
                attribute_score = 100 if attribute_score > 100 else attribute_score
                attribute_score = 0 if attribute_score < 0 else attribute_score

                group_scores.append(attribute_score)
                summary_report.update({attribute_id: attribute_score})
            
        # Calculate Total Score for this group
        group_score = sum(group_scores) / len(group_scores) if group_scores else 0
        group_score = group_score if group_score else 0
        group_score = 100 if group_score > 100 else group_score
        group_score = 0 if group_score < 0 else group_score
        summary_report.update({"TOTAL SCORE": group_score})
        summary_report_list.append(summary_report)
        detail_report_list.append(detail_report)
        overall_score += group_score
        overall_count += 1

    # Sanitize measure name for table names
    table_name = measure_id.replace("-", "_")
    detail_table_name = f"{table_name}_detail"
    summary_table_name = f"{table_name}_summary"

    # Insert Summary Records to associated tables
    if detail_report_list:
        create_and_insert_table(config, detail_table_name, detail_report_list)
    if summary_report_list:
        create_and_insert_table(config, summary_table_name, summary_report_list)
    
    return {
        "total_records": total_records,
        "overall_score": overall_score,
        "overall_count": overall_count
    }

def calculate_grouping_measure_score(total_records: int, overall_count: int, overall_score: int, measure: dict) -> float:
    """
    Calculate the overall score for the grouping measure
    """
    try:
        score = overall_score / overall_count
        score = score if score else 0
        score = 100 if score > 100 else score
        score = 0 if score < 0 else score

        # Update measure with calculated statistics
        # Calculate valid records using score and total_records
        valid_count = int((score / 100) * total_records) if total_records > 0 else 0
        invalid_count = total_records - valid_count if total_records > 0 else 0
        valid_percentage = (valid_count / total_records * 100) if total_records > 0 else 0
        invalid_percentage = (invalid_count / total_records * 100) if total_records > 0 else 0

        # Update measure with calculated statistics
        measure.update({
            "total_records": total_records,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "valid_percentage": valid_percentage,
            "invalid_percentage": invalid_percentage,
            "score": score,
            "value": valid_count
        })
    except Exception as e:
        log_error(f"Failed to calculate grouping measure score: {str(e)}", e)
        return 0.0


def save_measure_detail(config: dict, measure: dict):
    """
    Save metrics to the database
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

def get_asset_detail(config: dict, asset_id: str):
    """
    Get asset detail
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select asset.name, asset.technical_name, asset.properties, asset.type, asset.group, asset.query
            from core.asset
            where asset.id = '{asset_id}'
        """
        cursor.execute(query_string)
        response = fetchone(cursor)
        response = response if response else {}
        properties = response.get("properties")
        if isinstance(properties, str):
            properties = json.loads(properties)
            response.update({"properties": properties})
    return response

def get_measures(config: dict, measures: list):
    """
    Get measures
    """
    measures = measures if measures else []
    measures = [f"'{measure}'" for measure in measures]
    measures = ", ".join(measures)
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select base_measure.query, base_measure.technical_name as name, base_measure.type,
            measure.id, attribute.id as attribute_id, attribute.name as attribute_name,
            base_measure.properties, base_measure.category
            from core.measure
            join core.base_measure on base_measure.id = measure.base_measure_id
            left join core.attribute on attribute.id = measure.attribute_id
            where measure.id in ({measures})
        """
        cursor.execute(query_string)
        response = fetchall(cursor)
        response = response if response else []
    return response

def format_table_name(config: dict) -> str:
    """
    Constructs and returns the fully qualified table name based on the provided configuration.

    The function determines the table name format based on the connection type and other
    configuration details such as database name, schema, and table technical name. It supports
    various connection types including Snowflake, Athena, Redshift, MSSQL, Oracle, MySQL,
    BigQuery, and more.

    Args:
        config (str): A dictionary containing configuration details. Expected keys include:
            - "asset": A dictionary containing asset details.
                - "properties": A dictionary containing asset properties, including:
                    - "database" (str): The name of the database (optional).
            - "connection": A dictionary containing connection details.
                - "type" (str): The type of the connection (e.g., Snowflake, MSSQL, etc.).
            - "table_technical_name" (str): The technical name of the table.
            - "schema" (str): The schema name (optional).

    Returns:
        str: The fully qualified table name formatted according to the connection type.
    """
    asset = config.get("asset")
    asset_properties = asset.get("properties", {})
    db_name = asset_properties.get("database", "")
    schema = asset_properties.get("schema", "")
    table_technical_name = asset.get("technical_name", "")

    connection = config.get("connection", "")
    connection_type = connection.get("type", "")
    table_name = ""
    if connection_type in [
        ConnectionType.Snowflake.value,
        ConnectionType.Athena.value,
        ConnectionType.EmrSpark.value,
        ConnectionType.Redshift.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
        ConnectionType.Denodo.value,
        ConnectionType.Db2.value,
        ConnectionType.DB2IBM.value,
        ConnectionType.SapHana.value,
        ConnectionType.SapEcc.value,
        ConnectionType.Oracle.value,
    ]:
        table_name = f'"{schema}"."{table_technical_name}"'
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table_technical_name}"'
    elif connection_type in [ConnectionType.MSSQL.value, ConnectionType.Synapse.value]:
        table_name = f"[{schema}].[{table_technical_name}]"
        if db_name:
            table_name = f"[{db_name}].[{schema}].[{table_technical_name}]"

    elif connection_type == ConnectionType.MySql.value:
        table_name = f"{schema}.{table_technical_name}"
    elif connection_type == ConnectionType.Databricks.value:
        table_name = f'`{schema}`.`{table_technical_name}`'
        if db_name:
            table_name = f'`{db_name}`.`{schema}`.`{table_technical_name}`'
    elif connection_type == ConnectionType.BigQuery.value:
        table_name = f"`{schema}`.`{table_technical_name}`"
    else:
        table_name = f'"{schema}"."{table_technical_name}"'
        if db_name:
            table_name = f'"{db_name}"."{schema}"."{table_technical_name}"'

    return table_name