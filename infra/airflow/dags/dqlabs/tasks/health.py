"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
from dqlabs.app_helper.dq_helper import (
    get_attribute_label,
    convert_to_lower,
    get_derived_type_category,
    calculate_weightage_score,
    check_measure_result,
    get_max_workers,
    check_is_direct_query,
    process_direct_query,
    transform_mongodb_field_names,
)
from dqlabs.app_helper.db_helper import execute_query, split_queries, fetchone
from dqlabs.utils.extract_workflow import (
    get_health_measures,
    get_metadata_query,
    get_queries,
    update_attribute_row_count,
    update_metrics,
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    get_native_connection,
    get_query_string,
    delete_metrics
)
# NEW: Import the necessary threshold function
from dqlabs.utils.profile import get_stored_threshold
from dqlabs.app_constants.dq_constants import (
    HEALTH,
    PASSED,
    ATTRIBUTE_RANGE_COLUMNS,
    ATTRIBUTE_LENGTH_RANGE_COLUMNS,
)
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.tasks.scoring import update_scores
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.profile import get_temp_table_query
from dqlabs.utils import is_scoring_enabled
from collections import defaultdict


def check_if_first_run_for_range_measure(connection, measure_id, attribute_id, run_id):
    """
    Detect if this is a first run by checking if profile.py created baseline thresholds
    or if only (0,0) thresholds exist in drift_threshold table.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        run_id (str): Current run ID
        
    Returns:
        bool: True if first run (should skip), False if subsequent run (process normally)
    """
    try:
        with connection.cursor() as cursor:
            # Method 1: Check if profile.py created baseline threshold in current run
            baseline_check_query = f"""
                SELECT threshold FROM core.metrics 
                WHERE measure_id = '{measure_id}' 
                AND attribute_id = '{attribute_id}' 
                AND run_id = '{run_id}'
                AND threshold IS NOT NULL
                AND threshold::text LIKE '%"is_baseline": true%'
            """
            cursor = execute_query(connection, cursor, baseline_check_query)
            current_run_baseline = fetchone(cursor)
            
            if current_run_baseline:
                log_info(f"FIRST_RUN_CHECK: Found baseline threshold from profile.py in current run")
                return True
            
            # Method 2: Check if drift_threshold has only (0,0) values (indicates first run)
            drift_check_query = f"""
                SELECT lower_threshold, upper_threshold 
                FROM core.drift_threshold 
                WHERE measure_id = '{measure_id}' 
                AND attribute_id = '{attribute_id}'
                ORDER BY created_date DESC 
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, drift_check_query)
            drift_result = fetchone(cursor)
            
            if drift_result:
                lower = float(drift_result.get("lower_threshold", 0))
                upper = float(drift_result.get("upper_threshold", 0))
                
                # If drift table has (0,0), this indicates first run scenario
                if lower == 0 and upper == 0:
                    log_info(f"FIRST_RUN_CHECK: Drift table has (0,0) values - first run detected")
                    return True
                else:
                    log_info(f"FIRST_RUN_CHECK: Drift table has valid values ({lower}, {upper}) - subsequent run")
                    return False
            else:
                log_info(f"FIRST_RUN_CHECK: No drift threshold found - first run detected")
                return True
                
    except Exception as e:
        log_info(f"FIRST_RUN_CHECK: Error checking first run status, defaulting to normal processing: {e}")
        return False  # Default to normal processing if check fails


def update_attribute_run_id(config: dict) -> None:
    """
    Updates the run id's for all the attributes
    """
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    run_id = config.get("run_id")
    attributes = config.get("attributes")
    attributes = attributes if attributes else []
    attributes = [
        f"""'{attribute.get("id")}'""" for attribute in attributes if attribute
    ]
    attributes = ", ".join(attributes) if attributes else ""

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.attribute set last_run_id='{run_id}'
            where asset_id='{asset_id}' and id in ({attributes})
        """
        cursor = execute_query(connection, cursor, query_string)


def update_row_count(row_count: int, config: dict) -> None:
    """
    Updates the recent row count into metadata tables
    """
    asset_id = config.get("asset_id")
    run_id = config.get("run_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.asset set last_run_id='{run_id}'
            where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        query_string = f"""
            update core.data set row_count={row_count}
            where asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        attribute_query_string = f"""
            update core.attribute set row_count={row_count}
            where asset_id='{asset_id}' AND parent_attribute_id IS NULL
        """
        cursor = execute_query(connection, cursor, attribute_query_string)


def save_measures(basic_measures: dict, config: dict) -> None:
    """
    Write the basic measures for this run into postgres db
    """
    measure_level_queries = config.get("measure_level_queries", {})
    measure_level_queries = measure_level_queries if measure_level_queries else {}
    attributes = config.get("attributes", [])
    measures = config.get("measures", [])
    connection_type = config.get("connection_type")
    asset_id = config.get("asset_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    is_incremental = config.get("is_incremental", False)
    watermark = config.get("watermark", "")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    connection = get_postgres_connection(config)
    if attributes:
        update_attribute_row_count(config, attributes[0])

    total_records = config.get("total_records", 0)
    total_records = int(total_records) if total_records else 0
    with connection.cursor() as cursor:
        incremental_time = ""
        measure_input_values = []
        # Prepare attribute level measures for insertion
        for attribute in attributes:
            basic_profile = {}
            attribute_name = attribute.get("name")
            attribute_id = attribute.get("id")
            attribute_label = get_attribute_label(attribute_name)
            derived_type = attribute.get("derived_type", "").lower()
            derived_type_category = get_derived_type_category(derived_type)
            attribute_measures = list(
                filter(
                    lambda measure: measure.get("attribute_id") == attribute_id,
                    measures,
                )
            )

            for measure in attribute_measures:
                measure_name = measure.get("name")
                measure_id = measure.get("id")
                
                # FIRST RUN SKIP: Only skip range measures when profile.py has created baseline thresholds
                if measure_name in ['value_range', 'length_range']:
                    is_first_run = check_if_first_run_for_range_measure(connection, measure_id, attribute_id, run_id)
                    
                    if is_first_run:
                        log_info(f"HEALTH_SKIP: First run detected - skipping {measure_name}, handled by profile.py")
                        continue
                    else:
                        log_info(f"HEALTH_CONTINUE: Subsequent run detected - processing {measure_name} normally")
                
                key = f"{attribute_label}{measure_name}"
                value = basic_measures.get(key, 0)
                executed_query = measure_level_queries.get(key, "")
                executed_query = executed_query if executed_query else ""
                if measure_name.lower() not in ["max_length", "min_length"]and (measure_name.lower() not in ["blank_count", "space_count", "distinct_count"] or connection_type.lower() not in [ConnectionType.MSSQL.value, ConnectionType.SapHana.value, ConnectionType.Teradata.value, ConnectionType.MySql.value, ConnectionType.BigQuery.value]):
                    executed_query = executed_query.strip().replace("'", "''")
                
                if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
                    executed_query = executed_query.replace(
                        f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
                    )

                if (
                    (not incremental_time)
                    and is_incremental
                    and watermark == attribute_name
                    and derived_type_category == "date"
                    and measure_name.lower() == "max_value"
                ):
                    incremental_time = value if value else ""

                # Delete metrics for the same run id
                delete_metrics(
                    config,
                    run_id=run_id,
                    measure_id=measure_id,
                    attribute_id=attribute_id,
                )
                
                # --- START: New Integration Logic ---
                threshold_json_for_metric = None
                
                # 1. Fetch the enhanced or baseline threshold for this measure.
                #    On the first run, `found` will be False.
                lower_threshold, upper_threshold, found, feedback_stats = get_stored_threshold(
                    config,
                    measure_name,
                    attribute_id,
                    connection
                )

                # 2. Prepare the threshold JSON data for storage ONLY if a threshold was found.
                if found:
                    if feedback_stats:
                        # An enhanced threshold with user feedback was found.
                        threshold_data = {
                            "lt_percent": 0, "ut_percent": 0,
                            "lower_threshold": feedback_stats.get('enhanced_lower'),
                            "upper_threshold": feedback_stats.get('enhanced_upper'),
                            "feedback_enhanced": True,
                            "feedback_metadata": {
                                "enhancement_type": "context_aware_current_run",
                                "expand_requests": feedback_stats.get('stats', {}).get('expand_count', 0),
                                "contract_requests": feedback_stats.get('stats', {}).get('contract_count', 0),
                                "enhancement_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "confidence_threshold_used": 0.8
                            }
                        }
                        threshold_json_for_metric = json.dumps(threshold_data)
                    else:
                        # A baseline (but not enhanced) threshold was found.
                        threshold_data = {
                            "lt_percent": 0, "ut_percent": 0,
                            "lower_threshold": lower_threshold,
                            "upper_threshold": upper_threshold,
                            "feedback_enhanced": False,
                            "calculation_metadata": {
                                "calculation_type": "retrieved_baseline",
                                "method": "historical",
                                "source": "core.drift_threshold or core.metrics",
                                "calculation_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "is_baseline": True
                            }
                        }
                        threshold_json_for_metric = json.dumps(threshold_data)
                # --- END: New Integration Logic ---

                derived_types = measure.get("derived_type", [])
                if ("all" not in derived_types) and (derived_type not in derived_types):
                    continue
                is_positive = measure.get("is_positive", False)
                allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
                is_drift_enabled = measure.get("is_drift_enabled", False)
                level = measure.get("level")
                total_records = total_records if total_records else 0

                weightage = measure.get("weightage", 100)
                weightage = int(weightage) if weightage else 100
                valid_count = 0
                invalid_count = 0
                valid_percentage = 0
                invalid_percentage = 0
                score = None
                is_archived = False

                if total_records and allow_score:
                    valid_count = int(value) if value else 0
                    valid_count = valid_count if is_positive else (total_records - valid_count)
                    invalid_count = total_records - valid_count
                    valid_percentage = float(valid_count / total_records * 100)
                    invalid_percentage = float(100 - valid_percentage)
                    score = valid_percentage
                    score = calculate_weightage_score(score, weightage)
                    score = 100 if score > 100 else score
                    score = 0 if score < 0 else score

                if measure_name.lower() in ATTRIBUTE_LENGTH_RANGE_COLUMNS:
                    value = str(value) if value or value == 0 else "0"
                elif (
                    measure_name.lower() in ATTRIBUTE_RANGE_COLUMNS
                    and derived_type
                    and derived_type.lower() == "bit"
                ):
                    value = str(value) if value is not None else ""
                else:
                    value = str(value) if value or value == 0 else ""
                value = value.replace("'", "''")
                value = re.sub(r'\x00', '', value)
                basic_profile.update({str(measure_name): value})
                
                # MODIFIED: Added `threshold_json_for_metric` to the tuple
                query_input = (
                    str(uuid4()),
                    organization_id,
                    connection_id,
                    asset_id,
                    attribute_id,
                    measure_id,
                    run_id,
                    airflow_run_id,
                    attribute_name,
                    measure_name,
                    level,
                    value,
                    weightage,
                    total_records,
                    valid_count,
                    invalid_count,
                    valid_percentage,
                    invalid_percentage,
                    score,
                    PASSED,
                    is_archived,
                    executed_query,
                    allow_score,
                    is_drift_enabled,
                    True,
                    True,
                    False,
                    threshold_json_for_metric, 
                    attribute.get("parent_attribute_id", None),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                measure_input_values.append(query_param)

                pass_criteria_result = check_measure_result(measure, score)
                score = 'null' if score is None else score
                # Use the row_count from the attribute object (should now be updated from reliability processing)
                attribute_row_count = attribute.get("row_count", 0)
                attribute_row_count = int(attribute_row_count) if attribute_row_count else total_records
                update_measure_score_query = f"""
                    update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {attribute_row_count}, valid_rows = {valid_count}, invalid_rows = {invalid_count}, result = '{pass_criteria_result}'
                    where attribute_id='{attribute_id}' and id='{measure_id}'
                """
                cursor = execute_query(connection, cursor, update_measure_score_query)

            if basic_profile:
                basic_profile_input = (
                    json.dumps(basic_profile, default=str) if basic_profile else ""
                )
                update_basic_profile_query = f"""
                    update core.attribute set basic_profile='{basic_profile_input}'
                    where asset_id='{asset_id}'
                    and id='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, update_basic_profile_query)

        measures_input = split_queries(measure_input_values)
        for input_values in measures_input:
            try:
                query_input = ",".join(input_values)
                # MODIFIED: Added the 'threshold' column to the INSERT statement
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id, attribute_name, measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure, is_active, is_delete, threshold, parent_attribute_id, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("basic-measures : inserting new metrics", e)

        if incremental_time:
            try:
                query_string = f"""
                    UPDATE core.data SET incremental_time = '{incremental_time}'
                    WHERE asset_id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
            except Exception as e:
                log_error("basic-measures : update incremental time", e)

    if total_records:
        update_row_count(total_records, config)


def execute_queries(config, attribute_id, query, source_connection):
    """
    Execute health measure queries for the given attribute and returns the results.
    Handles both single queries and a list of queries.
    Aggregates results by attribute_id in the required format.
    """
    config.update({"sub_category": f"{config.get('category')} Health"})
    is_direct_query_asset = config.get("is_direct_query_asset")
    base_table_query = config.get("base_table_query")
    connection_type = config.get("connection_type")

    # Prepare the list of queries
    queries = [query] if isinstance(query, str) else query
    # Initialize variables for results
    aggregated_measures = defaultdict(int)
    final_source_connection = source_connection

    for single_query in queries:
        if connection_type == ConnectionType.MongoDB.value and (not single_query or single_query.strip() == ""):
            continue
            
        if is_direct_query_asset and base_table_query:
            single_query = process_direct_query(single_query, connection_type.lower())
            single_query = single_query.replace("<table_query>", base_table_query)

        attribute_measures, native_connection = execute_native_query(
            config, single_query, final_source_connection
        )

        if not final_source_connection and native_connection:
            final_source_connection = native_connection

        if attribute_measures:
            attribute_measures = convert_to_lower(attribute_measures)
            
            if connection_type == ConnectionType.MongoDB.value:
                # Get attribute name from config's attributes list using attribute_id
                attribute_name = ""
                attribute = config.get("attribute", {})
                if attribute and attribute.get("id") == attribute_id:
                    attribute_name = attribute.get("name", "")
                else:
                    # Fallback: search in attributes list
                    attributes = config.get("attributes", [])
                    if attributes and attribute_id:
                        for attr in attributes:
                            if attr.get("id") == attribute_id:
                                attribute_name = attr.get("name", "")
                                break
                
                if attribute_name:
                    attribute_measures = transform_mongodb_field_names(attribute_measures, attribute_name)
            
            # Aggregate the results by summing up values for each key
            for key, value in attribute_measures.items():
                try:
                    # Try to cast to float first
                    numeric_value = float(value)
                    # Check if it can be an integer
                    if numeric_value == int(numeric_value):
                        aggregated_measures[key] = int(numeric_value)
                    else:
                        aggregated_measures[key] = numeric_value
                except (ValueError, TypeError):
                    # If casting fails, retain as-is
                    aggregated_measures[key] = value

    # Convert aggregated_measures back to a regular dictionary
    aggregated_measures = dict(aggregated_measures)
    # Return the final result
    aggregated_measures = (
        aggregated_measures if isinstance(query, list) else aggregated_measures
    )
    return attribute_id, aggregated_measures


def extract_health_measures(config: dict, **kwargs) -> None:
    level = config.get("level")
    asset_id = config.get("asset_id")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    connection_type = config.get("connection_type")
    asset_type = asset.get("type") if asset else ""
    asset_type = asset_type.lower() if asset_type else ""
    view_type = asset.get("view_type") if asset else ""
    view_type = str(view_type).lower() if view_type else "table"
    is_direct_query_asset = check_is_direct_query(config)
    base_table_query = config.get("base_table_query")

    attribute = config.get("attribute", {})
    attribute = attribute if attribute else {}
    attribute_id = attribute.get("id")

    default_queries = get_queries(config)
    source_connection = (
        get_native_connection(config)
        if connection_type.lower()
        not in [ConnectionType.Redshift.value, ConnectionType.Redshift_Spectrum.value]
        else None
    )
    source_connection = (
        source_connection
        if source_connection
        and connection_type.lower()
        in [ConnectionType.Snowflake.value, ConnectionType.Denodo.value]
        else None
    )
    create_temp_table_query = get_temp_table_query(config, default_queries)
    if create_temp_table_query:
        execute_queries(config, None, create_temp_table_query, source_connection)

    row_count_query = get_metadata_query(
        "total_rows", default_queries, config, False, True
    )
    if is_direct_query_asset:
        direct_query_metadata = default_queries.get("direct_query_metadata", {})
        direct_query_metadata = direct_query_metadata if direct_query_metadata else {}
        row_count_query = direct_query_metadata.get("total_rows")
        row_count_query = row_count_query.replace("<table_query>", base_table_query)

    selected_attribute_id = attribute_id if level == "attribute" else ""
    queries, attributes, measures, measure_level_queries = get_health_measures(
        HEALTH, config, default_queries, selected_attribute_id
    )

    if selected_attribute_id:
        attributes = [
            attribute for attribute in attributes if attribute.get("id") == attribute_id
        ]
    config.update(
        {
            "airflow_run_id": str(kwargs.get("dag_run").run_id),
            "attributes": attributes,
            "measures": measures,
            "measure_level_queries": measure_level_queries,
        }
    )
    queries = queries if queries else {}
    if row_count_query and level == "attribute" and attribute_id:
        row_count_query = get_query_string(
            config, default_queries, row_count_query, is_full_query=True
        )
        row_count_data, native_connection = execute_native_query(
            config, row_count_query, source_connection
        )
        if not source_connection and native_connection:
            source_connection = native_connection
        row_count_data = convert_to_lower(row_count_data)
        row_count = row_count_data.get("total_rows") if row_count_data else 0
        row_count = row_count if row_count else 0
        config.update({"total_records": row_count})
        if attribute:
            attribute.update({"row_count": row_count})
            config.update({"attribute": attribute})
        update_row_count_query = f"""
            update core.attribute set row_count={row_count}
            where asset_id='{asset_id}' and id='{attribute_id}'
        """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            execute_query(connection, cursor, update_row_count_query)
    else:
        total_records = 0
        if attribute:
            total_records = attribute.get("row_count", 0)
            total_records = int(total_records) if total_records else 0
        if not total_records and asset:
            total_records = asset.get("row_count", 0)
            total_records = int(total_records) if total_records else 0
        config.update({"total_records": total_records})

    basic_measures = {}
    source_connection = (
        source_connection
        if connection_type.lower()
        in [ConnectionType.Snowflake.value, ConnectionType.Denodo.value]
        else None
    )
    if queries:
        max_workers = get_max_workers(ConnectionType.Denodo)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        results = []
        if executor:
            futures = [
                executor.submit(
                    execute_queries, config, attribute_id, query, source_connection
                )
                for attribute_id, query in queries.items()
            ]
            results = [future.result() for future in futures]
            executor.shutdown()

        for result in results:
            attribute_id, attribute_metadata = result
            if attribute_metadata:
                basic_measures.update(**attribute_metadata)

    if basic_measures:
        update_attribute_run_id(config)
        save_measures(basic_measures, config)
        update_metrics(basic_measures, config)

    update_scores(config)
    # Check for the alerts for the current run
    config.update({"is_asset": False, "job_type": HEALTH, "measure_id": None})
    check_alerts(config)

    # # Update the threshold value based on the drift config
    update_threshold(config)
    return source_connection