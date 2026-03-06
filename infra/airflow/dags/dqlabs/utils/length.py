"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
from uuid import uuid4
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    delete_metrics
)
from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries
from dqlabs.app_constants.dq_constants import PASSED
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    calculate_weightage_score,
    check_measure_result,
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils import is_scoring_enabled


def validate_length(length_frequency: list, min_length: int, max_length: int) -> dict:
    """
    Validate the length with min and max length
    """
    for frequency in length_frequency:
        length = int(frequency.get("length"))
        is_valid = bool(length >= min_length and length <= max_length)

        # consider valid if the values for both min & max length are not available
        if not min_length and not max_length:
            is_valid = True
        frequency.update({"is_valid": is_valid})
    return length_frequency


def is_active_measure(config: dict) -> list:
    """
    Returns true if the length measure is active
    """
    asset_id = config.get("asset_id")
    attribute = config.get("attribute", {})
    attribute_id = attribute.get("id")
    connection = get_postgres_connection(config)
    length_measure = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.* from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            where mes.asset_id = '{asset_id}'
            and mes.attribute_id = '{attribute_id}'
            and base.category='length' and
            mes.is_active=True and mes.is_delete=False
        """
        cursor = execute_query(connection, cursor, query_string)
        length_measure = fetchone(cursor)
    return bool(length_measure)


def execute_length_distbution(config: dict, default_queries: dict) -> None:
    """
    Executes the length distubution for the given attribute
    """
    queries = default_queries.get("frequency", {})
    measure = config.get("measure")
    attribute = config.get("attribute")
    category = measure.get("category")
    table_name = config.get("table_name")
    attribute_name = attribute.get("name")
    attribute_id = attribute.get("id")
    min_length = attribute.get("min_length", 0)
    min_length = int(min_length) if min_length else 0
    max_length = attribute.get("max_length", 0)
    max_length = int(max_length) if max_length else 0
    query = queries.get(category)
    is_active = is_active_measure(config)
    if not is_active:
        return

    length_frequency = []
    if query:
        query = prepare_query_string(
            query, attribute=attribute_name, table_name=table_name
        )
        source_connection = None
        config.update(
            {
                "sub_category": "FREQUENCY",
                "query_string": query,
            }
        )
        length_frequency, native_connection = execute_native_query(
            config, query, source_connection, is_list=True
        )
        if not source_connection and native_connection:
            source_connection = native_connection
        length_frequency = convert_to_lower(length_frequency)

    if length_frequency:
        length_frequency = validate_length(length_frequency, min_length, max_length)
        save_length_metrics(config, length_frequency)
        save_length_metadata(attribute_id, length_frequency, config)


def save_length_metrics(config: dict, length_frequency: list):
    """
    Stores the result into postgersql metrics table
    """
    attribute = config.get("attribute", {})
    measure = config.get("measure", {})
    measure_id = measure.get("id")
    is_positive_impact = measure.get("is_positive", False)
    allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
    is_drift_enabled = measure.get("is_drift_enabled", False)
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    executed_query = config.get("query_string")
    executed_query = executed_query if executed_query else ""
    executed_query = executed_query.strip().replace("'", "''")
    if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
        executed_query = executed_query.replace(
            f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
        )
    connection = get_postgres_connection(config)

    total_records = int(attribute.get("row_count", 0))
    with connection.cursor() as cursor:
        attribute_id = attribute.get("id")

        delete_metrics(
            config,
            run_id=run_id,
            measure_id=measure_id,
            attribute_id=attribute_id,
        )

        length_queries = []
        attribute_name = attribute.get("name")

        length_measure_count = 0
        for frequency in length_frequency:
            length = frequency.get("length")
            count = frequency.get("count")
            is_positive = frequency.get("is_valid", False)
            count = count if count else 0

            weightage = measure.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None
            is_archived = False

            if allow_score and total_records and count:
                valid_count = count
                if is_positive:
                    length_measure_count += count
                valid_count = (
                    valid_count if is_positive else (total_records - valid_count)
                )
                invalid_count = total_records - valid_count
                valid_percentage = float(valid_count / total_records * 100)
                invalid_percentage = float(100 - valid_percentage)
                score = valid_percentage
                score = calculate_weightage_score(score, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                # score = round(score, 2)

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
                str(length),
                "attribute",
                str(count),
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
                False,
                True,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            length_queries.append(query_param)

        length_measure_score = None
        if allow_score:
            length_measure_score = (
                round(float(length_measure_count / total_records * 100), 2)
                if length_measure_count and total_records
                else 0
            )
            length_measure_score = (
                length_measure_score
                if is_positive_impact
                else (100 - length_measure_score)
            )
            length_measure_score = (
                100 if length_measure_score > 100 else length_measure_score
            )
            length_measure_score = (
                0 if length_measure_score < 0 else length_measure_score
            )

        if length_queries:
            measure_name = measure.get("name")
            valid_count = length_measure_count
            invalid_count = total_records - valid_count
            valid_percentage = float(valid_count / total_records * 100)
            invalid_percentage = float(100 - valid_percentage)
            score = length_measure_score
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
                "attribute",
                str(length_measure_count),
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
                attribute.get("parent_attribute_id", None),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            length_queries.append(query_param)

        pass_criteria_result = check_measure_result(measure, length_measure_score)
        length_measure_score = 'null' if length_measure_score is None else length_measure_score
        update_measure_score_query = f"""
            update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={length_measure_score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count} , result = '{pass_criteria_result}'
            where attribute_id='{attribute_id}' and id='{measure_id}'
        """
        cursor = execute_query(connection, cursor, update_measure_score_query)

        measures_input = split_queries(length_queries)
        for input_values in measures_input:
            try:
                query_input = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,  is_active, is_delete, parent_attribute_id, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error(
                    "save length distribution: inserting new length distribution metrics",
                    e,
                )


def save_length_metadata(attribute_id: str, length_frequency: dict, config: dict):
    """
    Save the length frequency into metadata tables for the given attribute
    """
    asset_id = config.get("asset_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        length_data = length_frequency if length_frequency else []
        length_data = json.dumps(length_data, default=str)
        query_string = f"""
            update core.attribute set length_frequency = '{length_data}'
            where asset_id='{asset_id}' and id='{attribute_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
