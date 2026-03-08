"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
from uuid import uuid4
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    calculate_weightage_score,
    check_measure_result,
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    delete_metrics
)
from dqlabs.app_constants.dq_constants import PASSED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils import is_scoring_enabled


def activate_enum_measure(
    asset_id: str,
    attribute_id: str,
    is_active: bool,
    connection: object,
    cursor: object,
) -> str:
    """
    Insert new enum measure in core.masure table
    """
    enum_measure_query = f"""
        update core.measure set is_active={is_active}
        where asset_id='{asset_id}' and attribute_id='{attribute_id}'
        and technical_name='enum'
    """
    cursor = execute_query(connection, cursor, enum_measure_query)


def get_enum_attributes(config: dict) -> dict:
    """
    Checks the whether the given asset as defaults enum or not
    """
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    enum_attributes = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct mes.attribute_id, mes.values
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.asset_id='{asset_id}'
            and base.type='enum' and base.category='enum'
        """
        cursor = execute_query(connection, cursor, query_string)
        enum_attributes = fetchall(cursor)
        enum_attributes = {
            attribute.get("attribute_id"): attribute.get("query")
            for attribute in enum_attributes
        }
    return enum_attributes


def validate_enum(enum_measure: dict, enum_values: list) -> list:
    """
    Validates the enum values based on existing enum values
    """
    input_enum_values = enum_measure.get("enum_values", "[]")
    input_enum_values = (
        json.loads(input_enum_values)
        if input_enum_values and isinstance(input_enum_values, str)
        else input_enum_values
    )
    input_enum_values = input_enum_values if input_enum_values else []
    if not input_enum_values:
        return enum_values

    for enum_value in enum_values:
        value = enum_value.get("enum_value")
        existing_enum_value = next(
            (enum for enum in input_enum_values if enum.get("name") == value), None
        )
        if not existing_enum_value:
            enum_value.update({"is_new": True})
            continue
        is_valid = existing_enum_value.get("is_valid", False)
        enum_value.update({"is_valid": is_valid})
    return enum_values


def get_enum_query(config: dict) -> list:
    """
    Returns the list of default enums
    """
    asset_id = config.get("asset_id")
    attribute = config.get("attribute", {})
    attribute_id = attribute.get("id")
    connection = get_postgres_connection(config)
    enum_measure = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id,
            mes.attribute_id,
            mes.technical_name as name,
            mes.values as enum_values
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.asset_id = '{asset_id}'
            and mes.attribute_id = '{attribute_id}'
            and base.category='enum' and
            mes.is_active=True and
            mes.is_delete=False
            base.is_visible=True
        """
        cursor = execute_query(connection, cursor, query_string)
        enum_measure = fetchone(cursor)
    table_name = config.get("table_name")
    attribute_name = attribute.get("name")
    attribute_id = attribute.get("id")

    query_string = ""
    if enum_measure:
        default_queries = config.get("default_queries", {})
        frequency_query = default_queries.get("frequency", {})
        enum_query = frequency_query.get("enum") if frequency_query else ""
        enum_query = enum_query if enum_query else ""
        query_string = prepare_query_string(
            query=enum_query, attribute=attribute_name, table_name=table_name
        )
    return (attribute_id, query_string, enum_measure)


def save_enums(input_enums: dict, result: dict, config: dict):
    """
    Saves the enum into metrics table and postgres
    """
    if not input_enums:
        return
    measure = config.get("measure", {})
    is_positive_impact = measure.get("is_positive", False)
    allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
    is_drift_enabled = measure.get("is_drift_enabled", False)
    attribute = config.get("attribute", {})
    attribute_name = attribute.get("name")
    attribute_id = attribute.get("id")
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    term_id = attribute.get("term_id")
    term_approval_id = attribute.get("term_approval_id")
    executed_query = config.get("query_string")
    executed_query = executed_query if executed_query else ""
    executed_query = executed_query.strip().replace("'", "''")
    if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
        executed_query = executed_query.replace(
            f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
        )
    connection = get_postgres_connection(config)

    enums_metadata = []
    enum_queries = []
    enum_values = result.get(attribute_id, [])
    if term_approval_id and term_id:
        __save_semantic_enum(term_id, enum_values, config)
    total_records = int(attribute.get("row_count", 0)) if attribute else 0
    with connection.cursor() as cursor:
        measure_id = input_enums.get("id")
        # Delete the metrics for the given run id if exists
        delete_metrics(
            config,
            run_id=run_id,
            measure_id=measure_id,
            asset_id=asset_id,
            attribute_id=attribute_id,
        )

        enum_measure_count = 0
        for enum in enum_values:
            enum_count = enum.get("count")
            measure_name = enum.get("enum_value")
            is_positive = enum.get("is_valid", False)
            enums_metadata.append(
                {
                    "name": str(measure_name),
                    "count": enum_count,
                    "id": measure_id,
                    "is_valid": is_positive,
                }
            )

            weightage = measure.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None
            is_archived = False

            if allow_score and total_records and enum_count:
                valid_count = enum_count
                if is_positive:
                    enum_measure_count += enum_count
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
                str(measure_name),
                "attribute",
                str(enum_count),
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
            enum_queries.append(query_param)

        enum_measure_score = None
        if allow_score:
            enum_measure_score = (
                float(enum_measure_count / total_records * 100)
                if enum_measure_count and total_records
                else 0
            )
            enum_measure_score = (
                enum_measure_score if is_positive_impact else (100 - enum_measure_score)
            )
            enum_measure_score = 100 if enum_measure_score > 100 else enum_measure_score
            enum_measure_score = 0 if enum_measure_score < 0 else enum_measure_score
        if enum_queries:
            measure_name = measure.get("name")
            valid_count = enum_measure_count
            invalid_count = total_records - valid_count
            valid_percentage = float(valid_count / total_records * 100)
            invalid_percentage = float(100 - valid_percentage)
            score = enum_measure_score
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
                str(enum_measure_count),
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
            enum_queries.append(query_param)

        pass_criteria_result = check_measure_result(measure, enum_measure_score)
        enum_measure_score = 'null' if enum_measure_score is None else enum_measure_score
        update_measure_score_query = f"""
            update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={enum_measure_score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count}, result = '{pass_criteria_result}'
            where attribute_id='{attribute_id}' and id='{measure_id}'
        """
        cursor = execute_query(connection, cursor, update_measure_score_query)

        # Insert each enum values into a separate metrics
        measures_input = split_queries(enum_queries)
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
                log_error("save enums: inserting new enum metrics", e)

        # Update enum values into enum measure
        if enums_metadata:
            try:
                enum = json.dumps(enums_metadata, default=str)
                query_string = f"""
                    update core.measure
                    set values = '{enum}'
                    where id = '{measure_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
            except Exception as e:
                log_error("save enums: update enum measure", e)

    if enums_metadata:
        __save_enums_metadata(attribute_id, enums_metadata, config)


def __save_enums_metadata(attribute_id: str, enum_input: dict, config: dict):
    """
    Save the enums into metadata tables for the given attribute
    """
    asset_id = config.get("asset_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        attribute_enums = enum_input if enum_input else []
        attribute_enums = json.dumps(attribute_enums, default=str)
        attribute_enums = attribute_enums.replace("'", "''")
        query_string = f"""
            update core.attribute set enums = '{attribute_enums}'
            where asset_id='{asset_id}' and id='{attribute_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def __save_semantic_enum(term_id: str, enum_values: list, config: dict) -> None:
    enum_values = [
        {
            "value": enum.get("enum_value"),
            "is_accept": False,
            "is_decline": False,
            "is_valid": enum.get("is_valid", True),
        }
        for enum in enum_values
        if enum.get("is_new", False)
    ]
    if enum_values:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"select enum from core.terms where id='{term_id}'"
            cursor = execute_query(connection, cursor, query_string)
            existing_enums = fetchone(cursor)
            existing_enums = existing_enums.get("enum")
            enum_list = [*existing_enums, *enum_values]
            unique_enums = [dict(t) for t in {tuple(d.items()) for d in enum_list}]
            unique_enums = json.dumps(unique_enums, default=str)
            unique_enums = unique_enums.replace("'", "\\\\'")
            query_string = f"""
                update core.terms set enum = '{unique_enums}'
                where id='{term_id}'
            """
            cursor = execute_query(connection, cursor, query_string)


def execute_enums(config: dict) -> None:
    attribute_id, query_string, enum_measure = get_enum_query(config)
    source_connection = None

    attribute_enums = {}
    if query_string:
        config.update(
            {
                "sub_category": "FREQUENCY",
                "query_string": query_string,
            }
        )
        enum_result, native_connection = execute_native_query(
            config, query_string, source_connection, is_list=True
        )
        if not source_connection:
            source_connection = native_connection
        if enum_result:
            enum_result = convert_to_lower(enum_result)
            if enum_measure:
                enum_result = validate_enum(enum_measure, enum_result)
            attribute_enums.update({attribute_id: enum_result})

    if attribute_enums:
        save_enums(enum_measure, attribute_enums, config)
