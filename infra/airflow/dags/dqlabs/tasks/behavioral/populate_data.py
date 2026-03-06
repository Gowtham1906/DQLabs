"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
from datetime import datetime
from uuid import uuid4
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    get_query_string,
)
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    get_attribute_label,
    remove_decimal_categorical_column,
    parse_numeric_value,
)
from dqlabs.app_helper.db_helper import execute_query, split_queries
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.utils.behavioral import get_behavioral_table_name
from dqlabs.app_constants.dq_constants import PASSED, BEHAVIORAL
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.request_queue import update_queue_detail_query
from dqlabs.utils.drift import get_previous_depth
from dqlabs.utils import is_scoring_enabled


def prepare_table(measure: dict, config: dict, input_data: dict) -> None:
    """
    Creates a table for given behavioral measure
    """
    behavioral_table_name = get_behavioral_table_name(measure)
    columns = []
    for key, value in input_data.items():
        column_name = get_attribute_label(key, False)
        postgres_type = "varchar"
        if isinstance(value, int):
            postgres_type = "bigint"
        elif isinstance(value, float):
            postgres_type = "float"
        elif isinstance(value, datetime):
            postgres_type = "timestamp with time zone"
        else:
            postgres_type = "varchar"
        columns.append(f""" {column_name} {postgres_type} """)

    column_names = ", \n".join(columns)
    if not column_names:
        raise Exception("Cannot create a table with empty column name.")

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "behavioral"."{behavioral_table_name}" (
            {column_names},
            key varchar,
            organization_id uuid,
            connection_id uuid,
            asset_id uuid,
            attribute_id uuid,
            measure_id uuid,
            run_id uuid,
            created_date timestamp,
            airflow_run_id varchar,
            slice_key uuid,
            allow_score boolean,
            is_drift_enabled boolean,
            query varchar
        )
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, create_table_query)


def prepare_aggregation_query(measure: dict, config: dict) -> None:
    """
    Populate aggregation query for the given behavioral measure
    """
    default_slice_by = "day"
    default_slice = "1"
    default_day_interval = "7"
    connection_type = config.get("connection_type", "").lower()
    default_queries = config.get("default_queries", {})
    default_queries = default_queries if default_queries else {}
    behavioral_queries = default_queries.get("behavioral", {})
    slice_query = behavioral_queries.get("slice_query")
    max_query = behavioral_queries.get("max_query")
    default_filter = behavioral_queries.get("default_filter")
    last_date_filter = behavioral_queries.get("date_filter")
    aggregation_types = behavioral_queries.get("aggregation")
    aggregation_types = aggregation_types if aggregation_types else {}
    date_format = behavioral_queries.get("format_date")
    date_format = date_format if date_format else ""
    connection_type = config.get("connection_type")
    rule_config = measure.get("properties")
    if not rule_config:
        return

    # Behavioral reset query when depth changes
    asset = config.get("asset", {})
    asset = asset if asset else {}
    asset_id = config.get("asset_id")
    incremental_config = config.get("incremental_config", {})
    job_type = config.get("job_type")
    is_behavioral = (job_type == BEHAVIORAL) or (
        measure and measure.get("category") == BEHAVIORAL
    )
    current_depth = None
    previous_depth = None
    if incremental_config and is_behavioral:
        depth = incremental_config.get("depth", {})
        depth = depth if depth else {}
        depth_value = depth.get("value", 0)
        current_depth = int(depth_value) if depth_value else 0
        # previous_depth = get_previous_depth(config, asset_id)

    rule_config = (
        json.loads(rule_config) if isinstance(rule_config, str) else rule_config
    )
    last_processed_date = rule_config.get("last_processed_date")
    date_filter = last_date_filter if last_processed_date else default_filter
    if (current_depth and previous_depth) and (incremental_config and is_behavioral):
        if current_depth != previous_depth:
            date_filter = (
                default_filter  # Override last_processed_date if depth changes
            )
    if last_processed_date:
        date_filter = date_filter.replace("<max_date_value>", str(last_processed_date))
    date_filter = f" AND {date_filter} "

    aggregation_type = rule_config.get("aggregation_type", "Avg")
    aggregation_type = aggregation_type if aggregation_type else "Avg"
    aggregation_type = aggregation_type.lower()
    aggregate_function = aggregation_types.get(aggregation_type)
    aggregate_function = aggregate_function if aggregate_function else ""

    # attributes = rule_config.get("attributes", [])
    # DQL-2603 - Adding categoriacal and numerical columns seperately for behavioral query
    category_group_attributes = rule_config.get("attributes", [])
    aggregator_attributes = rule_config.get(
        "aggregator_attributes", []
    )  # only numerical columns allowed

    slice_interval = rule_config.get("slice", default_slice)
    slice_interval = str(slice_interval) if slice_interval else default_slice
    slice_by = rule_config.get("slice_by", default_slice_by)
    slice_by = slice_by if slice_by else default_slice_by
    day_interval = rule_config.get("day_interval", default_day_interval)
    # if the interval is in months/years convert it into days
    interval_period = rule_config.get("interval", None)
    interval_period = str(interval_period).lower() if interval_period else None
    """
    months: avg taken is 30 days per month
    years: avg taken is 365 days per year
    """
    multipliers = {"months": 30, "years": 365}
    if interval_period in multipliers:
        day_interval = int(day_interval) * multipliers[interval_period]
    day_interval = str(day_interval) if day_interval else default_day_interval
    if not (aggregation_type and category_group_attributes):
        return

    columns = []
    datatype_key = "data_type"
    if category_group_attributes and aggregator_attributes:
        categorical_attributes = list(
            filter(
                lambda attribute: (attribute.get(datatype_key) or "").lower() == "text"
                or (attribute.get(datatype_key) or "").lower() in ["integer", "numeric"],
                category_group_attributes,
            )
        )
    elif category_group_attributes and not aggregator_attributes:
        categorical_attributes = list(
            filter(
                lambda attribute: (attribute.get(datatype_key) or "").lower() == "text",
                category_group_attributes,
            )
        )
    categorical_attributes = [
        attribute.get("name") for attribute in categorical_attributes
    ]
    categorical_attributes = get_attribute_names(
        connection_type, categorical_attributes
    )
    columns.extend(categorical_attributes)
    categorical_attribute = ", ".join(categorical_attributes)

    if category_group_attributes and aggregator_attributes:
        numerical_attributes = list(
            filter(
                lambda attribute: attribute.get("data_type", "").lower()
                in ["integer", "numeric"],
                aggregator_attributes,
            )
        )
    elif category_group_attributes and not aggregator_attributes:
        numerical_attributes = list(
            filter(
                lambda attribute: attribute.get("data_type", "").lower()
                in ["integer", "numeric"],
                category_group_attributes,
            )
        )
    numerical_attributes = [attribute.get("name") for attribute in numerical_attributes]
    numerical_attributes = get_attribute_names(connection_type, numerical_attributes)
    columns.extend(numerical_attributes)
    numerical_attribute = ", ".join(numerical_attributes)

    aggregation_attributes = []
    for attribute in numerical_attributes:
        # attribute_lable = get_attribute_label(f"""{aggregate_function}___{attribute}""", False)
        if connection_type == ConnectionType.DB2IBM.value:
            aggregation = (
                f""" {aggregate_function}(cast({attribute} as double)) as {attribute}"""
            )
        else:
            aggregation = f""" {aggregate_function}({attribute}) as {attribute}"""
        if not aggregate_function:
            aggregation = f""" {attribute} """
        aggregation_attributes.append(aggregation)
    aggregation_expression = ", ".join(aggregation_attributes)

    time_attributes = list(
        filter(
            lambda attribute: "date" in (attribute.get(datatype_key) or "").lower()
            or "time" in (attribute.get(datatype_key) or "").lower(),
            category_group_attributes,
        )
    )
    time_attributes = [attribute.get("name") for attribute in time_attributes]
    time_attributes = get_attribute_names(connection_type, time_attributes)
    columns.extend(time_attributes)
    time_attribute = ", ".join(time_attributes)

    formatted_time_attributes = []
    if date_format and time_attributes:
        time_columns = [time_attributes[0]]
        for attribute in time_columns:
            formatted_attribute = date_format.replace("<date_colum>", attribute)
            formatted_time_attributes.append(formatted_attribute)
    formatted_time_attribute = ", ".join(formatted_time_attributes)

    columns_to_pull = ", ".join(columns)
    group_by_column = categorical_attribute
    if aggregation_type.lower() == "value" and numerical_attribute:
        group_by_column = f"{group_by_column}, " if group_by_column else ""
        group_by_column = f"{group_by_column}{numerical_attribute}"

    if connection_type != ConnectionType.Snowflake.value:
        slice_query = slice_query.get(slice_by)

    slice_query = slice_query.replace("<date_filter>", date_filter)
    categorical_attribute = (
        f", {categorical_attribute}" if categorical_attribute else ""
    )
    aggregation_expression = (
        f", {aggregation_expression}" if aggregation_expression else ""
    )
    group_by_column = f", {group_by_column}" if group_by_column else ""

    query_string = (
        slice_query.replace("<columns>", columns_to_pull)
        .replace("<categorical_column>", categorical_attribute)
        .replace("<numerical_column>", numerical_attribute)
        .replace("<date_colum>", time_attribute)
        .replace("<formatted_date_colum>", formatted_time_attribute)
        .replace("<aggregate_expression>", aggregation_expression)
        .replace("<slice_interval>", slice_interval)
        .replace("<slice_by>", slice_by)
        .replace("<day_interval>", day_interval)
        .replace("<group_by_column>", group_by_column)
        .replace("<limit_condition>", "")
    )

    table_name = config.get("table_name")
    
    if connection_type == ConnectionType.MongoDB.value:
        max_date_query = ""
        if time_attributes:
            max_query = max_query.replace("<date_colum>", time_attributes[0])
            max_date_query = max_query
    else:
        query_string = prepare_query_string(query_string, table_name=table_name)
        query_string = get_query_string(
            config, default_queries, query_string, is_full_query=True
        )

        max_date_query = ""
        if time_attributes:
            max_query = max_query.replace("<date_colum>", time_attributes[0])
            max_date_query = prepare_query_string(max_query, table_name=table_name)
            max_date_query = get_query_string(
                config, default_queries, max_date_query, is_full_query=True
            )

    return query_string, max_date_query


def insert_aggregated_data(aggregated_data: list, measure: dict, config: dict) -> None:
    """
    Insert the aggregated data into postgres
    """
    attribute = config.get("attribute", {})
    attribute_id = attribute.get("id")
    attribute_name = attribute.get("name")
    asset_id = config.get("asset_id")
    asset = config.get("asset")
    asset = asset if asset else {}
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    measure_id = measure.get("id")
    measure_name = measure.get("name")
    rule_config = measure.get("properties")
    allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
    is_drift_enabled = measure.get("is_drift_enabled", False)
    level = measure.get("level", "attribute")
    level = level if level else "attribute"
    if not rule_config:
        return

    rule_config = (
        json.loads(rule_config) if isinstance(rule_config, str) else rule_config
    )
    # attributes = rule_config.get("attributes", [])
    # DQL-2603 - Adding categoriacal and numerical columns seperately for behavioral query
    category_group_attributes = rule_config.get("attributes", [])
    aggregator_attributes = rule_config.get(
        "aggregator_attributes", []
    )  # only numerical columns allowed

    slice_by = rule_config.get("slice_by", "day")
    slice_by = slice_by if slice_by else "day"
    columns = list(dict(aggregated_data[0]).keys())
    columns = columns if columns else []

    categorical_attributes = list(
        filter(
            lambda attribute: attribute.get("data_type", "").lower() == "text"
            or attribute.get("data_type", "").lower() in ["integer", "numeric"],
            category_group_attributes,
        )
    )
    categorical_attributes = [
        get_attribute_label(str(attribute.get("name")), False)
        for attribute in categorical_attributes
    ]

    numerical_attributes = list(
        filter(
            lambda attribute: attribute.get("data_type", "").lower()
            in ["integer", "numeric"],
            aggregator_attributes,
        )
    )
    numerical_attributes = [
        get_attribute_label(str(attribute.get("name")), False)
        for attribute in numerical_attributes
    ]

    table_name = get_behavioral_table_name(measure)
    connection = get_postgres_connection(config)
    executed_query = config.get("query_string")
    executed_query = executed_query if executed_query else ""
    executed_query = executed_query.strip().replace("'", "''")
    if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
        executed_query = executed_query.replace(
            f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
        )
    with connection.cursor() as cursor:
        query_input_params = []
        for data in aggregated_data:
            query_input = [
                organization_id,
                connection_id,
                asset_id,
                attribute_id,
                measure_id,
                run_id,
                airflow_run_id,
                executed_query,
                allow_score,
                is_drift_enabled,
                str(uuid4()),
            ]
            key_columns = []
            column_values = []
            for column in columns:
                value = data.get(column)
                if column in numerical_attributes:
                    if value and not isinstance(value, int):
                        value = round(float(value), 2)
                    value = parse_numeric_value(value)
                column_values.append(value)
                if column in categorical_attributes:
                    key_columns.append(str(value))
            key_columns = remove_decimal_categorical_column(key_columns)
            behavioral_key = "___".join(key_columns)
            if behavioral_key:
                behavioral_key = get_attribute_label(
                    behavioral_key, include_separator=False, is_lower=False, is_behavioral=True
                )
            behavioral_key = behavioral_key if behavioral_key else ""
            query_input.append(behavioral_key)
            query_input.extend(column_values)

            row_input = tuple(query_input)
            input_literals = ", ".join(["%s"] * len(row_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)",
                row_input,
            ).decode("utf-8")
            query_input_params.append(query_param)

        # delete if the run_id is already exists
        query_string = (
            f"""delete from "behavioral".{table_name} where run_id='{run_id}' """
        )
        cursor = execute_query(connection, cursor, query_string)

        columns.append("created_date")
        source_column = ", ".join(f'"{get_attribute_label(col, False)}"' for col in columns)
        source_column = f", {source_column} " if source_column else ""
        behavioral_input = split_queries(query_input_params, 1000)
        for input_values in behavioral_input:
            try:
                query_input = ",".join(input_values)
                insert_query = f"""
                    insert into "behavioral".{table_name} (
                        organization_id, connection_id, asset_id, attribute_id, measure_id, run_id, airflow_run_id, query, allow_score, is_drift_enabled, slice_key, key {source_column} 
                    ) values {query_input}
                """
                cursor = execute_query(connection, cursor, insert_query)
            except Exception as e:
                log_error("Behavioral - Populate Data : insert aggregated data", e)

        weightage = 0
        total_records = 0
        valid_count = 0
        invalid_count = 0
        valid_percentage = 0
        invalid_percentage = 0
        score = None
        value = ""
        attribute_name = attribute_name if attribute_name else ""
        is_archived = False

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
            f"({input_literals}, CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")
        try:
            measure_query_string = f"""
                insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                query, allow_score, is_drift_enabled, is_measure, is_active, is_delete, parent_attribute_id, created_date)
                values {query_param}
            """
            cursor = execute_query(connection, cursor, measure_query_string)

            if attribute_id:
                update_measure_lastrun_query = f"""
                        update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP 
                        where attribute_id='{attribute_id}' and id='{measure_id}'
                """
                cursor = execute_query(connection, cursor, update_measure_lastrun_query)
        except Exception as e:
            log_error("Behavioral - Insert behavioral measure into metrics ", e)


def update_last_processed_date(max_date: dict, measure: dict, config: dict) -> None:
    """
    Insert the aggregated data into postgres
    """
    base_measure_id = measure.get("base_measure_id")
    rule_config = measure.get("properties")
    if not rule_config:
        return

    last_processed_date = max_date.get("max_date") if max_date else None
    if not last_processed_date:
        return

    last_processed_date = str(last_processed_date)
    if ":" not in last_processed_date:
        last_processed_date = f"{last_processed_date} 00:00:00"
    if "." in last_processed_date:
        last_processed_date = str(last_processed_date.split(".")[0])
    if "T" in last_processed_date:
        last_processed_date = str(last_processed_date).replace("T", " ")
    try:
        parsed_date = datetime.fromisoformat(last_processed_date)
    except ValueError:
        parsed_date = datetime.strptime(last_processed_date, "%Y-%m-%d %H:%M:%S")

    rule_config = (
        json.loads(rule_config) if isinstance(rule_config, str) else rule_config
    )
    rule_config.update({"last_processed_date": parsed_date})

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        config_data = json.dumps(rule_config, default=str)
        query_string = f"""
            update core.base_measure set properties = '{config_data}'
            where id='{base_measure_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def populate_aggregated_data(measure: dict, config: dict) -> None:
    """
    Populate the aggregated data based on given behavioral measure
    """
    query_string, max_date_query = prepare_aggregation_query(measure, config)
    if not query_string:
        raise Exception("Undefined query string.")

    update_queue_detail_query(config, query_string)
    source_connection = None
    aggregated_data = []
    max_date = None

    # Printing the query for debugging. Don't remove this print statement.
    config.update({"query_string": query_string})
    aggregated_data, native_connection = execute_native_query(
        config, query_string, source_connection, is_list=True
    )
    if not source_connection and native_connection:
        source_connection = native_connection
    aggregated_data = convert_to_lower(aggregated_data)

    if max_date_query:
        # Printing the query for debugging. Don't remove this print statement.
        max_date, native_connection = execute_native_query(
            config, max_date_query, source_connection
        )
        max_date = convert_to_lower(max_date)

    if aggregated_data:
        prepare_table(measure, config, aggregated_data[0])
        insert_aggregated_data(aggregated_data, measure, config)
        update_last_processed_date(max_date, measure, config)

        # Check for the alerts for the current run
        check_alerts(config)

        # # Update the threshold value based on the drift config
        update_threshold(config)
