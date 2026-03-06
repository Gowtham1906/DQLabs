from uuid import uuid4
from dateutil.parser import parse

from dqlabs.app_helper.dag_helper import get_postgres_connection, execute_query, delete_metrics
from dqlabs.app_helper.db_helper import execute_query, fetchall, split_queries, fetchone
from dqlabs.app_constants.dq_constants import PASSED
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold

from dqlabs.app_helper.log_helper import log_error

TASK_CONFIG = None
CONFIG_DETAIL = None

def execute_pipeline_measure(config: dict, type: dict, config_detail: dict, task_info: dict = {}, job_name: str = None, task_name: str = None):
    try:
        global TASK_CONFIG
        global CONFIG_DETAIL
        TASK_CONFIG = config
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = config.get("connection", {})
        credentials = connection.get("credentials")
        task_id = task_info.get("task_id")
        CONFIG_DETAIL = config_detail if config_detail else {}
        
        measures = get_pipeline_measures(asset_id, task_id)
        measures = validate_pipeline_measure(credentials, measures)
        metric_list = []

        # Delete Exisiting Metrics
        if measures:
            delete_existing_metrics(measures)

        # Execute Measures
        for measure in measures:
            execution_result = execute_measure(measure)
            if execution_result:
                if isinstance(execution_result, list):
                    metric_list.extend(execution_result)
                else:
                    metric_list.append(execution_result)

        if metric_list:
            prepare_insert_metrics(metric_list)
        if measures:
            update_pipeline_run(type, asset_id, task_id)
        if task_id:
            TASK_CONFIG.update({"is_pipeline_task": True, "pipeline_task_id": task_id})
        else:
            TASK_CONFIG.update({"is_asset_level": True, "is_asset": True})
        
        # Add pipeline context (job_name and task_name) to config for LLM alert message generation
        if job_name:
            TASK_CONFIG.update({"pipeline_job_name": job_name})
        if task_name:
            TASK_CONFIG.update({"pipeline_task_name": task_name})
        
        if measures:
            check_alerts(TASK_CONFIG)
            update_threshold(TASK_CONFIG)
    except Exception as e:
        log_error("Execute Measure", e)


def execute_measure(measure: dict):
    name = measure.get("name").lower()
    execution_result = None
    value = None
    if name == "latency":
        value = execute_latency_measure()
    elif name == "delay":
        value = execute_delay_measure()
    elif name == "change_volume":
        value = execute_volume_measure()
    if value is not None:
        if isinstance(value, list):
            execution_result = value
            execution_result = [{**measure, **metric} for metric in execution_result]
        else:
            execution_result = measure
            execution_result.update({"value": value})
    return execution_result


def execute_latency_measure():
    """
    Retrieves the Latency duration value from the global CONFIG_DETAIL dictionary.

    Returns:
        The value associated with the "duration" key in CONFIG_DETAIL if it exists, otherwise None.
    """
    value = None
    if CONFIG_DETAIL:
        value = CONFIG_DETAIL.get("duration")
    return value

def execute_delay_measure():
    """
    Calculates the delay measure as the time difference in seconds between the last and previous run dates.

    Returns:
        float or str: The time difference in seconds between 'last_run_date' and 'previous_run_date' if both are present and valid.
                      Returns "NA" if either date is missing. Returns None if CONFIG_DETAIL is not set.

    Notes:
        - Expects 'CONFIG_DETAIL' to be a dictionary containing 'last_run_date' and 'previous_run_date' keys.
        - Dates should be in the format "%Y-%m-%dT%H:%M:%S.%f%z" if provided as strings.
    """
    value = None
    if CONFIG_DETAIL:
        last_run_date = CONFIG_DETAIL.get("last_run_date")
        previous_run_date = CONFIG_DETAIL.get("previous_run_date")
        if last_run_date and previous_run_date:
            if isinstance(last_run_date, str):
                last_run_date = parse(last_run_date)
            if isinstance(previous_run_date, str):
                previous_run_date = parse(previous_run_date)
            value = (last_run_date - previous_run_date).total_seconds()
        else:
            value = 0
    return value


def execute_volume_measure():
    """
    Generates a list of volume measurement dictionaries based on configuration details.

    This function checks if the global CONFIG_DETAIL dictionary indicates that rows have been written
    (via the "is_row_written" key). If so, it collects information about the number of rows affected,
    inserted, updated, and deleted, and returns this data as a list of dictionaries. Each dictionary
    contains the value, a name describing the metric, and a boolean indicating if it is a primary measure.

    Returns:
        list[dict] or None: A list of dictionaries with volume measurement details if "is_row_written" is True,
        otherwise None. Each dictionary contains:
            - "value" (int): The metric value (e.g., number of rows affected).
            - "name" (str): The name of the metric ("volume", "Insert", "Update", "Delete").
            - "is_measure" (bool): True if this is the primary measure ("volume"), False otherwise.
    """
    value = None
    if CONFIG_DETAIL and CONFIG_DETAIL.get("is_row_written"):
        rows_affected = CONFIG_DETAIL.get("rows_affected", 0)
        rows_inserted = CONFIG_DETAIL.get("rows_inserted")
        rows_updated = CONFIG_DETAIL.get("rows_updated")
        rows_deleted = CONFIG_DETAIL.get("rows_deleted")
        value = []
        value.append({"value": rows_affected, "name": "change_volume", "is_measure": True})
        if rows_inserted is not None:
            value.append({"value": rows_inserted, "name": "Insert", "is_measure": False})
        if rows_updated is not None:
            value.append({"value": rows_updated, "name": "Update", "is_measure": False})
        if rows_deleted is not None:
            value.append({"value": rows_deleted, "name": "Delete", "is_measure": False})
    return value


def get_pipeline_measures(asset_id: str, task_id: str = None) -> list:
    """
    Fetches and returns a list of pipeline measures associated with a given asset ID.

    Args:
        asset_id (str): The unique identifier of the asset for which pipeline measures are to be retrieved.

    Returns:
        list: A list of dictionaries, each representing a pipeline measure with its associated attributes.

    Raises:
        Exception: If there is an error connecting to the database or executing the query.

    Note:
        - The function retrieves only active and non-deleted measures of type 'pipeline' for the specified asset.
        - The returned list may be empty if no matching measures are found.
    """

    connection = get_postgres_connection(TASK_CONFIG)
    measures = []
    task_condition = f" and mes.task_id ='{str(task_id)}'" if task_id else " and mes.task_id is null"
    with connection.cursor() as cursor:
        query_string = f"""
            select mes.id as id, base.id as base_measure_id, base.technical_name as name, base.name as measure_name,base.type, 
            base.level, base.category, base.derived_type, mes.task_id,
            mes.allow_score, mes.is_drift_enabled,  mes.asset_id, mes.is_positive, mes.drift_threshold,
            mes.weightage, mes.enable_pass_criteria, mes.pass_criteria_threshold, mes.pass_criteria_condition
            from core.measure as mes
            join core.base_measure as base on base.id = mes.base_measure_id
            where mes.is_active=true and mes.is_delete=false
            and mes.asset_id='{str(asset_id)}' {task_condition}
        """
        cursor.execute(query_string)
        measures = fetchall(cursor)
        measures = measures if measures else []
    return measures

def validate_pipeline_measure(credentials: dict, measures: list)-> list:
    connection_config = credentials.get("metadata", {})
    runs = connection_config.get("runs", False)
    if not runs:
        measures = []
    return measures
        

def prepare_insert_metrics(execution_results: list):
    run_id = TASK_CONFIG.get("run_id")
    airflow_run_id = TASK_CONFIG.get("airflow_run_id")
    connection_id = TASK_CONFIG.get("connection_id")
    organization_id = TASK_CONFIG.get("organization_id")
    measure_input_values = []
    connection = get_postgres_connection(TASK_CONFIG)
    with connection.cursor() as cursor:
        for result in execution_results:
            value = str(result.get("value"))
            measure_name = result.get("name")
            base_measure_name = result.get("measure_name")
            measure_id = result.get("id")
            asset_id = result.get("asset_id")
            level = "task" if  result.get("task_id") else "asset"
            task_id = result.get("task_id")
            is_drift_enabled = result.get("is_drift_enabled", False)
            is_measure = result.get("is_measure", True)
            weightage = result.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None
            is_archived = False
            query_input = (
                str(uuid4()),
                organization_id,
                connection_id,
                asset_id,
                task_id,
                measure_id,
                run_id,
                airflow_run_id,
                "",
                measure_name,
                base_measure_name,
                level,
                str(value),
                weightage,
                0,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                PASSED,
                is_archived,
                "",
                False,
                is_drift_enabled,
                is_measure,
                True,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            measure_input_values.append(query_param)
            update_measure_query = f"""
                update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, 
                  score=null, failed_rows=null, row_count = null, valid_rows = null,
                  invalid_rows = null
                where id='{measure_id}'
            """
            cursor = execute_query(connection, cursor, update_measure_query)

        measures_input = split_queries(measure_input_values)
        for input_values in measures_input:
            try:
                query_input = ",".join(input_values)
                metric_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, task_id,
                    measure_id, run_id, airflow_run_id, attribute_name, measure_name, base_measure_name, level, value, 
                    weightage, total_count,  valid_count, invalid_count, valid_percentage, invalid_percentage, 
                    score, status, is_archived, query, allow_score, is_drift_enabled, is_measure,  is_active,
                    is_delete, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, metric_insert_query)
            except Exception as e:
                log_error("Extract pipeline : inserting new metrics", e)
    

def update_pipeline_run(type: str, asset_id: str, task_id: str = None):
    run_id = TASK_CONFIG.get("run_id")

    connection = get_postgres_connection(TASK_CONFIG)
    with connection.cursor() as cursor:
        if type == "asset":
            query_string = f"""
                update core.asset set last_run_id='{run_id}'
                where id='{asset_id}'
            """
        else:
            query_string = f"""
                update core.pipeline_tasks set last_run_id='{run_id}'
                where id='{task_id}'
            """
        cursor = execute_query(connection, cursor, query_string)

def create_pipeline_task_measures(config: dict, tasks: list):
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    base_measures = get_base_measures(config)
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        for task in tasks:
            existing_measures = f"select count(id) from core.measure where task_id='{task}'"
            cursor = execute_query(connection, cursor, existing_measures)
            existing_measure = fetchone(cursor)
            existing_measure = existing_measure.get("count") if existing_measure else 0
            if not existing_measure:
                measure_input_values = []
                for measure in base_measures:
                    query_input = (
                        str(uuid4()),
                        measure.get("technical_name"),
                        measure.get("threshold"),
                        measure.get("is_drift_enabled"),
                        measure.get("is_positive"),
                        measure.get("allow_score"),
                        measure.get("is_active"),
                        measure.get("allow_export"),
                        measure.get("is_delete"),
                        True,
                        config.get("organization_id"),
                        asset_id,
                        measure.get("id"),
                        measure.get("dimension_id"),
                        connection_id,
                        "Verified",
                        measure.get("is_aggregation_query"),
                        task,
                        100,
                        False,
                        100,
                        "value",
                        ">=",
                        True,
                        0,
                        0
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")
                    measure_input_values.append(query_param)
                measures_input = split_queries(measure_input_values)
                for input_values in measures_input:
                    try:
                        query_input = ",".join(input_values)
                        measure_insert_query = f"""
                            insert into core.measure (id, technical_name, threshold, is_drift_enabled, is_positive,
                            allow_score, is_active, is_export, is_delete, is_auto, organization_id, asset_id, base_measure_id, 
                            dimension_id, connection_id, status, is_aggregation_query, task_id,
                            weightage, enable_pass_criteria,pass_criteria_threshold,pass_criteria_condition,is_validated,
                            alerts, issues, created_date)
                            values {query_input}
                        """
                        cursor = execute_query(connection, cursor, measure_insert_query)
                    except Exception as e:
                        log_error("Create pipeline measure: inserting new measure", e)

def get_base_measures(config: dict):
    connection = config.get("connection")
    connection_type = connection.get("type")
    connection = get_postgres_connection(config)
    measures = []
    with connection.cursor() as cursor:
        measure_condition = ""
        if connection_type in ["airflow", "databricks"]:
            measure_condition = " and technical_name in ('latency', 'delay')"
        query_string = f"""
            select id, level, technical_name, threshold, allow_score, is_drift_enabled, is_positive, is_active,
            allow_export, is_delete, organization_id, dimension_id, is_aggregation_query
            from core.base_measure where type = 'pipeline' and is_active=true {measure_condition}
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
        measures = measures if measures else []
    return measures


def get_pipeline_tasks(config: dict):
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    tasks = []
    with connection.cursor() as cursor:
        query_string = f"""select distinct id as task_id, source_id, name from core.pipeline_tasks 
        where asset_id='{asset_id}' and is_active=true and is_selected=true"""
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
        tasks = tasks if tasks else []
    return tasks


def delete_existing_metrics(measures: list):
    measures = [f"""'{measure.get("id")}'""" for measure in measures]
    measure_id_filter = ", ".join(measures)
    run_id = TASK_CONFIG.get("run_id")
    delete_metrics(
        TASK_CONFIG,
        run_id=run_id,
        measure_ids=measure_id_filter,
    )