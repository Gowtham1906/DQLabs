"""
    Migration Notes From V2 to V3:
    Migrations Completed
"""
import re
import json
from uuid import uuid4
from datetime import datetime, timedelta

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.pipeline_helper import pipeline_auto_tag_mapping, update_pipeline_last_runs
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.lineage_helper import (save_lineage, save_lineage_entity, 
update_pipeline_propagations, handle_alerts_issues_propagation,
 get_asset_metric_count, update_asset_metric_count, map_asset_with_lineage)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks


def extract_airflow_metadata(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)

        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")

        credentials = decrypt_connection_config(credentials, connection_type)
        is_valid = __validate_connection(config)

        if is_valid:
            dag_id = asset_properties.get("id")

            # Config
            metadata_pull_config = credentials.get("metadata", {})
            runs_pull = metadata_pull_config.get("runs", False)
            tasks_pull = metadata_pull_config.get("tasks", False)

            latest_run = False

            # Pull Dag Information
            dag_info = __get_dag_info(config, credentials, dag_id)

            # Pull and Save Task Details
            task_info, new_tasks = __get_tasks_by_dag_id(config, dag_id, tasks_pull)

            # Create Pipeline Task level Measure
            if new_tasks:
                create_pipeline_task_measures(config, new_tasks)

            # Pull and Save Runs Details
            if runs_pull:
                latest_run = __get_runs_by_dag_id(config, credentials, dag_id)

            # Update last Run Id in Dags and Task
            __update_last_run_stats(config)

            # Update Pipeline Statistics
            __update_pipeline_stats(config)

            # Prepare and Save Lineage Data
            lineage = __get_lineage(dag_id, task_info)
            openlineage = __prepare_openlineage(config, asset_id, connection, dag_id)
            if openlineage and openlineage.get("tables"):
                tables = openlineage.get("tables")

                # Save Report External Tables
                external_tables = [item for item in tables if item.get("level", 0) == 2]
                for table in external_tables:
                    table.update({"entity_name": table.get("table_id")})

                if external_tables:
                    map_asset_with_lineage(config, openlineage, "pipeline")
                    save_lineage_entity(config, external_tables, asset_id)
                save_lineage(config, "pipeline", openlineage, asset_id)
            else:
                save_lineage(config, "pipeline", lineage, asset_id)

            # Prepare Description
            description = __prepare_description(dag_info)
            description = description.replace("'", "''")

            # Prepare Search Key
            search_keys = __prepare_search_key(dag_info)
            search_keys = search_keys.replace("'", "''")

            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.asset set description='{description}', search_keys='{search_keys}'
                    where id = '{asset_id}' and is_active=true and is_delete = false
                """
                cursor = execute_query(connection, cursor, query_string)

            # Save Propagation Values
            update_pipeline_propagations(config, asset)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_pipeline_last_runs(config)

        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)

        if latest_run:
            extract_pipeline_measure(config, dag_id)

        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Airflow Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __replace_special_characters(input_str: str):
    # Replace any character that is not a letter, digit, or underscore with an underscore
    return re.sub(r"[^\w]", "_", input_str)


def __get_pipeline_table_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            pipeline = pipeline.get('id', None) if pipeline else None
            return pipeline
    except Exception as e:
        log_error(str(e), e)
        raise e


def __get_pipeline_job_last_run_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select
                    id,
                    source_id,
                    run_end_at,
                    status
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            return last_run
    except Exception as e:
        log_error(str(e), e)
        raise e


def __get_pipeline_job_runs(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select
                    id,
                    source_id
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc, source_id desc
            """
            cursor = execute_query(connection, cursor, query_string)
            runs_ids = fetchall(cursor)
            runs_ids = [x.get('source_id') for x in runs_ids]
            return runs_ids
    except Exception as e:
        log_error(str(e), e)
        raise e


def __get_pipeline_run_id(config: dict, source_id: str) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, source_id from core.pipeline_runs
                where asset_id = '{asset_id}' and source_id = '{source_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            runs = fetchone(cursor)
            return runs.get('id') if runs and runs.get('id') else None
    except Exception as e:
        log_error(
            f"Airflow Connector - Get Last Run Id By Asset ID Failed ", e)
        raise e


def __pipeline_parse_datetime(date_str):
    # parse the pipeline time accordingly
    try:
        # Try parsing with microseconds
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        # Fallback to parsing without microseconds
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')


def __prepare_description(data: dict) -> str:
    try:
        dag_id = data.get("dag_id", "")
        owners = data.get("owners")
        description = data.get("description")

        if description:
            return f"{description}".strip()

        description = f"""This Airflow DAG {dag_id} is Owned by {",".join(owners) if owners else ''} """
        return f"{description}".strip()
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Description Failed ", e)
        raise e


def __prepare_search_key(data: dict) -> str:
    try:
        search_keys = ""
        dag_id = data.get("dag_id", "")
        owners = data.get("owners")
        tags = data.get("tags")

        keys = {
            "id": dag_id,
            "owners": ",".join(owners),
            "tags": ",".join(f"'{x.get('name')}'" for x in tags)
        }
        keys = keys.values()
        keys = [x for x in keys if x]
        if len(keys) > 0:
            search_keys = " ".join(keys)

        return search_keys
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Search Keys Failed", e)
        raise e


def __get_response(config: dict, url, method_type: str = "get", params=None):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                request_url=url, request_type=method_type, request_params=params
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection(config: dict) -> tuple:
    is_valid = False
    try:
        response = __get_response(config, "pools")
        is_valid = bool(response) if response else False
        return (bool(is_valid), "")
    except Exception as e:
        log_error(f"Airflow Connector - Validate Connection Failed ", e)
        return (is_valid, str(e))


def __get_dag_info(config: dict, credentials: dict, dag_id: str):
    try:
        request_url = f"dags/{dag_id}"
        response = __get_response(config, request_url)
        response = response if response else {}
        if response:
            asset = config.get("asset", {})
            asset_id = asset.get("id")
            owners = response.get("owners")
            tags = response.get("tags")
            tags = [tag.get('name') for tag in tags] if tags else []
            schedule_interval = response.get("schedule_interval", {})
            schedule_interval = schedule_interval if schedule_interval else {}
            properties = {
                "fileloc": response.get("fileloc"),
                "file_token": response.get("file_token"),
                "is_active": "Yes" if response.get("is_active", False) else "No",
                "is_subdag": "Yes" if response.get("is_subdag", False) else "No",
                "is_paused": "Yes" if response.get("is_paused", False) else "No",
                "root_dag_id": response.get("root_dag_id", ""),
                "schedule": schedule_interval
            }
            source_code = __get_dag_source_code(
                config, response.get("file_token"))

            # Update Pipeline Data
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                update_query = f"""
                    UPDATE core.pipeline SET
                        description = '{response.get("description", "")}',
                        next_run_at =  {f"'{response.get('next_dagrun')}'" if response.get(
                            'next_dagrun') else "NULL"},
                        owner = '{",".join(owners) if owners else ""}',
                        tags ='{json.dumps(tags, default=str).replace("'", "''")}',
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                        source_code = '{source_code if source_code else ""}'
                    WHERE asset_id ='{asset_id}'
                """
                execute_query(connection, cursor, update_query)
            if credentials.get("auto_mapping_tags"):
                __airflow_tag_mapping(config, tags, asset_id)

        return response
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Info By Id ", e)
        raise e


def __get_dag_source_code(config: dict, file_token: str) -> str:
    try:
        response = ""
        if file_token:
            request_url = f"dagSources/{file_token}"
            response = __get_response(config, request_url)
            response = response if response else {}
            response = response.get("content", "") if response else ""
        return response.replace("'", "''")
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Source Code", e)
        raise e


def __get_tasks_by_dag_id(config: dict, dag_id: str, task_pull: bool) -> list:
    try:
        # Pull Task Information
        request_url = f"dags/{dag_id}/tasks?order_by=-start_date"
        response = __get_response(config, request_url)
        response = response if response else {}
        tasks_data = response.get("tasks", []) if response else []
        pipeline_new_tasks = []

        if task_pull:
            connection = config.get("connection", {})
            connection_id = connection.get("id")
            asset = config.get("asset", {})
            asset_id = asset.get("id")
            pipeline_id = __get_pipeline_table_id(config)
            tasks = []
            connection = get_postgres_connection(config)
            source_type = "task"

            with connection.cursor() as cursor:
                for task in tasks_data:
                    task_id = task.get("task_id")

                    # Validate Existing Task Information
                    query_string = f""" 
                        select id from core.pipeline_tasks 
                        where asset_id = '{asset_id}' and source_id = '{task_id}' 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    existing_task = fetchone(cursor)
                    existing_task = existing_task.get(
                        'id') if existing_task else None

                    sub_dag = task.get("sub_dag", None)
                    sub_dag_info = {}
                    if sub_dag:
                        sub_dag_info = {
                            "dag_id": sub_dag.get("dag_id"),
                            "root_dag_id": sub_dag.get("root_dag_id"),
                        }

                    properties = {
                        "trigger_rule": task.get("trigger_rule", ""),
                        "is_mapped": task.get("is_mapped", ""),
                        "retries": task.get("retries", ""),
                        "operator_name": task.get("operator_name", ""),
                        "downstream_task_ids": task.get("downstream_task_ids", ""),
                        "sub_dag":  sub_dag_info if sub_dag_info else None,
                    }

                    if existing_task:
                        query_string = f"""
                            update core.pipeline_tasks set 
                                owner = '{task.get('owner', '')}',
                                source_type = '{source_type}',
                                run_start_at = {f"'{task.get('start_date')}'" if task.get('start_date') else 'NULL'},
                                run_end_at = {f"'{task.get('end_date')}'" if task.get('end_date') else 'NULL'},
                                properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                            where id = '{existing_task}'
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    else:
                        task_id = str(uuid4())
                        query_input = (
                            task_id,
                            task.get("task_id"),
                            task.get("task_id"),
                            task.get("owner", ""),
                            json.dumps(properties, default=str).replace(
                                "'", "''"),
                            task.get("start_date", ""),
                            task.get("end_date", ""),
                            source_type,
                            str(pipeline_id),
                            str(asset_id),
                            str(connection_id),
                            True,
                            True,
                            False
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals})", query_input).decode("utf-8")
                        tasks.append(query_param)
                        pipeline_new_tasks.append(task_id)
                        

                # create each tasks
                tasks_input = split_queries(tasks)
                for input_values in tasks_input:
                    try:
                        query_input = ",".join(input_values)
                        tasks_insert_query = f"""
                            insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                            run_end_at, source_type, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete)
                            values {query_input}
                        """
                        cursor = execute_query(
                            connection, cursor, tasks_insert_query)
                    except Exception as e:
                        log_error(
                            "extract properties: inserting tasks level", e)

        return tasks_data, pipeline_new_tasks
    except Exception as e:
        log_error(f"Airflow Connector - Get Jobs Tests and Models ", e)
        raise e


def __get_runs_by_dag_id(config: dict, credentials: dict, dag_id: str):
    try:
        no_of_runs = credentials.get("no_of_runs", "30")
        status_filter = credentials.get("status", "all")
        last_run = __get_pipeline_job_last_run_id(config)
        last_run_id = last_run.get('source_id') if last_run else None
        if last_run_id:
            params = {
                "run_id": last_run_id,
                "condition": "gt",
            }
        else:
            params = {"initial": True}

        request_url = f"dags/{dag_id}/dagRuns?order_by=-execution_date"
        if status_filter != "all":
            status_filter = "success" if status_filter == "success" else "failed"
            request_url = f"{request_url}&state={status_filter}"
        if params:
            if params.get("initial", None):
                no_of_runs = int(no_of_runs) if type(
                    no_of_runs) == str else no_of_runs
                end = datetime.today()
                start = end - timedelta(days=no_of_runs)
                dates = [
                    start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]

                request_url = f"{request_url}&execution_date_gte={dates[0]}&execution_date_lte={dates[1]}"

            else:
                run_id = params.get("run_id", None)
                run_id_date = run_id.split("_")[-1]
                run_id_date = __pipeline_parse_datetime(run_id_date)
                run_id_date = run_id_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                condition = params.get("condition", None)
                if run_id and condition:
                    request_url = f"{request_url}&&execution_date_gte={run_id_date}"
            
            response = __get_response(config, request_url)
            response = response if response else {}
            runs = response.get("dag_runs", []) if response else []

            # Remove Existing Runs and Insert New Runs to avoid Duplicate
            existing_runs = __get_pipeline_job_runs(config)
            new_runs = []
            runs_detail = []
            for run in runs:
                if run["dag_run_id"] not in existing_runs and run["state"] not in ['running', 'pending', 'schedule']:
                    # Apply the remove char function to each 'dag_run_id'
                    original_id = run["dag_run_id"]
                    run["technical_id"] = __replace_special_characters(
                        original_id)

                    # Calculate Runs Duration
                    run["duration"] = 0
                    start_date = run["start_date"]
                    end_date = run["end_date"]
                    if start_date and end_date:
                        start_time = __pipeline_parse_datetime(start_date)
                        end_time = __pipeline_parse_datetime(end_date)
                        duration = end_time - start_time
                        run["duration"] = duration.total_seconds()

                    new_runs.append(run)

                    # Get Runs Detail Data
                    runs_detail = __get_dag_run_task_instance(
                        config, dag_id, run["dag_run_id"], runs_detail)
            if new_runs:
                __save_dag_runs(config, new_runs)

            if runs_detail:
                __save_dag_runs_detail(config, dag_id, runs_detail)

            return len(new_runs) > 0
    except Exception as e:
        log_error(f"Airflow Connector - Get Runs By Dag Details", e)
        raise e

def __get_dag_run_task_instance(config: dict, dag_id: str, dag_run_id: str, runs_detail: list) -> list:
    try:
        request_url = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        response = __get_response(config, request_url)
        response = response if response else {}
        response = response.get("task_instances", [])
        response = response if response else []
        runs_detail = runs_detail + response
        return runs_detail
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Last Task Run Details", e)
        raise e

def __save_dag_runs(config: dict, runs: list):
    try:
        connection = config.get("connection", {})
        connection_id = connection.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_table_id(config)

        insert_data = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in runs:
                properties = {
                    "note": run.get("note", ""),
                    "logical_date": run.get("logical_date", ""),
                    "external_trigger": run.get("external_trigger", ""),
                    "run_type": run.get("run_type", ""),
                    "last_scheduling_decision": run.get("last_scheduling_decision", "")
                }

                query_input = (
                    str(uuid4()),
                    run.get("dag_run_id"),
                    run.get("technical_id", ""),
                    run.get("start_date", ""),
                    run.get("end_date", ""),
                    get_pipeline_status(run.get("state")),
                    run.get("duration"),
                    json.dumps(properties, default=str).replace("'", "''"),
                    str(pipeline_id),
                    str(asset_id),
                    str(connection_id),
                    True,
                    False
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input).decode("utf-8")
                insert_data.append(query_param)

            # Create Runs
            insert_data_split = split_queries(insert_data)
            for input_values in insert_data_split:
                try:
                    query_input = ",".join(input_values)
                    insert_query = f"""
                        insert into core.pipeline_runs (id, source_id, technical_id,
                        run_start_at, run_end_at, status, duration, properties,
                        pipeline_id, asset_id,connection_id,is_active, is_delete)
                        values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, insert_query)
                except Exception as e:
                    log_error("Failed Inserting Runs", e)
                    raise e
    except Exception as e:
        log_error(f"Failed Inserting Runs ", e)
        raise e


def __save_dag_runs_detail(config: dict, dag_id: str, runs: list):
    try:
        connection = config.get("connection", {})
        connection_id = connection.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_table_id(config)

        insert_data = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in runs:
                pipeline_run_id = __get_pipeline_run_id(
                    config, run.get("dag_run_id"))
                properties = {
                    "note": run.get("note", ""),
                    "hostname": run.get("hostname", ""),
                    "unixname": run.get("unixname", ""),
                    "execution_date": run.get("execution_date", "")
                }
                status = get_pipeline_status(run.get("state"))
                error = None
                if run.get("state") != 'success':
                    error = __get_task_error(config, dag_id, run.get(
                        "dag_run_id"), run.get("task_id", ""), run.get("try_number"))

                query_input = (
                    str(uuid4()),
                    run.get("task_id", ""),
                    run.get("task_id", ""),
                    'task',
                    run.get("dag_run_id", ""),
                    run.get("start_date", ""),
                    run.get("end_date", ""),
                    status,
                    run.get("duration"),
                    json.dumps(error, default=str).replace("'", "''"),
                    json.dumps(properties, default=str).replace("'", "''"),
                    str(pipeline_run_id),
                    str(pipeline_id),
                    str(asset_id),
                    str(connection_id),
                    True,
                    False,
                    run.get("try_number"),
                    run.get("max_tries")
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input).decode("utf-8")
                insert_data.append(query_param)

            # Create Runs
            insert_data_split = split_queries(insert_data)
            for input_values in insert_data_split:
                try:
                    query_input = ",".join(input_values)
                    insert_query = f"""
                        insert into core.pipeline_runs_detail (id, name, source_id, type, run_id,
                        run_start_at, run_end_at, status, duration, error, properties,
                        pipeline_run_id, pipeline_id, asset_id, connection_id,is_active, is_delete, try_number, max_tries)
                        values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, insert_query)
                except Exception as e:
                    log_error("Failed Inserting Runs", e)
                    raise e
    except Exception as e:
        log_error(f"Failed Inserting Runs ", e)
        raise e


def __get_task_error(config: dict, dag_id: str, dag_run_id: str, task_id: str, try_number) -> list:
    try:
        request_url = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        response = __get_response(config, request_url)
        response = response if response else {}
        response = response.get("content", "")
        return response
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Task Run Error Message Failed", e)
        raise e


def __update_last_run_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            last_run = __get_pipeline_job_last_run_id(config)
            if last_run:
                run_id = last_run.get('source_id')
                status = last_run.get('status')
                last_run_at = last_run.get('run_end_at')
                query_string = f"""
                    update core.pipeline set 
                        run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                # Update Task Level Status
                query_string = f"""
                    select
                        id,
                        source_id,
                        status,
                        duration,
                        run_start_at,
                        run_end_at,
                        error
                    from
                        core.pipeline_runs_detail
                    where asset_id = '{asset_id}' and run_id = '{run_id}'
                    order by id desc
                """
                cursor = execute_query(connection, cursor, query_string)
                last_tasks_runs = fetchall(cursor)

                for task in last_tasks_runs:
                    task_id = task.get('source_id')
                    status = task.get('status')
                    duration = task.get('duration')
                    run_start_at = task.get('run_start_at')
                    run_end_at = task.get('run_end_at')
                    duration_value = duration if duration else 'NULL'
                    run_start_at = f"'{run_start_at}'" if run_start_at else 'NULL'
                    run_end_at = f"'{run_end_at}'" if run_end_at else 'NULL'
                    query_string = f"""
                        update core.pipeline_tasks set 
                            run_id='{run_id}', status='{status}', duration={duration_value},
                            run_start_at={run_start_at}, run_end_at={run_end_at},
                            error='{task.get('error', '')}'
                        where asset_id = '{asset_id}' and source_id = '{task_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                # Update Propagations Alerts and Issues Creation and Notifications
                handle_alerts_issues_propagation(config, run_id)

    except Exception as e:
        log_error(str(e), e)
        raise e


def __update_pipeline_stats(config: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                 select 
                 	pipeline.id,
	                pipeline.properties,
                    count(distinct pipeline_tasks.source_id) as tot_tasks,
	 				count(distinct pipeline_runs.id) as tot_runs
                from core.pipeline
                join core.asset on asset.id = pipeline.asset_id
                left join core.pipeline_tasks on pipeline_tasks.pipeline_id = pipeline.id
	 			left join core.pipeline_runs on pipeline.id = pipeline_runs.pipeline_id
 				left join core.pipeline_runs_detail on pipeline_runs_detail.pipeline_run_id = pipeline_runs.id
                where pipeline.asset_id='{asset_id}'
                group by pipeline.id
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)

            if pipeline:
                properties = pipeline.get('properties', {})
                properties.update({
                    "tot_tasks": pipeline.get('tot_tasks', 0),
                    "tot_runs":  pipeline.get('tot_runs', 0)
                })
                query_string = f"""
                    update core.pipeline set 
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        log_error(str(e), e)
        raise e

def __prepare_openlineage(config: dict, asset_id: str, connection: dict, dag_id: str) -> dict:
    try:
        pg_connection = get_postgres_connection(config)
        with pg_connection.cursor() as cursor:
            connection_id = connection.get("id")
            query_string = f"""
                select
                    id,
                    name,
                    source_id,
                    openlineage_events::jsonb as openlineage_events
                from core.pipeline
                where asset_id = '{asset_id}' and name = '{dag_id}' and connection_id = '{connection_id}'
                and is_active = true and is_delete = false
            """
            cursor = execute_query(pg_connection, cursor, query_string)
            pipeline = fetchone(cursor)

        events = {}
        if pipeline:
            events = pipeline.get("openlineage_events", {}) or {}
            if isinstance(events, str):
                try:
                    events = json.loads(events)
                except Exception:
                    events = {}

        tables_map = {}
        relations = []

        def parse_dataset_name(ds_name: str):
            """Parse dataset name like 'public.public.source_table' or 'schema.table' into components"""
            parts = ds_name.split('.')
            if len(parts) >= 2:
                # Handle formats:
                # - schema.table (2 parts)
                # - database.schema.table (3 parts)
                # - public.public.table (schema repeated, treat as schema.table)
                if len(parts) == 2:
                    return parts[0], parts[1], ""  # schema, table, database
                elif len(parts) == 3:
                    # If first two parts are same (like public.public.table), treat as schema.table
                    if parts[0] == parts[1]:
                        return parts[0], parts[2], ""  # schema, table, database
                    else:
                        return parts[1], parts[2], parts[0]  # schema, table, database
                else:
                    # 4+ parts: use last two as schema.table, rest as database
                    return parts[-2], parts[-1], '.'.join(parts[:-2])
            return "", ds_name, ""  # schema, table (full name), database

        def parse_namespace(namespace: str):
            """Parse namespace like 'postgres://100.28.138.26:5432' to extract connection type"""
            if not namespace:
                return "", ""
            # Extract connection type from protocol
            if "postgres" in namespace.lower():
                return "postgresql", namespace
            elif "mysql" in namespace.lower():
                return "mysql", namespace
            elif "snowflake" in namespace.lower():
                return "snowflake", namespace
            return "unknown", namespace

        def get_table_name_only(ds_name: str) -> str:
            """Extract just the table name (last part) from names like 'public.public.target_table'"""
            parts = ds_name.split('.')
            return parts[-1] if parts else ds_name

        def dataset_id(ds: dict) -> str:
            """Return only the table name (last part) as the ID"""
            name = ds.get("name", "")
            return get_table_name_only(name)

        def ensure_table_node(ds: dict, task_id_value: str):
            ds_name = ds.get("name", "")
            table_name = get_table_name_only(ds_name)  # Just the last part
            did = table_name  # Use table name as ID
            
            if did not in tables_map:
                namespace = ds.get("namespace", "")
                schema_name, _, database_name = parse_dataset_name(ds_name)
                connection_type, _ = parse_namespace(namespace)
                
                # Extract schema fields from facets
                fields = []
                facets = ds.get("facets", {}) or {}
                schema_facet = facets.get("schema", {}) or {}
                if schema_facet:
                    schema_fields = schema_facet.get("fields", []) or []
                    fields = schema_fields
                
                node = {
                    "table_id": table_name,  # Use only table name as ID
                    "table_name": table_name,  # Just table name for display
                    "id": table_name,  # For lineage matching - just table name
                    "name": table_name,  # Display name
                    "schema": schema_name,
                    "database": database_name if database_name else namespace,  # Use namespace if no database parsed
                    "databaseType": connection_type,
                    "connection_type": connection_type,
                    "level": 2,
                    "fields": fields,
                }
                tables_map[did] = node

        def ensure_task_node(task_id_value: str):
            tid = f"{dag_id}.{task_id_value}"
            if tid not in tables_map:
                tables_map[tid] = {
                    "id": task_id_value,
                    "name": task_id_value,
                    "dag_id": dag_id,
                    "type": "task",
                }
            return task_id_value

        # Build nodes and relations from OpenLineage task events
        for task_id_key, event in (events.items() if isinstance(events, dict) else []):
            inputs = event.get("inputs", []) or []
            outputs = event.get("outputs", []) or []

            task_node_id = ensure_task_node(task_id_key)

            for ds in inputs:
                ensure_table_node(ds, task_id_key)
            for ds in outputs:
                ensure_table_node(ds, task_id_key)

            # input dataset -> task
            for in_ds in inputs:
                in_id = dataset_id(in_ds)
                relations.append({"srcTableId": in_id, "tgtTableId": task_node_id})

            # task -> output dataset
            for out_ds in outputs:
                out_id = dataset_id(out_ds)
                relations.append({"srcTableId": task_node_id, "tgtTableId": out_id})

                # optional column-level relations from inputs to this output using columnLineage facet
                facets = out_ds.get("facets", {}) or {}
                col_lin = facets.get("columnLineage", {}) or {}
                fields_map = (col_lin.get("fields", {}) or {})
                for tgt_col, spec in fields_map.items():
                    input_fields = spec.get("inputFields", []) or []
                    for inf in input_fields:
                        src_ds_name = inf.get("name", "")
                        # Use only the table name (last part) for consistency
                        src_ds_id = get_table_name_only(src_ds_name)
                        relations.append({
                            "srcTableId": src_ds_id,
                            "tgtTableId": out_id,
                            "srcTableColName": inf.get("field"),
                            "tgtTableColName": tgt_col,
                        })

        lineage = {"tables": list(tables_map.values()), "relations": relations}
        return lineage
    except Exception as e:
        log_error(f"Airflow Connector - Prepare OpenLineage", e)
        raise e


def __get_lineage(dag_id: str, tasks: list) -> dict:
    try:
        lineage = {"tables": [], "relations": []}
        tables = []
        for task in tasks:
            tables.append(
                {
                    "id": task.get("task_id"),
                    "name": task.get("task_id"),
                    "task_id": task.get("task_id"),
                    "dag_id": dag_id,
                    "owner": task.get("owner", ""),
                    "downstream_task_ids": task.get("downstream_task_ids", "")
                }
            )
        lineage.update({"tables": tables})
        lineage = __get_lineage_relations(lineage)
        return lineage
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Lineage Tables", e)
        raise e


def __get_lineage_relations(lineage: dict) -> dict:
    try:
        tables = lineage.get("tables", [])
        relations = []
        for table in tables:
            task_id = table.get("id")
            downstream_task_ids = table.get("downstream_task_ids")
            for task in downstream_task_ids:
                relations.append({"srcTableId": task_id, "tgtTableId": task})
        lineage.update({"relations": relations})
        return lineage
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Lineage Relations", e)
        raise e


def __airflow_tag_mapping(config: dict, tags: list, asset_id: str):
    params = {
        "asset_id": asset_id,
        "level": "asset",
        "source": "airflow"
    }
    pipeline_auto_tag_mapping(config, tags, params)


def extract_pipeline_measure(config:dict, dag_id: str):
    # Fetch pipeline_name (job_name) from pipeline table
    asset = config.get("asset", {})
    asset_id = asset.get("id") if asset else config.get("asset_id")
    connection = get_postgres_connection(config)
    pipeline_name = None
    if asset_id:
        with connection.cursor() as cursor:
            pipeline_query = f"""
                select p.name as pipeline_name
                from core.pipeline p
                where p.asset_id = '{asset_id}'
                limit 1
            """
            cursor = execute_query(connection, cursor, pipeline_query)
            pipeline_result = fetchone(cursor)
            pipeline_name = pipeline_result.get("pipeline_name") if pipeline_result else None
    
    connection = config.get("connection")
    credentials = connection.get("credentials")
    metadata_pull_config = credentials.get("metadata", {})
    tasks_pull = metadata_pull_config.get("tasks", False)
    runs, run_detail_list = __get_runs_by_dag_measure(config, credentials, dag_id)
    last_run = runs[0] if runs else None
    previous_run = runs[1] if len(runs) > 1 else None
    last_run_detail = {
        "duration": last_run.get('duration') if last_run else None
    }
    if last_run and previous_run:
        last_run_detail.update({
            "last_run_date": last_run.get("start_date"),
            "previous_run_date": previous_run.get("start_date")
        })
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(config, "asset", last_run_detail, job_name=pipeline_name)
    if not tasks_pull:
        return
    
    tasks = get_pipeline_tasks(config)
    for task in tasks:
        task_name = task.get("name")
        task_run_list = [run for run in run_detail_list if run.get("task_id") == task.get("source_id")]
        if task_run_list:
            task_run_list = sorted(task_run_list, key=lambda x: x.get("start_date"))
            task_latest_run = task_run_list[-1] if task_run_list else None
            task_previous_run = task_run_list[0] if len(task_run_list) > 1 else None
            task_run_detail = {
                "duration": task_latest_run.get("duration")
            }
            if task_previous_run:
                task_run_detail.update({
                    "last_run_date": task_latest_run.get("start_date"),
                    "previous_run_date": task_previous_run.get("start_date")
                })
            # Pass pipeline_name (job_name) and task_name for task level measures
            execute_pipeline_measure(config, "task", task_run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)


def __get_runs_by_dag_id(config: dict, credentials: dict, dag_id: str):
    try:
        no_of_runs = credentials.get("no_of_runs", "30")
        status_filter = credentials.get("status", "all")
        last_run = __get_pipeline_job_last_run_id(config)
        last_run_id = last_run.get('source_id') if last_run else None
        if last_run_id:
            params = {
                "run_id": last_run_id,
                "condition": "gt",
            }
        else:
            params = {"initial": True}

        request_url = f"dags/{dag_id}/dagRuns?order_by=-execution_date"
        if status_filter != "all":
            status_filter = "success" if status_filter == "success" else "failed"
            request_url = f"{request_url}&state={status_filter}"
        if params:
            if params.get("initial", None):
                no_of_runs = int(no_of_runs) if type(
                    no_of_runs) == str else no_of_runs
                end = datetime.today()
                start = end - timedelta(days=no_of_runs)
                dates = [
                    start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]

                request_url = f"{request_url}&execution_date_gte={dates[0]}&execution_date_lte={dates[1]}"

            else:
                run_id = params.get("run_id", None)
                run_id_date = run_id.split("_")[-1]
                run_id_date = __pipeline_parse_datetime(run_id_date)
                run_id_date = run_id_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                condition = params.get("condition", None)
                if run_id and condition:
                    request_url = f"{request_url}&&execution_date_gte={run_id_date}"
            
            response = __get_response(config, request_url)
            response = response if response else {}
            runs = response.get("dag_runs", []) if response else []

            # Remove Existing Runs and Insert New Runs to avoid Duplicate
            existing_runs = __get_pipeline_job_runs(config)
            new_runs = []
            runs_detail = []
            for run in runs:
                if run["dag_run_id"] not in existing_runs and run["state"] not in ['running', 'pending', 'schedule']:
                    # Apply the remove char function to each 'dag_run_id'
                    original_id = run["dag_run_id"]
                    run["technical_id"] = __replace_special_characters(
                        original_id)

                    # Calculate Runs Duration
                    run["duration"] = 0
                    start_date = run["start_date"]
                    end_date = run["end_date"]
                    if start_date and end_date:
                        start_time = __pipeline_parse_datetime(start_date)
                        end_time = __pipeline_parse_datetime(end_date)
                        duration = end_time - start_time
                        run["duration"] = duration.total_seconds()

                    new_runs.append(run)

                    # Get Runs Detail Data
                    runs_detail = __get_dag_run_task_instance(
                        config, dag_id, run["dag_run_id"], runs_detail)
            if new_runs:
                __save_dag_runs(config, new_runs)

            if runs_detail:
                __save_dag_runs_detail(config, dag_id, runs_detail)

            return len(new_runs) > 0
    except Exception as e:
        log_error(f"Airflow Connector - Get Runs By Dag Details", e)
        raise e
    
def __get_runs_by_dag_measure(config: dict, credentials: dict, dag_id: str):
    runs = []
    run_detail_list = []
    try:
        metadata_config = credentials.get("metadata", {})
        no_of_runs = credentials.get("no_of_runs", "30")
        no_of_runs = int(no_of_runs) if type(
                    no_of_runs) == str else no_of_runs
        end = datetime.today()
        start = end - timedelta(days=no_of_runs)
        dates = [
            start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ]
            
        request_url = f"dags/{dag_id}/dagRuns?limit=2&order_by=-execution_date&execution_date_gte={dates[0]}&execution_date_lte={dates[1]}"
        response = __get_response(config, request_url)
        response = response if response else {}
        runs = response.get("dag_runs", []) if response else []
        if metadata_config.get("tasks"):
            for run in runs:
                start_date = run["start_date"]
                end_date = run["end_date"]
                if start_date and end_date:
                    start_time = __pipeline_parse_datetime(start_date)
                    end_time = __pipeline_parse_datetime(end_date)
                    duration = end_time - start_time
                    run["duration"] = duration.total_seconds()
                run_detail_list = __get_dag_run_task_instance(
                        config, dag_id, run["dag_run_id"], run_detail_list)
            
    except Exception as e:
        log_error(f"Airflow Connector - Get Runs By Dag Details", e)
        raise e
    finally:
        return runs, run_detail_list