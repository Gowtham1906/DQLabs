import json
from uuid import uuid4
from datetime import datetime
import requests

from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status, get_server_endpoint, delete_target_file
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import update_asset_run_id
from dqlabs.app_helper.pipeline_helper import (
    update_pipeline_last_runs, get_pipeline_job_input, get_task_previous_run_history, get_run_history
)
from dqlabs.app_helper.lineage_helper import (
    save_lineage,  update_pipeline_propagations, handle_alerts_issues_propagation,
    get_asset_metric_count, update_asset_metric_count)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks
from dqlabs.enums.schedule_types import ScheduleStatus

TASKS = []
def extract_airflow(config, **kwargs):
    target_path = None
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
        
        global TASKS
        
        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)

        job_input = get_pipeline_job_input(config, run_id)
        target_path = job_input.get("target", [])
        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = config.get("asset_id")
        credentials = connection.get("credentials")
        metadata_config = credentials.get("metadata", {})
        status_filter = credentials.get("status", "all")
        latest_run = False
        

        dags_info = get_airflow_metadata(target_path)
        if not dags_info:
            raise Exception("No Dags Found")
        dag = dags_info[0] if dags_info else {}
        pipeline_id = __get_pipeline_id(config)
        tasks = dag.get("tasks", [])
        tasks = tasks if tasks else []
        TASKS = tasks
        
        # Save Tasks
        if metadata_config.get("tasks") and tasks:
            new_tasks = save_tasks(config, pipeline_id, dag, tasks)
            # Create Pipeline Task level Measure
            if new_tasks:
                create_pipeline_task_measures(config, new_tasks)

        # Prepare and Save Lineage Data
        lineage = dag.get("lineage", {})
        if lineage:
            save_lineage(config, "pipeline", lineage, asset_id)
        
        if metadata_config.get("runs"):
            latest_run = save_runs(config, pipeline_id, dag, tasks, status_filter)

        
        # Save Propagation Values
        update_pipeline_propagations(config, asset)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        update_pipeline_last_runs(config)

        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(config, metadata_config.get("tasks"))
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Dbt Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        delete_target_file(target_path)


def get_airflow_metadata(target_path: list):
    url = target_path[0]
    response = requests.get(url)
    response = response.json()
    return response

def save_tasks(config: dict, pipeline_id: str, dag: dict, tasks: list):
    pipeline_new_tasks = []
    try:
        asset_id = config.get("asset_id")
        connection_id = config.get("connection_id")
        dag_tags = dag.get("tags", "")
        dag_tags = (
            dag_tags.split(",")
            if dag_tags and isinstance(dag_tags, str)
            else dag_tags
        )
        dag_tags = dag_tags if dag_tags else []

        connection = get_postgres_connection(config)

        insert_objects = []
        run_id = None
        with connection.cursor() as cursor:
            for task in tasks:
                if not task:
                    continue

                date_format = "%Y-%m-%d %H:%M:%S.%f%z"
                start_time = task.get("last_run_start_date")
                start_time = start_time if start_time else ""
                start_time = start_time.replace("T", " ").replace("t", "")
                start_time = (
                    datetime.strptime(start_time, date_format) if start_time else None
                )
                end_time = task.get("last_run_end_date")
                end_time = end_time if end_time else ""
                end_time = end_time.replace("T", " ").replace("t", "")
                end_time = (
                    datetime.strptime(end_time, date_format) if end_time else None
                )
                duration = None
                if start_time and end_time:
                    time_delta = end_time - start_time
                    duration = time_delta.total_seconds()
                status = get_pipeline_status(task.get("status", ""))

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
                    "sub_dag": sub_dag_info if sub_dag_info else None,
                }
                error_message = task.get("error", "")
                error_message = error_message if error_message else ""
                error_message = error_message.replace("<", "").replace(">", "")

                task_tags = task.get("tags", "")
                task_tags = (
                    task_tags.split(",")
                    if task_tags and isinstance(task_tags, str)
                    else task_tags
                )
                task_tags = task_tags if task_tags else []
                task_tags.extend(dag_tags)
                run_id = task.get("run_id")
                
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
                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            owner = '{task.get('owner', '')}',
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            status = '{status}',
                            duration = {duration},
                            error = '{error_message}',
                            run_start_at = '{start_time}',
                            run_end_at = '{end_time}',
                            run_id = '{run_id}',
                            tags = '{json.dumps(task_tags)}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                else:
                    task_id = str(uuid4())
                    query_input = (
                        task_id,
                        task.get("task_id"),
                        task.get("task_id"),
                        task.get("owner", ""),
                        json.dumps(properties, default=str).replace(
                            "'", "''"),
                        start_time,
                        end_time,
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        status,
                        duration,
                        json.dumps(task_tags),
                        error_message,
                        run_id,
                        True,
                        True,
                        False
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input).decode("utf-8")
                    insert_objects.append(query_param)
                    pipeline_new_tasks.append(task_id)

            # create each tasks
            tasks_input = split_queries(insert_objects)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)
                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                        run_end_at, pipeline_id, asset_id,connection_id,status, duration, tags, error,
                        run_id, is_selected, is_active, is_delete)
                        values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, tasks_insert_query)
                except Exception as e:
                    log_error(
                        "extract properties: inserting tasks level", e)
        # Update Propagations Alerts and Issues Creation and Notifications
        if run_id:
            handle_alerts_issues_propagation(config, run_id)
    except Exception as e:
        log_error(
            f"Airflow Connector - Save Models By Dag ID Failed ", e
        )
    finally:
        return pipeline_new_tasks
    
def __get_pipeline_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            if pipeline:
                return pipeline.get('id')
    except Exception as e:
        log_error(
            f"Airflow Connector - Get Pipeline Id ", e)
        raise e
    
def save_runs(config: dict, pipeline_id: str, dag: dict, tasks: list, status_filter: str):
    """
    Save / Update Runs For Pipeline
    """
    latest_run = False
    try:

        asset_id = config.get("asset_id")
        connection_id = config.get("connection_id")

        date_format = "%Y-%m-%d %H:%M:%S.%f%z"
        start_time = dag.get("start_date")
        start_time = start_time if start_time else ""
        start_time = start_time.replace("T", " ").replace("t", " ")
        start_time = (
            datetime.strptime(start_time, date_format) if start_time else None
        )
        end_time = dag.get("end_date")
        end_time = end_time if end_time else ""
        end_time = end_time.replace("T", " ").replace("t", " ")
        end_time = datetime.strptime(end_time, date_format) if end_time else None
        duration = None
        if start_time and end_time:
            time_delta = end_time - start_time
            duration = time_delta.total_seconds()

        overall_status = get_pipeline_status(dag.get("state", ""))
        selected_status_filter = get_pipeline_status(status_filter)
        run_id = dag.get("run_id", "")
        if (
            overall_status
            and status_filter != "all"
            and selected_status_filter != overall_status
        ):
            return

        save_data = {
            "asset": asset_id,
            "pipeline": str(pipeline_id),
            "connection": str(connection_id),
            "source_id": run_id,
            "technical_id": run_id,
            "run_start_at": start_time if start_time else None,
            "run_end_at": end_time if end_time else None,
            "duration": duration,
            "status": overall_status,
        }

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Delete Existing Runs Details Data
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
                    and run_id = '{run_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
        
            # Validating existing runs
            query_string = f"""
                select id from core.pipeline_runs
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
                    and source_id = '{run_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            existing_run = fetchone(cursor)
            existing_run = existing_run.get('id') if existing_run else None
            if existing_run:
                query_string = f"""
                    update core.pipeline_runs set 
                        status = '{save_data.get('status')}', 
                        run_start_at = {f"'{save_data.get('run_start_at')}'" if save_data.get('run_start_at') else 'NULL'},
                        run_end_at = {f"'{save_data.get('run_end_at')}'" if save_data.get('run_end_at') else 'NULL'},
                        duration = '{save_data.get('duration')}'
                    where id = '{existing_run}'
                """
                cursor = execute_query(connection, cursor, query_string)
            else:
                run_id = str(uuid4())
                existing_run = run_id
                query_string = f"""
                    insert into core.pipeline_runs (
                        id, asset_id, pipeline_id, connection_id, source_id, technical_id,
                        run_start_at, run_end_at, duration, status, is_active, is_delete
                    ) values (
                        '{run_id}',
                        '{save_data.get('asset')}',
                        '{save_data.get('pipeline')}',
                        '{save_data.get('connection')}',
                        '{save_data.get('source_id')}',
                        '{save_data.get('technical_id')}',
                        {f"'{save_data.get('run_start_at')}'" if save_data.get('run_start_at') else 'NULL'},
                        {f"'{save_data.get('run_end_at')}'" if save_data.get('run_end_at') else 'NULL'},
                        {save_data.get('duration') if save_data.get('duration') is not None else 'NULL'},
                        '{save_data.get('status')}',
                        {True},
                        {False}
                    )
                """
                cursor = execute_query(connection, cursor, query_string)
                latest_run = True
       

            # Update Pipeline Jobs Last Runs Details
            job_update = {
                "status": get_pipeline_status(overall_status),
                "duration": duration,
                "last_run_at": start_time,
            }
            # Build and execute a raw SQL update query for the pipeline record
            update_query = f"""
                UPDATE core.pipeline
                SET
                    status = '{job_update.get('status', '')}',
                    last_run_at = {f"'{job_update.get('last_run_at')}'" if job_update.get('last_run_at') else 'NULL'},
                    updated_at = NOW()
                WHERE asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, update_query)

        # Save / Update Runs Details
        save_runs_detail(config, pipeline_id, existing_run, dag)

    except Exception as e:
        log_error(f"Airflow Connector - Save Runs By Job ID Failed ", e)
    finally:
        return latest_run

def save_runs_detail(
    config: dict, pipeline_id: str,  pipeline_run_id: str, dag: dict
):
    """
    Save / Update Runs For Pipeline
    """
    try:

        asset_id = config.get("asset_id")
        connection_id = config.get("connection_id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            insert_objects = []
            for task in TASKS:
                if not task:
                    continue
                run_id = dag.get("run_id")
                date_format = "%Y-%m-%d %H:%M:%S.%f%z"
                start_time = task.get("last_run_start_date")
                start_time = start_time if start_time else ""
                start_time = start_time.replace("T", " ").replace("t", " ")
                start_time = (
                    datetime.strptime(start_time, date_format) if start_time else None
                )
                end_time = task.get("last_run_end_date")
                end_time = end_time if end_time else ""
                end_time = end_time.replace("T", " ").replace("t", " ")
                end_time = (
                    datetime.strptime(end_time, date_format) if end_time else None
                )
                duration = None
                if start_time and end_time:
                    time_delta = end_time - start_time
                    duration = time_delta.total_seconds()
                status = get_pipeline_status(task.get("status", ""))

                properties = {"execution_date": dag.get("execution_date", "")}
                error_message = task.get("error", "")
                error_message = error_message if error_message else ""
                error_message = error_message.replace("<", "").replace(">", "")

                save_data = {
                    "id": str(uuid4()),
                    "asset": asset_id,
                    "pipeline": pipeline_id,
                    "connection": connection_id,
                    "run_id": run_id,
                    "pipeline_run": str(pipeline_run_id),
                    "type": "task",
                    "name": task.get("task_id"),
                    "source_id": task.get("task_id"),
                    "run_start_at": start_time if start_time else None,
                    "run_end_at": end_time if end_time else None,
                    "duration": duration,
                    "status": status,
                    "error": error_message,
                    "source_code": dag.get("source_code", ""),
                    "properties": json.dumps(properties, default=str).replace(
                        "'", "''"
                    ),
                    "max_retries": task.get("retries", ""),
                }

                query_input = (
                    save_data.get("id"),
                    save_data.get("asset"),
                    save_data.get("pipeline"),
                    save_data.get("connection"),
                    save_data.get("run_id"),
                    save_data.get("pipeline_run"),
                    save_data.get("type"),
                    save_data.get("name"),
                    save_data.get("source_id"),
                    save_data.get("run_start_at"),
                    save_data.get("run_end_at"),
                    save_data.get("duration"),
                    save_data.get("status"),
                    save_data.get("source_code"),
                    save_data.get("properties"),
                    True,
                    False,
                    save_data.get("max_retries")
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)
            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs_detail (
                            id, asset_id, pipeline_id, connection_id, run_id, pipeline_run_id,
                            type, name, source_id, run_start_at, run_end_at, duration, status,
                            compiled_code, properties, is_active, is_delete, max_tries
                        ) values {query_input}
                    """
                    execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error('Airflow Runs Details Insert Failed  ', e)

    except Exception as e:
        log_error(
            f"Airflow Connector - Save Runs Details By Run ID Failed ", e
        )

def extract_pipeline_measure(config: dict, tasks_pull: bool):
    asset_id = config.get("asset_id")
    
    # Fetch pipeline_name (job_name) from pipeline table
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
    
    run_history = get_run_history(config, asset_id)
    if not run_history:
        return
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
    job_run_detail = {
        "duration": latest_run.get("duration"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(config, "asset", job_run_detail, job_name=pipeline_name)

    if not tasks_pull:
        return
    tasks = get_pipeline_tasks(config)
    previous_task_history = []
    if previous_run:
       previous_task_history = get_task_previous_run_history(config, previous_run.get("id"))
    if not tasks:
        return
    
    for task in tasks:
        source_id = task.get("source_id")
        task_name = task.get("name")
        previous_task_run = next((item for item in previous_task_history if item.get("source_id") == source_id), None)
        latest_task_run = next((item for item in TASKS if item.get("task_id") == source_id), None)
        if not latest_task_run:
            continue
        task_run_detail = {
            "duration": latest_task_run.get("last_run_duration"),
            "last_run_date": latest_task_run.get("last_run_start_date"),
            "previous_run_date": previous_task_run.get("started_at") if previous_task_run else None
        }
        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(config, "task", task_run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)