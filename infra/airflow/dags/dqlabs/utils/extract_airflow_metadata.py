"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import re
import json
from uuid import uuid4
from datetime import datetime, timedelta

from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone
from dqlabs.utils.connector import get_pipeline_job_last_run_id, pipeline_parse_datetime
from dqlabs.app_helper import agent_helper
from dqlabs.enums.connection_types import ConnectionType


def replace_special_characters(input_str: str):
    # Replace any character that is not a letter, digit, or underscore with an underscore
    return re.sub(r"[^\w]", "_", input_str)


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


def validate_connection(config: dict) -> tuple:
    is_valid = False
    try:
        response = __get_response(config, "pools")
        is_valid = bool(response) if response else False
        return (bool(is_valid), "")
    except Exception as e:
        log_error(f"Airflow Connector - Validate Connection Failed ", e)
        return (is_valid, str(e))


def get_dag_info(config: dict, credentials: dict, dag_id: str, data: dict) -> dict:
    try:
        request_url = f"dags/{dag_id}"
        response = __get_response(config, request_url)
        response = response if response else {}
        if response:
            owners = response.get("owners", [])
            tags = response.get("tags", [])
            schedule_interval = response.get("schedule_interval", {})
            schedule_interval = schedule_interval if schedule_interval else {}
            data.update(
                {
                    "id": response.get("dag_id"),
                    "name": response.get("dag_id"),
                    "type": "Pipeline",
                    "next_run": response.get("next_dagrun"),
                    "job_id": response.get("dag_id"),
                    "job_name": response.get("dag_id"),
                    "default_view": response.get("default_view"),
                    "description": response.get("description", ""),
                    "fileloc": response.get("fileloc"),
                    "file_token": response.get("file_token"),
                    "is_active": "Yes" if response.get("is_active", False) else "No",
                    "is_subdag": "Yes" if response.get("is_subdag", False) else "No",
                    "is_paused": "Yes" if response.get("is_paused", False) else "No",
                    "next_dagrun": response.get("next_dagrun"),
                    "owners": ",".join(owners),
                    "schedule_interval": response.get("schedule_interval"),
                    "schedule_interval_type": schedule_interval.get("__type", ""),
                    "schedule_interval_value": schedule_interval.get("value", ""),
                    "tags": ",".join(f"'{x.get('name')}'" for x in tags),
                    "root_dag_id": response.get("root_dag_id", ""),
                    "scheduler_lock": response.get("scheduler_lock"),
                    "has_import_errors": response.get("has_import_errors"),
                    "next_dagrun_data_interval_start": response.get(
                        "next_dagrun_data_interval_start"
                    ),
                    "next_dagrun_data_interval_end": response.get(
                        "next_dagrun_data_interval_end"
                    ),
                    "next_dagrun_create_after": response.get(
                        "next_dagrun_create_after"
                    ),
                }
            )

            data = get_dag_runs(config, dag_id, data)
            task_data = get_tasks_by_dag_id(config, dag_id, data)
            if task_data:
                data.update(task_data)
            save_airflow_pipeline_jobs(config, data)

            run_id = task_data.get("runId", None)
            last_run_id = get_pipeline_job_last_run_id(config, conn_type=ConnectionType.Airflow.value)
            if not last_run_id:
                run_params = {"initial": True}
                __get_runs_by_dag_id(config, credentials, dag_id, run_params)
            else:
                # last_run_id = last_run_details.get("run_id")
                last_run_id = (
                    last_run_id
                    if last_run_id.split("_")[-1] < run_id.split("_")[-1]
                    else run_id
                )
                run_params = {
                    "run_id": last_run_id,
                    "condition": "eq" if run_id == last_run_id else "gt",
                }
                __get_runs_by_dag_id(config, credentials, dag_id, run_params)
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Info By Id ", e)
    finally:
        return data


def get_tasks_by_dag_id(config: dict, dag_id, run_data) -> dict:
    try:
        request_url = f"dags/{dag_id}/tasks?order_by=-start_date"
        response = __get_response(config, request_url)
        response = response if response else {}
        tasks_data = response.get("tasks", []) if response else []
        total_entries = response.get("total_entries", 0) if response else 0
        tasks = []
        sub_dags = []
        last_dag_run_id = ""

        for task in tasks_data:
            sub_dag = task.get("sub_dag", None)
            sub_dag_info = {}
            if sub_dag:
                sub_dag_info = {
                    "dag_id": sub_dag.get("dag_id"),
                    "root_dag_id": sub_dag.get("root_dag_id"),
                }
                sub_dags = get_sub_dag_from_tasks(sub_dags, sub_dag)

            task_instance, last_dag_run_id = get_dag_last_run_task_instance(
                config, dag_id, task.get("task_id"), run_data
            )

            tasks.append(
                {
                    "uniqueId": task.get("task_id"),
                    "name": task.get("task_id"),
                    "owner": task.get("owner", ""),
                    "start_date": task.get("start_date", ""),
                    "end_date": task.get("end_date", ""),
                    "trigger_rule": task.get("trigger_rule", ""),
                    "is_mapped": task.get("is_mapped", ""),
                    "wait_for_downstream": task.get("wait_for_downstream", ""),
                    "retries": task.get("retries", ""),
                    "queue": task.get("queue", ""),
                    "pool": task.get("pool", ""),
                    "operator_name": task.get("operator_name", ""),
                    "priority_weight": task.get("priority_weight", ""),
                    "weight_rule": task.get("weight_rule", ""),
                    "ui_color": task.get("ui_color", ""),
                    "ui_fgcolor": task.get("ui_fgcolor", ""),
                    "downstream_task_ids": task.get("downstream_task_ids", ""),
                    "sub_dag": sub_dag_info if sub_dag_info else None,
                    **task_instance,
                }
            )

        response.update(
            {
                "models": tasks,
                "tot_task": total_entries,
                "sub_dags": sub_dags,
                "runId": last_dag_run_id,
            }
        )

        p_tasks = list(filter(lambda x: x.get("status") == "success", tasks))

        response.update(
            {
                "tot_tests": 0,
                "tot_task": len(tasks),
                "p_tasks": len(p_tasks),
                "p_tests": 0,
                "passed": len(p_tasks),
                "failed": (len(tasks)) - (len(p_tasks)),
            }
        )
        return response
    except Exception as e:
        log_error(f"Airflow Connector - Get Jobs Tests and Models ", e)


def save_airflow_pipeline_jobs(
    config: dict, data: dict
) -> None:  # Duplicate function - TBR
    """
    Save / Update Jobs For Pipeline
    """
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        if connection_id and data:
            with connection.cursor() as cursor:
                query_string = f"""
                select id, data from core.pipeline_data 
                where connection_id = '{connection_id}' and type ='jobs' and asset_id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                jobs = fetchone(cursor)
                pipeline_data_id = jobs.get("id", None) if jobs else None
                jobs = jobs.get("data", None) if jobs else None
                if pipeline_data_id and jobs:
                    job = next((x for x in jobs if x.get("id") == data.get("id")), None)
                    if job:
                        job = {**job, **data}
                        jobs = [
                            job if x.get("id") == data.get("id") else x for x in jobs
                        ]
                    else:
                        jobs.append(data)

                    data = (
                        json.dumps(jobs, default=str).replace("'", "''")
                        if jobs
                        else None
                    )
                    query_string = f"""
                        update core.pipeline_data set data='{data}'
                        where id='{pipeline_data_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                else:
                    data = (
                        json.dumps([data], default=str).replace("'", "''")
                        if data
                        else None
                    )
                    parsed_data = json.loads(data)
                    if parsed_data:
                        for run in parsed_data[0]["runs"]:
                            original_id = run["dag_run_id"]
                            run["original_dag_run_id"] = original_id
                            run["dag_run_id"] = replace_special_characters(original_id)

                    # update the runId for jobs
                    if parsed_data[0]["runId"]:
                        runId = replace_special_characters(parsed_data[0]["runId"])
                        parsed_data[0]["runId"] = runId

                    # convert the jsonb back to str to insert into the table
                    data = json.dumps(parsed_data)
                    query_input = (str(uuid4()), "jobs", data, asset_id, connection_id, False, True)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    query_string = f"""
                            insert into core.pipeline_data (
                                id, type, data, asset_id, connection_id, is_delete, is_active
                            ) values {query_param}
                        """
                    cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(str(e), e)


def __get_runs_by_dag_id(
    config: dict, credentials: dict, dag_id: str, params: dict = None
) -> dict:
    try:
        request_url = f"dags/{dag_id}/dagRuns?order_by=-execution_date&limit=30"
        if params:
            if params.get("initial", None):
                end = datetime.today()
                start = end - timedelta(days=300)
                dates = [
                    start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]
                request_url = f"{request_url}&execution_date_gte={dates[0]}&execution_date_lte={dates[1]}"
            else:
                run_id = params.get("run_id", None)
                run_id_date = run_id.split("_")[-1]
                run_id_date = pipeline_parse_datetime(run_id_date)
                run_id_date = run_id_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                condition = params.get("condition", None)
                if run_id and condition:
                    if condition == "eq":
                        request_url = f"dags/{dag_id}/dagRuns/{run_id}"
                    elif condition == "gt":
                        request_url = f"{request_url}&&execution_date_gte={run_id_date}"

            response = __get_response(config, request_url)
            response = response if response else {}
            runs = response.get("dag_runs", []) if response else []
            if response:
                if runs:
                    # Apply the remove char function to each 'dag_run_id'
                    for run in runs:
                        original_id = run["dag_run_id"]
                        run["original_dag_run_id"] = original_id
                        run["dag_run_id"] = replace_special_characters(original_id)
                        if run.get("models"):
                            for each_model in run.get("models"):
                                each_model["name"] = each_model.get("task_id")

                save_airflow_pipeline_runs(config, credentials, runs)

    except Exception as e:
        raise e


def save_airflow_pipeline_runs(config: dict, credentials: dict, runs: list) -> None:
    """
    Save / Update Runs For Pipeline
    """
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        if connection_id and runs:
            for i, item in enumerate(runs):
                start_time = pipeline_parse_datetime(item.get("start_date"))
                end_time = pipeline_parse_datetime(item.get("end_date"))
                duration = end_time - start_time
                duration = duration.total_seconds()
                runs[i] = {
                    "id": item.get("dag_run_id"),
                    "uniqueId": item.get("dag_run_id"),
                    "status": item.get("state"),
                    "status_humanized": item.get("state"),
                    "created_at": item.get("logical_date"),
                    "started_at": item.get("start_date"),
                    "finished_at": item.get("end_date"),
                    "job": item.get("dag_id"),
                    "in_progress": True if item.get("state") == "running" else False,
                    "is_complete": True if item.get("state") == "success" else False,
                    "is_success": True if item.get("state") == "success" else False,
                    "is_error": (
                        True
                        if item.get("state")
                        == [
                            "failed",
                            "upstream_failed",
                            "up_for_retry",
                            "deferred",
                            "removed",
                        ]
                        else False
                    ),
                    "is_cancelled": True if item.get("state") == ["removed"] else False,
                    "duration": duration,
                    "run_duration": duration,
                    "job_id": item.get("dag_id", ""),
                    "is_running": True if item.get("state") == "running" else False,
                    "run_type": item.get("run_type", ""),
                    "external_trigger": item.get("external_trigger"),
                    "original_dag_run_id": item.get("original_dag_run_id"),
                    "models": [{
                        "status": True
                            if item.get("state")
                            == [
                                "failed",
                                "upstream_failed",
                                "up_for_retry",
                                "deferred",
                                "removed",
                            ]
                            else False,
                        "name": item.get("dag_run_id"),
                        "uniqueId": item.get("dag_run_id")
                    }]
                }

            with connection.cursor() as cursor:
                query_string = f"""
                select id, data from core.pipeline_data 
                where connection_id = '{connection_id}' and type ='runs' and asset_id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_data = fetchone(cursor)
                pipeline_data_id = (
                    existing_data.get("id", None) if existing_data else None
                )
                existing_data = (
                    existing_data.get("data", None) if existing_data else None
                )

                if pipeline_data_id and existing_data:
                    for data in runs:
                        matched_data = next(
                            (x for x in existing_data if x.get("id") == data.get("id")),
                            None,
                        )
                        if matched_data:
                            matched_data = {**matched_data, **data}
                            existing_data = [
                                matched_data if x.get("id") == data.get("id") else x
                                for x in existing_data
                            ]
                        else:
                            more_info = get_tasks_by_run_id(
                                config,
                                item.get("job_id"),
                                item.get("original_dag_run_id"),
                            )
                            if more_info:
                                data.update(more_info)
                            existing_data.append(data)

                    runs = (
                        json.dumps(existing_data, default=str).replace("'", "''")
                        if existing_data
                        else None
                    )
                    query_string = f"""
                        update core.pipeline_data set data='{runs}'
                        where id='{pipeline_data_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                else:
                    for i, item in enumerate(runs):
                        more_info = get_tasks_by_run_id(
                            config,
                            item.get("job_id"),
                            item.get("original_dag_run_id"),
                        )
                        if more_info:
                            runs[i].update(more_info)

                    runs = (
                        json.dumps(runs, default=str).replace("'", "''")
                        if runs
                        else None
                    )

                    query_input = (str(uuid4()), "runs", runs, asset_id, connection_id, False, True)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    query_string = f"""
                            insert into core.pipeline_data (
                                id, type, data, asset_id, connection_id, is_delete, is_active
                            ) values {query_param}
                        """
                    cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        log_error(str(e), e)


def get_tasks_by_run_id(config: dict, dag_id: str, dag_run_id: str) -> dict:
    try:
        request_url = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"

        response = __get_response(config, request_url)
        response = response if response else {}
        response = response.get("task_instances", [])
        for task in response:
            task.update(
                {
                    "uniqueId": task.get("task_id"),
                    "name": task.get("task_display_name"),
                    "status": task.get("state"),
                    "executeStartedAt": task.get("start_date"),
                    "executeCompletedAt": task.get("end_date"),
                    "compiledSql": "",
                    "executionTime": task.get("duration"),
                }
            )
        tasks = {"models": response}
        return tasks
    except Exception as e:
        log_error(f"Airflow Connector - Get Jobs Tests and Models ", e)


def prepare_description(properties):
    dag_id = properties.get("dag_id", "")
    owners = properties.get("owners", "")
    dag_id = properties.get("dag_id", "")
    description = properties.get("description", "")

    if description:
        return f"{description}".strip()

    description = f"""This Airflow DAG {dag_id} is Owned by {owners}"""
    return f"{description}".strip()


def prepare_search_key(data: dict) -> dict:
    search_keys = ""
    try:
        keys = {
            "id": data.get("dag_id", ""),
            "owners": data.get("owners", ""),
            "tags": data.get("tags", ""),
        }
        keys = keys.values()
        keys = [x for x in keys if x]

        if len(keys) > 0:
            search_keys = " ".join(keys)
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Search Keys ", e)
    finally:
        return search_keys


def get_dag_runs(config: dict, dag_id: str, data: dict) -> dict:
    try:
        request_url = f"dags/{dag_id}/dagRuns?order_by=-execution_date&limit=20"
        response = __get_response(config, request_url)
        response = response if response else {}
        runs = response.get("dag_runs", []) if response else []
        total_entries = response.get("total_entries", 0) if response else 0
        data.update({"runs": runs, "total_runs": total_entries})
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Runs ", e)
    finally:
        return data


def get_dag_tasks(config: dict, dag_id: str, data: dict) -> dict:
    try:
        request_url = f"dags/{dag_id}/tasks?order_by=-start_date"
        response = __get_response(config, request_url, "get")
        response = response if response else {}
        tasks_data = response.get("tasks", []) if response else []
        total_entries = response.get("total_entries", 0) if response else 0
        tasks = []
        sub_dags = []

        for task in tasks_data:
            sub_dag = task.get("sub_dag", None)
            sub_dag_info = {}
            if sub_dag:
                sub_dag_info = {
                    "dag_id": sub_dag.get("dag_id"),
                    "root_dag_id": sub_dag.get("root_dag_id"),
                }
                sub_dags = get_sub_dag_from_tasks(sub_dags, sub_dag)

            task_instance, _ = get_dag_last_run_task_instance(
                config, dag_id, task.get("task_id"), data
            )

            tasks.append(
                {
                    "dag_id": dag_id,
                    "uniqueId": task.get("task_id"),
                    "name": task.get("task_id"),
                    "owner": task.get("owner", ""),
                    "start_date": task.get("start_date", ""),
                    "end_date": task.get("end_date", ""),
                    "trigger_rule": task.get("trigger_rule", ""),
                    "is_mapped": task.get("is_mapped", ""),
                    "wait_for_downstream": task.get("wait_for_downstream", ""),
                    "retries": task.get("retries", ""),
                    "queue": task.get("queue", ""),
                    "pool": task.get("pool", ""),
                    "operator_name": task.get("operator_name", ""),
                    "priority_weight": task.get("priority_weight", ""),
                    "weight_rule": task.get("weight_rule", ""),
                    "ui_color": task.get("ui_color", ""),
                    "ui_fgcolor": task.get("ui_fgcolor", ""),
                    "downstream_task_ids": task.get("downstream_task_ids", ""),
                    "sub_dag": sub_dag_info if sub_dag_info else None,
                    **task_instance,
                }
            )

        data.update(
            {"tasks": tasks, "total_tasks": total_entries, "sub_dags": sub_dags}
        )
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Tasks ", e)
    finally:
        return data


def get_dag_last_run_task_instance(
    config: dict, dag_id: str, task_id: str, data: dict
) -> dict:
    task_instance = {
        "executeStartedAt": "",
        "executeCompletedAt": "",
        "executionTime": "",
        "status": "",
    }
    try:
        last_run = data.get("runs", [])
        last_run = last_run[0] if last_run and len(last_run) else None
        last_run_id = last_run.get("dag_run_id")
        if last_run:
            request_url = f"dags/{dag_id}/dagRuns/{last_run_id}/taskInstances/{task_id}"
            response = __get_response(config, request_url)
            response = response if response else {}

            if response:
                task_instance.update(
                    {
                        "executeStartedAt": response.get("start_date", ""),
                        "executeCompletedAt": response.get("end_date", ""),
                        "executionTime": response.get("duration", ""),
                        "status": response.get("state", ""),
                    }
                )
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Last Task Run Details", e)
    finally:
        return task_instance, last_run_id


def get_dag_source_code(config: dict, data: dict) -> dict:
    try:
        file_token = data.get("file_token")
        request_url = f"dagSources/{file_token}"
        response = __get_response(config, request_url)
        response = response if response else {}
        response = response.get("content", "") if response else ""
        data.update({"source_code": response, "compiledSql": response})
    except Exception as e:
        log_error(f"Airflow Connector - Get Dag Source Code", e)
    finally:
        return data


def get_sub_dag_from_tasks(sub_dags: list, task_sub_dag: dict) -> dict:
    try:

        existing_dag = list(
            filter(
                lambda dag: dag.get("dag_id") == task_sub_dag.get("dag_id"), sub_dags
            )
        )

        if task_sub_dag and len(existing_dag) == 0:
            owners = task_sub_dag.get("owners", [])
            tags = task_sub_dag.get("tags", [])
            schedule_interval = task_sub_dag.get("schedule_interval", {})
            schedule_interval = schedule_interval if schedule_interval else {}
            sub_dags.append(
                {
                    "dag_id": task_sub_dag.get("dag_id"),
                    "root_dag_id": task_sub_dag.get("root_dag_id"),
                    "default_view": task_sub_dag.get("default_view"),
                    "description": task_sub_dag.get("description", ""),
                    "fileloc": task_sub_dag.get("fileloc"),
                    "file_token": task_sub_dag.get("file_token"),
                    "is_active": (
                        "Yes" if task_sub_dag.get("is_active", False) else "No"
                    ),
                    "is_subdag": (
                        "Yes" if task_sub_dag.get("is_subdag", False) else "No"
                    ),
                    "is_paused": (
                        "Yes" if task_sub_dag.get("is_paused", False) else "No"
                    ),
                    "next_dagrun": task_sub_dag.get("next_dagrun"),
                    "owners": ",".join(owners),
                    "schedule_interval": task_sub_dag.get("schedule_interval"),
                    "schedule_interval_type": schedule_interval.get("__type", ""),
                    "schedule_interval_value": schedule_interval.get("value", ""),
                    "tags": ",".join(f"'{x.get('name')}'" for x in tags),
                    "root_dag_id": task_sub_dag.get("root_dag_id", ""),
                }
            )

    except Exception as e:
        log_error(f"Airflow Connector - Get Sub Dag From Tasks", e)
    finally:
        return sub_dags


def get_lineage(dag_id: str, data: dict) -> dict:
    lineage = {"tables": [], "relations": []}
    try:
        tasks = data.get("tasks", [])
        tables = []
        for task in tasks:
            tables.append(
                {
                    "id": task.get("uniqueId"),
                    "name": task.get("uniqueId"),
                    "task_id": task.get("uniqueId"),
                    "dag_id": dag_id,
                    "owner": task.get("owner", ""),
                    "start_date": task.get("start_date", ""),
                    "end_date": task.get("end_date", ""),
                    "operator_name": task.get("operator_name", ""),
                    "ui_color": task.get("ui_color", ""),
                    "ui_fgcolor": task.get("ui_fgcolor", ""),
                    "downstream_task_ids": task.get("downstream_task_ids", ""),
                }
            )
        lineage.update({"tables": tables})
        lineage = get_lineage_relations(lineage)
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Lineage Tables", e)
    finally:
        return lineage


def get_lineage_relations(lineage: dict) -> dict:
    try:
        tables = lineage.get("tables", [])
        relations = []
        for table in tables:
            task_id = table.get("id")
            downstream_task_ids = table.get("downstream_task_ids")
            for task in downstream_task_ids:
                relations.append({"srcTableId": task_id, "tgtTableId": task})
        lineage.update({"relations": relations})
    except Exception as e:
        log_error(f"Airflow Connector - Prepare Lineage Relations", e)
    finally:
        return lineage
