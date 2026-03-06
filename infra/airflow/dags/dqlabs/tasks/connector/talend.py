"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import re
from dqlabs.app_helper.db_helper import execute_query
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.lineage_helper import save_lineage
from dqlabs.app_helper import agent_helper


TASK_CONFIG = None


def extract_talend_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        global TASK_CONFIG
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
        TASK_CONFIG = config

        is_valid = __validate_connection_establish()
        if is_valid:
            data = {}
            pipeline_detail = __get_pipeline_data(asset_properties)
            tasks = __get_tasks(asset_properties)
            runs = []

            for task in tasks:
                run_list = __get_runs(task.get("workspaceId"), task.get("flowId"))
                if run_list:
                    for run in run_list:
                        runs.append(
                            {
                                **run,
                                "flow_name": task.get("flowName"),
                                "flow_id": task.get("flowId"),
                            }
                        )
            run_failed = list(
                filter(
                    lambda x: x.get("flowStatus", "EXECUTION_SUCCESS")
                    == "EXECUTION_FAILED",
                    runs,
                )
            )
            run_failed = True if len(run_failed) else False

            data = {
                **pipeline_detail,
                "runs": runs,
                "tasks": tasks,
                "stats": {
                    "runs": len(runs),
                    "tasks": len(tasks),
                    "run_failed": run_failed,
                },
            }

            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                pipeline_name = data.get("name")
                query_string = f"""
                    update core.asset set name='{pipeline_name}', technical_name='{_get_technical_name(pipeline_name)}'
                    where id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
            save_lineage(config, "pipeline", None, data, asset_id)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Talend Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error=e)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __get_response(
    url,
    method_type: str = "get",
    is_metadata=False,
    is_dataset=False,
    params=None,
):
    try:
        global TASK_CONFIG
        pg_connection = get_postgres_connection(TASK_CONFIG)
        api_response = agent_helper.execute_query(
            TASK_CONFIG,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                request_url=url,
                request_type=method_type,
                request_params=params,
                is_metadata=is_metadata,
                is_dataset=is_dataset,
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection_establish() -> tuple:
    try:
        is_valid = False
        response = __get_response("subscription")
        is_valid = bool(response)
        return (bool(is_valid), "")
    except Exception as e:
        log_error(f"Talend Connector - Validate Connection Failed ", e)
        return (is_valid, str(e))


def __get_pipeline_data(properties: dict) -> dict:
    data = {}
    pipeline_id = properties.get("id")
    workspace_id = properties.get("workspace_id")
    request_url = f"workspaces/{workspace_id}/actions/{pipeline_id}"
    response = __get_response(request_url, is_metadata=True)
    if response:
        pipeline_content = response.get("content")
        dataset_content = pipeline_content.get("genericContent")
        dataset_list = []
        if dataset_content:
            dataset_list = dataset_content.get("datasets", [])
        datasets = []
        data = {
            "id": response.get("id"),
            "name": response.get("name"),
            "description": response.get("description"),
            "createDate": response.get("createDate"),
            "updateDate": response.get("updateDate"),
            "workspaceId": response.get("workspaceId"),
            "version": response.get("version"),
            "workspace_name": properties.get("workspace_name"),
            "environment_name": properties.get("environment_name"),
            "type": (
                "job"
                if response.get("jobType") == "standard"
                else response.get("jobType")
            ),
            "publisher": response.get("publisher"),
            "pipeline_detail": {
                "id": pipeline_content.get("id"),
                "name": pipeline_content.get("name"),
                "description": pipeline_content.get("description"),
                "details": pipeline_content.get("details"),
            },
        }
        for dataset in dataset_list:
            dataset_info = __get_dataset_detail(dataset.get("id"))
            datasets.append(dataset_info)
        data.update({"datasets": datasets})
    else:
        raise ValueError("No Artifact found")
    return data


def __get_runs(workspace_id: str, task_id: dict) -> dict:
    response = []
    request_url = (
        f"workspaces/{workspace_id}/flows/{task_id}/runHistory?offset=0&limit=10"
    )
    response = __get_response(request_url, is_metadata=True)
    response = response if response else []
    return response


def __get_tasks(properties: dict) -> dict:
    response = []
    try:
        pipeline_id = properties.get("id")
        workspace_id = properties.get("workspace_id")
        request_url = f"workspaces/{workspace_id}/actions/{pipeline_id}/listOfUse"
        response = __get_response(request_url, is_metadata=True)
        if response:
            response = response[0].get("utilization").get("tasks")
    except:
        response = []
    return response


def __get_dataset_detail(dataset_id: str) -> dict:
    response = {}
    request_url = dataset_id
    response = __get_response(request_url, is_dataset=True)
    if response:
        datastore = response.get("datastore")
        response.update(
            {
                "connection_type": datastore.get("typeLabel"),
                "connection_name": datastore.get("label"),
            }
        )
    return response


def _get_technical_name(name: str) -> str:
    """
    Returns the technical name for the given name
    """
    technical_name = ""
    if not name:
        return technical_name
    regex = "[^A-Za-z_0-9]"
    technical_name = re.sub(regex, "_", name.strip()).strip()
    return str(technical_name)
