"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.lineage_helper import save_lineage
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper import agent_helper


def extract_fivetran_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        is_valid = __validate_connection_establish(config)
        if is_valid:
            data = {}
            data = {**asset_properties}
            connector_id = asset_properties.get("connector_id")
            schema_metadata = __get_metadata(config, connector_id, "schemas")
            table_metadata = __get_metadata(config, connector_id, "tables")
            columns_metadata = __get_metadata(config, connector_id, "columns")
            nested_arr = __create_nested_array(
                schema_metadata, table_metadata, columns_metadata
            )
            data["metadata"] = nested_arr
            data.update(
                {
                    "schema_count": len(schema_metadata),
                    "table_count": len(table_metadata),
                    "column_count": len(columns_metadata),
                    "metadata": nested_arr,
                }
            )
            save_lineage(config, "pipeline", None, data, asset_id)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)

    except Exception as e:
        log_error("Fivetran Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __get_response(config: dict, url: str = "", method_type: str = "get", params=None):
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


def __validate_connection_establish(config: dict) -> tuple:
    try:
        is_valid = False
        response = __get_response(config, "groups")
        is_valid = response.get("is_valid")
        error = response.get("error")
        if error:
            raise Exception(error)
        return (bool(is_valid), "")
    except Exception as e:
        log_error.error(
            f"Fivetran Connector - Validate Connection Failed - {str(e)}", exc_info=True
        )
        return (is_valid, str(e))


def __get_metadata(config: dict, connector_id: str, level: str) -> list:
    try:
        request_url = f"metadata/connectors/{connector_id}/{level}"
        response = __get_response(config, request_url)
        response_data = response.get("data", {})
        items = response_data.get("items")
        # if items:
        #     data["metadata"] = [*data["metadata"], *items]
    except Exception as e:
        log_error.error(
            f"Fivetran Connector - Get Schema Metadata - {str(e)}", exc_info=True
        )
    finally:
        return items


def __create_nested_array(
    schema_metadata: list, table_metadata: list, columns_metadata: list
) -> list:
    nested_array = []
    for elem1 in schema_metadata:
        nested_elem1 = {**elem1, "table": []}
        for elem2 in table_metadata:
            if elem2["parent_id"] == elem1["id"]:
                nested_elem2 = {**elem2, "column": []}
                for elem3 in columns_metadata:
                    if elem3["parent_id"] == elem2["id"]:
                        nested_elem3 = {**elem3}
                        nested_elem2["column"].append(nested_elem3)
                nested_elem1["table"].append(nested_elem2)
        nested_array.append(nested_elem1)
    return nested_array
