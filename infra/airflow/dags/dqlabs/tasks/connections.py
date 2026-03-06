"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

from dqlabs.utils.connections import (
    get_dq_connections,
    get_assets,
    manage_connection,
    manage_connection_pool,
    delete_connection,
    delete_connection_pool,
    create_default_connection,
)
from dqlabs.app_constants.dq_constants import DEFAULT_POSTGRES_CONN
from dqlabs.app_helper.log_helper import log_error



def create_airflow_connections(**kwargs) -> None:
    """
    Updates the dqscore of the last run for the given asset
    into postgres
    """
    try:
        dag_run = kwargs.get("dag_run")
        task_config = dag_run.conf if dag_run and dag_run.conf else {}

        is_default = task_config.get("is_default", False)
        is_pool = task_config.get("is_pool", False)
        is_delete = task_config.get("is_delete", False)
        connection_id = task_config.get("connection_id")
        admin_organization = task_config.get("admin_organization")
        core_connection_id = task_config.get("core_connection_id")
        core_connection_id = core_connection_id if core_connection_id else None
        if not admin_organization and not core_connection_id:
            core_connection_id = DEFAULT_POSTGRES_CONN
        task_config.update({"core_connection_id": core_connection_id})

        if is_default:
            default_connection_id = create_default_connection(task_config)
            if default_connection_id:
                task_config.update(
                    {"core_connection_id": default_connection_id})

        if is_delete:
            if is_pool:
                pool_name = task_config.get("pool_name")
                delete_connection_pool(pool_name)
            else:
                airflow_connection_id = task_config.get(
                    "airflow_connection_id")
                delete_connection(airflow_connection_id)
            return

        if is_pool:
            asset_id = task_config.get("asset_id")
            assets = get_assets(task_config, asset_id)
            if not assets:
                return

            for asset in assets:
                manage_connection_pool(asset, task_config, is_delete)
        else:
            if (
                is_default
                and task_config
                and task_config.get("connection_id")
                == task_config.get("default_connection_id")
            ):
                return

            connections = get_dq_connections(task_config, connection_id)
            if not connections:
                return

            for connection in connections:
                manage_connection(connection, task_config, is_delete)

    except Exception as e:
        log_error(f"Failed create airflow connection task", e)
        raise e
    finally:
        if task_config and "postgres_connection" in task_config:
            del task_config["postgres_connection"]
