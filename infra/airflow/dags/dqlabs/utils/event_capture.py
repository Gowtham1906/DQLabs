import os
import requests
import json

from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.app_helper.workflow_helper import get_workflow_api_token
from dqlabs.app_helper.dq_helper import get_server_endpoint


def save_alert_event(config: dict, alerts: list, property: dict):
    """
    Save Alert Event
    """
    if alerts:
        alerts = f"""({','.join(f"'{alert}'" for alert in alerts)})"""
        query_string = f"""
                select 
                    metrics.id as metrics, metrics.connection_id as connection, metrics.asset_id as asset,
                    metrics.attribute_id as attribute, metrics.measure_id as measure, metrics.task_id as task
                from core.metrics where id in {alerts} and lower(metrics.drift_status) in ('high','medium', 'low')
            """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            alert_list = fetchall(cursor)
            alert_list = alert_list if alert_list else []
            # API Token
            token = get_workflow_api_token(config)

            # Check if token is None
            if token is None:
                log_info("No valid workflow API token found, skipping alert events")
                return

            for alert in alert_list:
                alert.update(
                    {
                        "event": "metrics",
                        "action": "create",
                        "event_type": "create_alert",
                        "properties": property,
                        "source": "airflow",
                    }
                )
                send_event(alert, token)

def save_groupby_alerts_event(config: dict, alerts: list, property: dict):
    """
    Save Groupby Alerts Event
    """
    if alerts:
        alerts = f"""({','.join(f"'{alert}'" for alert in alerts)})"""
        query_string = f"""
            select 
                metrics.id as groupby_metrics, metrics.connection_id as connection, metrics.asset_id as asset,
                metrics.attribute_id as attribute, metrics.measure_id as measure
            from core.groupby_metrics as metrics where id in {alerts} and lower(metrics.drift_status) in ('high','medium', 'low')
        """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            alerts_list = fetchall(cursor)
            alerts_list = alerts_list if alerts_list else []
            # API Token
            token = get_workflow_api_token(config)
            for alert in alerts_list:
                alert.update(
                    {
                        "event": "metrics",
                        "action": "create",
                        "event_type": "groupby_alert",
                        "properties": property,
                        "source": "airflow",
                    }
                )
                send_event(alert, token)

def capture_job_execution(config: dict, status: str):
    token = get_workflow_api_token(config)

    # Check if token is None
    if token is None:
        log_info("No valid workflow API token found, skipping execution event")
        return
    connection = config.get("connection", {})
    connection_name = connection.get("name", "")
    if config.get("asset_id"):
        notification_params = {
            "status": "Started" if status == "Running" else status,
            "job_name": (
                config.get("dag_category")
                if config.get("dag_category")
                else config.get("category")
            ),
            "asset_id": config.get("asset_id"),
            "attribute_id": config.get("attribute_id"),
            "asset_name": config.get("technical_name"),
            "attribute_name": config.get("attribute_name", ""),
            "type": "schedule",
            "organization_id": config.get("organization_id"),
            "connection_id": config.get("connection_id"),
            "asset_group": config.get("asset").get("group"),
            "connection_name": connection_name,
            "run_id": config.get("queue_id"),
        }
    elif config.get("measure_id"):
        notification_params = {
            "status": "Started" if status == "Running" else status,
            "job_name": (
                config.get("dag_category")
                if config.get("dag_category")
                else config.get("category")
            ),
            "measure_id": config.get("measure_id"),
            "measure_name": config.get("measure_name"),
            "type": "schedule",
            "organization_id": config.get("organization_id"),
            "connection_id": config.get("connection_id"),
            "connection_name": connection_name,
            "run_id": config.get("queue_id"),
        }
    queue_detail_id = config.get("queue_detail_id")
    execution_params = {
        "asset": config.get("asset_id"),
        "measure": config.get("measure_id"),
        "connection": config.get("connection_id"),
        "attribute": config.get("attribute_id"),
        "module_id": queue_detail_id,
        "event": "job",
        "action": "run",
        "event_type": "job_execution",
        "properties": notification_params,
        "source": "airflow",
    }
    send_event(execution_params, token)


def save_sync_event(config: dict, sync_data: dict):
    """
    Save Sync Event
    """
    token = get_workflow_api_token(config)
    if token is None:
        log_info("No valid workflow API token found, skipping sync events")
        return
    asset_id = sync_data.get("asset")

    query_string = f"""
        select 
            asset.id as asset, asset.name as asset_name, asset.connection_id as connection, asset.organization_id as organization
        from core.asset where id = '{asset_id}'
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        response = fetchone(cursor)
    
    # Prepare sync event data
    event_data = {
        "event": "sync",
        "action": sync_data.get("action", ''),
        "event_type": "sync",
        "properties": sync_data.get("properties", {}),
        "source": "airflow",
        "connection": response.get("connection", ''),
        "asset": response.get("asset", ''),
        "module_id": response.get("organization", ''),
    }
    
    send_event(event_data, token)


def send_event(data, token: dict):
    try:
        # Check if token is None
        if token is None:
            log_info("No valid token for event capture, skipping")
            return

        server_endpoint = get_server_endpoint()
        if server_endpoint and server_endpoint.endswith("/"):
            server_endpoint = "/".join(server_endpoint.split("/")[:-1])
        headers = {
            "client-id": token.get("client_id"),
            "client-secret": token.get("client_secret"),
            "Content-Type": "application/json",
        }
        endpoint = f"{server_endpoint}/api/event/"
        requests.post(url=endpoint, headers=headers, data=json.dumps(data))
    except Exception as e:
        log_error("Event capture: ", e)
