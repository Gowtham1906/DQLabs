"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
from datetime import datetime, timedelta
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status

from dqlabs.app_helper.lineage_helper import (
    save_lineage,
    handle_alerts_issues_propagation,
    update_pipeline_propagations,
    save_lineage_entity,
    get_asset_metric_count, update_asset_metric_count
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from uuid import uuid4
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.pipeline_helper import update_pipeline_last_runs
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks


def extract_databricks_pipeline_data(config, **kwargs):
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
        version_id = asset.get("version_id")
        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        connection_metadata = credentials.get("metadata")
        is_valid, e = __validate_connection_establish(config)
        lineage = {"tables": [], "relations": []}
        all_runs = []
        latest_run = False
        if is_valid:
            data = {}
            description = ""
            data = {**asset_properties}
            type = asset_properties.get("type")
            id = asset_properties.get("workflow_id")
            pipeline_id = __get_pipeline_id(config)
            last_run_id = ""

            if type.lower() == "pipeline":
                workflow_info, description, stats = get_pipeline_info(config, credentials, connection_metadata, id)
                # update last runs in asset properties
                updated_asset_properties = {
                    **asset_properties,
                    "last_runs": workflow_info.get("last_runs")[0] if len(workflow_info.get("last_runs")) > 0 else {}
                }
                # check if last run is failed & trigger alert so that doesn't repeat on each run
                if len(workflow_info.get("last_runs")):
                    last_run_id = workflow_info.get("last_runs")[0].get("update_id")
                    # last_run_status = workflow_info.get("last_runs")[0].get("state")
                    # asset_last_run_id = asset_properties.get("last_runs").get("update_id") if asset_properties.get("last_runs") else None
                    # if (last_run_id != asset_last_run_id and last_run_status.lower() == "failed") or (asset_last_run_id is None):
                    #     print ("TRIGGER ALERT", last_run_id)
                        
            else:
                workflow_info, description, stats, entities = get_job_info(config, credentials, connection_metadata, id)
                updated_asset_properties = asset_properties
            
            if connection_metadata.get("tasks"):
                # save tasks in pipeline_task table
                new_tasks = __save_pipeline_tasks(
                    config,
                    workflow_info,
                    pipeline_id,
                    asset_id,
                    asset_properties,
                    type.lower()
                )

                if new_tasks:
                    create_pipeline_task_measures(config, new_tasks)
            
            if connection_metadata.get("runs"):
                # save runs
                latest_run, all_runs = __save_pipeline_runs(
                    config,
                    workflow_info,
                    asset_id,
                    pipeline_id,
                    type.lower(),
                    workflow_info.get("workflow_events")
                )
                # Update Propagations Alerts and Issues Creation and Notifications
                __update_last_run_stats(config, type.lower(), last_run_id)

            warehouse_id = __get_warehouse_id(config, credentials)
            lineage_tables = __prepare_lineage_info(
                config, id, warehouse_id, connection_type
            )
            lineage_relations = __prepare_lineage_relations(config, id, warehouse_id)
            lineage.update({"tables": lineage_tables, "relations": lineage_relations})
            workflow_info.update({"lineage": lineage})

            data.update({"workflow_info": workflow_info, "stats": stats})
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                updated_asset_properties = json.dumps(updated_asset_properties, default=str)
                updated_asset_properties = updated_asset_properties.replace("'", "''")
                query_string = f"""
                    update core.asset set description='{description}',
                    properties='{updated_asset_properties}'
                    where id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
            
            if type.lower() == "job":
                entities_relations = get_task_relations(entities, config, asset_id)
                lineage.update({"relations": entities_relations + lineage['relations']})
                save_lineage(config, "pipeline", lineage, asset_id)
                # save lineage entity
                #save_lineage_entity(config, entities, asset_id, True)

        # save Propagation Values
        update_pipeline_propagations(config, asset, connection_type.lower())
        
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
            extract_pipeline_measure(config, all_runs, connection_metadata.get("tasks"))

        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Databricks Pipeline pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __get_response(config: dict, url: str = "", method_type: str = "get", params=""):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute_databricks_api_request",
            parameters=dict(
                request_url=url, request_type=method_type, request_params=params
            ),
        )
        return api_response
    except Exception as e:
        log_error.error(
            f"Databricks Connector - Get Response Failed - {str(e)}", exc_info=True
        )
    finally:
        return api_response


def __validate_connection_establish(config) -> tuple:
    try:
        is_valid = False
        is_valid = __get_response(config, "api/2.0/pipelines")
        return (bool(is_valid), "")
    except Exception as e:
        log_error.error(
            f"Databricks Connector - Validate Connection Failed - {str(e)}",
            exc_info=True,
        )
        return (is_valid, str(e))


def get_pipeline_info(config, credentials, connection_metadata, pipeline_id: str):

    try:
        stats = {}
        workflow_data = {}
        # Get databricks pipeline details
        request_url = f"api/2.0/pipelines/{pipeline_id}"
        update_request_url = f"api/2.0/pipelines/{pipeline_id}/updates"
        response = __get_response(config, request_url)
        latest_updates = __get_response(config, update_request_url)
        pipeline_last_runs = []
        pipeline_last_state = ""
        if latest_updates:
            pipeline_last_runs = latest_updates.get("updates", [])
            pipeline_last_state = (
                pipeline_last_runs[0].get("state", "") if pipeline_last_runs else ""
            )
        if response:
            pipeline_name = response.get("name", "")
            catalog = response.get("spec").get("catalog", "")
            schema = response.get("spec").get("target", "")
            owner = response.get("creator_user_name", "")
            last_modified = response.get("last_modified", "")
            failed = True if pipeline_last_state in ["FAILED", "CANCELED"] else False
            workflow_data.update(
                {
                    "pipeline_id": response.get("pipeline_id", ""),
                    "pipeline_type": response.get("spec").get("pipeline_type", ""),
                    "pipeline_name": pipeline_name,
                    "target_schema": schema,
                    "target_db": catalog,
                    "state": response.get("state", ""),
                    "cluster_id": response.get("cluster_id", ""),
                    "last_runs": pipeline_last_runs,
                    "last_modified": last_modified,
                    "owner": owner,
                    "pipeline_failed": failed,
                }
            )
            description = f"""This databricks Pipeline {pipeline_name} is related to Database {catalog} and Schema {schema}. Owned by {owner} and last modified at {last_modified}"""

        # Get databricks pipeline events
        workflow_events = []
        if connection_metadata.get("runs"):
            no_of_days = credentials.get("no_of_runs", 30)
            # Calculate timestamp for n days ago
            no_of_days_ago = datetime.now() - timedelta(days=int(no_of_days))
            timestamp_ms = int(no_of_days_ago.timestamp() * 1000)  # Convert to milliseconds
            request_url = f"api/2.0/pipelines/{pipeline_id}/events?max_results=50"

            response = __get_response(config, request_url, "get", {"since_time": timestamp_ms})
            response = response.get("events", None) if response else None
            if response:
                for i in range(len(response)):
                    if response[i].get("event_type", "").lower() in ["maintenance_progress", "create_maintenance"]:
                        continue
                    workflow_events.append(
                        {
                            "event_id": response[i].get("id", ""),
                            "start_time": response[i].get("timestamp", ""),
                            "state_message": response[i].get("message", ""),
                            "result_state": response[i].get("level", ""),
                            "type": response[i].get("event_type", ""),
                            "maturity_level": response[i].get("maturity_level", ""),
                            "output_rows": response[i]
                            .get("details", {})
                            .get("flow_progress", {})
                            .get("metrics", {})
                            .get("num_output_rows", ""),
                            "upsert_rows": response[i]
                            .get("details", {})
                            .get("flow_progress", {})
                            .get("metrics", {})
                            .get("num_upserted_rows", ""),
                            "delete_rows": response[i]
                            .get("details", {})
                            .get("flow_progress", {})
                            .get("metrics", {})
                            .get("num_deleted_rows", ""),
                        }
                    )
        workflow_data.update({"workflow_events": workflow_events})
        stats.update({"runs": len(pipeline_last_runs), "run_failed": failed})
    except Exception as e:
        log_error.error(
            f"Databricks Connector - Get pipeline info - {str(e)}", exc_info=True
        )
    finally:
        return workflow_data, description, stats


def get_job_info(config, credentials, connection_metadata, job_id: str):
    try:
        stats = {}
        entities = []
        workflow_data = {}
        # Get databricks job details
        request_url = f"api/2.1/jobs/get?job_id={job_id}&expand_tasks=true"
        response = __get_response(config, request_url)
        if response:
            tasks = response.get("settings").get("tasks")
            job_name = response.get("settings", "").get("name", "")
            creator = response.get("creator_user_name")
            workflow_data.update(
                {
                    "job_id": response.get("job_id", ""),
                    "creator_user_name": creator,
                    "run_as_user_name": response.get("run_as_user_name"),
                    "task": tasks,
                    "format": response.get("settings").get("format"),
                    "job_name": job_name,
                }
            )
            description = f"""This databricks Job {job_name} is Owned by {creator} and has {len(tasks)} tasks"""
            # Get execution order
            entities = get_task_dependencies(config, response)

        # Get databricks job runs
        workflow_events = []
        failed = False
        if connection_metadata.get("runs"):
            # Get the number of days from credentials
            # If not provided, default to 30 days
            no_of_days = credentials.get("no_of_runs", 30)
            # Calculate timestamp for 10 days ago
            no_of_days_ago = datetime.now() - timedelta(days=int(no_of_days))
            timestamp_ms = int(no_of_days_ago.timestamp() * 1000)  # Convert to milliseconds
            request_url = f"api/2.1/jobs/runs/list?job_id={job_id}&start_time_from={timestamp_ms}&limit=20&expand_tasks=true"
            response = __get_response(config, request_url)
            response = response.get("runs", None) if response else None
            if response:
                last_state = response[0].get("state", "").get("result_state", "")
                failed = True if last_state == "FAILED" else failed
                for i in range(len(response)):
                    workflow_events.append(
                        {
                            "event_id": str(response[i].get("run_id", "")),
                            "start_time": response[i].get("start_time", ""),
                            "end_time": response[i].get("end_time", ""),
                            "run_duration": response[i].get("run_duration", ""),
                            "trigger": response[i].get("trigger", ""),
                            "type": response[i].get("run_type", ""),
                            "life_cycle_state": response[i]
                            .get("state", "")
                            .get("life_cycle_state", ""),
                            "result_state": response[i]
                            .get("state", "")
                            .get("result_state", ""),
                            "state_message": response[i]
                            .get("state", "")
                            .get("state_message", ""),
                            "tasks": response[i].get("tasks", [])
                        }
                    )
                stats.update({"runs": len(response), "run_failed": failed})
        workflow_data.update({"workflow_events": workflow_events, "job_failed": failed})

    except Exception as e:
        log_error.error(
            f"Databricks Pipeline Connector - Get job info - {str(e)}", exc_info=True
        )
    finally:
        return workflow_data, description, stats, entities


def __prepare_lineage_relations(config, workflow_id: str, warehouse_id: str):
    relations = []
    request_url = f"api/2.0/sql/statements/"
    query = f""" 
        select source_table_full_name as  srcTableId, target_table_full_name as tgtTableId,
        source_column_name as srcTableColName, target_column_name as tgtTableColName 
        from system.access.column_lineage 
        where entity_id='{workflow_id}' 
        and target_table_full_name is not null
    """
    params = {
        "warehouse_id": warehouse_id,
        "catalog": "main",
        "schema": "dqlabs",
        "statement": query,
    }
    try:
        response = __get_response(config, request_url, "post", params)
        if response:
            column_relations = response.get("result", {}).get("data_array", [])
            for item in column_relations:
                relations.append(
                    {
                        "srcTableId": item[0],
                        "tgtTableId": item[1],
                        "srcTableColName": item[2],
                        "tgtTableColName": item[3],
                    }
                )
    except Exception as e:
        log_error.error(f"Databricks get relations - {str(e)}", exc_info=True)
    finally:
        return relations


def __prepare_lineage_info(
    config, workflow_id: str, warehouse_id: str, connection_type
):
    tables = []
    request_url = f"api/2.0/sql/statements/"
    query = f""" 
        WITH tables AS (
    SELECT 
        CONCAT(target_table_catalog, '.', target_table_schema, '.', target_table_name) AS full_table_name 
    FROM 
        system.access.column_lineage 
    WHERE 
        entity_id = '{workflow_id}' AND target_table_name IS NOT NULL
    UNION
    SELECT 
        CONCAT(source_table_catalog, '.', source_table_schema, '.', source_table_name) AS full_table_name 
    FROM 
        system.access.column_lineage 
    WHERE 
        entity_id = '{workflow_id}' AND target_table_name IS NOT NULL
    ),
    columns AS (
        SELECT 
            CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_name,
            COLLECT_LIST(
                STRUCT(
                    column_name AS name,
                    'column' AS type,
                    comment AS comment,
                    data_type AS datatype,
                    '' AS description
                )
            ) AS columns_info
        FROM information_schema.columns
        WHERE CONCAT(table_catalog, '.', table_schema, '.', table_name) IN (SELECT full_table_name FROM tables)
        GROUP BY CONCAT(table_catalog, '.', table_schema, '.', table_name)
    )

    SELECT 
        t.full_table_name,
        c.source_table_name,
        c.source_type,
        c.created_by,
        c.source_table_schema,
        c.source_table_catalog,
        c.event_time,
        t.columns_info
    FROM 
        system.access.column_lineage AS c
    JOIN 
        columns AS t ON c.source_table_full_name = t.full_table_name
    WHERE 
        c.entity_id = '{workflow_id}'
        AND t.full_table_name IS NOT NULL;
    """
    params = {
        "warehouse_id": warehouse_id,
        "catalog": "main",
        "schema": "dqlabs",
        "statement": query,
    }
    try:
        response = __get_response(config, request_url, "post", params)
        if response:
            table_list = response.get("result", {}).get("data_array", [])
            for item in table_list:
                tables.append(
                    {
                        "id": item[0],
                        "name": item[1],
                        "type": item[2],
                        "alias": item[1],
                        "level": 3,
                        "Owner": item[3],
                        "fields": json.loads(item[7]),
                        "schema": item[4],
                        "src_id": "",
                        "database": item[5],
                        "uniqueId": item[0],
                        "dependsOn": [],
                        "node_type": "downstream",
                        "target_id": "",
                        "childrenL1": [],
                        "runElapsedTime": "",
                        "runGeneratedAt": item[6],
                        "connection_type": connection_type,
                    }
                )
    except Exception as e:
        log_error.error(f"Databricks get tables - {str(e)}", exc_info=True)
    finally:
        return tables


def __get_warehouse_id(config, credentials):
    request_url = f"api/2.0/sql/warehouses"
    try:
        response = __get_response(config, request_url)
        warehouse_id = ""
        if response:
            warehouses = response.get("warehouses", [])
            for warehouse in warehouses:
                if warehouse.get("state", "") in (
                    "STARTING",
                    "RUNNING",
                    "STOPPING",
                    "STOPPED",
                ) and warehouse.get("odbc_params").get(
                    "hostname", ""
                ) == credentials.get(
                    "server"
                ):
                    warehouse_id = warehouse.get("id", "")
                    return warehouse_id
    except Exception as e:
        log_error.error(f"Databricks get warehouses - {str(e)}", exc_info=True)
    finally:
        return warehouse_id


def __save_pipeline_tasks(
    config: dict,
    pipeline_info: dict,
    pipeline_id: str,
    asset_id: str,
    pipeline_properties: dict = {},
    pipeline_type: str = "job"
) -> None:
    """
    Save / Update Task For Pipeline
    """
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        pipeline_tasks = pipeline_info.get("task")
        source_type = "task"
        tasks = []
        if not pipeline_tasks:
            return

        with connection.cursor() as cursor:
            for task in pipeline_tasks:
                task_id = f"""{pipeline_type}.{pipeline_properties.get('name').lower()}.{task.get('task_key').lower().replace(" ","_")}"""
                task_start_datetime = task.get('start_time') if task.get('start_time') else None
                task_end_datetime = task.get('end_time') if task.get('end_time') else None
                if pipeline_type == "job":
                    task_start_datetime = datetime.fromtimestamp(task_start_datetime / 1000) if task_start_datetime else None
                    task_end_datetime = datetime.fromtimestamp(task_end_datetime / 1000) if task_end_datetime else None
                # Validate Existing Task Information
                query_string = f""" 
                    select id from core.pipeline_tasks 
                    where asset_id = '{asset_id}' and source_id = '{task_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get("id") if existing_task else None
                properties = {
                    **pipeline_properties,
                    "is_mapped": task.get("is_mapped", ""),
                    "retries": task.get("policy", {}).get("retry", 0),
                    "policy": task.get("policy", {}),
                    "compute": task.get("compute", {}),
                    "staging": task.get("staging", {}),
                }
                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            owner = '{task.get('owner', '')}',
                            run_start_at = {f"'{task_start_datetime}'" if task_start_datetime else 'NULL'},
                            run_end_at = {f"'{task_end_datetime}'" if task_end_datetime else 'NULL'},
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.now()}',
                            source_type = '{source_type}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        task_id,
                        task.get("task_key"),
                        task.get("owner", ""),
                        json.dumps(properties, default=str).replace("'", "''"),
                        task_start_datetime,
                        task_end_datetime,
                        source_type,
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.now(),
                        (
                            task.get("description")
                            if task.get("description")
                            else __prepare_description(
                                {"task_name": task.get("name"), **pipeline_properties},
                                "task",
                            )
                        ),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    tasks.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            # create each tasks
            tasks_input = split_queries(tasks)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)

                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                        run_end_at, source_type, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete,
                        created_at, description)
                        values {query_input}
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, tasks_insert_query)
                    pipeline_tasks = fetchall(cursor)
                    # Save Dataflow Columns
                    #__save_columns(config, columns, pipeline_id, pipeline_tasks)
                except Exception as e:
                    log_error("extract properties: inserting tasks level", e)
    except Exception as e:
        log_error(f"Databricks Pipeline Connector - Saving tasks ", e)
        raise e
    finally:
        return new_pipeline_tasks

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
                return pipeline.get("id")
    except Exception as e:
        log_error(
            f"Databricks Connector - Get Pipeline Primary Key Information By Asset ID Failed ",
            e,
        )
        raise e

def __prepare_description(properties, type="job") -> str:
    name = properties.get("name", "")
    project = properties.get("data_factory", "")
    environment = properties.get("resource_group", "")
    description = f"""This {name} pipeline is under the Project {project} and Environment {environment}."""
    if type == "task":
        name = properties.get("task_name", "")
        pipeline = properties.get("name", "")
        description = f"""This {name} task is part of the pipeline {pipeline} and under the Project {project} and Environment {environment}."""
    return description

def __get_pipeline_runs(
    config: dict,
    credentials: dict,
    pipeline_id: str
) -> None:
    no_of_days = credentials.get("no_of_runs", 30)
    # Calculate timestamp for 10 days ago
    no_of_days_ago = datetime.now() - timedelta(days=int(no_of_days))
    timestamp_ms = int(no_of_days_ago.timestamp() * 1000)  # Convert to milliseconds
    request_url = f"api/2.0/pipelines/{pipeline_id}/events"
    try:
        response = __get_response(config, request_url, "get", {"since_time": timestamp_ms})
        return response.get("events", [])     
    except Exception as e:
        log_error.error(f"Databricks get pipeline runs - {str(e)}", exc_info=True)

def __save_pipeline_runs(
    config: dict,
    data: dict,
    asset_id: str,
    pipeline_id: str,
    pipeline_type: str = "job",
    runs: list = []
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    all_runs = []
    latest_run = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        if not connection_id or not runs:
            return
        pipeline_runs = []
        all_pipeline_runs = []
        with connection.cursor() as cursor:
            # Clear Existing Runs Details
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            insert_objects = []
            for i, item in enumerate(runs):
                if pipeline_type == "pipeline":
                    if item.get("type", "").lower() in ["maintenance_progress", "create_maintenance"]:
                        continue
                
                started_at = item.get("start_time") if item.get("start_time") else None
                finished_at = item.get("end_time") if item.get("end_time") else item.get("start_time")
                if pipeline_type == "job":
                    started_at = datetime.fromtimestamp(item.get("start_time") / 1000) if item.get("start_time") else None
                    finished_at = datetime.fromtimestamp(item.get("end_time") / 1000) if item.get("end_time") else None

                run_id = item.get("event_id")
                source_id = f"""{pipeline_type}.{item.get('event_id').lower()}.{item.get('type').lower().replace(" ","_")}"""
                status_humanized = item.get("result_state", "").lower() if item.get("result_state") else ""
                if pipeline_type != 'job':
                    status_humanized = "failed" if any(x in item.get("state_message", "").lower() for x in ["failed", "canceled"]) else "success"
                error_message = item.get("state_message", "").replace("'", "''").lower()
                duration = (
                    item.get("duration")
                    if item.get("duration")
                    else item.get("run_duration")
                )  # in miliseconds
                duration = duration / 1000 if duration else 0  # in seconds
                individual_run = {
                    "id": item.get("event_id"),
                    "trigger_id": item.get("trigger_id", ""),
                    "account_id": item.get("account_id", ""),
                    "environment_id": item.get("environment_id", ""),
                    "project_id": item.get("project_id", ""),
                    "project_name": data.get("project_name", ""),
                    "runGeneratedAt":item.get("start_time", ""),
                    "environment_name": data.get("environment_name", ""),
                    "job_definition_id": item.get("job_definition_id", ""),
                    "status": item.get("status", item.get("state_message", "")),
                    "status_humanized": status_humanized,
                    "dbt_version": item.get("dbt_version", ""),
                    "git_branch": item.get("git_branch", ""),
                    "git_sha": item.get("git_sha", ""),
                    "status_message": item.get("state_message", ""),
                    "artifact_s3_path": item.get("artifact_s3_path", ""),
                    "created_at": item.get("created_at", datetime.now()),
                    "updated_at": item.get("updated_at", ""),
                    "dequeued_at": item.get("id"),
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "job": item.get("job", ""),
                    "environment": item.get("environment", ""),
                    "run_steps": item.get("run_steps", ""),
                    "in_progress": item.get("in_progress", ""),
                    "is_complete": item.get("is_complete", ""),
                    "is_success": item.get("is_success", ""),
                    "is_error": item.get("is_error", ""),
                    "is_cancelled": item.get("is_cancelled", ""),
                    "duration": duration,
                    "run_duration": item.get("run_duration", ""),
                    "job_id": source_id,
                    "unique_id": source_id,
                    "is_running": item.get("is_running", ""),
                    "href": item.get("href", ""),
                    "uniqueId": source_id,
                    "unique_job_id": item.get("job_id", ""),
                    "models": [
                        {**each_activity, "uniqueId": source_id}
                        for each_activity in item.get("dataflow_activities", [])
                    ],
                    "tasks": item.get("tasks", [])
                }

                # make properties
                properties = individual_run

                # Validating existing runs
                query_string = f"""
                    select id from core.pipeline_runs
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and source_id = '{individual_run.get('job_id')}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)
                existing_run = existing_run.get("id") if existing_run else None
                
                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{status_humanized}', 
                            error = '{error_message}',
                            run_start_at = {f"'{started_at}'" if started_at else 'NULL'},
                            run_end_at = {f"'{finished_at}'" if finished_at else 'NULL'},
                            duration = '{duration}' ,
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                    all_pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                else:
                    query_input = (
                        uuid4(),
                        individual_run.get("job_id"),
                        run_id,
                        status_humanized,
                        error_message,
                        individual_run.get('started_at') if individual_run.get('started_at') else None,
                        individual_run.get('finished_at') if individual_run.get('finished_at') else None,
                        duration,
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    all_pipeline_runs.append(query_input)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if not latest_run:
                        latest_run = True
                all_runs.append(individual_run)

            insert_objects = split_queries(insert_objects)

            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs = fetchall(cursor)
                except Exception as e:
                    log_error("Databricks Runs Insert Failed  ", e)
            # save individual run detail
            __save_runs_details(config, all_runs, pipeline_id, pipeline_runs)
            return all_pipeline_runs
    except Exception as e:
        log_error(f"Databricks Pipeline Connector - Saving Runs Failed ", e)
    finally:
        return latest_run, all_runs

def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    return next((item for item in data if item[filter_key] == value), None)

def __save_runs_details(
    config: dict, data: list, pipeline_id: str, pipeline_runs: list = None
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in data:
                pipeline_run = get_data_by_key(
                    pipeline_runs, run.get("uniqueId")
                )
                query_input = (
                    uuid4(),
                    run.get("id", ""),
                    pipeline_run.get("source_id", "") if pipeline_run else "",
                    "task",
                    run.get("job_id"),
                    get_pipeline_status(run.get("status")),
                    run.get("status", ""),
                    (
                        run.get("rawSql", "").replace("'", "''")
                        if run.get("rawSql")
                        else ""
                    ),
                    (
                        run.get("compiledSql", "").replace("'", "''")
                        if run.get("compiledSql")
                        else ""
                    ),
                    run.get("started_at", None),
                    run.get("finished_at", None),
                    run.get("duration", None),
                    True,
                    False,
                    pipeline_run.get("id", "") if pipeline_run else None,
                    pipeline_id,
                    asset_id,
                    connection_id,
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
                        insert into core.pipeline_runs_detail(
                            id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                            run_start_at, run_end_at, duration, is_active, is_delete,
                            pipeline_run_id, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("Databricks Pipeline Runs Details Insert Failed  ", e)
    except Exception as e:
        log_error(f"Databricks Pipeline Connector - Save Runs Details By Run ID Failed ", e)
        raise e
    

def __update_last_run_stats(config: dict, type: str, last_run_id: str) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id,status, run_start_at, run_end_at from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_start_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id') 
                status = last_run.get('status')
                last_run_at = last_run.get('run_start_at') if type == "pipeline" else last_run.get('run_end_at')
                pipeline_query_string = ""
                pipeline_query_string = f"""
                    update core.pipeline set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                asset = config.get("asset", {})
                handle_alerts_issues_propagation(config, run_id)
    except Exception as e:
        log_error(f"Databricks Pipeline Connector - Update Run Stats to Job Failed ", e)
        raise e

def get_task_dependencies(config, workflow_json):
    tasks = workflow_json["settings"]["tasks"]
    task_dependencies = []
    
    # First pass: create a mapping of task_key to their generated IDs
    task_ids = {task["task_key"]: str(uuid4()) for task in tasks}
    
    for task in tasks:
        task_key = task["task_key"]
        current_task_id = task_ids[task_key]
        
        dependencies = []
        if "depends_on" in task:
            for dep in task["depends_on"]:
                dependency_key = dep["task_key"]
                dependencies.append({
                    "target_task_key": dependency_key,
                    "srcTableId": task_ids[dependency_key]
                })
        else:
            dependencies.append({
                "target_task_key": "",
                "srcTableId": None
            })
            
        task_dependencies.append({
            "task_id": current_task_id,
            "tgtTableId": current_task_id,
            "name": task_key,
            "entity_name": task_key,
            "dependencies": dependencies,
            "connection_type": config.get("connection", {}).get("type", ""),
        })
    
    return task_dependencies


def get_task_relations(tasks_with_deps, config, asset_id):   
    relations = []
    task_key_to_source_id = {}
    if asset_id:
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                # Get all task_keys from tasks_with_deps
                task_keys = [task["name"] for task in tasks_with_deps]
                if task_keys:
                    # Build query to get source_id for each task_key
                    # Match by name field which contains the task_key
                    task_keys_str = "', '".join([key.replace("'", "''") for key in task_keys])
                    query_string = f"""
                        SELECT source_id, name
                        FROM core.pipeline_tasks 
                        WHERE asset_id = '{asset_id}' 
                        AND name IN ('{task_keys_str}')
                        AND is_active = true 
                        AND is_delete = false
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    results = fetchall(cursor)
                    
                    # Create mapping: task_key (name) -> source_id
                    for result in results:
                        task_name = result.get('name')
                        source_id = result.get('source_id')
                        if task_name and source_id:
                            task_key_to_source_id[task_name] = source_id
        except Exception as e:
            log_error(f"Databricks Pipeline - Get Task Relations - Failed to fetch source_ids", e)
    
    task_key_to_source_id = {}
    if asset_id:
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                # Get all task_keys from tasks_with_deps
                task_keys = [task["name"] for task in tasks_with_deps]
                if task_keys:
                    # Build query to get source_id for each task_key
                    # Match by name field which contains the task_key
                    task_keys_str = "', '".join([key.replace("'", "''") for key in task_keys])
                    query_string = f"""
                        SELECT source_id, name
                        FROM core.pipeline_tasks 
                        WHERE asset_id = '{asset_id}' 
                        AND name IN ('{task_keys_str}')
                        AND is_active = true 
                        AND is_delete = false
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    results = fetchall(cursor)
                    
                    # Create mapping: task_key (name) -> source_id
                    for result in results:
                        task_name = result.get('name')
                        source_id = result.get('source_id')
                        if task_name and source_id:
                            task_key_to_source_id[task_name] = source_id
        except Exception as e:
            log_error(f"Databricks Pipeline - Get Task Relations - Failed to fetch source_ids", e)

    for task in tasks_with_deps:
        tgtTableId = task["name"]
        # Get source_id for target task (fallback to task_key if not found)
        tgtSourceId = task_key_to_source_id.get(tgtTableId, tgtTableId)
        

        for dep in task["dependencies"]:
            if dep["srcTableId"]:  # Only create relations for actual dependencies
                srcTaskKey = dep["target_task_key"]
                # Get source_id for source task (fallback to task_key if not found)
                srcSourceId = task_key_to_source_id.get(srcTaskKey, srcTaskKey)
                
                relations.append({
                    "srcTableId": srcSourceId,  # Use source_id instead of task_key
                    "tgtTableId": tgtSourceId,  # Use source_id instead of task_key
                    "relationship_type": "depends_on",
                    "srcEntityName": next((t["entity_name"] for t in tasks_with_deps if t["task_id"] == dep["srcTableId"]), srcTaskKey),
                    "tgtEntityName": task["entity_name"]
                })
    
    return relations

def extract_pipeline_measure(config: dict, run_history: list, tasks_pull: bool):
    if not run_history:
        return
    
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
    
    run_history = sorted(run_history, key=lambda x: x.get("started_at"), reverse=True)[:2]
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
    job_run_detail = {
        "duration": latest_run.get("duration"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(config, "asset", job_run_detail, job_name=pipeline_name)

    # Get Tasks and execute measure
    if not tasks_pull:
        return
    tasks = get_pipeline_tasks(config)
    if not tasks:
        return
    latest_run_task = latest_run.get("tasks", [])
    previous_run_task = previous_run.get("tasks", [])
    for task in tasks:
        task_name = task.get("name")
        last_task_run_detail = next(
            (t for t in latest_run_task if t.get("task_key") == task_name), None
        )
        previous_run_detail = next(
            (t for t in previous_run_task if t.get("task_key") == task_name), None
        )
        if not last_task_run_detail:
            continue
        last_task_started_at = datetime.fromtimestamp(last_task_run_detail.get("start_time") / 1000)
        previous_task_started_at = datetime.fromtimestamp(previous_run_detail.get("start_time") / 1000)
        run_detail = {
            "duration": last_task_run_detail.get("execution_duration") / 1000,
            "last_run_date": last_task_started_at,
            "previous_run_date": previous_task_started_at,
        }
        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(config, "task", run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)