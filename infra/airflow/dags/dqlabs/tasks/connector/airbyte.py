"""
Migration Notes From V2 to V3:
Migrations Completed
"""

import datetime
import json
import re
import sqlglot
from sqlglot import exp
from datetime import datetime, timedelta
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status, extract_table_name

from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.lineage_helper import (
    save_lineage,
    save_asset_lineage_mapping,
    save_lineage_entity,
    update_pipeline_propagations,
    handle_alerts_issues_propagation,
    get_asset_metric_count, update_asset_metric_count,
    map_asset_with_lineage
)
from dqlabs.app_helper import agent_helper
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from uuid import uuid4
from dqlabs.app_constants.dq_constants import ADF_DATASETS_TYPES
from dqlabs.app_helper.pipeline_helper import update_pipeline_last_runs, pipeline_auto_tag_mapping
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks

TASK_CONFIG = None


def extract_airbyte_data(config, **kwargs):
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
        connection_metadata = credentials.get("metadata")
        column_relations = []
        transformations = []
        all_pipeline_runs = []
        copy_activity_sql = {}
        pipeline_tasks = []
        latest_run = False
        TASK_CONFIG = config

        is_valid, is_exist, e = __validate_connection_establish(asset_properties)
        if not is_exist:
            raise Exception(
                f"Pipeline - {asset_properties.get('name')} doesn't exist in the datafactory - {asset_properties.get('data_factory')} "
            )
        lineage = {"tables": [], "relations": []}
        column_relations = []

        if is_valid:
            data = asset_properties if asset_properties else {}
            pipeline_name = asset_properties.get("name")
            pipeline_info, description= get_pipeline_info(
                data, pipeline_name, credentials
            )
            tasks_info = pipeline_info.get("task_info", {})
            pipeline_id = __get_pipeline_id(config)

             # update description
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.asset set description='{description}'
                    where id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
            
            # Save tasks if tasks_info is available
            pipeline_tasks = []
            if connection_metadata.get("tasks") and tasks_info:
                new_tasks, pipeline_tasks = __save_airbyte_pipeline_tasks(
                    config,
                    tasks_info,
                    pipeline_id,
                    asset_id,
                    asset_properties
                )
                # Create Pipeline Task Measures
                if new_tasks:
                    create_pipeline_task_measures(config, new_tasks)
                
                # Handle tag auto mapping (similar to DBT pattern)
                auto_tag_mapping = credentials.get("auto_mapping_tags", False)
                if auto_tag_mapping and pipeline_info.get("tags"):
                    __airbyte_tag_mapping(config, pipeline_info.get("tags"), asset_id)
                
                # Extract columns for tasks
                found_columns, entity_columns_list = __extract_airbyte_task_columns(
                    config,
                    pipeline_info,
                    tasks_info,
                    asset_properties
                )
                
                # Delete existing columns
                __delete_airbyte_columns(config, pipeline_id)
                
                # Get column relations (similar to ADF pattern)
                if found_columns:
                    for column in found_columns:
                        src_table_id = column.get("parent_entity_name")
                        target_table_id = column.get("parent_source_id")
                        if column.get("from_dataset") == "outbound":
                            src_table_id = column.get("parent_source_id")
                            target_table_id = column.get("parent_entity_name")
                        if column.get("from_dataset") == "transform":
                            src_table_id = column.get("parent_source_id")
                            target_table_id = column.get("parent_entity_name")
                        column_relations.append(
                            {
                                "srcTableId": src_table_id,
                                "tgtTableId": target_table_id,
                                "srcTableColName": column.get("name"),
                                "tgtTableColName": column.get("name"),
                            }
                        )
                
                # Save columns if found
                if found_columns:
                    __save_airbyte_columns(
                        config,
                        found_columns,
                        pipeline_id,
                        pipeline_tasks
                    )
                
                # Create lineage for each task
                lineage = __create_airbyte_task_lineage(
                    config,
                    pipeline_info,
                    tasks_info,
                    asset_properties,
                    entity_columns_list
                )
            
            # Save runs if runs are enabled
            if connection_metadata.get("runs"):
                pipeline_runs = __get_airbyte_pipeline_runs(config, asset, credentials)
                if pipeline_runs:
                    latest_run, all_runs = __save_airbyte_pipeline_runs(
                        config,
                        tasks_info,
                        asset_id,
                        pipeline_id,
                        "pipeline",
                        pipeline_runs,
                        pipeline_tasks
                    )
            # Save lineage if created
            if lineage :
                # Add column relations to lineage (similar to ADF pattern)
                lineage["relations"] = lineage.get("relations") + column_relations
                save_lineage_entity(config, lineage.get("tables"), asset_id, True)
                save_lineage(config, "pipeline", lineage, asset_id)
                
                # Generate Auto Mapping for lineage tables (excluding tasks)
                __map_airbyte_asset_with_lineage(config, lineage)
            
            # Update last run stats
            __update_airbyte_last_run_stats(config, "pipeline", connection_type,pipeline_tasks)

            if latest_run and all_runs:
                        # Get the latest run from database to ensure we have the most recent run_id
                db_connection = get_postgres_connection(config)
                with db_connection.cursor() as cursor:
                    query_string = f"""
                        select source_id
                        from core.pipeline_runs
                        where asset_id = '{asset_id}'
                        order by run_end_at desc
                        limit 1
                    """
                    cursor = execute_query(db_connection, cursor, query_string)
                    latest_run_result = fetchone(cursor)
                    latest_run_id = latest_run_result.get("source_id") if latest_run_result else ""
                    if latest_run_id:
                        handle_alerts_issues_propagation(config, latest_run_id)
            
            # # Update Job Run Stats
            __update_airbyte_pipeline_stats(config)
            update_pipeline_propagations(config, asset, connection_type)
            
            # Execute pipeline measures
            propagate_alerts = credentials.get("propagate_alerts", "table")
            metrics_count = None
            if propagate_alerts == "table":
                metrics_count = get_asset_metric_count(config, asset_id)
            if latest_run:
                __extract_airbyte_pipeline_measure(config)
            if propagate_alerts == "table" or propagate_alerts == "pipeline":
                update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_pipeline_last_runs(config)

  
    except Exception as e:
        log_error("Airbyte Pipeline pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error=e)


def __get_response(method_name: str, params: dict = {}):
    try:
        global TASK_CONFIG
        pg_connection = get_postgres_connection(TASK_CONFIG)
        api_response = agent_helper.execute_query(
            TASK_CONFIG,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(method_name=method_name, request_params=params),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection_establish(config) -> tuple:
    try:
        is_valid = False
        is_exist = False
        workspace_id = config.get("workspace_id", "")
        connection_asset_id = config.get("connection_id", "")
        params = dict(
            workspace_id=workspace_id,
            connection_id=connection_asset_id,
        )
        response = __get_response("validate_connection", params)
        is_exist = bool(response.get("is_exist"))
        is_valid = bool(response.get("is_valid"))
        return (bool(is_valid), bool(is_exist), response.get("connection_details", {}))
    except Exception as e:
        log_error("Airbyte Connector - Validate Connection Failed", e)
        return (is_valid, is_exist, str(e))


def get_pipeline_info(data, pipeline_name: str, credentials: dict) -> tuple:
    try:
        workspace_id = data.get("workspace_id", "")
        connection_asset_id = data.get("connection_id", "")
        params = dict(
            workspace_id=workspace_id,
            connection_asset_id=connection_asset_id,
        )
        response = __get_response("get_pipeline_info", params)
        pipeline_info = response.get("pipeline_info")
        description = response.get("description")
        if not description:
            description = __prepare_description(data)
        
        return pipeline_info, description
    except Exception as e:
        log_error("Airbyte Connector - Get Tasks info Failed", e)
    finally:
        return pipeline_info, description
        
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
            f"Airbyte Connector - Get Pipeline Primary Key Information By Asset ID Failed ",
            e,
        )
        raise e


def __get_airbyte_pipeline_runs(config, asset, credentials):
    """Get Airbyte pipeline runs from API"""
    try:
        no_of_days = credentials.get("no_of_runs", 30)
        # Calculate timestamp for n days ago
        days_ago_date = datetime.now() - timedelta(days=int(no_of_days))
        status = (credentials.get("status") or "all")
        
        workspace_id = asset.get("properties", {}).get("workspace_id", "")
        connection_asset_id = asset.get("properties", {}).get("connection_id", "")
        
        params = dict(
            workspace_id=workspace_id,
            connection_asset_id=connection_asset_id,
            after_date=str(days_ago_date),
            status=status,
        )
        
        response = __get_response("get_pipeline_runs", params)
        results = response.get("runs", []) if response else []
        
        if results and isinstance(results, list):
            def _normalize_status(value):
                if value is None:
                    return None
                v = str(value).strip().lower()
                if v in ["1", "completed", "complete", "success", "succeeded"]:
                    return "success"
                if v in ["0", "error", "failed", "failure", "cancelled", "canceled"]:
                    return "failed"
                if v in ["2", "running", "active"]:
                    return "running"
                return v

            selected_status = _normalize_status(status)
            is_filter_applied = False if selected_status == "all" else selected_status in ["success", "failed", "running"]
            def _parse_dt(value):
                try:
                    if isinstance(value, str):
                        # Handle ISO format with Z suffix
                        if value.endswith('Z'):
                            return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
                        else:
                            return datetime.fromisoformat(value)
                    if isinstance(value, (int, float)):
                        ts = value / 1000 if value > 1e12 else value
                        return datetime.fromtimestamp(ts)
                except Exception as e:
                    log_error(f"Date parsing error for '{value}'", e)
                    return None

            cutoff_date = days_ago_date
            filtered_results = []
            for item in results:
                # Use finished_at instead of endTime for Airbyte API response
                end_time = item.get("finished_at") or item.get("endTime")
                parsed_end_time = _parse_dt(end_time) if end_time else None
                end_time_ok = end_time and parsed_end_time and parsed_end_time >= cutoff_date
                if not end_time_ok:
                    continue
                item_status = _normalize_status(item.get("status"))
                if not is_filter_applied:
                    filtered_results.append(item)
                    continue
                if selected_status == "success" and item_status == "success":
                    filtered_results.append(item)
                elif selected_status == "failed" and item_status == "failed":
                    filtered_results.append(item)
                elif selected_status == "running" and item_status == "running":
                    filtered_results.append(item)
            return filtered_results
        else:
            return []
    except Exception as e:
        log_error("Airbyte Pipeline Runs Failed", e)
        return []


def __save_airbyte_pipeline_tasks(
    config: dict,
    tasks_info: dict,
    pipeline_id: str,
    asset_id: str,
    asset_properties: dict,
):
    """Save Airbyte pipeline tasks to database"""
    new_pipeline_tasks = []
    pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        if not tasks_info:
            return [], []

        with connection.cursor() as cursor:
            insert_objects = []
            for idx, task in enumerate(tasks_info, start=1):
                task_id = task.get("task_id", "")
                task_name = task.get("task_name","")
                task_type = task.get("type", "sync")
                task_status = task.get("task_status","")
                
                # Build deterministic source_id
                source_id = f"{str(connection_id)}.{task_id}"
                
                # Check if task already exists (ADF pattern)
                query_string = f""" 
                    select id from core.pipeline_tasks 
                    where asset_id = '{asset_id}' and source_id = '{source_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get("id") if existing_task else None
                
                properties = {
                    "task_name": task_name,
                    "task_id": task_id,
                    "task_type": task_type,
                    "source_connection": task.get("source_connection", {}),
                    "destination_connection": task.get("destination_connection", {}),
                    "sync_config": task.get("sync_config", {}),
                    "source": "airbyte",
                    "pipeline_name": asset_properties.get("name", ""),
                    "workspace_name": asset_properties.get("workspace_name", ""),
                }
                
                if existing_task:
                    # Update existing task (ADF pattern)
                    query_string = f"""
                        update core.pipeline_tasks set 
                            name = '{task_name}',
                            status = '{task_status}',
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.now()}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    new_pipeline_tasks.append(existing_task)
                    pipeline_tasks.append({
                        "id": existing_task,
                        "source_id": source_id,
                        "name": task_name,
                    })
                else:
                    # Insert new task
                    db_task_id = str(uuid4())
                    query_input = (
                        db_task_id,
                        source_id,
                        task_name,
                        "",
                        json.dumps(properties, default=str).replace("'", "''"),
                        None,
                        None,
                        'task',
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.now(),
                        __prepare_description(properties, "task"),
                        task_status,
                        ""
                    )
                    
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    new_pipeline_tasks.append(db_task_id)
            
            if insert_objects:
                inputs_split = split_queries(insert_objects)
                for input_values in inputs_split:
                    try:
                        query_input = ",".join(input_values)
                        tasks_insert_query = f"""
                            insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                            run_end_at, source_type, pipeline_id, asset_id, connection_id, is_selected, is_active, is_delete,
                            created_at, description, status, error)
                            values {query_input}
                            RETURNING id, source_id, name;
                        """
                        cursor = execute_query(connection, cursor, tasks_insert_query)
                        inserted_tasks = fetchall(cursor)
                        pipeline_tasks.extend(inserted_tasks)
                    except Exception as e:
                        log_error("Airbyte: inserting tasks", e)
        
        return new_pipeline_tasks, pipeline_tasks
    except Exception as e:
        log_error("Airbyte - Save Pipeline Tasks Failed", e)
        raise e


def __save_airbyte_pipeline_runs(
    config: dict,
    tasks_info: dict,
    asset_id: str,
    pipeline_id: str,
    pipeline_type: str = "pipeline",
    runs: list = [],
    pipeline_tasks: list = []
) -> tuple:
    """Save Airbyte pipeline runs to database"""
    all_runs = []
    latest_run = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        if not connection_id or not runs:
            return latest_run, all_runs
            
        pipeline_runs = []
        all_pipeline_runs = []
        
        with connection.cursor() as cursor:

            insert_objects = []
            for i, item in enumerate(runs):
                started_at = item.get("started_at") if item.get("started_at") else None
                finished_at = item.get("finished_at") if item.get("finished_at") else item.get("started_at")
                
                run_id = str(uuid4())
                name_value = item.get("name") or f"run_{i}"
                safe_name = str(name_value).lower().replace(" ", "_")
                
                # Build deterministic source_id
                ts_basis = started_at or finished_at or f"{i}"
                safe_ts = str(ts_basis).replace(":", "").replace("-", "").replace("Z", "").replace(".", "").replace("+00:00", "").replace("T", "t")
                source_id = item.get("run_id")

                def _status_to_human(value):
                    if value is None:
                        return ""
                    v = str(value).strip().lower()
                    if v in ["1", "completed", "complete", "success", "succeeded"]:
                        return "success"
                    if v in ["0", "error", "failed", "failure"]:
                        return "failed"
                    if v in ["cancelled", "canceled"]:
                        return "cancelled"
                    if v in ["2", "running", "active"]:
                        return "running"
                    return v

                status_humanized = _status_to_human(item.get("status"))
                error_message = item.get("errorMessage", "").replace("'", "''").lower()
                duration = None
                if started_at and finished_at:
                    try:
                        started_datetime = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                        finished_datetime = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                        time_delta = finished_datetime - started_datetime
                        duration = time_delta.total_seconds()
                    except Exception:
                        duration = None
                
                individual_run = {
                    "id": run_id,
                    "runGeneratedAt": started_at,
                    "status": item.get("status", ""),
                    "status_humanized": status_humanized,
                    "status_message": item.get("statusMessage", ""),
                    "created_at": item.get("started_at", datetime.now()),
                    "updated_at": item.get("finished_at", ""),
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "duration": duration,
                    "run_duration": duration,
                    "error_message": error_message,
                    "job_id": source_id,
                    "unique_id": source_id,
                    "uniqueId": source_id,
                }

                # make properties
                properties = individual_run

                # Validating existing runs (idempotent upsert by deterministic source_id)
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
                            duration = {f"'{duration}'" if duration is not None else 'NULL'},
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append({
                        "id": existing_run,
                        "source_id": individual_run.get("job_id"),
                        "is_update": True,
                    })
                    all_pipeline_runs.append({
                        "id": existing_run,
                        "source_id": individual_run.get("job_id"),
                        "is_update": True,
                    })
                else:
                    query_input = (
                        run_id,
                        individual_run.get("job_id"),
                        str(uuid4()),
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
                    all_pipeline_runs.extend(pipeline_runs)
                except Exception as e:
                    log_error("Airbyte Runs Insert Failed", e)
            # save individual run detail
            __save_airbyte_runs_details(config, all_runs, pipeline_id, all_pipeline_runs, pipeline_tasks)
            return latest_run, all_runs
    except Exception as e:
        log_error(f"Airbyte Pipeline Connector - Saving Runs Failed", e)
        return latest_run, all_runs


def __save_airbyte_runs_details(
    config: dict, data: list, pipeline_id: str, pipeline_runs: list = None,
    pipeline_tasks: list = []
):
    """Save Airbyte pipeline run details to database - following ADF pattern"""
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        insert_objects = []
        connection = get_postgres_connection(config)
        
        with connection.cursor() as cursor:
            update_values = []          
            for run in data:
                # Get pipeline_run_id from the pipeline_runs list (similar to ADF pattern)
                pipeline_run_id = None
                if pipeline_runs:
                    for pr in pipeline_runs:
                        if isinstance(pr, dict) and pr.get("source_id") == run.get("uniqueId"):
                            pipeline_run_id = pr.get("id")
                            break
                        elif isinstance(pr, tuple) and len(pr) > 1:
                            # Handle tuple format from insert operations
                            if pr[1] == run.get("uniqueId"):  # source_id is typically at index 1
                                pipeline_run_id = pr[0]  # id is typically at index 0
                                break
                
                if not pipeline_run_id:
                    # Skip if the parent pipeline_run row is not present to avoid FK violation
                    continue
                
                # Get run details
                run_id = run.get("job_id", "")
                status_humanized = get_pipeline_status(run.get("status"))
                error_message = run.get("error_message", "").replace("'", "''")
                started_at = run.get("started_at", None)
                finished_at = run.get("finished_at", None)
                duration = run.get("duration", None)

                for task in pipeline_tasks:
                    task_source_id = task.get("source_id")
                    task_name = task.get("name") or task_source_id
                    
                    query_string = f"""
                        SELECT id FROM core.pipeline_runs_detail
                        WHERE pipeline_run_id = '{pipeline_run_id}'
                        AND source_id = '{task_source_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    existing_detail = fetchone(cursor)
                    existing_detail_id = existing_detail.get("id") if existing_detail else None
                    
                    if existing_detail_id:
                        # Collect update values for batch update
                        update_values.append((
                            existing_detail_id,
                            status_humanized,
                            error_message,
                            started_at,
                            finished_at,
                            duration
                        ))
                    else:
                        # Insert new record
                        query_input = (
                            uuid4(),
                            run_id,                     # run_id (from run data)
                            task_source_id,           # source_id (task-specific)
                            "task",                    # type
                            task_name,                 # name
                            status_humanized,          # status
                            error_message,             # error
                            "",                       # source_code (not applicable for Airbyte)
                            "",                       # compiled_code (not applicable for Airbyte)
                            started_at,               # run_start_at
                            finished_at,              # run_end_at
                            duration,                 # duration
                            True,                     # is_active
                            False,                    # is_delete
                            pipeline_run_id,          # pipeline_run_id (FK to core.pipeline_runs.id)
                            pipeline_id,              # pipeline_id
                            asset_id,                 # asset_id
                            connection_id,            # connection_id
                        )
                        
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals})", query_input
                        ).decode("utf-8")
                        insert_objects.append(query_param)
            
            # Perform batch update for existing records (following LLM message pattern)
            if update_values:
                update_params = []
                for detail_id, status, error, start_at, end_at, dur in update_values:
                    query_input = (
                        detail_id,
                        status,
                        error,
                        start_at,
                        end_at,
                        dur
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    update_params.append(query_param)
                
                # Split into batches if needed
                update_batches = split_queries(update_params)
                
                for batch_values in update_batches:
                    try:
                        query_input = ",".join(batch_values)
                        batch_update_query = f"""
                            UPDATE core.pipeline_runs_detail
                            SET 
                                status = update_data.status,
                                error = update_data.error,
                                run_start_at = CASE 
                                    WHEN update_data.run_start_at IS NULL THEN NULL 
                                    ELSE update_data.run_start_at::timestamp with time zone 
                                END,
                                run_end_at = CASE 
                                    WHEN update_data.run_end_at IS NULL THEN NULL 
                                    ELSE update_data.run_end_at::timestamp with time zone 
                                END,
                                duration = update_data.duration
                            FROM (
                                VALUES {query_input}
                            ) AS update_data(id, status, error, run_start_at, run_end_at, duration)
                            WHERE pipeline_runs_detail.id::text = update_data.id::text
                        """
                        cursor = execute_query(connection, cursor, batch_update_query)
                    except Exception as e:
                        log_error(f"Error in batch update for Airbyte run details: {e}", e)
                        continue

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
                    log_error("Airbyte Pipeline Runs Details Insert Failed", e)
    except Exception as e:
        log_error(f"Airbyte Pipeline Connector - Save Runs Details By Run ID Failed", e)
        raise e


def __update_airbyte_last_run_stats(config: dict, type: str, connection_type: str = "",pipeline_tasks: list = []) -> str:
    """Update last run statistics for Airbyte pipeline"""
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id, status, run_start_at, run_end_at, duration from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_start_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id') 
                status = last_run.get('status')
                last_run_at = last_run.get('run_start_at') if type == "pipeline" else last_run.get('run_end_at')
                
                # Handle None values properly for SQL
                last_run_at_sql = f"'{last_run_at}'" if last_run_at is not None else 'NULL'
                run_id_sql = f"'{run_id}'" if run_id is not None else 'NULL'
                status_sql = f"'{status}'" if status is not None else 'NULL'
                
                pipeline_query_string = f"""
                    update core.pipeline set run_id={run_id_sql}, status={status_sql}, last_run_at={last_run_at_sql}
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                
                last_run_start_at = last_run.get('run_start_at')
                last_run_end_at = last_run.get('run_end_at')
                duration = last_run.get('duration')
                
                # Handle None values for task update
                last_run_start_at_sql = f"'{last_run_start_at}'" if last_run_start_at is not None else 'NULL'
                last_run_end_at_sql = f"'{last_run_end_at}'" if last_run_end_at is not None else 'NULL'
                duration_sql = f"'{duration}'" if duration is not None else 'NULL'
                run_id_task_sql = f"'{run_id}'" if run_id is not None else 'NULL'
                
                source_ids = "', '".join(task.get("source_id") for task in pipeline_tasks)
                update_task_query = f""" 
                    update core.pipeline_tasks set 
                        run_id={run_id_task_sql}, 
                        run_start_at={last_run_start_at_sql}, 
                        run_end_at={last_run_end_at_sql}, 
                        duration={duration_sql},
                        status={status_sql}
                    where asset_id = '{asset_id}' and source_id in ('{source_ids}')
                """
                execute_query(connection, cursor, update_task_query)
                tasks_deprectated_query = f"""
                    update core.pipeline_tasks set
                        status='DEPRECATED', is_active=false, is_delete=true
                    where asset_id = '{asset_id}' and source_id not in ('{source_ids}')
                """
                execute_query(connection, cursor, tasks_deprectated_query)
    except Exception as e:
        log_error(f"Airbyte Pipeline Connector - Update Run Stats to Job Failed", e)
        raise e


def __update_airbyte_pipeline_stats(config: dict) -> str:
    """Update pipeline statistics for Airbyte"""
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Get Last Run Info
            query_string = f"""
                select
                    id,
                    source_id,
                    run_end_at,
                    duration,
                    status
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            last_run = last_run if last_run else {}

            # Get Pipeline Stats
            query_string = f"""
                SELECT 
                pipeline.id,
                COUNT(DISTINCT pipeline_columns.id) as tot_columns,
                COUNT(DISTINCT pipeline_runs.id) as tot_runs,
                COUNT(DISTINCT pipeline_transformations.id) as tot_transformations
            FROM core.pipeline
            LEFT JOIN core.pipeline_columns ON pipeline_columns.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_runs ON pipeline_runs.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_transformations ON pipeline_transformations.asset_id = pipeline.asset_id
            WHERE pipeline.asset_id = '{asset_id}'
            GROUP BY pipeline.id;
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_stats = fetchone(cursor)
            pipeline_stats = pipeline_stats if pipeline_stats else {}

            # Update Pipeline Status
            query_string = f"""
                SELECT id, properties FROM core.pipeline
                WHERE asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            
            if report:
                properties = report.get("properties", {})
                properties.update({
                    "tot_columns": pipeline_stats.get("tot_columns", 0),
                    "tot_runs": pipeline_stats.get("tot_runs", 0),
                    "tot_transformations": pipeline_stats.get("tot_transformations", 0)
                })

                run_id = last_run.get("source_id", "")
                status = last_run.get("status", "")
                last_run_at = last_run.get("run_end_at")
                
                query_string = f"""
                    UPDATE core.pipeline SET 
                        run_id = '{run_id}', 
                        status = '{status}', 
                        last_run_at = {f"'{last_run_at}'" if last_run_at else 'NULL'},
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    WHERE asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Airbyte Connector - Update Run Stats to Job Failed", e)
        raise e
def __prepare_description(properties, type="pipeline") -> str:
    name = properties.get("name", "")
    workspace_name = properties.get("workspace_name", "")
    description = f"""This {name} pipeline is under the Workspace {workspace_name}."""
    if type == "task":
        task_name = properties.get("task_name", "")
        pipeline = properties.get("pipeline_name", "")
        workspace_name = properties.get("workspace_name", "")
        description = f"""This {task_name} task is part of the pipeline {pipeline} and under the Workspace {workspace_name}."""
    return description


def __extract_airbyte_pipeline_measure(config: dict):
    """Extract pipeline measures for Airbyte"""
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        # Fetch pipeline_name (job_name) from pipeline table
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
        
        # Pull the latest two pipeline runs for duration deltas
        with connection.cursor() as cursor:
            runs_query = f"""
                select source_id, run_start_at as started_at, run_end_at as finished_at, duration, id
                from core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_start_at desc
                limit 2
            """
            cursor = execute_query(connection, cursor, runs_query)
            runs = fetchall(cursor) or []

        if not runs:
            return
        # Build job (asset) measure
        latest = runs[0]
        previous = runs[1] if len(runs) > 1 else {}
        job_measure = {
            "duration": latest.get("duration"),
            "last_run_date": latest.get("started_at"),
            "previous_run_date": previous.get("started_at") if previous else None,
        }
        # Pass pipeline_name (job_name) for asset level measures
        execute_pipeline_measure(config, "asset", job_measure, job_name=pipeline_name)

        # Task measures: fetch tasks and use their last run timestamps from pipeline_runs_detail
        with connection.cursor() as cursor:
            tasks_query = f"""
                select id as task_id, source_id, name from core.pipeline_tasks
                where asset_id = '{asset_id}' and is_active = true and is_delete = false
            """
            cursor = execute_query(connection, cursor, tasks_query)
            tasks = fetchall(cursor) or []

        if not tasks:
            return

        for task in tasks:
            source_id = task.get("source_id")
            task_name = task.get("name")
            # Get latest two run details for this task
            with connection.cursor() as cursor:
                detail_query = f"""
                    select run_start_at as started_at, run_end_at as finished_at, duration
                    from core.pipeline_runs_detail
                    where asset_id = '{asset_id}' and source_id = '{source_id}'
                    order by run_start_at desc
                    limit 2
                """
                
                cursor = execute_query(connection, cursor, detail_query)
                details = fetchall(cursor) or []
            if not details:
                continue
            latest_d = details[0]
            previous_d = details[1] if len(details) > 1 else {}
            task_measure = {
                "duration": latest_d.get("duration"),
                "last_run_date": latest_d.get("started_at"),
                "previous_run_date": previous_d.get("started_at") if previous_d else None,
            }
            
            # Pass pipeline_name (job_name) and task_name for task level measures
            execute_pipeline_measure(config, "task", task_measure, task_info=task, job_name=pipeline_name, task_name=task_name)
    except Exception as e:
        log_error("Airbyte - extract measures", e)


def __create_airbyte_task_lineage(config, pipeline_info, tasks_info, asset_properties, entity_columns_list):
    """Create lineage for each Airbyte task - source -> task -> destination"""
    try:
        lineage = {"tables": [], "relations": []}
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id", "")
        
        # Get source and destination information from pipeline_info
        source_info = pipeline_info.get("source", {})
        destination_info = pipeline_info.get("destination", {})
        namespace_format = pipeline_info.get("namespace_format", "")
        prefix = pipeline_info.get("prefix", "")
        namespace_destination = pipeline_info.get("namespace_definition", "")
        if not source_info or not destination_info:
            return lineage
            
        # Extract source details
        source_database = source_info.get("database", "")
        source_schema = source_info.get("schema", "public")
        source_type = source_info.get("type", "unknown")
        source_name = source_info.get("name", "Unknown Source")
        source_host = source_info.get("host", "")
        source_port = source_info.get("port", "")
        source_warehouse = source_info.get("warehouse", "")
        source_role = source_info.get("role", "")
        source_user = source_info.get("user", "")
        if source_type == "postgres":
            source_type = "postgresql"
        if source_type == "bigquery":
            source_database = source_info.get("project_id", "")
            source_schema = source_info.get("dataset_id", "")
        
        # Extract destination details
        destination_database = destination_info.get("database", "")
        destination_schema = destination_info.get("schema", "public")
        destination_type = destination_info.get("type", "unknown")
        destination_name = destination_info.get("name", "Unknown Destination")
        destination_host = destination_info.get("host", "")
        destination_port = destination_info.get("port", "")
        destination_user = destination_info.get("user", "")
        if destination_type == "postgres":
            destination_type = "postgresql"
        if destination_type == "bigquery":
            destination_database = destination_info.get("project_id", "")
            destination_schema = destination_info.get("dataset_id", "")
        
        # For each task, create source -> task -> destination lineage
        for task in tasks_info:
            task_source_id = f"""{connection_id}.{task.get("task_id", "")}"""
            task_name = task.get("task_name", "")
            
            # Find task columns from the list
            task_columns = []
            for entity_data in entity_columns_list:
                if entity_data.get("task_name") == task_name:
                    task_columns = entity_data
                    break
            
            if not task_source_id:
                continue
                
            # Create source entity for this task
            source_entity_id = str(uuid4())
            source_entity = {
                "id": source_entity_id,
                "name": f"{task_name}",
                "entity_name": f"""{source_database}.{task_columns.get("source_schema", "")}.{task_name}""",
                "source_type": "Database",
                "type": "table",
                "dataset_type": "source",
                "level": 3,
                "connection_type": source_type,
                "source_id": f"source.{source_type}.{source_database}.{task_name}",
                "database": source_database,
                "schema": task_columns.get("source_schema", "") if task_columns else source_schema,
                "table": task_name,  # Task-specific table
                "host": source_host,
                "port": source_port,
                "task_id": task_source_id,
                "warehouse": source_warehouse,
                "role": source_role,
                "user": source_user,
                "fields": task_columns.get("source_columns", []) if task_columns else []
            }
            lineage["tables"].append(source_entity)
            
            # Create task entity
            task_entity_id = str(uuid4())
            task_entity = {
                "id": task_entity_id,
                "name": task_name,
                "entity_name": task_source_id,
                "source_type": "Task",
                "type": "task",
                "dataset_type": "stream",
                "level": 4,
                "connection_type": "airbyte",
                "source_id": task_source_id,
                "fields": []
            }
            lineage["tables"].append(task_entity)
            
            # Create destination entity for this task
            destination_entity_id = str(uuid4())
            name = f"{prefix}{task_name}"
            if namespace_destination == "source" or (namespace_destination == "custom_format" and namespace_format == "${SOURCE_NAMESPACE}"):
                destination_schema = task_columns.get("source_schema", "") if task_columns else source_schema
            elif namespace_destination == "custom_format":
                destination_schema = namespace_format
                
            destination_entity = {
                "id": destination_entity_id,
                "name": name,
                "entity_name": f"{destination_database}.{destination_schema}.{name}",
                "source_type": "Database",
                "type": "table",
                "dataset_type": "sink",
                "level": 2,
                "connection_type": destination_type,
                "source_id": f"destination.{destination_type}.{destination_database}.{name}",
                "database": destination_database,
                "schema": destination_schema,
                "table": name,  # Task-specific table
                "host": destination_host,
                "task_id": task_source_id,
                "port": destination_port,
                "user": destination_user,
                "fields": task_columns.get("destination_columns", []) if task_columns else []
            }
            lineage["tables"].append(destination_entity)
            
            # Create relationships: source -> task -> destination
            lineage["relations"].append({
                "srcTableId": source_entity.get("entity_name"),
                "tgtTableId": task_entity.get("entity_name")
            })
            lineage["relations"].append({
                "srcTableId": task_entity.get("entity_name"),
                "tgtTableId": destination_entity.get("entity_name")
            })
        
        return lineage
        
    except Exception as e:
        log_error("Airbyte - Create Task Lineage Failed", e)
        return {"tables": [], "relations": []}


def __extract_airbyte_task_columns(config, pipeline_info, tasks_info, asset_properties):
    """Extract columns for Airbyte tasks from source and destination schemas"""
    try:
        found_columns = []
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id", "")
        
        # Get source and destination information from pipeline_info
        source_info = pipeline_info.get("source", {})
        destination_info = pipeline_info.get("destination", {})
        namespace_format = pipeline_info.get("namespace_format", "")
        prefix = pipeline_info.get("prefix", "")
        namespace_destination = pipeline_info.get("namespace_definition", "")
        
        if not source_info or not destination_info:
            return found_columns
            
        # Extract source and destination details
        source_database = source_info.get("database", "")
        source_schema = source_info.get("schema", "public")
        source_type = source_info.get("type", "unknown")
        source_name = source_info.get("name", "Unknown Source")
        
        destination_database = destination_info.get("database", "")
        destination_schema = destination_info.get("schema", "public")
        destination_type = destination_info.get("type", "unknown")
        destination_name = destination_info.get("name", "Unknown Destination")
        entity_columns_list = []
        
        # For each task, extract columns from source and destination
        for task in tasks_info:
            task_source_id = f"""{connection_id}.{task.get("task_id", "")}"""
            task_name = task.get("task_name", "")
            if not task_source_id:
                continue
            
            # Apply prefix to task name for destination (same as lineage)
            destination_table_name = f"{prefix}{task_name}" if prefix else task_name
            
            # Determine destination schema based on namespace_destination and namespace_format (same as lineage)
            task_destination_schema = destination_schema
            if namespace_destination == "source" or (namespace_destination == "custom_format" and namespace_format == "${SOURCE_NAMESPACE}"):
                # Use source schema for destination - will be updated with task_columns if available
                task_destination_schema = source_schema
            elif namespace_destination == "custom_format":
                task_destination_schema = namespace_format
                
            # Get selectedFields from task info
            selected_fields = task.get("selected_fields", [])
            
            # Get source and destination IDs for stream properties
            source_id = config.get("asset").get("properties").get("source_id")
            destination_id = config.get("asset").get("properties").get("destination_id")
            
            # Get stream properties if source and destination IDs are available
            stream_properties = []
            
            if source_id and destination_id:
                try:
                    stream_properties = __get_stream_properties(config, source_id, destination_id)
                except Exception as e:
                    log_error(f"Failed to get stream properties for task {task_name}", e)
            
            # Find the specific stream for this task
            task_stream_properties = None
            if stream_properties:
                # Look for stream that matches task name or use the first stream
                for stream in stream_properties:
                    stream_name = stream.get("streamName", "")
                    if stream_name == task_name and not task_stream_properties:
                        task_stream_properties = stream
                        break
                
                # If no matching stream found, use the first one
                if not task_stream_properties and stream_properties:
                    task_stream_properties = stream_properties[0]
            
            
            # Initialize separate variables for source and destination columns
            source_columns_for_task = []
            destination_columns_for_task = []
            
            # Always process source columns from stream properties first
            if task_stream_properties:
                stream_name = task_stream_properties.get("streamName", "")
                property_fields = task_stream_properties.get("propertyFields", [])
                source_schema = task_stream_properties.get("streamnamespace", "")
                # Create source columns from stream properties (always all fields)
                for field_list in property_fields:
                    if field_list and len(field_list) > 0:
                        column_name = field_list[0]  # Take the first field
                        column_type = "string"  # Default type
                        
                        source_column = {
                            "id": str(uuid4()),
                            "name": column_name,
                            "type": "column",
                            "data_type": column_type,
                            "score": 0,
                            "table": stream_name,
                            "dataset_name": source_name,
                            "schema": source_schema,
                            "from_dataset": "inbound",
                            "dataflow_name": task_name,
                            "parent_entity_name": f"{source_database}.{source_schema}.{stream_name}",
                            "parent_source_id": task_source_id,
                            "transformation_name": task_name,
                            "task_id": task_source_id,
                            "lineage_entity_id": str(uuid4())
                        }
                        source_columns_for_task.append(source_column)
                        found_columns.append(source_column)
            
            # Update destination schema if namespace_destination requires source schema (same as lineage)
            if namespace_destination == "source" or (namespace_destination == "custom_format" and namespace_format == "${SOURCE_NAMESPACE}"):
                task_destination_schema = source_schema
            
            # Process destination columns based on selectedFields
            if selected_fields and len(selected_fields) > 0:
                # Use selectedFields as destination columns
                for field in selected_fields:
                    field_path = field.get("fieldPath", [])
                    if field_path:
                        column_name = field_path[0]  # Take the first field path element
                        column_type = "string"  # Default type, can be enhanced later
                        
                        # Create destination column from selectedFields
                        destination_column = {
                            "id": str(uuid4()),
                            "name": column_name,
                            "type": "column",
                            "data_type": column_type,
                            "score": 0,
                            "table": destination_table_name,
                            "dataset_name": destination_name,
                            "schema": task_destination_schema,
                            "from_dataset": "outbound",
                            "dataflow_name": task_name,
                           "parent_entity_name": f"{destination_database}.{task_destination_schema}.{destination_table_name}",
                            "parent_source_id": task_source_id,
                            "transformation_name": task_name,
                            "task_id": task_source_id,
                            "lineage_entity_id": str(uuid4())
                        }
                        destination_columns_for_task.append(destination_column)
                        found_columns.append(destination_column)
            else:
                print(f"No selectedFields for task {task_name}, using all source columns as destination columns")
                
                # If selectedFields is empty, use all source columns as destination columns
                for source_col in source_columns_for_task:
                    destination_column = {
                        "id": str(uuid4()),
                        "name": source_col["name"],
                        "type": "column",
                        "data_type": source_col["data_type"],
                        "score": 0,
                        "table": destination_table_name,
                        "dataset_name": destination_name,
                        "schema": task_destination_schema,
                        "from_dataset": "outbound",
                        "dataflow_name": task_name,
                        "parent_entity_name": f"{destination_database}.{task_destination_schema}.{destination_table_name}",
                        "parent_source_id": task_source_id,
                        "transformation_name": task_name,
                        "task_id": task_source_id,
                        "lineage_entity_id": str(uuid4())
                    }
                    destination_columns_for_task.append(destination_column)
                    found_columns.append(destination_column)
            
            # Always add Airbyte metadata columns to destination
            airbyte_metadata_columns = [
                {"name": "_AIRBYTE_RAW_ID", "type": "VARCHAR(16777216)"},
                {"name": "_AIRBYTE_EXTRACTED_AT", "type": "TIMESTAMP_TZ(9)"},
                {"name": "_AIRBYTE_META", "type": "VARIANT"},
                {"name": "_AIRBYTE_GENERATION_ID", "type": "NUMBER(38,0)"}
            ]
            
            for metadata_col in airbyte_metadata_columns:
                destination_column = {
                    "id": str(uuid4()),
                    "name": metadata_col["name"],
                    "type": "column",
                    "data_type": metadata_col["type"],
                    "score": 0,
                    "table": destination_table_name,
                    "dataset_name": destination_name,
                    "schema": task_destination_schema,
                    "from_dataset": "outbound",
                    "dataflow_name": task_name,
                    "parent_entity_name": f"{destination_database}.{task_destination_schema}.{destination_table_name}",
                    "parent_source_id": task_source_id,
                    "transformation_name": task_name,
                    "task_id": task_source_id,
                    "lineage_entity_id": str(uuid4())
                }
                destination_columns_for_task.append(destination_column)
                found_columns.append(destination_column)
            entity_columns_list.append({"task_name": task_name, "destination_columns": destination_columns_for_task,"source_columns": source_columns_for_task,"source_schema": source_schema}),
            
            # Store task-specific columns as separate variables
            
            # You can now use source_columns_for_task and destination_columns_for_task
            # for any additional processing specific to this task
        
        return found_columns, entity_columns_list
        
    except Exception as e:
        log_error("Airbyte - Extract Task Columns Failed", e)
        return []


def __get_stream_properties(config, source_id, destination_id):
    """Get stream properties from Airbyte API"""
    try:
        params = dict(
            source_id=source_id,
            destination_id=destination_id
        )
        response = __get_response("get_stream_properties", params)
        return response.get("streams", []) if response else []
    except Exception as e:
        log_error("Airbyte - Get Stream Properties Failed", e)
        return []


def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    """Helper function to get data by key from a list of dictionaries"""
    return next((item[get_key] for item in data if item[filter_key] == value), None)


def __delete_airbyte_columns(config: dict, pipeline_id: str):
    """Delete existing columns for a pipeline and asset"""
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        with connection.cursor() as cursor:
            query_string = f"""
                delete from core.pipeline_columns
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Airbyte Connector - Delete Columns By Pipeline ID Failed", e)
        raise e


def __save_airbyte_columns(
    config: dict, columns: list, pipeline_id: str, pipeline_tasks: list = None
):
    """Save Airbyte task columns to database following ADF pattern"""
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        with connection.cursor() as cursor:
            # Delete existing columns first
            query_string = f"""
                delete from core.pipeline_columns
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            if not columns:
                return

            for column in columns:
                properties = {
                    "id": column.get("id"),
                    "table": column.get("table"),
                    "dataset_name": column.get("dataset_name"),
                    "schema": column.get("schema"),
                    "from_dataset": column.get("from_dataset"),
                    "dataflow_name": column.get("dataflow_name"),
                    "transformation_name": column.get("transformation_name", ""),
                    "task_id": column.get("task_id"),
                    "source": "airbyte"
                }
                
                # Get pipeline_task_id using the task_id
                pipeline_task_id = get_data_by_key(
                    pipeline_tasks, column.get("task_id")
                )
                
                query_input = (
                    uuid4(),
                    column.get("name", ""),
                    column.get("description", ""),
                    column.get("comment"),
                    column.get("column_type", "string"),
                    column.get("tags") if column.get("tags") else [],
                    True,
                    False,
                    pipeline_task_id,
                    pipeline_id,
                    asset_id,
                    connection_id,
                    json.dumps(properties, default=str).replace("'", "''")
                )
                
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                insert_objects.append(query_param)

            # Insert columns in batches
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.pipeline_columns(
                                id, name, description, comment, data_type, tags,
                                is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id,
                                properties
                            ) values {query_input} 
                        """
                        cursor = execute_query(connection, cursor, query_string)
                    except Exception as e:
                        log_error("Airbyte Task Columns Insert Failed", e)
                        
    except Exception as e:
        log_error(f"Airbyte Connector - Save Task Columns Failed", e)
        raise e


def __airbyte_tag_mapping(config: dict, tags: list, asset_id: str):
    """Handle tag auto mapping for Airbyte pipeline (similar to DBT pattern)"""
    try:
        # Extract tag names from the tags list
        tag_names = []
        for tag in tags:
            if isinstance(tag, dict):
                tag_name = tag.get("name", "")
                if tag_name:
                    tag_names.append(tag_name)
            elif isinstance(tag, str):
                tag_names.append(tag)
        
        if tag_names:
            params = {
                "asset_id": asset_id,
                "level": "asset",  # Pipeline level tags
                "id": asset_id,
                "source": "airbyte"
            }
            pipeline_auto_tag_mapping(config, tag_names, params=params)
            
    except Exception as e:
        log_error("Airbyte - Tag Auto Mapping Failed", e)


def __map_airbyte_asset_with_lineage(config: dict, lineage: dict):
    """Map Airbyte lineage tables with existing assets based on connection_type (excluding tasks)"""
    try:
        tables = lineage.get("tables", [])
        if not tables:
            return
        
        # Filter out tasks and only process database tables
        database_tables = [
            table for table in tables 
            if table.get("source_type") != "Task" and 
            table.get("connection_type")
        ]
        
        if not database_tables:
            return
            
        for table in database_tables:
            connection_type = table.get("connection_type", "")
            database = table.get("database", "")
            schema = table.get("schema", "")
            name = table.get("name", "")
            
            if connection_type and database and schema and name:
                # Get assets by database, schema, and table name
                assets = __get_airbyte_asset_by_db_schema_name(
                    config, database, schema, name, connection_type
                )
                if assets:
                    save_asset_lineage_mapping(config, "pipeline", table, assets, True)
                    
    except Exception as e:
        log_error("Airbyte - Map Asset With Lineage Failed", e)
        raise e


def __get_airbyte_asset_by_db_schema_name(config: dict, db_name: str, schema_name: str, table_name: str, connection_type: str) -> list:
    """Get assets by database, schema, and table name for Airbyte auto mapping"""
    try:
        connection = get_postgres_connection(config)
        assets = []
        
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT 
                    asset.id,
                    asset.name,
                    asset.connection_id,
                    connection.type as connection_type,
                    asset.properties->>'database' as database,
                    asset.properties->>'schema' as schema_name
                FROM core.asset
                JOIN core.connection ON connection.id = asset.connection_id
                WHERE asset.is_active = true 
                AND asset.is_delete = false
                AND LOWER(asset.name) = LOWER('{table_name}')
                AND LOWER(asset.properties->>'schema') = LOWER('{schema_name}')
                AND LOWER(asset.properties->>'database') = LOWER('{db_name}')
                AND connection.type = '{connection_type}'
                ORDER BY asset.id ASC
            """
            cursor = execute_query(connection, cursor, query_string)
            results = fetchall(cursor)
            
            for result in results:
                assets.append({
                    "id": result.get("id"),
                    "name": result.get("name"),
                    "connection_id": result.get("connection_id"),
                    "connection_type": result.get("connection_type"),
                    "database": result.get("database"),
                    "schema": result.get("schema_name")
                })
                
        return assets
        
    except Exception as e:
        log_error("Airbyte - Get Asset By Database Schema Name Failed", e)
        return []
