"""
    Coalesce Pipeline Connector
    Handles pipeline columns and runs extraction from Coalesce
"""
import datetime
import json
import re
from uuid import uuid4

import dateutil
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
)
from dqlabs.app_helper import agent_helper
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.lineage_helper import (
    get_asset_metric_count,
    handle_alerts_issues_propagation,
    save_lineage,
    save_asset_lineage_mapping,
    save_lineage_entity,
    update_asset_metric_count,
    update_pipeline_propagations
)
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks
from uuid import uuid4

TASK_CONFIG = None


def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    """
    Helper function to get data by key value - similar to ADF implementation
    """
    return next((item[get_key] for item in data if str(item[filter_key]) == str(value)), None)

def collect_attribute_level_transformations(node_details: dict, asset_id: str, pipeline_id: str,pipeline_name: str):
    """
    Collect all columns from node_details['metadata']['columns'] that have a non-empty 'transform' in any of their 'sources'.
    Returns a list of transformation records (attribute-level transformations).
    """
    transformations = []
    columns = node_details.get("metadata", {}).get("columns", [])
    node_name = node_details.get("name", "")
    node_type = node_details.get("nodeType", "")
    
    for column in columns:
        for source in column.get("sources", []):
            transform_expr = source.get("transform", "")

            if transform_expr:
                transformation = {
                    "id": str(uuid4()),
                    "name": f"{pipeline_name}.{node_name}.{column.get('name', '')}",
                    "description": f"Transformation for column {column.get('name', '')} in node {node_name}",
                    "properties": json.dumps({
                        "node_id": node_details.get("id"),
                        "node_name": node_name,
                        "column_id": column.get("columnID"),
                        "column_name": column.get("name"),
                        "transform": transform_expr,
                        "source": source,
                        "node_type": node_type,
                        "pipeline_id": pipeline_id,
                        "asset_id": asset_id
                    }, default=str),
                    "source_id": f"{asset_id}.{column.get('name', '')}",
                    "pipeline_id": pipeline_id,
                    "source_type": "attribute_transform",
                    "asset_id": asset_id,
                    "connection_id": node_details.get("connection_id", None),
                    "is_active": True,
                    "is_delete": False,
                    "created_at": datetime.datetime.now(),
                    "updated_at": datetime.datetime.now(),
                    "status": None,
                    "error": None,
                    "run_start_at": None,
                    "run_end_at": None,
                    "duration": None,
                    "last_run_at": None
                }
                transformations.append(transformation)
    return transformations


def extract_coalesce_nodes(config, **kwargs):
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
        connection_id = connection.get("id")
        credentials = decrypt_connection_config(credentials, connection_type)
        
        TASK_CONFIG = config
        is_valid, is_exist, job_details = __validate_connection_establish(asset_properties)
        
        if not is_exist:
            raise Exception(
                f"Job - {asset_properties.get('name')} doesn't exist in workspace {asset_properties.get('workspace_id')} "
            )
        
        if is_valid:
            pipeline_name = asset_properties.get("name")
            pipeline_info, description, stats = get_pipeline_info(asset_properties, pipeline_name, credentials, job_details)
            pipeline_id = __get_pipeline_id(config)
            description = ""
            if not description:
                description = __prepare_description(asset_properties)
            
            # Update asset description
            __update_asset_description(config, asset_id, description)
            
            # Ensure defaults for downstream usage
            all_pipeline_runs = []
            latest_run = False
            all_runs = []
            latest_run_id = ""

            # NEW FLOW: Get tasks (nodes) from latest run
            tasks = pipeline_info.get("tasks", [])
            all_transformations = []
            all_static_tests = []
            pipeline_tasks = []  # Store tasks for run details
            
            # Process each task (node) from the latest run
            workspace_id = asset_properties.get("workspace_id", "")
            environment_id = asset_properties.get("environment_id", "")
            
            # Save tasks in core.pipeline_tasks
            if tasks and credentials.get("metadata").get("tasks"):
                pipeline_tasks,new_pipeline_tasks = __save_pipeline_tasks(
                    config,
                    pipeline_info,
                    pipeline_id,
                    asset_id,
                    asset_properties,
                    pipeline_name
                )
                
                # Create task-level measures for new tasks
                if new_pipeline_tasks:
                    create_pipeline_task_measures(config, new_pipeline_tasks)
                
                # Create mapping of node_id to pipeline_task_id for column linking
                node_to_task_id_map = {}
                for pt in pipeline_tasks:
                    node_id = pt.get("node_id", "")  # Use node_id field instead of source_id
                    task_id = pt.get("id", "")
                    if node_id and task_id:
                        node_to_task_id_map[node_id] = task_id
                
                
                __save_pipeline_columns(config, tasks, pipeline_id, node_to_task_id_map, pipeline_name)
                
                # Collect transformations and tests for each task
                for task in tasks:
                    node_id = task.get("node_id", "")
                    node_details = task.get("node_details", {})
                    
                    if not node_details:
                        continue
                    
                    # Collect transformations for this task
                    if credentials.get("metadata").get("transformations"):
                        task_transformations = collect_attribute_level_transformations(node_details, asset_id, pipeline_id,pipeline_name )
                        all_transformations.extend(task_transformations)
                    
                    # Collect tests for this task
                    if credentials.get("metadata").get("tests"):
                        task_tests = __collect_tests_metadata(node_details, connection_id)
                        all_static_tests.extend(task_tests)
            
            # DEBUG: Check for duplicate tests in all_static_tests
            if all_static_tests:
                test_source_ids = [t.get("source_id") for t in all_static_tests]
                unique_test_ids = set(test_source_ids)
            
            # Save pipeline runs (job-level runs)
            if credentials.get("metadata").get("runs"):
                all_pipeline_runs, latest_run, all_runs = __save_pipeline_runs(
                    config,
                    pipeline_info,
                    asset_id,
                    pipeline_id,
                    transformations=all_transformations,
                    static_tests=all_static_tests,
                    pipeline_tasks=pipeline_tasks,  # Pass tasks for run details
                )
                # Update Propagations Alerts and Issues Creation and Notifications for Tasks
                if latest_run:
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

            # Save tests for all tasks - collect all node_details and save in one batch
            if credentials.get("metadata").get("tests") and all_static_tests:
                # Collect all node_details from tasks (already fetched above)
                all_node_details = []
                for task in tasks:
                    node_details = task.get("node_details", {})
                    if node_details:
                        all_node_details.append(node_details)
                
                # Save all tests from all nodes in one batch
                if all_node_details:
                    __save_coalesce_tests_batch(config, all_node_details, pipeline_id, asset_id, all_pipeline_runs,asset_properties)
            
            # Save transformations for all tasks
            if credentials.get("metadata").get("transformations") and all_transformations:
                __save_transformations(
                    config,
                    pipeline_id,
                    asset_id,
                    all_transformations,
                    all_pipeline_runs,
                    credentials.get("metadata").get("runs"),
                )
            if tasks:
                lineage = __build_lineage_from_tasks(config, tasks, asset_id, asset_properties, pipeline_name)
            filter_tables = list(
                    filter(
                        lambda table: table.get("name") != pipeline_name,
                        lineage.get("tables"),
                    )
                )
            save_lineage_entity(config,filter_tables, asset_id,True)
            save_lineage(config,"pipeline",lineage,asset_id)
            __map_asset_with_lineage(config, lineage)
            __update_pipeline_stats(config)
            if latest_run_id:
                handle_alerts_issues_propagation(config, latest_run_id)
            update_pipeline_propagations(config, asset, connection_type)
            
            
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value)
        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(all_runs, credentials.get("metadata").get("tasks"))
        if propagate_alerts == "table"or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
        
    except Exception as e:
        log_error("Coalesce Pipeline extraction Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error=e)
    finally:
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value)

def __get_asset_by_db_schema_name(
    config, db_name, schema_name, table_name, connection_type
) -> list:
    assets = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select asset.id, asset.name from core.asset
                join core.connection on connection.id = asset.connection_id
                where asset.is_active = true and asset.is_delete = false
                and asset.name = '{table_name}'
                and lower(connection.type) = lower('{connection_type}')
                and asset.properties->>'schema' = '{schema_name}'
                and (asset.properties->>'database' = '{db_name}' OR connection.credentials->>'database' = '{db_name}')
                order by asset.created_date desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            assets = fetchall(cursor)
    except Exception as e:
        log_error("Coalecse Get Asset By Database, Schema and Name Failed ", e)
    finally:
        return assets


def __map_asset_with_lineage(config, lineage):
    # DQscore Based On Asset Matching
    try:
        table_ids = []
        lineage_tables = lineage.get("tables", [])
        if not lineage_tables:
            return

        for table in lineage_tables:
            if  "platform_kind" in table:
                connection_type = table.get("platform_kind","")
                if connection_type:
                    table_schema, table_name = table.get(
                        "schema", ""
                    ), table.get("name", "")
                    database = table.get(
                        "database", ""
                    )
                    if table_name and table_schema and database:
                        unique_id = f"{connection_type}_{table_schema}_{table_name}_{table.get('id')}"
                        if unique_id not in table_ids:
                            table_ids.append(unique_id)
                            assets = __get_asset_by_db_schema_name(
                                config,
                                database,
                                table_schema,
                                table_name,
                                connection_type,
                            )
                            if assets:
                                save_asset_lineage_mapping(
                                    config, "pipeline", table, assets, True
                                )
    except Exception as e:
        log_error("Error in mapping asset with lineage", e)


def get_node_details(node_id: str  , node_data: dict) -> dict:
    for id, node_details in node_data.items():
        if id == node_id:
            return node_details

def __get_response(method_name: str, params: dict = {}):
    """
    Send request to agent helper for Coalesce API calls
    """
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
    """
    Validate Coalesce connection and job existence
    Returns: (is_valid, is_exist, job_details)
    """
    try:
        is_valid = False
        is_exist = False
        job_details = {}
        
        project_id = config.get("project_id", "")
        workspace_id = config.get("workspace_id", "")
        job_name = config.get("name", "")
        job_id = config.get("job_id", "")
        
        params = dict(
            project_id=project_id,
            workspace_id=workspace_id,
            job_name=job_name,
            job_id=job_id
        )
        
        response = __get_response("validate_pipeline", params)
        is_exist = bool(response.get("is_exist"))
        is_valid = bool(response.get("is_valid"))
        job_details = response.get("job_details", {})
        
        return (bool(is_valid), bool(is_exist), job_details)
    except Exception as e:
        log_error("Coalesce Connector - Validate Connection Failed", e)
        return (is_valid, is_exist, {})

def get_pipeline_info(asset_properties, pipeline_name: str, credentials: dict, job_details: dict = {}) -> tuple:
    """
    Get pipeline info for a Coalesce job.
    NEW FLOW: Jobs are now the primary assets/pipelines.
    """
    try:
        job_id = asset_properties.get("job_id", "")
        workspace_id = asset_properties.get("workspace_id", "")
        project_id = asset_properties.get("project_id", "")
        environment_id = asset_properties.get("environment_id", "")
        no_of_runs = credentials.get("no_of_runs", 30)
        status_filter = credentials.get("status", "all")

        params = dict(
            job_id=job_id,
            workspace_id=workspace_id,
            project_id=project_id,
            environment_id=environment_id,
            pipeline_name=pipeline_name,
            no_of_runs=no_of_runs,
            status_filter=status_filter
        )
        
        # Get job-level runs and extract tasks (nodes) from latest run
        response = __get_response("get_coalesce_pipeline_info", params)
        pipeline_info = response.get("pipeline_info", {})
        description = response.get("description", "")
        stats = response.get("stats", {})
        
        return pipeline_info, description, stats
        
    except Exception as e:
        raise e

def __save_pipeline_tasks(
    config: dict,
    pipeline_info: dict,
    pipeline_id: str,
    asset_id: str,
    asset_properties: dict = {},
    pipeline_name: str = ""
) -> list:
    """
    Save tasks (nodes) in core.pipeline_tasks table.
    Tasks are extracted from the latest run's results.
    """
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        connection = get_postgres_connection(config)
        new_pipeline_tasks = []
        pipeline_tasks = []
        
        tasks = pipeline_info.get("tasks", [])
        latest_run_id = pipeline_info.get("latest_run_id", "")
        latest_run_results = pipeline_info.get("latest_run_results", {})
        
        if not tasks:
            return []
        
        workspace_id = asset_properties.get("workspace_id", "")
        environment_id = asset_properties.get("environment_id", "")
        
        with connection.cursor() as cursor:
            insert_objects = []
            
            for task in tasks:
                node_id = task.get("node_id", "")
                node_name = task.get("node_name", "")
                
                if not node_id:
                    continue
                
                # Fetch node details to get full information
                node_details = task.get("node_details", {})
                node_details = {
                        "id": node_id,
                        "name": node_name,
                        "database": node_details.get("database", ""),
                        "schema": node_details.get("schema", ""),
                        "description": node_details.get("description", "")
                    }
                
                # Create source_id for task in format: job_name_node_id_connection_id
                source_id = f"{pipeline_name}_{node_id}_{connection_id}"
                
                # Check if task already exists (also check by node_id in properties for backward compatibility)
                query_string = f"""
                    SELECT id, is_selected FROM core.pipeline_tasks
                    WHERE asset_id = '{asset_id}' AND source_id = '{source_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                
                # If not found by new format, try to find by node_id in properties
                if not existing_task:
                    query_string = f"""
                        SELECT id, is_selected FROM core.pipeline_tasks
                        WHERE asset_id = '{asset_id}' 
                        AND properties->>'node_id' = '{node_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    existing_task = fetchone(cursor)
                    
                    # If found by old format, update source_id to new format
                    if existing_task:
                        task_id = existing_task.get("id")
                        update_source_id_query = f"""
                            UPDATE core.pipeline_tasks SET source_id = '{source_id}'
                            WHERE id = '{task_id}'
                        """
                        cursor = execute_query(connection, cursor, update_source_id_query)
                
                # Get task run info from latest run results
                task_run_info = None
                run_results_data = latest_run_results.get("data", []) if isinstance(latest_run_results, dict) else []
                if not run_results_data and isinstance(latest_run_results, list):
                    run_results_data = latest_run_results
                
                for node_result in run_results_data:
                    if isinstance(node_result, dict) and node_result.get("nodeID") == node_id:
                        task_run_info = node_result
                        break
                
                # Extract run information
                run_state = task_run_info.get("runState", "") if task_run_info else ""
                query_results = task_run_info.get("queryResults", []) if task_run_info else []
                
                # Calculate duration from query results
                duration = 0.0
                run_start_at = None
                run_end_at = None
                if query_results:
                    # Get first and last query times
                    start_times = [q.get("startTime") for q in query_results if q.get("startTime")]
                    end_times = [q.get("endTime") for q in query_results if q.get("endTime")]
                    if start_times and end_times:
                        run_start_at = min(start_times)
                        run_end_at = max(end_times)
                        duration = __calculate_duration(run_start_at, run_end_at)
                
                # Determine status
                status = get_pipeline_status(run_state.lower()) if run_state else "unknown"
                
                # Get error if any
                error = ""
                if task_run_info:
                    failed_queries = [q for q in query_results if q.get("success") is False]
                    if failed_queries:
                        error_info = failed_queries[0].get("error", {})
                        error = error_info.get("errorString", "") or error_info.get("errorDetail", "") or ""
                
                # Build properties
                properties = {
                    "node_id": node_id,
                    "node_name": node_name,
                    "node_type": node_details.get("nodeType", ""),
                    "location": node_details.get("locationName", ""),
                    "database": node_details.get("database", ""),
                    "schema": node_details.get("schema", ""),
                    "table": node_details.get("table", ""),
                    "workspace_id": workspace_id,
                    "environment_id": environment_id,
                    "has_test_failures": task_run_info.get("hasTestFailures", False) if task_run_info else False
                }
                
                if existing_task:
                    task_id = existing_task.get("id")
                    is_selected = existing_task.get("is_selected", False)
                    
                    # Only update if task is selected
                    if is_selected:
                        update_query = f"""
                            UPDATE core.pipeline_tasks SET
                                name = '{node_name.replace("'", "''")}',
                                description = '{node_details.get("description", "").replace("'", "''")}',
                                database = '{node_details.get("database", "").replace("'", "''")}',
                                schema = '{node_details.get("schema", "").replace("'", "''")}',
                                status = '{status}',
                                duration = {duration if duration else 'NULL'},
                                run_start_at = {f"'{run_start_at}'" if run_start_at else 'NULL'},
                                run_end_at = {f"'{run_end_at}'" if run_end_at else 'NULL'},
                                error = '{error.replace("'", "''")}',
                                properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                                run_id = '{latest_run_id}'
                            WHERE id = '{task_id}'
                        """
                        cursor = execute_query(connection, cursor, update_query)
                        new_pipeline_tasks.append(task_id)
                        pipeline_tasks.append({
                            "id": task_id,
                            "source_id": source_id,
                            "name": node_name,
                            "node_id": node_id  # Store node_id for matching in run details
                        })
                else:
                    # Insert new task
                    task_id = str(uuid4())
                    query_input = (
                        task_id,
                        asset_id,
                        pipeline_id,
                        connection_id,
                        source_id,
                        latest_run_id,
                        node_name,
                        node_details.get("description", ""),
                        node_details.get("database", ""),
                        node_details.get("schema", ""),
                        "",  # source_code (can be added from node_details.get("sql") if needed)
                        "",  # compiled_code
                        status,
                        duration if duration else None,
                        run_start_at if run_start_at else None,
                        run_end_at if run_end_at else None,
                        json.dumps([]),  # tags
                        json.dumps(properties, default=str).replace("'", "''"),
                        None,  # created_at (from node_details if available)
                        True,  # is_selected
                        True,  # is_active
                        False,  # is_delete
                        error.replace("'", "''") if error else ""
                    )
                    new_pipeline_tasks.append(task_id)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    pipeline_tasks.append({
                        "id": task_id,
                        "source_id": source_id,
                        "name": node_name,
                        "node_id": node_id  # Store node_id for matching in run details
                    })
            
            # Insert new tasks
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            INSERT INTO core.pipeline_tasks(
                                id, asset_id, pipeline_id, connection_id, source_id, run_id, name, description,
                                database, schema, source_code, compiled_code, status, duration,
                                run_start_at, run_end_at, tags, properties, created_at,
                                is_selected, is_active, is_delete, error
                            ) VALUES {query_input}
                            RETURNING id, source_id;
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        inserted_tasks = fetchall(cursor)
                        pipeline_tasks.extend(inserted_tasks)
                    except Exception as e:
                        log_error("Coalesce Pipeline Tasks Insert Failed", e)
            
            # Mark tasks as deprecated if they are no longer present in current tasks
            if pipeline_tasks:
                source_ids = [task.get("source_id") for task in pipeline_tasks if task.get("source_id")]
                if source_ids:
                    source_ids_str = "', '".join(source_ids)
                    tasks_deprecated_query = f"""
                        UPDATE core.pipeline_tasks SET
                            status='DEPRECATED', is_active=false, is_delete=true
                        WHERE asset_id = '{asset_id}' AND source_id NOT IN ('{source_ids_str}')
                    """
                    execute_query(connection, cursor, tasks_deprecated_query)
        
        return pipeline_tasks,new_pipeline_tasks
        
    except Exception as e:
        log_error(f"Coalesce Connector - Save Pipeline Tasks Failed", e)
        return []

def __save_pipeline_columns(config: dict, tasks: list, pipeline_id: str, node_to_task_id_map: dict = None, pipeline_name: str = ""):
    """
    Save pipeline columns from node_details in tasks (from pipeline_info)
    Each task has node_details which contains the column information
    """
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        node_to_task_id_map = node_to_task_id_map or {}
        
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Delete existing columns for this pipeline
            query_string = f"""
                DELETE FROM core.pipeline_columns
                WHERE asset_id = '{asset_id}' AND pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            
            # Process each task and save its columns
            for task in tasks:
                node_id = task.get("node_id", "")
                node_details = task.get("node_details", {})
                
                if not node_details:
                    continue
                
                # Get pipeline_task_id from mapping
                pipeline_task_id = node_to_task_id_map.get(node_id)
                
                # Get columns from node_details metadata
                metadata = node_details.get("metadata", {})
                if not isinstance(metadata, dict):
                    continue
                    
                columns = metadata.get("columns", [])
                if not columns:
                    continue

                for column in columns:
                    # Check if column has a transform (attribute-level transformation)
                    has_transform = False
                    transform_expr = None
                    if column.get("sources") and len(column.get("sources", [])) > 0:
                        transform_expr = column.get("sources")[0].get("transform")
                        has_transform = bool(transform_expr)
                    
                    # Determine from_dataset value
                    from_dataset = "transform" if has_transform else "outbound"
                    
                    properties = {
                        "column_id": column.get("id"),
                        "node_name": node_details.get("name"),
                        "node_id": node_details.get("id"),
                        "environment_id": node_details.get("environment_id"),
                        # Store both formats for compatibility
                        "schema_name": node_details.get("schema"),
                        "table_name": node_details.get("table"),
                        "schema": node_details.get("schema"),  # For query compatibility
                        "table": node_details.get("table"),  
                        "column_name": column.get("name"),# For query compatibility
                        "data_type": column.get("dataType"),
                        "is_nullable": column.get("is_nullable"),
                        "node_type": node_details.get("nodeType"),
                        "transform": transform_expr,
                        "appliedColumnTests": column.get("appliedColumnTests", {}),
                        # Add properties expected by the query
                        "dataset_name": node_details.get("name", ""),  # Use node name as dataset name
                        "dataflow_name": pipeline_name,  # Use pipeline/job name as dataflow name
                        "from_dataset": from_dataset,  # "transform" if has transform, else "outbound"
                    }
                    
                    query_input = (
                        str(uuid4()),
                        column.get("name", "").upper(),
                        column.get("description", ""),
                        column.get("comment", ""),
                        column.get("dataType", ""),
                        json.dumps(column.get("tags", []), default=str),
                        True,
                        False,
                        pipeline_task_id,  # pipeline_task_id (link to task)
                        pipeline_id,
                        asset_id,
                        connection_id,
                        json.dumps(properties, default=str).replace("'", "''"),
                        None,  # lineage_entity_id
                    )
                    
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                    insert_objects.append(query_param)

            # Insert columns in batches
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        INSERT INTO core.pipeline_columns(
                            id, name, description, comment, data_type, tags,
                            is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id,
                            properties, lineage_entity_id
                        ) VALUES {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    
                log_info(f"Successfully inserted {len(insert_objects)} pipeline columns from node_details")
            else:
                log_info("No columns found in node_details")
                    
    except Exception as e:
        log_error(f"Coalesce Connector - Save Pipeline Columns Failed", e)
        raise e


def __save_coalesce_tests_batch(config: dict, all_node_details: list, pipeline_id: str, asset_id: str, pipeline_runs,asset_properties):
    """
    Persist node-level (appliedNodeTests) and column-level (appliedColumnTests) tests
    from multiple Coalesce nodes into core.pipeline_tests in one batch operation.
    - For node-level tests, column_name will be NULL
    - For column-level tests, one row per enabled test (e.g., hasNull) per column
    - Deletes all existing tests for the asset/pipeline once, then inserts all tests from all nodes
    """
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        last_run_detail = pipeline_runs[-1] if pipeline_runs else ()
        latest_run_id = last_run_detail[1] if last_run_detail and 'is_update' not in last_run_detail else (last_run_detail.get("source_id") if isinstance(last_run_detail, dict) else "")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Get test results from pipeline_runs_detail for all tests
            test_results = {}
            if last_run_detail:
                query_string = f"""
                    SELECT prd.source_id, prd.status, prd.error, 
                           prd.run_start_at, prd.run_end_at, prd.duration, prd.source_code,
                           pr.run_end_at as pipeline_run_time
                    FROM core.pipeline_runs_detail prd
                    JOIN core.pipeline_runs pr ON pr.id = prd.pipeline_run_id
                    WHERE pr.asset_id = '{asset_id}'
                    AND prd.type = 'test'
                    ORDER BY pr.run_end_at DESC
                """
                cursor = execute_query(connection, cursor, query_string)
                for test in fetchall(cursor):
                    test_results[test["source_id"]] = {
                        "status": test["status"],
                        "error": test["error"],
                        "run_start_at": test["run_start_at"],
                        "run_end_at": test["run_end_at"],
                        "duration": test["duration"],
                        "source_code": test["source_code"]
                    }
            
            # Delete ALL existing tests for this asset/pipeline ONCE (before processing all nodes)
            query_string = f"""
                DELETE FROM core.pipeline_tests
                WHERE asset_id = '{asset_id}' AND pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            
            # Process all node_details and collect all tests
            for node_details in all_node_details:
                node_id = node_details.get("id", "")
                if not node_details:
                    continue
                    
                metadata = node_details.get("metadata", {}) or {}
                applied_node_tests = metadata.get("appliedNodeTests", []) or []
                columns = metadata.get("columns", []) or []
                node_name = node_details.get("name", "")

                # Node-level tests
                if applied_node_tests:
                    for test_id in applied_node_tests:
                        test_name = test_id.get("name", "")
                        column_name = ""
                        source_id = f"{test_name}.{column_name}.{node_name}.{connection_id}"
                        test_result = test_results.get(source_id, {})
                        query_input = (
                            str(uuid4()),                 # id
                            source_id,  # source_id
                            test_name,                      # name
                            f"Node-level test {test_name} for node {node_name}",  # description
                            None,                         # column_name
                            test_result.get("status"),        # status
                            test_result.get("error", ""),     # error
                            test_id.get("templateString", ""),                               # source_code
                            test_result.get("source_code", ""),                               # compiled_code
                            json.dumps([], default=str),      # tags
                            json.dumps(
                                [f"{asset_properties.get('name')}_{node_id}_{connection_id}"],
                                default=str),
                            test_result.get("run_start_at"),  # run_start_at
                            test_result.get("run_end_at"),    # run_end_at
                            test_result.get("duration"),                       # duration
                            latest_run_id,                         # run_id
                            True,                         # is_active
                            False,                        # is_delete
                            pipeline_id,
                            asset_id,
                            connection_id,
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                        insert_objects.append(query_param)

                # Column-level tests
                for col in columns:
                    col_name = col.get("name", "")
                    col_tests = col.get("appliedColumnTests", {}) or {}
                    for test_key, is_enabled in col_tests.items():
                        if not is_enabled:
                            continue
                        source_id = f"{test_key}.{col_name}.{node_name}.{connection_id}"
                        test_result = test_results.get(source_id, {})
                        query_input = (
                            str(uuid4()),                # id
                            source_id,  # source_id
                            f"{test_key}_{col_name}_{node_name}",                     # name (e.g., hasNull)
                            f"Column-level test {test_key} on column {col_name}",  # description
                            col_name,                     # column_name
                            test_result.get("status"),        # status
                            test_result.get("error", ""),     # error
                            test_result.get("source_code", ""),                              # source_code
                            test_result.get("source_code", ""),                              # compiled_code
                            json.dumps([], default=str),      # tags
                            json.dumps(
                                [f"{asset_properties.get('name')}_{node_id}_{connection_id}"],
                                default=str),
                            test_result.get("run_start_at"),  # run_start_at
                            test_result.get("run_end_at"),    # run_end_at
                            test_result.get("duration"), 
                            latest_run_id,                         # run_id
                            True,                         # is_active
                            False,                        # is_delete
                            pipeline_id,
                            asset_id,
                            connection_id,
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                        insert_objects.append(query_param)

            # Insert all tests from all nodes in batches
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            INSERT INTO core.pipeline_tests(
                                id, source_id, name, description, column_name, status, error, source_code, compiled_code,
                                tags, depends_on, run_start_at, run_end_at, duration, run_id,
                                is_active, is_delete, pipeline_id, asset_id, connection_id
                            ) VALUES {query_input}
                        """
                        cursor = execute_query(connection, cursor, query_string)
                    except Exception as e:
                        log_error("Coalesce Tests Insert Failed", e)
    except Exception as e:
        log_error("Coalesce Connector - Save Tests Batch Failed", e)
        raise e

def __collect_tests_metadata(node_details: dict, connection_id: str) -> list:
    """
    Build a flat list of tests (node-level and column-level) from node_details metadata
    to be re-used for inserting into pipeline_runs_detail.
    Each item contains: source_id, name, description, column_name, node_id.
    """
    tests = []
    if not node_details:
        return tests
    metadata = node_details.get("metadata", {}) or {}
    applied_node_tests = metadata.get("appliedNodeTests", []) or []
    columns = metadata.get("columns", []) or []
    # {test_key}.{col_name}.{node_name}.{connection_id}
    # Node-level tests
    node_name = node_details.get("name")
    node_id = node_details.get("id", "")  # Get node_id for proper matching
    for test_id in applied_node_tests:
        column_name =""
        test_name = test_id.get("name")
        tests.append({
            "source_id": f"{test_name}.{column_name}.{node_name}.{connection_id}",
            "name": test_name,
            "description": f"Node-level test {test_name} for node {node_details.get('name', '')}",
            "column_name": None,
            "node_id": node_id,  # Add node_id for proper matching in run details
        })

    # Column-level tests
    for col in columns:
        col_name = col.get("name")
        col_tests = col.get("appliedColumnTests", {}) or {}
        for test_key, is_enabled in col_tests.items():
            if not is_enabled:
                continue
            tests.append({
                "source_id": f"{test_key}.{col_name}.{node_name}.{connection_id}",
                "name": test_key,
                "description": f"Column-level test {test_key} on column {col_name}",
                "column_name": col_name,
                "node_id": node_id,  # Add node_id for proper matching in run details
            })
    return tests

def __save_transformations(config, pipeline_id, asset_id, transformations,pipeline_runs,is_run_enabled):
    """
    Save attribute-level transformations to core.pipeline_transformations.
    """
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        connection_name = connection_obj.get("name")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Delete existing transformations for this asset and pipeline
            query_string = f"""
                DELETE FROM core.pipeline_transformations
                WHERE asset_id = '{asset_id}' AND pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run_detail = pipeline_runs[-1] if is_run_enabled and pipeline_runs else ()
            if not transformations:
                return
            runid =last_run_detail[1] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else last_run_detail["source_id"]
            run_string = f"""
                select * from core.pipeline_runs where source_id ='{runid}'
            """
            cursor = execute_query(connection, cursor, run_string)
            latest_run = fetchone(cursor)

            insert_objects = []
            for t in transformations:
                
                properties = json.loads(t["properties"])
                query_input = (
                    t["id"],
                    f'{t["name"]}',
                    t["description"],
                    latest_run.get("source_id","") if latest_run else "",
                    t["properties"],
                    t["source_id"],
                    pipeline_id,
                    t["source_type"],
                    asset_id,
                    connection_id,
                    True,  # is_active
                    False, # is_delete
                    t["created_at"],
                    t["updated_at"],
                    latest_run.get("status","") if latest_run else "",
                    latest_run.get("error","") if latest_run else "",
                    latest_run.get("run_start_at","") if latest_run else "",
                    latest_run.get("run_end_at","") if latest_run else "",
                    latest_run.get("duration","") if latest_run else "",
                    t["last_run_at"]
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                insert_objects.append(query_param)

            # Insert in batches
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        INSERT INTO core.pipeline_transformations(
                            id, name, description, run_id, properties, source_id,
                            pipeline_id, source_type, asset_id, connection_id, is_active, is_delete,
                            created_at, updated_at, status, error, run_start_at, run_end_at, duration, last_run_at
                        ) VALUES {query_input}
                    """
                    cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error("Coalesce Connector - Save Transformations Failed", e)
        raise e

def __save_pipeline_runs(
    config: dict,
    pipeline_info: dict,
    asset_id: str,
    pipeline_id: str,
    transformations: list = None,
    static_tests: list = None,
    pipeline_tasks: list = None,
):
    """
    Save pipeline runs from Coalesce
    """
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        runs = pipeline_info.get("pipeline_runs", [])
        if not runs:
            return [], False, []

        latest_run = False
        all_pipeline_runs = []
        all_runs = []
        pipeline_runs = []  # Track pipeline runs for details insertion
        
        with connection.cursor() as cursor:
            insert_objects = []
            for i, run in enumerate(runs):
                run_id = run.get("pipeline_run_id")
                status = run.get("status", "")
                duration = run.get("durationInSeconds", 0)
                
                properties = {
                    "environment_id": run.get("environment_id"),
                    "node_id": run.get("node_id"),
                    "node_name": run.get("node_name"),
                    "run_counter": run.get("run_counter"),
                    "created_by": run.get("created_by"),
                    "run_type": run.get("run_type"),
                    "environment_name": run.get("environment_name"),
                }

                # Build individual run data for all_runs
                individual_run = {
                    "pipeline_run_id": run_id,
                    "status": status,
                    "error": run.get("error", ""),
                    "started_at": run.get("started_at"),
                    "finished_at": run.get("finished_at"),
                    "durationInSeconds": duration,
                    "environment_id": run.get("environment_id"),
                    "node_id": run.get("node_id"),
                    "node_name": run.get("node_name"),
                    "run_counter": run.get("run_counter"),
                    "created_by": run.get("created_by"),
                    "run_type": run.get("run_type"),
                    "environment_name": run.get("environment_name"),
                }

                # Check if run already exists
                query_string = f"""
                    SELECT id FROM core.pipeline_runs
                    WHERE asset_id = '{asset_id}' 
                    AND pipeline_id = '{pipeline_id}' 
                    AND source_id = '{run_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)

                if existing_run:
                    # Update existing run
                    query_string = f"""
                        UPDATE core.pipeline_runs SET 
                            status = '{get_pipeline_status(status.lower())}', 
                            error = '{run.get('error', '')}',
                            run_start_at = {f"'{run.get('started_at')}'" if run.get('started_at') else 'NULL'},
                            run_end_at = {f"'{run.get('finished_at')}'" if run.get('finished_at') else 'NULL'},
                            duration = '{duration}',
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        WHERE id = '{existing_run.get("id")}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    # Add to pipeline_runs for details insertion
                    pipeline_runs.append({
                        "id": existing_run.get("id"),
                        "source_id": run_id,
                        "is_update": True,
                    })
                    all_pipeline_runs.append({
                        "id": existing_run.get("id"),
                        "source_id": run_id,
                        "is_update": True,
                    })
                else:
                    # Insert new run
                    query_input = (
                        str(uuid4()),
                        run_id,
                        run_id,
                        get_pipeline_status(status.lower()),
                        run.get("error", ""),
                        run.get("started_at") if run.get("started_at") else None,
                        run.get("finished_at") if run.get("finished_at") else None,
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
                    query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                    insert_objects.append(query_param)
                    if not latest_run:
                        latest_run = True
                    
                all_runs.append(individual_run)

            # Insert new runs
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            INSERT INTO core.pipeline_runs(
                                id, source_id, technical_id, status, error, run_start_at, run_end_at, duration,
                                properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                            ) VALUES {query_input} 
                            RETURNING id, source_id;
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        # Get the inserted pipeline runs for details insertion
                        inserted_runs = fetchall(cursor)
                        pipeline_runs.extend(inserted_runs)
                    except Exception as e:
                        log_error("Coalesce Pipeline Runs Insert Failed", e)
            # Save individual run details - similar to ADF implementation
            __save_runs_details(
                config,
                runs,
                pipeline_id,
                pipeline_runs,
                transformations=transformations or [],
                static_tests=static_tests or [],
                pipeline_tasks=pipeline_tasks or [],
            )

        return all_pipeline_runs, latest_run, all_runs

    except Exception as e:
        log_error(f"Coalesce Connector - Save Pipeline Runs Failed", e)
        return [], False, []

def _parse_ts(ts_str: str):
    if not ts_str:
        return None
    try:
        # Handle Zulu suffix by converting to +00:00 for fromisoformat
        norm = ts_str.replace("Z", "+00:00")
        return norm
    except Exception:
        return ts_str

def __calculate_duration(start_time: str, end_time: str) -> float:
    """Calculate duration in seconds with millisecond precision (e.g., 0.80)"""
    try:
        if not start_time or not end_time:
            return 0.0
        
        start_dt = dateutil.parser.parse(start_time)
        end_dt = dateutil.parser.parse(end_time)
        duration = (end_dt - start_dt).total_seconds()
        return round(duration, 3)  # keep 2 decimal places
    except Exception:
        return 0.0


def __save_runs_details(
    config: dict,
    data: list,
    pipeline_id: str,
    pipeline_runs: list = None,
    transformations: list = None,
    static_tests: list = None,
    pipeline_tasks: list = None,
):
    """
    Save pipeline run details for Coalesce - similar to ADF implementation
    Each run can have detailed information about tasks/nodes that were executed
    """
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        insert_objects = []
        transformations = transformations or []
        static_tests = static_tests or []
        connection = get_postgres_connection(config)
        
        # Collect all updates for batch processing (for test updates)
        update_values = []
        
        # Track processed tests per run to detect duplicates
        processed_tests_per_run = {}  # {pipeline_run_id: set(test_source_ids)}
        
        with connection.cursor() as cursor:
            for run in data:
                # Get the corresponding pipeline_run_id from the database
                pipeline_run_id = get_data_by_key(
                    pipeline_runs, run.get("pipeline_run_id")
                )
                if not pipeline_run_id:
                    continue

                run_id_val = run.get("pipeline_run_id")
                # Extract run results for test processing
                run_results = run.get("run_results", {})
                node_results = run_results.get("data") if isinstance(run_results, dict) else None
                if node_results is None:
                    # Try run_steps if run_results doesn't have data
                    node_results = run.get("run_steps", [])
                if not isinstance(node_results, list):
                    node_results = []
                
                # Initialize tracking for this run
                if pipeline_run_id not in processed_tests_per_run:
                    processed_tests_per_run[pipeline_run_id] = set()
                
                # Process tests for nodes in run results (keep tests as is)
                if static_tests and node_results:
                    # Track which tests we've seen for this run
                    tests_seen_for_run = set()
                    
                    for node_info in node_results:
                        if not isinstance(node_info, dict):
                            continue
                        
                        node_id = node_info.get("nodeID", "")
                        node_name = node_info.get("name", "")
                        
                        if not node_id:
                            continue
                                                
                        # Process tests for this node
                        for mt in static_tests:
                            test_name = mt.get("name")
                            mt_node_id = mt.get("node_id", "")
                            test_source_id = mt.get("source_id", "")
                            
                            # Only process tests for this specific node
                            # If node_id is not set in metadata, skip (shouldn't happen, but safety check)
                            if not mt_node_id:
                                continue
                            
                            # Only process tests that belong to this node
                            if mt_node_id != node_id:
                                continue
                            
                            # Check if we've already processed this test for this run
                            test_key_for_run = f"{pipeline_run_id}_{test_source_id}"
                            if test_key_for_run in tests_seen_for_run:
                                continue
                            
                            tests_seen_for_run.add(test_key_for_run)
                            
                            run_result_detail = None
                            # Look for test in this node's queryResults
                            for result in node_info.get("queryResults", []) or []:
                                q_type = result.get("type", "")
                                # Map metadata test names to rendered result names
                                test_check = test_name
                                if test_name in ["hasNull", "isDistinct"]:
                                    if test_name == "hasNull":
                                        test_check = f"{mt.get('column_name')}: Null"
                                    elif test_name == "isDistinct":
                                        test_check = f"{mt.get('column_name')}: Unique"
                                if (result.get("name") or "").lower() != (test_check or "").lower():
                                    continue

                                # Build detail payload
                                if result.get("success") is True:
                                    status_label = "success"
                                elif result.get("success") is False:
                                    status_label = "failed"
                                else:
                                    status_label = (result.get("status") or "").lower()

                                error_info = result.get("error") or {}
                                error_text = error_info.get("errorString") or error_info.get("errorDetail") or ""
                                sql_code = result.get("sql") or ""
                                start_ts = result.get("startTime")
                                end_ts = result.get("endTime")

                                duration_seconds = __calculate_duration(start_ts, end_ts)

                                run_result_detail = {
                                    "status_label": status_label,
                                    "error_text": error_text,
                                    "sql": sql_code,
                                    "start_ts": start_ts,
                                    "end_ts": end_ts,
                                    "duration_seconds": duration_seconds,
                                    "display_name": result.get("name"),
                                    "query_id": result.get("queryID"),
                                    "node_id": node_id,
                                    "node_name": node_name,
                                }
                                break
                            
                            # Only process when we found detail in run results
                            if run_result_detail:
                                try:
                                    test_source_id = mt.get("source_id")
                                    
                                    # DEBUG: Check for duplicate processing
                                    test_key = f"{pipeline_run_id}_{test_source_id}"
                                    if test_key in processed_tests_per_run[pipeline_run_id]:
                                        continue
                                    
                                    # Mark as processed
                                    processed_tests_per_run[pipeline_run_id].add(test_key)
                                    
                                    # Check if test record exists
                                    query_string = f"""
                                        SELECT id FROM core.pipeline_runs_detail
                                        WHERE pipeline_run_id = '{pipeline_run_id}'
                                        AND source_id = '{test_source_id}'
                                        AND type = 'test'
                                    """
                                    cursor = execute_query(connection, cursor, query_string)
                                    existing_test_detail = fetchone(cursor)
                                    existing_test_detail_id = existing_test_detail.get("id") if existing_test_detail else None
                                    
                                    if existing_test_detail_id:
                                        # Collect test update values for batch update
                                        update_values.append((
                                            existing_test_detail_id,
                                            get_pipeline_status(run_result_detail.get("status_label")),
                                            (run_result_detail.get("error_text") or "").replace("'", "''"),
                                            (run_result_detail.get("sql") or "").replace("'", "''"),
                                            run_result_detail.get("start_ts"),
                                            run_result_detail.get("end_ts"),
                                            run_result_detail.get("duration_seconds", 0)
                                        ))
                                    else:
                                        # Insert new test record
                                        meta_test_query_input = (
                                            str(uuid4()),
                                            run_id_val,  # run_id
                                            mt.get("source_id"),
                                            "test",  # type
                                            run_result_detail.get("display_name") or mt.get("name"),
                                            get_pipeline_status(run_result_detail.get("status_label")),  # status
                                            run_result_detail.get("error_text") or "",
                                            (run_result_detail.get("sql") or "").replace("'", "''"),
                                            "",   # compiled_code
                                            run_result_detail.get("start_ts"),
                                            run_result_detail.get("end_ts"),
                                            run_result_detail.get("duration_seconds"),
                                            True,
                                            False,
                                            pipeline_run_id,
                                            pipeline_id,
                                            asset_id,
                                            connection_id,
                                        )
                                        mt_input_literals = ", ".join(["%s"] * len(meta_test_query_input))
                                        mt_query_param = cursor.mogrify(
                                            f"({mt_input_literals})", meta_test_query_input
                                        ).decode("utf-8")
                                        insert_objects.append(mt_query_param)
                                except Exception as e:
                                    log_error("Coalesce - Build metadata test detail row failed", e)
            
            # Save task-level run details (type="task") - ONE PER TASK PER RUN
            # Loop through runs first, then check which tasks exist in each run
            if pipeline_tasks:
                for run in data:
                    pipeline_run_id = get_data_by_key(
                        pipeline_runs, run.get("pipeline_run_id")
                    )
                    if not pipeline_run_id:
                        continue
                    
                    # Get run results for this run
                    run_results = run.get("run_results", {})
                    node_results = run_results.get("data") if isinstance(run_results, dict) else None
                    if node_results is None:
                        node_results = run.get("run_steps", [])
                    if not isinstance(node_results, list):
                        node_results = []
                    
                    # For each task, check if it exists in this run's results
                    # Track which tasks we've already processed for this run to avoid duplicates
                    processed_tasks_for_run = set()
                    
                    for task in pipeline_tasks:
                        task_source_id = task.get("source_id", "")  # New format: job_name_node_id_connection_id
                        task_node_id = task.get("node_id", "")  # Extract node_id for matching
                        task_name = task.get("name", "")
                        
                        if not task_source_id or not task_node_id:
                            continue
                        
                        # Skip if we've already processed this task for this run
                        task_key = f"{pipeline_run_id}_{task_source_id}"
                        if task_key in processed_tasks_for_run:
                            continue
                        
                        # Find this task in this run's results by matching nodeID (not source_id)
                        task_run_info = None
                        for node_result in node_results:
                            if isinstance(node_result, dict) and str(node_result.get("nodeID")) == str(task_node_id):
                                task_run_info = node_result
                                break
                        
                        # Only create record if task exists in this run's results
                        if not task_run_info:
                            continue
                        
                        # Use name from task_run_info if task_name is empty
                        final_task_name = task_name if task_name else task_run_info.get("name", "")
                        
                        # Mark this task as processed for this run
                        processed_tasks_for_run.add(task_key)
                        
                        # Extract task run information from run results
                        run_state = task_run_info.get("runState", "")
                        query_results = task_run_info.get("queryResults", []) or []
                        
                        # Calculate duration from query results
                        duration = 0.0
                        run_start_at = None
                        run_end_at = None
                        if query_results:
                            start_times = [q.get("startTime") for q in query_results if q.get("startTime")]
                            end_times = [q.get("endTime") for q in query_results if q.get("endTime")]
                            if start_times and end_times:
                                run_start_at = min(start_times)
                                run_end_at = max(end_times)
                                duration = __calculate_duration(run_start_at, run_end_at)
                        
                        # Determine status
                        status = get_pipeline_status(run_state.lower()) if run_state else run.get("status", "")
                        
                        # Get error if any
                        error = ""
                        failed_queries = [q for q in query_results if q.get("success") is False]
                        if failed_queries:
                            error_info = failed_queries[0].get("error", {})
                            error = error_info.get("errorString", "") or error_info.get("errorDetail", "") or ""
                        
                        # Check if task run detail already exists (ONE PER TASK PER RUN)
                        query_string = f"""
                            SELECT id FROM core.pipeline_runs_detail
                            WHERE pipeline_run_id = '{pipeline_run_id}'
                            AND source_id = '{task_source_id}'
                            AND type = 'task'
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        existing_task_detail = fetchone(cursor)
                        
                        if existing_task_detail:
                            # Update existing task run detail
                            update_query = f"""
                                UPDATE core.pipeline_runs_detail SET
                                    name = '{final_task_name.replace("'", "''")}',
                                    status = '{status}',
                                    error = '{error.replace("'", "''")}',
                                    run_start_at = {f"'{run_start_at}'" if run_start_at else 'NULL'},
                                    run_end_at = {f"'{run_end_at}'" if run_end_at else 'NULL'},
                                    duration = {duration if duration else 'NULL'}
                                WHERE id = '{existing_task_detail.get("id")}'
                            """
                            cursor = execute_query(connection, cursor, update_query)
                        else:
                            # Insert new task run detail - ONLY ONE PER TASK PER RUN
                            task_detail_query_input = (
                                str(uuid4()),
                                run.get("pipeline_run_id", ""),  # run_id (source_id from run)
                                task_source_id,  # source_id (node_id)
                                "task",  # type
                                final_task_name,  # name (use from task_run_info if task_name is empty)
                                status,  # status
                                error.replace("'", "''") if error else "",  # error
                                "",  # source_code
                                "",  # compiled_code
                                run_start_at if run_start_at else None,  # run_start_at
                                run_end_at if run_end_at else None,  # run_end_at
                                duration if duration else None,  # duration
                                True,  # is_active
                                False,  # is_delete
                                pipeline_run_id,  # pipeline_run_id (database ID)
                                pipeline_id,  # pipeline_id
                                asset_id,  # asset_id
                                connection_id,  # connection_id
                            )
                            input_literals = ", ".join(["%s"] * len(task_detail_query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals})", task_detail_query_input
                            ).decode("utf-8")
                            insert_objects.append(query_param)
        
            # Perform unified batch updates for test records
            if update_values:
                update_params = []
                for detail_id, status, error, source_code, start_at, end_at, dur in update_values:
                    query_input = (
                        detail_id,
                        status,
                        error,
                        source_code,
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
                                source_code = update_data.source_code,
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
                            ) AS update_data(id, status, error, source_code, run_start_at, run_end_at, duration)
                            WHERE pipeline_runs_detail.id::text = update_data.id::text
                        """
                        cursor = execute_query(connection, cursor, batch_update_query)
                    except Exception as e:
                        log_error(f"Error in batch update for Coalesce run details: {e}", e)
                        continue

            # Insert run details in batches
            test_insert_count = sum(1 for obj in insert_objects if '"test"' in obj or "'test'" in obj)            
            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            INSERT INTO core.pipeline_runs_detail(
                                id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                                run_start_at, run_end_at, duration, is_active, is_delete,
                                pipeline_run_id, pipeline_id, asset_id, connection_id
                            ) VALUES {query_input}
                        """
                        cursor = execute_query(connection, cursor, query_string)
                    except Exception as e:
                        log_error("Coalesce Runs Details Insert Failed", e)
                                     
    except Exception as e:
        log_error(f"Coalesce Connector - Save Runs Details Failed", e)
        raise e

def __get_pipeline_id(config: dict) -> str:
    """
    Get pipeline ID from database
    """
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT id FROM core.pipeline
                WHERE asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            if pipeline:
                return pipeline.get("id")
    except Exception as e:
        log_error(f"Coalesce Connector - Get Pipeline ID Failed", e)
        raise e

def __update_asset_description(config: dict, asset_id: str, description: str):
    """
    Update asset description
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                UPDATE core.asset SET description='{description.replace("'", "''")}'
                WHERE id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Coalesce Connector - Update Asset Description Failed", e)

def __update_pipeline_stats(config: dict):
    """
    Update pipeline statistics
    """
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        with connection.cursor() as cursor:
            # Get Last Run Info
            query_string = f"""
                SELECT id, source_id, run_end_at, duration, status
                FROM core.pipeline_runs
                WHERE asset_id = '{asset_id}'
                ORDER BY run_end_at DESC
                LIMIT 1
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
                COUNT(DISTINCT pipeline_tests.id) as tot_tests,
                COUNT(DISTINCT pipeline_transformations.id) as tot_transformations
            FROM core.pipeline
            LEFT JOIN core.pipeline_columns ON pipeline_columns.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_runs ON pipeline_runs.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_tests ON pipeline_tests.asset_id = pipeline.asset_id
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
                    "tot_tests": pipeline_stats.get("tot_tests",0),
                    "tot_transformations": pipeline_stats.get("tot_transformations",0)
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
        log_error(f"Coalesce Connector - Update Pipeline Stats Failed", e)

def __prepare_description(properties, type="pipeline") -> str:
    """
    Prepare description for pipeline
    """
    name = properties.get("name", "")
    environment = properties.get("workspace", "")
    project = properties.get("project", "")
    
    if type == "pipeline":
        description = f"This {name} pipeline is under the Project {project} and Workspace {environment}."
    else:
        description = f"This {name} is under the Project {project} and Environment {environment}."
    
    return description

def extract_pipeline_measure(run_history: list, tasks_pull: bool):
    """
    Extract and execute pipeline measures for Coalesce runs
    """
    if not run_history:
        return
    
    # Fetch pipeline_name (job_name) from pipeline table
    asset = TASK_CONFIG.get("asset", {})
    asset_id = asset.get("id") if asset else TASK_CONFIG.get("asset_id")
    if not asset_id:
        asset_id = TASK_CONFIG.get("asset_id")
    
    connection = get_postgres_connection(TASK_CONFIG)
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
    
    # Sort by started_at timestamp and get latest 2 runs
    run_history = sorted(run_history, key=lambda x: x.get("started_at"), reverse=True)[:2]
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
    
    # Execute pipeline-level measure
    job_run_detail = {
        "duration": latest_run.get("durationInSeconds"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(TASK_CONFIG, "asset", job_run_detail, job_name=pipeline_name)

    # Get Tasks and execute measure for each task (if tasks are enabled)
    if not tasks_pull:
        return
    
    tasks = get_pipeline_tasks(TASK_CONFIG)
    if not tasks:
        return
    
    # Get task run details from pipeline_runs_detail for each task
    for task in tasks:
        task_id = task.get("task_id")
        source_id = task.get("source_id")
        task_name = task.get("name")
        
        if not task_id or not source_id:
            continue
        
        # Query latest and previous task runs from pipeline_runs_detail
        with connection.cursor() as cursor:
            task_runs_query = f"""
                SELECT 
                    prd.run_start_at,
                    prd.run_end_at,
                    prd.duration,
                    pr.run_start_at as pipeline_run_start_at
                FROM core.pipeline_runs_detail prd
                JOIN core.pipeline_runs pr ON pr.id = prd.pipeline_run_id
                WHERE prd.asset_id = '{asset_id}'
                AND prd.source_id = '{source_id}'
                AND prd.type = 'task'
                AND prd.is_active = true
                AND prd.is_delete = false
                ORDER BY pr.run_start_at DESC
                LIMIT 2
            """
            cursor = execute_query(connection, cursor, task_runs_query)
            task_runs = fetchall(cursor)
            
            if not task_runs:
                continue
            
            # Latest run is first, previous is second (if exists)
            latest_task_run = task_runs[0]
            previous_task_run = task_runs[1] if len(task_runs) > 1 else {}
            
            # Build task_run_detail similar to DBT
            task_run_detail = {
                "duration": latest_task_run.get("duration"),
                "last_run_date": latest_task_run.get("run_start_at") or latest_task_run.get("pipeline_run_start_at"),
                "previous_run_date": previous_task_run.get("run_start_at") if previous_task_run else None,
            }
            
            # Pass pipeline_name (job_name) and task_name for task level measures
            execute_pipeline_measure(TASK_CONFIG, "task", task_run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)

    
def __build_lineage_from_tasks(config: dict, tasks: list, asset_id: str, asset_properties: dict, pipeline_name: str) -> dict:
    """
    Build lineage from tasks with node_details.
    Creates tables with all metadata and relations based on sourceMapping and columnReferences.
    """
    try:
        lineage = {"tables": [], "relations": []}
        platform_kind = asset_properties.get("platform_kind", "")
        connection = config.get("connection")
        connection_id = connection.get("id")
        
        # Build node_id to node_details mapping for quick lookup
        node_details_map = {}
        for task in tasks:
            node_id = task.get("node_id", "")
            node_details = task.get("node_details", {})
            if node_id and node_details:
                node_details_map[node_id] = node_details
        
        # Build node_id to table mapping for relations
        node_id_to_table_id = {}
        
        # Step 1: Create tables from all tasks
        for task in tasks:
            node_id = task.get("node_id", "")
            node_details = task.get("node_details", {})
            
            if not node_details:
                continue
            
            # Get table metadata
            table_name = node_details.get("name", "")
            database = node_details.get("database", "")
            schema = node_details.get("schema", "")
            location = node_details.get("locationName", "")
            node_type = node_details.get("nodeType", "")
            
            # Use asset_id for the main pipeline node, otherwise use node name
            entity_name = f"{asset_properties.get('name')}_{node_id}_{connection_id}"
            
            # Get columns from metadata
            metadata = node_details.get("metadata", {})
            columns = metadata.get("columns", []) if isinstance(metadata, dict) else []
            
            # Normalize fields
            normalized_fields = []
            for col in columns:
                col_copy = dict(col)
                if "dataType" in col_copy:
                    col_copy["data_type"] = col_copy.pop("dataType")
                normalized_fields.append(col_copy)
            is_upstream = node_details.get("sourceMapping",[])
            stream_type = "downstream" if is_upstream else "upstream"
            
            # Create table entry
            table_id = str(uuid4())
            table = {
                "id": table_id,
                "name": table_name,
                "database": database,
                "schema": schema,
                "type": stream_type,
                "location": location,
                "entity_name": entity_name,
                "connection_type": "COALESCE",
                "fields": normalized_fields,
                "platform_kind": platform_kind,
                "source_id": node_id  # Store node_id for relation mapping
            }
            lineage["tables"].append(table)
            node_id_to_table_id[node_id] = node_id
        
        # Step 2: Create relations from sourceMapping dependencies and columnReferences
        for task in tasks:
            node_id = task.get("node_id", "")
            node_details = task.get("node_details", {})
            
            if not node_details:
                continue
            
            target_table_id = node_id_to_table_id.get(node_id)
            target_table_id = f"{asset_properties.get('name')}_{node_id}_{connection_id}"
            if not target_table_id:
                continue
            
            metadata = node_details.get("metadata", {})
            source_mapping = metadata.get("sourceMapping", [])
            columns = metadata.get("columns", []) if isinstance(metadata, dict) else []
            
            # Process each sourceMapping entry
            for mapping in source_mapping:
                dependencies = mapping.get("dependencies", [])
                
                for dep in dependencies:
                    source_node_name = dep.get("nodeName", "")
                    source_location = dep.get("locationName", "")
                    
                    # Find source node_id - prioritize finding by name+location in tasks, then check aliases
                    source_node_id = None
                    
                    # First, try to find by name and location in node_details_map (from tasks)
                    for nid, nd in node_details_map.items():
                        if nd.get("name") == source_node_name and nd.get("locationName") == source_location:
                            source_node_id = nid
                            break
                    
                    # If not found, try aliases (but verify the alias node_id exists in tasks)
                    if not source_node_id:
                        aliases = mapping.get("aliases", {})
                        if source_node_name in aliases:
                            alias_node_id = aliases[source_node_name]
                            # Only use alias if the node exists in our tasks
                            if alias_node_id in node_details_map:
                                source_node_id = alias_node_id
                    
                    if not source_node_id:
                        continue
                    
                    source_table_id = node_id_to_table_id.get(source_node_id)
                    source_table_id = f"{asset_properties.get('name')}_{source_node_id}_{connection_id}"
                    if not source_table_id:
                        continue
                    
                    # Create table-level relation
                    table_relation = {
                        "srcTableId": source_table_id,
                        "tgtTableId": target_table_id
                    }
                    
                    # Check if relation already exists
                    if not any(r.get("srcTableId") == source_table_id and r.get("tgtTableId") == target_table_id 
                              for r in lineage["relations"]):
                        lineage["relations"].append(table_relation)
                    
                    # Create column-level relations from columnReferences
                    source_node_details = node_details_map.get(source_node_id, {})
                    source_columns = source_node_details.get("metadata", {}).get("columns", []) if isinstance(source_node_details.get("metadata"), dict) else []
                    
                    # Build source column_id to column name mapping
                    source_col_id_to_name = {}
                    for sc in source_columns:
                        col_id = sc.get("columnID", "")
                        col_name = sc.get("name", "")
                        if col_id and col_name:
                            source_col_id_to_name[col_id] = col_name
                    
                    # Process target columns and their sources
                    for target_col in columns:
                        target_col_name = target_col.get("name", "")
                        target_col_sources = target_col.get("sources", [])
                        
                        for source in target_col_sources:
                            column_refs = source.get("columnReferences", [])
                            
                            for col_ref in column_refs:
                                ref_node_id = col_ref.get("nodeID", "")
                                ref_col_id = col_ref.get("columnID", "")
                                
                                # Only process if this reference points to our source node
                                if ref_node_id == source_node_id and ref_col_id:
                                    source_col_name = source_col_id_to_name.get(ref_col_id)
                                    
                                    if source_col_name and target_col_name:
                                        # Create field-level relation
                                        field_relation = {
                                            "srcTableId": source_table_id,
                                            "tgtTableId": target_table_id,
                                            "srcTableColName": source_col_name,
                                            "tgtTableColName": target_col_name
                                        }
                                        
                                        # Check if field relation already exists
                                        if not any(r.get("srcTableId") == source_table_id and 
                                                  r.get("tgtTableId") == target_table_id and
                                                  r.get("srcTableColName") == source_col_name and
                                                  r.get("tgtTableColName") == target_col_name
                                                  for r in lineage["relations"]):
                                            lineage["relations"].append(field_relation)

        return lineage
    except Exception as e:
        log_error("Coalesce Pipeline - Build Lineage from Tasks Failed", e)
        return {"tables": [], "relations": []}
