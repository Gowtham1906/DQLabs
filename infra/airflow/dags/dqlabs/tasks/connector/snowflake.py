"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
from uuid import uuid4
import datetime
import pytz

from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    get_native_connection,
)
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.utils.extract_workflow import get_queries, update_asset_run_id
from dqlabs.app_helper.lineage_helper import (save_snowflake_lineage, update_pipeline_propagations, handle_alerts_issues_propagation, 
get_asset_metric_count, update_asset_metric_count)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_constants.dq_constants import ASSET_TYPE_TASK, ASSET_TYPE_PIPE, ASSET_TYPE_PROCEDURE,STOPPED_OR_STALLED_STATES
from dqlabs.app_helper.pipeline_helper import pipeline_auto_tag_mapping, update_pipeline_last_runs
from dqlabs.utils.pipeline_measure import execute_pipeline_measure


def extract_snowflake_pipeline(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
        
        # Update Status
        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        update_asset_run_id(run_id, config)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        asset = config.get("asset", {})
        asset_type = asset.get("type").lower()

        # Update Database and schema
        asset_properties = asset.get("properties", {})
        asset_properties = asset_properties if asset_properties else {}
        asset_database = config.get("database_name")
        asset_schema = config.get("schema")
        if not asset_database and asset_properties:
            asset_database = asset_properties.get("database")
        if not asset_schema and asset_properties:
            asset_schema = asset_properties.get("schema")
        asset_database = asset_database if asset_database else ""
        asset_schema = asset_schema if asset_schema else ""
        config.update({"database_name": asset_database, "schema": asset_schema})
        credentials = config.get("connection").get("credentials")

        # Metadata Config
        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        tasks_pull = metadata_pull_config.get("tasks", False)
        transformations_pull = metadata_pull_config.get("transformations", False)
        latest_run = False

        # Get Queries
        default_queries = get_queries(config)

        # Get Metadata
        if asset_type == ASSET_TYPE_TASK and tasks_pull:
            __get_task_metadata(config, default_queries)
        elif asset_type == ASSET_TYPE_PIPE:
            __get_pipe_metadata(config, default_queries)
        else:
            if transformations_pull:
                __get_procedure_metadata(config, default_queries)

        # Get Runs
        if runs_pull:
           latest_run = __get_runs(config, default_queries, asset_type)
        __update_last_run_stats(config, asset_type, default_queries)
        __update_pipeline_properties(config, asset_type)


        lineage_is_enabled = (
            credentials.get("lineage", None) if credentials else None
        )
        if lineage_is_enabled:
            save_snowflake_lineage(config)

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
        asset_id = config.get("asset_id")
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)

        # Extract Pipeline Measure
        if latest_run:
            extract_pipeline_measure(config, default_queries, asset_type)

        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Snowflake Pipeline pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __get_task_metadata(config: dict, queries: dict):
    """
    Get Task Metadata
    """
    try:
        # Initialize Asset Detail
        asset = config.get("asset")
        asset_name = asset.get("name")
        database_name =  config.get("database_name")
        schema_name = config.get("schema")
        asset_id = asset.get("id")
        connection_id = asset.get("connection_id")

        # Get Task Metadata
        metadata_query = queries.get("pipeline_metadata")
        task_metadata_query = (metadata_query.get('task').replace('<task_name>', asset_name)
                            .replace('<database_name>',database_name)
                                .replace('<schema_name>', schema_name))
        source_connection = get_native_connection(config)
        task_metadata, _ = execute_native_query(
            config, task_metadata_query, source_connection
        )

        # Get Task Run Duration Detail
        task_run_query = (metadata_query.get('task_duration').replace('<task_name>', asset_name)
                            .replace('<database_name>',database_name)
                                .replace('<schema_name>', schema_name))
        source_connection = get_native_connection(config)
        task_run_detail, _ = execute_native_query(
            config, task_run_query, source_connection
        )
        run_start_at = task_run_detail.get("start_time") if task_run_detail else  datetime.datetime.now()
        run_end_at = task_run_detail.get("end_time") if task_run_detail else datetime.datetime.now()
        duration = task_run_detail.get("duration") if task_run_detail else None


        # Update Task Metadata Detail
        compiled_code = task_metadata.get('definition', '')
        compiled_code = compiled_code.replace("'", "''") if compiled_code else ""
        connection = get_postgres_connection(config)
        task_properties = {
            'schedule': task_metadata.get("schedule")
        }
        existing_task = get_existing_tasks(config)
        with connection.cursor() as cursor:
            update_column = f"""updated_at = '{task_metadata.get("last_committed_on")}',"""  if task_metadata.get("last_committed_on") else ""
            if existing_task:
                query_string = f"""
                    update core.pipeline_tasks set
                        source_id = '{task_metadata.get("id")}', 
                        source_code = '{compiled_code}',
                        compiled_code = '{compiled_code}',
                        created_at = '{task_metadata.get('created_on')}',
                        {update_column}
                        status = '{task_metadata.get('state')}',
                        properties = '{json.dumps(task_properties, default=str).replace("'", "''")}',
                        duration = '{duration}',
                        run_start_at = '{run_start_at}',
                        run_end_at = '{run_end_at}'
                    where asset_id = '{asset_id}'
                """
            else:
                update_at_column = "updated_at," if task_metadata.get("last_committed_on") else ""
                update_at_value = f"""'{task_metadata.get("last_committed_on")}',""" if task_metadata.get("last_committed_on") else ""
                pipeline_id = __get_pipeline_id(config)
                query_string = f"""
                    insert into core.pipeline_tasks (
                        id, asset_id, source_id, source_code, compiled_code, properties, created_at, {update_at_column} status,
                        pipeline_id, is_active, is_delete, is_selected, connection_id, name, database, schema, duration, run_start_at,
                        run_end_at
                    ) values (
                        '{uuid4()}', '{asset_id}', '{task_metadata.get("id")}', 
                        '{compiled_code}', '{compiled_code}', 
                        '{json.dumps(task_properties, default=str).replace("'", "''")}', 
                        '{task_metadata.get("created_on")}', {update_at_value}
                        '{task_metadata.get("state")}', '{pipeline_id}', {True}, {False}, {True}, '{connection_id}', '{asset_name}',
                        '{database_name}', '{schema_name}', '{duration}', '{run_start_at}', '{run_end_at}'
                    )
                """
            execute_query(connection, cursor, query_string)
            pipeline_query_string = f"""
                update core.pipeline set
                    source_code = '{compiled_code}',
                    compiled_code = '{compiled_code}',
                    {update_column}
                    created_at = '{task_metadata.get('created_on')}'
                where asset_id = '{asset_id}'
            """
            execute_query(connection, cursor, pipeline_query_string)
        config.update({"task_id": task_metadata.get("id")})
    except Exception as e:
        log_error(f"Snowflake Connector - Task metadata Failed ", e)
        raise e
    
def __get_pipe_metadata(config: dict, queries: dict):
    """
    Get Snow Pipe Metadata
    """
    try:
        # Initialize Asset Detail
        asset = config.get("asset")
        asset_name = asset.get("name")
        database_name = config.get("database_name")
        schema_name = config.get("schema")
        asset_id = asset.get("id")

        # Get Pipe Metadata
        metadata_query = queries.get("pipeline_metadata")
        pipe_metadata_query = (metadata_query.get('pipe').replace('<pipe_name>', asset_name)
                            .replace('<database_name>', database_name)
                            .replace('<schema_name>', schema_name))
        source_connection = get_native_connection(config)
        pipe_metadata, _ = execute_native_query(
            config, pipe_metadata_query, source_connection
        )

        # Update Pipe Metadata
        compiled_code = pipe_metadata.get('definition', '')
        compiled_code = compiled_code.replace("'", "''") if compiled_code else ""
        connection = get_postgres_connection(config)
        properties = {
            "pattern": pipe_metadata.get("pattern"),
            "auto_ingest": pipe_metadata.get("is_autoingest_enabled")
        }
        pipe_id = pipe_metadata.get("pipe_id")
        config.update({"pipe_id": str(pipe_id)})
        with connection.cursor() as cursor:
            query_string = f"""
                update core.pipeline set 
                    source_id = '{pipe_id}', 
                    source_code = '{compiled_code}',
                    compiled_code = '{compiled_code}',
                    properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                where asset_id = '{asset_id}'
            """
            execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Snowflake Connector - Pipe metadata Failed ", e)
        raise e


def __get_procedure_metadata(config: dict, queries: dict):
    try:
        # Initiaze Asset Detail
        asset = config.get("asset")
        asset_name = asset.get("name")
        database_name =  config.get("database_name")
        schema_name = config.get("schema")
        asset_id = asset.get("id")
        connection_id = asset.get("connection_id")

        # Get Procedure Metadata
        metadata_query = queries.get("pipeline_metadata")
        procedure_metadata_query = (metadata_query.get('procedure').replace('<procedure_name>', asset_name)
                            .replace('<database_name>',database_name)
                                .replace('<schema_name>', schema_name))
        source_connection = get_native_connection(config)
        procedure_metadata, _ = execute_native_query(
            config, procedure_metadata_query, source_connection
        )

        # Get Procedure Run Detail
        table_name = f"{database_name}.{schema_name}.{asset_name}"
        procedure_run_query = (metadata_query.get('procedure_duration').replace('<name>', f"call {asset_name.lower()}%")
                        .replace('<query_name>', f"call {table_name.lower()}%")
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        )
        source_connection = get_native_connection(config)
        procedure_run_detail, _ = execute_native_query(
            config, procedure_run_query, source_connection
        )
        run_start_at = procedure_run_detail.get("start_time") if procedure_run_detail else  datetime.datetime.now()
        run_end_at = procedure_run_detail.get("end_time") if procedure_run_detail else datetime.datetime.now()
        duration = procedure_run_detail.get("duration") if procedure_run_detail else None

        # Update Metadata
        compiled_code = procedure_metadata.get('procedure_definition', '')
        compiled_code = compiled_code.replace("'", "''") if compiled_code else ""
        properties = {
            "database": procedure_metadata.get("procedure_catalog"),
            "schema": procedure_metadata.get("procedure_schema"),
            "owner": procedure_metadata.get("procedure_owner"),
            "language":procedure_metadata.get("procedure_language")
        }
        existing_transformation = get_existing_transformation(config)
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if existing_transformation:
                query_string = f"""
                    update core.pipeline_transformations set
                        source_id = '{procedure_metadata.get("procedure_name")}', 
                        source_code = '{compiled_code}',
                        compiled_code = '{compiled_code}',
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                        created_at = '{procedure_metadata.get("created")}',
                        updated_at = '{procedure_metadata.get("last_altered")}',
                        duration = '{duration}',
                        run_start_at = '{run_start_at}',
                        run_end_at = '{run_end_at}'
                    where asset_id = '{asset_id}'
                """
            else:
                pipeline_id = __get_pipeline_id(config)
                query_string = f"""
                    insert into core.pipeline_transformations (
                        id, asset_id, source_id, source_code, compiled_code, properties, created_at, updated_at, run_start_at, run_end_at,
                        database, schema, owner, pipeline_id, is_active, is_delete, connection_id, name, duration
                    ) values (
                        '{uuid4()}', '{asset_id}', '{procedure_metadata.get("procedure_name")}', 
                        '{compiled_code}', '{compiled_code}', 
                        '{json.dumps(properties, default=str).replace("'", "''")}', 
                        '{procedure_metadata.get("created")}', '{procedure_metadata.get("last_altered")}',
                        '{run_start_at}', '{run_end_at}', '{database_name}', '{schema_name}',
                        '{procedure_metadata.get("procedure_owner")}', '{pipeline_id}', {True}, {False}, '{connection_id}', '{asset_name}',
                        '{duration}'
                    )
                """
            execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Snowflake Connector - Procedure metadata Failed ", e)
        raise e


def __extract_task_runs(run: dict, config_properties: dict):
    run_id = run.get("run_id")
    properties = {
        "root_task_id": run.get("root_task_id"), 
        "query_id": run.get("query_id"), 
        "scheduled_from" : run.get("scheduled_from"),
        "scheduled_time": run.get("scheduled_time")
    }
    run_uuid = uuid4()
    run_duration = calculate_duration(run.get("query_start_time"), run.get("completed_time"))
    pipeline_status = get_run_status(run.get('state'))
    query_input = (
        run_uuid,
        run_id,
        run_id,
        pipeline_status,
        run.get('error_message', ''),
        run.get('query_start_time') if run.get(
            'query_start_time') else None,
        run.get('completed_time') if run.get(
            'completed_time') else None,
        run_duration,
        json.dumps(properties, default=str).replace("'", "''"),
        True,
        False,
        config_properties.get("pipeline_id"),
        config_properties.get("asset_id"),
        config_properties.get("connection_id"),
    )
    run_detail = {
        "run_id": run_id,
        "pipeline_run_id": run_uuid,
        "error": run.get("error_message", ""),
        "start_time": run.get('query_start_time') if run.get('query_start_time') else None,
        "end_time": run.get('completed_time') if run.get('completed_time') else None,
        "duration": run_duration,
        "pipeline_id": config_properties.get("pipeline_id"),
        "asset_id": config_properties.get("asset_id"),
        "connection_id": config_properties.get("connection_id"),
        "query": run.get("query_text", ""),
        "type": "task",
        "status": pipeline_status,
        "source_id": config_properties.get("task_id"),
        "source_name": config_properties.get("asset_name")
    }
    return query_input, run_detail


def __extract_pipe_runs(run: dict, config_properties: dict):
    run_id = run.get("start_time")
    properties = {
        "credits_used": run.get("credits_used"), 
        "bytes_inserted": run.get("bytes_inserted"), 
        "files_inserted" : run.get("files_inserted")
    }
    run_uuid = uuid4()
    run_duration = calculate_duration(run.get("start_time"), run.get("end_time"))
    pipeline_status = "success" if run.get("files_inserted", 0) else "failed"
    query_input = (
        run_uuid,
        run_id,
        run_id,
        pipeline_status,
        "",
        run.get('start_time') if run.get(
            'start_time') else None,
        run.get('end_time') if run.get(
            'end_time') else None,
        run_duration,
        json.dumps(properties, default=str).replace("'", "''"),
        True,
        False,
        config_properties.get("pipeline_id"),
        config_properties.get("asset_id"),
        config_properties.get("connection_id"),
    )
    run_detail = {
        "run_id": run_id,
        "pipeline_run_id": run_uuid,
        "error": "",
        "start_time": run.get('start_time') if run.get('start_time') else None,
        "end_time": run.get('end_time') if run.get('end_time') else None,
        "duration": run_duration,
        "pipeline_id": config_properties.get("pipeline_id"),
        "asset_id": config_properties.get("asset_id"),
        "connection_id": config_properties.get("connection_id"),
        "query": run.get("query_text", ""),
        "type": "pipe",
        "status": pipeline_status,
        "source_id": config_properties.get("asset_name"),
        "source_name": config_properties.get("asset_name")
    }
    return query_input, run_detail

def __extract_procedure_runs(run: dict, config_properties: dict):
    run_id = run.get("query_id")
    run_uuid = uuid4()
    run_duration = calculate_duration(run.get("start_time"), run.get("end_time"))
    pipeline_status = get_run_status(run.get('execution_status'))
    query_input = (
        run_uuid,
        run_id,
        run_id,
        pipeline_status,
        run.get('error_message', ''),
        run.get('start_time') if run.get(
            'start_time') else None,
        run.get('end_time') if run.get(
            'end_time') else None,
        run_duration,
        json.dumps({}, default=str).replace("'", "''"),
        True,
        False,
        config_properties.get("pipeline_id"),
        config_properties.get("asset_id"),
        config_properties.get("connection_id"),
    )
    run_detail = {
        "run_id": run_id,
        "pipeline_run_id": run_uuid,
        "error": run.get("error_message", ""),
        "start_time": run.get('start_time') if run.get('start_time') else None,
        "end_time": run.get('end_time') if run.get('end_time') else None,
        "duration": run_duration,
        "pipeline_id": config_properties.get("pipeline_id"),
        "asset_id": config_properties.get("asset_id"),
        "connection_id": config_properties.get("connection_id"),
        "query": run.get("query_text", ""),
        "type": "transformation",
        "status": pipeline_status,
        "source_id": config_properties.get("asset_name"),
        "source_name": config_properties.get("asset_name")
    }
    return query_input, run_detail

def __get_runs(config: dict, queries: dict, asset_type: str):
    try:
        connection = config.get("connection", {})
        credentials = connection.get("credentials", {})
        connection_id = connection.get('id')
        asset = config.get("asset")
        asset_name = asset.get("name")
        database_name =  config.get("database_name")
        schema_name = config.get("schema")
        asset_id = asset.get("id")
        pipeline_id = __get_pipeline_id(config)
        latest_run = False

        # Runs Filter
        no_of_runs = credentials.get("no_of_runs", "30")
        status = credentials.get("status", "all")
        no_of_runs = int(no_of_runs) if type(no_of_runs) == str else no_of_runs
        end = datetime.datetime.today()
        start = end - datetime.timedelta(days=no_of_runs)
        start_time = start.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")
        
        metadata_query = queries.get("pipeline_metadata")
        if asset_type == ASSET_TYPE_TASK:
            run_filter = f"SCHEDULED_TIME BETWEEN '{start_time}' AND '{end_time}'"
            if status != "all":
                status = "SUCCEEDED" if status == "success" else "FAILED"
                run_filter = f"{run_filter} AND STATE = '{status}'"
            run_query = (metadata_query.get('task_runs').replace('<task_name>', asset_name)
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        .replace("<run_filter>", run_filter))
        elif asset_type == ASSET_TYPE_PIPE:
            if status == "failed":
                return False
            run_filter = f"START_TIME BETWEEN '{start_time}' AND '{end_time}'"
            pipe_id = config.get("pipe_id")
            run_query = metadata_query.get('pipe_runs').replace('<pipe_id>', pipe_id).replace("<run_filter>", run_filter)
        else:
            run_filter = f"START_TIME  BETWEEN '{start_time}' AND '{end_time}'"
            if status != "all":
                status = "SUCCESS" if status == "success" else "FAIL"
                run_filter = f"{run_filter} AND EXECUTION_STATUS = '{status}'"
            table_name = f"{database_name}.{schema_name}.{asset_name}"
            run_query = (metadata_query.get('procedure_runs').replace('<name>', f"call {asset_name.lower()}%")
                        .replace('<query_name>', f"call {table_name.lower()}%")
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        .replace("<run_filter>", run_filter)
                        )
        source_connection = get_native_connection(config)
        runs, _ = execute_native_query(
            config, run_query, source_connection, is_list=True
        )
        runs = runs if runs else []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            existing_run_query = f"select source_id from core.pipeline_runs where asset_id='{asset_id}' and pipeline_id='{pipeline_id}'"
            cursor = execute_query(connection, cursor, existing_run_query)
            existing_runs = fetchall(cursor)
            existing_runs = [run.get("source_id") for run in existing_runs]
            insert_runs = []
            run_details = []
            properties = {
                "asset_id": asset_id,
                "asset_name": asset_name,
                "connection_id": connection_id,
                "pipeline_id": pipeline_id
            }
            for run in runs:
                run_id = run.get("run_id")
                if asset_type == ASSET_TYPE_PIPE:
                    run_id = run.get("start_time")
                    run_id = datetime.datetime.fromisoformat(str(run_id))
                    run_id = run_id.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S+00')
                elif asset_type == ASSET_TYPE_PROCEDURE:
                    run_id = run.get("query_id")
                if str(run_id) not in existing_runs:
                    if asset_type == ASSET_TYPE_TASK:
                        properties.update({"task_id": config.get("task_id")})
                        query_input , run_detail = __extract_task_runs(run, properties)
                    elif asset_type == ASSET_TYPE_PIPE:
                        query_input , run_detail = __extract_pipe_runs(run, properties)
                    else:
                        query_input , run_detail = __extract_procedure_runs(run, properties)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_runs.append(query_param)
                    run_details.append(run_detail)
                    if not latest_run:
                        latest_run = True
           
            insert_runs = split_queries(insert_runs)
            for input_values in insert_runs:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('Snowflake Task Runs Insert Failed  ', e)
            if run_details:
                __save_run_details(config, run_details)
        return latest_run
    except Exception as e:
        log_error(f"Snowflake Connector - Task Runs Failed ", e)
        raise e
    
def __save_run_details(config: dict, runs: list):
    try:
        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in runs:
                query_input = (
                    uuid4(),
                    run.get("run_id", ''),
                    run.get('source_id'),
                    run.get("type", "task"),
                    run.get('source_name'),
                    run.get('status'),
                    run.get('error', ''),
                    run.get("query", ""),
                    run.get("query", ""),
                    run.get('start_time'),
                    run.get('end_time'),
                    run.get('duration'),
                    True,
                    False,
                    run.get("pipeline_run_id"),
                    run.get("pipeline_id"),
                    run.get("asset_id"),
                    run.get("connection_id"),
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
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('Snowflake Runs Details Insert Failed  ', e)
    except Exception as e:
        log_error('Snowflake Runs Details Insert Failed  ', e)


def __get_pipeline_id(config: dict) -> str:
    try:
        asset_id = config.get("asset", {}).get("id")

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
            f"Snowflake Connector - Get Pipeline Primary Key Information By Asset ID Failed ", e)
        raise e

def get_run_status(status: str)-> str:
    if not status:
        status = "failed"
    elif status == "SUCCEEDED":
        status = "success"
    elif status == "FAIL":
        status = "failed"
    return status.lower()

def calculate_duration(start_time: str, end_time: str):
    duration = 0
    try:
        duration = end_time - start_time
        duration = duration.total_seconds()
    except Exception as e:
        log_error(f"duration calculation failed", e)
    finally:
        return duration
    
def __update_pipeline_properties(config: dict, asset_type: str):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                 select 
                 	pipeline.id,
	                pipeline.properties,
	 				count(distinct pipeline_runs.id) as tot_runs
                from core.pipeline
                join core.asset on asset.id = pipeline.asset_id
	 			left join core.pipeline_runs on pipeline.id = pipeline_runs.pipeline_id
                where pipeline.asset_id='{asset_id}'
                group by pipeline.id
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)

            if pipeline:
                properties = pipeline.get("properties", {})
                properties.update({"tot_runs": pipeline.get("tot_runs") if pipeline.get("tot_runs") else 0})
                if asset_type == ASSET_TYPE_TASK:
                    properties.update({"tot_tasks": 1})
                elif asset_type == ASSET_TYPE_PROCEDURE:
                    properties.update({"tot_transformations": 1})
                    properties.update({"type": "transformation"})
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
        log_error(f"Pipeline Properties", e)
    
def __update_last_run_stats(config: dict, type: str, queries: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id,status,run_end_at from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id')
                status = last_run.get('status')
                last_run_at = last_run.get('run_end_at')
                if type == ASSET_TYPE_PROCEDURE:
                    query_string = f"""
                        update core.pipeline_transformations set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                        where asset_id = '{asset_id}'
                    """
                execute_query(connection, cursor, query_string)
                pipeline_query_string = f"""
                    update core.pipeline set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                asset = config.get("asset", {})
                asset_type = asset.get("type").lower()
                asset_name = asset.get("name")
                database_name =  config.get("database_name")
                schema_name = config.get("schema")
                execute_state_pipe= None
                if asset_type == ASSET_TYPE_PIPE:
                    metadata_query = queries.get("pipeline_metadata")
                    query_string = (metadata_query.get('pipe_status').replace('<pipe_name>', asset_name)
                    .replace('<database_name>',database_name)
                    .replace('<schema_name>', schema_name))
                    source_connection = get_native_connection(config)
                    pipe_status, _ = execute_native_query(
                          config,  query_string , source_connection
                    )
                    execute_state_pipe = pipe_status.get("execution_state") if pipe_status else None
                    execute_state_pipe = execute_state_pipe.strip('"')
                    
                    if execute_state_pipe.lower() in STOPPED_OR_STALLED_STATES:
                        pipeline_query_string = f"""
                          update core.pipeline set  status='failed' where asset_id = '{asset_id}'
                        """
                        execute_query(connection, cursor, pipeline_query_string)
                     
                handle_alerts_issues_propagation(config, run_id, execute_state_pipe=execute_state_pipe)
                if asset_type == ASSET_TYPE_PIPE:
                    pipeline_query_string = f"""
                            update core.pipeline set  status='{status}' where asset_id = '{asset_id}'
                            """
                    execute_query(connection, cursor, pipeline_query_string)
                     
    except Exception as e:
        log_error(f"Snowflake Pipe Connector - Update Run Stats to Job Failed ", e)
        raise e
    
def get_existing_transformation(config: dict):
    """
    Get Exisiting Transformation Data
    """
    try:
        asset_id = config.get("asset", {}).get("id")
        transformation = None
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline_transformations
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            transformation = fetchone(cursor)
        return transformation
    except Exception as e:
        log_error(
            f"Snowflake Connector - Get Pipeline Existing Transformation ", e)
        raise e
    
def get_existing_tasks(config: dict):
    """
    Get Exisiting Tasks Data
    """
    try:
        asset_id = config.get("asset", {}).get("id")
        task = None
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline_tasks
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            task = fetchone(cursor)
        return task
    except Exception as e:
        log_error(
            f"Snowflake Connector - Get Pipeline Existing Task ", e)
        raise e
    
def extract_pipeline_measure(config: dict, queries: dict, asset_type: str):
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
    
    runs_history = get_asset_pipeline_runs(config, queries, asset_type)
    if not runs_history:
        return
    last_run = runs_history[0]
    previous_run = runs_history[1] if len(runs_history)> 1 else None
    last_run_result = {
        "duration": last_run.get("duration")
    }
    if previous_run:
        last_run_result.update({
            "last_run_date": last_run.get("start_time"),
            "previous_run_date": previous_run.get("start_time")
        })
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(config, "asset", last_run_result, job_name=pipeline_name)


def get_asset_pipeline_runs(config: dict, queries: dict, asset_type: str):
    try:
        asset = config.get("asset")
        asset_name = asset.get("name")
        database_name =  config.get("database_name")
        schema_name = config.get("schema")
        connection = config.get("connection")
        credentials = connection.get("credentials")

        # Runs Filter
        no_of_runs = credentials.get("no_of_runs", "30")
        status = credentials.get("status", "all")
        no_of_runs = int(no_of_runs) if type(no_of_runs) == str else no_of_runs
        end = datetime.datetime.today()
        start = end - datetime.timedelta(days=no_of_runs)
        start_time = start.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end.strftime("%Y-%m-%d %H:%M:%S")

        # Get Runs History
        metadata_query = queries.get("pipeline_metadata")
        if asset_type == ASSET_TYPE_TASK:
            run_filter = f"SCHEDULED_TIME BETWEEN '{start_time}' AND '{end_time}'"
            if status != "all":
                status = "SUCCEEDED" if status == "success" else "FAILED"
                run_filter = f"{run_filter} AND STATE = '{status}'"
            run_query = (metadata_query.get('task_run_history').replace('<task_name>', asset_name)
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        .replace("<run_filter>", run_filter))
        elif asset_type == ASSET_TYPE_PIPE:
            if status == "failed":
                return False
            run_filter = f"START_TIME BETWEEN '{start_time}' AND '{end_time}'"
            pipe_id = config.get("pipe_id")
            run_query = metadata_query.get('pipe_run_history').replace('<pipe_id>', pipe_id).replace("<run_filter>", run_filter)
        else:
            run_filter = f"START_TIME  BETWEEN '{start_time}' AND '{end_time}'"
            if status != "all":
                status = "SUCCESS" if status == "success" else "FAIL"
                run_filter = f"{run_filter} AND EXECUTION_STATUS = '{status}'"
            table_name = f"{database_name}.{schema_name}.{asset_name}"
            run_query = (metadata_query.get('procedure_run_history').replace('<name>', f"call {asset_name.lower()}%")
                        .replace('<query_name>', f"call {table_name.lower()}%")
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        .replace("<run_filter>", run_filter)
                        )
        source_connection = get_native_connection(config)
        runs, _ = execute_native_query(
            config, run_query, source_connection, is_list=True
        )
        runs = runs if runs else []
        return runs
    except Exception as e:
        log_error(f"Snowflake Connector - Task Runs Failed ", e)
        raise e