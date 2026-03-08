from uuid import uuid4
import json
import copy
from datetime import datetime, timedelta, timezone

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection, execute_native_query
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.schedule import create_request_queue
from dqlabs.app_constants.dq_constants import RELIABILITY
from dqlabs.enums.schedule_types import ScheduleStatus

# Import Enums
from dqlabs.enums.connection_types import ConnectionType

def pipeline_auto_tag_mapping(config: dict, tags: list, params: dict):
    source = params.get("source")
    level = params.get("level")
    id = params.get("id")
    tag_list = insert_pipeline_tags(config, tags, source)
    asset_id = params.get("asset_id")
    conditions = [f"asset_id ='{asset_id}'"]
    connection_type = config.get("connection", {}).get("type")

    if level == "test":
        conditions.append(f"test_id='{id}' and level='test'")
        test_id = id
        task_id = None
        run_id = None
    elif level == "task":
        test_id = None
        task_id = id
        run_id = None
        conditions.append(f"task_id='{id}' and level='task'")
    elif level == "run":
        test_id = None
        task_id = None
        run_id = id
        conditions.append(f"run_id='{id}' and level='run'")
    elif level == "transformation":
        conditions.append(
            f"transformation_id='{id}' and level='transformation'")
    else:
        test_id = None
        task_id = None
        run_id = None
        conditions.append("level='asset'")

    conditions = " and ".join(conditions)

    if tag_list:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            exisiting_tag_mapping_query = f"select tags_id from core.tags_mapping where {conditions}"

            cursor = execute_query(
                connection, cursor, exisiting_tag_mapping_query)
            existing_tags = [tag.get("tags_id") for tag in fetchall(cursor)]
            existing_tags = existing_tags if existing_tags else []
            
            # Handle tag unmapping for Airbyte connections
            if connection_type == "airbyte":
                __handle_airbyte_tag_unmapping(config, asset_id, level, id, tag_list, existing_tags, conditions)
            
            tag_input = []
            for tag in tag_list:
                if tag not in existing_tags:
                    query_input = (
                        str(uuid4()),
                        level,
                        str(tag),
                        asset_id,
                        test_id,
                        task_id,
                        run_id
                    )
                    input_literals = ", ".join(
                        ["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                    ).decode("utf-8")
                    tag_input.append(query_param)
            tag_input = split_queries(tag_input)
            for input_values in tag_input:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                            insert into core.tags_mapping(
                                id, level, tags_id, asset_id, test_id, task_id, run_id, created_date
                            ) values {query_input}
                        """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error(
                        'Piepline Tags Map to Insert Failed  ', e)


def insert_pipeline_tags(config: dict, tags: list, source: str):
    organization_id = config.get("organization_id")
    connection = get_postgres_connection(config)
    new_tags = []
    tag_mapping_list = []
    with connection.cursor() as cursor:
        if tags:
            tag_names = f"""({','.join(f"'{i.lower()}'" for i in tags)})"""
            exisiting_tag_query = f"select id, name from core.tags where lower(name) in {tag_names} and source='{source}'"
            cursor = execute_query(connection, cursor, exisiting_tag_query)
            existing_tags = fetchall(cursor)
            existing_tag_names = [tag.get("name") for tag in existing_tags]
            tag_mapping_list = [tag.get("id") for tag in existing_tags]
            new_tags = [tag for tag in tags if tag not in existing_tag_names]
            if new_tags:
                tags_input = []
                for tag in new_tags:
                    new_tag_id = str(uuid4())
                    query_input = (
                        new_tag_id,
                        tag,
                        tag,
                        str(False),
                        "",
                        str(organization_id),
                        '#64AAEF',
                        source,
                        str(True),
                        str(False),
                        None,
                        json.dumps(
                            {"type": source}, default=str),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                    ).decode("utf-8")
                    tags_input.append(query_param)
                    tag_mapping_list.append(new_tag_id)

                tags_input = split_queries(tags_input)
                for input_values in tags_input:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.tags(
                                id, name, technical_name, is_mask_data,description,organization_id, color, source, is_active, is_delete, parent_id, properties, created_date
                            ) values {query_input}
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    except Exception as e:
                        log_error(f'{type} Tags Insert Failed  ', e)
    return tag_mapping_list


def get_pipeline_last_run_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = config.get("connection", {})
        connection_type = connection.get("type", "")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, source_id from core.pipeline_runs
                where asset_id = '{asset_id}'
                order by source_id desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            runs = fetchone(cursor)
            return runs.get('source_id') if runs and runs.get('source_id') else None
    except Exception as e:
        log_error(
            f"{connection_type} Connector - Get Last Run Id By Asset ID Failed ", e)
        raise e


def get_pipeline_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = config.get("connection", {})
        connection_type = connection.get("type", "")

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
            f"{connection_type} Connector - Get Pipeline Primary Key Information By Asset ID Failed ", e)
        raise e


def automatic_associcated_asset_profiling(config: dict, asset_id: str):
    """
    AutoMatic Associated Pipeline Asset Profiling
    """
    try:

        # Connection
        connection = config.get("connection", {})
        credentials = connection.get("credentials")
        automatic_profiling = credentials.get("automatic_profiling", False)

        # Automatic Profiling
        if automatic_profiling:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                   select distinct asset.id
                   from core.associated_asset
                   join core.asset on asset.id = associated_asset.associated_asset_id
                   where associated_asset.source_asset_id = '{asset_id}' 
                   and asset.run_status not in ('{ScheduleStatus.Pending.value}', '{ScheduleStatus.Running.value}')
                   and asset.is_active = true and asset.is_delete=false
                """
                cursor = execute_query(connection, cursor, query_string)
                assets = fetchall(cursor)
                if assets:
                    for asset in assets:
                        create_request_queue(level="asset", asset_id=asset.get(
                            "id"), job_type=RELIABILITY, config=config)

    except Exception as e:
        log_error(f"Associated Asset Profiling Failed", e)


def update_pipeline_last_runs(config: dict):
    """
    Update the last runs for the asset based on run history
    """
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
        WITH run_history AS (
            select run_history_id, jsonb_agg(jsonb_build_object('last_run_date', run_date, 'status', status)) as last_runs
                    from (
                        select 
                            pipeline_id as run_history_id,
                            run_end_at as run_date, 
                            status,
                            row_number() OVER (PARTITION BY pipeline_id ORDER BY run_end_at desc) as rn
                        from core.pipeline_runs 
                        order by run_end_at desc
                    ) as temp
                    where rn <= 7
                    group by run_history_id
        )
        UPDATE core.asset 
        SET last_runs = rh.last_runs
        FROM run_history rh
        JOIN core.pipeline_runs t ON rh.run_history_id = t.pipeline_id
        JOIN core.asset a ON t.asset_id = a.id
        WHERE t.asset_id = '{asset_id}' AND core.asset.id = t.asset_id
        """
        try:
            cursor = execute_query(connection, cursor, query_string)
        except Exception as e:
            log_error("update last runs for asset", e)
            connection.rollback()
            raise e
        
def get_pipeline_tests(config: dict, asset_id: str, run_id: str, configured_status: str) -> list:
    """
    Get pipeline tests
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            SELECT jsonb_object_agg(
                task_id,
                tests
            ) AS task_tests_map
            FROM (
                SELECT 
                    t.id as task_id,
                    jsonb_agg(DISTINCT jsonb_build_object(
                        'test_name', p.name,
                        'test_status', p.status,
                        'test_error', p.error,
                        'test_compiled_code', p.compiled_code
                )) AS tests
                FROM core.pipeline_tests p
                JOIN LATERAL (
                    SELECT jsonb_array_elements_text(p.depends_on) AS task_source_id
                ) d ON TRUE
                join core.pipeline_tasks t on d.task_source_id=t.source_id
                WHERE p.asset_id='{asset_id}' 
                AND p.run_id='{run_id}'
                AND lower(p.status) in {configured_status}
                GROUP BY t.id
            ) sub
        """
        cursor = execute_query(connection, cursor, query_string)
        tests = fetchone(cursor)
        tests = tests.get("task_tests_map") if tests else {}
        return tests
    
def get_pipeline_job_input(config: dict, run_id: str):
    connection = get_postgres_connection(config)
    job_input = {}
    with connection.cursor() as cursor:
        query_string = f"""
            select job_input from core.request_queue
            where id = '{run_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        job_input = fetchone(cursor)
        job_input = job_input.get("job_input") if job_input else {}
        if isinstance(job_input, str):
            job_input = json.loads(job_input)
    return job_input


def get_run_history(config: dict, asset_id: str):
    connection = get_postgres_connection(config)
    pipeline_runs = []
    with connection.cursor() as cursor:
        query_string = f"""
            select run_start_at as started_at, duration, id from core.pipeline_runs
            where asset_id='{asset_id}' 
            order by run_start_at desc limit 2
        """
        cursor = execute_query(connection, cursor, query_string)
        pipeline_runs = fetchall(cursor)
        pipeline_runs = pipeline_runs if pipeline_runs else []
    return pipeline_runs

def get_task_previous_run_history(config: dict, run_id: str):
    connection = get_postgres_connection(config)
    pipeline_runs = []
    with connection.cursor() as cursor:
        query_string = f"""
            select run_start_at as started_at, source_id from core.pipeline_runs_detail
            where pipeline_run_id='{run_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        pipeline_runs = fetchall(cursor)
        pipeline_runs = pipeline_runs if pipeline_runs else []
    return pipeline_runs

def update_pipeline_run_detail_telemetry(config: dict, associated_asset_id: str, task_source_id: str, is_sink: bool):
    """
    Update the pipeline run detail telemetry
    """
    pipeline_asset = config.get("asset", {})
    pipeline_asset_id = pipeline_asset.get("id")
    pipeline_connection_type = config.get("connection", {}).get("type")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        # get pipeline runs
        pipeline_runs = get_run_history(config, pipeline_asset_id)
        pipeline_runs = pipeline_runs[:1] if pipeline_runs else []
        if not pipeline_runs:
            return
        # get associated asset result
        query_string = f"""
            select asset.id, asset.name, asset.properties as asset_properties, connection.id as connection_id, connection.name as connection_name, 
            connection.type as connection_type, connection.properties, connection.credentials   
            from core.asset 
            join core.connection on connection.id = asset.connection_id
            where asset.id='{associated_asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        asset_result = fetchone(cursor)
        if asset_result.get("connection_type") in ["snowflake", "databricks", "synapse","bigquery"] and (is_sink or pipeline_connection_type == ConnectionType.Coalesce.value.lower()):
            associated_asset_config = prepare_associated_asset_config(config, asset_result)
            asset_properties = asset_result.get("asset_properties")
            asset_name = asset_result.get("name")
            database_name = asset_properties.get("database")
            schema_name = asset_properties.get("schema")
            pipeline_run = pipeline_runs[0]

            run_started = pipeline_run.get("started_at")
            duration_sec = pipeline_run.get("duration", 0.0)
            run_ended = run_started + timedelta(seconds=float(duration_sec))
            run_started = run_started.astimezone(timezone.utc)
            run_ended = run_ended.astimezone(timezone.utc)
            if asset_result.get("connection_type") == "snowflake":
                query_string = f"""
                    SELECT 
                        QUERY_ID,
                        QUERY_TEXT,
                        QUERY_TYPE,
                        ROWS_INSERTED,
                        ROWS_UPDATED,
                        ROWS_DELETED,
                        ROWS_INSERTED + ROWS_UPDATED + ROWS_DELETED AS VOLUMES,
                        CONVERT_TIMEZONE('UTC', START_TIME) AS START_TIME,
                        CONVERT_TIMEZONE('UTC', END_TIME)   AS END_TIME,
                        CASE 
                            WHEN ERROR_CODE IS NULL THEN 'PASS'
                            ELSE 'FAIL' 
                        END AS STATUS,
                        ROUND(DATEDIFF('millisecond', START_TIME, END_TIME) / 1000, 3) AS RUNTIME
                    FROM 
                        snowflake.account_usage.query_history
                    WHERE 
                        START_TIME BETWEEN '{run_started}' AND '{run_ended}'
                        AND (
                            (
                                UPPER(DATABASE_NAME) = '{database_name}'
                                AND UPPER(SCHEMA_NAME) = '{schema_name}'
                                AND CONTAINS(QUERY_TEXT, '{asset_name}')
                            )
                            OR CONTAINS(UPPER(QUERY_TEXT), UPPER('{database_name}.{schema_name}.{asset_name}'))
                            OR CONTAINS(UPPER(QUERY_TEXT), UPPER('{database_name}.{schema_name}."{asset_name}"'))
                        )
                        AND UPPER(QUERY_TYPE) IN (
                            'INSERT', 
                            'UPDATE', 
                            'DELETE'
                        )
                    ORDER BY 
                        END_TIME DESC;

                """
            elif asset_result.get("connection_type") == "databricks":
                query_string = f"""
                WITH delta_log AS (
                    SELECT
                        version,
                        timestamp AS commit_time,
                        operation,
                        operationParameters,
                        operationMetrics
                    FROM (
                        DESCRIBE HISTORY {database_name}.{schema_name}.{asset_name}
                    )
                    WHERE operation IN ('UPDATE','DELETE','WRITE')
                    AND timestamp between '{run_started}' and '{run_ended}'
                )
                SELECT
                delta_log.commit_time AS START_TIME,
                TRY_CAST(delta_log.operationMetrics.numOutputRows AS INT)   AS rows_affected,
                TRY_CAST(delta_log.operationMetrics.numOutputRows AS INT) AS rows_inserted,
                TRY_CAST(delta_log.operationMetrics.numUpdatedRows AS INT)  AS rows_updated,
                TRY_CAST(delta_log.operationMetrics.numDeletedRows AS INT)  AS rows_deleted

                from delta_log
                """
            elif asset_result.get("connection_type") == "synapse":
                query_string = f"""
                select request_id, operation_type, start_time AT TIME ZONE 'UTC' AS START_TIME, end_time AT TIME ZONE 'UTC' AS END_TIME,
                CASE WHEN command LIKE '%INSERT INTO%' THEN row_count ELSE 0 END AS ROWS_INSERTED,
                CASE WHEN command LIKE '%UpdateCTE%' THEN row_count ELSE 0 END AS ROWS_UPDATED,
                CASE WHEN command LIKE '%DeleteCTE%' THEN row_count ELSE 0 END AS ROWS_DELETED,
                command from sys.dm_pdw_request_steps where
                (command LIKE '%INSERT INTO%' OR command LIKE '%UpdateCTE%' OR command LIKE '%DeleteCTE%' )
                and start_time AT TIME ZONE 'UTC' between '{run_started}' and '{run_ended}'
                and operation_type = 'OnOperation' and status = 'Complete'
                and CHARINDEX('[{database_name}].[{schema_name}].[{asset_name}]', command) > 0
                ORDER BY START_TIME DESC
                """
            elif asset_result.get("connection_type") == "bigquery":
                location = asset_result.get("credentials", {}).get("location")
                query_string = f"""
                SELECT 
                    query AS QUERY_TEXT,
                    statement_type AS QUERY_TYPE,
                    start_time AS START_TIME,
                    end_time AS END_TIME,
                    user_email AS USER_NAME,
                    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND)/1000 AS RUNTIME,
                    dml_statistics.inserted_row_count AS ROWS_INSERTED,
                    dml_statistics.updated_row_count AS ROWS_UPDATED,
                    dml_statistics.deleted_row_count AS ROWS_DELETED,
                    total_bytes_processed AS BYTES_PROCESSED
                FROM `region-{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
                WHERE job_type = 'QUERY'
                AND state = 'DONE'
                AND error_result IS NULL                       
                AND creation_time between '{run_started}' and '{run_ended}'
                AND statement_type in ('INSERT', 'UPDATE', 'DELETE')
                AND (
                        LOWER(query) LIKE '%{(database_name).lower()}%'
                        AND LOWER(query) LIKE '%{(schema_name).lower()}%'
                        AND LOWER(query) LIKE '%{(asset_name).lower()}%' 
                    )
                ORDER BY creation_time DESC;
                """
            try:
                data, _ = execute_native_query(associated_asset_config, query_string, is_list=True)
            except Exception as e:
                log_error("Get Associated Asset Telemetry Failed", e)
                data = []
            finally:
                data = data if data else []
            for run in pipeline_runs:
                run_started = run.get("started_at")
                # run_duration = run.get("duration")
                run_id = run.get("id")
                insert, update, delete, volumes = 0, 0, 0, 0
                for row in data:
                    row_start_time = row.get("start_time")
                    if row_start_time and run_started:
                        # Convert to datetime objects for comparison
                        if isinstance(row_start_time, str):
                            row_start_time = datetime.fromisoformat(row_start_time.replace('Z', '+00:00'))
                        if isinstance(run_started, str):
                            run_started = datetime.fromisoformat(run_started.replace('Z', '+00:00'))
                        
                        if row_start_time >= run_started:
                            insert += int(row.get("rows_inserted")) if isinstance(row.get("rows_inserted"), int) else 0
                            update += int(row.get("rows_updated")) if isinstance(row.get("rows_updated"), int) else 0
                            delete += int(row.get("rows_deleted")) if isinstance(row.get("rows_deleted"), int) else 0
                            # volumes += row.get("volumes") if isinstance(row.get("volumes"), int) else 0
                task_source_condition = f" and source_id = '{task_source_id}'"
                task_source_condition = task_source_condition if is_sink else ""
                volumes = insert + update + delete
                query_string = f"""
                UPDATE core.pipeline_runs_detail
                SET rows_inserted = '{insert}', rows_updated = '{update}', rows_deleted = '{delete}', volumes = '{volumes}'
                WHERE pipeline_run_id = '{run_id}' and asset_id = '{pipeline_asset_id}' {task_source_condition}
                """
                cursor = execute_query(connection, cursor, query_string)


def prepare_associated_asset_config(config: dict, asset_result: dict):
    associated_asset_config = {}
    updated_config = config.copy()
    associated_asset_config.update({
        "connection_type": asset_result.get("connection_type"),
        "asset": {
            "id": asset_result.get("id"),
            "name": asset_result.get("name"),
            "properties": asset_result.get("asset_properties")
        },
        "connection": {
            "id": asset_result.get("connection_id"),
            "name": asset_result.get("connection_name"),
            "type": asset_result.get("connection_type"),
            "properties": asset_result.get("properties"),
            "credentials": asset_result.get("credentials")
        },
        "properties": asset_result.get("properties"),
        "credentials": asset_result.get("credentials")
    })
    updated_config.update(associated_asset_config)
    return updated_config


def __handle_airbyte_tag_unmapping(config: dict, asset_id: str, level: str, id: str, current_tags: list, existing_tags: list, conditions: str):
    """
    Handle tag unmapping for Airbyte connections when tags are removed from source
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Get all current tag IDs for this asset/level from tags_mapping table
            current_tag_ids = []
            if current_tags:
                # Get tag IDs from core.tags table for current tags
                tag_names = f"""({','.join(f"'{i.lower()}'" for i in current_tags)})"""
                current_tags_query = f"select tags_id from core.tags_mapping where asset_id = '{asset_id}' and level = '{level}' and tags_id in {tag_names}"
                cursor = execute_query(connection, cursor, current_tags_query)
                current_tag_results = fetchall(cursor)
                current_tag_ids = [tag.get("tags_id") for tag in current_tag_results]
            
            # Find tags that exist in tags_mapping but are not in current tags (need to be unmapped)
            tags_to_unmap = [tag for tag in existing_tags if tag not in current_tag_ids]
            
            if tags_to_unmap:
                # Delete unmapped tags from tags_mapping table
                for tag_id in tags_to_unmap:
                    delete_query = f"""
                        DELETE FROM core.tags_mapping 
                        WHERE {conditions} AND tags_id = '{tag_id}'
                    """
                    cursor = execute_query(connection, cursor, delete_query)
                
    except Exception as e:
        log_error("Airbyte - Tag Unmapping Failed", e)
    