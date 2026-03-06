import os
import traceback
import json
from pathlib import Path
from airflow.configuration import conf
from dotenv import load_dotenv
from typing import List, Dict, Union
from dqlabs.app_helper.db_helper import (
    execute_query,
    get_connection,
    fetchone,
    fetchall,
)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_constants.dq_constants import (
    SEMANTICS,
    RELIABILITY,
    PROFILE,
    MAX_LOG_LENGTH,
    METADATA,
    MAX_RETRY_DURATION
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    update_metrics_status,
    update_detailed_error,
    create_queue_detail,
)
from dqlabs.app_helper.log_helper import get_airflow_logs, log_error, log_info
from dqlabs.app_helper.dq_helper import load_env_vars
from dqlabs.file_crypto import decrypt_env_file


def get_last_reset_timestamp_for_runs(connection, measure_id, attribute_id):
    """
    Get the most recent reset timestamp for filtering get_last_runs results.
    
    Args:
        connection: Database connection object
        measure_id (str): ID of the measure
        attribute_id (str): ID of the attribute
        
    Returns:
        str or None: Reset timestamp or None if no reset found
    """
    try:
        attribute_condition = ""
        if attribute_id:
            attribute_condition = f"AND attribute_id = '{attribute_id}'"
        with connection.cursor() as cursor:
            query = f"""
                SELECT action_history, created_date
                FROM core.metrics
                WHERE measure_id = '{measure_id}'
                {attribute_condition}
                AND action_history IS NOT NULL
                AND jsonb_typeof(action_history) = 'object'
                AND action_history !='{{}}'::jsonb
                ORDER BY created_date DESC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            results = fetchall(cursor)
            
            for row in results:
                action_history = row.get("action_history", {})
                if isinstance(action_history, str):
                    try:
                        action_history = json.loads(action_history)
                    except json.JSONDecodeError:
                        continue
                
                if action_history.get("is_changed", False):
                    reset_timestamp = action_history.get("date")
                    if reset_timestamp:
                        log_info(f"RUNS_RESET_TIMESTAMP: Found reset at {reset_timestamp} for measure {measure_id}")
                        return reset_timestamp
            
            log_info(f"RUNS_RESET_TIMESTAMP: No reset found for measure {measure_id}")
            return None
            
    except Exception as e:
        log_error(f"Error getting reset timestamp for runs for measure {measure_id}: {e}", e)
        return None


def get_last_runs(
    connection: object,
    asset_id: str,
    limit: int = 1,
    exclude_current_run: bool = False,
    current_run_id: str = "",
    limit_type: str = "runs",
    attribute_id: str = "",
    measure_id: str = "",
) -> Union[Dict, List]:
    """
    ENHANCED: Returns the last run_id(s) for the given asset based on the limit.
    Now includes reset timestamp filtering to ensure only post-reset runs are returned.
    """
    queries = []
    exculde_query = (
        queries.append(f""" run_id != '{current_run_id}' """)
        if (exclude_current_run and current_run_id)
        else ""
    )
    if measure_id:
        queries.append(f"measure_id='{measure_id}'")
    if asset_id:
        queries.append(f"asset_id='{asset_id}'")
        queries.append("is_archived=False")
    if attribute_id:
        queries.append(f"attribute_id='{attribute_id}'")

    # ENHANCED: Add reset timestamp filtering if measure_id and attribute_id are provided
    reset_timestamp = None
    if measure_id and attribute_id:
        reset_timestamp = get_last_reset_timestamp_for_runs(connection, measure_id, attribute_id)
        log_info(("reset timestamp",reset_timestamp))
        if reset_timestamp:
            queries.append(f"created_date > '{reset_timestamp}'")
            log_info(f"RUNS_FILTERING: Using reset timestamp filter: {reset_timestamp}")

    filter_condition = ""
    if queries:
        filter_condition = " and ".join(queries)
        filter_condition = f"where {filter_condition}"

    condition_query = f" order by created_date desc limit {limit} "
    if limit_type == "days":
        condition_query = (
            f" where created_date >= CURRENT_DATE - {limit} order by created_date desc"
        )
    elif limit_type == "months":
        condition_query = f" where created_date >= CURRENT_DATE - INTERVAL '{limit} months' order by created_date desc"

    with connection.cursor() as cursor:
        query_string = f"""
            select * from (
                select distinct on (run_id) run_id, created_date
                from core.metrics
                {filter_condition}
                order by run_id, created_date desc
            ) as temp_table
            {condition_query}
        """
        cursor = execute_query(connection, cursor, query_string)
        response = None
        if limit > 1 or limit_type == "days" or limit_type == "months":
            response = fetchall(cursor)
        else:
            response = fetchone(cursor)
        
        # Log filtering results
        reset_info = f" (post-reset from {reset_timestamp})" if reset_timestamp else ""
        if isinstance(response, list):
            log_info(f"RUNS_FILTERING: Retrieved {len(response)} runs{reset_info}")
        elif response:
            log_info(f"RUNS_FILTERING: Retrieved 1 run{reset_info}")
        else:
            log_info(f"RUNS_FILTERING: No runs found{reset_info}")
            
        return response


def get_previous_run_id(
    connection: object, asset_id: str, current_run_id: str
) -> Union[Dict, List]:
    """
    Returns the previous run_id for the give asset based on the limit
    """
    previous_run_id = None
    with connection.cursor() as cursor:
        query_string = f"""
            select * from (
                select distinct on (run_id) run_id, created_date
                from core.metrics
                where asset_id='{asset_id}'
                order by run_id, created_date desc
            ) as temp_table
            where run_id != '{current_run_id}'
            order by created_date desc
            limit {1}
        """
        cursor = execute_query(connection, cursor, query_string)
        previous_run = fetchone(cursor)
        if previous_run:
            previous_run_id = previous_run.get("run_id")
    return previous_run_id


def get_general_settings(config: dict) -> dict:
    """
    Returns the list of scores for each attribute
    in a given asset, for the last run.
    """
    try:
        connection = get_postgres_connection(config)
        general_settings = {}
        with connection.cursor() as cursor:
            query_string = f"""
                select * from core.settings
            """
            cursor = execute_query(connection, cursor, query_string)
            general_settings = fetchone(cursor)
            general_settings if general_settings else {}
        return general_settings
    except Exception as e:
        raise e


def get_all_organizations() -> List:
    """
    Returns list of jobs to create dags.
    """
    current_env_name = os.environ.get("AIRFLOW_ENV_NAME")
    if not current_env_name:
        current_env_name = conf.get("core", "ENV_NAME")
    default_mwaa_environment = os.environ.get("MWAA_ENV_NAME")
    default_local_environment = os.environ.get("AIRFLOW_LOCAL_ENV_NAME")

    current_env_name = current_env_name if current_env_name else ""
    is_default_airflow = (
        not current_env_name
        or (current_env_name == default_mwaa_environment)
        or (current_env_name == default_local_environment)
    )

    organizations = []
    with get_connection() as connection:
        with connection.cursor() as cursor:
            # Filter organization based on airflow config
            instance_filter = ""
            org_instance_filter = ""
            if is_default_airflow:
                instance_filter = f"(airflow_config is null or airflow_config ='{{}}' or airflow_config::jsonb ->>'environment_name' is null or airflow_config::jsonb ->>'environment_name'='' or airflow_config::jsonb ->>'environment_name'='{current_env_name}')"
                org_instance_filter = f"(org.airflow_config is null or org.airflow_config ='{{}}' or org.airflow_config::jsonb ->>'environment_name'='{current_env_name}')"
            else:
                instance_filter = f"(airflow_config::jsonb ->>'environment_name'='{current_env_name}')"
                org_instance_filter = f"(org.airflow_config::jsonb ->>'environment_name'='{current_env_name}')"
            instance_filter = f" and {instance_filter}" if instance_filter else ""
            org_instance_filter = (
                f" and {org_instance_filter}" if org_instance_filter else ""
            )

            query_string = f"""
                select organization as admin_organization, default_connection_id as core_connection_id, domain
                from core.admin where is_deployed=true {instance_filter}
            """
            cursor = execute_query(connection, cursor, query_string)
            organizations = fetchall(cursor)

            query_string = f"""
                select org.id, org.admin_organization,
                con.airflow_connection_id as core_connection_id
                from core.connection as con
                join core.organization as org on org.id=con.organization_id
                where org.is_default = True and org.admin_organization is null
                and con.is_default=True
                {org_instance_filter}
            """
            cursor = execute_query(connection, cursor, query_string)
            default_organization = fetchone(cursor)
            if default_organization:
                organizations.append(default_organization)
    return organizations


def get_organization(config: dict):
    """
    Returns the organization details
    """
    organization = {}
    try:
        admin_organization = config.get("admin_organization")
        organization_id = config.get("id")
        filter_query = f" and org.admin_organization='{admin_organization}' "
        if not admin_organization and organization_id:
            filter_query = f" and org.id ='{organization_id}' "

        connection = get_postgres_connection(config)
        with connection:
            with connection.cursor() as cursor:
                query_string = f"""
                    select org.id, org.id as organization_id, org.is_active, org.airflow_config, org.livy_spark_config,
                    org.name, settings.semantics, row_to_json(settings.*) as settings
                    from core.organization as org
                    join core.settings on settings.organization_id=org.id
                    where org.is_active=True and org.is_delete=False {filter_query}
                """
                cursor = execute_query(connection, cursor, query_string)
                organization = fetchone(cursor)
    except Exception as e:
        log_error("get_organization", e)
        organization = None
    return organization


def load_env(base_dir_path: str):
    """
    load the env variables
    """
    base_path = str(Path(__file__).parents[1])
    env_file_path = os.path.join(base_path, ".env.enc")
    if not os.path.exists(env_file_path):
        env_file_path = os.path.join(base_path, ".env")

    if not os.path.exists(env_file_path):
        # If the dqlabs module is not installed as a python module, look for the .env files in the current dir.
        env_file_path = os.path.join(base_dir_path, ".env.enc")
        if not os.path.exists(env_file_path):
            env_file_path = os.path.join(base_dir_path, ".env")

    env_data = decrypt_env_file(env_file_path, file_output=False)
    load_env_vars(env_data)


def __get_failed_task_config(context: dict):
    """
    Retruns task related configurations
    """
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    attempt = task_instance.try_number
    dag_id = context.get("dag_run").dag_id
    execution_date = context.get("dag_run").execution_date

    task_config = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "attempt": attempt,
        "execution_date": execution_date,
        "dag": dag,
        "task_instance": task_instance,
    }
    return task_config


def on_task_failed(context: dict, config: dict):
    try:
        category = config.get("dag_category")
        is_retry_enabled = config.get("enable_retry")
        is_retry_enabled = is_retry_enabled if is_retry_enabled else False
        max_retries = config.get("max_retries")
        max_retry_duration = config.get("max_retry_duration", MAX_RETRY_DURATION)
        max_retries = max_retries if max_retries else 1
        task_attempt = config.get("attempt")
        task_attempt = int(task_attempt) if task_attempt is not None else -1
        task_attempt = task_attempt + 1
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        error_message = ""
        if exception: 
            if not isinstance(exception, str):
                error_message = " ".join(
                    traceback.format_exception(
                        exception, value=exception, tb=exception.__traceback__
                    )
                ).strip()
            else:
                error_message = exception if exception else ""
            if (
                error_message
                and task_instance
                and task_instance.task_id
                and task_instance.dag_id
            ):
                error_message = f"""The task `{str(task_instance.task_id)}` failed in dag `{str(task_instance.dag_id)}` with error - {error_message}"""
        error_message = error_message.replace("'", "''")
        task_config = __get_failed_task_config(context)
        detailed_error = get_airflow_logs(task_config)

        # Don't remove this print statement
        force_update = bool(category in [RELIABILITY, PROFILE, METADATA])
        duration = 0
        if task_instance and hasattr(task_instance, 'duration'):
            duration = getattr(task_instance, "duration", 0)
        status = (
            ScheduleStatus.UpForRetry.value
            if (
                is_retry_enabled
                and task_attempt < max_retries
                and duration <= max_retry_duration
                and (
                    (
                        error_message
                        and (
                            " killed " in error_message.lower()
                            or " sigterm " in error_message.lower()
                            or " sigkill " in error_message.lower()
                            or "psycopg2.errors." in error_message.lower()
                            or "external executor id" in error_message.lower()
                            or "exception: error during request to server" in error_message.lower()
                            or "exception: list.remove(x): x not in list" in error_message.lower()
                            or "photon ran out of memory" in error_message.lower()
                            or "error - detected zombie job" in error_message.lower()
                        )
                    )
                    or (
                        detailed_error
                        and (
                            " killed " in detailed_error.lower()
                            or " sigterm " in detailed_error.lower()
                            or " sigkill " in detailed_error.lower()
                            or "psycopg2.errors." in detailed_error.lower()
                            or " return code -9" in detailed_error.lower()
                            or "exception: error during request to server" in detailed_error.lower()
                            or "exception: list.remove(x): x not in list" in detailed_error.lower()
                            or "photon ran out of memory" in detailed_error.lower()
                            or "error - detected zombie job" in detailed_error.lower()
                        )
                    )
                )
            )
            else ScheduleStatus.Failed.value
        )
        true_fail = True if status == ScheduleStatus.Failed.value else False
        update_queue_detail_status(
            config, status, error_message, true_fail=true_fail)
        update_queue_status(
            config,
            (
                ScheduleStatus.Pending.value
                if status == ScheduleStatus.UpForRetry.value
                else ScheduleStatus.Completed.value
            ),
            force=force_update,
        )
        if category not in [SEMANTICS, METADATA]:
            update_metrics_status(config)

        # update detailed error log
        detailed_error = (
            (detailed_error[:MAX_LOG_LENGTH].replace("'", "''") + "..")
            if detailed_error and len(detailed_error) > 0
            else "No logs details could be found"
        )
        update_detailed_error(config, detailed_error)
    except Exception as e:
        log_error("on task failed", e)


def get_active_channel(config: dict):
    """
    Returns the Active API Channels
    """
    try:
        channels = []
        organization_id = config.get("organization_id")
        filter_query = f" integrations.organization_id='{organization_id}' "

        connection = get_postgres_connection(config)
        with connection:
            with connection.cursor() as cursor:
                query_string = f"""
                    select 
                        integrations.is_active as is_enabled, integrations.id,
                        technical_name, type, integrations.config as configuration
                    from core.integrations
                    join core.channels on channels.id=integrations.channel_id
                    where channels.technical_name in('collibra','hitachi', 'alation','hitachi_pdc','atlan','purview', 'databricks_uc', 'coalesce', 'datadotworld') 
                    and {filter_query} and integrations.is_active=true
                """
                print("query_string in get_active_channel", query_string)
                cursor = execute_query(connection, cursor, query_string)
                channels = fetchall(cursor)
                print("channels in get_active_channel", channels)
    except Exception as e:
        log_error("get_collibra", e)
    finally:
        return channels


def get_organization_detail(id: str, config: dict):
    """
    Returns the organization details
    """
    organization = {}
    try:
        if not id:
            return organization

        connection = get_postgres_connection(config)
        with connection:
            with connection.cursor() as cursor:
                query_string = f"""
                    select org.*, row_to_json(gen.*) as settings, row_to_json(theme.*)as theme
                    from core.organization as org
                    join core.settings as gen on gen.organization_id=org.id
                    join core.theme as theme on theme.organization_id=org.id
                    where org.is_active=True and org.is_delete=False
                    and org.id='{id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                organization = fetchone(cursor)
    except Exception as e:
        log_error("get_organization_detail", e)
    return organization


def clean_pg_idle_connections(config: dict, connection):
    """
    Cleanup all the postgresql idle connections
    """
    connection = connection if connection else get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = """
            WITH inactive_connections AS (
                SELECT pid, application_name, rank() over (partition by client_addr order by backend_start ASC) as rank
                FROM pg_stat_activity
                WHERE
                -- Exclude the thread owned connection (ie no auto-kill)
                pid <> pg_backend_pid( )
                AND
                -- Include connections to the same database the thread is connected to
                datname = current_database()
                AND
                -- Exclude known applications connections
                lower(application_name) !~ '(?:psql)|(?:pgadmin.+)'
                AND
                -- Include inactive connections only
                lower(state) in ('idle', 'idle in transaction', 'idle in transaction (aborted)', 'disabled')
                AND
                -- Include old connections (found with the state_change field)
                current_timestamp - state_change > interval '5 minutes' 
            )
            SELECT pg_terminate_backend(pid) FROM inactive_connections WHERE rank > 1
        """
        execute_query(connection, cursor, query_string)


def is_scoring_enabled(config: dict, measure_allow_score: bool = True) -> bool:
    """
    Check the scoring is enabled at the platform level
    """
    is_enabled = False

    if not measure_allow_score:
        return is_enabled

    general_settings = get_general_settings(config)
    general_settings = general_settings if general_settings else {}
    score_settings = general_settings.get(
        "scoring") if general_settings else {}
    score_settings = score_settings if score_settings else {}
    score_settings = (
        json.loads(score_settings, default=str)
        if score_settings and isinstance(score_settings, str)
        else score_settings
    )
    score_settings = score_settings if score_settings else {}
    is_enabled = score_settings.get("is_active", False)
    if not is_enabled:
        log_info(
            "Please enable score under settings -> platfrom -> configuration -> score to enable scoring for the measures...!"
        )

    return is_enabled