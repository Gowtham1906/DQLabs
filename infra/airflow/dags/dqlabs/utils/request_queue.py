import json
import pytz
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from dateutil.parser import parse
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.dag_helper import get_postgres_connection, get_license_config
from dqlabs.utils.schedule import create_request_queue
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.enums.trigger_types import TriggerType
from dqlabs.enums.schedule_list import ScheduleTypes
from dqlabs.app_constants.dq_constants import (
    RELIABILITY,
    HEALTH,
    PROFILE,
    DISTRIBUTION,
    STATISTICS,
    FREQUENCY,
    CUSTOM,
    BEHAVIORAL,
    FAILED,
    SEMANTIC_MEASURE,
    SEMANTICS,
    EXPORT_FAILED_ROWS,
    CATALOG_UPDATE,
    NOTIFICATION,
    USERACTIVITY,
    METADATA,
    CATALOG_SCHEDULE,
    SYNCASSET,
    USAGE_QUERY,
    ASSET_GROUP_PIPELINE,
    OBSERVE,
    PROCESS
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.enums.approval_status import ApprovalStatus
from dqlabs.app_helper.agent_helper import clear_connection
from dqlabs.app_helper.pipeline_helper import automatic_associcated_asset_profiling
from dqlabs.utils.event_capture import capture_job_execution
import traceback

from dqlabs.app_helper.dq_helper import log_error, log_info


def create_queue_detail(config: dict):
    pending_tasks = get_pending_tasks(config)
    log_info(f"create_queue_detail: pending_tasks is {pending_tasks}")
    if not pending_tasks:
        return

    categories = []
    current_dag = config.get("dag_category")
    general_settings = config.get("settings", {})
    if not general_settings:
        dag_info = config.get("dag_info", {})
        dag_info = dag_info if dag_info else {}
        general_settings = dag_info.get("settings", {})
    general_settings = general_settings if general_settings else {}
    general_settings = (
        json.loads(general_settings, default=str)
        if isinstance(general_settings, str)
        else general_settings
    )

    profile_settings = general_settings.get("profile")
    profile_settings = (
        json.loads(profile_settings, default=str)
        if profile_settings and isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    is_profiling_enabled = profile_settings.get("is_active")

    if current_dag == RELIABILITY and PROFILE in pending_tasks and is_profiling_enabled:
        categories = [PROFILE]
    else:
        tasks_to_remove = [RELIABILITY, PROFILE, DISTRIBUTION, FREQUENCY]
        if not is_profiling_enabled:
            tasks_to_remove.append(STATISTICS)
        for category in tasks_to_remove:
            if category in pending_tasks:
                pending_tasks.remove(category)
                log_info(f"create_queue_detail: pending_tasks after removing {category} is {pending_tasks}")
                log_error(f"Removed category {category} from pending tasks", None)
        for task in pending_tasks:
            categories.append(task)

    queue_id = config.get("queue_id")
    organization_id = config.get("organization_id")
    attribute_id = None
    measure_id = None
    connection = get_postgres_connection(config)
    existing_tasks = get_existing_tasks(config, connection)
    with connection.cursor() as cursor:
        jobs_to_create = []
        jobs_to_update = []
        for category in categories:
            if not is_profiling_enabled and category in [
                PROFILE,
                DISTRIBUTION,
                FREQUENCY,
                STATISTICS,
            ]:
                continue
            dag_id = f"{str(organization_id)}_{category}"
            if category in existing_tasks:
                log_info(f"create_queue_detail: category {category} already exists, adding to update list")
                jobs_to_update.append(category)
                continue

            grouped_tasks, is_grouped_category = get_tasks_by_group(config, category)
            log_info(f"create_queue_detail: grouped_tasks for {category}: {grouped_tasks}, is_grouped: {is_grouped_category}")
            if is_grouped_category and grouped_tasks:
                index = 0
                for task in grouped_tasks:
                    index = index + 1
                    attribute_id = task.get("attribute_id")
                    attribute_id = attribute_id if attribute_id else None
                    measure_id = task.get("measure_id")
                    measure_id = measure_id if measure_id else None
                    query_input = (
                        str(uuid4()),
                        category,
                        dag_id,
                        ScheduleStatus.Pending.value,
                        queue_id,
                        attribute_id,
                        measure_id,
                        False,
                        (
                            datetime.now(timezone.utc) + timedelta(seconds=index)
                        ).strftime("%Y-%m-%d %H:%M:%S.%s+00"),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})",
                        query_input,
                    ).decode("utf-8")
                    jobs_to_create.append(query_param)
            elif not is_grouped_category:
                attribute_id = None
                measure_id = None
                query_input = (
                    str(uuid4()),
                    category,
                    dag_id,
                    ScheduleStatus.Pending.value,
                    queue_id,
                    attribute_id,
                    measure_id,
                    False,
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%s+00"),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})",
                    query_input,
                ).decode("utf-8")
                jobs_to_create.append(query_param)
            else:
                continue

        if jobs_to_create:
            query_input = ",".join(jobs_to_create)
            query_string = f"""
                insert into core.request_queue_detail (
                    id, category, dag_id, status, queue_id, attribute_id, measure_id,
                    is_submitted, created_date
                ) values {query_input}
            """
            cursor = execute_query(connection, cursor, query_string)

        if jobs_to_update:
            job_categories = [f"""'{str(job)}'""" for job in jobs_to_update if job]
            job_category = ", ".join(job_categories)
            job_category = f"({job_category})" if job_category else ""
            query_string = f"""
                update core.request_queue_detail set status='{ScheduleStatus.Pending.value}'
                where queue_id='{queue_id}' and category in {job_category}
            """
            cursor = execute_query(connection, cursor, query_string)


def update_queue_detail_status(
    config: dict,
    status: str,
    error: str = "",
    task_config: dict = {},
    true_fail: bool = False,
):
    connection = get_postgres_connection(config)
    status_list = [ScheduleStatus.Completed.value, ScheduleStatus.Killed.value]
    status_to_check = [f"""'{str(status_value)}'""" for status_value in status_list]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"

    queue_detail_id = config.get("queue_detail_id")
    # Fetch previous status for logging
    previous_status = None
    try:
        with connection.cursor() as cursor:
            query_prev = f"""
                select status from core.request_queue_detail where id='{queue_detail_id}'
            """
            cursor = execute_query(connection, cursor, query_prev)
            result = fetchone(cursor)
            previous_status = result.get("status") if result else None
    except Exception as e:
        log_error(f"Could not fetch previous status for {queue_detail_id}: {e}", e)

    
    fields_to_update = []
    if status == ScheduleStatus.Running.value:
        attempt = config.get("attempt")
        attempt = int(attempt) if attempt is not None else -1
        attempt = attempt + 1
        fields_to_update.append("start_time=CURRENT_TIMESTAMP")
        fields_to_update.append("end_time=null")
        fields_to_update.append("error_message=null")
        fields_to_update.append("error_log=null")
        fields_to_update.append(f"attempt={attempt}")
    elif status in [
        ScheduleStatus.Completed.value,
        ScheduleStatus.Failed.value,
        ScheduleStatus.UpForRetry.value,
    ]:
        clear_connection(config, connection)
        fields_to_update.append(
            "start_time=(CASE WHEN start_time is not null then start_time else CURRENT_TIMESTAMP end)"
        )
        fields_to_update.append(
            """
            end_time=(
                CASE
                    WHEN end_time is not null THEN end_time
                    WHEN end_time is null and start_time is not null and start_time < CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                    ELSE (CURRENT_TIMESTAMP + INTERVAL '1 second')
                END
            )
        """
        )
        if status == ScheduleStatus.UpForRetry.value:
            fields_to_update.append(
                """
                try_number=(
                    CASE
                        WHEN try_number is not null THEN try_number + 1 ELSE 1
                    END
                )
            """
            )

    if error:
        error = str(error).replace("'", "''")
        fields_to_update.append(f"error_message='{error}'")
    if task_config:
        dag_id = task_config.get("dag_id")
        if dag_id:
            fields_to_update.append(f"dag_id='{dag_id}'")
        task_id = task_config.get("task_id")
        if task_id:
            fields_to_update.append(f"task_id='{task_id}'")
        airflow_task_config = json.dumps(task_config, default=str)
        fields_to_update.append(f"task_config='{airflow_task_config}'")

    attributes = ", ".join(fields_to_update)
    attributes = f", {attributes}" if attributes else ""
    with connection.cursor() as cursor:
        query_string = f"""
            update core.request_queue_detail
            set status='{status}', modified_date=CURRENT_TIMESTAMP{attributes}
            where id='{queue_detail_id}' and status not in {status_to_check}
        """
        cursor = execute_query(connection, cursor, query_string)
        if status in [ScheduleStatus.Completed.value, ScheduleStatus.Failed.value]:
            update_task_submitted_status(config, queue_detail_id, False)

    if config.get("asset_id") or config.get("measure_id"):
        if status not in [ScheduleStatus.Failed.value] or (
            status == ScheduleStatus.Failed.value and true_fail
        ):
            capture_job_execution(config, status)


def update_task_submitted_status(
    config: dict, queue_detail_id: str, is_submitted: bool
):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.request_queue_detail set is_submitted={is_submitted}
            where id='{queue_detail_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def update_queue_detail_query(config: dict, query: str = ""):
    query = query.replace("'", "''")
    queue_detail_id = config.get("queue_detail_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.request_queue_detail set query='{query}'
            where id='{queue_detail_id}'
        """
        execute_query(connection, cursor, query_string)


def update_detailed_error(config: dict, error: str = ""):
    """
    Update detailed error log for the failed task instance
    """
    queue_detail_id = config.get("queue_detail_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.request_queue_detail
            set error_log='{error}' where id='{queue_detail_id}'
        """
        execute_query(connection, cursor, query_string)


def update_queue_status(config: dict, status: str, force: bool = False):
    category = config.get("dag_category")
    module_name = config.get("module_name", "")

    status_list = [
        ScheduleStatus.Completed.value,
        ScheduleStatus.Failed.value,
        ScheduleStatus.Killed.value,
    ]

    if not force and (
        category
        not in [
            SEMANTICS,
            NOTIFICATION,
            METADATA,
            CATALOG_SCHEDULE,
            USAGE_QUERY,
            OBSERVE,
            PROCESS
        ]
        and (module_name != "report" and module_name != "dashboard")
    ):
        pending_tasks = get_pending_tasks(config)
        if pending_tasks:
            return

    if not force and (module_name != "report" and module_name != "dashboard"):
        is_created = create_asset_level_tasks(config, status)
        if is_created:
            return

    queue_id = config.get("queue_id")
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    measure_id = config.get("measure_id")
    level = config.get("level")
    notification_id = config.get("notification_id", "")
    asset = config.get("asset", {})
    asset_group = asset.get("group", "data")

    # Automatic Associated Profiling
    if status == ScheduleStatus.Completed.value and asset_group == ASSET_GROUP_PIPELINE:
        automatic_associcated_asset_profiling(config, asset_id)

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if status == ScheduleStatus.Completed.value:
            query_string = f"""
                select count(*) as tasks_queue from core.request_queue_detail 
                where queue_id='{queue_id}' and status in ('Pending', 'Running', 'UpForRetry')
            """
            cursor = execute_query(connection, cursor, query_string)
            tasks_queue = fetchone(cursor)
            tasks_queue = tasks_queue if tasks_queue else {}
            tasks_queue_count = tasks_queue.get("tasks_queue")
            tasks_queue_count = tasks_queue_count if tasks_queue_count else 0
            if tasks_queue_count > 0:
                return

        query_string = f"""
            update core.request_queue set start_time=CURRENT_TIMESTAMP
            where id='{queue_id}' and start_time is null
        """
        cursor = execute_query(connection, cursor, query_string)

        end_time_query = ""
        if status in status_list:
            end_time_query = f", end_time=CURRENT_TIMESTAMP"

        status_to_check = [f"""'{str(status_value)}'""" for status_value in status_list]
        status_to_check = ", ".join(status_to_check)
        status_to_check = f"({status_to_check})"

        query_string = f"""
            update core.request_queue set status='{status}', modified_date=CURRENT_TIMESTAMP{end_time_query}
            where id='{queue_id}' and status not in {status_to_check}
        """
        cursor = execute_query(connection, cursor, query_string)

        run_status = status
        if status in status_list:
            run_status = ScheduleStatus.Completed.value
        if asset_id:
            query_string = f"""
                update core.asset set run_status='{run_status}' where id='{asset_id}'
            """
        if category == SEMANTICS:
            asset_id = config.get("asset_id")
            if asset_id:
                query_string = f"""
                    update core.data set semantic_run_status='{run_status}' where asset_id='{asset_id}'
                """
        cursor = execute_query(connection, cursor, query_string)

        if category != SEMANTICS and asset_id:
            query_string = f"""
                update core.asset set modified_date=CURRENT_TIMESTAMP 
                where id = '{asset_id}' and is_active=true and is_delete = false
            """
            cursor = execute_query(connection, cursor, query_string)
        if measure_id and not asset_id:
            if level == "measure" and run_status == ScheduleStatus.Completed.value:
                trigger_event_job(config)

            query_string = f"""
                    update core.measure set run_status='{run_status}' where id='{measure_id}'
                """
            cursor = execute_query(connection, cursor, query_string)

        if level == "attribute" and asset_id and attribute_id:
            query_string = f"""
                update core.attribute set run_status='{run_status}' where id = '{attribute_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

        if category == "notification" and notification_id:
            if module_name == "report" or module_name == "dashboard":
                query_string = f"""
                    update core.widget set run_status='{run_status}' where id = '{notification_id}'
                """
                if module_name == "dashboard":
                    query_string = f"""
                    update core.dashboard set run_status='{run_status}' where id = '{notification_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

        if category == "catalog":
            catalog = config.get("dag_info", {}).get("catalog", [])
            integration_id = catalog[0].get("id") if catalog else None
            if integration_id:
                query_string = f""" update core.integrations set run_status='{run_status}' where id = '{integration_id}' """
                cursor = execute_query(connection, cursor, query_string)

        if category == PROCESS:
            procoss_id = config.get("process_id")
            query_string = f"""
                update core.process set run_status='{run_status}' where id = '{procoss_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

    if status in status_list:
        update_job_end_time(config)


def create_asset_level_tasks(config, status):
    """
    Create tasks at asset level after completing all the measure related tasks
    """
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    current_data = fetch_asset_current_data(connection, asset_id)
    row_count = current_data.get("row_count", 0)
    row_count = int(row_count) if row_count else None
    level = config.get("level")
    schedule_level = config.get("level")
    is_triggered = config.get("is_triggered")
    asset_id = config.get("asset_id", None)
    measure = config.get("measure")
    measure = measure if measure else {}
    measure_status = measure.get("status")
    measure_status = str(measure_status).lower() if measure_status else ""
    has_export_failed_rows_task = False
    has_catalog_update_task = False
    has_usage_query_update_task = False
    is_catalog_task_created = True
    has_row_count_zero = (row_count == 0)

    general_settings = config.get("settings", {})
    if not general_settings:
        dag_info = config.get("dag_info", {})
        dag_info = dag_info if dag_info else {}
        general_settings = dag_info.get("settings", {})
    general_settings = general_settings if general_settings else {}
    general_settings = (
        json.loads(general_settings, default=str)
        if isinstance(general_settings, str)
        else general_settings
    )

    report_settings = general_settings.get("reporting")
    report_settings = report_settings if report_settings else {}
    report_settings = (
        json.loads(report_settings, default=str)
        if isinstance(report_settings, str)
        else report_settings
    )

    discover_settings = general_settings.get("discover")
    discover_settings = discover_settings if discover_settings else {}
    discover_settings = (
        json.loads(discover_settings, default=str)
        if isinstance(discover_settings, str)
        else discover_settings
    )
    discover_settings = discover_settings if discover_settings else {}
    is_usage_enabled = discover_settings.get("usage", {}).get("is_active", True)

    if status == ScheduleStatus.Completed.value:
        connection_type = config.get("connection_type")
        asset_type = config.get("type")
        asset_type = asset_type.lower() if asset_type else ""

        include_catalog_task = connection_type not in [
            ConnectionType.Tableau.value,
            ConnectionType.Dbt.value,
            ConnectionType.Airflow.value,
            ConnectionType.Fivetran.value,
            ConnectionType.Talend.value,
            ConnectionType.ADF.value,
            ConnectionType.PowerBI.value,
            ConnectionType.Wherescape.value,
        ] and asset_type not in ["pipeline", "task", "pipe", "stored procedure"]
        current_dag = config.get("dag_category")
        pending_tasks_count = get_pending_tasks_count(config)

        # Check conditions for both tasks independently to allow parallel execution
        is_enabled = report_settings.get("is_active") or report_settings.get("metadata_is_active")
        measures_count = get_failed_rows_measures_count(config)
        create_export_failed_rows_task = (
            is_enabled
            and include_catalog_task
            and measures_count > 0
            and not has_row_count_zero 
            and (
                not is_triggered
                or (is_triggered and measure_status and measure_status == "verified")
            )
            and current_dag
            not in [EXPORT_FAILED_ROWS, SEMANTICS, CATALOG_UPDATE, USAGE_QUERY, OBSERVE, PROCESS, METADATA]
            and not pending_tasks_count
        )

        # Check conditions for catalog update task independently
        is_deprecated = is_deprecated_asset(config)
        catalog = config.get("dag_info").get("catalog")
        catalog = catalog if catalog else []
        create_catalog_update_task_condition = (
            include_catalog_task
            and catalog
            and not pending_tasks_count
            and current_dag not in [CATALOG_UPDATE, SEMANTICS, USAGE_QUERY, OBSERVE, PROCESS, METADATA]
            and not is_deprecated
        )
        # Create both tasks in parallel if conditions are met
        if create_export_failed_rows_task:
            has_export_failed_rows_task = True
            create_failed_rows_task(config, is_triggered)

        if create_catalog_update_task_condition:
            has_alation_schedule = validate_alation_catalog_schedule(config, level)
            if not has_alation_schedule:
                has_catalog_update_task = True
                is_catalog_task_created = create_catalog_update_task(config, is_triggered)

        # create usage query update task (only if neither export_failed_rows nor catalog_update was created)
        usage_query_update_task = (
            asset_id
            and include_catalog_task
            and not is_deprecated
            and not is_triggered
            and not pending_tasks_count
            and not has_export_failed_rows_task
            and (not has_catalog_update_task or (has_catalog_update_task and not is_catalog_task_created))
            and current_dag not in [USAGE_QUERY, OBSERVE]
            and is_usage_enabled
            and connection_type
            not in [ConnectionType.ADLS.value, ConnectionType.File.value, ConnectionType.S3.value]
        )
        if usage_query_update_task:
            has_usage_query_update_task = True
            create_usage_query_update_task(config, is_triggered)
        # create event trigger Task
        has_event_trigger = schedule_level == ScheduleTypes.Asset.value and (
            not pending_tasks_count
            and not has_export_failed_rows_task
            and not has_catalog_update_task
            and not has_usage_query_update_task
            and not is_deprecated
            and current_dag not in [OBSERVE]
        )
        if has_event_trigger:
            trigger_event_job(config)

        return has_export_failed_rows_task or has_catalog_update_task


def validate_alation_catalog_schedule(config, level):
    connection = get_postgres_connection(config)
    is_exist = False
    is_alation_activated = []
    schedule_exist = []
    with connection.cursor() as cursor:
        query_string = f"""
            SELECT schedules_mapping.id FROM core.schedules_mapping
            join core.integrations on integrations.id=schedules_mapping.channel_id
            where level='catalog' and integrations.is_active is true
        """
        cursor = execute_query(connection, cursor, query_string)
        schedule_exist = fetchall(cursor)
        if len(schedule_exist) > 0:
            return True

        alation_check = f"""
            select * from core.channels
            join core.integrations on integrations.channel_id = channels.id
            where channels.technical_name = 'alation' and integrations.is_active=true
        """
        cursor = execute_query(connection, cursor, alation_check)
        is_alation_activated = fetchall(cursor)
        if schedule_exist and is_alation_activated:
            is_exist = True
        collibra_check = f"""
            select * from core.channels
            join core.integrations on integrations.channel_id = channels.id
            where channels.technical_name in ('collibra', 'hitachi_pdc') and integrations.is_active=true
        """
        cursor = execute_query(connection, cursor, collibra_check)
        is_collibra_activated = fetchall(cursor)
        if is_collibra_activated:
            if level == "measure":
                if not schedule_exist:
                    is_exist = False
                else:
                    is_exist = True
                    return is_exist
            else:
                is_exist = False
                return is_exist
        if not is_exist and level == "measure":
            is_exist = True
            measure_details = config.get("measures", [])
            if measure_details and is_alation_activated and not is_collibra_activated:
                measure_details = measure_details[0]
                measure_details = measure_details.get("id")
                query_string = f"""
                        select * from core.domain_mapping 
	                    where measure_id='{measure_details}'
                        order by created_date desc
                """
                cursor = execute_query(connection, cursor, query_string)
                meassure_list = fetchone(cursor)
                if meassure_list:
                    is_exist = False
            elif measure_details and is_collibra_activated and not is_alation_activated:
                measure_details = measure_details[0]
                measure_details = measure_details.get("id")
                query_string = f"""
                        select fields.id from core.field_property fp 
                        join core.fields on fields.id=fp.field_id 
                        where measure_id='{measure_details}' 
                        and lower(fields.name)='collibra asset id'
                """
                cursor = execute_query(connection, cursor, query_string)
                meassure_list = fetchone(cursor)
                if meassure_list:
                    is_exist = False
    return is_exist


def get_failed_rows_measures_count(config: dict):
    measures_count = 0
    job_level = config.get("level")
    selected_attributes = config.get("attributes", [])
    selected_attributes = selected_attributes if selected_attributes else []
    selected_attributes = [
        str(attribute.get("id")) for attribute in selected_attributes if attribute
    ]
    is_attribute_level = len(selected_attributes) > 0
    general_settings = config.get("settings", {})
    if not general_settings:
        dag_info = config.get("dag_info", {})
        dag_info = dag_info if dag_info else {}
        general_settings = dag_info.get("settings", {})
    general_settings = general_settings if general_settings else {}
    general_settings = (
        json.loads(general_settings, default=str)
        if isinstance(general_settings, str)
        else general_settings
    )

    report_settings = general_settings.get("reporting")
    report_settings = report_settings if report_settings else {}
    report_settings = (
        json.loads(report_settings, default=str)
        if isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}

    is_custom_measure = report_settings.get("is_custom_measure", None)
    is_custom_measure = True if is_custom_measure is None else is_custom_measure
    is_custom_measure = is_custom_measure if is_custom_measure else False

    export_category = report_settings.get("export_category", "custom")

    attribute_query = ""
    if job_level == "attribute" and is_attribute_level:
        attributes = [
            f"""'{attribute}'""" for attribute in selected_attributes if attribute
        ]
        attributes = ", ".join(attributes)
        attribute_query = f""" and mes.attribute_id in ({attributes}) """

    asset_id = config.get("asset_id")
    if not asset_id:
        asset = config.get("asset")
        asset = asset if asset else {}
        asset_id = asset.get("id")

    asset_query = f" and mes.asset_id='{asset_id}'" if asset_id else ""
    if job_level in ("asset", "attribute") and not asset_id:
        return measures_count

    measure_query = ""
    if job_level == "measure":
        measure = config.get("measure")
        measure = measure if measure else {}
        measure_id = measure.get("id")
        measure_query = f" and mes.id='{measure_id}'" if measure_id else ""

    custom_measure_query = ""
    if export_category == "custom":
        # Include only regular custom/semantic measures for all connections (including Snowflake)
        # Constraint-based measures (null_count, distinct_count) are excluded from custom category
        custom_measure_query = f""" and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup', 'parameter', 'cross_source') """
    elif export_category == "auto":
        # Include only default measures, exclude constraint-based measures for all connections
        custom_measure_query = f""" and base.is_default = True AND base.technical_name NOT IN ('null_count', 'distinct_count') """

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select count(mes.*) as measure_count
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.allow_score=True and (mes.is_aggregation_query=False or (mes.is_aggregation_query=True and mes.has_failed_rows_query=True))
            and lower(mes.status)='verified' and mes.is_export = True and mes.is_delete = False and mes.is_active = True and mes.last_run_date is not null
            {asset_query}{attribute_query}{measure_query}{custom_measure_query}
        """
        cursor = execute_query(connection, cursor, query_string)
        total_measure = fetchone(cursor)
        measure_count = total_measure.get("measure_count", 0) if total_measure else 0
        measure_count = measure_count if measure_count else 0
    return measure_count


def is_deprecated_asset(config: dict):
    """
    Check whether the asset is deprecated or not
    """
    is_deprecated_asset = False
    job_level = config.get("level")
    selected_attributes = config.get("attributes", [])
    selected_attributes = selected_attributes if selected_attributes else []
    selected_attributes = [
        str(attribute.get("id")) for attribute in selected_attributes if attribute
    ]
    is_attribute_level = len(selected_attributes) > 0

    asset_id = config.get("asset_id")
    if not asset_id:
        asset = config.get("asset")
        asset = asset if asset else {}
        asset_id = asset.get("id")

    query_string = ""
    if job_level == "attribute" and is_attribute_level:
        attributes = [
            f"""'{attribute}'""" for attribute in selected_attributes if attribute
        ]
        attributes = ", ".join(attributes)

        query_string = f"""
            select asset.id from core.asset
            join core.attribute on attribute.asset_id=asset.id and attribute.is_selected=true
            where asset.is_active=true and asset.is_delete=false 
            and lower(asset.status)!='{ApprovalStatus.Deprecated.value.lower()}'
            and lower(attribute.status)!='{ApprovalStatus.Deprecated.value.lower()}'
            and attribute.id in ({attributes})
        """
    elif job_level == "asset" and asset_id:
        query_string = f"""
            select id from core.asset
            where asset.is_active=true and asset.is_delete=false 
            and lower(asset.status)!='{ApprovalStatus.Deprecated.value.lower()}'
            and asset.id='{asset_id}'
        """

    if query_string:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            asset = fetchone(cursor)
            is_deprecated_asset = bool(not asset)

    return is_deprecated_asset


def get_pending_tasks_count(config: dict):
    measure_count = 0
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    status_list = [
        ScheduleStatus.Pending.value,
        ScheduleStatus.Running.value,
        ScheduleStatus.UpForRetry.value,
    ]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"

    with connection.cursor() as cursor:
        query_string = f"""
            select count(*) as total_count from core.request_queue_detail
            where queue_id='{queue_id}'
            and lower(status) in {status_to_check}
        """
        cursor = execute_query(connection, cursor, query_string)
        total_measure = fetchone(cursor)
        measure_count = total_measure.get("total_count", 0) if total_measure else 0
        measure_count = measure_count if measure_count else 0
    return measure_count


def get_active_profile_measures_count(config: dict):
    health_measure_count = 0
    profile_measure_count = 0
    asset_id = config.get("asset_id")
    if not asset_id:
        asset = config.get("asset")
        asset = asset if asset else {}
        asset_id = asset.get("id")
    if not asset_id:
        return (health_measure_count, profile_measure_count)

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select base.category, count(mes.*)  as measure_count
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            join core.attribute as attr on attr.id=mes.attribute_id
            where mes.asset_id='{asset_id}' and mes.is_active=True and base.is_visible=True
            and base.type in ('{DISTRIBUTION}', '{FREQUENCY}')
            and base.technical_name != 'pattern' and attr.is_selected=true
            and attr.profile=true
            group by base.category
        """
        cursor = execute_query(connection, cursor, query_string)
        profile_measures = fetchall(cursor)
        health_measure_count = 0
        advanced_profile_measure_count = 0
        for measure in profile_measures:
            category = measure.get("category")
            measure_count = measure.get("measure_count")
            measure_count = int(measure_count) if measure_count else 0
            if category == HEALTH:
                health_measure_count = health_measure_count + measure_count
            else:
                advanced_profile_measure_count = advanced_profile_measure_count + measure_count
    return (health_measure_count, advanced_profile_measure_count)


def get_pending_tasks(config: dict):
    pending_task_details = []
    connection_type = config.get("connection_type")
    asset_type = config.get("type")
    asset_type = asset_type if asset_type else ""
    include_profile_task = connection_type not in [
        ConnectionType.Tableau.value,
        ConnectionType.Dbt.value,
        ConnectionType.Airflow.value,
        ConnectionType.Fivetran.value,
        ConnectionType.Talend.value,
        ConnectionType.ADF.value,
        ConnectionType.PowerBI.value,
        ConnectionType.Wherescape.value,
    ] and asset_type.lower() not in ["pipeline", "task", "pipe", "stored procedure"]
    is_triggered = config.get("is_triggered")
    has_incremental_rows = True
    asset_metadata = config.get("asset_metadata", {})
    if asset_metadata:
        has_incremental_rows = asset_metadata.get("row_count", 0) > 0    
    if is_triggered or not has_incremental_rows:
        return pending_task_details
    queue_id = config.get("queue_id")
    asset_id = config.get("asset_id")
    attribute_pattern_query = ""
    level = config.get("level")

    status_list = [
        ScheduleStatus.Running.value,
        ScheduleStatus.Completed.value,
        ScheduleStatus.Failed.value,
        ScheduleStatus.Killed.value,
    ]
    attribute_query = ""
    if level == "attribute":
        attributes = config.get("attributes")
        attributes = attributes if attributes else []
        attributes = [
            f"""'{str(attribute.get("id"))}'""" for attribute in attributes if attribute
        ]
        attribute = ", ".join(attributes)
        attribute = f"({attribute})" if attribute else ""
        attribute_query = f" and mes.attribute_id in {attribute}" if attribute else ""
        attribute_pattern_query = (
            f" and attribute.id in {attribute}" if attribute else ""
        )
    elif level == "measure":
        measures = config.get("measures")
        measures = measures if measures else []
        measures = [
            f"""'{str(measure.get("id"))}'""" for measure in measures if measure
        ]
        measure = ", ".join(measures)
        measure = f"({measure})" if measure else ""
        attribute_query = f" and mes.id in {measure}" if measure else ""

    connection = get_postgres_connection(config)
    task_details = []
    completed_tasks = []
    filter_query = ""
    if asset_id:
        filter_query = f"and mes.asset_id='{asset_id}'"
    license_config = get_license_config(config)
    license_config = license_config if license_config else {}
    advanced_measure_license_config = license_config.get("advanced_measure")
    advanced_measure_license_config = (
        advanced_measure_license_config if advanced_measure_license_config else {}
    )
    measure_condition = "and (base.level in ('attribute', 'term') or (base.level='asset' and base.type='custom'))"
    if not advanced_measure_license_config.get("custom"):
        measure_condition = "and (base.type not in ('custom','term'))"
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct base.type, base.category, count(*) as measure_count 
            from core.measure as mes
            join core.base_measure as base ON base.id = mes.base_measure_id and base.is_visible=True 
            left join core.asset on asset.id=mes.asset_id and asset.is_active=true and asset.is_delete=false
            left join core.attribute as attr on attr.id=mes.attribute_id and attr.is_selected=true and lower(attr.status)!='{ApprovalStatus.Deprecated.value.lower()}' and attr.profile=true
            where mes.is_active=True and mes.is_delete=False and lower(mes.status)!='{ApprovalStatus.Deprecated.value.lower()}' {filter_query}
            and base.type not in ('{DISTRIBUTION}', '{FREQUENCY}')
            and (attr.id is null or (attr.id is not null and attr.is_selected=true and attr.is_active=true))
            {measure_condition} {attribute_query}
            group by base.type, base.category
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        filtered_tasks = []
        for task in task_details:
            if task and task.get("measure_count") > 0:
                task_type = str(task.get("type")).lower()
                task_type = (
                    BEHAVIORAL if task.get("category") == BEHAVIORAL else task_type
                )
                if task_type == SEMANTIC_MEASURE:
                    task_type = CUSTOM
                filtered_tasks.append(task_type)
        task_details = list(set(filtered_tasks))
        health, advanced_profile = get_active_profile_measures_count(config)
        if (
            include_profile_task
            and PROFILE not in task_details
            and (health + advanced_profile) > 0
        ):
            task_details.append(PROFILE)

        asset_query = f" and asset_id='{asset_id}'" if asset_id else ""
        query_string = f"""
            select count(*) as pattern_count
            from core.attribute
            JOIN core.asset ON asset.id = attribute.asset_id
            where lower(attribute.status)!='{ApprovalStatus.Deprecated.value.lower()}'
            and jsonb_array_length(user_defined_patterns::jsonb ) != 0  
            and (attribute.active_measures->>'user_defined_patterns')::int > 0 {asset_query}
            {attribute_pattern_query}
        """
        cursor = execute_query(connection, cursor, query_string)
        log_info(f"get_pending_tasks: Executing pattern query: {query_string}")
        pattern_task = fetchone(cursor)
        pattern_count = pattern_task.get("pattern_count")
        pattern_count = pattern_count if pattern_count else 0
        if pattern_count:
            task_details.append(CUSTOM)

        dag_info = config.get("dag_info", {})
        dag_info = dag_info if dag_info else {}
        general_settings = dag_info.get("settings", {})
        profile_settings = general_settings.get("profile")
        profile_settings = (
            json.loads(profile_settings, default=str)
            if profile_settings and isinstance(profile_settings, str)
            else profile_settings
        )
        profile_settings = profile_settings if profile_settings else {}
        is_profiling_enabled = profile_settings.get("is_active")
        if not is_profiling_enabled:
            if PROFILE in task_details:
                task_details.remove(PROFILE)
            if STATISTICS in task_details:
                task_details.remove(STATISTICS)

        query_string = f"""
            select category, status from core.request_queue_detail
            where queue_id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
        completed_tasks = []
        pending_tasks = []
        for task in tasks:
            category = task.get("category")
            status = task.get("status")
            if status in status_list:
                completed_tasks.append(category)
            else:
                pending_tasks.append(category)
        completed_tasks = list(set(completed_tasks))
        pending_tasks = list(set(pending_tasks))
        completed_tasks = [
            task for task in completed_tasks if task not in pending_tasks
        ]
        if pending_tasks:
            task_details = [task for task in task_details if task in pending_tasks]

    pending_task_details = [
        task for task in task_details if task not in completed_tasks
    ]
    return [*set(pending_task_details)]


def get_existing_tasks(config: dict, connection: object = None):
    queue_id = config.get("queue_id")
    if not connection:
        connection = get_postgres_connection(config)
    existing_tasks = []
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct category from core.request_queue_detail where queue_id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        existing_tasks = fetchall(cursor)
        existing_tasks = [task.get("category") for task in existing_tasks]
    return existing_tasks


def get_tasks_by_group(config: dict, category: str):
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    level = config.get("level")
    measure_id = config.get("measure_id")
    attribute_condition = ""
    measure_condition = ""
    if level == "attribute" and attribute_id:
        attribute_condition = f""" and mes.attribute_id='{attribute_id}' """
    if measure_id:
        measure_condition = f""" and mes.id='{measure_id}' """

    tasks = []
    query_string = ""
    has_category = False
    if category == STATISTICS:
        has_category = True
        query_string = f"""
            select distinct mes.attribute_id, attr.technical_name as attribute_name
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            join core.attribute as attr on attr.id=mes.attribute_id
            where base.type='{category}' and mes.is_active=True and attr.is_selected=True and lower(mes.status)!='{ApprovalStatus.Deprecated.value.lower()}'
            and mes.asset_id='{asset_id}'{attribute_condition}{measure_condition}
        """
    elif category in [CUSTOM, BEHAVIORAL]:
        has_category = True
        category_query = f""" and base.type='{category}'"""
        if category == CUSTOM:
            category_query = f""" and base.type in ('{CUSTOM}', '{SEMANTIC_MEASURE}') and base.category!='behavioral'"""
        elif category == BEHAVIORAL:
            category_query = (
                f""" and base.type='custom' and base.category='behavioral'"""
            )
        asset_filter_condition = ""
        if asset_id:
            asset_filter_condition = f"""
                and ((mes.attribute_id is not null and attr.is_selected=True) or mes.attribute_id is null) 
                and mes.asset_id='{asset_id}'
            """
        query_string = f"""
            select distinct mes.attribute_id, attr.technical_name as attribute_name,
            mes.id as measure_id, mes.base_measure_id, mes.technical_name as measure_name 
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            left join core.attribute as attr on attr.id=mes.attribute_id
            where mes.is_active=True and lower(mes.status)!='{ApprovalStatus.Deprecated.value.lower()}' {category_query}
            {asset_filter_condition} {attribute_condition}{measure_condition}
        """

    if not query_string:
        return tasks, has_category

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
        if category == CUSTOM:
            has_category = True
            query_string = f"""
                select distinct mes.attribute_id, attr.technical_name as attribute_name,
                mes.id as measure_id, mes.base_measure_id, mes.technical_name as measure_name 
                from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                join core.attribute as attr on attr.id=mes.attribute_id
                where mes.asset_id='{asset_id}' and mes.is_active=True and mes.technical_name='pattern'
                and jsonb_array_length(attr.user_defined_patterns::jsonb ) != 0 and attr.is_selected=True
                and lower(mes.status)!='{ApprovalStatus.Deprecated.value.lower()}' {attribute_condition}
            """
            cursor = execute_query(connection, cursor, query_string)
            pattern_measures = fetchall(cursor)
            pattern_measures = pattern_measures if pattern_measures else []
            tasks.extend(pattern_measures)
    return tasks, has_category


def update_job_end_time(config: dict):
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.request_queue set
            start_time=(CASE WHEN start_time is not null THEN start_time else CURRENT_TIMESTAMP end),
            end_time=(
                CASE
                    WHEN end_time is not null THEN end_time
                    WHEN end_time is null and start_time is not null and start_time < CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                    ELSE (CURRENT_TIMESTAMP + INTERVAL '1 second')
                END
            )
            where id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def update_workflow_end_time(config: dict):
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.workflow_execution set
            start_time=(CASE WHEN start_time is not null THEN start_time else CURRENT_TIMESTAMP end),
            end_time=(
                CASE
                    WHEN end_time is not null THEN end_time
                    WHEN end_time is null and start_time is not null and start_time < CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
                    ELSE (CURRENT_TIMESTAMP + INTERVAL '1 second')
                END
            )
            where id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def update_metrics_status(config: dict):
    category = config.get("category")
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    queue_id = config.get("queue_id")
    measure_id = config.get("measure_id")
    filter_conditions = []

    if asset_id:
        filter_conditions.append(f""" mes.asset_id='{asset_id}'  """)
    if attribute_id:
        filter_conditions.append(f""" mes.attribute_id='{attribute_id}' """)
    if measure_id:
        filter_conditions.append(f""" mes.id='{measure_id}' """)

    if category == OBSERVE:
        filter_conditions.append(f""" base.type='{RELIABILITY}' and base.level='asset' """)
    elif category == RELIABILITY:
        filter_conditions.append(
            f""" base.type='{RELIABILITY}' and base.level='asset' """
        )
    elif category == PROFILE:
        filter_conditions.append(
            f""" base.type in ('{DISTRIBUTION}', '{FREQUENCY}') and base.level='attribute' """
        )
    elif category == STATISTICS:
        filter_conditions.append(f""" base.type='{STATISTICS}' """)
    elif category == USERACTIVITY:
        filter_conditions.append(f""" base.type='{USERACTIVITY}' """)
    elif category == SYNCASSET:
        filter_conditions.append(f""" base.type='{SYNCASSET}' """)
    elif category == CATALOG_SCHEDULE:
        filter_conditions.append(f""" base.type='{CATALOG_SCHEDULE}' """)
    filter_conditions = " and ".join(filter_conditions)
    filter_query = f" {filter_conditions} " if filter_conditions else ""
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with measure_ids as (
                select mes.id from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                where {filter_query}
            ) update core.metrics set status='{FAILED}'
            where run_id='{queue_id}' and id in (
                select id from measure_ids
            )
        """
        cursor = execute_query(connection, cursor, query_string)


def create_failed_rows_task(config: dict, is_triggered: bool = False):
    """
    Create a task for export failed rows
    for the current run
    """
    level = config.get("level")
    measure_id = None
    if level == "measure":
        measure_id = config.get("measure_id")

    attribute_id = None
    if level == "attribute":
        attribute_id = config.get("attribute_id")
        if not attribute_id:
            attribute = config.get("attribute")
            attribute = attribute if attribute else {}
            attribute_id = attribute.get("id")

    queue_id = config.get("queue_id")
    organization_id = config.get("organization_id")
    if not organization_id:
        return
    connection = get_postgres_connection(config)
    dag_id = f"{str(organization_id)}_{EXPORT_FAILED_ROWS}"
    with connection.cursor() as cursor:
        count_query = f"""
            select count(*) as task_count from core.request_queue_detail
            where category='{EXPORT_FAILED_ROWS}' and queue_id='{queue_id}'
            and status in ('{ScheduleStatus.Pending.value}', '{ScheduleStatus.Running.value}')
        """
        cursor = execute_query(connection, cursor, count_query)
        task_count = fetchone(cursor)
        task_count = task_count if task_count else {}
        task_count = task_count.get("task_count") if task_count else 0
        if task_count > 0:
            return

        query_input = (
            str(uuid4()),
            EXPORT_FAILED_ROWS,
            dag_id,
            ScheduleStatus.Pending.value,
            queue_id,
            attribute_id,
            measure_id,
            False,
            is_triggered,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals}, CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        delete_query = f"""
            delete from core.request_queue_detail
            where category='{EXPORT_FAILED_ROWS}' and queue_id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, delete_query)

        query_string = f"""
            insert into core.request_queue_detail (
                id, category, dag_id, status, queue_id, attribute_id, measure_id, is_submitted, is_triggered, created_date
            ) values {query_param}
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update request queue status
        query_string = f"""
            update core.request_queue set status='{ScheduleStatus.Running.value}'
            where id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def fetch_asset_current_data(connection, asset_id):
    """
    Fetch current asset data from core.asset table
    """
    try:
        if asset_id:
            with connection.cursor() as cursor:
                query_string = f"""
                    SELECT 
                        score,
                        alerts,
                        issues,
                        properties->>'row_count' as row_count,
                        properties->>'column_count' as column_count
                    FROM core.asset 
                    WHERE id = '{asset_id}'
                    AND is_active = true 
                    AND is_delete = false
                """
                cursor = execute_query(connection, cursor, query_string)
                result = fetchone(cursor)
                
                if result:
                    return {
                        "score": result.get("score"),
                        "alerts": result.get("alerts", 0),
                        "issues": result.get("issues", 0),
                        "row_count": result.get("row_count", 0),
                        "column_count": result.get("column_count", 0)
                    }
        return {}
    except Exception as e:
        return {}


def create_catalog_update_task(config: dict, is_triggered: bool = False):
    """
    Create a task for catalog update for the current run
    """
    is_triggered = is_triggered if is_triggered else False
    queue_id = config.get("queue_id")
    organization_id = config.get("organization_id")
    asset_id = config.get("asset_id")
    
    # Fetch current data from database
    connection = get_postgres_connection(config)
    
    asset_properties = config.get("asset", {}).get("properties", {})
    properties = config.get("asset", {})
    old_properties = asset_properties.get('old_properties', {})
    raw_last_runs = properties.get("last_runs")
    if isinstance(raw_last_runs, str):
        last_runs = json.loads(raw_last_runs)
    else:
        last_runs = raw_last_runs
    last_runs = last_runs if last_runs else []
    data = len(last_runs)
    # Fetch current data from database
    current_data = fetch_asset_current_data(connection, asset_id)
    
    row_count = current_data.get("row_count", 0)
    column_count = current_data.get("column_count", 0)
    score = current_data.get("score", 0) if current_data.get("score") else None
    issue_count = current_data.get("issues", 0)
    alert_count = current_data.get("alerts", 0)

    if ((data > 1 and int(row_count) > 0) or (data > 0 and int(row_count) == 0)):           # Use original old_properties for comparison
        previous_row_count = old_properties.get("row_count", 0)
        previous_column_count = old_properties.get("column_count", 0)
        previous_score = old_properties.get("score", None)
        previous_issue_count = old_properties.get("issues", 0)
        previous_alert_count = old_properties.get("alerts", 0)
        if (row_count == previous_row_count) and (column_count == previous_column_count) and (issue_count == previous_issue_count ) and (alert_count == previous_alert_count) and (score == previous_score):
            if (int(row_count) == 0):
                return True
            return False
    if not organization_id:
        return None
    dag_id = f"{str(organization_id)}_{CATALOG_UPDATE}"
    with connection.cursor() as cursor:
        query_input = (
            str(uuid4()),
            CATALOG_UPDATE,
            dag_id,
            ScheduleStatus.Pending.value,
            queue_id,
            False,
            is_triggered,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals}, CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        delete_query = f"""
            delete from core.request_queue_detail
            where category='{CATALOG_UPDATE}' and queue_id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, delete_query)

        query_string = f"""
            insert into core.request_queue_detail (
                id, category, dag_id, status, queue_id, is_submitted, is_triggered, created_date
            ) values {query_param}
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update request queue status
        query_string = f"""
            update core.request_queue set status='{ScheduleStatus.Running.value}'
            where id='{queue_id}'
        """
        execute_query(connection, cursor, query_string)

        # Update old_properties with current data from database for next comparison
        properties = config.get("asset", {})
        asset_id = config.get("asset_id")
        asset_properties = config.get("asset", {}).get("properties", {})

        # Get existing old_properties or create new one
        old_properties = asset_properties.get("old_properties", {})
        if not isinstance(old_properties, dict):
            old_properties = {} 
        # Update old_properties with current data from database
        old_properties["issues"] = issue_count
        old_properties["row_count"] = row_count
        old_properties["column_count"] = column_count
        old_properties["score"] = score
        old_properties["alerts"] = alert_count
        asset_properties["old_properties"] = old_properties
        asset_properties = json.dumps(asset_properties, default=str)
        asset_properties = asset_properties.replace("'", "''")
        query_string = f"""
                update core.asset set properties = '{asset_properties}'
                where id='{asset_id}'
            """
        cursor = execute_query(connection, cursor, query_string)
    return True


def trigger_event_job(config: dict):
    is_triggered = config.get("is_triggered")
    if is_triggered:
        return

    queue_id = config.get("queue_id")
    asset_id = config.get("asset_id")
    measure_id = config.get("measure_id")
    level = config.get("level")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if measure_id and not asset_id:
            query_string = f"""
                select * from core.schedules
                where schedules.target_measure_id='{measure_id}'
                order by created_date desc
                """
        else:
            query_string = f"""
                select * from core.schedules
                where schedules.target_asset_id='{asset_id}'
                order by created_date desc
            """
        cursor = execute_query(connection, cursor, query_string)
        schedule = fetchone(cursor)
        schedule = schedule if schedule else {}
        if not schedule:
            return

        schedule_id = schedule.get("id")
        if level != "measure":
            query_string = f"""
                select status, count(*) as task_count from core.request_queue_detail
                where queue_id='{queue_id}'
                group by status
            """
            cursor = execute_query(connection, cursor, query_string)
            job_status = fetchall(cursor)
            is_job_completed = True
            for task in job_status:
                status = task.get("status")
                task_count = task.get("task_count")
                if task_count and status.lower() != "completed":
                    is_job_completed = False
                    break
            if not is_job_completed:
                return

        query_string = f"""
            select *, schedules_mapping.asset_id as asset_id from core.schedules
            join core.schedules_mapping on schedules_mapping.schedule_id = schedules.id
            left join core.asset on asset.id=schedules_mapping.asset_id
            left join core.measure on measure.id=schedules_mapping.measure_id
            where schedules.id='{schedule_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        event_schedule_list = fetchall(cursor)

        for event_schedule in event_schedule_list:
            asset_id = str(event_schedule.get("asset_id", None))
            attribute_id = str(event_schedule.get("attribute_id", None))
            measure_id = str(event_schedule.get("measure_id", None))
            report_id = str(event_schedule.get("report_id", None))
            is_pause= event_schedule.get("is_pause", False)
            category = (
                event_schedule.get("schedule_type")
                if event_schedule.get("schedule_type")
                and event_schedule.get("schedule_type") == ScheduleTypes.Semantic.value
                else ""
            )
            category = category if category else ""
            sch_timezone = event_schedule.get("timezone")
            user_timezone = pytz.timezone(sch_timezone)
            schedule_time = event_schedule.get("start_date")

            try:
                schedule_time = user_timezone.localize(
                    datetime.strptime(schedule_time, "%m-%d-%Y %H:%M")
                )
            except:
                schedule_time = user_timezone.localize(
                    datetime.strptime(schedule_time, "%Y-%m-%d %H:%M")
                )

            current_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
            local_date = current_utc.astimezone(user_timezone)
            event_category = event_schedule.get("schedule_type", "")
            event_level = event_schedule.get("level", "")

            if level == "measure":
                if local_date > schedule_time and is_pause == False:
                    create_request_queue(
                        event_schedule.get("level"),
                        asset_id,
                        attribute_id,
                        measure_id,
                        report_id,
                        event_category,
                        TriggerType.Schedule,
                        config,
                    )
            else:
                if event_level == "measure":
                    query_string = f"""
                        select count(*) as pending_tasks from core.request_queue
                        where measure_id ='{measure_id}' and trigger_type='schedule'
                        and status in ('{ScheduleStatus.Pending.value}', '{ScheduleStatus.Running.value}')
                    """
                elif event_level == "report":
                    query_string = f"""
                        select count(*) as pending_tasks from core.request_queue
                        where report_id ='{report_id}' and trigger_type='schedule'
                        and status in ('{ScheduleStatus.Pending.value}', '{ScheduleStatus.Running.value}')
                    """
                else:
                    query_string = f"""
                        select count(*) as pending_tasks from core.request_queue
                        where asset_id ='{asset_id}' and trigger_type='schedule'
                        and status in ('{ScheduleStatus.Pending.value}', '{ScheduleStatus.Running.value}')
                    """

                cursor = execute_query(connection, cursor, query_string)
                has_existing_schedule = fetchone(cursor)
                has_existing_schedule = (
                    has_existing_schedule.get("pending_tasks")
                    if has_existing_schedule
                    else 0
                )
                has_existing_schedule = has_existing_schedule > 0
                allow_schedule = (
                    local_date > schedule_time and not has_existing_schedule
                )
                if allow_schedule and is_pause == False:
                    create_request_queue(
                        event_schedule.get("level"),
                        asset_id,
                        attribute_id,
                        measure_id,
                        report_id,
                        event_category,
                        TriggerType.Schedule,
                        config,
                    )


def create_usage_query_update_task(config: dict, is_triggered: bool = False):
    """
    Create a task for usage query update for the current run
    """
    queue_id = config.get("queue_id")
    organization_id = config.get("organization_id")
    if not organization_id:
        return
    connection = get_postgres_connection(config)
    dag_id = f"{str(organization_id)}_{USAGE_QUERY}"
    with connection.cursor() as cursor:
        query_input = (
            str(uuid4()),
            USAGE_QUERY,
            dag_id,
            ScheduleStatus.Pending.value,
            queue_id,
            False,
            is_triggered,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals}, CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        delete_query = f"""
            delete from core.request_queue_detail
            where category='{USAGE_QUERY}' and queue_id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, delete_query)

        query_string = f"""
            insert into core.request_queue_detail (
                id, category, dag_id, status, queue_id, is_submitted, is_triggered, created_date
            ) values {query_param}
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update request queue status
        query_string = f"""
            update core.request_queue set status='{ScheduleStatus.Running.value}'
            where id='{queue_id}'
        """
        execute_query(connection, cursor, query_string)


def update_workflow_queue_status(config: dict, status: str):
    status_list = [
        ScheduleStatus.Completed.value,
        ScheduleStatus.Failed.value,
        ScheduleStatus.Killed.value,
    ]

    queue_id = config.get("queue_id")
    workflow_id = config.get("workflow_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.workflow_execution set start_time=CURRENT_TIMESTAMP
            where id='{queue_id}' and start_time is null
        """
        cursor = execute_query(connection, cursor, query_string)

        end_time_query = ""
        if status in status_list:
            end_time_query = f", end_time=CURRENT_TIMESTAMP"

        status_to_check = [f"""'{str(status_value)}'""" for status_value in status_list]
        status_to_check = ", ".join(status_to_check)
        status_to_check = f"({status_to_check})"

        query_string = f"""
            update core.workflow_execution set status='{status}', modified_date=CURRENT_TIMESTAMP{end_time_query}
            where id='{queue_id}' and status not in {status_to_check}
        """
        cursor = execute_query(connection, cursor, query_string)

        run_status = status
        if status in status_list:
            run_status = ScheduleStatus.Completed.value
        if workflow_id:
            query_string = f"""
                update core.workflow set status='{run_status}' where id='{workflow_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

    if status in status_list:
        update_workflow_end_time(config)


def update_workflow_task_queue_status(
    config: dict,
    task: dict,
    input: dict,
    output: dict = None,
    error=None,
    status: str = "Running",
):
    task_id = task.get("id")
    workflow_id = config.get("workflow_id")
    queue_id = config.get("queue_id")
    trigger_type = config.get("trigger_type", "once")
    existing_tasks = get_existing_workflow_task_queue(config, task_id)
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if not existing_tasks:
            query_input = (
                str(uuid4()),
                workflow_id,
                queue_id,
                status,
                task_id,
                datetime.now(),
                datetime.now() if status == ScheduleStatus.Completed.value else None,
                trigger_type,
                json.dumps(input if input else {})
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            query_string = f"""
                insert into core.workflow_execution (
                    id, workflow_id, parent_id, status, workflow_task_id, start_time, end_time, trigger_type, input, created_date
                ) values {query_param}
            """
            cursor = execute_query(connection, cursor, query_string)

        else:
            error = str(error).replace("'", "''") if error else ""
            query_string = f"""update core.workflow_execution 
                set status='{status}', modified_date=CURRENT_TIMESTAMP, end_time=CURRENT_TIMESTAMP, 
                output='{json.dumps(output if output else {}).replace("'", "''")}', error='{error}' 
                where parent_id='{queue_id}' AND workflow_id='{workflow_id}' AND workflow_task_id='{task_id}'"""
            cursor = execute_query(connection, cursor, query_string)

        run_status = status
        status_list = [
            ScheduleStatus.Completed.value,
            ScheduleStatus.Failed.value,
            ScheduleStatus.Killed.value,
        ]
        if status in status_list:
            run_status = ScheduleStatus.Completed.value

        query_string = f"""
            update core.workflow_tasks set status='{run_status}'
            where workflow_id = '{workflow_id}' and id='{task_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def get_existing_workflow_task_queue(config: dict, task_id: str):
    queue_id = config.get("queue_id")
    workflow_id = config.get("workflow_id")
    existing_tasks = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct id from core.workflow_execution 
            where parent_id='{queue_id}' and workflow_id = '{workflow_id}' and workflow_task_id='{task_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        existing_tasks = fetchall(cursor)
    return existing_tasks
