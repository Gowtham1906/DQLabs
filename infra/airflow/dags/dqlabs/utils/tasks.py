"""
Migration Notes From V2 to V3:
Migrations Completed
Pending:True
"""

from decimal import Decimal
from uuid import uuid4
import os
import json
import pandas as pd
import numpy as np

from airflow.configuration import conf
from airflow.models import DAG, DagBag, DagRun
from airflow.settings import Session
from airflow.utils.state import State
from airflow.api.common import trigger_dag

# import dqlabs helpers and constants

from dqlabs.utils import get_all_organizations, get_active_channel
from dqlabs.utils import get_organization
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.utils.semantics.semantic_functions import has_semantic_terms
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection, delete_metrics
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.app_helper.dq_helper import calculate_weightage_score
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_constants.dq_constants import (
    SEMANTICS,
    CUSTOM,
    METADATA,
    BUSINESS_RULES,
    CATALOG_UPDATE,
    MAX_TASKS_TO_SUBMIT,
    MAX_RETRY_DELAY,
    USERACTIVITY,
    SYNCASSET,
    CATALOG_SCHEDULE,
    USAGE_QUERY,
    PROCESS
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.measure_comparison import (
    get_comparison_metrics,
    get_measure_properties,
    get_comparison_threshold_values,
    comparison_operator,
    time_conversion,
    update_last_runs_for_comparison_measure,
    update_comparison_last_run_alerts,
    send_event
)


def get_airflow_task_limit(config: dict, category: str = None):
    """
    Returns maximum tasks limit we can submit to airflow
    """

    default_tasks_limit = MAX_TASKS_TO_SUBMIT
    default_max_delay = MAX_RETRY_DELAY
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
    general_settings = general_settings if general_settings else {}

    profile_settings = general_settings.get("profile")
    profile_settings = profile_settings if profile_settings else {}
    profile_settings = (
        json.loads(profile_settings, default=str)
        if isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    profile_task_limit = profile_settings.get("max_tasks")
    max_retry_delay = profile_settings.get("max_retry_delay")

    profile_task_limit = (
        int(profile_task_limit) if profile_task_limit else default_tasks_limit
    )
    task_limit = profile_task_limit if profile_task_limit else default_tasks_limit

    max_retry_delay = int(max_retry_delay) if max_retry_delay else default_max_delay
    retry_delay = max_retry_delay if max_retry_delay else default_max_delay
    
    category_task_limit = task_limit
    if category:
        category_task_limit = int(profile_settings.get(f"{category}_limit", task_limit))

    return task_limit, retry_delay, category_task_limit


def get_tasks_by_category(config: dict, category: str):
    """
    Returns the tasks for the given category
    """
    tasks = []
    if not category:
        return tasks

    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, category)
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"

    if category_task_limit and category_task_limit > 0:
        category_task_limit = category_task_limit
    else:
        category_task_limit = task_limit
    category_task_limit = category_task_limit if category_task_limit else task_limit

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with active_assets as (
                select distinct 
                    que.asset_id,
                    que.id as queue_id,
                    que.created_date as queue_created_date
                from core.request_queue as que
                join core.request_queue_detail as det ON det.queue_id = que.id
                where lower(det.status) in ('{ScheduleStatus.Pending.value.lower()}', '{ScheduleStatus.Running.value.lower()}')
                and que.asset_id is not null
            ), filtered_tasks as (
                select det.id, det.queue_id, que.created_date as que_date, det.created_date as task_date, det.status, det.is_submitted,
                que.asset_id, det.category
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                left join core.connection as con on con.id = que.connection_id
                where 
                (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and abs(EXTRACT(EPOCH FROM (current_timestamp - det.end_time))) >= {retry_delay}))
                and not (
                    lower(det.status) in ('{ScheduleStatus.Pending.value.lower()}', '{ScheduleStatus.UpForRetry.value.lower()}')
                    and que.asset_id is not null
                    and exists (
                        select 1 from active_assets as aa
                        where que.id != aa.queue_id
                        and aa.queue_created_date < que.created_date
                        and que.asset_id = aa.asset_id
                    )
                )
                order by que_date asc, task_date asc, is_submitted desc
            ), ordered_tasks as (
                select * from filtered_tasks where lower(status) in ('{ScheduleStatus.Running.value.lower()}')
                union all
                select * from (
					select * from filtered_tasks where lower(status) not in ('{ScheduleStatus.Running.value.lower()}')
                    order by que_date asc, task_date asc, is_submitted desc
                ) as pending
            ), category_tasks as (
                select * from filtered_tasks where category = '{category}'
                order by is_submitted desc, que_date asc, task_date asc
                limit {category_task_limit}
            ), other_tasks as (
                select * from filtered_tasks where category != '{category}' or category is null
                order by is_submitted desc, que_date asc, task_date asc
            ), aggregated_tasks as (
                select * from category_tasks
                union all
                select * from other_tasks
                order by is_submitted desc, que_date asc, task_date asc
                limit {task_limit}
            )
            select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
            det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
            det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
            que.connection_id, que.job_input, que.airflow_config, ast.technical_name, ast.airflow_pool_name, 
            (
                case
                    when ast.organization_id is not null then ast.organization_id
                    when measure.organization_id is not null then measure.organization_id
                    else con.organization_id
                end
            ) as organization_id,
            data.failed_rows_table, data.reset_failed_rows_table, data.domain_failed_rows_table, con.airflow_connection_id as source_connection_id,
            con.type as connection_type, data.primary_columns, data.row_count, ast.status as asset_status,
            row_to_json(con.*) as connection, row_to_json(ast.*) as asset, measure.failed_rows_table as measure_failed_rows_table,
            measure.reset_failed_rows_table as measure_reset_failed_rows_table, measure.domain_failed_rows_table as measure_domain_failed_rows_table,
            measure.status as measure_status, data.is_incremental, data.watermark, data.incremental_config, data.custom_fingerprint
            from core.request_queue_detail as det
            join aggregated_tasks as task ON task.id = det.id
            join core.request_queue as que ON que.id = task.queue_id
            left join core.asset as ast ON ast.id = que.asset_id and ast.is_delete=False and ast.is_active=True and ast.is_valid=True
            left join core.data on data.asset_id = ast.id
            left join core.measure on measure.id=det.measure_id and measure.is_active=true
            left join core.connection as con on con.id = que.connection_id
            where det.id in (select id from aggregated_tasks) and det.category='{category}'
            order by det.created_date asc, det.is_submitted desc
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)

        task_categories = []
        # update attribute and measure details if any
        for task in tasks:
            category = task.get("category")
            task_categories.append(category)
            if category in [USERACTIVITY, SYNCASSET]:
                continue
            asset_id = task.get("asset_id")
            attribute_id = task.get("attribute_id")
            measure_id = task.get("measure_id")

            if category in [CATALOG_UPDATE]:
                job_input = task.get("job_input")
                job_input = job_input if job_input else {}
                asset_id = job_input.get("asset_id")
                attribute_id = job_input.get("attribute_id")
                measure_id = job_input.get("measure_id")

            measure_condition = ""
            if measure_id:
                measure_condition = f""" where mes.id='{measure_id}' """
            elif attribute_id:
                measure_condition = f""" where mes.attribute_id='{attribute_id}' and base.type='{category}' """

            # add measure details if mapped measure id
            if measure_condition:
                query = f"""
                    select mes.attribute_id, attribute.name as attribute_name, mes.status,
                    mes.id as id, base.id as base_measure_id, base.technical_name as name,
                    base.query, base.properties, base.type, base.level, base.category, base.derived_type,
                    mes.allow_score, mes.is_drift_enabled, mes.attribute_id, mes.asset_id, mes.is_positive,
                    mes.drift_threshold, base.term_id, mes.semantic_measure, mes.semantic_query, mes.weightage,
                    mes.is_aggregation_query, mes.has_failed_rows_query, mes.result as pass_criteria_result,
                    mes.enable_pass_criteria, mes.pass_criteria_threshold, mes.pass_criteria_condition, mes.advanced_config_id
                    from core.measure as mes
                    join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
                    left join core.asset as ast on ast.id = mes.asset_id
                    left join core.attribute on attribute.id = mes.attribute_id and attribute.asset_id=ast.id
                    {measure_condition}
                """
                cursor = execute_query(connection, cursor, query)
                measure = fetchone(cursor)
                if measure:
                    task.update(
                        {
                            "measure": measure,
                            "measure_id": measure.get("id"),
                            "measure_name": measure.get("name"),
                        }
                    )

            # add attribute details if mapped attribute id
            if attribute_id and asset_id:
                query = f"""
                    select distinct attribute.id, attribute.technical_name as attribute_name, attribute.is_selected, attribute.is_active,
                    attribute.name, attribute.derived_type, attribute.min_length, attribute.max_length, attribute.updated_properties,
                    data.row_count, terms_mapping.term_id, attribute.is_semantic_enabled,
                    attribute.min_value, attribute.max_value, terms_mapping.approved_by, attribute.status
                    from core.attribute
					join core.asset on asset.id = attribute.asset_id
	 				join core.data on data.asset_id = asset.id
					left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                    where attribute.asset_id='{asset_id}' and attribute.is_selected=True and attribute.id='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query)
                attribute = fetchone(cursor)
                if attribute:
                    task.update(
                        {
                            "attribute": attribute,
                            "attribute_id": attribute.get("id"),
                            "attribute_name": attribute.get("attribute_name"),
                        }
                    )
    return tasks


def get_semantic_tasks(config: dict):
    """
    Returns the task defails for the semantic job
    """
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    organization_id = config.get("organization_id")
    task_details = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, SEMANTICS)
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
        with attribute_metadata as (
                select distinct attribute.asset_id, count(*) as semantic_attributes
                from core.asset as ast
                join core.attribute on attribute.asset_id=ast.id
				left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                where ast.organization_id='{organization_id}' and ast.is_active=True
                and ast.is_delete=False and ast.is_valid=True
                and terms_mapping.term_id is null and attribute.is_semantic_enabled=True
                group by attribute.asset_id
                having count(*) > 0
            )
            select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
            det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
            det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
            que.connection_id, que.job_input, que.airflow_config, 
            ast.id as asset_id, ast.technical_name, ast.airflow_pool_name,
            ast.organization_id, data.primary_columns, data.row_count,
            con.id as connection_id, con.airflow_connection_id as source_connection_id, con.type as connection_type,
            row_to_json(ast.*) as asset, row_to_json(con.*) as connection, data.is_incremental, data.watermark, data.incremental_config, data.custom_fingerprint
            from core.request_queue_detail as det
            join core.request_queue as que ON que.id = det.queue_id
            join core.asset as ast on ast.id=que.asset_id 
			join core.data on data.asset_id = ast.id
            join core.connection as con on con.id = ast.connection_id
            join attribute_metadata as meta on meta.asset_id=ast.id
            where det.category='{SEMANTICS}'
            and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
            and que.asset_id is not null
            order by que.created_date desc
            limit {task_limit}
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        task_details = task_details if task_details else []

        for task in task_details:
            # update attribute details if any
            asset_id = task.get("asset_id")
            attribute_id = task.get("attribute_id")
            # add attribute details if mapped attribute id
            if not (asset_id and attribute_id):
                continue

            query = f"""
                select distinct attribute.id, attribute.technical_name as attribute_name, attribute.is_selected, attribute.is_active,
                attribute.name, attribute.derived_type, attribute.min_length, attribute.max_length, attribute.updated_properties,
                data.row_count, terms_mapping.term_id, attribute.is_semantic_enabled,
                attribute.min_value, attribute.max_value
                from core.attribute
                join core.asset as ast on ast.id=attribute.asset_id
	 			join core.data on data.asset_id =ast.id
				left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                where attribute.asset_id='{asset_id}' and attribute.is_selected=True and attribute.id='{attribute_id}'
                order by attribute.created_date desc
            """
            cursor = execute_query(connection, cursor, query)
            attribute = fetchone(cursor)
            attribute = attribute if attribute else {}
            if not attribute:
                continue
            task.update(
                {
                    "attribute": attribute,
                    "attribute_id": attribute.get("id"),
                    "attribute_name": attribute.get("attribute_name"),
                }
            )
    return task_details


def get_semantic_task_details(config: dict):
    """
    Returns the tasks for the given category
    """
    tasks = []
    has_verified_terms = has_semantic_terms(config)
    if not has_verified_terms:
        return tasks

    semantic_tasks = get_semantic_tasks(config)
    return semantic_tasks


def has_pending_tasks(config: dict):
    """
    Returns list of pending tasks if any
    """
    tasks = []
    task_by_category = {}
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
            select distinct category, array_agg(id) as tasks
            from core.request_queue_detail
            where lower(status) in {status_to_check}
            group by category
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        for task in task_details:
            category = task.get("category")
            task_list = task.get("tasks")
            if isinstance(task_list, str):
                task_list = task_list.replace("{", "").replace("}", "").split(",")
            dag_tasks = list(task_list)
            tasks.append(category)
            task_by_category.update({category: dag_tasks})
    return tasks, task_by_category


def has_workflow_pending_tasks(config: dict):
    """
    Returns list of pending workflows tasks if any
    """
    tasks = []
    task_by_category = {}
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
            select array_agg(distinct id) as tasks
            from core.workflow_execution
            where lower(status) in {status_to_check}
            and parent_id is null and workflow_task_id is null
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        for task in task_details:
            category = "workflow"
            task_list = task.get("tasks")
            if task_list:
                if isinstance(task_list, str):
                    task_list = task_list.replace("{", "").replace("}", "").split(",")
                dag_tasks = list(task_list)
                tasks.append(category)
                task_by_category.update({category: dag_tasks})
    return tasks, task_by_category


def get_task_config(config: dict, context: dict):
    """
    Retruns task related configurations
    """
    airflow_config = config.get("airflow_config")
    airflow_config = airflow_config if airflow_config else {}
    host_name = airflow_config.get("host")
    if not host_name:
        host_name = context.get("conf").get("cli", "endpoint_url")

    task_instance = context.get("task_instance")
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    attempt = task_instance.try_number
    dag_id = context.get("dag_run").dag_id
    execution_date = context.get("dag_run").execution_date
    execution_date = execution_date.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    execution_date = list(execution_date)
    execution_date.insert(-2, ":")
    execution_date = "".join(execution_date)

    task_config = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "attempt": attempt,
        "execution_date": str(execution_date),
        "host": host_name,
    }
    config.update({"task_config": task_config})
    return task_config


def check_task_status(config: dict, context=None):
    """
    Retruns true if task is already completed, false otherwise.
    """
    task_instance = context.get("task_instance") if context else {}
    task_id = task_instance.task_id
    try_number = config.get("try_number")
    if try_number and task_id and not str(task_id).endswith(str(try_number)):
        log_info("The task id got changed for the given task.")
        return True

    queue_detail_id = config.get("queue_detail_id")
    connection = get_postgres_connection(config)
    is_completed = False
    with connection.cursor() as cursor:
        query_string = f"""
            select status from core.request_queue_detail
            where id='{queue_detail_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        task_status = fetchone(cursor)
        task_status = task_status.get("status") if task_status else ""
        is_completed = bool(
            task_status
            not in [
                ScheduleStatus.Pending.value,
                ScheduleStatus.Running.value,
                ScheduleStatus.UpForRetry.value,
            ]
        )
    return is_completed


def schedule_tasks():
    """
    Schedule the tasks for all the categories
    """

    current_env_name = os.environ.get("AIRFLOW_ENV_NAME")
    if not current_env_name:
        current_env_name = conf.get("core", "ENV_NAME")
    running_states = [State.RUNNING, State.SCHEDULED, State.QUEUED]
    dagbag = None
    try:
        dagbag: DagBag = DagBag(dag_folder="dags/", include_examples=False)
    except:
        dagbag = None
    
    session = Session()
    try:
        organizations = get_all_organizations()
        for organization in organizations:
            try:
                if not organization:
                    continue

                dq_organization = get_organization(organization)
                if not dq_organization:
                    continue

                is_active_org = dq_organization.get("is_active")
                if not is_active_org:
                    continue
                organization.update(dq_organization)
                organization.update({"schedule_interval": "*/1 * * * *"})

                core_connection_id = organization.get("core_connection_id")
                if not core_connection_id:
                    continue

                config = organization

                pending_tasks, task_details = has_pending_tasks(config)
                workflow_pending_tasks, workflow_task_details = (
                    has_workflow_pending_tasks(config)
                )
                if workflow_pending_tasks:
                    pending_tasks = [*pending_tasks, *workflow_pending_tasks]
                    task_details = {**task_details, **workflow_task_details}
                if not pending_tasks:
                    continue

                organization_id = config.get("admin_organization")
                if not organization_id:
                    organization_id = config.get("id")

                for category in pending_tasks:
                    dag_name = BUSINESS_RULES if category == CUSTOM else category
                    dag_id = f"{str(organization_id)}_{dag_name}"

                    dag: DAG = None
                    if dagbag:
                        dag = dagbag.get_dag(dag_id)

                    # check is the dag running or not
                    waiting_tasks = task_details.get(category)
                    task_ids = list(dag.task_ids) if dag else []
                    has_pending_task = False
                    if task_ids:
                        for task_id in task_ids:
                            if has_pending_task:
                                break
                            for task in waiting_tasks:
                                if has_pending_task:
                                    break
                                if str(task_id).startswith(task):
                                    has_pending_task = True
                                    break
                    else:
                        has_pending_task = not task_ids and len(waiting_tasks) > 0

                    if not has_pending_task:
                        continue

                    # check is the dag running or not
                    query = session.query(DagRun).filter(DagRun.dag_id == dag_id)
                    dag_run = query.order_by(DagRun.execution_date.desc()).first()
                    is_running = dag_run and dag_run.state in running_states
                    if is_running:
                        continue

                    try:
                        # trigger the dag if it has tasks to run
                        trigger_dag.trigger_dag(dag_id)
                    except Exception as e:
                        log_error("Trigger dag failed. Error - ", e)
                        continue
            except Exception as e:
                log_error("schedule_tasks - For organization - Failed with error", e)
    except Exception as e:
        log_error("schedule_tasks - Failed with error", e)
    finally:
        if session:
            session.close()


def get_notification_task_details(config: dict):
    """
    Returns the tasks for the given category
    """
    tasks = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, 'notification')
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with notification as (
                select distinct
                    que.id as queue_id,
                    det.id as queue_detail_id,
                    noty.id as notification_id,
                    noty.module as module_name,
                    que.created_date as queue_created_date,
                    det.category,
                    det.status,
                    det.attempt,
                    det.task_id,
                    det.try_number,
                    det.created_date as task_date,
                    que.created_date as que_date,
                    det.attribute_id,
                    det.measure_id,
                    det.is_triggered,
                    det.task_config,
                    que.level,
                    que.attributes,
                    que.measures,
                    que.asset_id,
                    que.connection_id,
                    que.job_input,
                    que.airflow_config,
                    null::jsonb as properties,
                    noty.is_summary,
                    noty.type as notification_type
                from core.request_queue as que 
                join core.request_queue_detail as det ON que.id = det.queue_id
                join core.notifications as noty ON noty.id = que.notification_id
                join core.notifications_channel ON notifications_channel.notifications_id = noty.id
                join core.integrations ON integrations.channel_id = notifications_channel.channels_id
                join core.channels ON channels.id = integrations.channel_id
                join core.templates temp ON temp.type = noty.type and temp.channel_id=channels.id
                left join core.notifications_status ON notifications_status.notification_id = noty.id 
                    and notifications_status.channel_id = notifications_channel.channels_id
                left join core.users ON users.id::text = noty.trigger_by::text
                where det.category='notification' and que.level not in ('share_widget', 'share_dashboard')
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                and integrations.is_active = true and integrations.is_delete = false and channels.noty = true
                union all 
                select distinct
                    que.id as queue_id,
                    det.id as queue_detail_id,
                    case when que.level = 'report' or que.level = 'share_widget' then widget.id else dashboard.id end as report_id,
                    que.level as module_name,
                    que.created_date as queue_created_date,
                    det.category,
                    det.status,
                    det.attempt,
                    det.task_id,
                    det.try_number,
                    det.created_date as task_date,
                    que.created_date as que_date,
                    det.attribute_id,
                    det.measure_id,
                    det.is_triggered,
                    det.task_config,
                    que.level,
                    que.attributes,
                    que.measures,
                    que.asset_id,
                    que.connection_id,
                    que.job_input,
                    que.airflow_config,
                    case when notifications.id is not null then notifications.content  else schedules_mapping.properties end as properties,
                    false as is_summary,
                    notifications.type as notification_type
                from core.request_queue as que 
                join core.request_queue_detail as det ON que.id = det.queue_id
                left join core.widget on widget.id=que.report_id
				left join core.dashboard on dashboard.id=que.dashboard_id
                left join core.notifications on notifications.id=que.notification_id
                left join core.schedules_mapping on schedules_mapping.report_id=widget.id or schedules_mapping.dashboard_id=dashboard.id
                where det.category='notification' and (que.level = 'share_widget' or que.level ='share_dashboard' or que.level='report' or que.level='dashboard')
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                and (widget.id is null or widget.is_active = true) and (dashboard.id is null or dashboard.is_active=true)
            )
            select * from notification limit {task_limit}
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
    return tasks


def get_workflow_task_details(config: dict):
    """
    Returns the tasks for the given category
    """
    tasks = []
    task_limit, _, category_task_limit = get_airflow_task_limit(config, 'workflow')
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select distinct
                workflow_execution.id as queue_id,
                workflow.id as workflow_id,
                workflow.name as workflow_name,
                workflow_execution.input as workflow_input,
                "workflow" as category,
                workflow_execution.status,
                workflow_execution.created_date as task_date,
                workflow_execution.job_input,
                workflow_execution.airflow_config,
                workflow_execution.trigger_type
            from core.workflow_execution 
            join core.workflow on workflow.id = workflow_execution.workflow_id
            where lower(workflow_execution.status) in {status_to_check} and workflow.is_active=true
            and workflow_execution.workflow_task_id is null
            limit {task_limit}
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
    return tasks


def run_comparison(config: dict, measure_id: str):

    # Get the postgres connection
    connection = get_postgres_connection(config)
    run_id = config.get("run_id")
    connection_id = config.get("connection_id")
    message = ""
    last_runs_measures = {}
    run_result_priority = "Ok"

    # Get comparison metrics values from the backend which the user has created"""
    metrics = get_comparison_metrics(connection, measure_id)
    base_measure_id = metrics.get("id")
    priority = metrics.get("priority")
    executed_query = ""
    allow_score = False
    is_drift_enabled = False

    # Get threshold constraints for different health measure set by the user """
    response = get_measure_properties(connection, base_measure_id)
    response = response.get("properties").get("comparison", [])
    total_measure_count = 0
    failed_measure_count = 0
    result_measure = []
    result = {}
    for measure_group in response:
        rule_group_properties = measure_group.get("properties", {})
        source_name = rule_group_properties.get("source", "").get("label", "")
        target_name = rule_group_properties.get("target", "").get("label", "")
        rules_list = measure_group.get("rules", [])
        for rule in rules_list:
            comparison_counts = get_comparison_threshold_values(
                connection, rule_group_properties, rule
            )
            drift_threshold = {
                "lower_threshold": 0,
                "upper_threshold": 0,
            }
            diff_value = 0
            measure_name = rule.get("measure")
            try:
                if comparison_counts:
                    diff_value = int(comparison_counts.get("diff_value", "0"))
                    drift_threshold.update(
                        {
                            "lower_threshold": comparison_counts.get("source_count"),
                            "upper_threshold": comparison_counts.get("target_count"),
                        }
                    )
                    unique_key = f"""{source_name}||{target_name}||{measure_name}"""
                    rule.update(
                        {
                            "name": unique_key,
                            "difference": diff_value,
                            "threshold": drift_threshold,
                        }
                    )

                    # Getting the threshold value contrainsts set by the user
                    value_1 = float(rule.get("value1"))
                    value_2 = float(rule.get("value2")) if rule.get("value2") else 0
                    condition_measure = rule.get("condition").lower()

                    # Compute the result flag if the condition has passed/failed for all use cases
                    if diff_value is not None:
                        if rule.get("time") in (
                            "Minutess",
                            "Hours",
                            "Days",
                            "Weeks",
                            "Months",
                        ):
                            time_label = rule.get("time")
                            value_1 = time_conversion(value_1, time_label)
                            if value_2 > 0:
                                value_2 = time_conversion(value_2, time_label)
                        if condition_measure not in ("isbetween", "isnotbetween"):
                            result_flag = comparison_operator[condition_measure](
                                int(diff_value), value_1
                            )
                            rule.update({"result": result_flag})
                        else:
                            if condition_measure == "isbetween":
                                result_flag = value_1 <= int(diff_value) <= value_2
                                rule.update({"result": result_flag})
                            elif condition_measure == "isnotbetween":
                                result_flag = not (
                                    value_1 <= int(diff_value) <= value_2
                                )
                                rule.update({"result": result_flag})
                        result.update({rule.get("name"): result_flag})
                        result_measure.append(rule.get("name"))
            except AttributeError as e:
                diff_value = 0

    total_measure_count = len(result_measure)
    failed_measure_count = sum(1 for key in result if result[key] is False)

    # Delete metrics for the same run id
    delete_metrics(
        config,
        run_id=run_id,
        measure_id=measure_id,
    )

    # Insert in core.drift_alert tables
    with connection.cursor() as cursor:
        for measure_group in response:
            rules_list = measure_group.get("rules", [])
            for measure in rules_list:
                unique_id = measure.get("name", "")
                is_auto = "false"
                measure_status = priority
                if unique_id and unique_id in result_measure:
                    health_result = measure.get("result")
                    if not health_result:
                        message = (
                                    f"{failed_measure_count} out of {total_measure_count} Measures failed in the comparison.\n"
                                    f"{measure.get('name')} did not meet the expected result. Difference: {measure.get('difference')}\n "
                                    f"Source Count: {measure.get('threshold').get('lower_threshold')} | Target Count: {measure.get('threshold').get('upper_threshold')}"
                                )
                    else:
                        message = (
                            f"{failed_measure_count} out of {total_measure_count} Measures failed in the comparison.\n"
                            f"{measure_name} did not meet the expected result. Difference: {measure.get('difference')}\n"
                            f"Source Count: {measure.get('threshold').get('lower_threshold')} | Target Count: {measure.get('threshold').get('upper_threshold')}"
                        )
                        measure_status = "Ok"
                    run_result_priority = (
                        measure_status if measure_status != "Ok" else measure_status
                    )
                    alert_input_values = []
                    current_value = measure.get("difference")
                    drift_threshold = json.dumps(measure.get("threshold"), default=str)

                    """ Insert in core.metrics tables to display the data in frontend"""
                    # Declare non-null variables to be added to metrics table
                    total_count = current_value
                    valid_count = current_value
                    invalid_count = 0
                    valid_percentage = 100
                    invalid_percentage = 0
                    score = None
                    level = "measure"
                    status = "passed"
                    is_archived = False
                    __alert_input_values__ = []
                    query_input = (
                        str(uuid4()),
                        run_id,
                        config.get("airflow_run_id"),
                        unique_id,
                        str(current_value),
                        level,
                        status,
                        connection_id,
                        measure_id,
                        config.get("organization_id"),
                        total_count,
                        valid_count,
                        invalid_count,
                        valid_percentage,
                        invalid_percentage,
                        score,
                        is_archived,
                        is_auto,
                        message,
                        drift_threshold,
                        measure_status,
                        executed_query,
                        allow_score,
                        is_drift_enabled,
                        True,
                        True,
                        False,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals},CURRENT_TIMESTAMP)",
                        query_input,
                    ).decode("utf-8")

                    alert_input_values.append(query_param)

                    drift_alerts_input = split_queries(alert_input_values)
                    for input_values in drift_alerts_input:
                        try:
                            query_input = ",".join(input_values)
                            attribute_insert_query = f"""
                                insert into core.metrics (id, run_id, airflow_run_id, measure_name, value,
                                level,status, connection_id, measure_id, organization_id,total_count,
                                valid_count, invalid_count, valid_percentage, invalid_percentage, score, is_archived, 
                                is_auto, message, threshold, drift_status, query, allow_score, is_drift_enabled, is_measure,
                                is_active, is_delete, created_date)
                                values {query_input}
                                RETURNING id
                            """
                            __cursor = execute_query(
                                connection, cursor, attribute_insert_query
                            )
                            drift_alerts_id = fetchall(__cursor)
                        except Exception as e:
                            log_error("check alerts: inserting new alerts", e)
                            raise e

    # update the last run details for the measure
    last_run_update_query = f"""
        update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP
        where id='{measure_id}'
        """
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, last_run_update_query)

    last_runs_measures.update({measure_id: (run_result_priority, run_id)})
    update_comparison_last_run_alerts(config)
    update_last_runs_for_comparison_measure(config, last_runs_measures)
    send_event(config, run_id, measure_id)


def run_custom_comparison(config: dict, measure_id: str):
    connection = get_postgres_connection(config)
    run_id = config.get("run_id")
    connection_id = config.get("connection_id")
    message = ""
    last_runs_measures = {}
    run_result_priority = "Ok"
    measure = config.get("measure")
    # Get comparison metrics values from the backend which the user has created"""
    metrics = get_comparison_metrics(connection, measure_id)
    base_measure_id = metrics.get("id")
    is_count_only = True
    executed_query = ""
    allow_score = True
    is_drift_enabled = measure.get("is_drift_enabled", "")
    failed_rows_limit = (
        config.get("dag_info", {})
        .get("settings", {})
        .get("reporting", {})
        .get("count", {})
    )
    failed_rows_limit = int(failed_rows_limit) if failed_rows_limit else 0
    response = get_measure_properties(connection, base_measure_id)
    properties = response.get("properties", {})
    properties = properties if properties else {}
    comparison_category = properties.get("comparison_category", "")
    response = properties.get("customcomparison", {})
    response = [response] if response else []

    drift_threshold = {
        "lower_threshold": 0,
        "upper_threshold": 0,
    }
    for measure_group in response:
        source_properties = measure_group.get("source", "")
        target_properties = measure_group.get("target", "")
        source_query = source_properties.get("query", "")
        source_connection = source_properties.get("connection", {})
        source_connection = source_connection if source_connection else {}
        target_connection = target_properties.get("connection", {})
        target_connection = target_connection if target_connection else {}

        source_name = source_connection.get("label", "")
        source_connection_id = source_connection.get("id", "")
        source_type = source_connection.get("type", "")
        target_connection_id = target_connection.get("id", "")
        target_query = target_properties.get("query", "")
        target_type = target_connection.get("type", "")
        target_name = target_connection.get("label", "")

        source_connection_detail = None
        target_connection_detail = None
        if comparison_category == "attribute":
            source_table_name = properties.get("source_table_name", "")
            target_table_name = properties.get("target_table_name", "")
            selected_source_attributes = properties.get("source_attributes", [])
            selected_target_attributes = properties.get("target_attributes", [])

            source_attributes = source_properties.get("attributes", [])
            source_attribute = source_attributes[0] if source_attributes else {}

            target_attributes = target_properties.get("attributes", [])
            target_attribute = target_attributes[0] if target_attributes else {}

            source_name = source_attribute.get("name", "")
            source_name = source_name.split(".")[0] if source_name else ""
            source_connection_id = source_attribute.get("connection_id", "")
            source_connection_detail = get_connection_detail(
                connection, source_connection_id
            )
            source_connection_detail = (
                source_connection_detail if source_connection_detail else {}
            )
            source_type = source_connection_detail.get("type", "")
            selected_source_attributes = get_attribute_names(
                source_type, selected_source_attributes
            )
            source_attribute_query = (
                ", ".join(selected_source_attributes)
                if selected_source_attributes
                else "*"
            )
            source_query = f"SELECT {source_attribute_query} FROM {source_table_name}"
            if source_attribute_query != "*":
                source_query = f"{source_query} ORDER BY {source_attribute_query} ASC"

            target_name = target_attribute.get("name", "")
            target_name = target_name.split(".")[0] if target_name else ""
            target_connection_id = target_attribute.get("connection_id", "")
            target_connection_detail = get_connection_detail(
                connection, target_connection_id
            )
            target_connection_detail = (
                target_connection_detail if target_connection_detail else {}
            )
            target_type = target_connection_detail.get("type", "")
            selected_target_attributes = get_attribute_names(
                target_type, selected_target_attributes
            )
            target_attribute_query = (
                ", ".join(selected_target_attributes)
                if selected_target_attributes
                else "*"
            )
            target_query = f"SELECT {target_attribute_query} FROM {target_table_name}"
            if target_attribute_query != "*":
                target_query = f"{target_query} ORDER BY {target_attribute_query} ASC"

        source_count, _ = execute_native_query(
            config, source_query, is_count_only=is_count_only
        )
        source_details, _ = execute_native_query(config, source_query, is_list=True)
        if not target_connection_detail:
            target_connection_detail = get_connection_detail(
                connection, target_connection_id
            )
            target_connection_detail = (
                target_connection_detail if target_connection_detail else {}
            )
        target_connection.update(
            {"credentials": target_connection_detail.get("credentials", {})}
        )
        config.update(
            {
                "connection": target_connection,
                "connection_id": target_connection_id,
                "connection_type": target_type,
            }
        )
        target_count, _ = execute_native_query(
            config, target_query, is_count_only=is_count_only
        )
        target_details, _ = execute_native_query(config, target_query, is_list=True)
        if not source_connection_detail:
            source_connection_detail = get_connection_detail(
                connection, source_connection_id
            )
            source_connection_detail = (
                source_connection_detail if source_connection_detail else {}
            )
        source_connection.update(
            {"credentials": source_connection_detail.get("credentials", {})}
        )
        config.update(
            {
                "connection": source_connection,
                "connection_id": source_connection_id,
                "connection_type": source_type,
            }
        )

        df_source = pd.DataFrame(source_details)
        df_target = pd.DataFrame(target_details)

        source_columns = list(df_source.columns)
        target_columns = list(df_target.columns)

        df_source["composite_key"] = (
            df_source[source_columns].astype(str).agg("_".join, axis=1)
        )
        df_target["composite_key"] = (
            df_target[target_columns].astype(str).agg("_".join, axis=1)
        )

        # Sort the data by composite_key
        df_source = df_source.sort_values(by="composite_key").reset_index(drop=True)
        df_target = df_target.sort_values(by="composite_key").reset_index(drop=True)

        source_keys = set(df_source["composite_key"])
        target_keys = set(df_target["composite_key"])

        matched_keys = source_keys & target_keys
        matched_keys = set(matched_keys)
        df_source["is_matched"] = df_source["composite_key"].isin(matched_keys)
        df_target["is_matched"] = df_target["composite_key"].isin(matched_keys)

        matched_source = df_source[df_source["is_matched"] == True]
        matched_target = df_target[df_target["is_matched"] == True]
        unmatched_source = df_source[df_source["is_matched"] == False]
        unmatched_target = df_target[df_target["is_matched"] == False]
        matched_data_count = len(matched_source)

        valid_count = matched_data_count
        difference_count = source_count - valid_count if source_count else 0

        final_source_rows = json.loads(
            unmatched_source.replace({np.nan: None})
            .head(failed_rows_limit)
            .to_json(orient="records", date_format="iso")
        )

        final_target_rows = json.loads(
            unmatched_target.replace({np.nan: None})
            .head(failed_rows_limit)
            .to_json(orient="records", date_format="iso")
        )

        # If less than the limit, fill in with matched row
        if len(final_source_rows) < failed_rows_limit:
            remaining = failed_rows_limit - len(final_source_rows)
            extra_source_rows = matched_source.head(remaining).to_dict(orient="records")
            final_source_rows.extend(extra_source_rows)

        if len(final_target_rows) < failed_rows_limit:
            remaining = failed_rows_limit - len(final_target_rows)
            extra_target_rows = matched_target.head(remaining).to_dict(orient="records")
            final_target_rows.extend(extra_target_rows)

        # Prepare final failed_rows JSON
        failed_rows_json = {
            "source": final_source_rows,
            "target": final_target_rows,
        }
        failed_rows_str = json.dumps(
            failed_rows_json,
            default=lambda x: float(x) if isinstance(x, Decimal) else x
        )

        drift_threshold.update(
            {
                "lower_threshold": source_count,
                "upper_threshold": target_count,
                "source_count": source_count,
                "target_count": target_count,
            }
        )
        drift_threshold = json.dumps(drift_threshold)
        measure_status = None
        unique_id = f"""{source_name}||{target_name}||{measure.get("name", "")}"""
        is_positive = measure.get("is_positive", False)
        weightage = measure.get("weightage", 100)
        weightage = int(weightage) if weightage else 100
        score = None
        is_archived = False
        if source_count and allow_score:
            valid_count = valid_count
            valid_count = valid_count if is_positive else (source_count - valid_count)
            invalid_count = source_count - valid_count
            valid_percentage = float(valid_count / source_count * 100)
            invalid_percentage = float(100 - valid_percentage)
            score = valid_percentage
            score = calculate_weightage_score(score, weightage)
            score = 100 if score > 100 else score
            score = 0 if score < 0 else score

    with connection.cursor() as cursor:
        # Delete metrics for the same run id
        delete_metrics(
            config,
            run_id=run_id,
            measure_id=measure_id,
        )
        alert_input_values = []
        for measure_group in response:
            is_auto = False
            total_count = source_count
            valid_count = valid_count
            invalid_count = difference_count
            valid_percentage = 100
            invalid_percentage = 0
            score
            level = "measure"
            status = "passed"
            is_archived = False
            __alert_input_values__ = []
            query_input = (
                str(uuid4()),
                run_id,
                config.get("airflow_run_id"),
                unique_id,
                str(difference_count),
                level,
                status,
                connection_id,
                measure_id,
                config.get("organization_id"),
                total_count,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                is_archived,
                is_auto,
                message,
                drift_threshold,
                measure_status,
                executed_query,
                allow_score,
                is_drift_enabled,
                True,
                True,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")

            alert_input_values.append(query_param)

        drift_alerts_input = split_queries(alert_input_values)
        for input_values in drift_alerts_input:
            try:
                query_input = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.metrics (id, run_id, airflow_run_id, measure_name, value,
                    level,status, connection_id, measure_id, organization_id,total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, is_archived, 
                    is_auto, message, threshold, drift_status, query, allow_score, is_drift_enabled, is_measure,
                    is_active, is_delete, created_date)
                    values {query_input}
                    RETURNING id
                """
                __cursor = execute_query(connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("check alerts: inserting new alerts", e)
                raise e

    # update the last run details for the measure
    last_run_update_query = f"""
        update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP
        where id='{measure_id}'
        """
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, last_run_update_query)

    with connection.cursor() as cursor:
        # update failed_rows for measure
        unmatched_records_query = f"""
            update core.measure set failed_rows = %s
            where id = %s
        """
        cursor = execute_query(
            connection, cursor, unmatched_records_query, (failed_rows_str, measure_id)
        )

    last_runs_measures.update({measure_id: (run_result_priority, run_id)})
    update_comparison_last_run_alerts(config)
    update_last_runs_for_comparison_measure(config, last_runs_measures)
    check_alerts(config)
    update_threshold(config)


def get_catalogschedule_tasks(config: dict):
    """
    Returns the task defails for the metadata job
    """
    task_details = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, CATALOG_SCHEDULE)
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with metadata as (
                select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
                det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
                det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
                que.connection_id, que.job_input, que.airflow_config
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                where det.category='{CATALOG_SCHEDULE}'
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                order by que.created_date desc
                limit {task_limit}
            )
            select * from metadata
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        task_details = task_details if task_details else []
        return task_details


def get_metadata_tasks(config: dict):
    """
    Returns the task defails for the metadata job
    """
    task_details = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, METADATA)
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with metadata as (
                select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
                det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
                det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
                que.connection_id, que.job_input, que.airflow_config,
                con.id as connection_id, con.airflow_connection_id as source_connection_id, con.type as connection_type,
                row_to_json(con.*) as connection
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                left join core.connection as con on con.id = que.connection_id
                where det.category='{METADATA}'
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                order by que.created_date desc
                limit {task_limit}
            )
            select * from metadata
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        task_details = task_details if task_details else []
        return task_details

def get_process_tasks(config: dict):
    """
    Returns the task defails for the process job
    """
    task_details = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, PROCESS)
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with process as (
                select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
                det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
                det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
                que.job_input, que.airflow_config, process.id as process_id, process.name as process_name,
                con.id as connection_id, con.airflow_connection_id as source_connection_id, con.type as connection_type,
                row_to_json(con.*) as connection
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                join core.process on process.id=que.process_id
                join core.connection as con on con.id = process.connection_id
                where det.category='{PROCESS}'
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                order by que.created_date desc
                limit {task_limit}
            )
            select * from process
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        task_details = task_details if task_details else []
        return task_details


def get_usage_query_update_tasks(config: dict):
    """
    Returns the task defails for the usage query job
    """
    task_details = []
    task_limit, retry_delay, category_task_limit = get_airflow_task_limit(config, USAGE_QUERY)
    if category_task_limit and category_task_limit > 0:
        task_limit = category_task_limit
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with metadata as (
                select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
                det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
                det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
                que.connection_id, que.job_input, que.airflow_config,
                con.id as connection_id, con.airflow_connection_id as source_connection_id, con.type as connection_type,
                row_to_json(con.*) as connection
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                join core.connection as con on con.id = que.connection_id
                where det.category='{USAGE_QUERY}'
                and (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and EXTRACT(EPOCH FROM (current_timestamp - det.end_time)) >= {retry_delay}))
                order by que.created_date desc
                limit {task_limit}
            )
            select * from metadata
        """
        cursor = execute_query(connection, cursor, query_string)
        task_details = fetchall(cursor)
        task_details = task_details if task_details else []
        return task_details


def get_observe_tasks(config: dict):
    """
    Returns the task defails for the observe job
    """
    tasks = []
    task_limit, retry_delay, _ = get_airflow_task_limit(config)
    status_list = [ScheduleStatus.Pending.value, ScheduleStatus.Running.value]
    status_to_check = [
        f"""'{str(status_value).lower()}'""" for status_value in status_list
    ]
    status_to_check = ", ".join(status_to_check)
    status_to_check = f"({status_to_check})"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with filtered_tasks as (
                select det.id, det.queue_id, que.created_date as que_date, det.created_date as task_date, det.status, det.is_submitted
                from core.request_queue_detail as det
                join core.request_queue as que ON que.id = det.queue_id
                left join core.connection as con on con.id = que.connection_id
                where 
                (lower(det.status) in {status_to_check} or (lower(det.status) in ('{ScheduleStatus.UpForRetry.value.lower()}') and abs(EXTRACT(EPOCH FROM (current_timestamp - det.end_time))) >= {retry_delay}))
                order by que_date asc, task_date asc, is_submitted desc
            ), ordered_tasks as (
                select * from filtered_tasks where lower(status) in ('{ScheduleStatus.Running.value.lower()}')
                union all
                select * from (
					select * from filtered_tasks where lower(status) not in ('{ScheduleStatus.Running.value.lower()}')
                    order by que_date asc, task_date asc, is_submitted desc
				) as pending
            ), aggregated_tasks as (
                select * from filtered_tasks
                order by is_submitted desc
                limit {task_limit}
            )
            select det.id as queue_detail_id, det.queue_id, det.category, det.status, det.attempt, det.task_id,
            det.try_number, det.created_date as task_date, que.created_date as que_date, det.attribute_id, det.measure_id,
            det.is_triggered, det.task_config, que.level, que.attributes, que.measures, que.asset_id,
            que.connection_id, que.job_input, que.airflow_config, con.organization_id as organization_id,
            con.airflow_connection_id as source_connection_id, con.type as connection_type, row_to_json(con.*) as connection
            from core.request_queue_detail as det
            join aggregated_tasks as task ON task.id = det.id
            join core.request_queue as que ON que.id = task.queue_id
            join core.connection as con on con.id = que.connection_id
            where det.id in (select id from aggregated_tasks) and det.category='observe'
            order by det.created_date asc, det.is_submitted desc
        """
        cursor = execute_query(connection, cursor, query_string)
        tasks = fetchall(cursor)
    return tasks


def get_connection_detail(connection: object, connection_id):
    # Returns the list of latest comparison measure properties
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select type, credentials from core.connection
                where id='{connection_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            response = fetchone(cursor)
        return response
    except Exception as e:
        raise e


def check_workflow_task_status(config: dict):
    """
    Retruns true if task is already completed, false otherwise.
    """
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    is_completed = False
    with connection.cursor() as cursor:
        query_string = f"""
            select status from core.workflow_execution
            where id='{queue_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        task_status = fetchone(cursor)
        task_status = task_status.get("status") if task_status else ""
        is_completed = bool(
            task_status
            not in [
                ScheduleStatus.Pending.value,
                ScheduleStatus.Running.value,
                ScheduleStatus.UpForRetry.value,
            ]
        )
    return is_completed

def clear_duplicate_task(config: dict, category: str):
    """
    Delete duplicate tasks in the request queue detail table based on category and queue_id.
    """
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        delete_query = f"""
            DELETE FROM core.request_queue_detail
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                        ROW_NUMBER() OVER (
                            PARTITION BY category, queue_id
                            ORDER BY created_date ASC
                        ) AS rn
                    FROM core.request_queue_detail
                    WHERE category = '{category}'
                    AND queue_id = '{queue_id}'
                ) sub
                WHERE rn > 1
            );
        """
        cursor = execute_query(connection, cursor, delete_query)