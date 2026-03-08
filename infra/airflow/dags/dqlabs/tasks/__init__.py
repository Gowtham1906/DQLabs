import json
from airflow.operators.python_operator import PythonOperator
from functools import partial

from dqlabs.app_helper.dq_helper import get_attribute_label
from dqlabs.utils import on_task_failed
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_constants.dq_constants import (
    MAX_RETRY_DURATION,
    RELIABILITY,
    PROFILE,
    STATISTICS,
    BEHAVIORAL,
    CUSTOM,
    SEMANTICS,
    EXPORT_FAILED_ROWS,
    CATALOG_UPDATE,
    NOTIFICATION,
    USERACTIVITY,
    SYNCASSET,
    METADATA,
    CATALOG_SCHEDULE,
    USAGE_QUERY,
    SPARK,
    OBSERVE,
    WORKFLOW,
    CATALOG_DISABLED_ERROR,
    REPORTING_DISABLED_ERROR,
    SEMANTICS_DISABLED_ERROR,
    PROCESS,
    EXTRACT,
    COST,
    PERFORMANCE
)

# import tasks

from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    update_task_submitted_status,
)

from dqlabs.enums.schedule_types import ScheduleStatus


def __get_task(category: str, connection_type: str, asset_type: str, credentials: dict):
    """
    Get task method for the execution
    """
    if category == RELIABILITY:
        if connection_type == ConnectionType.Tableau.value:
            from dqlabs.tasks.connector.tableau import extract_tableau_data

            return extract_tableau_data
        if connection_type == ConnectionType.Sigma.value:
            from dqlabs.tasks.connector.sigma import extract_sigma_data

            return extract_sigma_data
        if connection_type == ConnectionType.Dbt.value:
            from dqlabs.tasks.connector.dbt import extract_dbt_data

            return extract_dbt_data
        if connection_type == ConnectionType.Fivetran.value:
            from dqlabs.tasks.connector.fivetran import extract_fivetran_data

            return extract_fivetran_data
        if connection_type == ConnectionType.ADF.value:
            from dqlabs.tasks.connector.adf import extract_adf_data

            return extract_adf_data
        if (
            connection_type == ConnectionType.Databricks.value
            and str(asset_type).lower() == "pipeline"
        ):
            from dqlabs.tasks.connector.databricks_pipeline import (
                extract_databricks_pipeline_data,
            )

            return extract_databricks_pipeline_data
        if connection_type == ConnectionType.Airflow.value:
            from dqlabs.tasks.connector.airflow import extract_airflow_metadata

            return extract_airflow_metadata
        if connection_type == ConnectionType.Talend.value:
            from dqlabs.tasks.connector.talend import extract_talend_data

            return extract_talend_data
        if connection_type == ConnectionType.PowerBI.value:
            from dqlabs.tasks.connector.powerbi import extract_powerbi_data

            return extract_powerbi_data
        if connection_type == ConnectionType.Wherescape.value:
            from dqlabs.tasks.connector.wherescape import extract_wherescape_data

            return extract_wherescape_data
        if connection_type == ConnectionType.Snowflake.value and str(
            asset_type
        ).lower() in ["task", "pipe", "stored procedure"]:
            from dqlabs.tasks.connector.snowflake import extract_snowflake_pipeline

            return extract_snowflake_pipeline
        
        if connection_type == ConnectionType.Coalesce.value and str(
            asset_type
        ).lower() in ["pipeline"]:
            from dqlabs.tasks.connector.coalesce import extract_coalesce_nodes
            return extract_coalesce_nodes
        if connection_type == ConnectionType.Synapse.value and str(
            asset_type
        ).lower() in ["pipeline"]:
            from dqlabs.tasks.connector.synapse import extract_synapse_pipeline

            return extract_synapse_pipeline
        if (
            connection_type == ConnectionType.SalesforceMarketing.value
            and credentials.get("connection_type", "") == "api"
        ):
            from dqlabs.tasks.connector.salesforce_marketing import extract_salesforce_marketing_data
            return extract_salesforce_marketing_data
        
        if (
            connection_type == ConnectionType.SalesforceDataCloud.value
            and str(asset_type).lower() in ["pipeline"]
        ):
            from dqlabs.tasks.connector.salesforce_data_cloud import extract_salesforce_data_cloud
            return extract_salesforce_data_cloud
        
        if (
            connection_type == ConnectionType.SalesforceDataCloud.value
            and str(asset_type).lower() in ["reports","dashboards"]
        ):
            from dqlabs.tasks.connector.salesforce_data_cloud import extract_salesforce_data_cloud_reports
            return extract_salesforce_data_cloud_reports
        if connection_type == ConnectionType.Airbyte.value:
            from dqlabs.tasks.connector.airbyte import extract_airbyte_data
            return extract_airbyte_data
        else:
            from dqlabs.tasks.reliability import extract_table_metadata

            return extract_table_metadata
    elif category == PROFILE:
        from dqlabs.tasks.profile import run_profile

        return run_profile
    elif category in [CUSTOM, STATISTICS, BEHAVIORAL]:
        from dqlabs.tasks.extract_advanced_measures import extract_measure

        return extract_measure
    elif category == SEMANTICS:
        from dqlabs.tasks.semantic_analysis import run_semantic_analysis

        return run_semantic_analysis
    elif category == OBSERVE:
        from dqlabs.tasks.observe import extract_observe_measures

        return extract_observe_measures
    elif category == EXPORT_FAILED_ROWS:
        from dqlabs.tasks.export_failed_rows import extract_failed_rows

        return extract_failed_rows
    elif category == CATALOG_UPDATE:
        from dqlabs.tasks.catalog_update import run_catalog_update

        return run_catalog_update
    elif category == NOTIFICATION:
        from dqlabs.tasks.notifications import handle_notification_tasks

        return handle_notification_tasks
    elif category == USERACTIVITY:
        from dqlabs.tasks.user_activity import run_useractivity

        return run_useractivity
    elif category == CATALOG_SCHEDULE:
        from dqlabs.tasks.catalog_schedule import run_catalog_schedule

        return run_catalog_schedule
    elif category == SYNCASSET:
        from dqlabs.tasks.sync_asset import sync_connection_assets

        return sync_connection_assets
    elif category == METADATA:
        from dqlabs.tasks.metadata import run_push_metadata

        return run_push_metadata
    elif category == USAGE_QUERY:
        from dqlabs.utils.usage import save_usage_queries_info

        return save_usage_queries_info
    elif category == WORKFLOW:
        from dqlabs.tasks.workflow import manage_workflow_tasks

        return manage_workflow_tasks
    elif category == PROCESS:
        from dqlabs.tasks.process import execute_process
        return execute_process
    elif category == EXTRACT:
        if connection_type == ConnectionType.Dbt.value:
            from dqlabs.tasks.connector.dbt_core import extract_dbt_core
            return extract_dbt_core
        elif connection_type == ConnectionType.Airflow.value:
            from dqlabs.tasks.connector.airflow_plugin import extract_airflow
            return extract_airflow
    else:
        return None


def create_tasks(dag_info: dict, category: str):
    """
    Create airflow tasks for the given category
    """

    from dqlabs.utils.tasks import (
        get_tasks_by_category,
        get_semantic_task_details,
        get_notification_task_details,
        get_metadata_tasks,
        get_catalogschedule_tasks,
        get_usage_query_update_tasks,
        get_observe_tasks,
        get_workflow_task_details,
        get_process_tasks
    )

    core_connection_id = dag_info.get("core_connection_id")
    task_details = []

    if category == SEMANTICS:
        task_details = get_semantic_task_details(dag_info)
    elif category == NOTIFICATION:
        task_details = get_notification_task_details(dag_info)
    elif category == METADATA:
        task_details = get_metadata_tasks(dag_info)
    elif category == CATALOG_SCHEDULE:
        task_details = get_catalogschedule_tasks(dag_info)
    elif category == USAGE_QUERY:
        task_details = get_usage_query_update_tasks(dag_info)
    elif category == OBSERVE:
        task_details = get_observe_tasks(dag_info)
    elif category == WORKFLOW:
        task_details = get_workflow_task_details(dag_info)
    elif category == PROCESS:
        task_details = get_process_tasks(dag_info)
    else:
        task_details = get_tasks_by_category(dag_info, category)

    tasks = []
    for task in task_details:
        level = task.get("level")
        asset_id = task.get("asset_id")
        attribute_id = task.get("attribute_id")
        if level in ["asset", "attribute"] and (not asset_id):
            continue

        attribute = task.get("attribute")
        connection_type = task.get("connection_type")
        queue_detail_id = task.get("queue_detail_id")
        asset_name = task.get("technical_name")
        job_input = task.get("job_input")
        job_input = job_input if job_input else {}
        input_config = job_input.get("input_config")
        input_config = input_config if input_config else {}
        asset_type = input_config.get("type", "")
        primary_columns = task.get("primary_columns")
        row_count = task.get("row_count")
        connection = task.get("connection", {})
        connection = connection if connection else {}
        asset = task.get("asset", {})
        asset = asset if asset else {}
        asset.update({"primary_columns": primary_columns, "row_count": row_count})

        keys_to_remove = ["job_input"]
        for key in keys_to_remove:
            if key in task:
                del task[key]

        input_config.update(
            {
                **task,
                "dag_category": category,
                "run_id": task.get("queue_id"),
                "asset": asset,
                "core_connection_id": core_connection_id,
                "dag_info": dag_info,
            }
        )

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
        profile_schema_name = profile_settings.get("schema")
        profile_schema_name = profile_schema_name if profile_schema_name else "DQLABS"
        profile_database_name = profile_settings.get("database")
        profile_database_name = profile_database_name if profile_database_name else ""
        enable_retry = profile_settings.get("enable_retry")
        enable_retry = enable_retry if enable_retry else None
        max_retries = profile_settings.get("max_retries")
        max_retries = int(max_retries) if max_retries else 1
        max_retry_duration = profile_settings.get("max_retry_duration")
        max_retry_duration = int(max_retry_duration) if max_retry_duration else MAX_RETRY_DURATION
        retry_delay = profile_settings.get("max_retry_delay")
        retry_delay = int(retry_delay) if retry_delay else 15

        credentials = {}
        db_name = ""
        if connection:
            credentials = connection.get("credentials")
            credentials = (
                json.loads(credentials)
                if credentials and isinstance(credentials, str)
                else credentials
            )
            credentials = credentials if credentials else {}
            db_name = credentials.get("database")
            db_name = db_name if db_name else ""
        if asset:
            asset_type = asset.get("type")
            properties = asset.get("properties", {})
            properties = (
                json.loads(properties) if isinstance(properties, str) else properties
            )
            properties = properties if properties else {}
            database = properties.get("database")
            db_name = database if database else db_name
            if asset_type and str(asset_type).lower() == "query":
                db_name = profile_database_name if profile_database_name else db_name
        input_config.update(
            {
                "profile_asset_database": db_name,
                "profile_schema_name": profile_schema_name,
                "profile_database_name": profile_database_name,
                "max_retries": max_retries,
                "max_retry_duration": max_retry_duration,
                "retry_delay": retry_delay,
                "enable_retry": enable_retry,
            }
        )

        if attribute_id and not attribute:
            error_message = f"Could not process the unselected attribute measures."
            update_queue_detail_status(
                input_config, ScheduleStatus.Failed.value, error_message
            )
            update_queue_status(input_config, ScheduleStatus.Completed.value)
            continue

        if (
            category
            not in [
                SEMANTICS,
                NOTIFICATION,
                USERACTIVITY,
                SYNCASSET,
                METADATA,
                CATALOG_SCHEDULE,
                USAGE_QUERY,
                OBSERVE,
                WORKFLOW,
                PROCESS
            ]
            and (not asset_id or not job_input)
            and level != "measure"
        ):
            error_message = f"No job input defined for the asset {asset_id}"
            update_queue_detail_status(
                input_config, ScheduleStatus.Failed.value, error_message
            )
            update_queue_status(input_config, ScheduleStatus.Completed.value)
            continue

        task_label = get_attribute_label(asset_name)
        if category == STATISTICS:
            attribute_name = task.get("attribute_name")
            if not attribute_name:
                error_message = "Undefined attribute details"
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
            task_label = get_attribute_label(attribute_name)
        elif category in [CUSTOM, BEHAVIORAL]:
            measure_name = task.get("measure_name")
            if not measure_name:
                error_message = "Undefined measure details"
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
            task_label = get_attribute_label(measure_name)
        elif category in [NOTIFICATION]:
            channel_name = task.get("module_name")
            if not channel_name:
                error_message = "Undefined Notification details"
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
            task_label = get_attribute_label(channel_name)
        elif category in [EXPORT_FAILED_ROWS, METADATA]:
            report_settings = general_settings.get("reporting")
            report_settings = report_settings if report_settings else {}
            report_settings = (
                json.loads(report_settings, default=str)
                if isinstance(report_settings, str)
                else report_settings
            )
            is_export_enabled = report_settings.get("is_active") or report_settings.get("metadata_is_active")
            if not is_export_enabled:
                error_message = REPORTING_DISABLED_ERROR
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
            if category in [EXPORT_FAILED_ROWS] and level == "measure":
                measure_name = task.get("measure_name")
                task_label = get_attribute_label(measure_name)
            elif category in [METADATA]:
                task_label = category
        elif category in [SEMANTICS]:
            semantic_settings = general_settings.get("semantics", {})
            semantic_settings = semantic_settings if semantic_settings else {}
            semantic_settings = (
                json.loads(semantic_settings, default=str)
                if isinstance(semantic_settings, str)
                else semantic_settings
            )
            is_semantic_enabled = semantic_settings.get("is_active")
            if not is_semantic_enabled:
                error_message = SEMANTICS_DISABLED_ERROR
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
        elif category == WORKFLOW:
            workflow_name = task.get("workflow_name")
            queue_detail_id = task.get("queue_id")
            task_label = get_attribute_label(workflow_name)
        elif category in [CATALOG_UPDATE, CATALOG_SCHEDULE]:
            is_catalog_enabled = bool(dag_info.get("catalog", []))
            if not is_catalog_enabled:
                error_message = CATALOG_DISABLED_ERROR
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue

            task_label = category
        elif category == PROCESS:
            process_name = task.get("process_name")
            if not process_name:
                error_message = "Undefined Process details"
                update_queue_detail_status(
                    input_config, ScheduleStatus.Failed.value, error_message
                )
                update_queue_status(input_config, ScheduleStatus.Completed.value)
                continue
            task_label = get_attribute_label(process_name)
        elif category in [
            USERACTIVITY,
            SYNCASSET,
            USAGE_QUERY,
            OBSERVE,
        ]:
            task_label = category
        
        if not task_label:
            error_message = "Invalid task name defined"
            update_queue_detail_status(
                input_config, ScheduleStatus.Failed.value, error_message
            )
            update_queue_status(input_config, ScheduleStatus.Completed.value)
            continue
        task_id = f"{queue_detail_id}__{task_label.strip()}"
        task_id = task_id[:230] if task_id else ""

        task_to_run = __get_task(category, connection_type, asset_type, credentials)
        if not task_to_run or not task_id:
            error_message = "No tasks defined"
            update_queue_detail_status(
                input_config, ScheduleStatus.Failed.value, error_message
            )
            update_queue_status(input_config, ScheduleStatus.Completed.value)
            continue

        try_number = task.get("try_number")
        if try_number:
            task_id = f"{task_id}_{str(try_number)}"


        task_failure_callback = partial(on_task_failed, config=input_config)
        airflow_task = PythonOperator(
            task_id=task_id,
            python_callable=task_to_run,
            op_kwargs={"config": input_config},
            on_failure_callback=task_failure_callback,
            on_retry_callback=task_failure_callback,
        )
        tasks.append(airflow_task)
        if category != WORKFLOW:
            update_task_submitted_status(input_config, queue_detail_id, True)
    return tasks
