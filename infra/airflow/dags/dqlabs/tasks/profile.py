import json
from concurrent.futures import ThreadPoolExecutor
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.extract_workflow import (
    get_queries,
    is_deprecated,
    deprecate_asset,
    deprecate_attributes,
    update_recent_run_alert,
)
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
    get_active_profile_measures_count,
)
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.profile import (
    get_health_metrics,
    run_deep_profile,
)
from dqlabs.utils.extract_workflow import get_selected_attributes
from dqlabs.tasks.health import extract_health_measures
from dqlabs.app_constants.dq_constants import PROFILE, DEPRECATED_ASSET_ERROR
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.tasks.scoring import update_scores
from dqlabs.app_helper.dag_helper import (
    get_native_connection,
)
from dqlabs.app_helper.dq_helper import get_max_workers, check_is_direct_query
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.log_helper import log_error


def run_profile(config: dict, **kwargs):
    """
    Run failed rows extraction task
    """
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return

    level = config.get("level")
    connection_type = config.get("connection_type")
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
    profile_settings = profile_settings if profile_settings else {}
    profile_settings = (
        json.loads(profile_settings, default=str)
        if isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    config.update({"profile_settings": profile_settings})
    check_is_direct_query(config)

    task_config = get_task_config(config, kwargs)
    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    is_profiling_enabled = profile_settings.get("is_active")
    if not is_profiling_enabled:
        raise Exception(
            f"Please trun on profiling under settings -> measures -> profiling -> enable profiling to enable profiling...!"
        )
    
    asset = config.get("asset")
    asset = asset if asset else {}
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

    default_queries = get_queries(config)
    selected_attributes = config.get("attributes", [])
    selected_attributes = selected_attributes if selected_attributes else []
    selected_attributes = [
        attribute.get("id") for attribute in selected_attributes if attribute
    ]

    attribute = config.get("attribute", {})
    attribute = attribute if attribute else {}
    attribute_id = attribute.get("id")
    selected_attribute_id = attribute_id if level == "attribute" else ""

    attribute = get_selected_attributes(config, selected_attribute_id , 'profile')
    attributes = [attribute] if attribute and isinstance(attribute, dict) else attribute

    # If no attributes are selected or not enabled for profile, return
    if not attributes:
        log_error("No active attributes are found to process", None)
        update_queue_detail_status(
            config, ScheduleStatus.Completed.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        return

    attributes = [
        attr
        for attr in attributes
        if attr
        and str(attr.get("datatype") or "").lower()
        not in ["geometry", "geography", "vector"]
    ]

    unsupported_types = ["varbinary", "image", "bindata"]
    skipped_attributes = [
        attr for attr in attributes if str(attr.get("datatype", "")).lower() in unsupported_types
    ]
    for attr in skipped_attributes:
        log_error(f"Skipping unsupported datatype: {attr.get('datatype')} for attribute {attr.get('name')}", None)
    attributes = [
        attr for attr in attributes if str(attr.get("datatype", "")).lower() not in unsupported_types
    ]
    attributes = attributes if attributes else []
    if selected_attributes:
        attributes = list(
            filter(
                lambda attribute: attribute.get("id") in selected_attributes, attributes
            )
        )
    if selected_attribute_id:
        attributes = list(
            filter(
                lambda attribute: attribute.get("id") == selected_attribute_id,
                attributes,
            )
        )

    if connection_type not in [
        ConnectionType.S3Select.value,
        ConnectionType.File.value,
        ConnectionType.ADLS.value,
        ConnectionType.S3.value
    ]:
        is_asset_deprecated, deprecated_attributes = is_deprecated(
            config, default_queries, attributes
        )

        if is_asset_deprecated:
            deprecate_asset(config)
            raise Exception(DEPRECATED_ASSET_ERROR)

        if deprecated_attributes:
            deprecated_attribute_ids = [
                attribute.get("id") for attribute in deprecated_attributes if attribute
            ]
            deprecate_attributes(config, deprecated_attribute_ids)

            # Filter deprecated attributes and the attributes which are not enabled for profile
            selected_attributes = list(
                filter(
                    lambda attribute: attribute.get("id")
                    not in deprecated_attribute_ids,
                    attributes,
                )
            )
            config.update({"attributes": selected_attributes})

    if level == "attribute" and selected_attribute_id and not selected_attributes:
        raise Exception(DEPRECATED_ASSET_ERROR)

    if not attributes:
        log_error("No active attributes are found to process!", None)
        update_queue_detail_status(
            config, ScheduleStatus.Completed.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Completed.value, True)
        return

    is_attribute_level = len(selected_attributes) > 0
    config.update(
        {
            "is_attribute_level": is_attribute_level,
            "selected_attributes": selected_attributes,
        }
    )
    health, advanced_profile = get_active_profile_measures_count(config)

    source_connection = None
    if health > 0:
        # run health measures
        print("Started Basic Profiling")
        source_connection = extract_health_measures(config, **kwargs)
        print("Completed Basic Profiling")

    # run profile measures
    if advanced_profile > 0:
        print("Started Advanced Profiling")
        if not source_connection:
            source_connection = (
                get_native_connection(config)
                if connection_type.lower()
                not in [
                    ConnectionType.Redshift.value,
                    ConnectionType.Redshift_Spectrum.value,
                ]
                else None
            )
        source_connection = (
            source_connection
            if source_connection
            and connection_type.lower()
            in [ConnectionType.Snowflake.value, ConnectionType.Denodo.value]
            else None
        )

        health_metrics = get_health_metrics(config)
        health_metrics_dict = {}
        for metric in health_metrics:
            attribute_id = metric.get("attribute_id")
            if not attribute_id:
                continue

            if attribute_id not in health_metrics_dict:
                health_metrics_dict[attribute_id] = {"attribute_id": attribute_id}
            health_metrics_dict[attribute_id].update(
                {metric.get("name"): metric.get("value")}
            )

            if attribute_id not in health_metrics_dict:
                health_metrics_dict[attribute_id] = {"attribute_id": attribute_id}
            health_metrics_dict[attribute_id].update(
                {metric.get("name"): metric.get("value")}
            )
        # populate values frequency for each attribute
        results = []
        max_workers = get_max_workers(ConnectionType.Denodo)
        total_attributes = len(attributes) if attributes else 0
        max_workers = (
            total_attributes
            if total_attributes and total_attributes < max_workers
            else max_workers
        )
        executor = ThreadPoolExecutor(max_workers=max_workers)
        if executor:
            futures = [
                executor.submit(
                    run_deep_profile,
                    config,
                    default_queries,
                    attribute,
                    health_metrics,
                    source_connection,
                    health_metrics_dict
                )
                for attribute in attributes
            ]
            results = [future.result() for future in futures]
            executor.shutdown()

        error_messages = []
        for result in results:
            attribute_id, error = result

            if error and "The multi-part identifier" not in error :
                error_messages.append(f"attribute_id - {attribute_id} - Error: {error}")

        print("Completed Advanced Profiling")
        print("Started to update scores")
        update_scores(config)
        # Check for the alerts for the current run
        config.update({"is_asset": False, "job_type": PROFILE, "measure_id": None})
        check_alerts(config)

        # # Update the threshold value based on the drift config
        print("Started to update threshold")
        update_threshold(config)
        print("Update threshold completed")
        if error_messages:
            error_message = "\n".join(error_messages)
            is_triggered = config.get("is_triggered")
            if not is_triggered:
                create_queue_detail(config)
            raise Exception(
                f"Profiling failed for the following attributes - {error_message}"
            )

    is_triggered = config.get("is_triggered")
    if not is_triggered:
        print("Create queue detail - Started")
        create_queue_detail(config)
        print("Create queue detail - Completed")

    print("Update the current task details")
    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)
    print("Started to update recent run alerts")
    update_recent_run_alert(config, attributes)
    print("Updated recent run alerts")
