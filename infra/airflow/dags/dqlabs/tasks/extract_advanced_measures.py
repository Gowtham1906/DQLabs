"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

from dqlabs.utils.extract_workflow import (
    execute_measure,
    execute_measures,
    get_queries,
    get_selected_attributes,
    update_attribute_row_count,
    update_recent_run_alert,
)
from dqlabs.utils.lookup_process import execute_lookup_measure
from dqlabs.utils.grouping import execute_grouping_measure
from dqlabs.utils.file_validation import execute_file_validation_measure
from dqlabs.utils.cost_performance import execute_cost_performance_measure
from dqlabs.app_constants.dq_constants import (
    BEHAVIORAL,
    PATTERN,
    STATISTICS,
    COMPARISON,
    LOOKUP,
    GROUPING,
    FILE_VALIDATION,
    DEPRECATED_ASSET_ERROR,
    COST,
    PERFORMANCE
)
from dqlabs.utils.patterns import execute_patterns
from dqlabs.tasks.behavioral.populate_data import populate_aggregated_data
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.tasks.scoring import update_scores
from dqlabs.utils.tasks import get_task_config, check_task_status, run_comparison, run_custom_comparison
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.enums.approval_status import ApprovalStatus
from dqlabs.app_helper.dq_helper import check_is_direct_query


def extract_measure(config: dict, **kwargs) -> None:
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return

    task_config = get_task_config(config, kwargs)
    asset_status = config.get("asset_status")
    is_asset_level = config.get("level") == "asset"
    is_measure_level = config.get("level") == "measure"
    check_is_direct_query(config)

    if not is_measure_level:
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        attribute_status = attribute.get("status")
        if not is_asset_level and not attribute:
            raise Exception("Could not found attribute details")

        if (
            is_asset_level
            and asset_status
            and str(asset_status).lower() == ApprovalStatus.Deprecated.value.lower()
        ):
            raise Exception(DEPRECATED_ASSET_ERROR)

        if (
            not is_asset_level
            and attribute_status
            and str(attribute_status).lower() == ApprovalStatus.Deprecated.value.lower()
        ):
            raise Exception(DEPRECATED_ASSET_ERROR)

        attribute_id = attribute.get("id")
        selected_attribute = get_selected_attributes(config, attribute_id)
        selected_attribute = (
            selected_attribute
            if selected_attribute and isinstance(selected_attribute, dict)
            else {}
        )
        is_selected = selected_attribute.get("is_selected")
        is_active = selected_attribute.get("is_active")
        if attribute and not (is_active and is_selected):
            raise Exception("Given attribute is not an active attribute")

    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    config.update(
        {
            "airflow_run_id": str(kwargs.get("dag_run").run_id),
        }
    )
    measure = config.get("measure")
    measure_id = measure.get("id")

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

    measure_type = measure.get("type", "").lower()
    level = measure.get("level")
    category = measure.get("category", "")
    category = category.lower() if category else ""
    comparison_type = measure["properties"].get("comparisontype", "")
    comparison_type = comparison_type.lower() if comparison_type else ""

    default_queries = get_queries(config)
    config.update(
        {
            "default_queries": default_queries,
            "measure_id": measure_id,
            "measure_type": measure_type,
            "category": category,
        }
    )
    if level == "attribute":
        update_attribute_row_count(config)

    if measure_type == STATISTICS:
        execute_measures(measure_type, config, default_queries)
    elif category == BEHAVIORAL:
        populate_aggregated_data(measure, config)
    elif category == COMPARISON:
        if comparison_type == "custom":
            run_custom_comparison(config, measure_id)
        else:
            run_comparison(config, measure_id)
    elif category == LOOKUP:
        execute_lookup_measure(measure, config, default_queries)
    elif category == GROUPING:
        execute_grouping_measure(measure, config, default_queries)
    # elif category == CROSS_SOURCE:
    #     execute_cross_source_measure(measure, config, default_queries)
    elif category == FILE_VALIDATION:
        execute_file_validation_measure(measure, config)
    elif category in [COST, PERFORMANCE]:
        execute_cost_performance_measure(measure, config, default_queries)
    else:
        if category == PATTERN:
            execute_patterns(config)
        else:
            execute_measure(measure, config, default_queries)

    if category != BEHAVIORAL and category != COMPARISON:
        if not is_measure_level:
            update_scores(config)

        # Check for the alerts for the current run
        config.update(
            {
                "is_asset": level == "asset",
                "job_type": measure_type,
                "category": category,
            }
        )
        check_alerts(config)

        # # Update the threshold value based on the drift config
        update_threshold(config)

    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)

    if measure_type != STATISTICS and category != BEHAVIORAL:
        update_recent_run_alert(config)
