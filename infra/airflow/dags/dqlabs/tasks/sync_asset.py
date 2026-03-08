"""
    Migration Notes From V2 to V3:
    No Migration Changes 
"""

import json
import re

from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.log_helper import log_info, log_error
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.sync_asset import (
    update_config_details,
    get_source_assets,
    get_deprecated_dqassets,
    update_version_table,
    update_asset_table,
    get_valid_dqassets,
    update_version_history,
    update_measure_status,
    disable_duplicate_asset,
    get_databricks_source_assets,
    get_valid_dqpipe_dqreports,
    get_tableau_reports,
    get_powerbi_reports,
    get_dbt_pipelines,
    get_airflow_pipelines,
    get_talend_reports,
    update_tableau_id,
    update_powerbi_id,
    update_databricks_id,
    update_talend_id,
    get_valid_schemas,
    updated_version_userid,
    update_attribute_status
)
from dqlabs.utils.connections import get_dq_connections_database
from dqlabs.enums.connection_types import ConnectionType
import traceback

# Add regex pattern for valid schema name
pattern = r'"'


def update_reverse_deprecated_dqassets(
    config: dict, native_source_asset_list: list, connection_type: str = None
):
    """
    Reverse Deprecated:

    If an asset was already configured for a certain connection and it ran a few times in the DQ portal to collect DQ score, alerts, etc.,
    this asset was marked as deprecated as it was deleted from the source. However, it was re-configured again in the source.
    We need to re-configure this asset to the old asset ID and remove the deprecated status from it.

    Parameters:
        Check if deprecated dq_asset is present in the source valid datasources
        config: A dictionary containing configuration information.
        source_asset_list: A list of dictionaries representing source assets.
    """
    try:
        
        if native_source_asset_list:
            log_info(f"Sample native source asset: {native_source_asset_list[0] if len(native_source_asset_list) > 0 else 'No assets'}")
            log_info(f"Native source asset keys: {list(native_source_asset_list[0].keys()) if native_source_asset_list and len(native_source_asset_list) > 0 else 'No assets'}")
        
        # Get the source assets - handle Snowflake field names
        source_asset_list = []
        for asset in native_source_asset_list:
            # For Snowflake, use table_name instead of name
            if "table_name" in asset:
                asset_name = asset.get("table_name", "").lower()
            else:
                asset_name = asset.get("name", "").lower()
            if asset_name:  # Only add non-empty names
                source_asset_list.append(asset_name)
        log_info(f"Extracted source asset names: {source_asset_list}")

        # Get all valid deprecated assets for the connection
        deprecated_assets = get_deprecated_dqassets(config)
        if deprecated_assets:
            for dq_asset in deprecated_assets:
                dq_asset_name = str(dq_asset.get("name")).lower()
                valid_schema = dq_asset.get("schema", "")
                if valid_schema:
                    valid_schema = str(valid_schema)
                    valid_schema = re.sub(pattern, "", valid_schema)
                    source_list = list(
                        filter(
                            lambda schema_entry: (schema_entry.get("schema", "") or schema_entry.get("table_schema", "")).lower()
                            == valid_schema.lower(),
                            native_source_asset_list,
                        )
                    )
                    # Handle Snowflake field names for filtered assets
                    source_asset_list = []
                    for asset in source_list:
                        if "table_name" in asset:
                            asset_name = asset.get("table_name", "").lower()
                        else:
                            asset_name = asset.get("name", "").lower()
                        if asset_name:
                            source_asset_list.append(asset_name)
                if dq_asset_name in source_asset_list:
                    dq_asset_id = str(dq_asset.get("id"))
                    log_info(("dq_asset_id", dq_asset_id))

                    # Update the deprecated assets in the version and asset table
                    update_version_table(
                        config, dq_asset_id, dq_asset_name, reverse_deprecate=True
                    )
                    update_asset_table(
                        config, dq_asset_id, dq_asset_name, reverse_deprecate=True
                    )
                    update_attribute_status(config,
                                            dq_asset_id,
                                            reverse_deprecate=True)
                    log_info(
                        (f"""Completed Reverse deprecated of asset{dq_asset_name}""")
                    )
                    update_measure_status(config, dq_asset_id, reverse_deprecate=True)
                    log_info(
                        (
                            f"""Completed Reverse deprecated for measure of asset{dq_asset_name}"""
                        )
                    )
                    update_version_history(config, dq_asset_id, reverse_deprecate=True)
                    log_info(
                        (
                            f"""Version history updated for deprecated asset{dq_asset_name}"""
                        )
                    )

                    # Update unique_ids and view_ids for pipelines and report connections
                    if connection_type == ConnectionType.Tableau.value:
                        asset_details = [
                            item
                            for item in native_source_asset_list
                            if (item.get("name", "") or item.get("table_name", "")).lower() == dq_asset_name
                        ]
                        workbook_id = asset_details[0].get("workbook_id")
                        view_id = asset_details[0].get("view_id")
                        update_tableau_id(
                            config,
                            asset_id=dq_asset_id,
                            workbook_id=workbook_id,
                            view_id=view_id,
                        )
                    elif connection_type == ConnectionType.PowerBI.value:
                        # Access the 'properties' JSON data and update the 'report_id' field
                        asset_details = [
                            item
                            for item in native_source_asset_list
                            if (item.get("name", "") or item.get("table_name", "")).lower() == dq_asset_name
                        ]
                        workspace_id = asset_details[0].get("workspace_id")
                        report_id = asset_details[0].get("report_id")
                        update_powerbi_id(
                            config,
                            asset_id=dq_asset_id,
                            workspace_id=workspace_id,
                            report_id=report_id,
                        )
                    elif connection_type == ConnectionType.Databricks.value:
                        # Access the 'properties' JSON data and update the 'report_id' field
                        asset_details = [
                            item
                            for item in native_source_asset_list
                            if (item.get("name", "") or item.get("table_name", "")).lower() == dq_asset_name
                        ]
                        workflow_id = asset_details[0].get("workflow_id")
                        update_databricks_id(
                            config,
                            asset_id=dq_asset_id,
                            workflow_id=workflow_id,
                            asset_name=dq_asset_name,
                        )
                    elif connection_type == ConnectionType.Dbt.value:
                        # Access the 'properties' JSON data and update the 'report_id' field
                        asset_details = [
                            item
                            for item in native_source_asset_list
                            if (item.get("name", "") or item.get("table_name", "")).lower() == dq_asset_name
                        ]
                        _id = asset_details[0].get("id")
                        update_talend_id(config, asset_id=dq_asset_id, _id=_id)
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"update_reverse_deprecated_dqassets {e}")


def update_mark_dqassets_as_deprecated(config: dict, source_asset_list: list):
    """
    Update Mark DQ Assets as Deprecated:

    Marks data quality (DQ) assets as deprecated if they are no longer present in the source data.

    Parameters:
        config: A dictionary containing configuration information.
        source_asset_list: A list of dictionaries representing source assets.
    """
    if source_asset_list:
        log_info(f"Sample source asset: {source_asset_list[0] if len(source_asset_list) > 0 else 'No assets'}")
        log_info(f"Source asset keys: {list(source_asset_list[0].keys()) if source_asset_list and len(source_asset_list) > 0 else 'No assets'}")
    
    filter_source_unique_id = []

    # Get all valid dqassets and dqpipelines
    dq_valid_assets = get_valid_dqassets(config)
    try:
        if dq_valid_assets:
            # Extract unique schema from valid DQ assets
            for i, valid_asset in enumerate(dq_valid_assets):
                log_info(f"Processing valid asset {i+1}/{len(dq_valid_assets)}: {valid_asset}")
                
                valid_asset_name = str(valid_asset.get("name").lower())
                src_list_cpy = source_asset_list.copy()
                valid_schema = str(valid_asset.get("schema").lower())
                valid_schema = re.sub(pattern, "", valid_schema)

                source_list_filtered = list(
                    filter(
                        lambda schema_entry: (schema_entry.get("schema", "") or schema_entry.get("table_schema", "")).lower()
                        == valid_schema.lower(),
                        src_list_cpy,
                    )
                )
                log_info(f"Filtered source list length: {len(source_list_filtered)}")
                
                # Handle Snowflake field names for filtered assets
                base_source_list = []
                for asset in source_list_filtered:
                    if "table_name" in asset:
                        asset_name = asset.get("table_name", "").lower()
                    else:
                        asset_name = asset.get("name", "").lower()
                    if asset_name:
                        base_source_list.append(asset_name)
                log_info(f"Base source list: {base_source_list}")

                # Check if asset is not in the filtered source unique IDs
                if valid_asset_name not in base_source_list:
                    dq_asset_id = str(valid_asset.get("id").lower())
                    asset_unique_id = str(valid_asset.get("unique_id").lower())
                    # Update the deprecated assets in the version and asset table
                    update_version_table(config, dq_asset_id, asset_unique_id)
                    update_attribute_status(config,
                                            dq_asset_id)
                    log_info(
                        (
                            f"""Deprecated the asset {valid_asset.get("name")} as it is not present in the source"""
                        )
                    )
                    update_measure_status(config, dq_asset_id)
                    update_version_history(config, dq_asset_id)
                    log_info(
                        (
                            f"""Version history updated for asset{valid_asset.get("name")}"""
                        )
                    )
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"update_mark_dqassets_as_deprecated{e}")


def update_mark_dqpipe_dqreports_as_deprecated(
    config: dict, source_pipeline_list: list
):
    """
    Update Mark DQ Pipelines and as Deprecated:

    Marks data quality (DQ) assets as deprecated if they are no longer present in the source data.

    Parameters:
        config: A dictionary containing configuration information.
        source_asset_list: A list of dictionaries representing source assets.
    """
    log_info(("source_pipeline_list", source_pipeline_list))
    try:
        filter_source_pipelines = []
        filter_source_pipelines = [
            pipe.get("name").lower() for pipe in source_pipeline_list
        ]
        log_info(("filter_source_pipelines", filter_source_pipelines))

        # Get all valid dqassets and dqpipelines
        dq_valid_assets = get_valid_dqpipe_dqreports(config)
        if dq_valid_assets:
            if filter_source_pipelines:
                for valid_asset in dq_valid_assets:
                    asset_unique_id = valid_asset.get("name").lower()
                    # Check if asset is not in the filtered source unique IDs
                    if asset_unique_id not in filter_source_pipelines:
                        dq_asset_id = valid_asset.get("id")
                        # Update the deprecated assets in the version and asset table
                        update_version_table(config, dq_asset_id, asset_unique_id)
                        update_asset_table(config, dq_asset_id, asset_unique_id)
                        update_attribute_status(config,
                                                dq_asset_id)
                        log_info(
                            (
                                f"""Deprecated the asset {valid_asset.get("name")} as it is not present in the source"""
                            )
                        )
                        update_version_history(config, dq_asset_id)
                        log_info(
                            (
                                f"""Version history updated for asset{valid_asset.get("name")}"""
                            )
                        )
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"update_mark_dqpipe_dqreports_as_deprecated{e}")


def sync_connection_assets(config: dict, **kwargs):
    """
    Run user activity task
    """
    # Update the config with the connection details
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        # Get Reports Settings
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
        # Update the config with the connection details
        task_config = get_task_config(config, kwargs)

        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)

        config = update_config_details(config)
        connection_id = config.get("connection_id")
        connection = config.get("connection")
        connection = connection if connection else {}
        credentials = connection.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}
        connection_type = connection.get("type")
        source_databases = credentials.get("database")
        source_databases = (
            source_databases if source_databases else credentials.get("databases")
        )
        source_databases = (
            [source_databases]
            if source_databases and isinstance(source_databases, str)
            else source_databases
        )
        connected_database = source_databases if source_databases else []

        source_asset_list = []
        if connection_type == ConnectionType.Databricks.value:
            databricks_list = get_databricks_source_assets(config)
            asset_list = databricks_list.get("asset", [])
            pipeline_list = databricks_list.get("pipeline", [])
            true_asset_list = asset_list.copy()
            asset_list.extend(pipeline_list)
            combined_list = asset_list.copy()
            # Reverse all deprecated assets if they are re-instated in the source directory
            update_reverse_deprecated_dqassets(config, combined_list)
            # Mark asset as deprecated if they are not present in the source list
            update_mark_dqassets_as_deprecated(config, true_asset_list)
            # Mark pipelines as deprecated if they are not present in the source list
            update_mark_dqpipe_dqreports_as_deprecated(config, pipeline_list)

            update_queue_detail_status(config, ScheduleStatus.Completed.value)

        elif connection_type in [
            ConnectionType.Talend.value,
            ConnectionType.Fivetran.value,
            ConnectionType.Dbt.value,
            ConnectionType.Airflow.value,
            ConnectionType.ADF.value,
            ConnectionType.Tableau.value,
            ConnectionType.PowerBI.value,
        ]:
            if connection_type == ConnectionType.Tableau.value:
                source_asset_list = get_tableau_reports(config)
            elif connection_type == ConnectionType.PowerBI.value:
                source_asset_list = get_powerbi_reports(config)
            elif connection_type == ConnectionType.Talend.value:
                source_asset_list = get_talend_reports(config)
            elif connection_type == ConnectionType.Dbt.value:
                source_asset_list = get_dbt_pipelines(config)
            elif connection_type == ConnectionType.Airflow.value:
                source_asset_list = get_airflow_pipelines(config)
            else:
                source_asset_list = get_source_assets(config)
                source_asset_list = [
                    {key.lower(): val for key, val in item.items()}
                    for item in source_asset_list
                ]
            # Reverse all deprecated assets if they are re-instated in the source directory
            update_reverse_deprecated_dqassets(
                config, source_asset_list, connection_type
            )

            # Mark asset as deprecated if they are not present in the source list
            update_mark_dqpipe_dqreports_as_deprecated(config, source_asset_list)

            # Mark  request queue as completed once the job is finished
            update_queue_detail_status(config, ScheduleStatus.Completed.value)
        else:
            source_asset_list = []
            if connection_type == ConnectionType.Snowflake.value:
                if not connected_database:
                    connected_database = get_dq_connections_database(
                        config, connection_id, limit=0
                    )
                for database_config in connected_database:
                    if not database_config:
                        continue
                    database = database_config.get("name")
                    if not database:
                        continue
                    config.update({"database": database})
                    asset_list = get_source_assets(config)
                    if asset_list:
                        source_asset_list.extend(asset_list)
            elif connection_type == ConnectionType.BigQuery.value:
                source_asset_list = get_source_assets(config)
                
                if source_asset_list:
                    # Validate format for deprecation functions
                    sample_asset = source_asset_list[0]
                    has_name = "name" in sample_asset
                    has_schema = "schema" in sample_asset
                    
                    if not has_name or not has_schema:
                        log_error("WARNING: BigQuery assets missing required fields for deprecation functions")
                else:
                    log_info("No BigQuery assets retrieved")
            else:
                source_asset_list = get_source_assets(config)
            
            # Ensure we have a valid source_asset_list
            if not source_asset_list:
                source_asset_list = get_source_assets(config)
            
            # Normalize asset keys to lowercase for consistency
            source_asset_list = [
                {key.lower(): val for key, val in item.items()}
                for item in source_asset_list
            ]
            # Reverse all deprecated assets if they are re-instated in the source directory
            log_info("=== Starting reverse deprecation process ===")
            update_reverse_deprecated_dqassets(config, source_asset_list)
            log_info("=== Reverse deprecation process completed ===")

            # Mark asset as deprecated if they are not present in the source list
            log_info("=== Starting asset deprecation process ===")
            update_mark_dqassets_as_deprecated(config, source_asset_list)
            log_info("=== Asset deprecation process completed ===")

            # Mark  request queue as completed once the job is finished
            update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        traceback.print_exc()
        # update request queue status
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)
