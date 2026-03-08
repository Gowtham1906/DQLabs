"""
Configuration helper functions for export_failed_rows.
These helpers extract repetitive configuration parsing logic while maintaining
backward compatibility with existing code.
"""

import json
import logging
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass
    
from dqlabs.utils.connections import get_dq_connections, get_dq_connections_database

logger = logging.getLogger(__name__)

    
@dataclass
class ExportSettings:
    """Consolidated export settings class to eliminate duplication"""
    general_settings: Dict[str, Any]
    profile_settings: Dict[str, Any]
    report_settings: Dict[str, Any]
    export_batch_size: int
    export_row_limit: int
    export_column_limit: int
    schema_name: str  # For backward compatibility - points to failed_rows schema
    export_group: str
    export_category: str
    delimiter: str
    is_custom_measure: bool
    summarized_report: bool
    exception_outlier: bool

def get_complete_export_settings(config: dict) -> ExportSettings:
    """
    Get all export settings in one consolidated function.
    This eliminates the need for repeated settings extraction across functions.
    """
    # Extract general settings
    general_settings = extract_general_settings(config)
    
    # Extract profile settings
    profile_settings = extract_profile_settings(general_settings)
    
    # Extract report settings
    report_settings = extract_report_settings(config, general_settings)
    
    # Get export limits
    export_row_limit, export_column_limit = get_export_limits(report_settings)
    
    # Get batch size
    export_batch_size = get_export_batch_size(profile_settings)
    
    # Extract activation flags for failed_rows and metadata
    schema_name = report_settings.get("schema", "")
    metadata_schema_name = report_settings.get("metadata_schema", "")
    
    if not schema_name:
        raise Exception(
           "Please define the schema name in settings -> platform -> configuration -> remediate -> push down metrics section to export the data."
        )
    
    
    # Get export configuration
    export_config = setup_export_configuration(report_settings)
    
    # Get delimiter
    delimiter = report_settings.get("delimiter", "|")
    
    return ExportSettings(
        general_settings=general_settings,
        profile_settings=profile_settings,
        report_settings=report_settings,
        export_batch_size=export_batch_size,
        export_row_limit=export_row_limit,
        export_column_limit=export_column_limit,
        schema_name=schema_name,
        export_group=export_config.get("export_group"),
        export_category=export_config.get("export_category"),
        delimiter=delimiter,
        is_custom_measure=export_config.get("is_custom_measure"),
        summarized_report=export_config.get("summarized_report"),
        exception_outlier=export_config.get("exception_outlier")
    )


def extract_general_settings(config: dict) -> dict:
    """
    Extract general settings from config.
    EXACT LOGIC FROM ORIGINAL extract_failed_rows function.
    """
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
    return general_settings


def extract_profile_settings(general_settings: dict) -> dict:
    """
    Extract profile settings from general settings.
    EXACT LOGIC FROM ORIGINAL extract_failed_rows function.
    """
    profile_settings = general_settings.get("profile")
    profile_settings = (
        json.loads(profile_settings, default=str)
        if profile_settings and isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    return profile_settings


def extract_report_settings(config: dict, general_settings: dict = None) -> dict:
    """
    Extract report settings from config.
    EXACT LOGIC FROM ORIGINAL extract_iceberg_failed_rows and extract_connection_failed_rows.
    """
    if general_settings is None:
        general_settings = extract_general_settings(config)
    
    report_settings = config.get("dag_info", {}).get("report_settings")
    if not report_settings:
        report_settings = general_settings.get("reporting", {})
    report_settings = (
        json.loads(report_settings, default=str)
        if report_settings and isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}
    return report_settings


def get_export_batch_size(profile_settings: dict) -> int:
    """
    Get export batch size from profile settings.
    EXACT LOGIC FROM ORIGINAL functions.
    """
    export_batch_size = profile_settings.get("export_batch_size", None)
    export_batch_size = int(export_batch_size) if export_batch_size else 100
    return export_batch_size


def get_export_limits(report_settings: dict) -> Tuple[int, int]:
    """
    Get export row and column limits from report settings.
    EXACT LOGIC FROM ORIGINAL functions.
    """
    export_row_limit = report_settings.get("export_row_limit", None)
    export_row_limit = int(export_row_limit) if export_row_limit else 1000
    export_column_limit = report_settings.get("export_column_limit", None)
    export_column_limit = int(export_column_limit) if export_column_limit else 1000
    return export_row_limit, export_column_limit


def extract_asset_info(config: dict) -> dict:
    """
    Extract asset information from config.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    asset = config.get("asset", {})
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
    
    return {
        "asset": asset,
        "asset_properties": asset_properties,
        "asset_database": asset_database,
        "asset_schema": asset_schema
    }


def extract_primary_attributes(asset: dict, connection_type: str) -> list:
    """
    Extract primary attributes from asset.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    primary_attributes = asset.get("primary_columns", [])
    primary_attributes = (
        json.loads(primary_attributes, default=str)
        if isinstance(primary_attributes, str)
        else primary_attributes
    )
    primary_attributes = primary_attributes if primary_attributes else []
    
    # Get connection type to determine which field to use for attribute names
    from dqlabs.enums.connection_types import ConnectionType
    # For SAP ECC, use column_name instead of name
    if connection_type == ConnectionType.SapEcc.value:
        primary_attributes = [attribute.get("name") or attribute.get("column_name") for attribute in primary_attributes]
    else:
        primary_attributes = [attribute.get("name") for attribute in primary_attributes]
    
    return primary_attributes


def process_measure_export_limits(measure: dict, default_row_limit: int, default_column_limit: int) -> dict:
    """
    Process measure-specific export limits.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    export_limit_config = measure.get("export_limit_config")
    export_limit_config = (
        json.loads(export_limit_config)
        if isinstance(export_limit_config, str)
        else export_limit_config
    )

    limit_config = export_limit_config if export_limit_config else {}
    is_enabled = limit_config.get("override", False)
    is_enabled = is_enabled if is_enabled else False
    row_count = limit_config.get("row_count")
    column_count = limit_config.get("column_count")
    
    measure_export_row_limit = (
        int(row_count) if is_enabled and row_count else default_row_limit
    )
    measure_export_row_limit = (
        measure_export_row_limit
        if measure_export_row_limit and measure_export_row_limit > 0
        else default_row_limit
    )
    
    column_count = column_count if column_count else 0
    column_count = int(column_count) if isinstance(column_count, str) else column_count
    measure_export_column_limit = (
        int(column_count)
        if is_enabled and column_count > 0
        else default_column_limit
    )
    measure_export_column_limit = (
        measure_export_column_limit
        if measure_export_column_limit and measure_export_column_limit > 0
        else default_column_limit
    )
    
    return {
        "measure_export_row_limit": measure_export_row_limit,
        "measure_export_column_limit": measure_export_column_limit
    }


def validate_destination_connection(destination_connection_object: list, destination_connection_id: str):
    """
    Validate destination connection.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    if not destination_connection_object or len(destination_connection_object) == 0:
        raise Exception("Missing Connection Details.")

    destination_connection_detail = destination_connection_object[0]
    is_active = destination_connection_detail.get("is_active", False) 
    is_valid = destination_connection_detail.get("is_valid", False)  

    # Raise an exception if any condition fails
    if not is_active:
        raise Exception(f"Destination connection with ID: {destination_connection_id} is not active!")
    if not is_valid:
        raise Exception(f"Destination connection with ID: {destination_connection_id} is not valid!")


def get_database_name_for_connection(connection_type: str, destination_conn_database: str, 
                                    config: dict, source_connection_id: str) -> str:
    """
    Get database name for specific connection types.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    from dqlabs.enums.connection_types import ConnectionType
    from dqlabs.utils.connections import get_dq_connections_database
    
    if connection_type == ConnectionType.DB2IBM.value and not destination_conn_database:
        return "sample"
    
    if (connection_type == ConnectionType.Snowflake.value and 
        not destination_conn_database):
        destination_connection_database = get_dq_connections_database(config, source_connection_id)
        return destination_connection_database.get("name")
    
    return destination_conn_database


def setup_destination_config(config: dict, destination_connection_object: dict, 
                            task_id: str, core_connection_id: str,
                            destination_connection_id: Optional[str] = None) -> dict:
    """
    Setup destination configuration.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    destination_config = {
        "queue_detail_id": task_id,
        "core_connection_id": core_connection_id,
        "connection_type": destination_connection_object.get("type"),
        "source_connection_id": destination_connection_object.get("airflow_connection_id"),
        "connection": {**destination_connection_object},
        "category": config.get("category"),
    }
    
    destination_connection_object.update({
        "queue_detail_id": task_id,
        "category": config.get("category"),
        "core_connection_id": core_connection_id,
        "connection_id": destination_connection_id if destination_connection_id else None,
        "connection_type": destination_connection_object.get("type"),
    })
    
    return destination_config


def setup_export_configuration(report_settings: dict) -> dict:
    """
    Setup export configuration parameters.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    export_row_limit, export_column_limit = get_export_limits(report_settings)
    
    is_custom_measure = report_settings.get("is_custom_measure", None)
    is_custom_measure = True if is_custom_measure is None else is_custom_measure
    is_custom_measure = is_custom_measure if is_custom_measure else False
    
    export_group = report_settings.get("export_group")
    export_group = export_group if export_group else "individual"
    
    export_category = report_settings.get("export_category", "custom")
    
    summarized_report = report_settings.get("summarized_report", False)
    exception_outlier = report_settings.get("exception_outlier", False)
    
    delimiter = report_settings.get("delimiter")
    delimiter = delimiter if delimiter else "|"
    
    return {
        "export_row_limit": export_row_limit,
        "export_column_limit": export_column_limit,
        "is_custom_measure": is_custom_measure,
        "export_group": export_group,
        "export_category": export_category,
        "summarized_report": summarized_report,
        "delimiter": delimiter,
        "exception_outlier": exception_outlier
    }


def prepare_destination_connection(config: dict, export_settings: ExportSettings, is_metadata: bool = False) -> dict:
    """
    Prepare destination connection.
    EXACT LOGIC FROM ORIGINAL extract_connection_failed_rows function.
    """
    connection_id = config.get("connection_id")
    task_id = config.get("queue_detail_id")
    core_connection_id = config.get("core_connection_id")

    connection = export_settings.report_settings.get("connection")
    if is_metadata:
        connection = export_settings.report_settings.get("metadata_connection")

    destination_connection_id = connection
    destination_connection_id = (
        destination_connection_id if destination_connection_id else {}
    )
    destination_connection_id = destination_connection_id.get("id", None)
    source_connection_id = (
        destination_connection_id if destination_connection_id else connection_id
    )
    destination_connection_object = get_dq_connections(config, source_connection_id)
    
    validate_destination_connection(destination_connection_object, destination_connection_id)

    destination_connection_object = destination_connection_object[0]
    destination_connection_object = (
        destination_connection_object if destination_connection_object else {}
    )
    destination_config = {}
    destination_database = ""
    if destination_connection_object:
        destination_conn_credentials = destination_connection_object.get("credentials")
        destination_conn_database = destination_conn_credentials.get("database")
        report_settings_database = export_settings.report_settings.get("database")
        destination_connection_type = connection.get("type")
        connection_type = config.get("connection_type")
        
        destination_conn_database = get_database_name_for_connection(
            connection_type, destination_conn_database, config, source_connection_id
        )
        
        if not report_settings_database:
            export_settings.report_settings.update({
                "database": destination_conn_database if destination_conn_database else ""
            })
        
        # IMPROVED: Use helper function for destination config setup
        destination_config = setup_destination_config(
            config, destination_connection_object, task_id, core_connection_id, destination_connection_id
        )
    return destination_connection_object, destination_config, destination_database, destination_conn_database, source_connection_id