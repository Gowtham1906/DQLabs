"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
import logging
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.tasks import check_task_status
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.utils.connections import get_dq_connections, get_dq_connections_database
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.utils.metadata_push import (
    export_measure_metadata,
    export_connection_metadata,
    export_asset_metadata,
    export_attribute_metadata,
    export_user_metadata,
    create_metadata_base_tables,
    get_connection_metadata,
    get_asset_metadata,
    get_measure_metadata,
    get_attribute_metadata,
    get_user_session_metadata,
    get_user_activity_metadata,
    
)
from dqlabs.utils.extract_failed_rows import get_new_custom_fields, save_to_storage, get_export_failed_rows_job_config, delete_temp_metadata
from dqlabs.utils.extract_failed_rows import (
    is_schema_exists
)
from dqlabs_agent.services.livy_service import LivyService
from dqlabs.app_helper.log_helper import log_error

logger = logging.getLogger(__name__)


def run_push_metadata(config: dict, **kwargs) -> None:
    """
    This function is used to push metadata to the reporting database
    """
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
        profile_settings = general_settings.get("profile")
        profile_settings = (
            json.loads(profile_settings, default=str)
            if profile_settings and isinstance(profile_settings, str)
            else profile_settings
        )
        profile_settings = profile_settings if profile_settings else {}

        export_batch_size = profile_settings.get("export_batch_size", None)
        export_batch_size = int(export_batch_size) if export_batch_size else 100

        report_settings = config.get("dag_info", {}).get("report_settings")
        print("step-2: report_settings", report_settings)
        if not report_settings:
            report_settings = general_settings.get("reporting", {})
        report_connection = report_settings.get("metadata_connection", {})
        if report_connection.get("id") == "external_storage":
            execute_spark__iceberg_metadata(config, **kwargs)
        else:
            print("step-3: execute_connection_metadata")
            execute_connection_metadata(config, **kwargs)
    except Exception as e:
        log_error("Failed to extract metadata rows", e)
        raise e
    

def execute_spark__iceberg_metadata(config: dict, **kwargs) -> None:
    try:
        update_queue_detail_status(config, ScheduleStatus.Running.value)
        update_queue_status(config, ScheduleStatus.Running.value, True)
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
       
        external_storage = general_settings.get("external_storage", {})
        external_storage = json.loads(external_storage) if isinstance(external_storage, str) else external_storage
        if not external_storage.get("custom_storage"):
            raise Exception(
                "Please define the external storage in settings -> platform -> configuration -> external storage to export the data."
            )
        
         # Get Livy Spark configuration
        dag_info = config.get("dag_info", {})
        livy_spark_config = dag_info.get("livy_spark_config", {})
        livy_spark_config = livy_spark_config if livy_spark_config else {}
        livy_server_url = livy_spark_config.get("livy_url")
        livy_driver_file_path = livy_spark_config.get("drivers_path")
        spark_conf = livy_spark_config.get("spark_config", {})
        spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)
        metadata_types = general_settings.get("reporting",{}).get("metadata_types", [])
        # Get Metadata
        custom_fields = get_new_custom_fields(config)
        connection_metadata = get_connection_metadata(config) if "Connection Metadata" in metadata_types else []
        asset_metadata = get_asset_metadata(config) if "Asset Metadata" in metadata_types else []
        attribute_metadata = get_attribute_metadata(config) if "Attribute Metadata" in metadata_types else []
        measure_metadata = get_measure_metadata(config) if "Measure Metadata" in metadata_types else [] 
        user_sessions = get_user_session_metadata(config)  if "User Metadata" in metadata_types else []
        user_activity = get_user_activity_metadata(config) if "User Metadata" in metadata_types else []
    


        input_config = {
            "metadata": {
                "connection_metadata": connection_metadata,
                "asset_metadata":  asset_metadata,
                "attribute_metadata": attribute_metadata,
                "measure_metadata": measure_metadata,
                "user_metadata": user_sessions,
                "user_activity": user_activity
            },
            "custom_fields": custom_fields,
            "measures": []
        }
        file_url = save_to_storage(config, input_config)
        job_config = {
            "queue_id": config.get("queue_id"),
            "queue_detail_id": config.get("queue_detail_id"),
            "livy_server_url": livy_server_url,
            "livy_driver_file_path": livy_driver_file_path,
            "level": "metadata",
            "job_type": "metadata",
            "metadata_types": metadata_types,
            "failed_rows_file_url": file_url
        }
        print('metadata_types-----', metadata_types)
        print('job_config-----', job_config)
        # Process Logic
        livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)

        # Prepare the job configuration
        external_job_config = get_export_failed_rows_job_config(config)
        job_config.update({"external_credentials": external_job_config})

        # Run the deduplication process
        response = livy_service.run_export_metrics(
            spark_conf=spark_conf, job_config=job_config)
        
        delete_temp_metadata(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value)

        
    except Exception as e:
        raise Exception(f"Push Metadata Failed - {e}")


def execute_connection_metadata(config: dict, **kwargs) -> None:
    """
    This function is used to execute the connection metadata
    """
    try:
        destination_connection = None
        core_connection_id = config.get("core_connection_id")
        connection_id = config.get("connection_id")

        # Get the Reporting Settings
        general_settings = config.get("settings", {})
        if not general_settings:
            dag_info = config.get("dag_info", {})
            dag_info = dag_info if dag_info else {}
            general_settings = dag_info.get("settings", {})
        general_settings = json.loads(general_settings, default=str) if isinstance(
            general_settings, str) else general_settings

        report_settings = config.get("dag_info", {}).get("report_settings", {})
        if not report_settings:
            report_settings = general_settings.get("report_settings", {})
        report_settings = json.loads(report_settings, default=str) if isinstance(
            report_settings, str) else report_settings
        schema_name = report_settings.get("metadata_schema", "")

        if not schema_name:
            raise Exception(
                "Please define the schema name in settings -> platform -> configuration -> remediate -> push down metrics section to export the metadata.")

        # Get the destination connection
        destination_connection_id = report_settings.get('metadata_connection')
        print("step-4: destination_connection_id:", destination_connection_id)
        destination_connection_id = destination_connection_id if destination_connection_id else {}
        destination_connection_id = destination_connection_id.get('id', None)
        source_connection_id = destination_connection_id if destination_connection_id else connection_id
        destination_connection_object = get_dq_connections(
            config, source_connection_id)
        print("step-5: destination_connection_object:", destination_connection_object)
        if not destination_connection_object or len(destination_connection_object) == 0:
            raise Exception("Missing Connection Details.")

        destination_connection_object = destination_connection_object[0]
        destination_connection_object = destination_connection_object if destination_connection_object else {}
        destination_config = {}
        if destination_connection_object:
            destination_conn_credentials = destination_connection_object.get(
                'credentials')
            destination_conn_database = destination_conn_credentials.get(
                'database')
            report_settings_database = report_settings.get("metadata_database")
            connection_type = config.get("connection_type")
            if connection_type == ConnectionType.DB2IBM.value and not report_settings_database:
                report_settings_database = 'sample'
            if connection_type == ConnectionType.Snowflake.value and not destination_conn_database:
                destination_connection_database = get_dq_connections_database(
                    config, source_connection_id)
                destination_conn_database = destination_connection_database.get(
                    'name') if destination_connection_database else ''
            if not report_settings_database:
                report_settings.update({
                    'database': destination_conn_database if destination_conn_database else ''
                })
            destination_config = {
                "core_connection_id": core_connection_id,
                "connection_type": destination_connection_object.get('type'),
                "source_connection_id": destination_connection_object.get('airflow_connection_id'),
                "connection": {**destination_connection_object}
            }
            destination_connection_object.update({
                "core_connection_id": core_connection_id,
                "connection_id": destination_connection_id if destination_connection_id else None,
                "connection_type": destination_connection_object.get('type')
            })

        # Get Queries Based On Connection Type
        default_destination_queries = get_queries(
            {**config, "connection_type": destination_connection_object.get('type')})
        default_source_queries = get_queries(config)
        print("step-5: default_destination_queries:", default_destination_queries)

        failed_rows_query = default_destination_queries.get("failed_rows", {})

        # Get the schema Query
        current_date_query = failed_rows_query.get("current_date")
        print("step-6: current_date_query:", current_date_query)
        current_date = str(current_date_query.split('AS')[0]).strip()
        print("step-7: current_date:", current_date)
        schema_query = failed_rows_query.get("schema")
        schema_query = str(schema_query) if schema_query else ""
        print("step-8: schema_query:", schema_query)
        # Set Database Name for Queries
        database_name = report_settings.get("metadata_database")
        metadata_database = report_settings.get("metadata_database")
        print("step-9: metadata_database:", metadata_database ,"database_name:", database_name)
        connection_type = config.get("connection_type")
        print("step-10: connection_type:", connection_type)
        if connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = 'sample'
        delimiter = report_settings.get("delimiter")
        delimiter = delimiter if delimiter else "|"
        database = database_name
        dest_connection_type = config.get("connection_type")
        print("step-11: dest_connection_type:", dest_connection_type)
        print("step-12: destination_config:", destination_config)
        if destination_config:
            dest_connection_type = destination_config.get("connection_type")
        print("step-13: dest_connection_type:", dest_connection_type)
        database_name = get_attribute_names(
            dest_connection_type, [database_name]) if database_name else ""
        database_name = "".join(database_name).strip()
        print("step-14: database_name:", database_name)
        double_quotes = "\""
        if dest_connection_type in [ConnectionType.Synapse.value, ConnectionType.Redshift_Spectrum.value, ConnectionType.Redshift.value, ConnectionType.Oracle.value, ConnectionType.BigQuery.value, ConnectionType.MSSQL.value, ConnectionType.Snowflake.value, ConnectionType.Teradata.value]:
            double_quotes = ""
        if database_name:
            database_name = f"{database_name}."
        print("step-15: database_name:", database_name)
        config.update({
            "failed_rows_schema": schema_name,
            "failed_rows_database": database_name,
            "delimiter": delimiter,
            "destination_config": destination_config,
            "destination_database": database,
            "destination_connection_object": destination_connection_object,
            "metadata_database": metadata_database
        })
        print("step-16: config:", config)

        update_queue_detail_status(config, ScheduleStatus.Running.value)
        update_queue_status(config, ScheduleStatus.Running.value, True)
        metadata_types = report_settings.get("metadata_types", [])
        print("step-17: metadata_types:", metadata_types)
        print("step-18: failed_rows_query:", failed_rows_query)
        destination_connection = create_metadata_base_tables(
            config, failed_rows_query, metadata_types)
        print("step-19: destination_connection:", destination_connection)
        has_schema = is_schema_exists(config, schema_query)
        print("step-20: has_schema:", has_schema)

        if not has_schema:
            error_message = "No database or schema found in the Reporting connection to export the metadata."
            update_queue_detail_status(
                config, ScheduleStatus.Failed.value, error_message)
            update_queue_status(config, ScheduleStatus.Completed.value)
            raise Exception(error_message)
        else:
            print("Schema exists in the reporting database")


        metadata_types = metadata_types if metadata_types else ''

        # Get and Insert User Metadata
        if "User Metadata" in metadata_types or "All" in metadata_types:
            try:
                destination_connection = export_user_metadata(
                    config, destination_connection_id, schema_name, **kwargs)
            except Exception as e:
                log_error(
                    f"Failed to insert user metadata with error: {str(e)}", e)

        # Get and Insert Connection Metadata
        if "Connection Metadata" in metadata_types or "All" in metadata_types:
            try:
                destination_connection = export_connection_metadata(
                    config, failed_rows_query, destination_connection)
            except Exception as e:
                log_error(
                    f"Failed to insert connection metadata with error: {str(e)}", e)

        # Get and Insert Asset Metadata
        if "Asset Metadata" in metadata_types or "All" in metadata_types:
            try:
                destination_connection = export_asset_metadata(
                    config, default_destination_queries, destination_connection)
            except Exception as e:
                log_error(
                    f"Failed to insert asset metadata with error: {str(e)}", e)

        # Get and Insert Attribute Metadata
        if "Attribute Metadata" in metadata_types or "All" in metadata_types:
            try:
                destination_connection = export_attribute_metadata(
                    config, default_destination_queries, destination_connection)
            except Exception as e:
                log_error(
                    f"Failed to insert attribute metadata with error: {str(e)}", e)

        # Get and Insert Measure Metadata
        if "Measure Metadata" in metadata_types or "All" in metadata_types:
            try:
                destination_connection = export_measure_metadata(
                    config, default_destination_queries, destination_connection)
            except Exception as e:
                log_error(
                    f"Failed to insert measure metadata with error: {str(e)}", e)

        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value, True)
    except Exception as e:
        raise Exception(f"Push Metadata Failed - {e}")