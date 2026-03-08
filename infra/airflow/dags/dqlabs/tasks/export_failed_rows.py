"""
    Migration Notes From V2 to V3:
    No Migration Changes
    
    IMPROVED: Fixed indentation issues, syntax errors, and implemented dictionary standards.
"""

import json
import re
import gc
from copy import deepcopy
import logging
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict
from dqlabs.utils.extract_failed_rows import (
    get_measures,
    get_failed_row_query,
    refresh_failed_rows,
    update_failed_rows_stat,
    get_failed_rows_table_name,
    update_failed_rows_table_name,
    create_base_tables,
    is_schema_exists,
    insert_measure_metadata,
    insert_failed_rows_metadata,
    alter_table_schema,
    get_failed_rows_tablename_by_export_group,
    get_metadata,
    update_metadata,
    has_limit,
    lookup_processing_query,
    get_measure_fields,
    get_unsupported_attributes,
    get_new_custom_fields,
    get_connection_metadata,
    get_asset_metadata,
    get_attribute_metadata_data,
    get_measure_metadata,
    get_summary_measure_data,
    prepare_measure_metadata,
    save_to_storage,
    delete_temp_metadata,
    get_export_failed_rows_job_config,
    get_retention_run_history,
    update_summarized_columns
)
from dqlabs.utils.connections import get_dq_connections, get_dq_connections_database
from dqlabs_agent.services.livy_service import LivyService
from dqlabs.utils.extract_workflow import get_queries, get_selected_attributes
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    check_is_direct_query,
    get_derived_type
)
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.utils.tasks import get_task_config, check_task_status, clear_duplicate_task
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.connection_helper import (
    get_attribute_names,
    check_is_same_source,
    decrypt_connection_config
)
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_constants.dq_constants import (
    REPORT_EXPORT_GROUP_DOMAINS,
    IDENTIFIER_KEY_COLUMN_NAME,
    QUERY,
    PARAMETER,
    DIRECT_QUERY_BASE_TABLE_LABEL,
    EXPORT_FAILED_ROWS,
    CROSS_SOURCE
)
from dqlabs.app_helper.db_helper import fetchall, fetchmany, split_queries, fetchone, log_info
from dqlabs.utils.export_failed_rows.measure_helpers import process_measure_export_limits
from dqlabs.utils.export_failed_rows.config_helpers import (
    get_database_name_for_connection,
    setup_destination_config,
    validate_destination_connection,
    get_complete_export_settings,
    extract_asset_info,
    extract_primary_attributes,
    prepare_destination_connection
)
from dqlabs.utils.export_failed_rows.database_helpers import (
    format_boolean_value,
    get_current_timestamp,
    build_select_values_query,
    format_identifier,
    format_json_for_database
)
from dqlabs.utils.export_failed_rows.exception_outlier import (
    extract_exception_outlier, 
    get_marked_normal_outlier_values,
    prepare_marked_normal_where_clause
)
from dqlabs.app_helper.sql_group_parse import append_where_to_query
from dqlabs.utils.cross_source import execute_export_failed_rows_query, prepare_cross_source_query
from dqlabs.app_helper.agent_helper import get_vault_data
        

logger = logging.getLogger(__name__)

def extract_failed_rows(config: dict, **kwargs):
    """
    Extract failed rows from the source and export to the destination.
    IMPROVED: Now uses helper functions for cleaner configuration management.
    """
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        clear_duplicate_task(config, EXPORT_FAILED_ROWS)
        
        export_settings = get_complete_export_settings(config)

        report_connection = export_settings.report_settings.get("connection", {})
        metadata_active = export_settings.report_settings.get('metadata_is_active')
        metadata_connection = export_settings.report_settings.get('metadata_connection')

        if export_settings.report_settings.get('is_active'):
            if report_connection.get("id") == "external_storage":
                extract_iceberg_failed_rows(config, **kwargs)
            else:
                extract_connection_failed_rows(config, **kwargs)
        # Route to appropriate extraction method (same logic as original)
        
        if metadata_active and metadata_connection.get('id')== "external_storage":
            extract_iceberg_metadata_failed_rows(config, **kwargs)
        elif metadata_active:
            extract_metadata_failed_rows(config, **kwargs)
        

    except Exception as e:
        log_error("Failed to extract failed rows", e)
        raise e

def extract_iceberg_failed_rows(config: dict, **kwargs):
    """
    IMPROVED: Extract iceberg failed rows with better configuration management.
    All original functionality preserved.
    """
    level = config.get("level", "")
    
    # IMPROVED: Use consolidated settings function to eliminate duplication
    export_settings = get_complete_export_settings(config)
    
    # Validate external storage (same logic as original)
    external_storage = export_settings.general_settings.get("external_storage", {})
    external_storage = json.loads(external_storage) if isinstance(external_storage, str) else external_storage
    if not external_storage.get("custom_storage"):
        raise Exception(
            "Please define the external storage in settings -> platform -> configuration -> external storage to export the data."
        )
    failed_rows_table = config.get("failed_rows_table")
    reset_failed_rows_table = config.get("reset_failed_rows_table", False)
    if level == "measure":
        failed_rows_table = config.get("measure_failed_rows_table", "")
        reset_failed_rows_table = config.get("measure_reset_failed_rows_table", False)
    
    # Use consolidated settings values
    export_group = export_settings.export_group
    export_category = export_settings.export_category
    delimiter = export_settings.delimiter
    is_summarized = export_settings.summarized_report
    exception_outlier = export_settings.exception_outlier
    metadata = get_metadata(config)
    config.update({"delimiter": delimiter, "export_category": export_category, "metadata": metadata})
    is_direct_query_asset = check_is_direct_query(config)


    # Get Livy Spark configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    livy_server_url = livy_spark_config.get("livy_url")
    livy_driver_file_path = livy_spark_config.get("drivers_path")
    spark_conf = livy_spark_config.get("spark_config", {})
    spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)
    task_config = get_task_config(config, kwargs)
    
    # Prepare Job Config
    asset = config.get("asset", {})
    attribute = config.get("attribute", {})
    measure = config.get("measure", {})
    connection = config.get("connection", {})
    base_table_query = config.get("base_table_query")
    connection_type = connection.get("type")
    credentials = connection.get("credentials", {})
    credentials.update({"job_type": "failed_rows"})
    asset_properties = asset.get("properties", {})
    asset_properties = asset_properties if asset_properties else {}
    asset_database = asset_properties.get("database")
    asset_schema = asset_properties.get("schema")
    asset_database = asset_database if asset_database else ""
    asset_schema = asset_schema if asset_schema else ""
    credentials = decrypt_connection_config(credentials, connection_type)
    credentials.update({"db_type": connection_type, "table": "test"})

    if credentials.get("is_vault_enabled"):
        pg_connection = get_postgres_connection(config)
        vault_credentials = get_vault_data(config, pg_connection, credentials)
        credentials = {**credentials, **vault_credentials}
        credentials = decrypt_connection_config(credentials, connection_type)

    if level != "measure":
        credentials.update({"database": asset_database, "schema": asset_schema})

    if level == "measure":
        category = measure.get("category")
        if category == CROSS_SOURCE:
            config.update({"connection_type": ConnectionType.Spark.value})
       
    asset_config = {
        "id": asset.get("id"),
        "name": asset.get("name"),
        "group": asset.get("group"),
        "type": asset.get("type"),
        "unique_id": asset.get("unique_id")
    }
    
    connection = {
        "id": connection.get("id"),
        "name": connection.get("name"),
        "type": connection.get("type"),
        "credentials": credentials
    }
    primary_attributes = asset.get("primary_columns", [])
    primary_attributes = (
        json.loads(primary_attributes, default=str)
        if isinstance(primary_attributes, str)
        else primary_attributes
    )
    primary_attributes = primary_attributes if primary_attributes else []
    primary_attributes = [attribute.get("name") for attribute in primary_attributes]
    
    job_config = {
        "asset": asset_config,
        "queue_id": config.get("queue_id"),
        "queue_detail_id": config.get("queue_detail_id"),
        "connection": connection,
        "failed_rows_table": failed_rows_table,
        "reset_failed_rows_table": reset_failed_rows_table,
        "export_group": export_group,
        "export_category": export_category,
        "database_name": config.get("database_name", ""),
        "schema": config.get("schema", ""),
        "livy_server_url": livy_server_url,
        "livy_driver_file_path": livy_driver_file_path,
        "level": level,
        "primary_attributes": primary_attributes,
        "credentials": credentials,
        "asset_id": asset_config.get("id"),
        "attribute_id": attribute.get("id"),
        "measure_id": measure.get("id"),
        "job_type": "failed_rows",
        "connection_type": connection.get("type"),
        "is_summarized": is_summarized,
        "is_failed_rows": True
    }

    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    # prepare and execute failed rows query for each measure
    default_source_queries = get_queries(config)
    default_source_queries = default_source_queries if default_source_queries else {}
    measures = get_measures(config)
    limit_condition_query = default_source_queries.get("limit_query")

    if not measures:
        raise Exception("No measures found")

    if exception_outlier:
        extract_exception_outlier(config)
    
    # Get and update failed rows table name
    is_reset = False
    if not failed_rows_table or reset_failed_rows_table:
        failed_rows_table = get_failed_rows_table_name(config)
        is_reset = reset_failed_rows_table if reset_failed_rows_table else False
    update_failed_rows_table_name(config, failed_rows_table, is_reset)
    export_group_table_name = get_failed_rows_tablename_by_export_group(
        config, export_group, level
    )
    if export_group_table_name:
        failed_rows_table = export_group_table_name.lower()
        if export_group == REPORT_EXPORT_GROUP_DOMAINS:
            update_failed_rows_table_name(
                config, failed_rows_table, False, REPORT_EXPORT_GROUP_DOMAINS
            )
    
    custom_fields = get_new_custom_fields(config)
    job_config.update({"failed_rows_table": failed_rows_table, "is_reset": is_reset })
    measure_list = [measure.get("id") for measure in measures]
    measure_metadata = get_measure_metadata(config, measure_list)

    measure_data = []
    for measure in measures:
        measure_id = measure.get("id")
        metadata = next((x for x in measure_metadata if x.get("measure_id") == measure_id), None)
        if not metadata:
            continue
        attribute_id = measure.get("attribute_id")
        selected_attribute = {}
        if attribute_id:
            selected_attribute = get_selected_attributes(config, attribute_id)
        selected_attribute = selected_attribute if selected_attribute else {}
        measure_category = measure.get("category")

        
        export_limits = process_measure_export_limits(
            measure, export_settings.export_row_limit, export_settings.export_column_limit
        )
        measure_export_row_limit = export_limits.measure_export_row_limit
        measure_export_column_limit = export_limits.measure_export_column_limit

        invalid_select_query, failed_rows_metadata, has_failed_rows_query = (
                get_failed_row_query(config, measure, default_source_queries, is_direct_query_asset)
            )

        check_selected_attributes = (
            not has_failed_rows_query and measure_category == "conditional"
        )
        config.update({"check_selected_attributes": check_selected_attributes})

        if not invalid_select_query:
            measure_metadata = [measure for measure in measure_metadata if measure.get("measure_id") != measure_id]
            continue

        # Get Marked Normal Outlier Values
        if exception_outlier:
            marked_normal_outlier_values = get_marked_normal_outlier_values(config, measure_id)
            outlier_where_clause = prepare_marked_normal_where_clause(config, marked_normal_outlier_values, connection_type)
        else:
            outlier_where_clause = None


        select_query = ""
        if measure_export_row_limit and limit_condition_query:
            if (
                "order by" in invalid_select_query
                and "order by (select 1)" in limit_condition_query
            ):
                limit_condition_query = limit_condition_query.replace(
                    "order by (select 1)", ""
                )
            connection_type = config.get("connection_type", "")
            connection_type = connection_type.lower()
            if not has_limit(connection_type, invalid_select_query):
                if (
                    measure_category == "lookup"
                    and "<query_string>" in invalid_select_query
                ):
                    invalid_select_query = f"{invalid_select_query}"
                else:
                    if "distinct" not in invalid_select_query.lower():
                        from dqlabs.utils.export_failed_rows.measure_helpers import merge_limit_query
                        if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
                            limit_query_to_merge = limit_condition_query.replace("<count>", str(measure_export_row_limit)) if limit_condition_query else limit_condition_query
                        else:
                            limit_query_to_merge = limit_condition_query if limit_condition_query else ""
                        invalid_select_query = merge_limit_query(
                            invalid_select_query, limit_query_to_merge, connection_type
                        )
            if measure_category == CROSS_SOURCE:
                invalid_select_query = prepare_cross_source_query(config, measure, invalid_select_query)
                job_config.update({"connection_type": "spark"})

            select_query = invalid_select_query
            if (
                    measure_category == "lookup"
                    and "<query_string>" in invalid_select_query
                ):
                    (
                        select_query,
                        invalid_select_query,
                        lookup_group_query,
                    ) = lookup_processing_query(
                        config,
                        measure,
                        select_query,
                        invalid_select_query,
                        limit_condition_query,
                        measure_export_row_limit,
                        True,
                    )
            else:
                if measure_category == "lookup":
                    if ("count(*)" in invalid_select_query.lower()) and (
                        "count(*) as " not in invalid_select_query.lower()
                    ):
                        compiled = re.compile(re.escape("count(*)"), re.IGNORECASE)
                        invalid_select_query = compiled.sub(
                            'count(*) as "COUNT(*)"', invalid_select_query
                        )
                        
            if outlier_where_clause:
                if connection_type != ConnectionType.MongoDB.value:
                    invalid_select_query = append_where_to_query(invalid_select_query, outlier_where_clause)

            invalid_select_query = invalid_select_query.replace(
                    "<count>", str(measure_export_row_limit)
                )

            if (
                (measure_category == QUERY or measure_category == PARAMETER)
                and is_direct_query_asset
                and base_table_query
            ):
                invalid_select_query = invalid_select_query.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )
                select_query = select_query.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )
            
            if (
                (measure_category == QUERY or measure_category == PARAMETER)
                and is_direct_query_asset
                and base_table_query
            ):
                invalid_select_query = invalid_select_query.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )
                select_query = select_query.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )

            # Need to add query and lookup Measure
            failed_rows_metadata.update({
                "domains": metadata.get("domains"),
                "products": metadata.get("product"),
                "applications": metadata.get("applications"),
                "terms": metadata.get("terms"),
                "tags": metadata.get("tags")
            })

            failed_rows_metadata.pop("created_date", None)
            failed_rows_metadata_columns = [f"'{v}' as {k.upper()}" for k, v in failed_rows_metadata.items()]
            failed_rows_metadata_columns.append("CURRENT_TIMESTAMP as CREATED_DATE")
            measure_data.append({
                "measure_id": measure_id,
                "measure_name": measure.get("measure_name"),
                "category": measure.get("category"),
                "type": measure.get("type"),
                "level": measure.get("level"),
                "select_query": select_query,
                "invalid_select_query": invalid_select_query,
                "column_limit": export_settings.export_column_limit,
                "failed_rows_metadata_columns": failed_rows_metadata_columns
            })
    status = ScheduleStatus.Completed.value
    error_message = ""
    if measures:
        measure_metadata = prepare_measure_metadata(config, measure_metadata)
        last_runs = get_retention_run_history(config)
        unsupported_attributes = get_unsupported_attributes(config)
        input_config = {
            "metadata": {
                "measure_metadata": measure_metadata
            },
            "custom_fields": custom_fields,
            "measures": measure_data,
            "retention_runs": last_runs,
            "unsupported_attributes": unsupported_attributes,
            "is_failed_rows": True
        }
        file_url = save_to_storage(config, input_config)
        job_config.update({"failed_rows_file_url": file_url})
        # Process Logic
        livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)

        # Prepare the job configuration
        external_job_config = get_export_failed_rows_job_config(config)
        job_config.update({"external_credentials": external_job_config})

        # Run the deduplication process
        response = livy_service.run_export_metrics(
            spark_conf=spark_conf, job_config=job_config)
        response = json.loads(response) if isinstance(response, str) else response
        error_message = None
        if response.get("status") == "error":
            error_message = response.get("message")
            status = ScheduleStatus.Failed.value
        update_failed_rows_stat(config)
        
    delete_temp_metadata(config)
    update_queue_detail_status(config, status, error=error_message)
    update_queue_status(config, status)

def extract_iceberg_metadata_failed_rows(config: dict, **kwargs):
    """ seperate funtion to create and push metadata to the reportin database"""
    level = config.get("level", "")

    # Get Livy Spark configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    livy_server_url = livy_spark_config.get("livy_url")
    livy_driver_file_path = livy_spark_config.get("drivers_path")
    spark_conf = livy_spark_config.get("spark_config", {})
    spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

    # Prepare Job Config
    asset = config.get("asset", {})
    attribute = config.get("attribute", {})
    measure = config.get("measure", {})
    job_config = {
        "queue_id": config.get("queue_id"),
        "queue_detail_id": config.get("queue_detail_id"),
        "database_name": config.get("database_name", ""),
        "schema": config.get("schema", ""),
        "livy_server_url": livy_server_url,
        "livy_driver_file_path": livy_driver_file_path,
        "job_type": "failed_rows",
        "asset_id": asset.get("id"),
        "attribute_id": attribute.get("id"),
        "measure_id": measure.get("id"),
        "is_metadata": True
    }
    measures = get_measures(config)
    custom_fields = get_new_custom_fields(config)
    measure_list = [measure.get("id") for measure in measures]
    measure_metadata = get_measure_metadata(config, measure_list)

    if measure_metadata:
        connection_metadata = get_connection_metadata(config)
        measure_metadata = prepare_measure_metadata(config, measure_metadata)
        asset_metadata = None
        attribute_metadata = None
        if level != "measure":
            asset_metadata = get_asset_metadata(config)
            attribute_metadata = get_attribute_metadata_data(config)
        input_config = {
            "metadata": {
                "connection_metadata": connection_metadata,
                "asset_metadata":  asset_metadata,
                "attribute_metadata": attribute_metadata,
                "measure_metadata": measure_metadata
            },
            "custom_fields": custom_fields
        }
        file_url = save_to_storage(config, input_config)
        job_config.update({"failed_rows_file_url": file_url})
        # Process Logic
        livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)

        # Prepare the job configuration
        external_job_config = get_export_failed_rows_job_config(config)

        job_config.update({"external_credentials": external_job_config})

        # Run the deduplication process
        response = livy_service.run_export_metrics(
            spark_conf=spark_conf, job_config=job_config)

        response = json.loads(response) if isinstance(response, str) else response
        status = ScheduleStatus.Completed.value
        error_message = None
        if response.get("status") == "error":
            error_message = response.get("message")
            status = ScheduleStatus.Failed.value
        update_failed_rows_stat(config)
        
    delete_temp_metadata(config)
    update_queue_detail_status(config, status, error=error_message)
    update_queue_status(config, status)



def extract_metadata_failed_rows(config: dict, **kwargs):
    """ seperate funtion to create and push metadata to the reportin database"""
    source_connection = None
    destination_connection = None
    connection_id = config.get("connection_id")
    core_connection_id = config.get("core_connection_id")
    task_id = config.get("queue_detail_id")
    level = config.get("level")
    task_config = get_task_config(config, kwargs)
    export_settings = get_complete_export_settings(config)
    destination_connection_id = export_settings.report_settings.get("metadata_connection")
    schema_name = export_settings.report_settings.get("metadata_schema")
    
    destination_database = ""
    destination_connection_object, destination_config, destination_database, destination_conn_database, source_connection_id = prepare_destination_connection(config, export_settings, is_metadata=True)
        
    default_destination_queries = get_queries(
        {**config, "connection_type": destination_connection_object.get("type")}
    )
    
    # Set Database Name for Queries
    failed_rows_query = default_destination_queries.get("failed_rows", {})
    schema_query = failed_rows_query.get("schema")
    schema_query = str(schema_query) if schema_query else ""
    database_name = export_settings.report_settings.get("metadata_database")
    connection_type = config.get("connection_type")
    if (
        destination_connection_object.get("type") == ConnectionType.DB2IBM.value
        and not destination_database
    ):
        database_name = "sample"
    if (
        destination_connection_object.get("type") == ConnectionType.Snowflake.value
        and not destination_conn_database
        and not database_name
    ):
        destination_connection_database = get_dq_connections_database(
            config, source_connection_id
        )
        database_name = destination_connection_database.get("name")
    delimiter = export_settings.delimiter
    database = database_name
    dest_connection_type = config.get("connection_type")
    if destination_config:
        dest_connection_type = destination_config.get("connection_type")

    
    database_name = (
        get_attribute_names(dest_connection_type, [database_name])
        if database_name
        else ""
    )
    database_name = "".join(database_name).strip()
    double_quotes = '"'
    if dest_connection_type in [
        ConnectionType.SapHana.value,
        ConnectionType.Synapse.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Redshift.value,
        ConnectionType.Oracle.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MSSQL.value,
        ConnectionType.Snowflake.value,
        ConnectionType.Teradata.value,
        ConnectionType.MySql.value,
    ]:
        double_quotes = ""
    elif dest_connection_type in [
        ConnectionType.Databricks.value,
        ConnectionType.Hive.value,
    ]:
        double_quotes = "`"
    if database_name:
        database_name = f"{database_name}."

    metadata = get_metadata(config)
    config.update({
        "failed_rows_schema": schema_name,
        "failed_rows_database": database_name,
        "delimiter": delimiter,
        "destination_config": destination_config,
        "destination_database": database,
        "destination_connection_object": destination_connection_object
    })
    has_schema = is_schema_exists(config, schema_query)
    if not has_schema:
        error_message = "No database or schema found in the source connection to export the failed rows data."
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error_message)
        update_queue_status(config, ScheduleStatus.Completed.value)
        raise Exception(error_message)

    # ===== METADATA OPERATIONS SECTION =====
    # Step 1: Setup metadata connection and configuration
    metadata_connection_id = export_settings.report_settings.get("metadata_connection", {}).get("id")
    metadata_database = export_settings.report_settings.get("metadata_database", "")
    metadata_schema = export_settings.report_settings.get("metadata_schema", "")
    
    # Get metadata connection object
    metadata_connection_object = get_dq_connections(config, metadata_connection_id) if metadata_connection_id else None
    metadata_connection = None
    metadata_config = {}
    
    if metadata_connection_object and len(metadata_connection_object) > 0:
        metadata_connection_object = metadata_connection_object[0]
        metadata_connection_object = metadata_connection_object if metadata_connection_object else {}
        
        # Setup metadata destination config
        metadata_config = setup_destination_config(
            config, metadata_connection_object, task_id, core_connection_id, metadata_connection_id
        )
        
        # Get metadata queries based on connection type
        metadata_queries = get_queries(
            {**config, "connection_type": metadata_connection_object.get("type")}
        )
        
        # Update config with metadata-specific settings
        config.update({
            "metadata_database": metadata_database,
            "metadata_schema": metadata_schema,
            "metadata_connection": metadata_connection_object
        })
    create_base_tables(config, failed_rows_query, is_metadata=True)
    
    # Step 2: Get measures for metadata processing
    measures = get_measures(config)
    if not measures:
        raise Exception("No measures found for metadata processing")
    
    # Step 3: Process each measure for metadata insertion
    failed_measures = []
    for measure in measures:
        try:
            measure_id = measure.get("id")
            
            # Insert measure metadata using metadata connection
            if metadata_connection_object:
                native_connection = insert_measure_metadata(
                    config,
                    metadata_queries.get("failed_rows", {}),
                    measure,
                    metadata_connection,
                    metadata_queries,
                )
                if not metadata_connection:
                    metadata_connection = native_connection
            
        except Exception as e:
            logger.error(f"Error processing metadata for measure {measure_id}: {str(e)}", exc_info=True)
            failed_measures.append(measure_id)
    
    # Step 4: Final metadata operations after all measures are processed
    if measures and metadata_connection_object:
        
        # Insert failed rows metadata
        insert_failed_rows_metadata(
            config,
            metadata_queries.get("failed_rows", {}),
            metadata_connection,
            metadata_queries,
        )
    
    # Step 5: Handle completion status
    if failed_measures:
        logger.info(f"Metadata processing completed: {len(measures) - len(failed_measures)}/{len(measures)} successful")
        logger.warning(f"Failed measures: {failed_measures}")
    
    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)
    

def extract_connection_failed_rows(config: dict, **kwargs):
    """
    Run failed rows extraction task
    IMPROVED: Now uses helper functions for cleaner configuration management.
    All original functionality preserved.
    """
    source_connection = None
    destination_connection = None
    core_connection_id = config.get("core_connection_id")
    connection_id = config.get("connection_id")
    task_id = config.get("queue_detail_id")
    level = config.get("level")
    measure = config.get("measure", {})
    task_config = get_task_config(config, kwargs)
    failed_rows_table = config.get("failed_rows_table")
    reset_failed_rows_table = config.get("reset_failed_rows_table")
    asset_attributes = []
    if level == "measure":
        failed_rows_table = config.get("measure_failed_rows_table")
        reset_failed_rows_table = config.get("measure_reset_failed_rows_table")
    else:
        asset_attributes = get_selected_attributes(config)
    config.update({"asset_attributes": asset_attributes})
    is_direct_query_asset = check_is_direct_query(config)
    base_table_query = config.get("base_table_query")
    connection = config.get("connection", {})
    source_connection_type = connection.get("type")
    export_settings = get_complete_export_settings(config)
    asset_info = extract_asset_info(config)
    config.update({
        "database_name": asset_info.get("asset_database", ""), 
        "schema": asset_info.get("asset_schema", "")
    })

    # IMPROVED: Use helper function for primary attributes
    primary_attributes = extract_primary_attributes(
        asset_info.get("asset", {}), config.get("connection_type")
    )

    # Extract values from consolidated settings for backward compatibility
    schema_name = export_settings.schema_name
    export_row_limit = export_settings.export_row_limit
    export_column_limit = export_settings.export_column_limit
    export_batch_size = export_settings.export_batch_size
    is_custom_measure = export_settings.is_custom_measure
    export_group = export_settings.export_group
    export_category = export_settings.export_category
    summarized_report = export_settings.summarized_report
    exception_outlier = export_settings.exception_outlier
    # Get Destination Connection
    destination_connection_object, destination_config, destination_database, destination_conn_database, source_connection_id = prepare_destination_connection(config, export_settings)

    # Get Queries Based On Connection Type
    default_destination_queries = get_queries(
        {**config, "connection_type": destination_connection_object.get("type")}
    )
    default_source_queries = get_queries(config)

    # create failed rows table
    failed_rows_query = default_destination_queries.get("failed_rows", {})
    dq_datatypes = default_destination_queries.get("dq_datatypes")
    drop_table_query = failed_rows_query.get("drop_table")
    concat_query = failed_rows_query.get("concat_ws")
    drop_temp_view_query = failed_rows_query.get("drop_temp_view")
    invalid_insert_query = failed_rows_query.get("insert_failed_rows_data")
    insert_values_query = failed_rows_query.get("insert_values")
    invalid_insert_query_with_select = failed_rows_query.get(
        "insert_failed_rows_data_with_select"
    )

    create_temp_table = failed_rows_query.get("create_temp_table")
    create_temp_view = failed_rows_query.get("create_temp_view")
    get_temp_table_columns_query = failed_rows_query.get("get_temp_table_columns")
    insert_from_temp_table = failed_rows_query.get("insert_from_temp_table")
    insert_failed_rows_data_direct_query = failed_rows_query.get(
        "insert_failed_rows_data_direct_query", ""
    )
    concatenate_attribute = failed_rows_query.get("concatenate_attribute", "")

    current_date_query = failed_rows_query.get("current_date")
    current_date = str(current_date_query.split("AS")[0]).strip()
    schema_query = failed_rows_query.get("schema")
    schema_query = str(schema_query) if schema_query else ""
    text_value_query = failed_rows_query.get("text_value")
    text_value_query = str(text_value_query) if text_value_query else ""
    lookup_group_query = ""
    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)
    if not invalid_insert_query:
        raise Exception("Cannot save invalid rows for this connector")

    database_name = export_settings.report_settings.get("database")
    connection_type = config.get("connection_type")
    if (
        destination_connection_object.get("type") == ConnectionType.DB2IBM.value
        and not destination_database
    ):
        database_name = "sample"
    if (
        destination_connection_object.get("type") == ConnectionType.Snowflake.value
        and not destination_conn_database
        and not database_name
    ):
        destination_connection_database = get_dq_connections_database(
            config, source_connection_id
        )
        database_name = destination_connection_database.get("name")
    delimiter = export_settings.delimiter
    database = database_name
    dest_connection_type = config.get("connection_type")
    if destination_config:
        dest_connection_type = destination_config.get("connection_type")
    
    # Set summarized_report to False if connector is not in the supported list
    if dest_connection_type not in [
        ConnectionType.SapHana.value,
        ConnectionType.MSSQL.value,
        ConnectionType.Snowflake.value,
        ConnectionType.Databricks.value,
        ConnectionType.Oracle.value,
        ConnectionType.Redshift.value
    ]:
        summarized_report = False
    
    database_name = (
        get_attribute_names(dest_connection_type, [database_name])
        if database_name
        else ""
    )
    database_name = "".join(database_name).strip()
    double_quotes = '"'
    if dest_connection_type in [
        ConnectionType.SapHana.value,
        ConnectionType.Synapse.value,
        ConnectionType.Redshift_Spectrum.value,
        ConnectionType.Redshift.value,
        ConnectionType.Oracle.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MSSQL.value,
        ConnectionType.Snowflake.value,
        ConnectionType.Teradata.value,
        ConnectionType.MySql.value,
    ]:
        double_quotes = ""
    elif dest_connection_type in [
        ConnectionType.Databricks.value,
        ConnectionType.Hive.value,
    ]:
        double_quotes = "`"
    if database_name:
        database_name = f"{database_name}."

    # Check is both source and target same or not
    is_same_source = bool(
        config
        and destination_connection_object
        and create_temp_table
        and check_is_same_source(config, destination_connection_object)
    )
    source_connection_type = config.get("connection_type")

    if level == "measure":
        category = measure.get("category")
        if category == CROSS_SOURCE:
            config.update({"connection_type": ConnectionType.Spark.value})

    # Prepare query selected attributes
    query_selected_attributes = asset_attributes if asset_attributes else []
    query_selected_attributes = [attribute.get("name") for attribute in query_selected_attributes]
    query_selected_attributes = get_attribute_names(dest_connection_type, query_selected_attributes)
    selected_attributes_query = ", ".join(query_selected_attributes) if query_selected_attributes else "*"

    metadata = get_metadata(config)
    config.update(
        {
            "failed_rows_schema": schema_name,
            "failed_rows_database": database_name,
            "delimiter": delimiter,
            "destination_config": destination_config,
            "destination_database": database,
            "destination_connection_object": destination_connection_object,
            "max_row_limit": export_row_limit,
            "max_column_limit": export_column_limit,
            "is_custom_measure": is_custom_measure,
            "export_category": export_category,
            "export_group_name": export_group,
            "metadata": metadata,
            "is_same_source": is_same_source,
            "selected_attributes_query": selected_attributes_query
        }
    )

    if not failed_rows_table or reset_failed_rows_table:
        failed_rows_table = get_failed_rows_table_name(config)
        is_reset = reset_failed_rows_table if reset_failed_rows_table else False
        if reset_failed_rows_table:
            try:
                table_name = f"{failed_rows_table}"
                drop_table = (
                    drop_table_query.replace("<table_name>", table_name)
                    .replace("<schema_name>", schema_name)
                    .replace("<database_name>", database)
                )
                _, native_connection = execute_native_query(
                    destination_config,
                    drop_table,
                    destination_connection,
                    True,
                    no_response=True,
                )
                if not destination_connection and native_connection:
                    destination_connection = native_connection
                is_reset = False
            except Exception as e:
                log_error("Drop table failed ", e)
        update_failed_rows_table_name(config, failed_rows_table, is_reset)
    if not failed_rows_table:
        raise Exception("Invalid table name.")

    export_group_table_name = get_failed_rows_tablename_by_export_group(
        config, export_group, level
    )
    if export_group_table_name:
        failed_rows_table = export_group_table_name
        if export_group == REPORT_EXPORT_GROUP_DOMAINS:
            update_failed_rows_table_name(
                config, failed_rows_table, False, REPORT_EXPORT_GROUP_DOMAINS
            )
    config.update({"failed_rows_table": failed_rows_table})

    has_schema = is_schema_exists(config, schema_query)
    if not has_schema:
        error_message = "No database or schema found in the source connection to export the failed rows data."
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error_message)
        update_queue_status(config, ScheduleStatus.Completed.value)
        raise Exception(error_message)

    create_base_tables(config, failed_rows_query)

    queue_id = config.get("queue_id")
    delete_query = failed_rows_query.get("delete")
    delete_query = (
        delete_query.replace("<schema_name>", schema_name)
        .replace("<failed_rows_table>", failed_rows_table)
        .replace("<run_id>", queue_id)
        .replace("<database_name>.", database_name)
    )
    _, native_connection = execute_native_query(
        destination_config, delete_query, destination_connection, True, no_response=True
    )
    if not destination_connection and native_connection:
        destination_connection = native_connection

    # prepare and execute failed rows query for each measure
    measures = get_measures(config)
    limit_condition_query = default_source_queries.get("limit_query")
    failed_measures = []
    max_columns_count = []

    if exception_outlier:
        extract_exception_outlier(config)

    # Track successfully processed measures for secondary destination metadata export
    processed_measures_for_secondary = []
    
    # Check if summarized report is enabled
    if summarized_report:
        params =  {
            "measures": measures,
            "failed_rows_query": failed_rows_query,
            "default_source_queries": default_source_queries,
            "default_destination_queries": default_destination_queries,
            "destination_connection": destination_connection,
            "exception_outlier": exception_outlier,
            "primary_attributes": primary_attributes,
            "asset_attributes": asset_attributes,
            "source_connection_type": source_connection_type,
            "destination_config": destination_config
        }
        failed_measures = process_summarized_failed_rows(config, params)
    else:
        for measure in measures:
            try:
                metadata = get_metadata(config, measure)
                config.update(
                    {
                        "max_row_limit": export_row_limit,
                        "max_column_limit": export_column_limit,
                        "metadata": metadata,
                    }
                )
                measure_id = measure.get("id")
                attribute_id = measure.get("attribute_id")
                selected_attribute = {}
                if attribute_id:
                    selected_attribute = get_selected_attributes(config, attribute_id)
                selected_attribute = selected_attribute if selected_attribute else {}
                measure_name = measure.get("technical_name")
                measure_type = measure.get("type")
                measure_category = measure.get("category")
                measure_level = measure.get("level")
                measure_name = measure.get("measure_name")
                temp_table_name = f"{measure_category}_{measure_id}"

                if measure_category == CROSS_SOURCE:
                    is_same_source = False
                    config.update({"is_same_source": is_same_source})
                
                export_limits = process_measure_export_limits(
                    measure, export_settings.export_row_limit, export_settings.export_column_limit
                )
                measure_export_row_limit = export_limits.measure_export_row_limit
                measure_export_column_limit = export_limits.measure_export_column_limit
                config.update(
                    {
                        "max_row_limit": measure_export_row_limit,
                        "max_column_limit": measure_export_column_limit,
                    }
                )
                max_columns_count.append(measure_export_column_limit)

                # Get Marked Normal Outlier Values
                if exception_outlier:
                    marked_normal_outlier_values = get_marked_normal_outlier_values(config, measure_id)
                    outlier_where_clause = prepare_marked_normal_where_clause(config, marked_normal_outlier_values, source_connection_type)
                else:
                    outlier_where_clause = None

                if is_same_source and (
                    dest_connection_type
                    in [ConnectionType.Athena.value]
                ):
                    is_same_source = False
                    config.update({"is_same_source": is_same_source})

                invalid_select_query, failed_rows_metadata, has_failed_rows_query = (
                    get_failed_row_query(config, measure, default_source_queries, is_direct_query_asset)
                )
                if not invalid_select_query:
                    continue
                check_selected_attributes = (
                    not has_failed_rows_query and measure_category == "conditional"
                )
                config.update({"check_selected_attributes": check_selected_attributes})

                if metadata and failed_rows_metadata:
                    failed_rows_metadata = update_metadata(failed_rows_metadata, metadata)
                
                failed_row_columns = list(failed_rows_metadata.keys())

                if not failed_row_columns or (
                    measure_level == "asset"
                    and str(measure_name).lower() == "duplicates"
                    and not primary_attributes
                ):
                    continue
                if not invalid_select_query:
                    continue
                if (
                        dest_connection_type
                        in (
                            ConnectionType.Redshift_Spectrum.value,
                            ConnectionType.Redshift.value,
                        )
                        and selected_attribute.get("derived_type") == "Bit"
                    ):
                    continue
                
                invalid_select_query_with_limit = ""
                if measure_export_row_limit and limit_condition_query:
                    if (
                        "order by" in invalid_select_query
                        and "order by (select 1)" in limit_condition_query
                    ):
                        limit_condition_query = limit_condition_query.replace(
                                "order by (select 1)", ""
                            )
                        connection_type = config.get("connection_type")
                    if not has_limit(connection_type, invalid_select_query):
                        if (
                            measure_category == "lookup"
                            and "<query_string>" in invalid_select_query
                        ):
                            invalid_select_query = f"{invalid_select_query}"
                        else:
                            if "distinct" not in invalid_select_query.lower():
                                from dqlabs.utils.export_failed_rows.measure_helpers import merge_limit_query
                                if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
                                    limit_query_to_merge = limit_condition_query.replace("<count>", str(measure_export_row_limit)) if limit_condition_query else limit_condition_query
                                else:
                                    limit_query_to_merge = limit_condition_query if limit_condition_query else ""
                                invalid_select_query = merge_limit_query(
                                    invalid_select_query, limit_query_to_merge, connection_type
                                )
                    if is_same_source:
                        invalid_select_query_with_limit = (
                            invalid_select_query.replace("<count>", "1")
                            .replace("<metadata_attributes>", "")
                            .replace("<query_attributes>", '*')
                        )
                        if (
                            str(measure_category).lower() in ["query", "parameter"]
                            or has_failed_rows_query
                        ) and str(asset_info.get("asset", {}).get("view_type", "")).lower() != "direct query":
                            invalid_select_query_with_limit = (
                                get_temp_table_columns_query.replace(
                                    "<temp_table_name>", temp_table_name
                                )
                                .replace("<schema_name>", schema_name)
                                .replace("<database_name>.", database_name)
                            )

                    invalid_select_query = invalid_select_query.replace(
                        "<count>", str(measure_export_row_limit)
                    )
                    if (
                        (measure_category == QUERY or measure_category == PARAMETER)
                        and is_direct_query_asset
                        and base_table_query
                    ):
                        invalid_select_query = process_direct_query(invalid_select_query, source_connection_type)
                        invalid_select_query = invalid_select_query.replace(
                            DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                        )
                        invalid_select_query_with_limit = process_direct_query(
                            invalid_select_query_with_limit, source_connection_type
                        )
                        invalid_select_query_with_limit = (
                            invalid_select_query_with_limit.replace(
                                DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                            )
                        )

                    if (
                        measure_category == "lookup"
                        and "<query_string>" in invalid_select_query
                    ):
                        (
                            invalid_select_query,
                            invalid_select_query_with_limit,
                            lookup_group_query,
                        ) = lookup_processing_query(
                            config,
                            measure,
                            invalid_select_query,
                            invalid_select_query_with_limit,
                            limit_condition_query,
                            measure_export_row_limit,
                            is_same_source,
                        )
                    else:
                        if measure_category == "lookup":
                            if ("count(*)" in invalid_select_query.lower()) and (
                                "count(*) as " not in invalid_select_query.lower()
                            ):
                                compiled = re.compile(re.escape("count(*)"), re.IGNORECASE)
                                invalid_select_query = compiled.sub(
                                    'count(*) as "COUNT(*)"', invalid_select_query
                                )
                                invalid_select_query_with_limit = compiled.sub(
                                    'count(*) as "COUNT(*)"',
                                    invalid_select_query_with_limit,
                                )

                # Apply Outlier Where Clause
                if outlier_where_clause:
                    if connection_type != ConnectionType.MongoDB.value:
                        invalid_select_query = append_where_to_query(invalid_select_query, outlier_where_clause)

                if (
                    is_same_source
                    and invalid_select_query
                    and str(asset_info.get("asset", {}).get("view_type", "")).lower() != "direct query"
                    and (str(measure_category).lower() in ["query", "parameter"] or has_failed_rows_query)
                ):
                    temp_table_create_query = create_temp_table
                    if dest_connection_type == ConnectionType.MSSQL.value and str(
                        invalid_select_query
                    ).lower().strip().startswith("with "):
                        temp_table_create_query = create_temp_view
                        invalid_select_query = invalid_select_query.replace("'", "''")
                    elif dest_connection_type == ConnectionType.SapHana.value:
                        invalid_select_query = invalid_select_query.replace("'", "''")
                    temp_table_create_query = (
                        temp_table_create_query
                        if temp_table_create_query
                        else create_temp_table
                    )

                    temp_table_query = (
                        temp_table_create_query.replace(
                            "<temp_table_name>", temp_table_name
                        )
                        .replace("<schema_name>", schema_name)
                        .replace("<database>", database)
                        .replace("<database_name>.", database_name)
                        .replace("<query_string>", invalid_select_query)
                    )
                    is_exists = True
                    if dest_connection_type == ConnectionType.Teradata.value:
                        query_string = failed_rows_query.get("metadata_table_exists")
                        query_string = query_string.replace(
                            "<db_name>", schema_name
                        ).replace("<table_name>", temp_table_name)
                        table_metadata, _ = execute_native_query(
                            config, query_string, None, True
                        )
                        is_exists = bool(table_metadata)

                    if temp_table_query and drop_table_query and is_exists:
                        temp_table_drop_query = drop_table_query
                        if dest_connection_type == ConnectionType.MSSQL.value and str(
                            invalid_select_query
                        ).lower().strip().startswith("with "):
                            temp_table_drop_query = drop_temp_view_query
                        drop_table = (
                            temp_table_drop_query.replace("<table_name>", temp_table_name)
                            .replace("<schema_name>", schema_name)
                            .replace("<database_name>", database)
                        )
                        _, native_connection = execute_native_query(
                            config, drop_table, source_connection, True, no_response=True
                        )
                        if not source_connection and native_connection:
                            source_connection = native_connection

                    if (
                        dest_connection_type in [ConnectionType.Synapse.value]
                        and "order by (select 1)" in temp_table_query
                    ):
                        temp_table_query = temp_table_query.replace(
                            "order by (select 1)", ""
                        )


                    _, native_connection = execute_native_query(
                        config,
                        temp_table_query,
                        source_connection,
                        True,
                        no_response=True,
                        convert_lower=False,
                    )
                    if not source_connection and native_connection:
                        source_connection = native_connection
                    config.update({"temp_table_name": temp_table_name})
                select_query = (
                    invalid_select_query_with_limit
                    if invalid_select_query_with_limit
                    else invalid_select_query
                )

                if connection_type == ConnectionType.Synapse.value:
                    if "order by (select 1)" in select_query:
                        select_query = select_query.replace(
                            "order by (select 1)", ""
                        )
                    default_failed_query = default_source_queries.get("failed_rows").get("default_failed_query")
                    select_query = default_failed_query.replace("<count>", str(export_row_limit)).replace("<query>", select_query)
                    
                unsupported_columns = get_unsupported_attributes(config)
                if is_same_source:
                    identifier_column_name = IDENTIFIER_KEY_COLUMN_NAME
                    identifier_column_name = (
                        identifier_column_name.lower()
                        if connection_type in [ConnectionType.Postgres.value, ConnectionType.AlloyDB.value]
                        else identifier_column_name
                    )
                    failed_rows, native_connection = execute_native_query(
                        config,
                        select_query,
                        source_connection,
                        is_list=True,
                        convert_lower=False,
                    )
                    if not source_connection and native_connection:
                        source_connection = native_connection
                    if not failed_rows:
                        continue

                    column_names = alter_table_schema(
                        config,
                        failed_rows_query,
                        default_source_queries,
                        failed_rows,
                        destination_connection,
                        default_destination_queries
                    )
                    if not column_names:
                        continue

                    table_columns = [
                        column.get("column_name")
                        for column in column_names
                        if str(column.get("column_name")) not in unsupported_columns
                    ]
                    row_data = failed_rows[0] if len(failed_rows) > 0 else {}
                    if not row_data:
                        continue
                    selected_data_columns = list(row_data.keys())
                    selected_data_columns = [
                        str(column) for column in selected_data_columns
                    ]

                    columns_to_insert = []
                    columns_to_select = []
                    metadata_column_names = []
                    metadata_values = []
                    boolean_column_name = []
                    for column in table_columns:
                        if (
                            measure_name.lower()
                            in ["distinct", "duplicate", "duplicate_count"]
                            and column.lower() == "row_number"
                        ):
                            continue
                        if column.lower() in failed_row_columns:
                            metadata_column_names.append(column)
                            value = failed_rows_metadata.get(column.lower())
                            """ If String Having single quotes and multi quotes remove empty those string """
                            double_escap_character = '"'
                            single_escap_character = "'"
                            backslash_escap_character = "\\'"
                            empty_string = ""
                            if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value]:
                                value = (
                                    f"""'{str(value).replace(double_escap_character, empty_string).replace(single_escap_character, backslash_escap_character)}'"""
                                    if value
                                    else "''"
                                )
                            else:
                                value = (
                                    f"""'{str(value).replace(double_escap_character, empty_string).replace(single_escap_character, "''")}'"""
                                    if value
                                    else "''"
                                )
                            if column.lower() == "created_date":
                                metadata_values.append(current_date_query)
                            elif column.lower() == "is_summarised":
                                bool_value = format_boolean_value(dest_connection_type, False)
                                metadata_values.append(f"{bool_value} AS {column}")  
                                # Set to false for non-summarized method
                            elif column.lower() == "measure_data":
                                # For non-summarized method, set measure_data to NULL
                                metadata_values.append(f"NULL AS {column}")
                            elif column.lower() == "failed_row_data":
                                # For non-summarized method, set failed_row_data to NULL
                                metadata_values.append(f"NULL AS {column}")
                            elif column.lower() == "exportrow_id":
                                # For non-summarized method, set exportrow_id to empty string
                                metadata_values.append(f"'' AS {column}")
                            else:
                                """Here We added Double quotes to column name some times column name having space and number issue happen"""
                                if dest_connection_type not in [ConnectionType.Hive.value]:
                                    metadata_values.append(
                                        f""" {value} AS {double_quotes}{column}{double_quotes} """
                                    )
                                else:
                                    metadata_values.append(
                                        f""" {value} AS {column} """
                                    )
                        elif column in selected_data_columns:
                            if dest_connection_type in [
                                ConnectionType.Redshift.value,
                                ConnectionType.Redshift_Spectrum.value,
                            ]:
                                column_datatype = next(
                                    (
                                        column_name
                                        for column_name in column_names
                                        if column_name.get("column_name") == column
                                    ),
                                    None,
                                )
                                column_datatype = (
                                    column_datatype.get("data_type")
                                    if column_datatype
                                    else ""
                                )
                                column_datatype = (
                                    column_datatype if column_datatype else "string"
                                )
                                column_datatype = (
                                    "numeric"
                                    if str(column_datatype).lower().startswith("numeric")
                                    else column_datatype
                                )
                                if column_datatype.lower() == "boolean":
                                    bool_column = get_attribute_names(
                                        dest_connection_type, [column]
                                    )
                                    boolean_column_name.append(bool_column[0])

                            if dest_connection_type in [ConnectionType.BigQuery.value]:
                                column_datatype = next(
                                    (
                                        column_name
                                        for column_name in column_names
                                        if column_name.get("column_name") == column
                                    ),
                                    None,
                                )
                                column_datatype = (
                                    column_datatype.get("data_type")
                                    if column_datatype
                                    else ""
                                )
                                column_datatype = (
                                    column_datatype if column_datatype else "string"
                                )
                                column_datatype = (
                                    "numeric"
                                    if str(column_datatype).lower().startswith("numeric")
                                    else column_datatype
                                )
                                columns_to_select.append(
                                    f"cast(`{column}` as {column_datatype})"
                                )
                                columns_to_insert.append(column)
                            elif dest_connection_type in [ConnectionType.Oracle.value]:
                                column_datatype = next(
                                    (
                                        column_name
                                        for column_name in column_names
                                        if column_name.get("column_name") == column
                                    ),
                                    None,
                                )
                                column_datatype = (
                                    column_datatype.get("data_type")
                                    if column_datatype
                                    else ""
                                )
                                column_datatype = (
                                    column_datatype if column_datatype else "string"
                                )
                                column_datatype = (
                                    "string"
                                    if str(column_datatype).lower() == "clob"
                                    else column_datatype
                                )
                                columns_to_select.append(
                                    f"{column} as {column_datatype}"
                                )
                                column = get_attribute_names(dest_connection_type, [column])
                                columns_to_insert.append(column[0])
                            else:
                                column = get_attribute_names(dest_connection_type, [column])
                                columns_to_insert.append(column[0])
                                columns_to_select.append(column[0])
                    columns_to_insert = columns_to_insert[:int(config.get("max_column_limit"))]
                    columns_to_select = columns_to_select[:int(config.get("max_column_limit"))]

                    # Start - Prepare identifier key query

                    identifier_key_columns = []
                    if primary_attributes:
                        selected_columns = deepcopy(table_columns) if table_columns else []
                        for column in primary_attributes:
                            if column not in selected_columns:
                                continue
                            identifier_key_columns.append(column)

                        if selected_attribute:
                            attribute_name = selected_attribute.get("attribute_name")
                            if (
                                attribute_name not in identifier_key_columns
                                and attribute_name in selected_columns
                            ):
                                identifier_key_columns.append(attribute_name)

                        if identifier_key_columns:
                            identifier_key_columns = list(set(identifier_key_columns))
                            identifier_key_columns = get_attribute_names(
                                dest_connection_type, identifier_key_columns
                            )

                    if measure_category and (
                        measure_category.lower() in ["query", "lookup", "parameter"]
                    ):
                        identifier_key_columns = []
                    else:
                        """This was used for coditional method in Failed row query we need to push all column in identifier so we added"""
                        validate_failed_query = measure.get("properties", {}).get(
                            "failed_rows_query", {}
                        )
                        if validate_failed_query.get("is_enabled", False):
                            identifier_key_columns = []
                    if dest_connection_type == ConnectionType.Databricks.value:
                            identifier_key_columns = []
                    if not identifier_key_columns:
                        identifier_key_columns = deepcopy(columns_to_insert)

                    identifier_key_columns.sort()
                    """ Identifier Key only need to push so we update that one """
                    columns_to_insert = deepcopy(identifier_key_columns)
                    columns_to_select = deepcopy(identifier_key_columns)
                    """ Here We added Double quotes to column name some times column name having space and number issue happen """
                    if dest_connection_type not in [ConnectionType.Databricks.value, ConnectionType.Hive.value]:
                        identifier_key_columns = [
                            f"""{double_quotes}{str(currentColumn).strip()}{double_quotes}"""
                            for currentColumn in identifier_key_columns
                        ]
                        boolean_column_name = [
                            f"""{double_quotes}{str(currentColumn).strip()}{double_quotes}"""
                            for currentColumn in boolean_column_name
                        ]
                    concat_query_string = ""
                    if (
                        identifier_key_columns
                        and measure_category == "lookup"
                        and lookup_group_query
                        and lookup_group_query in invalid_select_query
                    ):
                        identifier_key_columns = [
                            "t1." + str(suit).strip() for suit in identifier_key_columns
                        ]

                    if dest_connection_type in [
                        ConnectionType.SapHana.value,
                        ConnectionType.MSSQL.value,
                        ConnectionType.Redshift.value,
                        ConnectionType.BigQuery.value,
                        ConnectionType.Redshift_Spectrum.value,
                        ConnectionType.Db2.value,
                        ConnectionType.Oracle.value,
                        ConnectionType.DB2IBM.value,
                        ConnectionType.Teradata.value,
                    ]:
                        if measure_category == "lookup":
                            identifier_key_columns = [
                                column
                                for column in identifier_key_columns
                                if column.replace("t1.", "") not in boolean_column_name
                            ]
                        else:
                            identifier_key_columns = [
                                column
                                for column in identifier_key_columns
                                if column not in boolean_column_name
                            ]
                        identifier_key_columns.sort()

                        if identifier_key_columns and len(identifier_key_columns) > 1:
                            if dest_connection_type == ConnectionType.Db2.value:
                                concat_query_string = (
                                    f"::varchar || '{delimiter}' || ".join(
                                        identifier_key_columns
                                    )
                                )
                                concat_query_string = (
                                    f"""({concat_query_string}::varchar), ''"""
                                )
                            elif dest_connection_type == ConnectionType.DB2IBM.value:
                                concat_query_string = f"|| '{delimiter}' || ".join(
                                    identifier_key_columns
                                )
                            elif dest_connection_type == ConnectionType.SapHana.value:
                                concat_query_string = f" || '{delimiter}' || ".join(
                                    identifier_key_columns
                                )
                                concat_query_string = f""""""
                                temp_index = 1
                                for identifier_key_column in identifier_key_columns:
                                    concat_query_string = f"""{concat_query_string} case when {identifier_key_column} is not null then CAST({identifier_key_column} AS VARCHAR) else '' end"""
                                    if temp_index != len(identifier_key_columns):
                                        concat_query_string = (
                                            f"""{concat_query_string} || '{delimiter}' ||"""
                                        )
                                    temp_index = temp_index + 1
                            elif dest_connection_type == ConnectionType.Teradata.value:
                                temp_index = 1
                                for identifier_key_column in identifier_key_columns:
                                    concat_query_string += f"COALESCE(CAST({identifier_key_column} AS VARCHAR(255)), '')"
                                    if temp_index != len(identifier_key_columns):
                                        concat_query_string = (
                                            f"""{concat_query_string}, ' {delimiter} ',"""
                                        )
                                    temp_index = temp_index + 1
                            elif dest_connection_type == ConnectionType.BigQuery.value:
                                concat_query_string = f""""""
                                for columncontent in identifier_key_columns:
                                    column_datatype = next(
                                        (
                                            column_name_info
                                            for column_name_info in column_names
                                            if column_name_info.get("column_name") == columncontent
                                        ),
                                        None,
                                    )
                                    column_datatype = (
                                        column_datatype.get("data_type")
                                        if column_datatype
                                        else "string"
                                    )
                                    column_datatype = (
                                        "numeric"
                                        if str(column_datatype).lower().startswith("numeric")
                                        else column_datatype
                                    )
                                    concat_query_string = f"""{concat_query_string}cast({columncontent} as {column_datatype}), '{delimiter}', """
                                concat_query_string = concat_query_string[:-7]
                                concat_query_string = f"""{concat_query_string}"""
                            elif dest_connection_type == ConnectionType.Oracle.value:
                                concat_query_string = f" || '{delimiter}' || ".join(
                                    identifier_key_columns
                                )
                            elif dest_connection_type == ConnectionType.MSSQL.value:
                                concat_query_string = f", '{delimiter}', ".join(
                                    identifier_key_columns
                                )
                            else:
                                concat_query_string = f"::text || '{delimiter}' || ".join(
                                    identifier_key_columns
                                )
                                concat_query_string = (
                                    f"""({concat_query_string}), ''"""
                                )
                        elif identifier_key_columns and len(identifier_key_columns) == 1:
                            if dest_connection_type == ConnectionType.BigQuery.value:
                                # Find the datatype for this column
                                column_name = "".join(identifier_key_columns)
                                column_datatype = next(
                                    (
                                        column_name_info
                                        for column_name_info in column_names
                                        if column_name_info.get("column_name") == column_name
                                    ),
                                    None,
                                )
                                column_datatype = (
                                    column_datatype.get("data_type")
                                    if column_datatype
                                    else "string"
                                )
                                column_datatype = (
                                    "numeric"
                                    if str(column_datatype).lower().startswith("numeric")
                                    else column_datatype
                                )
                                concat_query_string = f""" cast({column_name} as {column_datatype}) """
                                concat_query_string = f"""{concat_query_string}"""
                            else:
                                concat_query_string = "".join(identifier_key_columns)

                    identifier_key_query = ""
                    if (
                        identifier_key_columns
                        and len(identifier_key_columns) > 1
                        and concat_query
                    ):
                        if dest_connection_type in [
                            ConnectionType.SapHana.value,
                            ConnectionType.MSSQL.value,
                            ConnectionType.BigQuery.value,
                            ConnectionType.Redshift.value,
                            ConnectionType.Redshift_Spectrum.value,
                            ConnectionType.Db2.value,
                            ConnectionType.Oracle.value,
                            ConnectionType.DB2IBM.value,
                            ConnectionType.Teradata.value,
                        ]:
                            identifier_key_query = (
                                concat_query.replace("<delimiter>", "")
                                .replace("<trim_delimiter>", delimiter)
                                .replace("<attributes>", concat_query_string)
                            )
                        else:
                            if "<delimiter>" not in concat_query and delimiter:
                                delimiters = [f"'{delimiter}'"] * len(
                                    identifier_key_columns
                                )
                                delimited_identifiers = zip(
                                    identifier_key_columns, delimiters
                                )
                                identifier_key_columns = [
                                    x
                                    for delimited_identifier in delimited_identifiers
                                    for x in delimited_identifier
                                ]
                                identifier_key_columns = identifier_key_columns[:-1]
                            identifier_key_columns.sort()
                            if concatenate_attribute:
                                identifier_key_columns = [
                                    concatenate_attribute.replace("<attribute>", x)
                                    for x in identifier_key_columns
                                ]
                            identifier_key_column = ", ".join(identifier_key_columns)
                            identifier_key_column = (
                                identifier_key_column if identifier_key_column else ""
                            )
                            identifier_key_query = (
                                concat_query.replace("<delimiter>", delimiter)
                                .replace("<trim_delimiter>", delimiter)
                                .replace("<attributes>", identifier_key_column)
                            )
                    elif identifier_key_columns and len(identifier_key_columns) == 1:
                        identifier_key_query = "".join(identifier_key_columns)

                    if identifier_key_query:
                        if identifier_column_name not in " ".join(metadata_values):
                            identifier_column = get_attribute_names(
                                dest_connection_type, [identifier_column_name]
                            )
                            metadata_values.append(
                                f"{identifier_key_query} AS {identifier_column[0]}"
                            )
                        if identifier_column_name not in " ".join(metadata_column_names):
                            metadata_column_names.append(identifier_column_name)
                    # End - Prepare identifier key query

                    table_column_names = get_attribute_names(
                        dest_connection_type, metadata_column_names
                    )
                    table_column_names = [*table_column_names, *columns_to_insert]
                    """ Here We added Double quotes to column name some times column name having space and number issue happen """
                    if dest_connection_type not in [ConnectionType.Databricks.value, ConnectionType.Hive.value]:
                        table_column_names = [
                            f"""{double_quotes}{str(currentColumn).strip()}{double_quotes}"""
                            for currentColumn in table_column_names
                        ]
                    columns_to_insert = ", ".join(table_column_names)

                    metadata_attributes = ""
                    if metadata_values:
                        metadata_attributes = ", ".join(metadata_values)
                        metadata_attributes = (
                            f"{metadata_attributes}, " if metadata_attributes else ""
                        )

                    query_attributes = ""
                    if columns_to_select:
                        """Here We added Double quotes to column name some times column name having space and number issue happen"""

                        """ This conditions we convert all collumn in string in bigquery insert """
                        if dest_connection_type == ConnectionType.BigQuery.value:
                            columns_to_select_with_datatype = []
                            for currentColumn in columns_to_select:
                                column_name = str(currentColumn).strip()
                                if column_name.startswith("cast(`") and "` as " in column_name:
                                    columns_to_select_with_datatype.append(f"""{column_name}""")
                                else:
                                    column_datatype = next(
                                        (
                                            column_name_info
                                            for column_name_info in column_names
                                            if column_name_info.get("column_name") == column_name
                                        ),
                                        None,
                                    )
                                    column_datatype = (
                                        column_datatype.get("data_type")
                                        if column_datatype
                                        else "string"
                                    )
                                    column_datatype = (
                                        "numeric"
                                        if str(column_datatype).lower().startswith("numeric")
                                        else column_datatype
                                    )
                                    column_name = column_name if column_name.startswith("`") else f"`{column_name}`"
                                    columns_to_select_with_datatype.append(
                                        f"""cast({column_name} as {column_datatype})"""
                                    )
                            columns_to_select = columns_to_select_with_datatype
                        elif dest_connection_type == ConnectionType.Db2.value:
                            columns_to_select = [
                                f"""cast({str(currentColumn).strip()} as varchar)"""
                                for currentColumn in columns_to_select
                            ]
                        elif dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value]:
                            columns_to_select = [
                                f"""{str(currentColumn).strip()}"""
                                for currentColumn in columns_to_select
                            ]
                        else:
                            columns_to_select = [
                                f"""{double_quotes}{str(currentColumn).strip()}{double_quotes}"""
                                for currentColumn in columns_to_select
                            ]
                        if (
                            measure_category == "lookup"
                            and lookup_group_query
                            and lookup_group_query in invalid_select_query
                        ):
                            """Stating of this code Bigquery already casted so we do conditional check update seprately"""
                            if dest_connection_type in [
                                ConnectionType.BigQuery.value,
                                ConnectionType.Db2.value,
                            ]:
                                columns_to_select = [
                                    str(suit).strip().replace("cast(", "cast(t1.")
                                    for suit in columns_to_select
                                ]
                            else:
                                columns_to_select = [
                                    "t1." + str(suit).strip() for suit in columns_to_select
                                ]
                        query_attributes = ", ".join(columns_to_select)
                        query_attributes = (
                            f"{query_attributes} " if query_attributes else ""
                        )

                    if not query_attributes and str(metadata_attributes).strip().endswith(
                        ","
                    ):
                        metadata_attributes = str(metadata_attributes).strip()[:-1]
                    invalid_select_query = invalid_select_query.replace(
                        "<metadata_attributes>", metadata_attributes
                    ).replace("<query_attributes>", query_attributes)
                    if measure_category == "lookup" and lookup_group_query:
                        invalid_select_query = invalid_select_query.replace(
                            lookup_group_query,
                            f"""{metadata_attributes} {query_attributes}""",
                        )

                    columns_to_insert = (
                        columns_to_insert.lower()
                        if columns_to_insert
                        and connection_type in [ConnectionType.Postgres.value, ConnectionType.AlloyDB.value]
                        else columns_to_insert
                    )
                    insert_failed_rows_query_with_select = (
                        str(deepcopy(invalid_insert_query_with_select))
                        .replace("<failed_rows_table>", failed_rows_table)
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>.", database_name)
                        .replace("<columns>", columns_to_insert)
                        .replace("<failed_query>", invalid_select_query)
                    )
                    if str(measure_category).lower() in ["query", "parameter"] or has_failed_rows_query:
                        if str(asset_info.get("asset", {}).get("view_type", "")).lower() == "direct query":
                            temp_table_name = (
                                f"""({asset_info.get("asset", {}).get("query", "")}) as direct_query_table"""
                            )
                            if dest_connection_type and str(
                                dest_connection_type
                            ).lower() in [
                                ConnectionType.Oracle.value.lower(),
                                ConnectionType.BigQuery.value.lower(),
                            ]:
                                temp_table_name = f"""({asset_info.get("asset", {}).get("query", "")})"""
                            insert_from_temp_table = insert_failed_rows_data_direct_query

                            if dest_connection_type == ConnectionType.Oracle.value:
                                query_attributes = ", ".join(
                                    f"TO_CHAR({col})" if "DATE" in col.upper() or "TIMESTAMP" in col.upper() 
                                    else col for col in columns_to_select
                                )
                        insert_failed_rows_query_with_select = (
                            insert_from_temp_table.replace(
                                "<failed_rows_table>", failed_rows_table
                            )
                            .replace("<temp_table_name>", temp_table_name)
                            .replace("<schema_name>", schema_name)
                            .replace("<database_name>.", database_name)
                            .replace("<columns>", columns_to_insert)
                            .replace("<metadata_attributes>", metadata_attributes)
                            .replace("<query_attributes>", query_attributes)
                            .replace("<failed_direct_query>", invalid_select_query)
                        )
                    if source_connection:
                        destination_connection = source_connection

                    if (
                        dest_connection_type in [ConnectionType.Synapse.value]
                        and "order by (select 1)" in insert_failed_rows_query_with_select
                    ):
                        insert_failed_rows_query_with_select = (
                            insert_failed_rows_query_with_select.replace(
                                "order by (select 1)", ""
                            )
                        )

                    _, native_connection = execute_native_query(
                        destination_config,
                        insert_failed_rows_query_with_select,
                        destination_connection,
                        True,
                        no_response=True,
                        convert_lower=False,
                    )
                    if not destination_connection and native_connection:
                        destination_connection = native_connection

                    if (
                        temp_table_name
                        and drop_table_query
                        and str(asset_info.get("asset", {}).get("view_type", "")).lower() != "direct query"
                        and (
                            str(measure_category).lower() in ["query", "parameter"]
                            or has_failed_rows_query
                        )
                    ):
                        temp_table_drop_query = drop_table_query
                        if dest_connection_type == ConnectionType.MSSQL.value and str(
                            invalid_select_query
                        ).lower().strip().startswith("with "):
                            temp_table_drop_query = drop_temp_view_query
                        drop_table = (
                            temp_table_drop_query.replace("<table_name>", temp_table_name)
                            .replace("<schema_name>", schema_name)
                            .replace("<database_name>", database)
                        )
                        execute_native_query(
                            config, drop_table, source_connection, True, no_response=True
                        )
                else:
                    if measure_category == CROSS_SOURCE:
                        is_create_table = False if exception_outlier else True
                        failed_rows_data = execute_export_failed_rows_query(config, measure, select_query, is_create_table=is_create_table)
                    else:
                        failed_rows_data, native_connection = execute_native_query(
                            config,
                            select_query,
                            source_connection,
                            is_list=True,
                            convert_lower=False,
                            parameters= {"run_query": True},
                        )
                    failed_rows_data = [failed_rows_data] if failed_rows_data else []
                    if not source_connection and native_connection:
                        source_connection = native_connection
                    column_names = []
                    columns_to_insert = ""
                    for failed_rows in failed_rows_data:
                        if not failed_rows:
                            continue

                        if not column_names:
                            column_names = alter_table_schema(
                                config,
                                failed_rows_query,
                                default_source_queries,
                                failed_rows,
                                destination_connection,
                                default_destination_queries,
                            )
                            table_columns = [
                                column.get("column_name") for column in column_names
                            ]
                            table_column_names = get_attribute_names(
                                dest_connection_type, table_columns
                            )
                            """ Here We added Double quotes to column name some times column name having space and number issue happen """
                            if dest_connection_type not in [ConnectionType.Databricks.value, ConnectionType.Hive.value]:
                                table_column_names = [
                                    f"""{double_quotes}{str(currentColumn).strip()}{double_quotes}"""
                                    for currentColumn in table_column_names
                                ]
                            
                            # For ASSET_DATA table, we DON'T apply case fixing to INSERT columns since they should match existing table schema
                            
                            columns_to_insert = ", ".join(table_column_names)

                        if not columns_to_insert:
                            break


                        insert_failed_rows_query = (
                            str(deepcopy(invalid_insert_query))
                            .replace("<failed_rows_table>", failed_rows_table)
                            .replace("<schema_name>", schema_name)
                            .replace("<database_name>.", database_name)
                            .replace("<columns>", columns_to_insert)
                        )

                        insert_failed_rows_values = []
                        row_level_columns = failed_rows[0] if failed_rows else {}
                        row_level_columns = (
                            dict(row_level_columns) if row_level_columns else {}
                        )
                        row_level_columns = (
                            list(row_level_columns.keys()) if row_level_columns else []
                        )
                        row_level_columns = [
                            row_level_column
                            for row_level_column in row_level_columns
                            if str(row_level_column).lower() not in unsupported_columns
                        ]
                        query_columns = []
                        if row_level_columns:
                            row_level_columns = [
                                str(column).lower() for column in row_level_columns
                            ]
                            query_columns = row_level_columns[:measure_export_column_limit]

                        for row in failed_rows:
                            row_input = []
                            row = convert_to_lower(row, True)
                            identifier_keys = []
                            for key in row:
                                if key not in query_columns:
                                    continue
                                value = row.get(key)
                                if dest_connection_type in [
                                    ConnectionType.Databricks.value,
                                    ConnectionType.Hive.value
                                ]:
                                    value = (
                                        str(value).replace('"', "").replace("'", "\\'")
                                        if not (value is None)
                                        else "null"
                                    )
                                else:
                                    value = (
                                        str(value).replace('"', "").replace("'", "''")
                                        if not (value is None)
                                        else "null"
                                    )
                                identifier_keys.append(value)
                            identifier_key = f"{delimiter}".join(identifier_keys)
                            row.update({"identifier_key": identifier_key})

                            for column in column_names:
                                value = None
                                column_name = column.get("column_name")
                                if (
                                    measure_name
                                    in ["distinct", "duplicate", "duplicate_count"]
                                    and column_name.lower() == "row_number"
                                ):
                                    continue
                                datatype = column.get("data_type")
                                derived_type = get_derived_type(dq_datatypes, datatype)
                                derived_type = (
                                    str(derived_type).lower() if derived_type else ""
                                )
                                column_name = column_name.lower()
                                if column_name == "created_date":
                                    value = current_date
                                elif column_name == "is_summarised":
                                    value = format_boolean_value(dest_connection_type, False)
                                elif column_name == "measure_data":
                                    # For non-summarized method, set measure_data to NULL
                                    value = "null"
                                elif column_name == "failed_row_data":
                                    if source_connection_type and source_connection_type.lower() == ConnectionType.MongoDB.value.lower():
                                        try:
                                            row_data_for_json = {k: v for k, v in row.items() 
                                                                if k not in ['identifier_key'] 
                                                                and k not in failed_row_columns}
                                            failed_row_json = json.dumps(row_data_for_json, default=str)
                                            if dest_connection_type in [
                                                ConnectionType.Databricks.value,
                                                ConnectionType.Hive.value
                                            ]:
                                                failed_row_json = failed_row_json.replace("'", "\\'")
                                            else:
                                                failed_row_json = failed_row_json.replace("'", "''")
                                            value = f"""'{failed_row_json}'"""
                                        except Exception:
                                            value = "null"
                                    else:
                                        value = "null"
                                elif column_name == "exportrow_id":
                                    # For non-summarized method, set exportrow_id to empty string
                                    value = "''"
                                elif column_name.lower() in unsupported_columns:
                                    value = "null"
                                elif column_name in failed_row_columns:
                                    value = failed_rows_metadata.get(column_name)
                                    if value is not None:
                                        if dest_connection_type in [
                                            ConnectionType.Databricks.value,
                                            ConnectionType.Hive.value
                                        ]:
                                            value = (
                                                str(value)
                                                .replace('"', "")
                                                .replace("'", "\\'")
                                            )
                                        else:
                                            value = (
                                                str(value)
                                                .replace('"', "")
                                                .replace("'", "''")
                                            )
                                        value = f"""'{value}'"""
                                    else:
                                        value = "null"
                                else:
                                    if (
                                        column_name in row_level_columns
                                        and column_name not in query_columns
                                    ):
                                        row_input.append("null")
                                        continue
                                    value = row.get(column_name)
                                    if derived_type in ["integer", "numeric"]:
                                        if value is not None and value in [True, False]:
                                            value = int(value)
                                        value = str(value) if value is not None else "null"
                                        value = (
                                            str(value).replace('"', "").replace("'", "")
                                            if value.replace(".", "", 1).isdigit()
                                            else "null"
                                        )
                                    elif derived_type in ["bit"]:
                                        if value is not None:
                                            value = int(value)
                                        value = (
                                            str(value).replace('"', "").replace("'", "")
                                            if value is not None
                                            else "null"
                                        )
                                    else:
                                        if text_value_query:
                                            value = (
                                                text_value_query.replace(
                                                    "<value>",
                                                    str(value)
                                                    .replace('"', "")
                                                    .replace("'", ""),
                                                )
                                                if value is not None
                                                else "null"
                                            )
                                        else:
                                            if value is not None:
                                                value = (
                                                    str(value)
                                                    .replace('"', "")
                                                    .replace("'", "")
                                                )
                                                value = f"""'{value}'"""
                                            else:
                                                value = "null"
                                row_input.append(value)

                            if dest_connection_type == ConnectionType.Teradata.value:
                                query_input = f"""{insert_failed_rows_query} VALUES ({",".join(row_input)})"""
                            elif dest_connection_type == ConnectionType.Oracle.value:
                                query_input = f"""{insert_values_query} VALUES ({",".join(row_input)})"""
                            elif dest_connection_type == ConnectionType.Synapse.value:
                                query_input = f"""{insert_values_query if insert_values_query else ''} SELECT {",".join(row_input)} UNION ALL"""
                            elif dest_connection_type == ConnectionType.SapHana.value:
                                input_rows = ",".join(row_input)
                                query_input = insert_values_query.replace(
                                    "<values>", input_rows
                                )
                            else:
                                query_input = f"""({",".join(row_input)})"""
                            insert_failed_rows_values.append(query_input)
                        insert_failed_rows_input = split_queries(insert_failed_rows_values, export_batch_size)
                        for input_values in insert_failed_rows_input:
                            if dest_connection_type == ConnectionType.Teradata.value:
                                query_input = "\n; ".join(input_values)
                                query_string = f"{query_input} \n;" if query_input else ""
                            elif dest_connection_type == ConnectionType.Synapse.value:
                                query_input = " ".join(input_values)
                                query_string = f"{query_input}" if query_input else ""
                                if query_string:
                                    query_string = (
                                        f"{insert_failed_rows_query} {query_input}"
                                    )
                                    query_string = query_string[:-10]
                            elif dest_connection_type == ConnectionType.Oracle.value:
                                query_input = "\n ".join(input_values)
                                query_string = f"{query_input} \n" if query_input else ""
                                if query_string:
                                    query_string = str(
                                        deepcopy(invalid_insert_query)
                                    ).replace("<values>", query_string)
                                    query_string = (
                                        query_string.replace(
                                            "<failed_rows_table>", failed_rows_table
                                        )
                                        .replace("<schema_name>", schema_name)
                                        .replace("<database_name>.", database_name)
                                        .replace("<columns>", columns_to_insert)
                                    )
                            elif dest_connection_type == ConnectionType.SapHana.value:
                                query_input = "\n ".join(input_values)
                                query_string = f"{query_input} \n" if query_input else ""
                                if query_string:
                                    query_string = str(
                                        deepcopy(invalid_insert_query)
                                    ).replace("<insert_queries>", query_string)
                                    query_string = (
                                        query_string.replace(
                                            "<failed_rows_table>", failed_rows_table
                                        )
                                        .replace("<schema_name>", schema_name)
                                        .replace("<database_name>.", database_name)
                                        .replace("<columns>", columns_to_insert)
                                    )
                            else:
                                query_input = ",".join(input_values)
                                query_string = (
                                    f"{insert_failed_rows_query} VALUES {query_input}"
                                )

                            if query_string:
                                _, native_connection = execute_native_query(
                                    destination_config,
                                    query_string,
                                    destination_connection,
                                    True,
                                    no_response=True,
                                    convert_lower=False,
                                )
                                if not destination_connection and native_connection:
                                    destination_connection = native_connection
            except Exception as e:
                logger.error(f"Error processing measure {measure_id}: {str(e)}", exc_info=True)

    if measures:
        refresh_failed_rows(config, failed_rows_query, destination_connection, destination_config)
        update_failed_rows_stat(config)
    
    # IMPROVED: Use error helper for consistent exception creation
    if failed_measures:
        logger.info(f"Processing completed: {len(failed_measures)}/{len(measures)} successful")
    
    metadata_is_active = export_settings.report_settings.get('metadata_is_active', False)
    if not metadata_is_active:
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_queue_status(config, ScheduleStatus.Completed.value)





def process_summarized_failed_rows(config: dict, params: dict) -> list:

    measures = params.get("measures", [])
    failed_rows_query = params.get("failed_rows_query", "")
    default_source_queries = params.get("default_source_queries", {})
    default_destination_queries = params.get("default_destination_queries", {})
    destination_connection = params.get("destination_connection", {})
    exception_outlier = params.get("exception_outlier", False)
    primary_attributes = params.get("primary_attributes", [])
    selected_attribute = params.get("asset_attributes", [])
    source_connection_type = params.get("source_connection_type", "")
    destination_config = params.get("destination_config")
    export_settings = get_complete_export_settings(config)
    source_connection = None
    failed_rows_table = config.get("failed_rows_table")
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    report_connection = export_settings.report_settings.get("connection", {})
    destination_connection_type = report_connection.get("type")
    base_table_query = config.get("base_table_query")
    is_direct_query_asset = check_is_direct_query(config)

    # Get queries
    current_date_query = failed_rows_query.get("current_date", "CURRENT_TIMESTAMP")
    current_date = str(current_date_query.split("AS")[0]).strip() if "AS" in current_date_query else current_date_query
    insert_failed_rows_query = failed_rows_query.get("insert_failed_rows_data", "")
    insert_values_query = failed_rows_query.get("insert_values", "")
    summarize_insert_query = failed_rows_query.get("summary_failed_insert_query", "")
    
    measure_list = [measure.get("id") for measure in measures]
    measure_metadata = get_summary_measure_data(config, measure_list)
    
    # Get unsupported columns
    unsupported_columns = get_unsupported_attributes(config)

    # Use defaultdict to group by identifier key
    identifier_groups = defaultdict(lambda: {
        "metadata": {},
        "measures": [],
        "failed_row_data": None
    })
    for measure in measures:
        semantic_metadata = get_metadata(config, measure)
        measure_id = measure.get("id")
        measure_name = measure.get("measure_name")
        
        metadata = next((x for x in measure_metadata if x.get("measure_id") == measure_id), None)
        measure_level = measure.get("level")
        measure_category = measure.get("category")
        measure_type = measure.get("type")
        attribute_id = measure.get("attribute_id")

        # Get Marked Normal Outlier Values
        if exception_outlier:
            marked_normal_outlier_values = get_marked_normal_outlier_values(config, measure_id)
            outlier_where_clause = prepare_marked_normal_where_clause(config, marked_normal_outlier_values, source_connection_type)
        else:
            outlier_where_clause = None

        
        export_limits = process_measure_export_limits(
            measure, export_settings.export_row_limit, export_settings.export_column_limit
        )
        measure_export_row_limit = export_limits.measure_export_row_limit
        export_column_limit = export_limits.measure_export_column_limit

        # Prepare Failed Row Query
        invalid_select_query, failed_rows_metadata, has_failed_rows_query = (
            get_failed_row_query(config, measure, default_source_queries, False)
        )

        if semantic_metadata and failed_rows_metadata:
            failed_rows_metadata = update_metadata(failed_rows_metadata, semantic_metadata)

        if not invalid_select_query:
            continue
        
        if (
            measure_level == "asset"
            and str(measure_name).lower() == "duplicates"
            and not primary_attributes
        ):
            continue
            

        # Apply limits to the query
        limit_condition_query = default_source_queries.get("limit_query")
        if measure_export_row_limit and limit_condition_query:
            if (
                "order by" in invalid_select_query
                and "order by (select 1)" in limit_condition_query
            ):
                limit_condition_query = limit_condition_query.replace(
                    "order by (select 1)", ""
                )
            connection_type = config.get("connection_type")
            if not has_limit(connection_type, invalid_select_query):
                if (
                    measure_category == "lookup"
                    and "<query_string>" in invalid_select_query
                ):
                    invalid_select_query = f"{invalid_select_query}"
                else:
                    if "distinct" not in invalid_select_query.lower():
                        from dqlabs.utils.export_failed_rows.measure_helpers import merge_limit_query
                        if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
                            limit_query_to_merge = limit_condition_query.replace("<count>", str(measure_export_row_limit)) if limit_condition_query else limit_condition_query
                        else:
                            limit_query_to_merge = limit_condition_query if limit_condition_query else ""
                        invalid_select_query = merge_limit_query(
                            invalid_select_query, limit_query_to_merge, connection_type
                        )
            invalid_select_query = invalid_select_query.replace(
                "<count>", str(measure_export_row_limit)
            )
            
            # Add lookup processing logic for lookup measures
            if (
                measure_category == "lookup"
                and "<query_string>" in invalid_select_query
            ):
                (
                    invalid_select_query,
                    invalid_select_query_with_limit,
                    lookup_group_query,
                ) = lookup_processing_query(
                    config,
                    measure,
                    invalid_select_query,
                    "",
                    limit_condition_query,
                    measure_export_row_limit,
                    False,
                )
            else:
                if measure_category == "lookup":
                    if ("count(*)" in invalid_select_query.lower()) and (
                        "count(*) as " not in invalid_select_query.lower()
                    ):
                        compiled = re.compile(re.escape("count(*)"), re.IGNORECASE)
                        invalid_select_query = compiled.sub(
                            'count(*) as "COUNT(*)"', invalid_select_query
                        )


            if (
                (measure_category == QUERY or measure_category == PARAMETER)
                and is_direct_query_asset
                and base_table_query
            ):
                invalid_select_query = invalid_select_query.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )
        
        metadata_attributes = ""
        query_attributes = "*"
        invalid_select_query = invalid_select_query.replace(
            "<metadata_attributes>", metadata_attributes
        ).replace("<query_attributes>", query_attributes)

            # Apply Outlier Where Clause
        if outlier_where_clause:
            if source_connection_type != ConnectionType.MongoDB.value:
                invalid_select_query = append_where_to_query(invalid_select_query, outlier_where_clause)

        # Get Failed Row Data - this will include all columns from the source table
        native_connection = None
        if measure_category == CROSS_SOURCE:
            is_create_table = False if exception_outlier else True
            failed_rows = execute_export_failed_rows_query(config, measure, invalid_select_query, is_create_table=is_create_table)
        else:
            failed_rows, native_connection = execute_native_query(
                config, invalid_select_query, source_connection, is_list=True, convert_lower=False
            )

        if not source_connection and native_connection:
            source_connection = native_connection
            
        if not failed_rows:
            continue

        # Get selected attributes
        selected_attribute = {}
        if attribute_id:
            selected_attribute = get_selected_attributes(config, attribute_id)
        selected_attribute = selected_attribute if selected_attribute else {}

        # Handle Failed Rows Data
        for row in failed_rows:
            # Determine identifier columns
            identifier_key_columns = []
            row_keys = list(row.keys())
            row_keys = [col for col in row_keys if col not in ["row_number"]]
            
            # For query/parameter measures, use all columns (up to limit)
            if measure_category in ["query", "parameter"]:
                identifier_key_columns = [col for col in row_keys if col not in unsupported_columns][:export_column_limit]
            elif not identifier_key_columns:
                identifier_key_columns = [col for col in row_keys if col not in unsupported_columns][:export_column_limit]
            
            # Generate identifier key
            identifier_parts = []
            for col in sorted(identifier_key_columns):
                value = row.get(col)
                if value is None:
                    value = ""
                identifier_parts.append(str(value))
            identifier_key = "|".join(identifier_parts)
            
            if not identifier_key or identifier_key.strip() == "":
                continue
            
            # Prepare measure data JSON
            measure_data = {
                "weightage": float(metadata.get("weightage", 0)) if metadata.get("weightage") else None,
                "category": measure_category,
                "created_date": str(metadata.get("created_date", "")),
                "total_count": int(metadata.get("total_count", 0)) if metadata.get("total_count") else 0,
                "valid_count": int(metadata.get("valid_count", 0)) if metadata.get("valid_count") else 0,
                "invalid_count": int(metadata.get("invalid_count", 0)) if metadata.get("invalid_count") else 0,
                "score": float(metadata.get("score", 0)) if metadata.get("score") and metadata.get("score") != "null" else None,
                "dimension_name": str(metadata.get("dimension_name", "")),
                "is_positive": True,
                "last_run_status": str(metadata.get("last_run_status", "")),
                "run_date": str(metadata.get("run_date", "")),
                "measure_status": str(metadata.get("measure_status", "")),
                "description": str(metadata.get("comment", "")),
                "is_active": bool(metadata.get("is_active", True)),
                "type": str(metadata.get("type", ""))  
            }

            if destination_connection_type == ConnectionType.Redshift.value:
                measure_data.update({
                    "measure_id": measure_id,
                    "measure_name": measure_name,
                    "attribute_id": attribute_id,
                    "attribute_name": metadata.get("attribute_name") if metadata.get("attribute_name") else selected_attribute.get("attribute_name", ""),
                    "terms": failed_rows_metadata.get("terms", ""),
                    "tags": failed_rows_metadata.get("tags", "")
                })
            else:
                measure_data.update({
                    "MEASURE_ID": measure_id,
                    "MEASURE_NAME": measure_name,
                    "ATTRIBUTE_ID": attribute_id,
                    "ATTRIBUTE_NAME": metadata.get("attribute_name") if metadata.get("attribute_name") else selected_attribute.get("attribute_name", ""),
                    "TERMS": failed_rows_metadata.get("terms", ""),
                    "TAGS": failed_rows_metadata.get("tags", "")
                })
            
            # Store in identifier groups
            if identifier_key not in identifier_groups:
                # Extract metadata from failed_rows_metadata
                identifier_groups[identifier_key]["metadata"] = {
                    "CONNECTION_ID": failed_rows_metadata.get("connection_id", ""),
                    "CONNECTION_NAME": failed_rows_metadata.get("connection_name", ""),
                    "ASSET_ID": failed_rows_metadata.get("asset_id") if failed_rows_metadata.get("asset_id") else "",
                    "ASSET_NAME": failed_rows_metadata.get("asset_name") if failed_rows_metadata.get("asset_name") else "",
                    "ATTRIBUTE_ID": "",
                    "ATTRIBUTE_NAME": "",
                    "RUN_ID": config.get("queue_id", ""),
                    "MEASURE_CONDITION": failed_rows_metadata.get("measure_condition", ""),
                    "DOMAINS": failed_rows_metadata.get("domains", ""),
                    "PRODUCTS": failed_rows_metadata.get("products", ""),
                    "APPLICATIONS": failed_rows_metadata.get("applications", ""),
                    "TERMS": "",
                    "TAGS": "",
                    "IDENTIFIER_KEY": identifier_key,
                    "EXPORTROW_ID": "",
                    "MEASURE_ID": "",
                    "MEASURE_NAME": "",
                    "IS_SUMMARISED": True
                }
                # Store first failed row data as sample
                identifier_groups[identifier_key]["failed_row_data"] = row
            
            # Add measure data to the group
            identifier_groups[identifier_key]["measures"].append(measure_data)

    gc.collect()
    
    # update summarized columns
    update_summarized_columns(
        config,
        failed_rows_query,
        default_destination_queries,
        destination_connection
    )
    MAX_SQL_SIZE = 5_000_000  # 5 MB SQL limit
    batch_data = []
    current_sql_size = 0
    
    for identifier_key, group_data in identifier_groups.items():
        metadata = group_data["metadata"]
        measures_list = group_data["measures"]
        failed_row_data = group_data["failed_row_data"]
        
       
        # Prepare row data for insertion
        measure_data_json = json.dumps(measures_list, default=str)
        failed_row_data_json = json.dumps(failed_row_data, default=str) if failed_row_data else "{}"
        row_data = {
            "CONNECTION_ID": metadata.get("CONNECTION_ID", ""),
            "CONNECTION_NAME": metadata.get("CONNECTION_NAME", ""),
            "ASSET_ID": metadata.get("ASSET_ID", ""),
            "ASSET_NAME": metadata.get("ASSET_NAME", ""),
            "ATTRIBUTE_ID": metadata.get("ATTRIBUTE_ID", ""),
            "ATTRIBUTE_NAME": metadata.get("ATTRIBUTE_NAME", ""),
            "MEASURE_ID": "",
            "MEASURE_NAME": "",
            "RUN_ID": config.get("queue_id", ""),
            "MEASURE_CONDITION": metadata.get("MEASURE_CONDITION", ""),
            "DOMAINS": metadata.get("DOMAINS", ""),
            "PRODUCTS": metadata.get("PRODUCTS", ""),
            "APPLICATIONS": metadata.get("APPLICATIONS", ""),
            "TERMS": metadata.get("TERMS", ""),
            "TAGS": metadata.get("TAGS", ""),
            "IDENTIFIER_KEY": identifier_key,
            "EXPORTROW_ID": "",
            "MEASURE_DATA": measure_data_json,
            "FAILED_ROW_DATA": failed_row_data_json,
            "IS_SUMMARISED": True,
            "CREATED_DATE": current_date
        }
        temp_sql = "(" + ",".join(f"'{str(v)}'" for v in row_data.values()) + ")"
        row_sql_size = len(temp_sql.encode("utf-8"))
        batch_data.append(row_data)

        
        # Insert in batches
        if current_sql_size + row_sql_size > MAX_SQL_SIZE:
            summarized_params = {
                "insert_batches": batch_data,
                "destination_connection_type": destination_connection_type,
                "destination_connection": destination_connection,
                "destination_config": destination_config,
                "failed_rows_table": failed_rows_table,
                "schema_name": schema_name,
                "database_name": database_name,
                "insert_failed_rows_query": str(deepcopy(insert_failed_rows_query)),
                "insert_values_query": str(deepcopy(insert_values_query)),
                "summarize_insert_query": str(deepcopy(summarize_insert_query))
            }
            insert_summarized_failed_rows(config, summarized_params)
            current_sql_size = 0
            batch_data = []
        current_sql_size += row_sql_size
    
    # Add remaining data
    if batch_data:
        summarized_params = {
            "insert_batches": batch_data,
            "destination_connection_type": destination_connection_type,
            "destination_connection": destination_connection,
            "destination_config": destination_config,
            "failed_rows_table": failed_rows_table,
            "schema_name": schema_name,
            "database_name": database_name,
            "insert_failed_rows_query": str(deepcopy(insert_failed_rows_query)),
            "insert_values_query": str(deepcopy(insert_values_query)),
            "summarize_insert_query": str(deepcopy(summarize_insert_query))
        }
        insert_summarized_failed_rows(config, summarized_params)

def insert_summarized_failed_rows(config: dict, params: list) -> list:
    insert_batches = params.get("insert_batches", [])
    destination_connection_type = params.get("destination_connection_type", "")
    destination_connection = params.get("destination_connection", {})
    destination_config = params.get("destination_config", {})
    failed_rows_table = params.get("failed_rows_table", "")
    schema_name = params.get("schema_name", "")
    database_name = params.get("database_name", "")
    insert_failed_rows_query = params.get("insert_failed_rows_query", "")
    insert_values_query = params.get("insert_values_query", "")
    summarize_insert_query = params.get("summarize_insert_query", "")
    try:
        if destination_connection_type == ConnectionType.Oracle.value:
            insert_values = []
            column_names = []
            for row_index, row in enumerate(insert_batches):
                row_values = []
                column_names = list(row.keys())
                
                for column in column_names:
                    value = row.get(column)
                    column_name = column.lower()
                    
                    if column_name == "created_date":
                        row_values.append(value)
                    elif column_name == "is_summarised":
                        row_values.append(format_boolean_value(destination_connection_type, value))
                    else:
                        str_value = str(value)
                        str_value = str_value.replace("'", "''")
                        # If value exceeds 3000 characters, use TO_CLOB chunking
                        if len(str_value) > 3000:
                            # Split into chunks of 3000 characters
                            chunks = [str_value[i:i+3000] for i in range(0, len(str_value), 3000)]
                            clob_parts = [f"TO_CLOB('{chunk}')" for chunk in chunks]
                            row_values.append(" || ".join(clob_parts))
                        else:
                            row_values.append(f"'{str_value}'")
                
                column_list = ", ".join(column_names)
                values = ",".join(row_values)
                query_input = str(deepcopy(insert_values_query))
                query_input = (query_input.replace("<columns>", column_list)
                                    .replace("<schema_name>", schema_name)
                                    .replace("<database_name>.", database_name)
                                    .replace("<failed_rows_table>", failed_rows_table))
                query_input = f"{query_input} VALUES ({values})"
                insert_values.append(query_input)

            columns = ", ".join(column_names)
            insert_failed_rows_query = (insert_failed_rows_query.replace("<columns>", columns)
                        .replace("<schema_name>", schema_name)
                        .replace("<database_name>.", database_name)
                        .replace("<failed_rows_table>", failed_rows_table))
                        
            query_string = str(deepcopy(insert_failed_rows_query))
            query_string = query_string.replace("<values>", "".join(insert_values))
                    
            if query_string:
                _, native_connection = execute_native_query(
                    destination_config,
                    query_string,
                    destination_connection,
                    True,
                    no_response=True,
                    convert_lower=False,
                )
                if not destination_connection and native_connection:
                    destination_connection = native_connection
        else:
            insert_values = []
            column_names = []
            for row in insert_batches:
                row_values = []
                column_names = list(row.keys())
                for column in column_names:
                    value = row.get(column)
                    column_name = column.lower()
                    if column_name not in "created_date":
                        if column_name == "is_summarised":
                            value = format_boolean_value(destination_connection_type, value)
                        else:
                            value = f"'{str(value)}'"
                    row_values.append(value)
                
                if destination_connection_type == ConnectionType.SapHana.value:
                    data = ",".join(row_values)
                    query_input = f" select {data} from dummy"
                    insert_values.append(query_input)
                else:
                    insert_values.append(f"({','.join(row_values)})")

            # Replace query with columns, schema, database and failed rows table
            columns = ", ".join(column_names)
            insert_failed_rows_query = (insert_failed_rows_query.replace("<columns>", columns)
                                        .replace("<schema_name>", schema_name)
                                        .replace("<database_name>.", database_name)
                                        .replace("<failed_rows_table>", failed_rows_table))


            if destination_connection_type == ConnectionType.SapHana.value:
                values = " union all ".join(insert_values)
                query_string = str(deepcopy(summarize_insert_query))
                query_string = (
                    query_string.replace(
                        "<failed_rows_table>", failed_rows_table
                    )
                    .replace("<schema_name>", schema_name)
                    .replace("<columns>", columns)
                    .replace("<insert_queries>", values)
                )
            else:
                query_string = f"{insert_failed_rows_query} VALUES {','.join(insert_values)}"

            if query_string:
                _, native_connection = execute_native_query(
                    destination_config,
                    query_string,
                    destination_connection,
                    True,
                    no_response=True,
                    convert_lower=False,
                )
                if not destination_connection and native_connection:
                    destination_connection = native_connection
    except Exception as e:
        logger.error(f"Error inserting: {str(e)}")