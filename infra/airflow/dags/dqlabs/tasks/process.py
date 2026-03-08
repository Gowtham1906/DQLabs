from dqlabs_agent.services.livy_service import LivyService
import json
import os
from copy import deepcopy
import logging
from datetime import datetime, timezone

from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
)
from dqlabs.utils import get_general_settings
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection, get_technical_name
)
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
from dqlabs.app_helper.connection_helper import decrypt_connection_config, get_attribute_names
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.extract_workflow import (
    get_queries
)
from dqlabs.app_helper.agent_helper import get_vault_data
from dqlabs.app_helper.crypto_helper import decrypt
from dqlabs.app_helper.log_helper import log_info
logger = logging.getLogger(__name__)


def execute_process(config: dict, **kwargs) -> None:
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)

        # Get Livy Spark configuration
        dag_info = config.get("dag_info", {})
        livy_spark_config = dag_info.get("livy_spark_config", {})
        livy_spark_config = livy_spark_config if livy_spark_config else {}
        livy_server_url = livy_spark_config.get("livy_url")
        livy_driver_file_path = livy_spark_config.get("drivers_path")
        spark_conf = livy_spark_config.get("spark_config", {})
        spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

        # Process Logic
        livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)

        # Prepare the job configuration
        job_config = prepare_deduplication_job_config(config)
        log_info(f"Job config of deduplication process: {json.dumps(job_config, indent=4)}")
        
        is_source_target = bool(job_config.get("field_mapping"))
        run_type = config.get("run_type", "dedupe")
        
        if is_source_target and run_type == "dedupe":
            log_info(f"[Source-to-Target] Running complete dedupe process: training → clusters → extend → similar → merge")
            log_info(f"[Source-to-Target] Step 1/5: Running source_asset_training...")
            training_job_config = deepcopy(job_config)
            training_job_config["settings"]["mode"] = "source_asset_training"
            training_response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=training_job_config)
            if isinstance(training_response, str) and training_response:
                training_response = json.loads(training_response)
            if not training_response or training_response.get("status") != "success":
                error_message = training_response.get("message", "Unknown error") if training_response else "No response from Livy training service"
                raise Exception(f"[Source-to-Target] Training failed: {error_message}")
            log_info(f"[Source-to-Target] Step 1/5: Training completed successfully")
            
            log_info(f"[Source-to-Target] Step 2/5: Running source_asset_clusters...")
            clusters_job_config = deepcopy(job_config)
            clusters_job_config["settings"]["mode"] = "source_asset_clusters"
            clusters_response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=clusters_job_config)
            if isinstance(clusters_response, str) and clusters_response:
                clusters_response = json.loads(clusters_response)
            if not clusters_response or clusters_response.get("status") != "success":
                error_message = clusters_response.get("message", "Unknown error") if clusters_response else "No response from Livy clusters service"
                raise Exception(f"[Source-to-Target] Clusters failed: {error_message}")
            log_info(f"[Source-to-Target] Step 2/5: Clusters completed successfully")
            
            log_info(f"[Source-to-Target] Step 3/5: Running source_asset_extend_clusters...")
            extend_job_config = deepcopy(job_config)
            extend_job_config["settings"]["mode"] = "source_asset_extend_clusters"
            extend_response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=extend_job_config)
            if isinstance(extend_response, str) and extend_response:
                extend_response = json.loads(extend_response)
            if not extend_response or extend_response.get("status") != "success":
                error_message = extend_response.get("message", "Unknown error") if extend_response else "No response from Livy extend service"
                raise Exception(f"[Source-to-Target] Extend clusters failed: {error_message}")
            log_info(f"[Source-to-Target] Step 3/5: Extend clusters completed successfully")
            
            log_info(f"[Source-to-Target] Step 4/5: Running source_asset_similar_clusters...")
            similar_job_config = deepcopy(job_config)
            similar_job_config["settings"]["mode"] = "source_asset_similar_clusters"
            similar_response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=similar_job_config)
            if isinstance(similar_response, str) and similar_response:
                similar_response = json.loads(similar_response)
            if not similar_response or similar_response.get("status") != "success":
                error_message = similar_response.get("message", "Unknown error") if similar_response else "No response from Livy similar clusters service"
                raise Exception(f"[Source-to-Target] Similar clusters failed: {error_message}")
            log_info(f"[Source-to-Target] Step 4/5: Similar clusters completed successfully")
            
            log_info(f"[Source-to-Target] Step 5/5: Running merge...")
            merge_job_config = deepcopy(job_config)
            merge_job_config["settings"]["mode"] = "merge"
            merge_response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=merge_job_config)
            if isinstance(merge_response, str) and merge_response:
                merge_response = json.loads(merge_response)
            if not merge_response or merge_response.get("status") != "success":
                error_message = merge_response.get("message", "Unknown error") if merge_response else "No response from Livy merge service"
                raise Exception(f"[Source-to-Target] Merge failed: {error_message}")
            log_info(f"[Source-to-Target] Step 5/5: Merge completed successfully")
            
            response = merge_response
        else:
            response = livy_service.run_dedupe(spark_conf=spark_conf, job_config=job_config)
        
        # Parse response if it's a string (matching server behavior)
        if isinstance(response, str) and response:
            try:
                response = json.loads(response)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse response as JSON: {response}")
                raise Exception(f"Invalid response format from Livy service: {response}")
        
        log_info(f"Response of deduplication process: {json.dumps(response, indent=4)}")
        
        # Check if the response contains data
        if response and response.get("status") == "success":
            update_last_runs(config, response.get("data", {}))
            update_queue_detail_status(config, ScheduleStatus.Completed.value)
            update_queue_status(config, ScheduleStatus.Completed.value)
        else:
            error_message = response.get("message", "Unknown error") if response else "No response from Livy service"
            update_last_runs(config, {"status": "failed", "message": error_message})
            raise Exception(f"Livy deduplication job failed: {error_message}")

    except Exception as e:
        error_message = str(e)
        logger.error(f"Error in execute_process: {error_message}", exc_info=True)
        update_queue_detail_status(
            config, ScheduleStatus.Failed.value, error=error_message)
        update_queue_status(config, ScheduleStatus.Failed.value, True)
        raise e


def prepare_deduplication_job_config(config: dict) -> dict:
    try:
        process_id = config.get("process_id")
        run_type = config.get("run_type", "dedupe")
        if not run_type:
            run_type = "dedupe"
        params = config.get("params", {})
        req_data = config.get("data", {})

        query_string = f"""
            select 
                process.id, process.name, process.description, process.fields, process.connection_id, process.asset_id, process.threshold,
                process.filter_properties, process.filter_query, process.is_source_target, process.source_connection_id, process.source_asset_id, 
                process.source_filter_properties, process.source_filter_query,
                connection.name as connection_name, connection.type as connection_type, connection.credentials, connection.organization_id,
                asset.name as asset_name, asset.type as asset_type, asset.properties, attribute.name AS primary_key,
                source_connection.name as source_connection_name, source_connection.type as source_connection_type, 
                source_connection.credentials as source_connection_credentials, source_connection.organization_id as source_organization_id,
                source_asset.name as source_asset_name, source_asset.type as source_asset_type, source_asset.properties as source_asset_properties
            from core.process
            join core.connection on connection.id = process.connection_id
            join core.asset on asset.id = process.asset_id
            LEFT JOIN core.connection as source_connection on source_connection.id = process.source_connection_id
            LEFT JOIN core.asset as source_asset on source_asset.id = process.source_asset_id
            LEFT JOIN LATERAL (
                SELECT name
                FROM core.attribute
                WHERE attribute.asset_id = asset.id AND attribute.is_primary_key = true
                ORDER BY attribute.id ASC
                LIMIT 1
            ) AS attribute ON true
            where process.id ='{process_id}'
        """

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            try:
                cursor = execute_query(connection, cursor, query_string)
                response = fetchone(cursor)
            except Exception as e:
                error_str = str(e).lower()
                if any(col in error_str for col in ["filter_properties", "is_source_target", "source_connection", "source_asset", "source_filter_properties", "filter_query", "source_filter_query"]):
                    query_string_fallback = f"""
                        select 
                            process.id, process.name, process.description, process.fields, process.connection_id, process.asset_id, process.threshold,
                            connection.name as connection_name, connection.type as connection_type, connection.credentials, connection.organization_id,
                            asset.name as asset_name, asset.type as asset_type, asset.properties, attribute.name AS primary_key
                        from core.process
                        join core.connection on connection.id = process.connection_id
                        join core.asset on asset.id = process.asset_id
                        LEFT JOIN LATERAL (
                            SELECT name
                            FROM core.attribute
                            WHERE attribute.asset_id = asset.id AND attribute.is_primary_key = true
                            ORDER BY attribute.id ASC
                            LIMIT 1
                        ) AS attribute ON true
                        where process.id ='{process_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string_fallback)
                    response = fetchone(cursor)
                    if response:
                        # Set default values for missing columns
                        response["filter_properties"] = {}
                        response["filter_query"] = None
                        response["is_source_target"] = False
                        response["source_connection_id"] = None
                        response["source_asset_id"] = None
                        response["source_filter_properties"] = {}
                        response["source_filter_query"] = None
                        response["source_connection_name"] = None
                        response["source_connection_type"] = None
                        response["source_connection_credentials"] = None
                        response["source_organization_id"] = None
                        response["source_asset_name"] = None
                        response["source_asset_type"] = None
                        response["source_asset_properties"] = None
                else:
                    raise
            
            if response:
                # Determine if source-to-asset mode
                is_source_asset = response.get("is_source_target", False) and \
                                 response.get("source_connection_id") and \
                                 response.get("source_asset_id")
                
                if is_source_asset:
                    if not response.get("source_connection_id"):
                        raise ValueError("source_connection_id is required for source-to-asset mode")
                    if not response.get("source_asset_id"):
                        raise ValueError("source_asset_id is required for source-to-asset mode")
                
                custom_fields = response.get("fields", [])
                if isinstance(custom_fields, str):
                    try:
                        custom_fields = json.loads(custom_fields)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse fields JSON: {str(e)}, fields value: {custom_fields}")
                        raise ValueError(f"Fields contains invalid JSON: {str(e)}")
                
                if not custom_fields or len(custom_fields) == 0:
                    raise ValueError(
                        "Fields are required for deduplication. Please define fields in step 2."
                    )
                
                # Check if source_attributes exist in fields
                has_source_attributes = any(
                    field.get("source_attributes") for field in custom_fields
                ) if custom_fields else False
                
                if is_source_asset and not has_source_attributes:
                    raise ValueError(
                        "source_attributes are required in fields for source-to-asset mode. "
                        "Each field must have both 'source_attributes' and 'attributes'."
                    )
                
                fields = []
                source_fields = []
                blocking_fields_list = []
                source_blocking_fields_list = []
                attributes = []
                source_attributes = []
                concat_fields = []
                source_concat_fields = []
                field_mapping = {}
                
                for item in custom_fields:
                    # Handle target_attributes -> attributes mapping (for backward compatibility)
                    if "target_attributes" in item and "attributes" not in item:
                        item["attributes"] = item["target_attributes"]
                    
                    if not item.get("attributes") or len(item["attributes"]) == 0:
                        raise ValueError("Field is missing 'attributes' array")
                    
                    if not item.get("compareBys") or len(item["compareBys"]) == 0:
                        raise ValueError("Field is missing 'compareBys' array")
                    
                    has_source_attrs = item.get("source_attributes") and len(item.get("source_attributes", [])) > 0
                    
                    if len(item["attributes"]) == 1:
                        attribute_name = item["attributes"][0]["value"]["name"]
                        compare_by_types = [compare["value"]["value"] for compare in item["compareBys"]]

                        field_obj = {
                            "field": attribute_name,
                            "type": compare_by_types
                        }
                        
                        if has_source_attrs and len(item["source_attributes"]) == 1:
                            source_attr_name = item["source_attributes"][0]["value"]["name"]
                            field_obj["source_field"] = source_attr_name
                            field_mapping[source_attr_name] = attribute_name
                            
                            source_fields.append({
                                "field": source_attr_name,
                                "type": compare_by_types
                            })
                            source_attributes.append(source_attr_name)
                            source_blocking_fields = [source_attr_name]
                            source_blocking_fields_list.append(source_blocking_fields)
                        
                        fields.append(field_obj)
                        attributes.append(attribute_name)
                        blocking_fields = [item["attributes"][0]["value"]["name"]]
                        blocking_fields_list.append(blocking_fields)
                        
                    elif len(item["attributes"]) > 1:
                        attribute_names = [attribute["value"]["name"] for attribute in item["attributes"]]
                        compare_by_types = [compare["value"]["value"] for compare in item["compareBys"]]
                        filed_name = "_".join([attr["value"]["name"] for attr in item["attributes"]]) + "_concat"
                        
                        field_obj = {
                            "field": filed_name,
                            "type": compare_by_types
                        }
                        
                        if has_source_attrs and len(item["source_attributes"]) > 1:
                            source_attr_names = [attr["value"]["name"] for attr in item["source_attributes"]]
                            source_filed_name = "_".join(source_attr_names) + "_concat"
                            field_obj["source_field"] = source_filed_name
                            
                            for src_attr, tgt_attr in zip(source_attr_names, attribute_names):
                                field_mapping[src_attr] = tgt_attr
                            
                            source_fields.append({
                                "field": source_filed_name,
                                "type": compare_by_types
                            })
                            source_attributes = source_attributes + source_attr_names
                            source_blocking_fields = source_attr_names
                            source_blocking_fields_list.append(source_blocking_fields)
                            
                            source_concat_fields.append({
                                "output_field": source_filed_name,
                                "input_fields": source_attr_names,
                                "separator": " "
                            })
                        
                        fields.append(field_obj)
                        attributes = attributes + attribute_names
                        blocking_fields = [attribute["value"]["name"] for attribute in item["attributes"]]
                        blocking_fields_list.append(blocking_fields)

                        concat_fields.append({
                            "output_field": filed_name,
                            "input_fields": attribute_names,
                            "separator": " "
                        })

                # Prepare Target Select Query
                credentials = response.get("credentials", {})
                credentials = (json.loads(credentials) if isinstance(
                    credentials, str) else credentials)
                connection_type = response.get("connection_type")
                asset_name = response.get("asset_name")
                asset_properties = response.get("properties", {})
                asset_properties = (json.loads(asset_properties) if isinstance(
                    asset_properties, str) else asset_properties)
                primary_key = response.get("primary_key", None)

                credentials = decrypt_connection_config(
                    credentials, connection_type)
                default_queries = get_queries(config)
                attributes = get_attribute_names(connection_type, attributes)
                select_query = default_queries.get("select_query", {})

                asset_id = response.get("asset_id")
                selected_attributes = get_selected_attribute_names(config, asset_id) if asset_id else []
                selected_attributes_list = selected_attributes if selected_attributes else None

                # Prepare attributes string based on connection type
                if (connection_type in (
                    ConnectionType.SapEcc.value,
                    ConnectionType.MSSQL.value,
                    ConnectionType.Synapse.value,
                    ConnectionType.Salesforce.value,
                    ConnectionType.SalesforceMarketing.value
                ) and selected_attributes_list):
                    # Format attributes based on connection type
                    if connection_type in (ConnectionType.MSSQL.value, ConnectionType.Synapse.value):
                        # Use square brackets for MSSQL and Synapse
                        quoted_attributes = [f"[{attr}]" for attr in selected_attributes_list]
                    else:
                        # Use double quotes for SAP ECC, Salesforce, and Salesforce Marketing
                        quoted_attributes = [f'"{attr}"' for attr in selected_attributes_list]
                    attributes_str = ", ".join(quoted_attributes)
                    query = deepcopy(select_query).replace("<attributes>", attributes_str
                                                           ).replace("<database_name>", asset_properties.get("database", "")
                                                                     ).replace("<schema_name>", asset_properties.get("schema", "")
                                                                               ).replace("<table_name>", asset_name)
                else:
                    query = deepcopy(select_query).replace("<attributes>", '*'
                                                           ).replace("<database_name>", asset_properties.get("database", "")
                                                                     ).replace("<schema_name>", asset_properties.get("schema", "")
                                                                               ).replace("<table_name>", asset_name)

                # Apply filter_query from process table (not from asset)
                # Replace <filter_condition> with process.filter_query if exists, or empty string
                filter_query = response.get("filter_query", None)
                if filter_query:
                    # Add WHERE clause if filter_query exists
                    filter_condition = f" WHERE {filter_query}"
                else:
                    filter_condition = ""
                
                # Replace <filter_condition> placeholder with filter condition
                query = query.replace("<filter_condition>", filter_condition)

                # Build source query and credentials if source-to-asset mode
                source_query = None
                source_credentials = None
                if is_source_asset:
                    # Get source connection and asset details
                    source_connection_id = response.get("source_connection_id")
                    source_asset_id = response.get("source_asset_id")
                    source_connection_name = response.get("source_connection_name")
                    source_connection_type = response.get("source_connection_type")
                    source_connection_credentials = response.get("source_connection_credentials", {})
                    source_organization_id = response.get("source_organization_id")
                    source_asset_name = response.get("source_asset_name")
                    source_asset_type = response.get("source_asset_type")
                    source_asset_properties = response.get("source_asset_properties", {})
                    
                    if isinstance(source_connection_credentials, str):
                        source_connection_credentials = json.loads(source_connection_credentials) if source_connection_credentials else {}
                    elif not source_connection_credentials:
                        source_connection_credentials = {}
                    
                    if isinstance(source_asset_properties, str):
                        source_asset_properties = json.loads(source_asset_properties) if source_asset_properties else {}
                    elif not source_asset_properties:
                        source_asset_properties = {}
                    
                    # Prepare source connection credentials
                    source_credentials = decrypt_connection_config(source_connection_credentials, source_connection_type)
                    
                    # Get source queries
                    source_default_queries = get_queries(config)
                    source_attributes_formatted = get_attribute_names(source_connection_type, source_attributes)
                    source_select_query = source_default_queries.get("select_query", {})
                    
                    # Get source selected attributes
                    source_selected_attributes = get_selected_attribute_names(config, source_asset_id) if source_asset_id else []
                    source_selected_attributes_list = source_selected_attributes if source_selected_attributes else source_attributes_formatted
                    
                    # Build source query
                    if (source_connection_type in (
                        ConnectionType.SapEcc.value,
                        ConnectionType.MSSQL.value,
                        ConnectionType.Synapse.value,
                        ConnectionType.Salesforce.value,
                        ConnectionType.SalesforceMarketing.value
                    ) and source_selected_attributes_list):
                        if source_connection_type in (ConnectionType.MSSQL.value, ConnectionType.Synapse.value):
                            quoted_source_attributes = [f"[{attr}]" for attr in source_selected_attributes_list]
                        else:
                            quoted_source_attributes = [f'"{attr}"' for attr in source_selected_attributes_list]
                        source_attributes_str = ", ".join(quoted_source_attributes)
                        source_query = deepcopy(source_select_query).replace("<attributes>", source_attributes_str
                                                                              ).replace("<database_name>", source_asset_properties.get("database", "")
                                                                                        ).replace("<schema_name>", source_asset_properties.get("schema", "")
                                                                                                  ).replace("<table_name>", source_asset_name)
                    else:
                        source_query = deepcopy(source_select_query).replace("<attributes>", '*'
                                                                             ).replace("<database_name>", source_asset_properties.get("database", "")
                                                                                       ).replace("<schema_name>", source_asset_properties.get("schema", "")
                                                                                                 ).replace("<table_name>", source_asset_name)
                    
                    # Apply source_filter_query from process table (not from source_asset)
                    # Replace <filter_condition> with process.source_filter_query if exists, or empty string
                    source_filter_query = response.get("source_filter_query", None)
                    if source_filter_query:
                        # Add WHERE clause if source_filter_query exists
                        source_filter_condition = f" WHERE {source_filter_query}"
                    else:
                        source_filter_condition = ""
                    
                    # Replace <filter_condition> placeholder with source filter condition
                    source_query = source_query.replace("<filter_condition>", source_filter_condition)
                    
                    # Build source credentials dict
                    source_credentials = {
                        **source_credentials,
                        "db_type": source_connection_type,
                        **source_asset_properties
                    }

                threshold = response.get("threshold", {})
                threshold = (json.loads(threshold) if isinstance(
                    threshold, str) else threshold)

                # Prepare External Storage Configuration
                general_settings = get_general_settings(config)
                general_settings = general_settings if general_settings else {}
                general_settings = (
                    json.loads(general_settings, default=str)
                    if isinstance(general_settings, str)
                    else general_settings
                )
                external_storage_settings = general_settings.get(
                    "external_storage") if general_settings else {}
                external_storage_settings = json.loads(external_storage_settings) if external_storage_settings and isinstance(
                    external_storage_settings, str) else external_storage_settings
                external_storage_settings = external_storage_settings if external_storage_settings else {}
                custom_storage = external_storage_settings.get(
                    "custom_storage", False)
                folder = ""
                provider = "aws"  # Default provider
                if custom_storage:
                    provider = external_storage_settings.get(
                        "storage_bucket", "aws").lower()

                    if provider == "aws":
                        parts = external_storage_settings.get(
                            "s3stagingdir")[5:].split("/", 1)
                        bucket_name = parts[0]
                        folder = parts[1] if len(parts) > 1 else ""
                        if folder:
                            folder = folder if folder.endswith(
                                "/") else folder + "/"
                        bucket_name = bucket_name.rstrip(
                            "/")  # Ensure no trailing slash

                        external_credentials = {
                            "bucket_name": bucket_name,
                            "access_key": external_storage_settings.get("awsaccesskey"),
                            "secret_key": external_storage_settings.get("awssecretaccesskey"),
                            "region": external_storage_settings.get("region"),
                            "base_url": (
                                os.environ.get("S3_BASE_URL")
                                .replace('<bucket_name>', bucket_name)
                                .replace('<region_name>', external_storage_settings.get("region"))
                            ),
                            "training_model_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/training_model.json",
                            "clusters_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/clusters.json",
                            "extend_clusters_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/extend_clusters.json",
                            "similar_clusters_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/similar_clusters.json",
                            "output_with_cluster_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/output_with_cluster.csv",
                            "output_with_merged_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/output_with_merged.csv",
                            "sample_data_path": f"s3a://{bucket_name}/{folder}dedupe/{process_id}/sample_data.json",
                            "iceberg_warehouse": f"s3a://{bucket_name}/{folder}dedupe"
                        }
                    elif provider == "disc":
                        local_directory = external_storage_settings.get("local_directory", "")
                        if local_directory and not local_directory.endswith("/"):
                            local_directory += "/"

                        external_credentials = {
                            **external_storage_settings,
                            "training_model_path": f"{local_directory}dedupe/{process_id}/training_model.json",
                            "clusters_path": f"{local_directory}dedupe/{process_id}/clusters.json",
                            "extend_clusters_path": f"{local_directory}dedupe/{process_id}/extend_clusters.json",
                            "similar_clusters_path": f"{local_directory}dedupe/{process_id}/similar_clusters.json",
                            "output_with_cluster_path": f"{local_directory}dedupe/{process_id}/output_with_cluster.csv",
                            "output_with_merged_path": f"{local_directory}dedupe/{process_id}/output_with_merged.csv",
                            "sample_data_path": f"{local_directory}dedupe/{process_id}/sample_data.json",
                            "iceberg_warehouse": f"{local_directory}dedupe"
                        }
                    elif provider == "azure":
                        folder = external_storage_settings.get(
                            "directory", "").strip("/")
                        if folder:
                            folder = folder if folder.endswith(
                                "/") else folder + "/"
                        external_credentials = {
                            "tenant_id": external_storage_settings.get("tenant_id"),
                            "client_id": external_storage_settings.get("client_id"),
                            "secret_id": external_storage_settings.get("client_secret"),
                            "container_name": external_storage_settings.get("container"),
                            "storage_account_name": external_storage_settings.get("storage_account_name"),
                            "storage_account_key": external_storage_settings.get("storage_account_key"),
                            "base_url": f"https://{external_storage_settings.get('storage_account_name')}.blob.core.windows.net/{external_storage_settings.get('container')}/",
                            "training_model_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/training_model.json",
                            "clusters_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/clusters.json",
                            "extend_clusters_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/extend_clusters.json",
                            "similar_clusters_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/similar_clusters.json",
                            "output_with_cluster_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/output_with_cluster.csv",
                            "output_with_merged_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/output_with_merged.csv",
                            "sample_data_path": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe/{process_id}/sample_data.json",
                            "iceberg_warehouse": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}dedupe"
                        }
                    elif provider == "gcp":
                        if external_storage_settings.get("is_vault_enabled", False):
                            pg_connection = get_postgres_connection(config)
                            keyjson = get_vault_data(config, pg_connection, external_storage_settings)
                            keyjson = {k: decrypt(v) for k, v in keyjson.items()}
                            external_storage_settings.update({"keyjson": keyjson})
                        keyjson = external_storage_settings.get("keyjson", {})
                        directory = external_storage_settings.get("directory", "")
                        if directory and not directory.endswith("/"):
                            directory += "/"
                        external_credentials = {
                            "gcp_bucket_name": external_storage_settings.get("gcp_bucket_name"),
                            "directory": directory,
                            "training_model_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/training_model.json",
                            "clusters_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/clusters.json",
                            "extend_clusters_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/extend_clusters.json",
                            "similar_clusters_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/similar_clusters.json",
                            "sample_data_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/sample_data.json",
                            "output_with_cluster_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/output_with_cluster.csv",
                            "output_with_merged_path": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe/{process_id}/output_with_merged.csv",
                            "type": keyjson.get("type"),
                            "keyjson": keyjson,
                            "project_id": keyjson.get("project_id"),
                            "client_email": keyjson.get("client_email"),
                            "client_id": keyjson.get("client_id"),
                            "auth_uri": keyjson.get("auth_uri"),
                            "token_uri": keyjson.get("token_uri"),
                            "auth_provider_x509_cert_url": keyjson.get("auth_provider_x509_cert_url"),
                            "client_x509_cert_url": keyjson.get("client_x509_cert_url"),
                            "universe_domain": keyjson.get("universe_domain"),
                            "iceberg_warehouse": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{directory}dedupe"
                        }
                else:
                    external_credentials = {
                        "bucket_name": os.environ.get("S3_BUCKET"),
                        "access_key": os.environ.get("S3_ACCESS_KEY"),
                        "secret_key": os.environ.get("S3_SECRET_KEY"),
                        "region": os.environ.get("S3_REGION"),
                        "training_model_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/training_model.json",
                        "clusters_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/clusters.json",
                        "extend_clusters_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/extend_clusters.json",
                        "similar_clusters_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/similar_clusters.json",
                        "output_with_cluster_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/output_with_cluster.csv",
                        "output_with_merged_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/output_with_merged.csv",
                        "sample_data_path": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe/{process_id}/sample_data.json",
                        "iceberg_warehouse": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}dedupe"
                    }

                # Get Iceberg Table Name (keep existing Airflow architecture)
                iceberg_table = get_technical_name(process_id)
                external_credentials = {
                    **external_credentials,
                    "provider": provider,
                    "iceberg_catalog": "dedupe_catalog",
                    "iceberg_schema": "clusters",
                    "iceberg_clusters_table_identifier": f"dedupe_catalog.clusters.{iceberg_table}",
                }
                
                # Add source-to-asset file paths if in source-to-asset mode (matching server logic)
                if is_source_asset:
                    # Update file paths for source-to-asset mode
                    base_path = external_credentials.get("training_model_path", "").replace("/training_model.json", "")
                    if base_path:
                        source_asset_training_model_path = f"{base_path}/source_asset_training_model.json"
                        source_asset_sample_data_path = f"{base_path}/source_asset_sample_data.json"
                        external_credentials.update({
                            "source_asset_training_model_path": source_asset_training_model_path,
                            "source_asset_clusters_path": f"{base_path}/source_asset_clusters.json",
                            "source_asset_extend_clusters_path": f"{base_path}/source_asset_extend_clusters.json",
                            "source_asset_similar_clusters_path": f"{base_path}/source_asset_similar_clusters.json",
                            "source_asset_output_with_cluster_path": f"{base_path}/source_asset_output_with_cluster.csv",
                            "source_asset_output_with_merged_path": f"{base_path}/source_asset_output_with_merged.csv",
                            "source_asset_sample_data_path": source_asset_sample_data_path,
                        })
                        external_credentials["training_model_path"] = source_asset_training_model_path
                        external_credentials["sample_data_path"] = source_asset_sample_data_path

                dedupe_config = {
                    "process_id": process_id,
                    "fields": fields,
                    "settings": {
                        "mode": run_type,
                        "sample_size": 2000,
                        "max_pairs": 100,
                        "min_pairs": 10,
                        "match_threshold": int(threshold.get("match", 80)) if threshold else 80,
                        "distinct_threshold": int(threshold.get("distinct", 50)) if threshold else 50,
                        "extension_threshold": 90,
                        "threshold": round((int(threshold.get("duplicate", 50)) if threshold else 50) / 100, 2),
                        "similarity_threshold": 90,
                        "blocking_fields": blocking_fields_list,
                        "concat_fields": concat_fields,
                        "params": params,
                        "req_data": req_data,
                        "primary_key": primary_key
                    },
                    "merge_rules": {},
                    "query": f""" {query} """,
                    "credentials": {**credentials, "db_type": connection_type, **asset_properties},
                    "external_credentials": external_credentials,
                }
                
                # Add source-to-asset fields if in source-to-asset mode
                if is_source_asset:
                    dedupe_config["source_query"] = f""" {source_query} """
                    dedupe_config["source_credentials"] = source_credentials
                    dedupe_config["source_fields"] = source_fields
                    dedupe_config["field_mapping"] = field_mapping
                    # Add source blocking fields and concat fields if needed
                    if source_blocking_fields_list:
                        dedupe_config["settings"]["source_blocking_fields"] = source_blocking_fields_list
                    if source_concat_fields:
                        dedupe_config["settings"]["source_concat_fields"] = source_concat_fields
                
                return dedupe_config
            else:
                raise Exception(f"No process found with ID: {process_id}")
    except Exception as e:
        logger.error(f"Error in prepare_deduplication_job_config: {str(e)}", exc_info=True)
        raise e


def update_last_runs(config: dict, new_run: dict):
    """
    Update the last run history of a process, keeping only the last 7 runs.

    :param config: Configuration dictionary containing process details
    :param new_run: New run data to be added
    """
    try:
        process_id = config.get("process_id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(
                connection, cursor, f"SELECT run_history FROM core.process WHERE id ='{process_id}'")
            response = fetchone(cursor)
            run_history = response.get("run_history", []) if response else []
            if isinstance(run_history, str):
                run_history = json.loads(run_history)
            if not isinstance(run_history, list):
                run_history = []
            new_run.update(
                {"run_date": datetime.now(timezone.utc).isoformat()})
            run_history.append(new_run)
            if len(run_history) > 7:
                run_history = run_history[-7:]

            # Cast JSON string to JSONB for PostgreSQL - escape single quotes for SQL
            run_history_json = json.dumps(run_history, default=str).replace("'", "''")
            update_query = f""" UPDATE core.process SET run_history = '{run_history_json}'::jsonb, last_run_date = CURRENT_TIMESTAMP  WHERE id = '{process_id}' """
            cursor = execute_query(connection, cursor, update_query)
    except Exception as e:
        raise Exception(f"Error while updating last runs: {str(e)}")


def get_selected_attribute_names(config: dict, asset_id: str):
    try:
        if not asset_id:
            return []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select name
                from core.attribute
                where asset_id = '{asset_id}'
                    and is_selected = true
                    and is_active = true
                    and is_delete = false
                order by coalesce(attribute_index, 0) asc, name asc
            """
            cursor = execute_query(connection, cursor, query_string)
            rows = fetchall(cursor)
            return [row.get("name") for row in rows] if rows else []
    except Exception as e:
        logger.warning(f"Failed to fetch selected attributes for asset {asset_id}: {e}")
        return []
