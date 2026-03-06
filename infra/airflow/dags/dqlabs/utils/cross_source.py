import json
import logging
from uuid import uuid4
from copy import deepcopy

from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import fetchone
from dqlabs_agent.services.livy_service import LivyService
from dqlabs.utils.extract_failed_rows import get_export_failed_rows_job_config
from dqlabs.app_helper.connection_helper import decrypt_connection_config

logger = logging.getLogger(__name__)


def prepare_cross_source_query(config: dict, measure: dict, query: str=None, is_create_table:bool=True):
    properties = measure.get("properties", {})
    if isinstance(properties, str):
        properties = json.loads(properties)

    primary_source_id = properties.get("primary_source_id")
    secondary_source_id = properties.get("secondary_source_id")
    if not query:
        query = properties.get("query", "")


    # Get library details and create iceberg tables
    primary_library = get_library_detail(config, primary_source_id)
    secondary_library = get_library_detail(config, secondary_source_id)


    primary_table_name = create_iceberg_table(config, primary_library, is_create_table)
    secondary_table_name = create_iceberg_table(config, secondary_library, is_create_table)
    # Replace @library_name placeholders in the query with actual iceberg table names
    primary_source_name = primary_library.get("name").replace(" ", "_")
    secondary_source_name = secondary_library.get("name").replace(" ", "_")
    query = query.replace(f"@{primary_source_name}", primary_table_name)
    query = query.replace(f"@{secondary_source_name}", secondary_table_name)
    return query


def create_iceberg_table(config: dict, library: dict, is_create_table: bool) -> None:
    """
    Create iceberg table from library query
    
    Args:
        config (dict): Configuration containing database connection info
        library (dict): Library details including name, query, properties, etc.
    """
    
    # Extract Library Details
    library_name = library.get("name")
    library_id = library.get("id").replace("-", "_")
    properties = library.get("properties")
    query = properties.get("query", "")
    query = query.replace(";", "") if query and query.endswith(";") else query
    connection_id = properties.get("connection")
    table_name = f"{library_name}_{library_id}".replace(" ", "_",).lower()

    # Get Connnection
    connection = get_library_connection(config, connection_id)
    connection_type = connection.get("type")
    credentials = connection.get("credentials")
    credentials.update({"job_type": "cross_source"})
    credentials = decrypt_connection_config(credentials, connection_type)
    credentials.update({"db_type": connection_type, "table": "test"})

    # Get Livy Spark configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    livy_server_url = livy_spark_config.get("livy_url")
    livy_driver_file_path = livy_spark_config.get("drivers_path")
    spark_conf = livy_spark_config.get("spark_config", {})
    spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

    
    livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)
    # Prepare the job configuration
    external_job_config = get_export_failed_rows_job_config(config, is_failed_rows=False)
    external_job_config.update({"table_name": table_name})
    catalog_name = external_job_config.get("iceberg_catalog")
    namespace = external_job_config.get("iceberg_schema")
    
    qualified_table_name = f'{catalog_name}.{namespace}.{table_name}'

    if is_create_table:
        job_config = {
            "livy_server_url": livy_server_url,
            "livy_driver_file_path": livy_driver_file_path,
            "spark_conf": spark_conf,
            "credentials": credentials,
            "query": query,
            "external_credentials": external_job_config
        }

        # Run the create iceberg table process
        response = livy_service.run_cross_source_iceberg(
            spark_conf=spark_conf, job_config=job_config)

        response = json.loads(response) if isinstance(response, str) else response
        if response.get("status") == "failure":
            raise Exception(response.get("message"))
        response = response.get("response", {}) if response else {}
        response = response.get("data") if response else 0
    
    return qualified_table_name


def execute_cross_source_query(config: dict, query: str, is_count_only: bool = False) -> dict:
    """
    Run query on Livy server
    
    Args:
        config (dict): Configuration containing database connection info
        query (str): Query to run
    """
    # Get Livy Spark configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    livy_server_url = livy_spark_config.get("livy_url")
    livy_driver_file_path = livy_spark_config.get("drivers_path")
    spark_conf = livy_spark_config.get("spark_config", {})
    spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

    external_job_config = get_export_failed_rows_job_config(config)
    job_config = {
        "external_credentials": external_job_config,
        "query_string": query,
        "is_count_only": is_count_only
    }

    livy_service = LivyService(livy_server_url, livy_driver_file_path, logger=logger)
    response = livy_service.run_external_query(spark_conf, job_config)
    response = json.loads(response) if isinstance(response, str) else response
    if response.get("status") == "failure":
        raise Exception(response.get("message"))
    response = response.get("response", {}) if response else {}
    response = response.get("data") if response else 0
    return response


def execute_export_failed_rows_query(config: dict, measure: dict, query: str, is_create_table: bool = True) -> dict:
    """
    Run query on Livy server
    
    Args:
        config (dict): Configuration containing database connection info
        query (str): Query to run
    """
    query = prepare_cross_source_query(config, measure, query, is_create_table)
    response = execute_cross_source_query(config, query)
    return response


def get_library_detail(config: dict, library_id: str) -> dict:
    """
    Get library detail from core.libraries table
    
    Args:
        config (dict): Configuration containing database connection info
        library_id (str): Library ID to retrieve
        
    Returns:
        dict: Library details including name, query, properties, etc.
        
    Raises:
        Exception: If library is not found or inactive
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select id, name, properties from core.libraries 
            where id='{library_id}' limit 1
        """
        cursor.execute(query_string)
        library = fetchone(cursor)
        
        # Parse properties if it's a string
        properties = library.get("properties")
        if isinstance(properties, str):
            properties = json.loads(properties)
        library.update({"properties": properties})
        return library

def get_library_connection(config: dict, connection_id: str) -> dict:
    """
    Get connection detail from core.connection table
    
    Args:
        config (dict): Configuration containing database connection info
        connection_id (str): Connection ID to retrieve
        
    Returns:
        dict: Connection details including name, type, credentials, etc.
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select id, name, type, credentials from core.connection 
            where id='{connection_id}' and is_active=true and is_delete=false limit 1
        """
        cursor.execute(query_string)
        connection_data = fetchone(cursor)
        
        credentials = connection_data.get("credentials", {})
        if isinstance(credentials, str):
            credentials = json.loads(credentials)
        connection_data.update({"credentials": credentials})
        return connection_data
