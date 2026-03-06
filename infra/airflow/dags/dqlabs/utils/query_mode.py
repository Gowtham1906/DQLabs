"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""


import json
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.dag_helper import execute_native_query
from dqlabs.app_helper.log_helper import log_error,log_info
from dqlabs.utils.extract_failed_rows import is_table_exists
from dqlabs.app_helper.dq_helper import prepare_databricks_query
from dqlabs.app_helper.crypto_helper import decrypt


def create_view(config: dict, default_queries: dict) -> list:
    """
    Creates the view for the give query in query mode
    """
    try:
        connection_type = config.get("connection_type")
        profile_database_name = config.get("profile_database_name")
        profile_database_name = profile_database_name if profile_database_name else ""
        profile_schema_name = config.get("profile_schema_name")
        profile_schema_name = profile_schema_name if profile_schema_name else ""
        query_mode_queries = default_queries.get("query_mode")
        create_view_query = query_mode_queries.get("create_view")
        #CREATE OR REPLACE VIEW `<project_id>.<schema_name>.<table_name>` AS <query_string>
        drop_view_query = query_mode_queries.get("drop_view")
        connection = config.get("connection")
        connection = connection if connection else {}
        asset = config.get("asset")
        asset = asset if asset else {}
        database = "" 
        if connection:
            credentials = connection.get("credentials")
            credentials = json.loads(credentials) if credentials and isinstance(
                credentials, str) else credentials
            credentials = credentials if credentials else {}
            database = credentials.get("database")
            database = database if database else ""

        properties = asset.get("properties", {})
        properties = json.loads(properties) if isinstance(
            properties, str) else properties
        properties = properties if properties else {}

        database_name = properties.get("database")
        database = database_name if database_name else database
        database = profile_database_name if profile_database_name else database
        schema = properties.get("schema")
        schema = profile_schema_name if profile_schema_name else schema
        query = asset.get("query")
        table_name = asset.get("technical_name")
        view_type = asset.get("view_type")
        view_type = view_type if view_type else "Table"
        if str(view_type).lower() == "table":
            create_table_query = query_mode_queries.get("create_table")
            drop_table_query = query_mode_queries.get("drop_table")
            create_view_query = create_table_query if create_table_query else create_view_query
            drop_view_query = drop_table_query if drop_table_query else drop_view_query
        else:
            if connection_type == ConnectionType.Synapse.value:
                query = query.replace("'", "''")

        is_query_mode = bool(str(asset.get("type")).lower() == 'query')
        if not is_query_mode or not query or not create_view_query:
            return    
        if str(view_type).lower() == "direct query":
            return
        source_connection = None
        queries = default_queries.get("failed_rows", {})
        is_exists, _ = is_table_exists(config, queries, table_name, database=database, schema=schema)
        if is_exists:
            return

        if (
            str(view_type).lower() == "table" and drop_view_query
            and (
                connection_type in [
                    ConnectionType.MSSQL.value, ConnectionType.Oracle.value,
                    ConnectionType.Redshift.value, ConnectionType.Redshift_Spectrum.value,
                    ConnectionType.MySql.value
                ]
            )
        ):
            drop_view_query_string = drop_view_query.replace("<database_name>", database).replace(
                "<schema_name>", schema).replace("<table_name>", table_name)
            if connection_type == ConnectionType.Databricks.value:
                drop_view_query_string = prepare_databricks_query(
                    config, drop_view_query_string)
            _, native_connection = execute_native_query(
                config, drop_view_query_string, source_connection, True, no_response=True)
            if not source_connection and native_connection:
                source_connection = native_connection

        if connection_type == ConnectionType.MSSQL.value or connection_type == ConnectionType.SapHana.value:
            query = query.replace("'", "''")

        if connection_type == ConnectionType.BigQuery.value:
            connection = config.get("connection")
            credentials = connection.get("credentials")
            keyjson = credentials.get("keyjson")
            decrypted_json = decrypt(keyjson) # decrypt the credentials
            if isinstance(decrypted_json, str):
                jsondata = json.loads(decrypted_json)  # Convert string to dictionary
            else:
                jsondata = decrypted_json
            project_id = jsondata.get("project_id")
            query_string = create_view_query.replace("<project_id>", project_id).replace(
                "<schema_name>", schema).replace("<table_name>", table_name).replace("<query_string>", query)
        elif connection_type == ConnectionType.Denodo.value:
            query_string = create_view_query.replace("<table_name>", table_name).replace("<query_string>", query) 
        else:
            query_string = create_view_query.replace("<database_name>", database).replace(
                "<schema_name>", schema).replace("<table_name>", table_name).replace("<query_string>", query)
        if connection_type == ConnectionType.Databricks.value:
            query_string = prepare_databricks_query(config, query_string)
        _, native_connection = execute_native_query(
            config, query_string, source_connection, True, no_response=True)
        if not source_connection and native_connection:
            source_connection = native_connection
        return source_connection
    except Exception as e:
        log_error("create view", e)
        raise e
