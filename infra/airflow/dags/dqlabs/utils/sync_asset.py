"""
    Migration Notes From V2 to V3:
    Pending:True
    Migrations Completed Except two method is not clear. Need to validate with developer
    disable_duplicate_asset
    updated_version_userid
"""

from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.enums.approval_status import ApprovalStatus
from dqlabs.app_helper.log_helper import log_error, log_info
from typing import Optional
from uuid import uuid4

# Get all pipeline data from source

from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.utils.sync_connector_api import (
    TableauConnector,
    PowerBiConnector,
    TalendConnector,
    DbtConnector,
    AirflowConnector,
)
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper import agent_helper
from dqlabs.utils.event_capture import save_sync_event


def update_config_details(config: dict) -> dict:
    """
    Update Configuration Details:

    Retrieves and updates configuration details from the database based on the provided connection ID.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        Updated configuration dictionary.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT cty.*
                , con.type AS connection_type
                , con.airflow_connection_id AS source_connection_id
                , con.credentials
                FROM core.connection_type cty
                LEFT OUTER JOIN core.connection con 
                ON cty.type = con.type
                WHERE con.id = '{connection_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            connection_details = fetchall(cursor)

            for conn in connection_details:
                config.update({"connection": conn})
                config.update({"connection_type": conn.get("connection_type")})
                config.update({"credentials": conn.get("credentials")})
                config.update(
                    {"source_connection_id": conn.get("source_connection_id")}
                )

        return config

    except Exception as e:
        raise e


def get_source_assets(config: dict):
    """
    Get Source Assets:

    Retrieves source assets based on the provided configuration.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of dictionaries representing source assets.
    """
    connection_type = config["connection"]["type"]
    
    # Handle BigQuery with CTE approach
    if connection_type == ConnectionType.BigQuery.value:
        return get_bigquery_source_assets(config)
    
    # Existing template-based logic for other connection types
    default_queries = get_queries(config)
    table_query = default_queries.get("tables")
    credentials = config.get("credentials", "")
    schema_name = credentials.get("schema") if connection_type!= ConnectionType.Snowflake.value else credentials.get("included_schema")
    database_name = credentials.get("database") if connection_type!= ConnectionType.Snowflake.value else credentials.get("included_databases")
    if not database_name and "database" in config:
        database_name = config.get("database")
    
    # Handle list vs string for database_name
    original_database_list = None
    if isinstance(database_name, list):
        if database_name:
            original_database_list = database_name
            # For multiple databases, use the new multiple database handler
            if len(database_name) > 1:
                return get_source_assets_multiple_databases(config, database_name, schema_name)
            else:
                # Single database in list, extract it
                database_name = database_name[0]
        else:
            database_name = ""
    elif isinstance(database_name, str):
        database_name = database_name
    else:
        database_name = ""
    
    # Handle list vs string for schema_name
    if isinstance(schema_name, list):
        log_info(f"Multiple schemas found: {schema_name}")
    elif isinstance(schema_name, str):
        schema_name = [schema_name] if schema_name else []
    else:
        schema_name = []

    main_schema_condition = ""
    schema_condition = ""
    view_schema_condition = ""
    database_condition = ""
    if schema_name:
        
        # Handle different schema formats
        processed_schemas = []
        for schema in schema_name:
            if isinstance(schema, str):
                # For Snowflake, schemas might be in format "DATABASE.SCHEMA"
                if connection_type == ConnectionType.Snowflake.value and '.' in schema:
                    # Extract just the schema part after the dot
                    schema_part = schema.split('.')[-1]
                    processed_schemas.append(schema_part.upper())
                else:
                    processed_schemas.append(schema.upper())
            else:
                processed_schemas.append(str(schema).upper())
        
        
        # Construct schema conditions
        if connection_type == ConnectionType.SapHana.value:
            schemas = ", ".join([f"'{name}'" for name in processed_schemas])
            schema_condition = f"WHERE UPPER(T.SCHEMA_NAME) IN ({schemas})"
            main_schema_condition = f"AND UPPER(T.SCHEMA_NAME) IN ({schemas})"
            database_condition = f" AND UPPER(SCHEMA_OWNER) = UPPER('{database_name}')" if database_name else ""
        else:
            schemas = ", ".join([f"'{name}'" for name in processed_schemas])
            # For Snowflake, use TABLES alias instead of T since CTE is named TABLES
            if connection_type == ConnectionType.Snowflake.value:
                schema_condition = f"AND UPPER(TABLES.TABLE_SCHEMA) IN ({schemas})"
                main_schema_condition = f"AND UPPER(TABLES.TABLE_SCHEMA) IN ({schemas})"
            else:
                schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"
                main_schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"
            view_schema_condition = f"WHERE UPPER(s.Name) IN ({schemas})"


    # Construct the final query string with all required placeholders
    query_string = table_query
    
    # Replace database name
    if database_name:
        # Ensure database_name is a string and clean it
        if isinstance(database_name, str):
            clean_database_name = database_name.strip().upper()
        else:
            clean_database_name = str(database_name).strip().upper()
        query_string = query_string.replace("<database_name>", clean_database_name)
    else:
        query_string = query_string.replace("<database_name>", "")
    
    # Replace schema conditions
    query_string = query_string.replace("<schema_condition>", schema_condition)
    query_string = query_string.replace("<main_schema_condition>", main_schema_condition)
    query_string = query_string.replace("<view_schema_condition>", view_schema_condition)
    query_string = query_string.replace("<database_condition>", database_condition)
    
    
    # Replace additional placeholders that might be in the template
    # For Snowflake, we need to qualify the INFORMATION_SCHEMA with database name
    if connection_type == ConnectionType.Snowflake.value and database_name:
        table_query_replacement = f"SELECT * FROM {database_name}.INFORMATION_SCHEMA.TABLES"
    else:
        table_query_replacement = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
    
    query_string = query_string.replace("<table_query>", table_query_replacement)
    log_info(f"Replaced <table_query> with: {table_query_replacement}")
    
    # Fix the where_condition replacement - ensure proper WHERE clause syntax
    if schema_condition:
        # If schema_condition starts with AND, replace it with WHERE
        if schema_condition.strip().upper().startswith('AND'):
            where_condition = schema_condition.strip()[3:].strip()  # Remove 'AND'
            where_condition = f"WHERE {where_condition}"
        else:
            where_condition = schema_condition
    else:
        where_condition = ""
    
    query_string = query_string.replace("<where_condition>", where_condition)
    
    # Fix priority_asc replacement - handle Snowflake-specific syntax
    if "<priority_asc>" in query_string and connection_type == ConnectionType.Snowflake.value:
        # Check if the template expects "NAME ASC" format
        if "ORDER BY <priority_asc> NAME" in query_string:
            query_string = query_string.replace("ORDER BY <priority_asc> NAME", "ORDER BY TABLE_NAME ASC")
        elif "ORDER BY <priority_asc>" in query_string:
            query_string = query_string.replace("ORDER BY <priority_asc>", "ORDER BY")
        else:
            # Default replacement
            query_string = query_string.replace("<priority_asc>", "ASC")
    elif "<priority_asc>" in query_string:
        # For non-Snowflake databases, use default replacement
        query_string = query_string.replace("<priority_asc>", "ASC")
    
    query_string = query_string.replace("<limit_condition>", "1000")
    
    # Additional Snowflake-specific fixes
    if connection_type == ConnectionType.Snowflake.value:
        # Ensure proper case handling for Snowflake
        query_string = query_string.replace("upper(", "UPPER(")
        query_string = query_string.replace("lower(", "LOWER(")
        
        # Fix any potential case sensitivity issues with column names
        query_string = query_string.replace("table_schema", "TABLE_SCHEMA")
        query_string = query_string.replace("table_name", "TABLE_NAME")
        
        # Fix missing columns in Snowflake INFORMATION_SCHEMA.TABLES
        # Remove COLUMN_COUNT from ORDER BY clause as it doesn't exist in Snowflake
        if "COLUMN_COUNT" in query_string:
            log_info("Removing COLUMN_COUNT from Snowflake query")
            # Remove COLUMN_COUNT from ORDER BY clause
            query_string = query_string.replace(", COLUMN_COUNT DESC", "")
            query_string = query_string.replace(", COLUMN_COUNT", "")
            query_string = query_string.replace("COLUMN_COUNT DESC", "")
            query_string = query_string.replace("COLUMN_COUNT", "")
            log_info(f"Query after removing COLUMN_COUNT: {query_string}")
    
    log_info(f"Replaced <where_condition> with: {where_condition}")
    
    # Validate that all placeholders have been replaced
    remaining_placeholders = []
    import re
    placeholder_pattern = r'<[^>]+>'
    matches = re.findall(placeholder_pattern, query_string)
    if matches:
        remaining_placeholders = matches
        log_error(f"WARNING: Unreplaced placeholders found in query: {remaining_placeholders}",error=True)
        log_error(f"Query with unreplaced placeholders: {query_string}",error=True)
        raise Exception(f"Query template has unreplaced placeholders: {remaining_placeholders}")
    
    # Log the final query for debugging
    log_info(f"Final query after replacement: {query_string}")
    
    source_connection = None
    asset_details, _ = execute_native_query(
        config, query_string, source_connection, is_list=True
    )
    return asset_details


def get_source_assets_multiple_databases(config: dict, database_list: list, schema_list: list):
    """
    Handle multiple databases by processing each database separately and combining results.
    
    Parameters:
        config: Configuration dictionary
        database_list: List of database names
        schema_list: List of schemas in format "DATABASE.SCHEMA"
    
    Returns:
        Combined list of assets from all databases
    """
    all_assets = []
    
    log_info(f"Processing {len(database_list)} databases: {database_list}")
    
    for database in database_list:
        log_info(f"Processing database: {database}")
        
        # Filter schemas for this specific database
        database_schemas = []
        for schema in schema_list:
            if schema.startswith(f"{database}."):
                schema_part = schema.split('.')[-1]
                database_schemas.append(schema_part)
                log_info(f"Found schema '{schema_part}' for database '{database}'")
        
        if not database_schemas:
            log_info(f"No schemas found for database '{database}', skipping")
            continue
        
        # Create a modified config for this database
        db_config = config.copy()
        db_config["database"] = database
        db_config["credentials"] = config["credentials"].copy()
        db_config["credentials"]["schema"] = database_schemas
        
        log_info(f"Processing database '{database}' with schemas: {database_schemas}")
        
        try:
            # Get assets for this database
            db_assets = get_source_assets_single_database(db_config)
            if db_assets:
                all_assets.extend(db_assets)
                log_info(f"Retrieved {len(db_assets)} assets from database '{database}'")
            else:
                log_info(f"No assets found in database '{database}'")
        except Exception as e:
            log_error(f"Error processing database '{database}': {str(e)}")
            # Continue with other databases even if one fails
            continue
    
    log_info(f"Total assets retrieved from all databases: {len(all_assets)}")
    return all_assets


def get_source_assets_single_database(config: dict):
    """
    Get source assets for a single database (modified version of get_source_assets).
    """
    connection_type = config["connection"]["type"]
    
    # Get query templates
    default_queries = get_queries(config)
    table_query = default_queries.get("tables")
    
    if not table_query:
        raise Exception("No 'tables' query template found")
    
    credentials = config.get("credentials", {})
    schema_name = credentials.get("schema", [])
    database_name = config.get("database", "")
    
    log_info(f"Single database processing - Database: {database_name}, Schemas: {schema_name}")
    
    # Ensure schema_name is a list
    if isinstance(schema_name, str):
        schema_name = [schema_name] if schema_name else []
    elif not isinstance(schema_name, list):
        schema_name = []
    
    # Process schemas
    main_schema_condition = ""
    schema_condition = ""
    view_schema_condition = ""
    
    if schema_name:
        processed_schemas = [schema.upper() for schema in schema_name]
        schemas = ", ".join([f"'{name}'" for name in processed_schemas])
        
        if connection_type == ConnectionType.SapHana.value:
            schema_condition = f"WHERE UPPER(T.SCHEMA_NAME) IN ({schemas})"
            main_schema_condition = f"AND UPPER(T.SCHEMA_NAME) IN ({schemas})"
        else:
            # For Snowflake, use TABLES alias instead of T since CTE is named TABLES
            if connection_type == ConnectionType.Snowflake.value:
                schema_condition = f"AND UPPER(TABLES.TABLE_SCHEMA) IN ({schemas})"
                main_schema_condition = f"AND UPPER(TABLES.TABLE_SCHEMA) IN ({schemas})"
            else:
                schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"
                main_schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"
            view_schema_condition = f"WHERE UPPER(s.Name) IN ({schemas})"
    
    # Replace placeholders
    query_string = table_query
    query_string = query_string.replace("<database_name>", database_name.upper() if database_name else "")
    query_string = query_string.replace("<schema_condition>", schema_condition)
    query_string = query_string.replace("<main_schema_condition>", main_schema_condition)
    query_string = query_string.replace("<view_schema_condition>", view_schema_condition)
    # For Snowflake, we need to qualify the INFORMATION_SCHEMA with database name
    if connection_type == ConnectionType.Snowflake.value and database_name:
        table_query_replacement = f"SELECT * FROM {database_name}.INFORMATION_SCHEMA.TABLES"
    else:
        table_query_replacement = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
    
    query_string = query_string.replace("<table_query>", table_query_replacement)
    log_info(f"Replaced <table_query> with: {table_query_replacement}")
    
    # Fix the where_condition replacement - ensure proper WHERE clause syntax
    if schema_condition:
        # If schema_condition starts with AND, replace it with WHERE
        if schema_condition.strip().upper().startswith('AND'):
            where_condition = schema_condition.strip()[3:].strip()  # Remove 'AND'
            where_condition = f"WHERE {where_condition}"
        else:
            where_condition = schema_condition
    else:
        where_condition = ""
    
    query_string = query_string.replace("<where_condition>", where_condition)
    
    # Fix priority_asc replacement - handle Snowflake-specific syntax
    if "<priority_asc>" in query_string and connection_type == ConnectionType.Snowflake.value:
        # Check if the template expects "NAME ASC" format
        if "ORDER BY <priority_asc> NAME" in query_string:
            query_string = query_string.replace("ORDER BY <priority_asc> NAME", "ORDER BY TABLE_NAME ASC")
        elif "ORDER BY <priority_asc>" in query_string:
            query_string = query_string.replace("ORDER BY <priority_asc>", "ORDER BY")
        else:
            # Default replacement
            query_string = query_string.replace("<priority_asc>", "ASC")
    elif "<priority_asc>" in query_string:
        # For non-Snowflake databases, use default replacement
        query_string = query_string.replace("<priority_asc>", "ASC")
    
    query_string = query_string.replace("<limit_condition>", "1000")
    
    # Additional Snowflake-specific fixes
    if connection_type == ConnectionType.Snowflake.value:
        # Ensure proper case handling for Snowflake
        query_string = query_string.replace("upper(", "UPPER(")
        query_string = query_string.replace("lower(", "LOWER(")
        
        # Fix any potential case sensitivity issues with column names
        query_string = query_string.replace("table_schema", "TABLE_SCHEMA")
        query_string = query_string.replace("table_name", "TABLE_NAME")
        
        # Fix missing columns in Snowflake INFORMATION_SCHEMA.TABLES
        # Remove COLUMN_COUNT from ORDER BY clause as it doesn't exist in Snowflake
        if "COLUMN_COUNT" in query_string:
            log_info("Removing COLUMN_COUNT from Snowflake query")
            # Remove COLUMN_COUNT from ORDER BY clause
            query_string = query_string.replace(", COLUMN_COUNT DESC", "")
            query_string = query_string.replace(", COLUMN_COUNT", "")
            query_string = query_string.replace("COLUMN_COUNT DESC", "")
            query_string = query_string.replace("COLUMN_COUNT", "")
            log_info(f"Query after removing COLUMN_COUNT: {query_string}")
    
    # Validate placeholders
    import re
    placeholder_pattern = r'<[^>]+>'
    matches = re.findall(placeholder_pattern, query_string)
    if matches:
        raise Exception(f"Unreplaced placeholders found: {matches}")
    
    log_info(f"Executing query for database '{database_name}': {query_string}")
    
    # Execute query
    source_connection = None
    asset_details, _ = execute_native_query(
        config, query_string, source_connection, is_list=True
    )
    
    return asset_details


def get_bigquery_source_assets(config: dict):
    """
    Get BigQuery Source Assets using CTE approach:

    Retrieves BigQuery source assets using Common Table Expressions (CTE)
    for better performance with multiple schemas.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of dictionaries representing BigQuery source assets.
        Expected format: [{"name": "table_name", "schema": "schema_name", ...}, ...]
    """
    try:
        
        # Get BigQuery-specific queries
        default_queries = get_queries(config)
        log_info(f"Retrieved queries: {list(default_queries.keys()) if default_queries else 'None'}")
        
        table_cte_query = default_queries.get("tables_cte")
        table_select_query = default_queries.get("tables_select")

        credentials = config.get("credentials", "")
        schema_name = credentials.get("schema")
        
        if not table_cte_query or not table_select_query:
            error_msg = "BigQuery query templates (tables_cte, tables_select) not found in configuration"
            log_error(error_msg)
            raise Exception(error_msg)
        
        credentials = config.get("credentials", "")
        schema_name = credentials.get("schema", [])
        
        # If no schemas provided, get all available schemas
        if len(schema_name) == 0 or not schema_name:
            log_info("No schemas provided, discovering available schemas...")
            schema_name = get_bigquery_database_schemas(config, "")
            log_info(f"Discovered schemas: {schema_name}")
        
        asset_cte_query = []
        asset_select_query = []
        tables = []
        
        if len(schema_name) > 0:
            log_info(f"Processing {len(schema_name)} schemas: {schema_name}")
            
            for i, schema in enumerate(schema_name):
                log_info(f"Processing schema {i+1}/{len(schema_name)}: {schema}")
                
                # Create CTE for each schema
                cte_query = table_cte_query.replace("<schema_name>", schema)
                asset_cte_query.append(cte_query)
                
                # Create SELECT query for each schema
                select_query = table_select_query.replace("<schema_name>", schema)
                asset_select_query.append(select_query)

            # Combine all CTEs
            asset_cte_query = ", ".join([query for query in asset_cte_query])
            
            # Combine all SELECT queries with UNION ALL
            asset_select_query = " UNION ALL ".join(
                [query for query in asset_select_query]
            )

            # Construct final query with CTE and UNION ALL
            asset_cte_query = f"WITH {asset_cte_query}"
            asset_select_query = f" finallist as ({asset_select_query}) SELECT * FROM finallist ORDER BY row_count desc"
            query_string = f"{asset_cte_query}, {asset_select_query}"
            
            # Validate that all placeholders have been replaced
            import re
            placeholder_pattern = r'<[^>]+>'
            matches = re.findall(placeholder_pattern, query_string)
            if matches:
                raise Exception(f"BigQuery query template has unreplaced placeholders: {matches}")
            
            # Execute the query
            source_connection = None
            tables, _ = execute_native_query(
                config, query_string, source_connection, is_list=True
            )
            
            if tables:
                
                # Verify expected format for deprecation functions
                sample_table = tables[0]
                has_name = "name" in sample_table
                has_schema = "schema" in sample_table
                
                if not has_name:
                    log_error("WARNING: BigQuery results missing 'name' field - this will cause issues in deprecation functions")
                if not has_schema:
                    log_error("WARNING: BigQuery results missing 'schema' field - this will cause issues in deprecation functions")
            else:
                log_info("No tables returned from BigQuery query")
        else:
            log_info("No schemas to process")
        return tables
        
    except Exception as e:
        log_error(f"BigQuery get_source_assets failed: {str(e)}", exc_info=True)
        log_error(f"Exception type: {type(e)}")
        log_error(f"Exception args: {e.args}")
        raise e


def get_bigquery_database_schemas(config: dict, dbname: str) -> list:
    """
    Get BigQuery database schemas:

    Retrieves available schemas from BigQuery for the given configuration.

    Parameters:
        config: A dictionary containing configuration information.
        dbname: Database name (not used for BigQuery but kept for consistency).

    Returns:
        List of schema names.
    """
    try:
        # Get BigQuery schema discovery query
        default_queries = get_queries(config)
        log_info(f"Available queries: {list(default_queries.keys()) if default_queries else 'None'}")
        
        schema_query = default_queries.get("schemas")
        log_info(f"Schema query found: {bool(schema_query)}")
        
        if not schema_query:
            # Fallback: return empty list if no schema query available
            log_info("No BigQuery schema discovery query found, returning empty schema list")
            return []
        
        log_info(f"BigQuery schema_query: {schema_query}")
        
        # Execute schema discovery query
        source_connection = None
        log_info("Executing schema discovery query...")
        schema_results, _ = execute_native_query(
            config, schema_query, source_connection, is_list=True
        )
        
        # Extract schema names from results
        schemas = []
        if schema_results:
            log_info(f"Sample schema result: {schema_results[0] if len(schema_results) > 0 else 'No results'}")
            
            for i, result in enumerate(schema_results):
                log_info(f"Processing schema result {i+1}/{len(schema_results)}: {result}")
                
                # Try different possible field names for schema name
                schema_name = (result.get("schema_name") or 
                             result.get("SCHEMA_NAME") or 
                             result.get("schema") or 
                             result.get("SCHEMA") or
                             result.get("name") or
                             result.get("NAME"))
                
                log_info(f"Extracted schema name: {schema_name}")
                
                if schema_name:
                    schemas.append(schema_name)
                    log_info(f"Added schema: {schema_name}")
                else:
                    log_error(f"Could not extract schema name from result: {result}")
        else:
            log_info("No schema results returned from query")
        
        return schemas
        
    except Exception as e:
        log_error(f"BigQuery get_database_schemas failed: {str(e)}", exc_info=True)
        log_error(f"Exception type: {type(e)}")
        log_error(f"Exception args: {e.args}")
        # Return empty list on error to allow processing to continue
        return []


def get_databricks_source_assets(config: dict):
    default_queries = get_queries(config)
    table_query = default_queries.get("tables")
    credentials = config.get("credentials", "")
    schema_name = credentials.get("schema")
    database_name = credentials.get("database")
    connection_type = config["connection"]["type"]

    credentials = decrypt_connection_config(credentials, connection_type)

    pipelines = get_databricks_pipeline(config)
    pipelines = pipelines.get("statuses", "")

    schema_condition = ""
    main_schema_condition = ""
    if schema_name:
        # Construct schema conditions
        schemas = ", ".join([f"'{name.upper()}'" for name in schema_name])
        schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"
        main_schema_condition = f"AND UPPER(T.TABLE_SCHEMA) IN ({schemas})"

    # Construct the final query string
    query_string = (
        table_query.replace("<database_name>", database_name.strip().upper())
        .replace("<schema_condition>", schema_condition)
        .replace("<main_schema_condition>", main_schema_condition)
    )

    source_connection = None
    asset_details, _ = execute_native_query(
        config, query_string, source_connection, is_list=True
    )
    source_asset_list = [
        {key.lower(): val for key, val in item.items()} for item in asset_details
    ]
    # source_asset_list = [asset.get("name").lower() for asset in source_asset_list]

    asset_list = {"pipeline": pipelines, "asset": source_asset_list}
    return asset_list


def get_deprecated_dqassets(config: dict):
    """
    Get Deprecated DQ Assets:

    Retrieves deprecated data quality (DQ) assets based on the provided configuration.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of dictionaries representing deprecated DQ assets.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT asset.id, asset.name,cast(asset.properties -> 'schema' as varchar) as schema
                FROM core.asset
                WHERE asset.status = '{ApprovalStatus.Deprecated.value}'
                AND asset.connection_id = '{connection_id}'
                AND LOWER(asset.type) IN ('base table', 'table', 'view', 'external table', 'pipeline','pipelines','reports','sheet','dashnoard')
            """
            cursor = execute_query(connection, cursor, query_string)
            deprecated_assets = fetchall(cursor)
            return deprecated_assets

    except Exception as e:
        raise e


def get_valid_dqassets(config: dict):
    """
    Get Valid DQ Assets:

    Retrieves valid data quality (DQ) assets based on the provided configuration.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of dictionaries representing valid DQ assets.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT asset.id, asset.name, asset.unique_id, asset.type,cast(asset.properties -> 'schema' as varchar) as schema
                FROM core.asset
                WHERE asset.connection_id = '{connection_id}'
                and asset.is_active = True
                AND LOWER(asset.type) IN ('base table', 'table', 'view', 'external table')
            """
            cursor = execute_query(connection, cursor, query_string)
            valid_dqassets = fetchall(cursor)
            return valid_dqassets

    except Exception as e:
        raise e


def get_valid_dqpipe_dqreports(config: dict):
    """
    Get Valid DQ Pipelines & DQ Reports:

    Retrieves valid data quality (DQ) assets based on the provided configuration.

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of dictionaries representing valid DQ assets.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT ast.id, ast.name, ast.unique_id, ast.type
                FROM core.asset ast
                WHERE ast.connection_id = '{connection_id}'
                and ast.is_active = True
                AND LOWER(ast.type) IN ('pipeline','pipelines','reports','sheet','dashnoard')
            """
            cursor = execute_query(connection, cursor, query_string)
            valid_dqassets = fetchall(cursor)
            return valid_dqassets

    except Exception as e:
        raise e


def update_version_table(
    config: dict,
    asset_id: str,
    dq_asset_name: str,
    reverse_deprecate: Optional[bool] = False,
):
    """
    Update Version Table:

    Updates the status of an asset in the version table.

    Parameters:
        config: A dictionary containing configuration information.
        asset_id: The ID of the asset to be updated.
        dq_asset_name: The name of the data quality (DQ) asset.
        reverse_deprecate: A boolean indicating whether to reverse the deprecation status.
    """
    connection = get_postgres_connection(config)

    try:
        if reverse_deprecate:
            status = 'restored'
        else:
            status = 'deprecated'
        with connection.cursor() as cursor:
            if reverse_deprecate:
                query_string = f"""
                    UPDATE core.asset
                    SET status = '{ApprovalStatus.Pending.value}'
                    WHERE asset.id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                log_info(
                    (
                        f"Deprecated asset {dq_asset_name} has been re-configured in the version table"
                    )
                )
            else:
                query_string = f"""
                    UPDATE core.asset
                    SET status = '{ApprovalStatus.Deprecated.value}'
                    WHERE asset.id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                log_info(
                    (
                        f"Asset {dq_asset_name} has been deprecated from the version table"
                    )
                )
        sync_data = {
            "asset": asset_id,
            "event": "sync",
            "action": status,
            "event_type": "sync",
            "properties": {
                "type": "asset_sync",
                "status": status
            },
            "source": "airflow"
        }
        save_sync_event(config, sync_data)

    except Exception as e:
        raise e


def update_asset_table(
    config: dict,
    asset_id: str,
    dq_asset_name: str,
    reverse_deprecate: Optional[str] = None,
):
    """
    Update Asset Table:

    Updates the status of an asset in the asset table.

    Parameters:
        config: A dictionary containing configuration information.
        asset_id: The ID of the asset to be updated.
        dq_asset_name: The name of the data quality (DQ) asset.
        reverse_deprecate: A boolean indicating whether to reverse the deprecation status.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            if reverse_deprecate:
                query_string = f"""
                    UPDATE core.asset ast
                    SET is_active = True
                    WHERE ast.id = '{asset_id}'
                    AND ast.connection_id = '{connection_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                log_info(
                    (
                        f"Deprecated asset has been re-configured in the {dq_asset_name} table"
                    )
                )

    except Exception as e:
        raise e


def get_version_details(config: dict, asset_id: str):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select * from core.version
                where asset_id = '{asset_id}'
                and is_active = True
            """
            cursor = execute_query(connection, cursor, query_string)
            valid_dqassets = fetchone(cursor)
            return valid_dqassets

    except Exception as e:
        raise e


def update_version_history(
    config: dict, asset_id: str, reverse_deprecate: Optional[str] = None
):
    from datetime import datetime

    current_timestamp = datetime.now()
    connection = get_postgres_connection(config)
    version_details = get_version_details(config, asset_id)
    if version_details:
        deprecate_type = (
            "reverse_deprecated"
            if reverse_deprecate
            else ApprovalStatus.Deprecated.value
        )
        uuid = str(uuid4())
        created_by_id = version_details.get("created_by")
        user_id = get_userid(config)
        created_by_id = user_id if not created_by_id else created_by_id
        organization_id = version_details.get("organization_id")
        _id = version_details.get("id")
        query_string = (
            f"'{uuid}'",
            f"'{deprecate_type}'",
            f"'{deprecate_type}'",
            str(True),
            f"'{current_timestamp}'",
            str(True),
            str(False),
            f"'{asset_id}'",
            f"'{created_by_id}'",
            f"'{_id}'",
        )
        query_input = ", ".join(query_string)
        try:
            with connection.cursor() as cursor:
                query_string = f"""
                    insert into core.version_history(id,type,sub_type,is_audit,created_date,
                    is_active,is_delete,asset_id,created_by,version_id)
                    values ({query_input})
                """
                cursor = execute_query(connection, cursor, query_string)
        except Exception as e:
            raise e


def update_attribute_status(
    config: dict, asset_id: str, reverse_deprecate: Optional[str] = None
):
    connection = get_postgres_connection(config)
    deprecate_type = (
        ApprovalStatus.Pending.value
        if reverse_deprecate
        else ApprovalStatus.Deprecated.value
    )
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.attribute
                set status = '{deprecate_type}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e


def update_measure_status(
    config: dict, asset_id: str, reverse_deprecate: Optional[str] = None
):
    connection = get_postgres_connection(config)
    deprecate_type = (
        ApprovalStatus.Pending.value
        if reverse_deprecate
        else ApprovalStatus.Deprecated.value
    )
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.measure
                set status = '{deprecate_type}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e


def disable_duplicate_asset(config: dict):
    connection = get_postgres_connection(config)
    connection_id = config.get("connection_id")
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select ver.id as version_id 
                from core.version ver
                join core.asset ast on ast.id = ver.asset_id
                join core.connection con on con.id = ast.connection_id
                where con.id = '{connection_id}'
                and ver.id not in (select version_id from core.asset)
            """
            cursor = execute_query(connection, cursor, query_string)
            duplicate_assets = fetchall(cursor)
            if duplicate_assets:
                for asset in duplicate_assets:
                    version_id = asset.get("version_id", "")
                    query_string = f"""
                                update core.version
                                set status = ''
                                where id = '{version_id}';
                                """
                    execute_query(connection, cursor, query_string)
                    query_string = f"""
                                update core.version
                                set is_active = False
                                where id = '{version_id}';
                                """
                    execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e


def get_userid(config):
    """get the user_id for deprecated assets"""
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select id
                from core.users
                where is_superuser = True
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            user = fetchone(cursor)
            user_id = user.get("id", "")
    except Exception as e:
        raise e
    return user_id


def updated_version_userid(config: dict):
    connection = get_postgres_connection(config)
    connection_id = config.get("connection_id")
    user_id = get_userid(config)
    try:
        with connection.cursor() as cursor:
            if user_id:
                query_string = f"""
                    update core.version
                    set created_by = '{user_id}'
                    where created_by is null
                    and asset_id in (select id from core.asset where connection_id = '{connection_id}')
                    """
                execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e


def get_databricks_pipeline(config, params=""):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute_databricks_api_request",
            parameters=dict(
                request_url="api/2.0/pipelines",
                request_type="get",
                request_params=params,
            ),
        )
        return api_response
    except Exception as e:
        log_error.error(
            f"Databricks Connector - Get Response Failed - {str(e)}", exc_info=True
        )
    finally:
        return api_response


def get_tableau_reports(config: dict):

    credentials = config.get("credentials", "")
    connection_type = config["connection"]["type"]
    credentials = decrypt_connection_config(credentials, connection_type)

    input_config = {
        "user": credentials.get("user"),
        "password": credentials.get("password"),
        "site": credentials.get("site"),
        "authentication_type": credentials.get("authentication_type"),
        "ssl": True,
        "host": credentials.get("host"),
        "port": "",
    }
    tableau_conn = TableauConnector(input_config)

    tableau_assets = tableau_conn.get_assets()
    return tableau_assets


def get_powerbi_reports(config: dict):

    credentials = config.get("credentials", "")
    connection_type = config["connection"]["type"]
    credentials = decrypt_connection_config(credentials, connection_type)

    input_config = {
        "client_id": credentials.get("client_id"),
        "authentication_type": credentials.get("authentication_type"),
        "client_secret": credentials.get("client_secret"),
        "tenant_id": credentials.get("tenant_id"),
    }
    powerbi_conn = PowerBiConnector(input_config)

    powerbi_assets = powerbi_conn.get_assets()
    return powerbi_assets


def get_talend_reports(config: dict):

    credentials = config.get("credentials", "")
    connection_type = config["connection"]["type"]
    credentials = decrypt_connection_config(credentials, connection_type)

    input_config = {
        "api_key": credentials.get("api_key"),
        "endpoint": credentials.get("endpoint"),
        "connection_type": connection_type,
    }
    talend_conn = TalendConnector(input_config)

    talend_assets = talend_conn.get_assets()
    return talend_assets


def get_dbt_pipelines(config: dict):

    credentials = config.get("credentials", "")
    connection_type = config["connection"]["type"]
    credentials = decrypt_connection_config(credentials, connection_type)

    input_config = {
        "api_key": credentials.get("api_key"),
        "endpoint": credentials.get("endpoint"),
        "account_id": credentials.get("account_id"),
        "projects": credentials.get("projects"),
        "environments": credentials.get("environments"),
        "web_hook_name": credentials.get("web_hook_name"),
        "web_hook_enabled": credentials.get("web_hook_enabled"),
        "connection_type": connection_type,
    }
    dbt_conn = DbtConnector(input_config)

    dbt_assets = dbt_conn.get_assets()
    return dbt_assets


def get_airflow_pipelines(config: dict):

    credentials = config.get("credentials", "")
    connection_type = config["connection"]["type"]
    credentials = decrypt_connection_config(credentials, connection_type)

    input_config = {
        "server": credentials.get("server"),
        "username": credentials.get("username"),
        "password": credentials.get("password"),
        "projects": credentials.get("projects"),
        "base_auth_key": credentials.get("base_auth_key"),
        "authentication_type": credentials.get("authentication_type"),
        "connection_type": connection_type,
    }
    airflow_conn = AirflowConnector(input_config)

    airflow_assets = airflow_conn.get_assets()
    return airflow_assets


def update_tableau_id(
    config: dict, asset_id: str, workbook_id: str = None, view_id: str = None
):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset 
                set properties = jsonb_set(
                                    properties, 
                                    '{{"id"}}', 
                                    '"{view_id}"',
                                    true
                                )
                where id = '{asset_id}'
            """
            execute_query(connection, cursor, query_string)
            log_info((f"Asset properties view_id has been updated to {view_id}"))
            # Update another table (example)
            unique_id = f"{workbook_id}_{view_id}"
            update_unique_id_query = f"""
                update core.asset  
                set unique_id = '{unique_id}'
                where id = '{asset_id}'
            """
            execute_query(connection, cursor, update_unique_id_query)
            log_info((f"Asset properties unique_id has been updated to {unique_id}"))
            # Commit the transaction
            connection.commit()
    except Exception as e:
        raise e


def update_powerbi_id(
    config: dict, asset_id: str, workspace_id: str = None, report_id: str = None
):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset 
                set properties = jsonb_set(
                                    properties, 
                                    '{{"report_id"}}',  
                                    '"{report_id}"',
                                    true
                                )
                where id = '{asset_id}'
            """
            log_info(("query_string", query_string))
            execute_query(connection, cursor, query_string)
            log_info((f"Asset properties report_id has been updated to {report_id}"))
            # Update another table (example)
            unique_id = f"{workspace_id}_{report_id}"
            update_unique_id_query = f"""
                update core.asset  
                set unique_id = '{unique_id}'
                where id = '{asset_id}'
            """
            log_info(("update_unique_id_query", update_unique_id_query))
            execute_query(connection, cursor, update_unique_id_query)
            log_info((f"Asset properties unique_id has been updated to {unique_id}"))
            # Commit the transaction
            connection.commit()
    except Exception as e:
        raise e


def update_databricks_id(
    config: dict, asset_id: str, workflow_id: str = None, asset_name: str = None
):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset 
                set properties = jsonb_set(
                                    properties, 
                                    '{{"workflow_id"}}',  
                                    '"{workflow_id}"',
                                    true
                                )
                where id = '{asset_id}'
            """
            log_info(("query_string", query_string))
            execute_query(connection, cursor, query_string)
            log_info((f"Asset properties report_id has been updated to {workflow_id}"))
            # Update another table (example)
            unique_id = f"{workflow_id}_{asset_name}"
            update_unique_id_query = f"""
                update core.asset  
                set unique_id = '{unique_id}'
                where id = '{asset_id}'
            """
            log_info(("update_unique_id_query", update_unique_id_query))
            execute_query(connection, cursor, update_unique_id_query)
            log_info((f"Asset properties unique_id has been updated to {unique_id}"))
            # Commit the transaction
            connection.commit()
    except Exception as e:
        raise e


def update_talend_id(config: dict, asset_id: str, _id: str = None):
    connection = get_postgres_connection(config)
    try:
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset 
                set properties = jsonb_set(
                                    properties, 
                                    '{{"id"}}',  
                                    '"{_id}"',
                                    true
                                )
                where id = '{asset_id}'
            """
            log_info(("query_string", query_string))
            execute_query(connection, cursor, query_string)
            log_info((f"Asset properties report_id has been updated to {_id}"))
            # Commit the transaction
            connection.commit()
    except Exception as e:
        raise e


def get_valid_schemas(config: dict):
    """
    Get DQ valid schemas for filtering:

    Retrieves schemas for assets configured in DQLabs

    Parameters:
        config: A dictionary containing configuration information.

    Returns:
        List of schemas valid in DQ assets.
    """
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)

    try:
        with connection.cursor() as cursor:
            query_string = f"""
                select properties -> 'schema' as valid_schema from core.asset 
                where connection_id ='{connection_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            schemas = fetchall(cursor)
            return schemas

    except Exception as e:
        raise e
