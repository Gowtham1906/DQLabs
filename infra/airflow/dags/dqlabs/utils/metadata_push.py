"""
    Migration Notes From V2 to V3:
    Pending:True
    Need to validate license for below method
    export_connection_metadata
"""

import json
import pandas as pd
import numpy as np
from dqlabs.app_constants.dq_constants import (
    FAILED_ROWS_METADATA_TABLES,
    MEASURE_METADATA,
    ASSET_METADATA,
    ATTRIBUTE_METADATA,
    CONNECTION_METADATA
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import get_postgres_connection, execute_native_query
from dqlabs.app_helper.db_helper import fetchall, fetchone
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.extract_failed_rows import (
    update_custom_field_columns,
    update_new_metadata_columns,
    convert_json_string
)
from dqlabs.app_helper.dq_helper import convert_to_lower, parse_teradata_columns
from dqlabs.app_helper.db_helper import execute_query
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.utils.user_activity import (
    get_user_session,
    get_user_activity,
    save_user_activity_log,
    get_user_activity_log,
    insert_user_activity_log
)
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.utils.tasks import get_task_config
from dqlabs.utils.lookup_process import (
    generate_lookup_source_large_datatype
)
from copy import deepcopy
from dqlabs.app_helper.log_helper import log_error
import re


def create_metadata_base_tables(config: dict, queries: dict, metadata_types: list):
    """
    Create base tables for storing metadata
    """
    destination_config = config.get("destination_config")

    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    print("step-1: schema_name failed_rows_schema:", schema_name)
    print("step-2: database_name failed_rows_database:", database_name)
    destination_connection = None
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = 'sample'
    if "All" in metadata_types:
        metadata_types = FAILED_ROWS_METADATA_TABLES
    for metadata_table in metadata_types:
        print("metadata_table:", metadata_table)
        if metadata_table == "User Metadata":
            continue
        # Convert "Connection Metadata" to "Connection_Metadata" format
        metadata_table = metadata_table.replace(" ", "_")
        key = f"create_{metadata_table.lower()}"
        metadata_table_query = queries.get(key)
        print("metadata_table_query:", metadata_table_query)
        if not metadata_table_query:
            continue

        metadata_table_query = (
            metadata_table_query.replace("<schema_name>", schema_name)
            .replace("<database_name>.", database_name)
        )
        print("metadata_table_query:", metadata_table_query)
        is_exists, destination_connection = is_table_exists(
            config, queries, metadata_table, destination_connection)
        print("is_exists:", is_exists)
        if not is_exists:
            print("step-3: executing metadata_table_query:", metadata_table_query)
            _, native_connection = execute_native_query(
                destination_config, metadata_table_query, destination_connection, True, no_response=True)
            if not destination_connection and native_connection:
                destination_connection = native_connection
            print("executed metadata_table_query successfully")

    metadata_params = {
        "destination_config": destination_config,
        "queries": queries,
        "destination_connection": destination_connection,
    }
    # update custom fields
    update_custom_field_columns(config, metadata_params)
    update_new_metadata_columns(config, metadata_params)
    return destination_connection


def is_table_exists(config: dict, queries: dict, table_name: str, destination_connection=None):
    """
    Checks whether the given table is exists or not
    """
    is_exists = False
    destination_config = config.get("destination_config")
    destination_config = destination_config if destination_config else config
    connection_type = destination_config.get("connection_type")
    connection_type = connection_type.lower() if connection_type else ""
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("destination_database")
    database_name = database_name if database_name else ""

    metadata_table_exists_query = queries.get("metadata_table_exists")
    if metadata_table_exists_query and connection_type in [ConnectionType.Teradata.value, ConnectionType.DB2IBM.value]:
        query_string = (
            metadata_table_exists_query
            .replace("<table_name>", table_name)
            .replace("<schema_name>", schema_name)
            .replace("<db_name>", database_name)
        )
        metadata_table, native_connection = execute_native_query(
            destination_config, query_string, destination_connection, True)
        if native_connection and native_connection:
            destination_connection = native_connection
        is_exists = bool(metadata_table)
    return is_exists, destination_connection


def execute_metadata_push_query(config: dict, input_data: list, query: str, destination_connection, multi_insert: bool = False):
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    destination_config = config.get("destination_config")
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = 'sample'
    if input_data and query:
        query = (
            query.replace("<schema_name>", schema_name)
            .replace("<database_name>.", database_name)
        )
        if not multi_insert:
            input_data = ['NULL' if value == 'NULL' or value is None else str(
                value) for value in input_data]
        else:
            for data in input_data:
                data = ['NULL' if value == 'NULL' or value is None else str(
                    value) for value in input_data]

        """ synapse database Insert Method is Different so we added conditions here """
        destination_connection_object = config.get(
            "destination_connection_object")

        if destination_connection_object.get("type").lower() in [ConnectionType.Synapse.value, ConnectionType.SapHana.value]:
            if destination_connection_object.get("type").lower() == ConnectionType.SapHana.value:
                failed_record_query = f"""
                                DO
                                BEGIN 
                                    <insert_queries>
                                END;
                                """
                query_string = f""""""
                query_input = (
                    f"""{query_string}{query} values ({",".join(input_data)});"""
                    if not multi_insert
                    else f"""{query_string}{query} values 
                    {f"{query_string}{query} values ".join([
                        f"({','.join(str(item) if item == 'NULL' or isinstance(item, (int, float)) else item for item in row)});"
                        for row in input_data
                    ])}
                """
                )
                query_string = failed_record_query.replace(
                    "<insert_queries>", query_input)
            else:
                query_input = (
                    f"""{",".join(input_data)}"""
                    if not multi_insert
                    else f"""{" UNION ALL SELECT ".join([
                        f"{','.join(str(item) if item == 'NULL' or isinstance(item, (int, float)) else item for item in row)}"
                        for row in input_data
                    ])}
                    """
                )
                query_string = f"{query} SELECT {query_input}"
        elif destination_connection_object.get("type").lower() in [ConnectionType.Oracle.value]:
            query = query.replace("INSERT ", '')
            insert_value_query = "INSERT ALL <values> SELECT 1 FROM dual"
            query_input = (
                f"""{query} VALUES ({",".join(input_data)})"""
                if not multi_insert
                else f"""{query} VALUES 
                    {f"{query} VALUES ".join([
                        f"({','.join(str(item) if item == 'NULL' or item is None or isinstance(item, (int, float)) else item for item in row)})"
                        for row in input_data
                    ])}
                """
            )
            query_string = insert_value_query.replace("<values>", query_input)
        elif destination_connection_object.get("type").lower() in [ConnectionType.Teradata.value]:
            query_input = (
                f"""{query} VALUES ({",".join(input_data)})"""
                if not multi_insert
                else f"""{query} VALUES 
                    {f"{query} VALUES ".join([
                        f"({','.join(str(item) if item == 'NULL' or item is None or isinstance(item, (int, float)) else item for item in row)});"
                        for row in input_data
                    ])}
                """
            )
            query_string = query_input
        else:
            query_input = (
                f"""({",".join(input_data)})"""
                if not multi_insert
                else f"""
                    {", ".join([
                        f"({','.join(str(item) if item == 'NULL' or item is None or isinstance(item, (int, float)) else item for item in row)})"
                        for row in input_data
                    ])}
                """
            )
            query_string = f"{query} VALUES {query_input}"
        
        # Additional validation for VALUES clause to prevent syntax errors
        if "VALUES" in query_string:
            # Check if the VALUES clause is properly formatted
            values_start = query_string.find("VALUES")
            values_clause = query_string[values_start:]
            
            # Ensure proper spacing and formatting
            if not values_clause.startswith("VALUES "):
                values_clause = "VALUES " + values_clause[6:]
            
            # Check for any malformed parentheses
            open_parens = values_clause.count("(")
            close_parens = values_clause.count(")")
            if open_parens != close_parens:
                log_error(f"Unbalanced parentheses in VALUES clause: {open_parens} open, {close_parens} close", Exception("Parentheses mismatch"))
            
            # Replace the VALUES clause with the cleaned version
            query_string = query_string[:values_start] + values_clause
        
        # Final validation and cleaning for SQL Server to prevent syntax errors
        if destination_connection_object and destination_connection_object.get("type").lower() == ConnectionType.MSSQL.value:
            # Check for potential issues in the query
            if "''''" in query_string:
                # Fix double-quoted strings that might cause issues
                query_string = query_string.replace("''''", "''")
            if "''''''" in query_string:
                # Fix triple-quoted strings
                query_string = query_string.replace("''''''", "''")
            # Remove any problematic characters that might cause syntax errors
            query_string = query_string.replace('\x00', '')  # Remove null bytes
            query_string = query_string.replace('\x1a', '')  # Remove EOF characters
            
            # Additional SQL Server specific fixes
            # Fix any malformed string literals
            # Find and fix any string literals that might have issues
            pattern = r"'([^']*(?:''[^']*)*)'"
            def fix_string_literal(match):
                content = match.group(1)
                # Ensure proper escaping
                content = content.replace("''''", "''")  # Fix quadruple quotes
                content = content.replace("'''", "''")   # Fix triple quotes
                return f"'{content}'"
            
            query_string = re.sub(pattern, fix_string_literal, query_string)
            
            # Check for any remaining problematic patterns
            if "''''" in query_string:
                query_string = query_string.replace("''''", "''")
        
        try:
            _, native_connection = execute_native_query(
                destination_config, query_string, destination_connection, True, no_response=True)
            if not destination_connection and native_connection:
                destination_connection = native_connection
        except Exception as e:
            # Log the query for debugging purposes
            log_error(f"Failed to execute metadata push query. Query: {query_string[:500]}... Error: {str(e)}", e)
            # Also log the input data structure for debugging
            if input_data and len(input_data) > 0:
                sample_data = input_data[0] if isinstance(input_data[0], list) else input_data
                log_error(f"Input data sample (first 10 items): {sample_data[:10] if len(sample_data) > 10 else sample_data}", e)
            
            # Additional debugging for SQL Server specific issues
            if destination_connection_object and destination_connection_object.get("type").lower() == ConnectionType.MSSQL.value:
                # Log the full query for debugging (truncated to avoid log size issues)
                log_error(f"Full SQL Query (truncated): {query_string[:2000]}...", e)
                # Check for specific problematic patterns
                if "''''" in query_string:
                    log_error("Found quadruple quotes in query - this may cause syntax errors", e)
                if "'''" in query_string:
                    log_error("Found triple quotes in query - this may cause syntax errors", e)
                # Count the number of single quotes to check for imbalance
                single_quotes = query_string.count("'")
                if single_quotes % 2 != 0:
                    log_error(f"Uneven number of single quotes ({single_quotes}) - this may cause syntax errors", e)
            
            raise e

    return destination_connection


def export_connection_metadata(config: dict, queries: dict, destination_connection):
    """
    Insert all the connection metadata into the destination database
    """
    try:
        current_date = queries.get("current_date")
        current_date = str(current_date.split('AS')[0]).strip()
        queue_id = config.get("queue_id")
        insert_connection_metadata = queries.get("insert_connection_metadata")
        schema_name = config.get("failed_rows_schema")
        database_name = config.get("failed_rows_database")
        destination_config = config.get("destination_config")
        connection_type = config.get("connection_type")
        if connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = 'sample'
        table = CONNECTION_METADATA
        delete_metadata_query = queries.get("delete_metadata_record")

        condition = " IS NOT NULL"
        delete_metadata_query = (
            delete_metadata_query.replace("<schema_name>", schema_name)
            .replace("<table_name>", table)
            .replace("<database_name>.", database_name)
            .replace("='<run_id>'", condition)
        )
        try:
            _, native_connection = execute_native_query(
                destination_config, delete_metadata_query, destination_connection, True, no_response=True)
        except Exception as e:
            log_error(
                f"Failed to delete connection metadata with error: {str(e)}", e)

        if not destination_connection and native_connection:
            destination_connection = native_connection

        connection_metadata = {}
        metadata = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, name, type, is_valid, created_date
                from core.connection
                where is_delete='false' and is_default='false'
            """
            cursor = execute_query(connection, cursor, query_string)
            connection_metadata = fetchall(cursor)
            connection_metadata = connection_metadata if connection_metadata else {}

        if not connection_metadata:
            return destination_connection

        for connection in connection_metadata:
            created_date = connection.get("created_date")
            created_date = created_date if created_date else 'NULL'
            if config.get("destination_connection_object").get("type").lower() in [ConnectionType.MSSQL.value, ConnectionType.BigQuery.value, ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Synapse.value]:
                created_date = str(created_date).split('.')[0].strip()

            if config.get("destination_connection_object").get("type").lower() == ConnectionType.Oracle.value and str(created_date) != 'NULL':
                created_date = f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                    created_date) != 'NULL' else 'NULL'
            else:
                created_date = f"'{created_date}'" if str(
                    created_date) != 'NULL' else 'NULL'

            data = [
                f"""'{connection.get("id")}'""",
                f"""'{connection.get("name")}'""",
                f"""'{connection.get("type")}'""",
                "'Valid'" if connection.get("is_valid") else "'Deprecated'",
                f"'{queue_id}'",
                created_date,
            ]
            metadata.append(data)
        destination_connection = execute_metadata_push_query(
            config, metadata, insert_connection_metadata, destination_connection, True)
        return destination_connection
    except Exception as e:
        raise e


def export_asset_metadata(config: dict, default_queries: dict, destination_connection):
    """
    Insert all the asset metadata into the ASSET_METADATA reporting table
    """
    try:
        default_column = ["CONNECTION_ID", "DATABASE_NAME", "SCHEMA_NAME", "ASSET_ID", "TABLE_NAME",
                          "ALIAS_NAME", "DQ_SCORE", "TOTAL_RECORDS", "PASSED_RECORDS", "FAILED_RECORDS",
                          "ISSUE_COUNT", "ALERT_COUNT", "STATUS", "IDENTIFIER_KEY", "RUN_ID", "CONVERSATIONS",
                          "DESCRIPTION", "STEWARD_USER_ID", "STEWARD_USERS", "DOMAIN", "PRODUCT", "LAST_RUN_TIME",
                          "APPLICATIONS", "TERMS", "CREATED_DATE"]
        asset_metadata = {}
        schema_name = config.get("failed_rows_schema")
        database_name = config.get("failed_rows_database")
        dest_connection_type = config.get(
            "destination_connection_object").get("type").lower()
        if not dest_connection_type:
            dest_connection_type = config.get(
                "destination_connection_object").get("type").lower()
        queries = default_queries.get("failed_rows", {})
        if dest_connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = 'sample'
        destination_config = config.get("destination_config")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            asset_count_query = f"""
                select count(*) as asset_count
                from core.asset as ast
                where ast.is_active is True and ast.is_delete is false
            """
            cursor = execute_query(connection, cursor, asset_count_query)
            total_assets = fetchone(cursor)
            total_assets = total_assets.get(
                "asset_count") if total_assets else 0

        batch_size = 999
        if dest_connection_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Teradata.value]:
            batch_size = 499
        offset = 0

        extract_custom_field_name = []
        fetch_source_table_schema_query = queries.get(
            "fetch_table_schema")

        if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value] and database_name.replace("`", "").replace(".", "") == 'hive_metastore':
            fetch_source_table_schema_query = default_queries.get(
                "hive_metastore", {}).get("failed_rows", {}).get("fetch_table_schema", "")
        fetch_source_table_schema_query = fetch_source_table_schema_query if fetch_source_table_schema_query else ""

        fetch_source_table_schema_query = (
            fetch_source_table_schema_query.replace("<database_name>", database_name.replace(
                ".", "").replace("`", "").replace("\"", "").replace("[", "").replace("]", ""))
            .replace("<schema_name>", config.get("failed_rows_schema"))
            .replace("<table_name>", ASSET_METADATA)
        )
        source_assets_column_input = []
        if dest_connection_type not in [ConnectionType.Synapse.value]:
            source_assets_column_input, _ = execute_native_query(config.get(
                "destination_config"), fetch_source_table_schema_query, None, True, is_list=True)
            source_assets_column_input = convert_to_lower(
                source_assets_column_input)
        if source_assets_column_input:
            if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value] and database_name.replace("`", "").replace(".", "") == "hive_metastore":
                source_assets_column_input = [
                    {"column_name": obj["col_name"],
                        "data_type": obj["data_type"]}
                    for obj in source_assets_column_input
                ]
            elif dest_connection_type == ConnectionType.Teradata.value:
                source_assets_column_input = parse_teradata_columns(source_assets_column_input, default_queries)
            for source_measure_column in source_assets_column_input:
                extract_custom_field_name.append(
                    source_measure_column.get('column_name'))
        diff_list = list(
            set(extract_custom_field_name) - set(default_column))

        while offset < total_assets:
            with connection.cursor() as cursor:
                query_string = f"""
                    with comments as (
                        select conversations.asset_id as asset_id,conversations.id as conversation_id, comment,conversations.title,conversations.created_date as created_at, rating,
                        case when users.first_name is not null then concat(users.first_name,' ', users.last_name) else users.email end as username
                        from core.conversations 
                        join core.users on users.id=conversations.created_by_id
                        where conversations.level='asset'
                    ),
                    custom_field_data as (
                        select
                        field_property.asset_id, 
                        json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
                        from core.fields
                        left join core.field_property on field_property.field_id = fields.id
                        where value is not null and level='Asset' and fields.is_active is true
                        group by field_property.asset_id
                    ),
                    max_invalid_records as (
                        select max(measure.invalid_rows) as max_invalid_rows, 
                        measure.asset_id from core.measure join core.base_measure on base_measure.id=measure.base_measure_id 
                        and base_measure.level!='measure' where measure.is_active=true  
                        group by measure.asset_id)
                    select con.id as connection_id, con.name as connection_name, con.type as connection_type,
                    con.is_valid, con.credentials, ast.status, ast.last_run_id,
                    ast.id as asset_id, ast.name as asset_name, ast.properties, 
                    ast.score as asset_score, data.row_count as asset_total_rows,
                    ast.issues as asset_issues, ast.alerts as asset_alerts, data.primary_columns, ast.description,
                    coalesce(json_agg(distinct users.id) filter(where users.id is not null and user_mapping.level='asset'), '[]') as steward_users_id, 
                    coalesce(json_agg(distinct users.email) filter(where users.id is not null and user_mapping.level='asset'), '[]') as steward_users,
                    coalesce(json_agg(distinct domain.technical_name) filter (where domain.id is not null), '[]') domains,
                    coalesce(json_agg(distinct product.technical_name) filter (where product.id is not null), '[]') product,
                    coalesce(json_agg(distinct application.name) filter (where application.id is not null), '[]') applications,
                    coalesce(json_agg(distinct terms.technical_name) filter (where terms.id is not null), '[]') as terms,
                    coalesce(json_agg(distinct jsonb_build_object('comment',comments.comment,'title', comments.title,'created_at', 
                    comments.created_at, 'rating',comments.rating, 'username',comments.username)) filter (where conversation_id is not null), '[]') as conversation,
                    CAST(custom_field_data.custom_fields as JSON) as custom_fields, ast.created_date, max_invalid_records.max_invalid_rows as asset_invalid_rows
                    from core.asset as ast
	 				join core.data on data.asset_id = ast.id
                    join core.connection as con on con.id=ast.connection_id
                    left join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
                    left join core.user_mapping on user_mapping.asset_id=ast.id
                    left join core.users on users.id=user_mapping.user_id
                   	left join core.domain_mapping as vdom on vdom.asset_id=ast.id 
                   	left join core.product_mapping as vpt on vpt.asset_id=ast.id 
                    left join core.application_mapping as vapp on vapp.asset_id=ast.id 
	 				left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
                    left join core.domain on domain.id = vdom.domain_id
                    left join core.product on product.id = vpt.product_id
                    left join core.application on application.id = vapp.application_id
                    left join core.terms on terms.id=vterms.term_id 
                    left join comments on comments.asset_id=ast.id
                    left join custom_field_data on ast.id=custom_field_data.asset_id
                    left join core.field_property on field_property.asset_id = ast.id
                    left join core.fields on field_property.field_id = fields.id and fields.level='Asset' and field_property.value is not null
                    left join max_invalid_records on ast.id=max_invalid_records.asset_id
                    where ast.is_active is true and ast.is_delete is false and con.is_active is true
                    group by con.id, ast.id, data.id, custom_field_data.custom_fields, max_invalid_records.max_invalid_rows
                    limit {batch_size} offset {offset}
                """
                cursor = execute_query(connection, cursor, query_string)
                asset_metadata = fetchall(cursor)
                asset_metadata = asset_metadata if asset_metadata else {}

            if not asset_metadata:
                return destination_connection

            appended_metadata = []
            last_run_ids = []
            appended_asset_ids = []
            appended_custom_field_queries = []

            for asset in asset_metadata:
                asset_id = asset.get("asset_id")
                appended_asset_ids.append(asset_id)
                insert_asset_metadata = queries.get("insert_asset_metadata")
                with connection.cursor() as cursor:
                    job_query_string = f"""
                        select queue.start_time from core.request_queue as rq
                        join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category not in ('catalog_update', 'export_failed_rows')
                        where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
                        order by queue.created_date desc limit 1
                    """
                    cursor = execute_query(
                        connection, cursor, job_query_string)
                    last_job_detail = fetchone(cursor)
                    last_job_detail = last_job_detail if last_job_detail else {}

                connection_type = asset.get("connection_type")
                delimiter = config.get("delimiter")
                current_date = queries.get("current_date")
                current_date = str(current_date.split('AS')[0]).strip()

                connection_credentials = asset.get("credentials")
                connection_credentials = json.loads(connection_credentials) if isinstance(
                    connection_credentials, str) else connection_credentials
                connection_credentials = connection_credentials if connection_credentials else {}

                asset_properties = asset.get("properties")
                asset_properties = json.loads(asset_properties) if isinstance(
                    asset_properties, str) else asset_properties
                asset_properties = asset_properties if asset_properties else {}
                schema = asset_properties.get("schema")
                schema = schema if schema else ""
                if connection_type == ConnectionType.Teradata.value:
                    schema = ""
                database = asset_properties.get("database")

                primary_attributes = asset.get("primary_columns")
                primary_attributes = json.loads(primary_attributes) if isinstance(
                    primary_attributes, str) else primary_attributes
                primary_attributes = primary_attributes if primary_attributes else []
                primary_attributes = [attribute.get(
                    "name") for attribute in primary_attributes]
                attributes = get_attribute_names(
                    connection_type, primary_attributes)
                identifier_key = f"{delimiter}".join(attributes)

                steward_users = asset.get("steward_users")
                steward_users = convert_json_string(steward_users, "", ",")
                steward_users_id = asset.get("steward_users_id")
                steward_users_id = convert_json_string(
                    steward_users_id, "", ",")

                domains = asset.get("domains")
                domains = convert_json_string(domains, "", ",")
                domains = domains.replace("'","''") if domains else ""

                product = asset.get("product")
                product = convert_json_string(product, "", ",")
                product = product.replace("'","''") if product else ""

                applications = asset.get("applications")
                applications = convert_json_string(applications, "", ",")
                applications = applications.replace("'","''") if applications else ""
                terms = asset.get("terms")
                terms = convert_json_string(terms, "", ",")
                terms = terms.replace("'","''") if terms else ""
                last_run_time = last_job_detail.get(
                    'start_time') if last_job_detail.get('start_time') else 'NULL'

                created_date = asset.get('created_date') if asset.get(
                    'created_date') else 'NULL'

                if dest_connection_type in [ConnectionType.MSSQL.value, ConnectionType.BigQuery.value, ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Synapse.value]:
                    created_date = str(created_date).split('.')[0].strip()
                    last_run_time = str(last_run_time).split('.')[0].strip()

                if dest_connection_type == ConnectionType.Oracle.value:
                    created_date = f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        created_date) != 'NULL' else 'NULL'
                    last_run_time = f"""TO_TIMESTAMP_TZ('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        last_run_time) != 'NULL' else 'NULL'
                else:
                    created_date = f"'{created_date}'" if str(
                        created_date) != 'NULL' else 'NULL'
                    last_run_time = f"'{last_run_time}'" if str(
                        last_run_time) != 'NULL' else 'NULL'

                last_run_id = asset.get("last_run_id")
                if last_run_id != 'NULL' and last_run_id not in last_run_ids:
                    last_run_ids.append(last_run_id)
                last_run_id = f"'{last_run_id}'" if last_run_id is not None else 'NULL'

                asset_total_rows = asset.get("asset_total_rows")
                asset_total_rows = asset_total_rows if asset_total_rows else 0

                asset_invalid_rows = asset.get("asset_invalid_rows")
                asset_invalid_rows = asset_invalid_rows if asset_invalid_rows else 0

                valid_records = asset_total_rows - asset_invalid_rows

                asset_issues = asset.get("asset_issues")
                asset_issues = asset_issues if asset_issues else 0

                asset_alerts = asset.get("asset_alerts")
                asset_alerts = asset_alerts if asset_alerts else 0

                asset_score = asset.get("asset_score")
                asset_score = asset_score if asset_score else 'NULL'

                conversations = asset.get("conversation")
                comments = []
                for conversation in conversations:
                    comment_text = ""
                    comment = conversation.get("comment", {})
                    comment = json.loads(comment)
                    comment_block = comment.get("blocks", [])
                    for block in comment_block:
                        comment_text += block["text"] + "\n"
                    comment_text = comment_text.replace(
                        "'", '') if comment_text else ""
                    comments.append({
                        "title": conversation.get("title"),
                        "rating": conversation.get("rating"),
                        "created_at": conversation.get("created_at"),
                        "username": conversation.get("username"),
                        "comment": comment_text
                    })
                comments = json.dumps(comments) if comments else ""
                comments = comments.replace("'", '') if comments else ""

                custom_fields = deepcopy(asset.get("custom_fields", []))

                description = asset.get('description')
                description = description.replace('"', "").replace(
                    "'", "''").replace("\n", "") if description else ""
                asset_name = asset.get("asset_name")
                asset_name = asset_name.replace('"', "").replace(
                    "'", "''") if asset_name else ""
                if dest_connection_type == ConnectionType.Databricks.value:
                    domains = domains.replace("''","\\'") if domains else ""
                    terms = terms.replace("''","\\'") if terms else ""
                    applications = applications.replace("''","\\'") if applications else ""
                    product = product.replace("''","\\'") if product else ""
                    asset_name = asset_name.replace("''","\\'") if asset_name else ""  
                    description = description.replace("''","\\'") if description else ""

                asset_metadata = [
                    f"""'{asset.get("connection_id")}'""",
                    f"'{database}'" if database else "''",
                    f"'{schema}'" if schema else "''",
                    f"""'{asset.get("asset_id")}'""",
                    f"'{asset_name}'",
                    f"'{asset_name}'",
                    asset_score,
                    asset_total_rows,
                    valid_records,
                    asset_invalid_rows,
                    asset_issues,
                    asset_alerts,
                    f"""'{asset.get("status")}'""",
                    f"'{identifier_key}'",
                    last_run_id,
                    f"'{comments}'",
                    f"'{description}'",
                    f"'{steward_users_id}'",
                    f"'{steward_users}'",
                    f"'{domains}'",
                    f"'{product}'",
                    last_run_time,
                    f"'{applications}'",
                    f"'{terms}'",
                    created_date,
                ]

                source_assets_column_input = []
                source_measure_columm = []
                extract_custom_field_name = []
                insert_asset_metadata = deepcopy(
                    queries.get("insert_asset_metadata"))
                insert_asset_metadata_custom_query = deepcopy(
                    queries.get("insert_asset_metadata"))

                """ Databricks Connection column name need ` string we added dynamically """
                insert_value = "\""
                if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value, ConnectionType.MySql.value, ConnectionType.Hive.value]:
                    insert_value = "`"

                if custom_fields:
                    query_details = {}
                    source_measure_columm = []
                    for entry in custom_fields:
                        field_name, value = next(iter(entry.items()))
                        source_measure_columm.append(field_name)
                        if isinstance(value, str):
                            value = value.replace('"', "").replace("'", "")
                        asset_metadata.append(f"'{value}'")
                    if diff_list:
                        final_diff_list = list(
                            set(deepcopy(diff_list)) - set(source_measure_columm))
                        final_diff_list = list(set(final_diff_list))
                        for entry in final_diff_list:
                            asset_metadata.append(f"''")
                            custom_fields.append({f"{entry}": entry})
                    # Remove duplicates
                    unique_json_list = []
                    seen = set()

                    for obj in custom_fields:
                        # Convert to a string with sorted keys
                        serialized = json.dumps(obj, sort_keys=True)
                        if serialized not in seen:
                            seen.add(serialized)
                            unique_json_list.append(obj)
                    custom_fields_columns = ', '.join(
                        [f'{insert_value}{field_name.upper() if dest_connection_type in [ConnectionType.Db2.value,ConnectionType.DB2IBM.value] else field_name}{insert_value}' for entry in unique_json_list for field_name, _ in entry.items()])
                    if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value]:
                        insert_asset_metadata_custom_query = insert_asset_metadata_custom_query.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}')
                    else:
                        insert_asset_metadata_custom_query = insert_asset_metadata_custom_query.replace(
                            'CREATED_DATE', f'CREATED_DATE, {custom_fields_columns}')

                    query_details.update(
                        {"query": insert_asset_metadata_custom_query, "values": asset_metadata})
                    appended_custom_field_queries.append(query_details)
                else:
                    if dest_connection_type in [ConnectionType.Databricks.value]:
                        final_diff_list = list(set(deepcopy(diff_list)))
                        for entry in final_diff_list:
                            asset_metadata.append(f"''")

                        custom_fields_columns = ', '.join(
                            [f'{insert_value}{entry}{insert_value}' for entry in final_diff_list])
                        insert_asset_metadata = insert_asset_metadata.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}') if custom_fields_columns else insert_asset_metadata
                    appended_metadata.append(asset_metadata)

            table = ASSET_METADATA
            delete_metadata_query = queries.get("delete_metadata_record")
            run_ids_str = ", ".join(
                "'" + str(run_id) + "'" for run_id in last_run_ids)
            asset_ids_str = ", ".join(
                "'" + str(asset_id) + "'" for asset_id in appended_asset_ids)
            run_ids_str = " IS NULL AND ASSET_ID IN (" + asset_ids_str + ") OR (RUN_ID IN (" + \
                run_ids_str + ") AND ASSET_ID IN (" + asset_ids_str + "))"
            delete_metadata_query = (
                delete_metadata_query.replace("<schema_name>", schema_name)
                .replace("<table_name>", table)
                .replace("<database_name>.", database_name)
                .replace("='<run_id>'", run_ids_str)
            )
            try:
                execute_native_query(
                    destination_config, delete_metadata_query, destination_connection, True, no_response=True)
            except Exception as e:
                log_error(
                    f"Failed to delete asset metadata with error: {str(e)}", e)

            native_connection = execute_metadata_push_query(
                config, appended_metadata, insert_asset_metadata, destination_connection, True)

            final_asset_metadata_input = []
            final_insert_asset_metadata_custom_query = ""
            for value in appended_custom_field_queries:
                asset_metadata_input = value.get("values")
                final_insert_asset_metadata_custom_query = value.get("query")
                final_asset_metadata_input.append(asset_metadata_input)
            try:
                native_connection = execute_metadata_push_query(
                    config, final_asset_metadata_input, final_insert_asset_metadata_custom_query, destination_connection, True)
            except Exception as e:
                log_error(
                    f"Failed to insert asset metadata with error: {str(e)}", e)

            if not destination_connection:
                destination_connection = native_connection

            offset += batch_size

        return destination_connection

    except Exception as e:
        raise e


def export_attribute_metadata(config: dict, default_queries: dict, destination_connection):
    """
    Insert all the Attribute metadata into the ATTRIBUTE_METADATA reporting table
    """
    try:
        default_column = [
            "ASSET_ID",
            "ATTRIBUTE_ID",
            "ATTRIBUTE_NAME",
            "DQ_SCORE",
            "TOTAL_RECORDS",
            "PASSED_RECORDS",
            "FAILED_RECORDS",
            "ISSUE_COUNT",
            "ALERT_COUNT",
            "STATUS",
            "RUN_ID",
            "DESCRIPTION",
            "TERM",
            "MIN_LENGTH",
            "MAX_LENGTH",
            "DATATYPE",
            "MIN_VALUE",
            "MAX_VALUE",
            "IS_PRIMARY",
            "TAGS",
            "LAST_RUN_TIME",
            "JOB_STATUS",
            "CREATED_DATE",
        ]
        attribute_metadata = []
        connection = get_postgres_connection(config)
        queries = default_queries.get("failed_rows", {})
        insert_attribute_metadata = queries.get("insert_attribute_metadata")
        current_date = queries.get("current_date")
        current_date = str(current_date.split('AS')[0]).strip()
        schema_name = config.get("failed_rows_schema")
        database_name = config.get("failed_rows_database")
        destination_config = config.get("destination_config")
        destination_connection_object = config.get(
            "destination_connection_object")
        dest_connection_type = destination_connection_object.get(
            "type").lower()
        if dest_connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = 'sample'
        with connection.cursor() as cursor:
            total_attributes_query = """
                select count(*) as count
                from core.asset as ast
                join core.connection as con on con.id=ast.connection_id
                join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
            """
            cursor = execute_query(connection, cursor, total_attributes_query)
            total_attributes = fetchone(cursor)
            total_attributes = total_attributes.get(
                "count") if total_attributes else 0

        batch_size = 999
        if dest_connection_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Teradata.value]:
            batch_size = 499
        offset = 0

        extract_custom_field_name = []
        fetch_source_table_schema_query = queries.get(
            "fetch_table_schema")
        if dest_connection_type == ConnectionType.Databricks.value and database_name.replace("`", "").replace(".", "") == "hive_metastore":
            fetch_source_table_schema_query = (default_queries.get(
                "hive_metastore", {}).get("failed_rows", {}).get("fetch_table_schema", ""))
        fetch_source_table_schema_query = fetch_source_table_schema_query if fetch_source_table_schema_query else ""

        fetch_source_table_schema_query = (
            fetch_source_table_schema_query.replace("<database_name>", database_name.replace(
                ".", "").replace("`", "").replace("\"", "").replace("[", "").replace("]", ""))
            .replace("<schema_name>", config.get("failed_rows_schema"))
            .replace("<table_name>", ATTRIBUTE_METADATA)
        )
        source_attributes_column_input = []
        if dest_connection_type not in [ConnectionType.Synapse.value]:
            source_attributes_column_input, _ = execute_native_query(
                destination_config, fetch_source_table_schema_query, None, True, is_list=True)
            source_attributes_column_input = convert_to_lower(
                source_attributes_column_input)
        if source_attributes_column_input:
            if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value] and database_name.replace("`", "").replace(".", "") == "hive_metastore":
                source_attributes_column_input = [
                    {"column_name": obj["col_name"],
                        "data_type": obj["data_type"]}
                    for obj in source_attributes_column_input
                ]
            elif dest_connection_type == ConnectionType.Teradata.value:
                source_attributes_column_input = parse_teradata_columns(source_attributes_column_input, default_queries)
            for source_measure_column in source_attributes_column_input:
                extract_custom_field_name.append(
                    source_measure_column.get('column_name'))
        diff_list = list(
            set(extract_custom_field_name) - set(default_column))

        while offset < total_attributes:
            with connection.cursor() as cursor:
                query_string = f"""
                    with last_run as (
                        select rq.asset_id, queue.start_time, queue.status,
                        row_number() over (partition by rq.asset_id order by queue.start_time desc) as rownum
                        from core.request_queue as rq
                        left join core.request_queue_detail as queue on queue.queue_id = rq.id
                        where lower(queue.status) in ('failed', 'completed', 'pending')  and category = 'profile'
                        order by rownum
                    ),
                    custom_field_data as (
                        select
                        field_property.attribute_id, 
                        json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
                        from core.fields
                        left join core.field_property on field_property.field_id = fields.id
                        where value is not null and level='Attribute' and fields.is_active is true
                        group by field_property.attribute_id
                    ),
                    max_invalid_records as (
                        select max(measure.invalid_rows) as max_invalid_rows, 
                        measure.attribute_id from core.measure join core.base_measure on base_measure.id=measure.base_measure_id 
                        and base_measure.level!='measure' where measure.is_active=true  
                        group by measure.attribute_id
                    )
                    select ast.id as asset_id, attr.id as attribute_id, attr.name as attribute_name,attr.status, attr.score as attribute_score,
                    attr.alerts as attribute_alerts, attr.issues as attribute_issues,attr.description,term.technical_name as term,
                    attr.row_count as attribute_total_rows, max_invalid_records.max_invalid_rows as attribute_invalid_rows,attr.is_blank,
                    attr.min_length,attr.max_length,attr.derived_type as datatype,attr.min_value,attr.max_value,attr.is_null,
                    attr.is_primary_key as is_primary,attr.is_unique, attr.last_run_id, 
                    coalesce(json_agg(distinct tags.technical_name) filter (where tags.id is not null), '[]') as tags,
                    max(det.start_time) as start_time, det.status as job_status, attr.created_date,
                    CAST(custom_field_data.custom_fields as JSON) as custom_fields
                    from core.asset as ast
                    join core.connection as con on con.id=ast.connection_id
                    join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true and parent_attribute_id is null
                    left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
                    left join core.tags_mapping as vtags on vtags.attribute_id = attr.id
                    left join core.terms as term on term.id=vterms.term_id                   
                    left join core.tags as tags on tags.id=vtags.tags_id
                    left join last_run as det on det.asset_id=ast.id and det.rownum = 1
                    left join max_invalid_records on max_invalid_records.attribute_id=attr.id
                    left join custom_field_data on attr.id=custom_field_data.attribute_id
                    where ast.is_active is true and ast.is_delete is false and con.is_active is true
                    group by ast.id,attr.id,term.id, det.status, attr.last_run_id, attribute_invalid_rows, custom_field_data.custom_fields
                    limit {batch_size} offset {offset}
                """
                cursor = execute_query(connection, cursor, query_string)
                attribute_metadata = fetchall(cursor)
                attribute_metadata = attribute_metadata if attribute_metadata else []
                
                # DEBUG: Log the length of extracted data from query
                log_error(f"DEBUG: Attribute metadata extracted from query (offset {offset}): {len(attribute_metadata)} records", Exception("Debug Info"))

            if not attribute_metadata:
                return destination_connection

            appended_metadata = []
            last_run_ids = []
            appended_attribute_ids = []
            appended_custom_field_queries = []

            for metadata in attribute_metadata:
                attribute_id = metadata.get("attribute_id")
                appended_attribute_ids.append(attribute_id)
                attribute_name = metadata.get("attribute_name")
                attribute_name = attribute_name.replace(
                    '"', "").replace("'", "''") if attribute_name else ""
                tags = metadata.get("tags")
                tags = convert_json_string(tags, "", ",")
                tags = tags.replace("'","''") if tags else ""
                description = metadata.get('description')
                description = description.replace('"', "").replace(
                    "'", "''") if description else ""
                term = metadata.get("term")
                term = term if term else ""
                term = term.replace("'","''") if term else ""
                if dest_connection_type  ==  ConnectionType.Databricks.value:
                    attribute_name = attribute_name.replace("''","\\'") if attribute_name else ""
                    term = term.replace("''","\\'") if term else ""
                    tags = tags.replace("''","\\'") if tags else ""
                    description = description.replace("''","\\'") if description else ""
                min_value = metadata.get("min_value")
                min_value = min_value if min_value else ''
                if min_value and isinstance(min_value, str):
                    min_value = (
                        min_value.replace("\\", '\\\\')
                        .replace("'", "''")
                    )
                max_value = metadata.get("max_value")
                max_value = max_value if max_value else ''
                if max_value and isinstance(max_value, str):
                    max_value = (
                        max_value.replace("\\", '\\\\')
                        .replace("'", "''")
                    )
                min_length = metadata.get("min_length")
                min_length = min_length if min_length else ''
                max_length = metadata.get("max_length")
                max_length = max_length if max_length else ''
                is_primary = metadata.get("is_primary") if metadata.get(
                    "is_primary") else False

                if dest_connection_type in [ConnectionType.BigQuery.value, ConnectionType.SapHana.value]:
                    is_primary = bool(is_primary)
                else:
                    is_primary = int(is_primary)

                last_run_time = metadata.get('start_time')
                last_run_time = last_run_time if last_run_time else 'NULL'

                created_date = metadata.get('created_date')
                created_date = created_date if created_date else 'NULL'

                if dest_connection_type in [ConnectionType.MSSQL.value, ConnectionType.BigQuery.value, ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Synapse.value]:
                    created_date = str(created_date).split('.')[0].strip()
                    last_run_time = str(last_run_time).split('.')[0].strip()

                if dest_connection_type == ConnectionType.Oracle.value:
                    created_date = f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        created_date) != 'NULL' else 'NULL'
                    last_run_time = f"""TO_TIMESTAMP_TZ('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        last_run_time) != 'NULL' else 'NULL'
                else:
                    created_date = f"'{created_date}'" if str(
                        created_date) != 'NULL' else 'NULL'
                    last_run_time = f"'{last_run_time}'" if str(
                        last_run_time) != 'NULL' else 'NULL'

                job_status = metadata.get('job_status')
                job_status = job_status if job_status else "Completed"

                last_run_id = metadata.get("last_run_id")
                if last_run_id != 'NULL' and last_run_id not in last_run_ids:
                    last_run_ids.append(last_run_id)
                last_run_id = f"'{last_run_id}'" if last_run_id is not None else 'NULL'

                custom_fields = deepcopy(metadata.get("custom_fields", []))

                status = metadata.get("status") if metadata.get(
                    "status") else "Not Run"
                attribute_score = metadata.get(
                    "attribute_score") if metadata.get("attribute_score") else 'NULL'
                attribute_invalid_rows = metadata.get(
                    "attribute_invalid_rows") if metadata.get("attribute_invalid_rows") else 0

                attribute_total_rows = metadata.get("attribute_total_rows")
                attribute_total_rows = attribute_total_rows if attribute_total_rows else 0
                attribute_valid_rows = attribute_total_rows - attribute_invalid_rows

                attribute_issues = metadata.get("attribute_issues")
                attribute_issues = attribute_issues if attribute_issues else 0

                attribute_alerts = metadata.get("attribute_alerts")
                attribute_alerts = attribute_alerts if attribute_alerts else 0

                if not metadata.get('datatype', ''):
                    min_value = ''
                    max_value = ''

                attribute_metadata_input = [
                    f"""'{metadata.get("asset_id")}'""",
                    f"""'{attribute_id}'""",
                    f"'{attribute_name}'",
                    attribute_score,
                    attribute_total_rows,
                    attribute_valid_rows,
                    attribute_invalid_rows,
                    attribute_issues,
                    attribute_alerts,
                    f"""'{status}'""",
                    last_run_id,
                    f"'{description}'",
                    f"'{term}'",
                    f"'{min_length}'",
                    f"'{max_length}'",
                    f"'{metadata.get('datatype')}'",
                    f"'{min_value}'",
                    f"'{max_value}'",
                    is_primary,
                    f"'{tags}'",
                    last_run_time,
                    f"'{job_status}'",
                    created_date,
                ]

                # appended_metadata.append(attribute_metadata_input)
                source_attributes_column_input = []
                source_attribute_columm = []
                extract_custom_field_name = []
                insert_attribute_metadata = deepcopy(
                    queries.get("insert_attribute_metadata"))
                insert_attribute_metadata_custom_query = deepcopy(
                    queries.get("insert_attribute_metadata"))

                """ Databricks Connection column name need ` string we added dynamically """
                destination_connection_object = config.get(
                    "destination_connection_object")
                insert_value = "\""
                if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value, ConnectionType.MySql.value, ConnectionType.Hive.value]:
                    insert_value = "`"

                if custom_fields:
                    query_details = {}
                    original_input_length = len(attribute_metadata_input)
                    custom_fields_count = 0
                    for entry in custom_fields:
                        field_name, value = next(iter(entry.items()))
                        source_attribute_columm.append(field_name)
                        if isinstance(value, str):
                            value = value.replace('"', "").replace("'", "")
                        attribute_metadata_input.append(f"'{value}'")
                        custom_fields_count += 1
                    
                    # DEBUG: Log custom fields processing
                    log_error(f"DEBUG: Attribute {attribute_id} - Original input length: {original_input_length}, Custom fields added: {custom_fields_count}, New length: {len(attribute_metadata_input)}", Exception("Debug Info"))
                    
                    if diff_list:
                        final_diff_list = list(
                            set(deepcopy(diff_list)) - set(source_attribute_columm))
                        final_diff_list = list(set(final_diff_list))
                        empty_fields_added = 0
                        for entry in final_diff_list:
                            attribute_metadata_input.append(f"''")
                            custom_fields.append({f"{entry}": entry})
                            empty_fields_added += 1
                        
                        # DEBUG: Log empty fields addition
                        log_error(f"DEBUG: Attribute {attribute_id} - Empty fields added for schema alignment: {empty_fields_added}, Final length: {len(attribute_metadata_input)}", Exception("Debug Info"))
                    
                    custom_fields_columns = ', '.join(
                        [f'{insert_value}{field_name.upper() if dest_connection_type in [ConnectionType.Db2.value,ConnectionType.DB2IBM.value] else field_name}{insert_value}' for entry in custom_fields for field_name, _ in entry.items()])
                    if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value]:
                        insert_attribute_metadata_custom_query = insert_attribute_metadata_custom_query.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}')
                    else:
                        insert_attribute_metadata_custom_query = insert_attribute_metadata_custom_query.replace(
                            'CREATED_DATE', f'CREATED_DATE, {custom_fields_columns}')

                    query_details.update(
                        {"query": insert_attribute_metadata_custom_query, "values": attribute_metadata_input})
                    appended_custom_field_queries.append(query_details)
                else:
                    original_input_length = len(attribute_metadata_input)
                    if dest_connection_type in [ConnectionType.Databricks.value]:
                        final_diff_list = list(set(deepcopy(diff_list)))
                        empty_fields_added = 0
                        for entry in final_diff_list:
                            attribute_metadata_input.append(f"''")
                            empty_fields_added += 1
                        
                        # DEBUG: Log Databricks empty fields addition
                        log_error(f"DEBUG: Attribute {attribute_id} (Databricks) - Original input length: {original_input_length}, Empty fields added: {empty_fields_added}, Final length: {len(attribute_metadata_input)}", Exception("Debug Info"))

                        custom_fields_columns = ', '.join(
                            [f'{insert_value}{entry}{insert_value}' for entry in final_diff_list])
                        insert_attribute_metadata = insert_attribute_metadata.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}') if custom_fields_columns else insert_attribute_metadata
                    else:
                        # DEBUG: Log regular attribute processing (no custom fields)
                        log_error(f"DEBUG: Attribute {attribute_id} (no custom fields) - Input length: {len(attribute_metadata_input)}", Exception("Debug Info"))
                    
                    appended_metadata.append(attribute_metadata_input)

            table = ATTRIBUTE_METADATA
            delete_metadata_query = queries.get("delete_metadata_record")
            run_ids_str = ", ".join(
                "'" + str(run_id) + "'" for run_id in last_run_ids)
            attribute_ids_str = ", ".join(
                "'" + str(attribute_id) + "'" for attribute_id in appended_attribute_ids)
            run_ids_str = " IS NULL AND ATTRIBUTE_ID IN (" + attribute_ids_str + ") OR (RUN_ID IN (" + \
                run_ids_str + \
                ") AND ATTRIBUTE_ID IN (" + attribute_ids_str + "))"
            delete_metadata_query = (
                delete_metadata_query.replace("<schema_name>", schema_name)
                .replace("<table_name>", table)
                .replace("<database_name>.", database_name)
                .replace("='<run_id>'", run_ids_str)
            )
            try:
                execute_native_query(
                    destination_config, delete_metadata_query, destination_connection, True, no_response=True)
            except Exception as e:
                log_error(
                    f"Failed to delete attribute metadata with error: {str(e)}", e)

            # DEBUG: Log summary before first INSERT
            log_error(f"DEBUG: Attribute metadata batch summary (offset {offset}) - Regular attributes to insert: {len(appended_metadata)}, Custom field attributes to insert: {len(appended_custom_field_queries)}", Exception("Debug Info"))
            
            native_connection = execute_metadata_push_query(
                config, appended_metadata, insert_attribute_metadata, destination_connection, True)

            final_attribute_metadata_input = []
            final_insert_attribute_metadata_custom_query = ""
            for value in appended_custom_field_queries:
                attribute_metadata_input = value.get("values")
                final_insert_attribute_metadata_custom_query = value.get(
                    "query")
                final_attribute_metadata_input.append(attribute_metadata_input)
            
            # DEBUG: Log custom field processing summary
            if final_attribute_metadata_input:
                log_error(f"DEBUG: Custom field attributes processing - Total custom field records to insert: {len(final_attribute_metadata_input)}", Exception("Debug Info"))
            try:
                native_connection = execute_metadata_push_query(
                    config, final_attribute_metadata_input, final_insert_attribute_metadata_custom_query, destination_connection, True)
            except Exception as e:
                log_error(
                    f"Failed to insert Attribute metadata with error: {str(e)}", e)

            if not destination_connection:
                destination_connection = native_connection

            # DEBUG: Log batch completion summary
            total_processed = len(appended_metadata) + len(appended_custom_field_queries)
            log_error(f"DEBUG: Attribute metadata batch {offset//batch_size + 1} completed - Total records processed: {total_processed} (Regular: {len(appended_metadata)}, Custom: {len(appended_custom_field_queries)})", Exception("Debug Info"))

            attribute_metadata = []
            offset += batch_size

        return destination_connection
    except Exception as e:
        raise e


def export_measure_metadata(config: dict, default_queries: dict, destination_connection):
    """
    Insert the measure metadata into the destination
    """
    try:
        native_connection = None
        default_column = ["CONNECTION_ID", "ASSET_ID", "ATTRIBUTE_ID", "MEASURE_ID", "MEASURE_NAME",
                          "MEASURE_DIMENSION", "MEASURE_WEIGHTAGE", "MEASURE_TYPE", "MEASURE_THRESHOLD",
                          "MEASURE_QUERY", "TOTAL_RECORDS_SCOPE", "TOTAL_RECORDS_SCOPE_QUERY",
                          "DQ_SCORE", "TOTAL_RECORDS", "PASSED_RECORDS", "FAILED_RECORDS", "STATUS",
                          "RUN_STATUS", "DOMAIN", "PRODUCT", "APPLICATION", "COMMENT", "RUN_ID", "RUN_DATE", "MEASURE_STATUS", "IS_ACTIVE", "PROFILE", "CREATED_DATE"]

        schema_name = config.get("failed_rows_schema")

        database_name = config.get("failed_rows_database")
        destination_config = config.get("destination_config")
        destination_connection_object = config.get(
            "destination_connection_object")
        dest_connection_type = destination_connection_object.get(
            "type").lower()
        if dest_connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = 'sample'

        queries = default_queries.get("failed_rows", {})
        insert_measure_metadata = queries.get("insert_measure_metadata")
        measure_metadata = {}
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            total_records_query = """
            select count(*) from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and base.is_visible=true
            """
            cursor = execute_query(connection, cursor, total_records_query)
            total_records = fetchone(cursor)
            total_records = total_records.get("count") if total_records else 0

        batch_size = 999
        if dest_connection_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Teradata.value]:
            batch_size = 499
        offset = 0

        diff_list = []
        extract_custom_field_name = []

        fetch_source_table_schema_query = queries.get("fetch_table_schema")

        if dest_connection_type == ConnectionType.Databricks.value and database_name.replace("`", "").replace(".", "") == "hive_metastore":
            fetch_source_table_schema_query = (default_queries.get(
                "hive_metastore", {}).get("failed_rows", {}).get("fetch_table_schema", ""))

        fetch_source_table_schema_query = fetch_source_table_schema_query if fetch_source_table_schema_query else ""

        fetch_source_table_schema_query = (
            fetch_source_table_schema_query.replace(
                "<database_name>", database_name.replace(".", "").replace("`", "").replace("\"", "").replace('[', '').replace(']', ''))
            .replace("<schema_name>", schema_name)
            .replace("<table_name>", MEASURE_METADATA)
        )

        source_measure_column_input = []
        if dest_connection_type not in [ConnectionType.Synapse.value]:
            source_measure_column_input, _ = execute_native_query(config.get(
                "destination_config"), fetch_source_table_schema_query, None, True, is_list=True)
            source_measure_column_input = convert_to_lower(
                source_measure_column_input)
        if source_measure_column_input:
            if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.Hive.value] and database_name.replace("`", "").replace(".", "") == "hive_metastore":
                source_measure_column_input = [
                    {"column_name": obj["col_name"],
                        "data_type": obj["data_type"]}
                    for obj in source_measure_column_input
                ]
            elif dest_connection_type == ConnectionType.Teradata.value:
                source_measure_column_input = parse_teradata_columns(source_measure_column_input, default_queries)
            for source_measure_column in source_measure_column_input:
                extract_custom_field_name.append(
                    source_measure_column.get('column_name'))
        diff_list = list(
            set(extract_custom_field_name) - set(default_column))

        while offset < total_records:
            with connection.cursor() as cursor:
                query_string = f"""
                    with custom_field_data as (
                        select
                        field_property.measure_id, 
                        json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
                        from core.fields
                        left join core.field_property on field_property.field_id = fields.id
                        where value is not null and level='Measure' and fields.is_active is true
                        group by field_property.measure_id
                    ),
                    max_start_time AS (
                        SELECT mes.id as measure_id, max(queue_detail.start_time) AS max_start_time
                        from core.measure as mes
                        left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and queue_detail.category not in ('export_failed_rows', 'catalog_update')
                        group by mes.id
                    ),
                    custom_measure_status AS (
                        select mes.id, queue_detail.status
                        from core.measure as mes
                        left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and mes.id=queue_detail.measure_id
                        where queue_detail.status is not null and queue_detail.category not in ('export_failed_rows')
                        group by mes.id, queue_detail.status 
                    )
                    select mes.connection_id as connection_id, ast.id as asset_id, attr.id as attribute_id,
                    mes.id as measure_id, base.name as measure_name, mes.weightage, base.category,
                    mes.row_count as measure_total_count, mes.valid_rows AS measure_valid_count, mes.invalid_rows AS measure_invalid_count,
                    mes.score as measure_score, dim.name as dimension_name, base.properties,mes.last_run_id,
                    coalesce(json_agg(distinct domain.technical_name) filter (where domain.id is not null), '[]') domains,
                    coalesce(json_agg(distinct product.technical_name) filter (where product.id is not null), '[]') product,
                    coalesce(json_agg(distinct application.name) filter (where application.id is not null), '[]') applications,
                    queue_detail.status as last_run_status, CAST(custom_field_data.custom_fields as JSON) as custom_fields,
                    queue_detail.start_time as run_date, mes.created_date, base.description, mes.is_positive, mes.drift_threshold,
                    mes.status as measure_status, (
                                select 
                                case 
                                    when bas_mes.category = 'behavioral' then 
                                    case 
                                        when measure.drift_threshold = '{{}}'::jsonb then 'failed'
                                        else 'completed'
                                    end
                                    else custom.status
                                end as status
                                from core.measure
                                left join core.base_measure bas_mes on bas_mes.id = measure.base_measure_id
                                left join custom_measure_status custom on measure.id = custom.id
                                where measure.id = mes.id) as custom_measure_run_status,
                                 base.query, mes.semantic_query, base.type, mes.is_active, base.profile
                    from core.measure as mes
                    join core.base_measure as base on base.id=mes.base_measure_id and mes.connection_id is not null and base.is_visible=true
                    left join core.asset as ast on ast.id=mes.asset_id
                    left join core.connection as conn on conn.id=mes.connection_id
                    left join core.dimension as dim on dim.id=mes.dimension_id
                    left join core.attribute as attr on attr.id=mes.attribute_id
                    left join core.domain_mapping as vdom on (vdom.asset_id=ast.id or vdom.measure_id=mes.id)
                    left join core.product_mapping as vpt on (vpt.asset_id=ast.id or vpt.measure_id=mes.id)
                    left join core.application_mapping as vapp on (vapp.asset_id=ast.id or vapp.measure_id=mes.id)
                    left join core.domain on domain.id = vdom.domain_id
                    left join core.product on product.id = vpt.product_id
                    left join core.application on application.id = vapp.application_id
                    left join core.request_queue_detail as queue_detail on mes.last_run_id=queue_detail.queue_id
                    left join custom_field_data on mes.id=custom_field_data.measure_id
                    left join custom_measure_status as custom on mes.id=custom.id
                    join max_start_time ON mes.id = max_start_time.measure_id 
                    AND (queue_detail.start_time = max_start_time.max_start_time or (queue_detail.start_time is null 
                    and max_start_time.max_start_time is null))
                    where (ast.is_active is true or ast.is_active is null) and (ast.is_delete is false or ast.is_delete is null) 
                    and conn.is_active is true and conn.is_delete is false
                    and (attr.is_selected is true or attr.is_selected is null)
                    and attr.parent_attribute_id is null
                    and mes.is_delete is false
                    group by mes.connection_id, ast.id, attr.id, mes.id, base.name, base.category, dim.name, custom.status,
                    base.properties, custom_field_data.custom_fields, queue_detail.status, queue_detail.start_time, base.description,
                    base.query, base.type, base.profile
                    order by custom_field_data.custom_fields asc, asset_id desc
                    limit {batch_size} offset {offset}
                """
                cursor = execute_query(connection, cursor, query_string)
                measure_metadata = fetchall(cursor)
                measure_metadata = measure_metadata if measure_metadata else {}

            if not measure_metadata:
                return destination_connection

            custom_fields = []
            appended_metadata = []
            appended_custom_field_queries = []
            delete_metadata_query = ''

            current_date = queries.get("current_date")
            current_date = str(current_date.split('AS')[0]).strip()
            level = config.get("level")

            insert_measure_metadata = queries.get("insert_measure_metadata")

            source_measure_column_input = []
            source_measure_columm = []

            """ Databricks Connection column anme need ` string we added dynamically """
            destination_connection_object = config.get(
                "destination_connection_object")
            insert_value = "\""
            if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value, ConnectionType.MySql.value, ConnectionType.Hive.value]:
                insert_value = "`"

            for measure in measure_metadata:
                double_escap_character = "\""
                single_escap_character = "\'"
                empty_string = ""
                measure_id = measure.get("measure_id")
                asset_id = measure.get("asset_id")
                asset_id = f"""'{asset_id}'""" if asset_id else 'NULL'

                attribute_id = measure.get("attribute_id")
                attribute_id = f"""'{attribute_id}'""" if attribute_id else 'NULL'

                measure_properties = measure.get("properties")
                measure_properties = json.loads(measure_properties) if isinstance(
                    measure_properties, str) else measure_properties
                measure_properties = measure_properties if measure_properties else {}
                conditional_scoring = measure_properties.get(
                    "conditional_scoring")
                conditional_scoring = conditional_scoring if conditional_scoring else {}
                is_total_records_scope = conditional_scoring.get("is_enabled")
                is_total_records_scope = is_total_records_scope if is_total_records_scope else False
                is_measure_active = measure.get("is_active")
                is_measure_active = is_measure_active if is_measure_active else False
                total_records_scope_query = conditional_scoring.get("query")
                total_records_scope_query = total_records_scope_query if total_records_scope_query else "''"
                drift_threshold = measure.get("drift_threshold")
                try:
                    drift_threshold = json.dumps(drift_threshold, default=str)[
                        :900] if drift_threshold else ""
                except Exception as e:
                    drift_threshold = ""
                domain = measure.get("domains")
                domain = convert_json_string(domain, "", ",")
                domain = domain.replace("'","''") if domain else ""

                product = measure.get("product")
                product = convert_json_string(product, "", ",")
                product = product.replace("'","''") if product else ""

                application = measure.get("applications")
                application = convert_json_string(application, "", ",")
                application = application.replace("'","''") if application else ""

                last_run_status = measure.get("last_run_status")
                custom_measure_run_status = measure.get(
                    "custom_measure_run_status")

                last_run_status = custom_measure_run_status if custom_measure_run_status else last_run_status
                last_run_status = last_run_status if last_run_status else 'Not Run'

                comment = measure.get("description")
                # Fix comment field processing to prevent SQL syntax errors
                if comment:
                    # For SQL Server, we need to escape single quotes by doubling them
                    comment = comment.replace('"', "").replace("'", "''").replace("`", "")
                    # Also remove any other problematic characters
                    comment = comment.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                else:
                    comment = ""

                measure_status = measure.get("measure_status")
                measure_status = measure_status if measure_status else ''

                last_run_id = measure.get("last_run_id")
                last_run_id = f"'{last_run_id}'" if last_run_id is not None else 'NULL'

                measure_score = measure.get("measure_score")
                measure_score = measure_score if measure_score else 'NULL'

                run_date = measure.get('run_date') if measure.get(
                    'run_date') else 'NULL'
                created_date = measure.get('created_date') if measure.get(
                    'created_date') else 'NULL'

                if dest_connection_type in [ConnectionType.MSSQL.value, ConnectionType.BigQuery.value, ConnectionType.Db2.value, ConnectionType.DB2IBM.value, ConnectionType.Synapse.value]:
                    created_date = str(created_date).split('.')[0].strip()
                    run_date = str(run_date).split('.')[0].strip()

                if dest_connection_type == ConnectionType.Oracle.value:
                    created_date = f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        created_date) != 'NULL' else 'NULL'
                    run_date = f"""TO_TIMESTAMP_TZ('{run_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')""" if str(
                        run_date) != 'NULL' else 'NULL'
                else:
                    created_date = f"'{created_date}'" if str(
                        created_date) != 'NULL' else 'NULL'
                    run_date = f"'{run_date}'" if str(
                        run_date) != 'NULL' else 'NULL'

                custom_fields = deepcopy(measure.get("custom_fields", []))

                is_total_records_scope = int(is_total_records_scope)
                is_measure_active = int(is_measure_active)
                weigtage = float(measure.get("weightage")) if measure.get(
                    "weightage") else 100
                dimension_name = str(measure.get("dimension_name")).replace(
                    "'", "''") if measure.get("dimension_name") else ""
                measure_name = str(measure.get("measure_name")).replace(
                    "'", "''") if measure.get("measure_name") else ""
                query = str(measure.get("query")).replace("'", "''") if measure.get(
                    "type") != 'semantic' else str(measure.get("semantic_query")).replace("'", "''")
                total_records_query = total_records_scope_query.replace(
                    "'", "''")
                if dest_connection_type == ConnectionType.Databricks.value:
                    domain = domain.replace("''","\\'") if domain else ""
                    product = product.replace("''","\\'") if product else ""
                    application = application.replace("''","\\'") if application else ""
                    measure_name = measure_name.replace("''","\\'") if measure_name else ""
                    dimension_name = dimension_name.replace("''","\\'") if dimension_name else ""
                    comment = comment.replace("''","\\'") if comment else ""
                if dest_connection_type in [ConnectionType.BigQuery.value]:
                    is_total_records_scope = bool(is_total_records_scope)
                    query = str(measure.get("query")).replace(
                        "\n", ' ').replace("\r", ' ').replace("\\", "\\\\")
                    total_records_query = total_records_scope_query.replace(
                        "\n", ' ').replace("\r", ' ').replace("\\", "\\\\")
                    is_measure_active = bool(is_measure_active)
                elif dest_connection_type in [ConnectionType.SapHana.value]:
                    is_total_records_scope = bool(is_total_records_scope)
                    is_measure_active = bool(is_measure_active)
                elif dest_connection_type in [ConnectionType.Oracle.value]:
                    query = query[:999]

                # Ensure all values are properly formatted to prevent NoneType errors
                measure_metadata_input = [
                    f"""'{measure.get("connection_id")}'""",
                    asset_id,
                    attribute_id,
                    f"""'{measure.get("measure_id")}'""",
                    f"""'{measure_name}'""",
                    f"""'{dimension_name}'""",
                    weigtage,
                    f"""'{measure.get("category")}'""" if measure.get("category") else "''",
                    f"""'{drift_threshold}'""",
                    f"""'{query.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string)}'""",
                    is_total_records_scope,
                    f"'{total_records_query.replace(single_escap_character, empty_string)}'" if total_records_query and total_records_query != "''" else "''",
                    measure_score,
                    measure.get("measure_total_count") if measure.get("measure_total_count") is not None else 0,
                    measure.get("measure_valid_count") if measure.get("measure_valid_count") is not None else 0,
                    measure.get("measure_invalid_count") if measure.get("measure_invalid_count") is not None else 0,
                    "'Valid'" if measure.get("is_positive") else "'Invalid'",
                    f"'{last_run_status}'",
                    f"'{domain}'",
                    f"'{product}'",
                    f"'{application}'",
                    f"""'{comment}'""",
                    last_run_id,
                    run_date,
                    f"'{measure_status}'",
                    is_measure_active,
                    f"""'{measure.get("profile")}'""" if measure.get("profile") else "''",
                    created_date
                ]

                insert_measure_metadata = deepcopy(
                    queries.get("insert_measure_metadata"))
                insert_measure_metadata_custom_query = deepcopy(
                    queries.get("insert_measure_metadata"))

                """ Databricks Connection column name need ` string we added dynamically """
                insert_value = "\""
                if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value, ConnectionType.MySql.value, ConnectionType.Hive.value]:
                    insert_value = "`"

                last_run_id = measure.get("last_run_id") if measure.get(
                    "last_run_id") is not None else 'NULL'
                if delete_metadata_query == '':
                    table = MEASURE_METADATA
                    delete_metadata_query = queries.get(
                        "delete_measure_metadata_record")
                    delete_metadata_query = (
                        delete_metadata_query.replace(
                            "<schema_name>", schema_name)
                        .replace("<table_name>", table)
                        .replace("<database_name>.", database_name)
                        .replace("<measure_id>", measure_id)
                    )
                    delete_metadata_query = (delete_metadata_query.replace("<run_id>", last_run_id)) if last_run_id != 'NULL' else (
                        delete_metadata_query.replace("='<run_id>'", " IS NULL"))
                else:
                    append_query = f"""OR (RUN_ID='<run_id>' and MEASURE_ID='{measure_id}')"""
                    append_query = (append_query.replace("<run_id>", last_run_id)) if last_run_id != 'NULL' else (
                        append_query.replace("='<run_id>'", " IS NULL"))
                    delete_metadata_query = f"""{delete_metadata_query} {append_query}"""

                if not destination_connection:
                    destination_connection = native_connection

                diff_lists = deepcopy(diff_list)

                if custom_fields:
                    source_measure_columm = []
                    query_details = {}
                    for entry in custom_fields:
                        field_name, value = next(iter(entry.items()))
                        if field_name not in source_measure_columm:
                            source_measure_columm.append(field_name)
                            if isinstance(value, str):
                                # Properly escape the value for SQL Server
                                value = value.replace('"', "").replace("'", "''")
                                # Also remove any other problematic characters
                                value = value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                            # Ensure value is properly quoted
                            if value is None:
                                measure_metadata_input.append('NULL')
                            elif isinstance(value, (int, float)):
                                measure_metadata_input.append(str(value))
                            else:
                                measure_metadata_input.append(f"'{value}'")
                    # if destination_connection_object.get("type").lower() in [ConnectionType.Databricks.value]:
                    if diff_lists:
                        diff_lists = list(
                            set(diff_lists) - set(source_measure_columm))
                        diff_lists = list(set(diff_lists))
                        for entry in diff_lists:
                            measure_metadata_input.append(f"''")
                            custom_fields.append({f"{entry}": entry})
                    # Remove duplicates
                    unique_json_list = []
                    seen = set()

                    for obj in custom_fields:
                        # Convert to a string with sorted keys
                        serialized = json.dumps(obj, sort_keys=True)
                        if serialized not in seen:
                            seen.add(serialized)
                            unique_json_list.append(obj)
                    custom_fields_columns = ', '.join(
                        [f'{insert_value}{field_name.upper() if dest_connection_type in [ConnectionType.Db2.value,ConnectionType.DB2IBM.value] else field_name}{insert_value}' for entry in unique_json_list for field_name, _ in entry.items()])
                    if dest_connection_type in [ConnectionType.Databricks.value, ConnectionType.BigQuery.value]:
                        insert_measure_metadata_custom_query = insert_measure_metadata_custom_query.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}')
                    else:
                        insert_measure_metadata_custom_query = insert_measure_metadata_custom_query.replace(
                            'CREATED_DATE', f'CREATED_DATE, {custom_fields_columns}')
                    query_details.update(
                        {"query": insert_measure_metadata_custom_query, "values": measure_metadata_input})
                    appended_custom_field_queries.append(query_details)
                else:
                    if dest_connection_type in [ConnectionType.Databricks.value]:
                        diff_lists = list(set(diff_lists))
                        for entry in diff_lists:
                            measure_metadata_input.append(f"''")

                        custom_fields_columns = ', '.join(
                            [f'{insert_value}{entry}{insert_value}' for entry in diff_lists])
                        insert_measure_metadata = insert_measure_metadata.replace(
                            '`CREATED_DATE`', f'`CREATED_DATE`, {custom_fields_columns}') if custom_fields_columns else insert_measure_metadata
                    appended_metadata.append(measure_metadata_input)

                # Optimized data cleaning - only clean if necessary
                # validation_check = False
                # for item in measure_metadata_input:
                #     if item is None or (isinstance(item, str) and not item.startswith("'") and not item.startswith('"') and item != 'NULL'):
                #         validation_check = True
                #         break
                
                # if validation_check:
                #     # Only clean if there are items that need cleaning
                #     cleaned_measure_metadata_input = []
                #     for item in measure_metadata_input:
                #         if item is None:
                #             cleaned_measure_metadata_input.append('NULL')
                #         elif item == 'NULL':
                #             cleaned_measure_metadata_input.append('NULL')
                #         elif isinstance(item, (int, float)):
                #             cleaned_measure_metadata_input.append(str(item))
                #         elif isinstance(item, str):
                #             # If it's already a quoted string, keep it as is
                #             if (item.startswith("'") and item.endswith("'")) or (item.startswith('"') and item.endswith('"')):
                #                 cleaned_measure_metadata_input.append(item)
                #             else:
                #                 # If it's not quoted, quote it properly
                #                 cleaned_measure_metadata_input.append(f"'{item}'")
                #         else:
                #             cleaned_measure_metadata_input.append(f"'{str(item)}'")

                #     measure_metadata_input = cleaned_measure_metadata_input

                #     # Update the appended_metadata list with cleaned data
                #     if not custom_fields:
                #         # Remove the last item (which was the uncleaned data) and add the cleaned data
                #         if appended_metadata and len(appended_metadata) > 0:
                #             appended_metadata.pop()
                #         appended_metadata.append(cleaned_measure_metadata_input)
                #     else:
                #         # Update the custom field queries with cleaned data
                #         for query_detail in appended_custom_field_queries:
                #             query_detail["values"] = cleaned_measure_metadata_input

            try:
                _, native_connection = execute_native_query(
                    destination_config, delete_metadata_query, destination_connection, True, no_response=True)
            except Exception as e:
                log_error(
                    f"Failed to delete measure metadata with error: {str(e)}", e)
            native_connection = execute_metadata_push_query(
                config, appended_metadata, insert_measure_metadata, destination_connection, True)

            final_meassure_metadata_input = []
            final_insert_measure_metadata_custom_query = ""
            for value in appended_custom_field_queries:
                asset_metadata_input = value.get("values")
                final_insert_measure_metadata_custom_query = value.get("query")
                final_meassure_metadata_input.append(asset_metadata_input)
            try:
                native_connection = execute_metadata_push_query(
                    config, final_meassure_metadata_input, final_insert_measure_metadata_custom_query, destination_connection, True)
            except Exception as e:
                log_error(
                    f"Failed to insert measure metadata with error: {str(e)}", e)

            if not destination_connection:
                destination_connection = native_connection

            offset += batch_size
        return destination_connection

    except Exception as e:
        raise e


def export_user_metadata(config: dict, destination_connection_id, schema_name, **kwargs):
    """
    Insert all the User metadata into the USER_METADATA reporting table
    """
    try:
        destination_connection_object = config.get(
            "destination_connection_object")
        destination_conn_database = config.get("destination_database")

        isexits, output_details = get_user_activity_log(config, {'connection_id': destination_connection_id, 'database': destination_conn_database,
                                                                 'schema': schema_name})

        last_push_date = None
        if output_details:
            last_push_date = output_details.get('start_time')

        insert_data = {
            "connection_id": destination_connection_id,
            "queue_id": config.get('queue_id'),
            "status": ScheduleStatus.Running.value,
            "database": destination_conn_database,
            "schema": schema_name
        }
        getid = insert_user_activity_log(config, insert_data)
        max_date = None

        user_session = get_user_session(config)

        # Get Queries Based On Connection Type
        default_destination_queries = get_queries(
            {**config, "connection_type": destination_connection_object.get('type')})

        failed_rows_query = default_destination_queries.get("failed_rows", {})

        current_date_query = failed_rows_query.get("current_date")
        current_date = str(current_date_query.split('AS')[0]).strip()
        schema_query = failed_rows_query.get("schema")
        schema_query = str(schema_query) if schema_query else ""
        text_value_query = failed_rows_query.get("text_value")
        text_value_query = str(text_value_query) if text_value_query else ""
        task_config = get_task_config(config, kwargs)

        df = pd.DataFrame.from_dict(user_session)
        df = df.replace(np.nan, '')

        column_list = ""
        static_col = ""

        source_datatype = generate_lookup_source_large_datatype(
            {'connection_type': destination_connection_object.get("type").lower()})

        special_string = "`" if destination_connection_object.get("type").lower(
        ) in [ConnectionType.BigQuery.value, ConnectionType.Databricks.value, ConnectionType.MySql.value, ConnectionType.Hive.value] else "\""

        for col in df.columns:
            column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
            static_col = f"""{static_col}{special_string}{col}{special_string} ,"""
        column_list = column_list[:-1]
        static_col = static_col[:-1]
        column_list = f"""{column_list}"""
        static_col = f"""{static_col}"""

        lookup_process_queries = default_destination_queries.get(
            "lookup_process", {})
        create_table_query = lookup_process_queries.get("create_table")
        insert_table_query = lookup_process_queries.get("insert_table")
        drop_table_query = lookup_process_queries.get("drop_table")

        config.update({
            "connection_type": destination_connection_object.get('type'),
            "source_connection_id": destination_connection_object.get('airflow_connection_id'),
            "connection": destination_connection_object
        })
        is_exists = True
        if destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                    query_string = failed_rows_query.get("metadata_table_exists")
                    query_string = query_string.replace("<db_name>", schema_name).replace("<table_name>", f"""USER_SESSION""")
                    table_metadata, _ = execute_native_query(config, query_string, None, True)
                    is_exists = bool(table_metadata)
        if is_exists:
            drop_table_query = drop_table_query.replace("<database_name>", destination_conn_database).replace("<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<query_string>", column_list)
            execute_native_query(config, drop_table_query,None, True, no_response=True)
        create_table_query = create_table_query.replace("<database_name>", destination_conn_database).replace(
            "<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<query_string>", column_list)
        execute_native_query(config, create_table_query,
                             None, True, no_response=True)
        insert_query = insert_table_query.replace("<database_name>", destination_conn_database).replace(
            "<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<columns>", static_col)
        orginal_value = f""""""
        df_length = len(df)
        saphana_insert_queries = f""""""
        teradata_insert_queries = f""""""
        failed_record_query = f"""
                                DO
                                BEGIN 
                                    <insert_queries>
                                END;
                                """
        for index, row in df.iterrows():
            index_value = index + 1
            insert_value = "values"
            if config.get('connection_type', '').lower().lower() in [ConnectionType.SapHana.value]:
                string_list = [str(element) for element in row.values]
                orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                    string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""
                orginal_value = orginal_value[:-1]
                saphana_insert_query = deepcopy(insert_query)
                saphana_insert_queries = f"""{saphana_insert_queries}{saphana_insert_query.replace("<insert_query>", f'''{insert_value} {orginal_value}''')};"""
                orginal_value = f""""""
            elif destination_connection_object.get("type").lower() in [ConnectionType.Teradata.value]:
                        string_list = [str(element) for element in row.values]
                        orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                            string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""
                        orginal_value = orginal_value[:-1]
                        teradata_insert_query = deepcopy(insert_query)
                        teradata_insert_query = f"""{teradata_insert_queries} {teradata_insert_query.replace("<insert_query>", f'''{insert_value} {orginal_value}''')};"""
                        teradata_insert_queries = teradata_insert_query
                        orginal_value = f""""""
            else:
                if config.get('connection_type', '').lower() == ConnectionType.Synapse.value or config.get('connection_type', '').lower() == ConnectionType.Oracle.value:
                    special_value = ""
                    if config.get('connection_type', '').lower() == ConnectionType.Oracle.value:
                        special_value = f"""FROM DUAL"""
                    string_list = [
                        f"""'{str(element)}'""" for element in row.values]
                    insert_value = ""
                    orginal_value = f"""{orginal_value} SELECT {str(string_list[0])} {special_value} UNION ALL""" if len(
                        string_list) == 1 else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} UNION ALL"""
                else:
                    string_list = [str(element) for element in row.values]
                    orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                        string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""

            if df_length == index_value:
                if destination_connection_object.get("type").lower() == ConnectionType.SapHana.value:
                    saphana_insert_query = saphana_insert_queries
                    failed_record_query = failed_record_query.replace(
                        "<insert_queries>", saphana_insert_query)
                    execute_native_query(
                        config, failed_record_query, None, True, no_response=True)
                    saphana_insert_queries = f""""""
                elif destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                            execute_native_query(
                                config, teradata_insert_queries, None, True, no_response=True)
                            teradata_insert_queries = f""""""
                else:
                    orginal_value = orginal_value[:-
                                                  1] if insert_value else orginal_value[:-9]
                    insert_data = insert_query.replace(
                        "<insert_query>", f"""{insert_value} {orginal_value}""")
                    execute_native_query(
                        config, insert_data, None, True, no_response=True)
                    orginal_value = f""""""
            elif (index_value % 998 == 0):
                if destination_connection_object.get("type").lower() == ConnectionType.SapHana.value:
                    saphana_insert_query = saphana_insert_queries
                    failed_record_query = failed_record_query.replace(
                        "<insert_queries>", saphana_insert_query)
                    execute_native_query(
                        config, failed_record_query, None, True, no_response=True)
                    saphana_insert_queries = f""""""
                elif destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                            execute_native_query(
                                config, teradata_insert_queries, None, True, no_response=True)
                            teradata_insert_queries = f""""""
                else:
                    orginal_value = orginal_value[:-
                                                  1] if insert_value else orginal_value[:-9]
                    insert_data = insert_query.replace(
                        "<insert_query>", f"""{insert_value} {orginal_value}""")
                    execute_native_query(
                        config, insert_data, None, True, no_response=True)
                    orginal_value = f""""""

        """ User Activity Insert Details """
        user_activity = get_user_activity(config, last_push_date)
        saphana_insert_queries = f""""""
        failed_record_query = f"""
                                DO
                                BEGIN 
                                    <insert_queries>
                                END;
                                """
        if user_activity:
            df = pd.DataFrame.from_dict(user_activity)
            df = df.replace(np.nan, '')
            max_date = max(df['CREATED_DATE'])

            column_list = ""
            static_col = ""

            for col in df.columns:
                column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
                static_col = f"""{static_col}{special_string}{col}{special_string} ,"""
            column_list = column_list[:-1]
            static_col = static_col[:-1]
            column_list = f"""{column_list}"""
            static_col = f"""{static_col}"""

            lookup_process_queries = default_destination_queries.get(
                "lookup_process", {})
            create_table_query = lookup_process_queries.get("create_table")
            insert_table_query = lookup_process_queries.get("insert_table")
            drop_table_query = lookup_process_queries.get("drop_table")
            is_exists = True
            if destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                    query_string = failed_rows_query.get("metadata_table_exists")
                    query_string = query_string.replace("<db_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""")
                    table_metadata, _ = execute_native_query(config, query_string, None, True)
                    is_exists = bool(table_metadata)
            # Create USER_ACTIVITY table (same logic as USER_SESSION)
            if is_exists:
                drop_table_query = drop_table_query.replace("<database_name>", destination_conn_database).replace(
                    "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<query_string>", column_list)
                execute_native_query(
                    config, drop_table_query, None, True, no_response=True)
            create_table_query = create_table_query.replace("<database_name>", destination_conn_database).replace(
                "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<query_string>", column_list)
            execute_native_query(
                config, create_table_query, None, True, no_response=True)
            if user_activity:
                insert_query = insert_table_query.replace("<database_name>", destination_conn_database).replace(
                    "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<columns>", static_col)
                orginal_value = f""""""
                df_length = len(df)
                for index, row in df.iterrows():
                    index_value = index + 1
                    insert_value = "values"
                    if destination_connection_object.get("type").lower() in [ConnectionType.SapHana.value]:
                        string_list = [str(element) for element in row.values]
                        orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                            string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""
                        orginal_value = orginal_value[:-1]
                        saphana_insert_query = deepcopy(insert_query)
                        saphana_insert_query = f"""{saphana_insert_queries} {saphana_insert_query.replace("<insert_query>", f'''{insert_value} {orginal_value}''')};"""
                        saphana_insert_queries = saphana_insert_query
                        orginal_value = f""""""
                    elif destination_connection_object.get("type").lower() in [ConnectionType.Teradata.value]:
                        string_list = [str(element) for element in row.values]
                        orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                            string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""
                        orginal_value = orginal_value[:-1]
                        teradata_insert_query = deepcopy(insert_query)
                        teradata_insert_query = f"""{teradata_insert_queries} {teradata_insert_query.replace("<insert_query>", f'''{insert_value} {orginal_value}''')};"""
                        teradata_insert_queries = teradata_insert_query
                        orginal_value = f""""""
                    else:
                        if destination_connection_object.get("type").lower() == ConnectionType.Synapse.value or destination_connection_object.get("type").lower() == ConnectionType.Oracle.value:
                            special_value = ""
                            if destination_connection_object.get("type").lower() == ConnectionType.Oracle.value:
                                special_value = f"""FROM DUAL"""
                            string_list = [
                                f"""'{str(element)}'""" for element in row.values]
                            insert_value = ""
                            orginal_value = f"""{orginal_value} SELECT {str(string_list[0])} {special_value} UNION ALL""" if len(
                                string_list) == 1 else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} UNION ALL"""
                        else:
                            string_list = [str(element)
                                           for element in row.values]
                            orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                                string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""

                    if df_length == index_value:
                        if destination_connection_object.get("type").lower() == ConnectionType.SapHana.value:
                            saphana_insert_query = saphana_insert_queries
                            failed_record_query = failed_record_query.replace(
                                "<insert_queries>", saphana_insert_query)
                            execute_native_query(
                                config, failed_record_query, None, True, no_response=True)
                            saphana_insert_queries = f""""""
                        elif destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                            execute_native_query(
                                config, teradata_insert_queries, None, True, no_response=True)
                            teradata_insert_queries = f""""""
                        else:
                            orginal_value = orginal_value[:-
                                                          1] if insert_value else orginal_value[:-9]
                            insert_data = insert_query.replace(
                                "<insert_query>", f"""{insert_value} {orginal_value}""")
                            execute_native_query(
                                config, insert_data, None, True, no_response=True)
                            orginal_value = f""""""
                    elif (index_value % 998 == 0):
                        if destination_connection_object.get("type").lower() == ConnectionType.SapHana.value:
                            saphana_insert_query = saphana_insert_queries
                            failed_record_query = failed_record_query.replace(
                                "<insert_queries>", saphana_insert_query)
                            execute_native_query(
                                config, failed_record_query, None, True, no_response=True)
                            saphana_insert_queries = f""""""
                        elif destination_connection_object.get("type").lower() == ConnectionType.Teradata.value:
                            execute_native_query(
                                config, teradata_insert_queries, None, True, no_response=True)
                            teradata_insert_queries = f""""""
                        else:
                            orginal_value = orginal_value[:-
                                                          1] if insert_value else orginal_value[:-9]
                            insert_data = insert_query.replace(
                                "<insert_query>", f"""{insert_value} {orginal_value}""")
                            execute_native_query(
                                config, insert_data, None, True, no_response=True)
                            orginal_value = f""""""
    finally:
        insert_data = {
            "status": ScheduleStatus.Completed.value,
            "start_time": max_date,
            "id": getid.get('id') if getid else None
        }
        save_user_activity_log(config, insert_data)

def get_user_session_metadata(config: dict):
    user_metadata = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = """
            SELECT 
                u.id AS user_id,
                u.last_login,
                r.name AS userrole,
                CONCAT(u.first_name, ' ', u.last_name) AS username,
                u.email AS usermailid,
                session_stats.minimum_duration,
                session_stats.maximum_duration,
                session_stats.average_duration,
                COALESCE(login_data.login_count, 0) AS login_count,
                COALESCE(log_data.no_of_logs, 0) AS no_of_logs
            FROM core.users u
            INNER JOIN core.roles r ON r.id = u.role_id
            LEFT JOIN (
                SELECT 
                    user_id,
                    MIN(duration) AS minimum_duration,
                    MAX(duration) AS maximum_duration,
                    AVG(duration) AS average_duration
                FROM (
                    SELECT 
                        user_id,
                        EXTRACT(EPOCH FROM (session_end_time - session_start_time)) AS duration
                    FROM core.user_session_track
                    WHERE session_end_time IS NOT NULL
                ) AS durations
                GROUP BY user_id
            ) AS session_stats ON session_stats.user_id = u.id
            LEFT JOIN (
                SELECT 
                    user_id, 
                    COUNT(DISTINCT id) AS login_count
                FROM core.user_session_track
                GROUP BY user_id
            ) AS login_data ON login_data.user_id = u.id
            LEFT JOIN (
                SELECT 
                    created_by::uuid as created_by, 
                    COUNT(DISTINCT id) AS no_of_logs
                FROM core.version_history
                GROUP BY created_by
            ) AS log_data ON log_data.created_by = u.id
            """
            cursor = execute_query(connection, cursor, query_string)
            metadata = fetchall(cursor)
            metadata = metadata if metadata else 0

            for data in metadata:
                user_metadata.append({
                    "USER_ID": data.get('user_id'),
                    "LAST_LOGGED_IN": data.get('last_login'),
                    "USERROLE": data.get('userrole'),
                    "USERNAME": data.get('username'),
                    "USERMAILID": data.get('usermailid'),
                    "TOTAL_LOGIN_COUNT": data.get("login_count"),
                    "AVG_SESSION_TIME": data.get("average_duration"),
                    "MIN_SESSION_TIME": data.get("minimum_duration"),
                    "MAX_SESSION_TIME": data.get("maximum_duration"),
                    "AUDITS_COUNT": data.get("no_of_logs")
                })
    except Exception as e:
        log_error("User Session Metadata", e)
    finally:
        return user_metadata
    

def get_connection_metadata(config: dict):
    """
    Insert the connection metadata for the current run
    """
    connection_metadata = []
    queue_id = config.get("queue_id")
    connection_psql = get_postgres_connection(config)
    with connection_psql.cursor() as cursor:
        query_string = f"""
            select id, name, type, is_valid, created_date
            from core.connection
            where is_delete='false' and is_default='false'
        """
        cursor = execute_query(connection_psql, cursor, query_string)
        connection_data = fetchall(cursor)
        connection_data = connection_data if connection_data else []
        for connection in connection_data:
            connection_metadata.append({
                "CONNECTION_ID": connection.get("id"),
                "CONNECTION_NAME": connection.get("name"),
                "DATASOURCE": connection.get("type"),
                "STATUS": "Valid" if connection.get("is_valid") else "Deprecated",
                "CREATED_DATE": str(connection.get("created_date")),
                "RUN_ID": queue_id
            })
    return connection_metadata

def get_asset_metadata(config: dict):
    """
    Insert the connection metadata for the current run
    """
    asset_metadata = []
    queue_id = config.get("queue_id")
    delimeter = config.get("delimeter")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with comments as (
            select conversations.asset_id as asset_id,conversations.id as conversation_id, comment,conversations.title,conversations.created_date as created_at, rating,
            case when users.first_name is not null then concat(users.first_name,' ', users.last_name) else users.email end as username
            from core.conversations 
            join core.users on users.id=conversations.created_by_id
            where conversations.level='asset'
        ),
        custom_field_data as (
            select
            field_property.asset_id, 
            json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
            from core.fields
            left join core.field_property on field_property.field_id = fields.id
            where value is not null and level='Asset' and fields.is_active is true
            group by field_property.asset_id
        ),
        max_invalid_records as (
            select max(measure.invalid_rows) as max_invalid_rows, 
            measure.asset_id from core.measure join core.base_measure on base_measure.id=measure.base_measure_id 
            and base_measure.level!='measure' where measure.is_active=true  
            group by measure.asset_id)
        select con.id as connection_id, con.name as connection_name, con.type as connection_type,
        con.is_valid, con.credentials, ast.status, ast.last_run_id,
        ast.id as asset_id, ast.name as asset_name, ast.properties, 
        ast.score as asset_score, data.row_count as asset_total_rows,
        ast.issues as asset_issues, ast.alerts as asset_alerts, data.primary_columns, ast.description,
        coalesce(string_agg(distinct users.id::TEXT, ','), '') as steward_users_id,
        coalesce(string_agg(distinct users.email::TEXT, ','), '') as steward_users,
        coalesce(string_agg(distinct domain.technical_name::TEXT, ','), '') domains,
        coalesce(string_agg(distinct product.technical_name::TEXT, ','), '')  product,
        coalesce(string_agg(distinct application.name::TEXT, ','), '') applications,
        coalesce(string_agg(distinct terms.technical_name::TEXT, ','), '')  as terms,
        coalesce(json_agg(distinct jsonb_build_object('comment',comments.comment,'title', comments.title,'created_at', 
        comments.created_at, 'rating',comments.rating, 'username',comments.username)) filter (where conversation_id is not null), '[]') as conversation,
        CAST(custom_field_data.custom_fields as JSON) as custom_fields, ast.created_date, max_invalid_records.max_invalid_rows as asset_invalid_rows
        from core.asset as ast
        join core.data on data.asset_id = ast.id
        join core.connection as con on con.id=ast.connection_id
        left join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
        left join core.user_mapping on user_mapping.asset_id=ast.id
        left join core.users on users.id=user_mapping.user_id
        left join core.domain_mapping as vdom on vdom.asset_id=ast.id 
        left join core.product_mapping as vpt on vpt.asset_id=ast.id 
        left join core.application_mapping as vapp on vapp.asset_id=ast.id 
        left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
        left join core.domain on domain.id = vdom.domain_id
        left join core.product on product.id = vpt.product_id
        left join core.application on application.id = vapp.application_id
        left join core.terms on terms.id=vterms.term_id 
        left join comments on comments.asset_id=ast.id
        left join custom_field_data on ast.id=custom_field_data.asset_id
        left join core.field_property on field_property.asset_id = ast.id
        left join core.fields on field_property.field_id = fields.id and fields.level='Asset' and field_property.value is not null
        left join max_invalid_records on ast.id=max_invalid_records.asset_id
        where ast.is_active is true and ast.is_delete is false and con.is_active is true
        group by con.id, ast.id, data.id, custom_field_data.custom_fields, max_invalid_records.max_invalid_rows
        """
        cursor = execute_query(connection, cursor, query_string)
        asset_data = fetchall(cursor)
        asset_data = asset_data if asset_data else []
        for metadata in asset_data:
            asset_id = metadata.get("asset_id")
            job_query_string = f"""
                select queue.start_time from core.request_queue as rq
                join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category not in ('catalog_update', 'export_failed_rows')
                where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
                order by queue.created_date desc limit 1
            """
            cursor = execute_query(
                connection, cursor, job_query_string)
            last_job_detail = fetchone(cursor)
            last_job_detail = last_job_detail if last_job_detail else {}

            conversations = metadata.get("conversation")
            comments = []
            for conversation in conversations:
                comment_text = ""
                comment = conversation.get("comment", {})
                comment = json.loads(comment)
                comment_block = comment.get("blocks", [])
                for block in comment_block:
                    comment_text += block["text"] + "\n"
                comment_text = comment_text.replace(
                    "'", '') if comment_text else ""
                comments.append({
                    "title": conversation.get("title"),
                    "rating": conversation.get("rating"),
                    "created_at": conversation.get("created_at"),
                    "username": conversation.get("username"),
                    "comment": comment_text
                })
            comments = json.dumps(comments) if comments else ""
            comments = comments.replace("'", '') if comments else ""

            # Get Database and schema
            asset_properties = metadata.get("properties")
            asset_properties = (
                json.loads(asset_properties)
                if isinstance(asset_properties, str)
                else asset_properties
            )
            asset_properties = asset_properties if asset_properties else {}
            schema = asset_properties.get("schema")
            schema = schema if schema else ""
            database = asset_properties.get("database", "")
            description = metadata.get("description")
            description = description.replace('"', "").replace("'", "''") if description else ""
            asset_name = metadata.get("asset_name")
            asset_name = asset_name.replace('"', "").replace("'", "''") if asset_name else ""

            # Invalid Identifier
            primary_attributes = metadata.get("primary_columns")
            primary_attributes = (
                json.loads(primary_attributes)
                if isinstance(primary_attributes, str)
                else primary_attributes
            )
            primary_attributes = primary_attributes if primary_attributes else []
            primary_attributes = [attribute.get("name") for attribute in primary_attributes]
            identifier_key = f"{delimeter}".join(primary_attributes)
            custom_fields = deepcopy(metadata.get("custom_fields", []))
            if isinstance(custom_fields, str):
                custom_fields = json.loads(custom_fields)
            custom_fields = custom_fields if custom_fields else []


            asset_total_rows = metadata.get("asset_total_rows")
            asset_total_rows = asset_total_rows if asset_total_rows else 0
            asset_invalid_rows = metadata.get("asset_invalid_rows")
            asset_invalid_rows = asset_invalid_rows if asset_invalid_rows else 0
            passed_records = asset_total_rows - asset_invalid_rows

            metadata_item = {
                "CONNECTION_ID": metadata.get("connection_id"),
                "DATABASE_NAME": database,
                "SCHEMA_NAME": schema,
                "ASSET_ID": metadata.get("asset_id"),
                "TABLE_NAME": asset_name,
                "ALIAS_NAME": asset_name,
                "DQ_SCORE": metadata.get("asset_score") if metadata.get("asset_score") else 'NULL',
                "TOTAL_RECORDS": metadata.get("asset_total_rows"),
                "PASSED_RECORDS":  passed_records,
                "FAILED_RECORDS": metadata.get("asset_invalid_rows"),
                "ISSUE_COUNT":  metadata.get("asset_issues"),
                "ALERT_COUNT": metadata.get("asset_alerts"),
                "STATUS": metadata.get("status"),
                "IDENTIFIER_KEY": identifier_key,
                "RUN_ID": queue_id,
                "CONVERSATIONS": comments,
                "DESCRIPTION": description,
                "STEWARD_USER_ID": metadata.get("steward_users_id"),
                "STEWARD_USERS": metadata.get("steward_users"),
                "DOMAIN": metadata.get("domains"),
                "PRODUCT": metadata.get("product"),
                "APPLICATIONS": metadata.get("applications"),
                "TERMS": metadata.get("terms"),
                "LAST_RUN_TIME": str(last_job_detail.get("start_time")) if last_job_detail.get("start_time")  else "NULL",
                "CREATED_DATE": str(metadata.get("created_date"))
            }

            # Update Custom Field
            for field in custom_fields:
                for key in field.keys():
                    value = field[key]
                    value = value if value else ""
                    metadata_item.update({key: value})
            asset_metadata.append(metadata_item)
    return asset_metadata


def get_attribute_metadata(config: dict):
    """
    Insert all the Attribute metadata into the ATTRIBUTE_METADATA reporting table
    """
    try:
        attribute_metadata = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                with last_run as (
                    select rq.asset_id, queue.start_time, queue.status,
                    row_number() over (partition by rq.asset_id order by queue.start_time desc) as rownum
                    from core.request_queue as rq
                    left join core.request_queue_detail as queue on queue.queue_id = rq.id
                    where lower(queue.status) in ('failed', 'completed', 'pending')  and category = 'profile'
                    order by rownum
                ),
                custom_field_data as (
                    select
                    field_property.attribute_id, 
                    json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
                    from core.fields
                    left join core.field_property on field_property.field_id = fields.id
                    where value is not null and level='Attribute' and fields.is_active is true
                    group by field_property.attribute_id
                ),
                max_invalid_records as (
                    select max(measure.invalid_rows) as max_invalid_rows, 
                    measure.attribute_id from core.measure join core.base_measure on base_measure.id=measure.base_measure_id 
                    and base_measure.level!='measure' where measure.is_active=true  
                    group by measure.attribute_id
                )
                select ast.id as asset_id, attr.id as attribute_id, attr.name as attribute_name,attr.status, attr.score as attribute_score,
                attr.alerts as attribute_alerts, attr.issues as attribute_issues,attr.description,term.technical_name as term,
                attr.row_count as attribute_total_rows, max_invalid_records.max_invalid_rows as attribute_invalid_rows,attr.is_blank,
                attr.min_length,attr.max_length,attr.derived_type as datatype,attr.min_value,attr.max_value,attr.is_null,
                attr.is_primary_key as is_primary,attr.is_unique, attr.last_run_id, 
                coalesce(string_agg(distinct tags.technical_name::TEXT, ','), '') tags,
                max(det.start_time) as start_time, det.status as job_status, attr.created_date,
                CAST(custom_field_data.custom_fields as JSON) as custom_fields
                from core.asset as ast
                join core.connection as con on con.id=ast.connection_id
                join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
                left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
                left join core.tags_mapping as vtags on vtags.attribute_id = attr.id
                left join core.terms as term on term.id=vterms.term_id                   
                left join core.tags as tags on tags.id=vtags.tags_id
                left join last_run as det on det.asset_id=ast.id and det.rownum = 1
                left join max_invalid_records on max_invalid_records.attribute_id=attr.id
                left join custom_field_data on attr.id=custom_field_data.attribute_id
                where ast.is_active is true and ast.is_delete is false and con.is_active is true
                group by ast.id,attr.id,term.id, det.status, attr.last_run_id, attribute_invalid_rows, custom_field_data.custom_fields
            """
            cursor = execute_query(connection, cursor, query_string)
            attribute_list = fetchall(cursor)
            attribute_list = attribute_list if attribute_list else []

            for metadata in attribute_list:
                min_value = metadata.get("min_value")
                min_value = min_value if min_value else ''
                if min_value and isinstance(min_value, str):
                    min_value = (
                        min_value.replace("\\", '\\\\')
                        .replace("'", "''")
                    )
                max_value = metadata.get("max_value")
                max_value = max_value if max_value else ''
                if max_value and isinstance(max_value, str):
                    max_value = (
                        max_value.replace("\\", '\\\\')
                        .replace("'", "''")
                    )
                min_length = metadata.get("min_length")
                min_length = min_length if min_length else ''
                max_length = metadata.get("max_length")
                max_length = max_length if max_length else ''
                is_primary = metadata.get("is_primary") if metadata.get(
                    "is_primary") else False
                last_run_time = str(metadata.get('start_time')) if metadata.get('start_time') else None
                last_run_time = last_run_time if last_run_time else 'NULL'

                created_date = metadata.get('created_date')
                created_date = created_date if created_date else 'NULL'
                job_status = metadata.get('job_status')
                job_status = job_status if job_status else "Completed"
                custom_fields = deepcopy(metadata.get("custom_fields", []))
                if isinstance(custom_fields, str):
                    custom_fields = json.loads(custom_fields)
                custom_fields = custom_fields if custom_fields else []
                status = metadata.get("status") if metadata.get(
                    "status") else "Not Run"
                
                attribute_invalid_rows = metadata.get("attribute_invalid_rows", 0) if metadata.get("attribute_invalid_rows", 0) else 0
                attribute_total_rows = metadata.get("attribute_total_rows", 0) if metadata.get("attribute_total_rows", 0) else 0
                attribute_valid_rows = attribute_total_rows - attribute_invalid_rows
                attribute = {
                    "ASSET_ID": metadata.get("asset_id"),
                    "ATTRIBUTE_ID": metadata.get("attribute_id"),
                    "ATTRIBUTE_NAME": metadata.get("attribute_name"),
                    "DQ_SCORE": metadata.get("attribute_score") if metadata.get("attribute_score") else "null",
                    "TOTAL_RECORDS": attribute_total_rows,
                    "PASSED_RECORDS": attribute_valid_rows,
                    "FAILED_RECORDS": attribute_invalid_rows,
                    "ISSUE_COUNT": metadata.get("attribute_issues"),
                    "ALERT_COUNT": metadata.get("attribute_alerts"),
                    "STATUS": status,
                    "RUN_ID":  metadata.get("last_run_id"),
                    "DESCRIPTION": metadata.get("description"),
                    "TERM": metadata.get("term") if metadata.get("term") else "",
                    "MIN_LENGTH": metadata.get("min_length") if metadata.get("min_length") else "",
                    "MAX_LENGTH": metadata.get("max_length") if metadata.get("max_length") else "",
                    "DATATYPE": metadata.get('datatype'),
                    "MIN_VALUE": min_value,
                    "MAX_VALUE": max_value,
                    "IS_PRIMARY": is_primary,
                    "TAGS": metadata.get("tags"),
                    "LAST_RUN_TIME": last_run_time,
                    "JOB_STATUS": job_status,
                    "CREATED_DATE": str(created_date),
                }
                # Update Custom Field
                for field in custom_fields:
                    for key in field.keys():
                        value = field[key]
                        value = value if value else ""
                        attribute.update({key: value})
                attribute_metadata.append(attribute)

        return attribute_metadata
    except Exception as e:
        raise e


def get_measure_metadata(config: dict):
    measure_metadata = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with custom_field_data as (
                select
                field_property.measure_id, 
                json_agg(json_build_object(fields.name, field_property.value)order by fields.name desc)::TEXT as custom_fields
                from core.fields
                left join core.field_property on field_property.field_id = fields.id
                where value is not null and level='Measure' and fields.is_active is true
                group by field_property.measure_id
            ),
            max_start_time AS (
                SELECT mes.id as measure_id, max(queue_detail.start_time) AS max_start_time
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and queue_detail.category not in ('export_failed_rows', 'catalog_update')
                group by mes.id
            ),
            custom_measure_status AS (
                select mes.id, queue_detail.status
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and mes.id=queue_detail.measure_id
                where queue_detail.status is not null and queue_detail.category not in ('export_failed_rows')
                group by mes.id, queue_detail.status 
            )
            select mes.connection_id as connection_id, ast.id as asset_id, attr.id as attribute_id,
            mes.id as measure_id, base.name as measure_name, mes.weightage, base.category,
            mes.row_count as measure_total_count, mes.valid_rows AS measure_valid_count, mes.invalid_rows AS measure_invalid_count,
            mes.score as measure_score, dim.name as dimension_name, base.properties,mes.last_run_id,
            coalesce(json_agg(distinct domain.technical_name) filter (where domain.id is not null), '[]') domains,
            coalesce(json_agg(distinct product.technical_name) filter (where product.id is not null), '[]') product,
            coalesce(json_agg(distinct application.name) filter (where application.id is not null), '[]') applications,
            queue_detail.status as last_run_status, CAST(custom_field_data.custom_fields as JSON) as custom_fields,
            queue_detail.start_time as run_date, mes.created_date, base.description, mes.is_positive, mes.drift_threshold,
            mes.status as measure_status, (
                        select 
                        case 
                            when bas_mes.category = 'behavioral' then 
                            case 
                                when measure.drift_threshold = '{{}}'::jsonb then 'failed'
                                else 'completed'
                            end
                            else custom.status
                        end as status
                        from core.measure
                        left join core.base_measure bas_mes on bas_mes.id = measure.base_measure_id
                        left join custom_measure_status custom on measure.id = custom.id
                        where measure.id = mes.id) as custom_measure_run_status,
                            base.query, mes.semantic_query, base.type, mes.is_active, base.profile
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id and mes.connection_id is not null
            left join core.asset as ast on ast.id=mes.asset_id
            left join core.connection as conn on conn.id=mes.connection_id
            left join core.dimension as dim on dim.id=mes.dimension_id
            left join core.attribute as attr on attr.id=mes.attribute_id
            left join core.domain_mapping as vdom on (vdom.asset_id=ast.id or vdom.measure_id=mes.id)
            left join core.product_mapping as vpt on (vpt.asset_id=ast.id or vpt.measure_id=mes.id)
            left join core.application_mapping as vapp on (vapp.asset_id=ast.id or vapp.measure_id=mes.id)
            left join core.domain on domain.id = vdom.domain_id
            left join core.product on product.id = vpt.product_id
            left join core.application on application.id = vapp.application_id
            left join core.request_queue_detail as queue_detail on mes.last_run_id=queue_detail.queue_id
            left join custom_field_data on mes.id=custom_field_data.measure_id
            left join custom_measure_status as custom on mes.id=custom.id
            join max_start_time ON mes.id = max_start_time.measure_id 
            AND (queue_detail.start_time = max_start_time.max_start_time or (queue_detail.start_time is null 
            and max_start_time.max_start_time is null))
            where (ast.is_active is true or ast.is_active is null) and (ast.is_delete is false or ast.is_delete is null) 
            and conn.is_active is true and conn.is_delete is false
            and (attr.is_selected is true or attr.is_selected is null)
            and mes.is_delete is false
            group by mes.connection_id, ast.id, attr.id, mes.id, base.name, base.category, dim.name, custom.status,
            base.properties, custom_field_data.custom_fields, queue_detail.status, queue_detail.start_time, base.description,
            base.query, base.type, base.profile
            order by custom_field_data.custom_fields asc, asset_id desc
        """
        cursor = execute_query(connection, cursor, query_string)
        measure_list = fetchall(cursor)
        measure_list = measure_list if measure_list else []

        for metadata in measure_list:
            drift_threshold = metadata.get("drift_threshold")
            drift_threshold = json.dumps(drift_threshold, default=str) if drift_threshold else ""
            query = (
                str(metadata.get("query")).replace("'", "''")
                if metadata.get("type") != "semantic"
                else str(metadata.get("semantic_query")).replace("'", "''")
            )
            double_escap_character = '"'
            single_escap_character = "'"
            empty_string = ""
            query = query.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string)
            measure_properties = metadata.get("properties")
            measure_properties = (
                json.loads(measure_properties)
                if isinstance(measure_properties, str)
                else measure_properties
            )
            measure_properties = measure_properties if measure_properties else {}
            conditional_scoring = measure_properties.get("conditional_scoring", {})
            conditional_scoring = conditional_scoring if conditional_scoring else {}
            total_records_scope_query = conditional_scoring.get("query") if conditional_scoring else ""
            total_records_scope_query = total_records_scope_query if total_records_scope_query else "''"
            last_run_status = metadata.get("last_run_status")
            custom_measure_run_status = metadata.get("custom_measure_run_status")
            last_run_status = (
                custom_measure_run_status if custom_measure_run_status else last_run_status
            )
            last_run_status = last_run_status if last_run_status else "Not Run"
            comment = metadata.get("description", "")
            comment = comment.replace('"', "").replace("'", "\\'").replace("`", "") if comment else ""
            measure_status = metadata.get("measure_status")
            measure_status = measure_status if measure_status else ""
            is_measure_active = metadata.get("is_active")
            is_measure_active = is_measure_active if is_measure_active else False
            last_run_id = metadata.get("last_run_id") if metadata.get(
                    "last_run_id") is not None else 'NULL'
            custom_fields = deepcopy(metadata.get("custom_fields", []))
            if isinstance(custom_fields, str):
                custom_fields = json.loads(custom_fields)
            custom_fields = custom_fields if custom_fields else []
            measure = {
                "CONNECTION_ID": metadata.get("connection_id"),
                "ASSET_ID": metadata.get("asset_id"),
                "ATTRIBUTE_ID": metadata.get("attribute_id"),
                "MEASURE_ID": metadata.get("measure_id"),
                "MEASURE_NAME": metadata.get("measure_name"),
                "MEASURE_DIMENSION": metadata.get("dimension_name") if metadata.get("dimension_name") else '',
                "MEASURE_WEIGHTAGE": float(metadata.get("weightage")) if metadata.get("weightage") else 100,
                "MEASURE_TYPE": metadata.get("category"),
                "MEASURE_THRESHOLD":  str(drift_threshold),
                "MEASURE_QUERY": query,
                "TOTAL_RECORDS_SCOPE": conditional_scoring.get("is_enabled") if conditional_scoring.get("is_enabled") else False,
                "TOTAL_RECORDS_SCOPE_QUERY": total_records_scope_query,
                "DQ_SCORE": metadata.get("measure_score") if metadata.get("measure_score") else "null",
                "TOTAL_RECORDS": metadata.get("measure_total_count", 0),
                "PASSED_RECORDS": metadata.get("measure_valid_count", 0),
                "FAILED_RECORDS": metadata.get("measure_invalid_count", 0),
                "STATUS": "Valid" if metadata.get("is_positive") else "Invalid",
                "RUN_STATUS": last_run_status.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string),
                "DOMAIN": metadata.get("domains"),
                "PRODUCT": metadata.get("product"),
                "APPLICATION": metadata.get("applications"),
                "COMMENT": comment,
                "RUN_ID": last_run_id,
                "RUN_DATE": str(metadata.get("run_date")) if metadata.get("run_date") else None,
                "MEASURE_STATUS": measure_status,
                "IS_ACTIVE": is_measure_active,
                "PROFILE": metadata.get("profile"),
                "CREATED_DATE": str(metadata.get("created_date")),
            }
            # Update Custom Field
            for field in custom_fields:
                for key in field.keys():
                    value = field[key]
                    value = value if value else ""
                    measure.update({key: value})
            measure_metadata.append(measure)

    return measure_metadata

def get_user_activity_metadata(config: dict):
    user_activity_metadata = []
    max_dates = []
    try:
        isexits, output_details = get_user_activity_log(config, {'connection_id': 'external_storage'})
        last_push_date = None
        if output_details:
            last_push_date = output_details.get('start_time')
        insert_data = {
            "connection_id": "exernal_storage",
            "queue_id": config.get('queue_id'),
            "status": ScheduleStatus.Running.value,
            "database": '',
            "schema": ''
        }
        activity_insert_id = insert_user_activity_log(config, insert_data)
        filter_query = f"""where al.created_date > '{last_push_date}'""" if last_push_date else f""""""
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                SELECT al.created_by as user_id,
                CONCAT(users.first_name, ' ', users.last_name) AS username,
                al.role as user_role, al.email as Email, al.attribute_id, al.asset_id,
                al.created_date as created_date, 
                al.page, al.module, al.submodule, al.message,
                ms.technical_name as measure_name, asset.name as asset_name, attribute.name as attribute_name,
                al.connection_id
                FROM core.audit_logs as al
                left join core.measure as ms on cast(ms.id as TEXT) = cast(al.measure_id as TEXT)
                join core.users as users on cast(users.id as TEXT)=cast(al.created_by as TEXT)
                left join core.asset on asset.id = al.asset_id::uuid
                left join core.attribute on attribute.id = al.attribute_id::uuid
                {filter_query}
                order by al.created_date
            """
            cursor = execute_query(connection, cursor, query_string)
            user_activity_list = fetchall(cursor)
            user_activity_list = user_activity_list if user_activity_list else []

            for activity in user_activity_list:
                user_activity_metadata.append({
                    "USER_ID": activity.get("user_id"),
                    "USERNAME": activity.get("username"),
                    "USERROLE": activity.get("user_role"),
                    "ATTRIBUTR_ID": activity.get("attribute_id"),
                    "CREATED_DATE": str(activity.get("created_date")),
                    "PAGE": activity.get("page"),
                    "MODULE": activity.get("module"),
                    "SUBMODULE": activity.get("submodule"),
                    "ASSETS": activity.get("asset_name"),
                    "CONNECTION_ID": activity.get("connection_id"),
                    "ATTRIBUTE": activity.get("attribute_name"),
                    "NOTIFICATION_TEXT": activity.get("message")
                })
                max_dates.append(activity.get("created_date"))

    except Exception as e:
        log_error("Get user activity", e)
    finally:
        max_date = max(max_dates) if max_dates else None
        insert_data = {
            "status": ScheduleStatus.Completed.value,
            "start_time": max_date,
            "id": activity_insert_id.get('id') if activity_insert_id else None
        }
        save_user_activity_log(config, insert_data)
        return user_activity_metadata
    