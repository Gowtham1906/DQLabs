import json
import time
import re
import ast
import requests
import base64
import os
from datetime import datetime


from dqlabs.app_helper.s3select_helper import (
    map_s3_attr_index,
    map_s3_json_attribute,
    parse_s3_query_response,
)

from airflow.utils.module_loading import import_string
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
)
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.date_formats import DATE_FORMATS
from dqlabs.app_constants.dq_constants import (
    AIRFLOW_DEPENDS_ON_PAST,
    AIRFLOW_EMAIL,
    AIRFLOW_EMAIL_ON_FAILURE,
    AIRFLOW_EMAIL_ON_RETRY,
    AIRFLOW_OWNER,
    AIRFLOW_RETRIES,
    APP_NAME,
    DIRECT_QUERY_BASE_TABLE_LABEL,
    HEALTH,
    STATISTICS,
    SPARK,
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.app_helper.db_helper import (
    fetchone,
    fetchall,
    execute_query,
)
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.crypto_helper import decrypt, encrypt

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

default_args: dict = {
    "owner": AIRFLOW_OWNER,
    "email": AIRFLOW_EMAIL,
    "email_on_failure": AIRFLOW_EMAIL_ON_FAILURE,
    "email_on_retry": AIRFLOW_EMAIL_ON_RETRY,
    "retries": AIRFLOW_RETRIES,
    "depends_on_past": AIRFLOW_DEPENDS_ON_PAST,
}

# S3 JSON TYPES - To determine various json structure types
DICT_TYPE = "dict_type"
LIST_TYPE = "list_type"
LINES_TYPE = "lines_type"
UNKNOWN = "unknown"


def get_hook_object(connection_type: str, connection_id: str, config: dict = {}):
    """
    Returns the airflow connection provider object at runtime
    for the given connection type and connection id.
    """
    provider: str = ""
    provider_args: dict = {}
    credentials: dict = {}
    if not connection_id:
        raise Exception("Invalid connection identifier")

    if config:
        connection = config.get("connection")
        connection = connection if connection else {}
        credentials = connection.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}


    if connection_type == ConnectionType.Postgres.value:
        provider = "airflow.providers.postgres.hooks.postgres.PostgresHook"
        provider_args = {"postgres_conn_id": connection_id}
    hook_object = None
    if provider and provider_args:
        hook_object = import_string(provider)
        hook_object = hook_object(**provider_args)
    return hook_object


def prepare_query_string(
    query: str,
    schema_name: str = "",
    table_name: str = "",
    attribute: str = "",
    database_name: str = "",
    schema_condition: str = "",
    location: str = "",
    tag_name: str = "",
) -> str:
    """
    Prepares the query string by adding a table and attibute names
    """
    if schema_name:
        if schema_condition:
            schema_condition = schema_condition.replace("<schema_name>", schema_name)
            query = query.replace("<schema_condition>", schema_condition)
        query = query.replace("<schema_name>", schema_name.strip())
        query = query.replace("<schema>", schema_name.strip())
    if table_name:
        query = query.replace("<table_name>", table_name.strip())
    if attribute:
        query = query.replace("<attribute>", attribute.strip())
    if database_name:
        query = query.replace("<database_name>", database_name.strip())
    # for bigquery
    if location:
        query = query.replace("<location>", location.strip())
    if tag_name:
        query = query.replace("<tag_name>", tag_name.strip())

    return query


def prepare_nosql_pipeline(
    pipeline: str,
    collection_name: str = "",
    attribute: str = "",
    database_name: str = "",
    attributes: list = None,
    filter_condition: dict = None,
) -> str:
    
    if not pipeline or pipeline == "":
        return pipeline
    
    if isinstance(pipeline, (list, dict)):
        pipeline = json.dumps(pipeline)
    
    if collection_name:
        pipeline = pipeline.replace("<collection_name>", collection_name.strip())
    
    if database_name:
        pipeline = pipeline.replace("<database_name>", database_name.strip())
    
    if attribute:
        attribute = attribute.strip()
        pipeline = pipeline.replace('"$<attribute>"', f'"${attribute}"')
        pipeline = pipeline.replace('"<attribute>"', f'"{attribute}"')
        pipeline = pipeline.replace('<attribute>', attribute)
    
    if attributes and isinstance(attributes, list):
        if len(attributes) > 1:
            composite_key = {attr: f"${attr}" for attr in attributes}
            composite_key_str = json.dumps(composite_key)
            pipeline = pipeline.replace('"<attribute>"', composite_key_str)
        elif len(attributes) == 1:
            attribute = attributes[0].strip()
            pipeline = pipeline.replace('"$<attribute>"', f'"${attribute}"')
            pipeline = pipeline.replace('"<attribute>"', f'"{attribute}"')
            pipeline = pipeline.replace('<attribute>', attribute)
    
    if filter_condition and isinstance(filter_condition, dict):
        try:
            pipeline_list = json.loads(pipeline)
            if isinstance(pipeline_list, list):
                match_stage = {"$match": filter_condition}
                pipeline_list.insert(0, match_stage)
                pipeline = json.dumps(pipeline_list)
        except json.JSONDecodeError:
            pass
    
    return pipeline


def get_postgres_connection(config: dict):
    """
    Returns default postgres connection object
    """
    postgres_connection = config.get("postgres_connection")
    if postgres_connection:
        return postgres_connection

    postgres_connection_id = config.get("core_connection_id")
    postgres_hook_object = get_hook_object(
        ConnectionType.Postgres.value, postgres_connection_id
    )
    postgres_connection = postgres_hook_object.get_conn()
    config.update({"postgres_connection": postgres_connection})
    return postgres_connection


def execute_native_query(
    config: dict,
    query: str,
    connection_object: object = None,
    force_commit: bool = False,
    retry_count: int = 0,
    is_list: bool = False,
    is_columns_only: bool = False,
    is_count_only: bool = False,
    no_response: bool = False,
    convert_lower: bool = True,
    limit: int = 0,
    method_name: str = "",
    parameters: dict = {},
    **kwargs,
):
    try:
        connection = None
        response = None
        connection_type = config.get("connection_type")
        if connection_type == ConnectionType.MongoDB.value:
            print("Mongo query", query)
        is_agent_enabled_connector = agent_helper.is_agent_enabled(connection_type)
        if is_agent_enabled_connector:
            task_category = config.get("category")
            pg_connection = get_postgres_connection(config)
            credentials = config.get("connection", {}).get("credentials")
            credentials = credentials if credentials else {}
            auth_type = credentials.get("authentication_type", "")
            if (
                auth_type.lower() == "oauth"
                and connection_type == ConnectionType.Snowflake.value
            ):
                credentials = refresh_oauth_token(config, credentials)
                config.get("connection").update({"credentials": credentials})
            is_windows_auth = (
                auth_type and auth_type.lower() == "windows authentication"
            )
            include_connection_id = bool(
                connection_type == ConnectionType.Snowflake.value
            )
            if (
                connection_type == ConnectionType.MSSQL.value
                and query
                and " as direct_query_table" in query
                and "with(nolock)" in query
            ):
                query = query.replace("with(nolock)", "")

            # Get Asset Properties
            asset = config.get("asset")
            asset = asset if asset else {}
            asset_properties = (
                asset.get("properties") if asset.get("properties") else {}
            )
            asset_properties = asset_properties if asset_properties else {}

            if connection_type.lower() == "mongo":
                database_name = config.get("schema")
                
                if isinstance(database_name, list):
                    database_name = database_name[0] if database_name else ""
                
                collection_name = asset.get("technical_name") or asset.get("name") or ""
                
                database_name = database_name if database_name else ""
                collection_name = collection_name if collection_name else ""
                
                if not parameters:
                    parameters = {}
                parameters.update({
                    "database": database_name,
                    "collection": collection_name,
                })
            
            if connection_type.lower() == "mongo" and (not query or query.strip() == ""):
                return [] if is_list else {}, connection
            
            if not parameters and connection_type == ConnectionType.S3Select.value:
                is_header = (
                    asset_properties.get("is_header")
                    if asset_properties.get("is_header")
                    else asset.get("is_header")
                )

                connection = config.get("connection")
                connection = connection if connection else {}
                credentials = connection.get("credentials")
                credentials = credentials if credentials else {}
                s3_bucket_name = credentials.get("bucket")
                s3_bucket_name = s3_bucket_name.strip()

                s3_folder_path = credentials.get("folder")
                s3_folder_path = s3_folder_path if s3_folder_path else ""
                s3_file_name = asset.get("name")
                s3_file_name = s3_file_name if s3_file_name else ""
                s3_key = (
                    f"{s3_folder_path}/{s3_file_name}"
                    if s3_folder_path
                    else s3_file_name
                )
                parameters = dict(
                    file_name=s3_file_name,
                    key=s3_key,
                    is_header=is_header,
                )

            connection_type = config.get("connection_type")
            if connection_type == ConnectionType.MongoDB.value and query:
                query = fix_mongodb_query_before_execution(query)
            
            # Prepare Agent Config
            query, method_name, parameters = prepare_agent_config(
                config, query, method_name, parameters
            )

            response = agent_helper.execute_query(
                config,
                pg_connection,
                query,
                force_commit,
                is_list,
                is_columns_only,
                is_count_only,
                no_response,
                convert_lower,
                limit,
                include_connection_id,
                method_name=method_name,
                parameters=parameters,
                **kwargs,
            )
            if query and connection_type == ConnectionType.S3Select.value:
                s3columns = get_s3select_attributes(config)
                response = parse_s3_query_response(response, config, query, s3columns)
            return response, connection

    except Exception as error:
        if retry_count < 1 and connection_type == ConnectionType.Denodo.value:
            error = None
            connection_object = None
            connection = None
            time.sleep(30)
            retry_count = retry_count + 1
            return execute_native_query(
                config,
                query,
                connection_object,
                force_commit,
                retry_count,
                is_list,
                is_columns_only,
                is_count_only,
                no_response,
                convert_lower,
                limit,
            )

        if error:
            log_error(f"Executed Query - {retry_count}", query)
            log_error("Execute Native Query", error)
            if connection and hasattr(connection, "rollback") and connection.rollback:
                connection.rollback()
            # For certain columns we are facing 'The multi-part identifier "m2.MemberID" could not be bound' error in salesforce marketing cloud so adding this condition untill we find the reason .
            if "The multi-part identifier" in str(error):
                return response, connection
            raise error
        if "The multi-part identifier" in str(error):
            return response, connection
        raise error


def get_native_connection(
    config: dict, connection_object: object = None, retry_count: int = 0
):
    try:
        connection_type = config.get("connection_type")
        is_agent_enabled_connector = agent_helper.is_agent_enabled(connection_type)
        if is_agent_enabled_connector:
            return None

    except Exception as error:
        if retry_count < 1 and connection_type == ConnectionType.Denodo.value:
            error = None
            connection_object = None
            connection = None
            time.sleep(30)
            retry_count = retry_count + 1
            return get_native_connection(config, connection_object, retry_count)
        if error:
            raise error


def get_watermark_datatype(
    config: dict,
    watermark_query: str,
    watermark: str,
    table_name: str,
    is_custom_fingerprint: bool,
    custom_fingerprint_date: str,
) -> str:
    source_connection = ""
    date_datatype_filter = ""
    watermark_datatype = config.get("watermark_datatype")
    watermark_datatype = watermark_datatype if watermark_datatype else ""
    schema = config.get("schema")
    database = config.get("database_name")
    connection_type = config.get("connection_type", "")
    watermark_query = (
        watermark_query.replace("<table_name>", table_name)
        .replace("<schema_name>", schema)
        .replace("<watermark_column>", watermark)
        .replace("<database_name>", database)
    )

    if not watermark_datatype:
        try:
            if (
                connection_type == ConnectionType.Databricks.value
                and config.get("database_name") == "hive_metastore"
            ):
                result, _ = execute_native_query(
                    config, watermark_query, source_connection, is_list=True
                )
                watermark_datatype = next(
                    (
                        {"column": col["col_name"], "datatype": col["data_type"]}
                        for col in result
                        if col["col_name"] == watermark
                    ),
                    None,
                )
            elif connection_type == ConnectionType.Hive.value:
                result, _ = execute_native_query(
                    config, watermark_query, source_connection, is_list=True
                )
                if result and isinstance(result, list) and len(result) > 0:
                    watermark_datatype = result[0].get('data_type', '').lower()
                    watermark_datatype = {"datatype": watermark_datatype} if watermark_datatype else None
                else:
                    watermark_datatype = None
            elif connection_type in [ConnectionType.ADLS.value, ConnectionType.S3.value]:
                watermark_datatype = ""
            else:
                watermark_datatype, _ = execute_native_query(
                    config, watermark_query, source_connection
                )
            watermark_datatype = convert_to_lower(watermark_datatype)
            watermark_datatype = (
                watermark_datatype.get("datatype") if watermark_datatype else ""
            )
            watermark_datatype = watermark_datatype if watermark_datatype else ""
            if watermark_datatype:
                config.update({"watermark_datatype": watermark_datatype})
        except Exception as e:
            log_error(str(e), e)

    if connection_type in [
        ConnectionType.Snowflake.value,
        ConnectionType.Databricks.value,
        ConnectionType.Hive.value,
    ]:
        date_datatype_filter = (
            f"current_date()"
            if (
                (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10)
                or (watermark_datatype and watermark_datatype.lower() == "date")
            )
            else f"current_timestamp()"
        )
    elif connection_type in [
        ConnectionType.SapHana.value,
        ConnectionType.Athena.value,
        ConnectionType.Teradata.value,
        ConnectionType.AlloyDB.value,
        ConnectionType.Postgres.value,
        ConnectionType.SapEcc.value,
    ]:
        date_datatype_filter = (
            f"CURRENT_DATE"
            if (
                (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10)
                or (watermark_datatype and watermark_datatype.lower() == "date")
            )
            else f"CURRENT_TIMESTAMP"
        )
    elif connection_type in [ConnectionType.Oracle.value]:
        date_datatype_filter = (
            f"TRUNC(CURRENT_DATE)"
            if (
                (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10)
                or (watermark_datatype and watermark_datatype.lower() == "date")
            )
            else f"CURRENT_TIMESTAMP"
        )
    elif connection_type in [
        ConnectionType.MSSQL.value,
        ConnectionType.Redshift.value,
        ConnectionType.Redshift_Spectrum.value,
    ]:
        date_datatype_filter = (
            f"cast(getdate() as date)) as datetime)"
            if (
                (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10)
                or (watermark_datatype and watermark_datatype.lower() == "date")
            )
            else f"cast(getdate() as datetime)) as datetime)"
        )
    elif connection_type in [ConnectionType.MySql.value]:
        date_datatype_filter = (
            f"CAST(CURDATE() AS DATE) AS datetime"
            if (
                (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10)
                or (watermark_datatype and watermark_datatype.lower() == "date")
            )
            else f"CAST(NOW() AS DATETIME) AS datetime"
        )
    elif connection_type == ConnectionType.BigQuery.value:
        if watermark_datatype == "timestamp":
            date_datatype_filter = (
                f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL <interval> DAY)"
            )
        elif (is_custom_fingerprint and len(str(custom_fingerprint_date)) <= 10) or (
            watermark_datatype == "date"
        ):
            date_datatype_filter = f"DATE_SUB(CURRENT_DATE(), INTERVAL <interval> DAY)"
        else:
            date_datatype_filter = (
                f"DATETIME_SUB(CURRENT_DATETIME(), INTERVAL <interval> DAY)"
            )
    return date_datatype_filter


def get_fingerprint_format(connection_type: str, date_format: str):
    """
    Returns the target format and its length for a given connection type and format.

    Args:
        connection_type (str): The type of the database connection.
        format (str): The format string to be converted.

    Returns:
        tuple: A tuple containing the target format string and its length.
    """
    target_format = date_format if date_format else ""
    target_length = len(str(target_format))
    if connection_type in DATE_FORMATS:
        date_formats = DATE_FORMATS.get(connection_type)
        if date_format in date_formats:
            target_format = date_formats.get(date_format)

    if connection_type in [
        ConnectionType.Athena.value,
        ConnectionType.Oracle.value,
    ]:
        custom_date_format = date_format if date_format else ""
        if custom_date_format.lower() == "yyyy-mm-dd hh24:mi:ss":
            target_format = "yyyy-mm-dd hh:mi:ss"
            target_length = f"LENGTH('{target_format}')"
            target_format = custom_date_format.lower()
        else:
            target_length = f"LENGTH('{custom_date_format}')"
    target_format = target_format if target_format else date_format
    return target_format, str(target_length)


def format_fingerprint_attribute(
    connection_type: str,
    custom_fingerprint_format: str,
    fingerprint_column: str,
    fingerprint_attribute_query: str,
    is_custom_fingerprint: bool,
    custom_fingerprint_column_type: str,
):
    """
    Formats the fingerprint query based on the connection type and custom fingerprint format.

    Args:
        connection_type (str): The type of database connection (e.g., MSSql, Postgres).
        custom_fingerprint_format (str): The custom format for the fingerprint (e.g., "DDMMYYYY").
        fingerprint_column (str): The name of the fingerprint column.
        fingerprint_attribute_query (str): The query string containing the fingerprint attribute placeholder.

    Returns:
        str: The formatted fingerprint query with the appropriate transformations applied.
    """
    if (
        connection_type == ConnectionType.MSSQL.value
        and custom_fingerprint_format == "DDMMYYYY"
    ):
        fingerprint_attribute_query = fingerprint_attribute_query.replace(
            f"[<watermark_attribute>]",
            f"STUFF(STUFF([<watermark_attribute>], 3, 0, '-'), 6, 0, '-')",
        )
    elif (
        connection_type == ConnectionType.Postgres.value
        and custom_fingerprint_format == "DDMMYYYY"
    ):
        fingerprint_attribute_query = fingerprint_attribute_query.replace(
            f'"<watermark_attribute>"',
            f"""CASE WHEN CAST("<watermark_attribute>" AS TEXT) ~ '^\d{{8}}$' THEN """
            f"""SUBSTRING(CAST("<watermark_attribute>" AS TEXT) FROM 1 FOR 2) || '-' || """
            f"""SUBSTRING(CAST("<watermark_attribute>" AS TEXT) FROM 3 FOR 2) || '-' || """
            f"""SUBSTRING(CAST("<watermark_attribute>" AS TEXT) FROM 5 FOR 4) """
            f"""ELSE NULL END""",
        )
    column = str(fingerprint_column) if fingerprint_column else ""
    fingerprint_attribute_query = fingerprint_attribute_query.replace(
        "<watermark_attribute>", column
    )
    cast_datetype_format = ""
    if is_custom_fingerprint and custom_fingerprint_format:
        cast_datetype_format = (
            f"TO_DATE" if len(custom_fingerprint_format) <= 10 else f"TO_TIMESTAMP"
        )
        if connection_type == ConnectionType.MSSQL.value:
            cast_datetype_format = (
                f"DATE" if len(custom_fingerprint_format) <= 10 else f"DATETIME2"
            )
        elif connection_type == ConnectionType.Snowflake.value:
            cast_datetype_format = (
                f"TRY_TO_DATE"
                if len(custom_fingerprint_format) <= 10
                else f"TRY_TO_TIMESTAMP"
            )
        elif connection_type == ConnectionType.Teradata.value and (
            custom_fingerprint_column_type
            and custom_fingerprint_column_type.lower() in ["integer", "number"]
        ):
            cast_datetype_format = (
                f"DATE" if len(custom_fingerprint_format) <= 10 else f"TIMESTAMP"
            )
        elif connection_type == ConnectionType.Hive.value:
            cast_datetype_format = (
                f"DATE" if len(custom_fingerprint_format) <= 10 else f"TIMESTAMP"
            )
        elif connection_type in [ConnectionType.ADLS.value, ConnectionType.S3.value]:
            cast_datetype_format = custom_fingerprint_format.lower()
            if cast_datetype_format == "yyyy-mm-dd hh:mi:ss":
                cast_datetype_format = "yyyy-MM-dd HH:mm:ss"
        elif connection_type == ConnectionType.BigQuery.value:
            cast_datetype_format = (
                f"PARSE_DATE" if len(custom_fingerprint_format) <= 10 else f"PARSE_TIMESTAMP"
            )
    fingerprint_attribute_query = fingerprint_attribute_query.replace(
        "<cast_datetype_format>", cast_datetype_format
    )
    return fingerprint_attribute_query


def get_asset_filter_query(filter_query: str, is_incremental: bool):
    """
    Constructs and returns different variations of asset filter queries based on the input parameters.

    Args:
        filter_query (str): The initial filter query string.
        is_incremental (bool): A flag indicating whether the query is incremental.

    Returns:
        tuple: A tuple containing three strings:
            - source_asset_filter_query (str): The modified source filter query without 'where' clause and appended with 'AND'.
            - asset_filter_query (str): The asset filter query with 'AND' if incremental and source filter exists.
            - validate_filter_query (str): The validation filter query with 'AND' if source filter exists.
    """

    source_filter = filter_query if filter_query else ""
    source_filter = source_filter if source_filter else ""
    join_condition = " AND " if is_incremental else " WHERE "
    asset_filter_query = f" {join_condition} {source_filter} " if source_filter else ""
    validate_filter_query = f" AND {source_filter}" if source_filter else ""
    source_asset_filter_query = f" {source_filter} AND " if source_filter else ""
    return source_asset_filter_query, asset_filter_query, validate_filter_query


def replace_placeholders(query_string, replacements, connection_type=None):
    """
    Replaces placeholders in a query string with corresponding values from a dictionary.
    """
    
    query_string = query_string if query_string else ""
    for placeholder, value in replacements.items():
        if placeholder in ["<date>", "<max_date_value>", "<date_datatype_filter>"]:
            if isinstance(value, datetime):
                if connection_type and connection_type.lower() == "mongo":
                    replace_value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    replace_value = str(value)
            elif value:
                replace_value = str(value)
            else:
                replace_value = ""
        else:
            replace_value = str(value) if value else ""
        query_string = query_string.replace(placeholder, replace_value)
    return query_string


def _remove_trailing_commas_before_closing_bracket(query_string: str) -> str:
    """
    Remove trailing commas before closing bracket in MongoDB queries.
    """
    if not query_string or not isinstance(query_string, str):
        return query_string
    
    query_string = re.sub(r',\s*\]', ']', query_string)
    
    return query_string


def fix_mongodb_query_before_execution(query_string):
    """
    Final safety check to fix malformed MongoDB queries right before execution.
    This catches any queries that slipped through previous cleanup steps.
    """
    
    if not query_string or not isinstance(query_string, str):
        return query_string
    
    # Check if query looks like MongoDB aggregation pipeline (starts with [)
    if not query_string.strip().startswith('['):
        return query_string
    
    try:
        def convert_tuple_in_match(match):
            full_match = match.group(0)
            tuple_str = match.group(2)  # Get the tuple part
            try:
                tuple_value = ast.literal_eval(tuple_str)
                if isinstance(tuple_value, tuple):
                    json_array = json.dumps(list(tuple_value))
                    return match.group(1) + json_array  # Replace tuple with JSON array
                elif isinstance(tuple_value, list):
                    json_array = json.dumps(tuple_value)
                    return match.group(1) + json_array
            except (ValueError, SyntaxError, TypeError):
                pass
            return full_match  # Return original if conversion fails
        
        query_string = re.sub(r'("(?:\$nin|\$in)"\s*:\s*)(\([^)]+\))', convert_tuple_in_match, query_string)
        
        if re.search(r'\]\s*\[', query_string):
            parts = re.split(r'\]\s*\[', query_string.strip())
            merged = []
            for i, part in enumerate(parts):
                if i == 0:
                    arr_str = part + ']'
                elif i == len(parts) - 1:
                    arr_str = '[' + part
                else:
                    arr_str = '[' + part + ']'
                try:
                    arr = json.loads(arr_str)
                    if isinstance(arr, list):
                        merged.extend(arr)
                    else:
                        merged.append(arr)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
            if merged:
                filtered = [item for item in merged if item not in (None, "", {}) and isinstance(item, dict)]
                if filtered:
                    query_string = json.dumps(filtered, separators=(',', ':'))
                    return query_string
        
        parsed = json.loads(query_string.strip())
        if isinstance(parsed, list):
            # Filter out invalid elements and ensure all are objects
            filtered = []
            for item in parsed:
                if item not in (None, "", {}) and isinstance(item, dict):
                    filtered.append(item)
            if filtered:
                query_string = json.dumps(filtered, separators=(',', ':'))
            else:
                raise ValueError("No valid pipeline stages found")
    except (json.JSONDecodeError, TypeError, ValueError):
        try:
            parts = re.split(r'\]\s*\[', query_string.strip())
            if len(parts) > 1:
                # Reconstruct and parse each array, then merge
                merged = []
                for i, part in enumerate(parts):
                    if i == 0:
                        arr_str = part + ']'
                    elif i == len(parts) - 1:
                        arr_str = '[' + part
                    else:
                        arr_str = '[' + part + ']'
                    try:
                        arr = json.loads(arr_str)
                        if isinstance(arr, list):
                            merged.extend(arr)
                        else:
                            merged.append(arr)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                if merged:
                    # Filter out invalid elements and ensure all are objects
                    filtered = [item for item in merged if item not in (None, "", {}) and isinstance(item, dict)]
                    if filtered:
                        query_string = json.dumps(filtered, separators=(',', ':'))
                        return query_string
        except Exception:
            pass
        
        try:
            json_objects = []
            pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
            matches = re.finditer(pattern, query_string)
            for match in matches:
                try:
                    obj = json.loads(match.group(0))
                    if isinstance(obj, dict) and any(key.startswith('$') for key in obj.keys()):
                        json_objects.append(obj)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
            
            if json_objects:
                query_string = json.dumps(json_objects, separators=(',', ':'))
                return query_string
        except Exception:
            pass
    
    return query_string


def get_incremental_query_string(
    config: dict, default_queries: dict, query_string: str, category: str = "", **kwargs
) -> str:
    """
    Prepares an incremental query by modifying the given query string based on the asset's incremental configuration.

    Args:
        query_string (str): The base query string to be modified.
        connector: The connector object that provides connection details and queries.
        asset: The asset object containing data asset details and configurations.

    Returns:
        str: The modified query string with incremental conditions and filters applied.
    """
    connection_type = config.get("connection_type")
    is_mongodb = connection_type == ConnectionType.MongoDB.value
    
    kwargs = kwargs if kwargs else {}
    asset = config.get("asset", {})
    asset = asset if asset else {}
    connection = config.get("connection", {})
    connection = connection if connection else {}
    measure = config.get("measure", {})
    measure_type = measure.get("category")
    lookup_alias = "t1." if (measure_type == "lookup" or category == "lookup") else ""
    watermark = config.get("watermark")
    is_custom_fingerprint = asset.get("is_custom_fingerprint")

    qualified_table_name = config.get("table_name")
    is_incremental = config.get("is_incremental", False)
    date_datatype_filter = ""

    has_temp_table = config.get("has_temp_table")
    if has_temp_table:
        qualified_table_name = config.get("temp_view_table_name")
    is_filtered = asset.get("is_filtered", False)
    filter_query = asset.get("filter_query", None)
    filter_query = filter_query if is_filtered and filter_query else ""
    if is_mongodb:
        if filter_query:
            try:
                filter_pipeline = json.loads(filter_query) if isinstance(filter_query, str) else filter_query
                
                if isinstance(filter_pipeline, list) and len(filter_pipeline) > 0:
                    asset_filter_match = None
                    for stage in filter_pipeline:
                        if isinstance(stage, dict) and "$match" in stage:
                            asset_filter_match = stage["$match"]
                            break
                    
                    if asset_filter_match:
                        asset_filter_pipeline = json.dumps([{"$match": asset_filter_match}])
                        source_asset_filter_pipeline = asset_filter_pipeline
                        
                        asset_filter_query = json.dumps({"$match": asset_filter_match})
                        source_asset_filter_query = asset_filter_query
                        validate_filter_query = ""
                    else:
                        asset_filter_query = ""
                        source_asset_filter_query = ""
                        validate_filter_query = ""
                else:
                    asset_filter_query = ""
                    source_asset_filter_query = ""
                    validate_filter_query = ""
            except (json.JSONDecodeError, TypeError, AttributeError):
                asset_filter_query = ""
                source_asset_filter_query = ""
                validate_filter_query = ""
        else:
            asset_filter_query = ""
            source_asset_filter_query = ""
            validate_filter_query = ""
    else:
        source_asset_filter_query, asset_filter_query, validate_filter_query = (
            get_asset_filter_query(filter_query, is_incremental)
        )

    custom_fingerprint_column = None
    custom_fingerprint_format = None
    custom_fingerprint_column_type = None
    custom_finger_print_default_value = None
    if is_custom_fingerprint:
        custom_fingerprint = config.get("custom_fingerprint")
        custom_fingerprint = (
            json.loads(custom_fingerprint)
            if custom_fingerprint and isinstance(custom_fingerprint, str)
            else custom_fingerprint
        )
        custom_fingerprint = custom_fingerprint if custom_fingerprint else {}
        custom_fingerprint_column = custom_fingerprint.get("value")
        custom_fingerprint_format = custom_fingerprint.get("format")
        custom_fingerprint_column_type = custom_fingerprint.get("derived_type", "")
        custom_finger_print_default_value = custom_fingerprint.get("default_value")
        custom_fingerprint_column = (
            custom_fingerprint_column if custom_fingerprint_column else None
        )
        custom_fingerprint_format = (
            custom_fingerprint_format if custom_fingerprint_format else None
        )

    fingerprint_column = (
        custom_fingerprint_column
        if is_custom_fingerprint
        and custom_fingerprint_column
        and custom_fingerprint_format
        else watermark
    )
    fingerprint_column = fingerprint_column if fingerprint_column else ""
    table_name = config.get("technical_name")
    table_name = table_name if table_name else ""
    schema_name = config.get("schema")
    schema_name = schema_name if schema_name else ""
    database = config.get("database_name")
    database = database if database else ""
    incremental_queries = default_queries.get("incremental", {})
    primary_fingerprint = incremental_queries.get("primary_fingerprint", "")
    secondary_fingerprint = incremental_queries.get("secondary_fingerprint", "")
    integer_secondary_fingerprint = incremental_queries.get(
        "integer_secondary_fingerprint", ""
    )
    secondary_filter = incremental_queries.get("secondary_filter", "")
    incremental_query = incremental_queries.get("filter", "")
    timestamp_interval = incremental_queries.get("timestamp_interval", "")
    date_interval = incremental_queries.get("date_interval", "")
    watermark_datatype_query = incremental_queries.get("watermark_datatype", "")

    is_primary_filter = not is_custom_fingerprint or (
        is_custom_fingerprint and not custom_fingerprint_format
    )
    fingerprint_attribute_query = (
        primary_fingerprint if is_primary_filter else secondary_fingerprint
    )
    if connection_type == ConnectionType.Teradata.value:
        interval_query = (
            date_interval
            if custom_fingerprint_format and len(custom_fingerprint_format) <= 10
            else timestamp_interval
        )
        incremental_query = incremental_query.replace(
            "<interval_query>", interval_query
        )
    if connection_type == ConnectionType.Teradata.value and (
        custom_fingerprint_column_type
        and custom_fingerprint_column_type.lower() in ["integer", "number"]
    ):
        fingerprint_attribute_query = integer_secondary_fingerprint
    if (
        connection_type == ConnectionType.SapHana.value
        and custom_fingerprint_format
        and custom_fingerprint_format.lower() == "yyyymmdd"
        and custom_finger_print_default_value == "00000000"
    ):
        fingerprint_attribute_query = primary_fingerprint
    fingerprint_format, fingerprint_length = get_fingerprint_format(
        connection_type, custom_fingerprint_format
    )

    fingerprint_attribute_query = format_fingerprint_attribute(
        connection_type,
        custom_fingerprint_format,
        fingerprint_column,
        fingerprint_attribute_query,
        is_custom_fingerprint,
        custom_fingerprint_column_type,
    )

    secondary_filter = secondary_filter.replace(
        "<secondary_fingerprint>", fingerprint_attribute_query
    )
    watermark_column = primary_fingerprint if is_primary_filter else secondary_filter
    if connection_type == ConnectionType.BigQuery.value:
        cast_to_timestamp_value = ""
        if is_custom_fingerprint and custom_fingerprint_format:
            cast_to_timestamp_value = (
                "" if len(custom_fingerprint_format) <= 10 else "TIMESTAMP"
            )
        incremental_query = incremental_query.replace(
            "<cast_to_timestamp>", cast_to_timestamp_value
        )
    incremental_query = incremental_query.replace(
        "<watermark_column>", watermark_column
    )

    # incremental_behavioral_query = incremental_queries.get("behavioral_days", "")
    incremental_watermark_datatype_query = incremental_queries.get(
        "watermark_datatype", ""
    )
    if is_incremental and (
        incremental_watermark_datatype_query
        or connection_type == ConnectionType.Teradata.value
    ) and (connection_type not in [ConnectionType.ADLS.value, ConnectionType.S3.value]):
        date_datatype_filter = get_watermark_datatype(
            config,
            watermark_datatype_query,
            watermark,
            table_name,
            is_custom_fingerprint,
            custom_fingerprint_format,
        )

    replacement_placeholders = {
        "<secondary_filter>": secondary_filter,
        "<secondary_fingerprint>": fingerprint_attribute_query,
        "<incremental_filter>": "",
        "<incremental_condition>": "",
        "<database_name>": database,
        "<schema_name>": schema_name,
        "<table_name>": qualified_table_name,
        "<watermark_attribute>": fingerprint_column,
        "<attribute>": fingerprint_column,
        "<date_datatype_filter>": date_datatype_filter,
        "<date_format_length>": str(fingerprint_length),
        "<date_format>": str(fingerprint_format),
        "<validate_filter_query>": validate_filter_query,
        "<source_asset_filter_condition>": source_asset_filter_query,
        "<asset_filter_condition>": asset_filter_query,
        "<lookup_alias>": lookup_alias,
    }
    
    incremental_stages_str = ""
    asset_filter_stages_str = ""
    incremental_query_string = ""  # Initialize for non-incremental queries
    
    if is_mongodb:
        if incremental_query_string and incremental_query_string.strip():
            try:
                incremental_parsed = json.loads(incremental_query_string.strip())
                if isinstance(incremental_parsed, list) and len(incremental_parsed) > 0:
                    incremental_stages_str = json.dumps(incremental_parsed)[1:-1]
                elif isinstance(incremental_parsed, dict):
                    incremental_stages_str = incremental_query_string
                else:
                    incremental_stages_str = incremental_query_string
            except (json.JSONDecodeError, TypeError, AttributeError):
                incremental_stages_str = incremental_query_string
        
        if asset_filter_query and asset_filter_query.strip():
            try:
                asset_filter_parsed = json.loads(asset_filter_query.strip()) if isinstance(asset_filter_query, str) else asset_filter_query
                if isinstance(asset_filter_parsed, list) and len(asset_filter_parsed) > 0:
                    first_stage = asset_filter_parsed[0]
                    if isinstance(first_stage, dict) and "$match" in first_stage:
                        asset_filter_stages_str = json.dumps(first_stage)
                    else:
                        asset_filter_stages_str = json.dumps(asset_filter_parsed)[1:-1]
                elif isinstance(asset_filter_parsed, dict):
                    asset_filter_stages_str = json.dumps(asset_filter_parsed)
                else:
                    asset_filter_stages_str = asset_filter_query if isinstance(asset_filter_query, str) else json.dumps(asset_filter_query)
            except (json.JSONDecodeError, TypeError, AttributeError):
                asset_filter_stages_str = asset_filter_query if isinstance(asset_filter_query, str) else (json.dumps(asset_filter_query) if asset_filter_query else "")
        
        if asset_filter_stages_str:
            asset_filter_stages_str_stripped = asset_filter_stages_str.strip()
            if asset_filter_stages_str_stripped.startswith('[') and asset_filter_stages_str_stripped.endswith(']'):
                try:
                    asset_filter_parsed_final = json.loads(asset_filter_stages_str_stripped)
                    if isinstance(asset_filter_parsed_final, list) and len(asset_filter_parsed_final) > 0:
                        first_stage = asset_filter_parsed_final[0]
                        if isinstance(first_stage, dict) and "$match" in first_stage:
                            asset_filter_stages_str = json.dumps(first_stage)
                        else:
                            asset_filter_stages_str = json.dumps(asset_filter_parsed_final)[1:-1]
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
        
        asset_filter_replacement = ""
        if asset_filter_stages_str:
            asset_filter_replacement = asset_filter_stages_str + ","
        elif asset_filter_query and asset_filter_query.strip():
            asset_filter_replacement = asset_filter_query + ","
        
        replacement_placeholders.update(
            {
                "<incremental_condition>": (incremental_stages_str + ",") if incremental_stages_str else "",
                "<incremental_filter>": (incremental_stages_str + ",") if incremental_stages_str else "",
                "<asset_filter_condition>": asset_filter_replacement,
                "<source_asset_filter_condition>": asset_filter_replacement,
            }
        )
    
    if not is_incremental:
        query_string = replace_placeholders(query_string, replacement_placeholders, connection_type)
        if "<incremental_filter>" in query_string:
            query_string = query_string.replace("<incremental_filter>", "")
        return query_string

    incremental_config = config.get("incremental_config", {})
    incremental_config = (
        json.loads(incremental_config)
        if incremental_config and isinstance(incremental_config, str)
        else incremental_config
    )
    incremental_config = incremental_config if incremental_config else {}

    depth = incremental_config.get("depth", {})
    depth = depth if depth else {}
    depth_value = depth.get("value", 0)
    depth_value = int(depth_value) if depth_value else 0
    depth_value = depth_value if depth_value else 0

    replacement_placeholders.update({"<interval>": str(depth_value)})
    
    if is_mongodb:
        template_key = "filter" if is_primary_filter else "secondary_filter"
        incremental_query_template = incremental_queries.get(template_key, "")
        
        if incremental_query_template:
            incremental_query_string = replace_placeholders(
                incremental_query_template, replacement_placeholders, connection_type
            )
            
            try:
                incremental_pipeline = json.loads(incremental_query_string)
                if isinstance(incremental_pipeline, list) and len(incremental_pipeline) > 0:
                    if len(incremental_pipeline) == 1:
                        incremental_content = json.dumps(incremental_pipeline[0])
                    else:
                        incremental_content = incremental_query_string
                    
                    incremental_query_string = incremental_content
                    incremental_filter_query = incremental_content
                else:
                    incremental_query_string = ""
                    incremental_filter_query = ""
            except json.JSONDecodeError:
                incremental_filter_query = incremental_query_string
        else:
            incremental_query_string = ""
            incremental_filter_query = ""
    else:
        incremental_query_string = replace_placeholders(
            incremental_query, replacement_placeholders, connection_type
        )
    incremental_filter_query = ""
    if incremental_query_string:
        query_string = re.sub("(<incremental_filter>)", "<incremental_filter>", query_string, flags=re.IGNORECASE)
        if measure_type == "query" or category == "query":
            pattern1 = r'[^<]+?\s+and\s+<incremental_filter>'
            pattern2 = r'[^<]+?\s+where\s+<incremental_filter>'
            pattern3 = r'[^<]+?\s+or\s+<incremental_filter>'
            if re.search(pattern1, query_string, re.IGNORECASE):
                incremental_filter_query = re.sub(
                "(where)", "", incremental_query_string, flags=re.IGNORECASE
                )
            elif re.search(pattern2, query_string, re.IGNORECASE):
                incremental_filter_query = re.sub(
                "(where)", "", incremental_query_string, flags=re.IGNORECASE
                )
            elif re.search(pattern3, query_string, re.IGNORECASE):
                incremental_filter_query = re.sub(
                "(where)", "", incremental_query_string, flags=re.IGNORECASE
                )
            elif re.search(r'where', query_string, re.IGNORECASE):
                incremental_filter_query = re.sub(
                "(where)", "and", incremental_query_string, flags=re.IGNORECASE
            )
            else:
                incremental_filter_query = incremental_query_string
            incremental_filter_query = (f" {incremental_filter_query} " if incremental_filter_query else "")
        else:
            incremental_filter_query = re.sub(
                "( where )", "", incremental_query_string, flags=re.IGNORECASE
            )
            incremental_filter_query = incremental_filter_query if incremental_filter_query else ""
            if incremental_filter_query and not incremental_filter_query.strip().lower().endswith("and") and not source_asset_filter_query.strip().lower().startswith("and"):
                incremental_filter_query = f" {incremental_filter_query} AND "

    if is_mongodb:
        if incremental_query_string and incremental_query_string.strip():
            try:
                incremental_parsed = json.loads(incremental_query_string.strip())
                if isinstance(incremental_parsed, list) and len(incremental_parsed) > 0:
                    incremental_stages_str = json.dumps(incremental_parsed)[1:-1]
                elif isinstance(incremental_parsed, dict):
                    incremental_stages_str = incremental_query_string
                else:
                    incremental_stages_str = incremental_query_string
            except (json.JSONDecodeError, TypeError, AttributeError):
                incremental_stages_str = incremental_query_string
        
        if asset_filter_query and asset_filter_query.strip() and not asset_filter_stages_str:
            try:
                asset_filter_parsed = json.loads(asset_filter_query.strip()) if isinstance(asset_filter_query, str) else asset_filter_query
                if isinstance(asset_filter_parsed, list) and len(asset_filter_parsed) > 0:
                    first_stage = asset_filter_parsed[0]
                    if isinstance(first_stage, dict) and "$match" in first_stage:
                        asset_filter_stages_str = json.dumps(first_stage)
                    else:
                        asset_filter_stages_str = json.dumps(asset_filter_parsed)[1:-1]
                elif isinstance(asset_filter_parsed, dict):
                    asset_filter_stages_str = json.dumps(asset_filter_parsed)
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
        
        asset_filter_replacement = ""
        if asset_filter_stages_str:
            asset_filter_replacement = asset_filter_stages_str + ","
        elif asset_filter_query and asset_filter_query.strip():
            asset_filter_replacement = asset_filter_query + ","
        
        replacement_placeholders.update(
            {
                "<incremental_condition>": (incremental_stages_str + ",") if incremental_stages_str else "",
                "<incremental_filter>": (incremental_stages_str + ",") if incremental_stages_str else "",
                "<asset_filter_condition>": asset_filter_replacement,
                "<source_asset_filter_condition>": asset_filter_replacement,
            }
        )
    else:
        replacement_placeholders.update(
            {
                "<incremental_condition>": incremental_query_string,
                "<incremental_filter>": incremental_filter_query,
            }
        )
    
    query_string = replace_placeholders(query_string, replacement_placeholders, connection_type)
    
    if is_mongodb:
        query_string = _remove_trailing_commas_before_closing_bracket(query_string)
        try:
            query_pipeline = json.loads(query_string.strip())
            if isinstance(query_pipeline, list):
                flattened_pipeline = []
                for stage in query_pipeline:
                    if isinstance(stage, list):
                        flattened_pipeline.extend([s for s in stage if s is not None and s != "" and s != {}])
                    elif stage is not None and stage != "" and stage != {}:
                        flattened_pipeline.append(stage)
                
                flattened_pipeline = [
                    stage for stage in flattened_pipeline 
                    if not (isinstance(stage, dict) and "$match" in stage and stage["$match"] == {})
                ]
                
                query_string = json.dumps(flattened_pipeline)
                
                try:
                    json.loads(query_string)
                except (json.JSONDecodeError, TypeError, ValueError):
                    try:
                        parsed = json.loads(query_string)
                        if isinstance(parsed, list):
                            filtered = [s for s in parsed if s is not None and s != "" and s != {}]
                            query_string = json.dumps(filtered)
                    except:
                        pass
        except (json.JSONDecodeError, TypeError, ValueError):
            try:
                query_pipeline = json.loads(query_string.strip())
                if isinstance(query_pipeline, list):
                    query_pipeline = [s for s in query_pipeline if s is not None and s != "" and s != {}]
                    query_string = json.dumps(query_pipeline)
            except Exception:
                pass
    
    if is_mongodb and is_incremental and incremental_query_string:
        has_incremental_placeholder = (
            "<incremental_condition>" in query_string or 
            "<incremental_filter>" in query_string
        )
        has_asset_filter_placeholder = (
            "<asset_filter_condition>" in query_string or
            "<source_asset_filter_condition>" in query_string
        )

    return query_string


def get_query_lookup_string(
    config: dict,
    default_queries: dict,
    query: str,
    total_records: int = 0,
    is_full_query: bool = False,
    category: str = "",
) -> str:
    """
    Prepares the query string for the given asset

    """
    has_temp_table = config.get("has_temp_table")
    asset = config.get("asset", {})
    connection_type = config.get("connection_type")
    table_name = config.get("table_name")

    asset = config.get("asset")
    asset = asset if asset else {}

    if has_temp_table:
        table_name = config.get("temp_view_table_name")

    if str(asset.get("view_type", "")).lower() == "direct query":
        table_name = f"""({asset.get("query")}) as direct_query_table"""
        if connection_type and str(connection_type).lower() in [
            ConnectionType.Oracle.value.lower(),
            ConnectionType.BigQuery.value.lower(),
        ]:
            table_name = f"""({asset.get("query")})"""

    query_string = default_queries.get("default_query", "")
    is_incremental = config.get("is_incremental", False)

    query_string = (
        query_string.replace("<query>", query).replace("<table_name>", table_name)
        if not is_full_query
        else query
    )
    query_string = get_incremental_query_string(
        config, default_queries, query_string, category=category
    )
    query_string = query_string.replace("<incremental_filter>", "").replace(
        "<source_asset_filter_condition>", ""
    )
    return query_string


def get_query_string(
    config: dict,
    default_queries: dict,
    query: str,
    total_records: int = 0,
    is_full_query: bool = False,
    skip_incremental_query: bool = False,
    attribute_name: str = "",
    query_type: str = "",
    attribute: dict = {},
) -> str:
    """
    Prepares the query string for the given asset

    """

    has_temp_table = config.get("has_temp_table")
    asset = config.get("asset", {})
    asset = asset if asset else {}
    connection_type = config.get("connection_type")
    
    if connection_type == ConnectionType.MongoDB.value:
        return get_incremental_query_string(config, default_queries, query)
    
    is_direct_query_asset = config.get("is_direct_query_asset")
    base_table_query = config.get("base_table_query")
    category = config.get("category")
    log_info(("category", category))
    # Custom Table Name for S3Select
    properties = asset.get("properties")
    properties = (
        json.loads(properties)
        if properties and isinstance(properties, str)
        else properties
    )
    properties = properties if properties else {}
    table_name = config.get("table_name","")
    if not table_name and ConnectionType.Salesforce.value in connection_type:
        table_name = asset.get("name","")
    if not table_name and ConnectionType.SalesforceMarketing.value in connection_type:
        table_name = asset.get("name","")
    if not table_name and ConnectionType.SalesforceDataCloud.value in connection_type:
        table_name = asset.get("name","")
    if connection_type == ConnectionType.S3Select.value:
        asset_name = asset.get("name")
        asset_name = asset_name if asset_name else ""
        if asset_name.endswith(".csv"):
            table_name = "s3object s"
        elif asset_name.endswith(".json"):
            table_name = "s3object s"

    asset = config.get("asset")
    asset = asset if asset else {}

    if has_temp_table:
        table_name = config.get("temp_view_table_name")

    if str(asset.get("view_type", "")).lower() == "direct query":
        table_name = f"""({asset.get("query")}) as direct_query_table"""
        if connection_type and str(connection_type).lower() in [
            ConnectionType.Oracle.value.lower(),
            ConnectionType.BigQuery.value.lower(),
        ]:
            table_name = f"""({asset.get("query")})"""
    query_string = default_queries.get("default_query", "")
    query_cte_string = default_queries.get(
        "default_cte_query", ""
    )  # cte queries for performance enhacements

    # Prepare
    if attribute_name:
        # Import here to avoid circular dependency
        from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query
        query_cte_string = prepare_json_attribute_flatten_query(config, attribute_name, query_cte_string, "cte")
        query_string = prepare_json_attribute_flatten_query(config, attribute_name, query_string, "wrap")

    if connection_type == ConnectionType.S3Select.value:
        # custom query string for s3select connector
        file_type = properties.get("file_type")
        if file_type == "csv":
            is_header = asset["properties"].get("is_header", None)
            if not is_header:
                query = map_s3select_attributes(config, query)
        elif file_type == "json":
            query = map_s3select_attributes(config, query)
        query_string = (
            query_string.replace("<query>", query).replace("<table_name>", table_name)
            if not is_full_query
            else query
        )
        if is_full_query:
            # only for s3select to replace tablename
            query_string = query_string.replace("<table_name>", table_name)
    else:
        if query_cte_string and (query_type in [HEALTH, STATISTICS]):
            query_string = (
                query_cte_string.replace("<query>", query)
                .replace("<table_name>", table_name)
                .replace("<attribute>", attribute_name)
                if not is_full_query
                else query_cte_string
            )
        else:
            query_string = (
                query_string.replace("<query>", query).replace(
                    "<table_name>", table_name
                )
                if not is_full_query
                else query
            )
        if connection_type == ConnectionType.Salesforce.value and '<table_name>' in query_string:
            query_string = query_string.replace("<table_name>", table_name)
        if connection_type == ConnectionType.SalesforceMarketing.value and '<table_name>' in query_string:
            query_string = query_string.replace("<table_name>", table_name)
        if connection_type == ConnectionType.SalesforceDataCloud.value and '<table_name>' in query_string:
            query_string = query_string.replace("<table_name>", table_name)
            
        if connection_type == ConnectionType.BigQuery.value:
            if f"{table_name} a" not in query_string:
                query_string = query_string.replace(table_name, f"{table_name} a")
        if category == "behavioral":
            return query_string
    query_string = get_incremental_query_string(config, default_queries, query_string)
    if query_string and is_direct_query_asset and base_table_query:
        query_string = query_string.replace(
            DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
        )
    return query_string




""" S3 Select Helper Functions and Packages """


def get_s3select_attributes(config: dict):
    """Get all the attributes for s3 select in the right index order"""
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    columns = []
    with connection.cursor() as cursor:
        query_string = f"""
            select name as column_name from core.attribute
            where asset_id = '{asset_id}'
            and status != 'Deprecated'
            order by created_date asc
        """
        cursor = execute_query(connection, cursor, query_string)
        columns = fetchall(cursor)

    return columns


def map_s3select_attributes(config: dict, query: str):
    """Map attributes to its index for no header files"""
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    columns = []
    with connection.cursor() as cursor:
        query_string = f"""
            select name as column_name, 
            '_' || cast(attribute_index AS varchar) AS attr_index
            from core.attribute 
            where asset_id = '{asset_id}'
            and status != 'Deprecated'
            order by attribute_index
        """
        cursor = execute_query(connection, cursor, query_string)
        columns = fetchall(cursor)

    asset = config.get("asset", {})
    properties = asset.get("properties")
    file_type = properties.get("file_type")
    if file_type == "csv":
        mapped_query = map_s3_attr_index(query, columns)
    elif file_type == "json":
        mapped_query = map_s3_json_attribute(query, columns)
    return mapped_query


def get_snowflake_connection(config: dict):
    """
    create snowflake connection
    """
    is_mutliple = False
    connection = None
    if config:
        connection_info = config.get("connection")
        connection_info = connection_info if connection_info else {}
        credentials = connection_info.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}
        if credentials:
            connection_asset = config.get("asset", {})
            connection_asset_properties = (
                connection_asset.get("properties")
                if connection_asset and "properties" in connection_asset
                else {}
            )
            database = (
                connection_asset_properties.get("database")
                if connection_asset_properties
                else ""
            )

            # get query category tag
            query_tag = get_snowflake_query_tag(connection_info, config)
            # connection = __create_snowflake_connection(credentials, database, query_tag)
            connection = None
            is_mutliple = True
    return connection, is_mutliple


def get_snowflake_query_tag(connection, config):
    """
    get snowflake query tag
    """
    version = APP_NAME
    airflow_conn_object = connection.get("airflow_connection_object", "")
    if airflow_conn_object:
        extras = json.loads(airflow_conn_object.get("extra", {}))
        version = extras.get("version") if extras else version

    # get query category tag
    query_category = (
        config.get("category").capitalize() if config.get("category") else ""
    )
    query_sub_category = (
        config.get("sub_category").capitalize() if config.get("sub_category") else ""
    )
    measure_type = (
        config.get("measure_type").capitalize() if config.get("measure_type") else ""
    )
    if query_category is None or len(query_category) == 0:
        query_category = (
            os.environ.get("AIRFLOW_CTX_DAG_ID").split("_")[1].capitalize()
            if "_" in os.environ.get("AIRFLOW_CTX_DAG_ID")
            else None
        )

    if query_category:
        query_category = (
            f"Auto Measure {query_category}"
            if query_category in ["Reliability", "Statistics"]
            else f"{query_category} Measure"
        )
    else:
        if measure_type:
            if query_sub_category:
                query_category = f"{measure_type} {query_sub_category} Measure"
            else:
                query_category = f"{measure_type} Measure"
        else:
            query_category = (
                f"{query_sub_category} Measure" if query_sub_category else ""
            )

    return f"{version}, {query_category}" if query_category else version


def get_license_config(config):
    """
    Get License Config
    """
    license = {}
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
           select licensed from core.license where status='Active' limit 1
        """
        cursor = execute_query(connection, cursor, query_string)
        license = fetchone(cursor)
        if license:
            license = decrypt(license.get("licensed"))
            license = (
                json.loads(license) if license and isinstance(license, str) else license
            )
            license = license if license else {}
    return license


def refresh_oauth_token(config, credentials):
    """
    Refresh Oauth Token
    """
    connection_id = config.get("connection_id")
    if credentials.get("expiry", 0):
        if time.time() >= credentials["expiry"] - 60:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": decrypt(credentials.get("refresh_token")),
                "client_id": decrypt(credentials.get("client_id")),
                "client_secret": decrypt(credentials.get("client_secret")),
            }
            token_url = f"https://{credentials.get('account')}.snowflakecomputing.com/oauth/token-request"
            response = requests.post(token_url, data=data)
            if response.status_code == 200:
                response = json.loads(response.text)
                credentials.update(
                    {
                        "access_token": encrypt(response.get("access_token")),
                        "expiry": time.time() + response["expires_in"],
                    }
                )
            else:
                response = response.json()
                if response.get("error") != "invalid_client":
                    response = generate_oauth_token(credentials)
                    credentials = {**credentials, **response}
                else:
                    raise ValueError(response.get("message"))
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.connection set credentials ='{json.dumps(credentials, default=str)}' where id='{connection_id}'
                """
                execute_query(connection, cursor, query_string)
    return credentials


def generate_oauth_token(credentials):
    """
    Generate Oauth Token
    """
    client_id = decrypt(credentials.get("client_id"))
    client_secret = decrypt(credentials.get("client_secret"))
    token_url = f"https://{credentials.get('account')}.snowflakecomputing.com/oauth/token-request"

    # Exchange the authorization code for an access token
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": decrypt(credentials.get("code")),
        "redirect_uri": decrypt(credentials.get("redirect_url")),
    }
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code != 200:
        return f"Error fetching token: {response.text}"

    response = response.json()
    response = {
        "expiry": time.time() + response["expires_in"],
        "access_token": encrypt(response["access_token"]),
        "refresh_token": encrypt(response["refresh_token"]),
    }
    return response


def convert_sql_to_databricks_spark_format(sql_format: str) -> str:
    """
    Converts SQL-standard datetime format characters to Spark-compatible datetime format characters.

    Args:
        sql_format (str): SQL-standard datetime format string.

    Returns:
        str: Spark-compatible datetime format string.
    """
    # Define the mapping of SQL format characters to Spark-compatible format characters
    format_mapping = {
        "YYYY": "yyyy",  # Year (4 digits)
        "YY": "yy",  # Year (2 digits)
        "MM": "MM",  # Month (2 digits)
        "DD": "dd",  # Day of the month (2 digits)
        "HH": "HH",  # Hour (24-hour format, 2 digits)
        "MI": "mm",  # Minute (2 digits)
        "SS": "ss",  # Second (2 digits)
        "HH12": "hh",  # Hour (12-hour format, 2 digits)
        "AM": "a",  # AM/PM marker
        "PM": "a",  # AM/PM marker
    }

    # Replace each SQL format character with its Spark-compatible equivalent
    spark_format = sql_format
    for sql_char, spark_char in format_mapping.items():
        spark_format = spark_format.replace(sql_char, spark_char)

    return spark_format


def handle_execution_before_task_run(config: dict):
    """
    Handles execution steps before a task run based on the provided configuration.

    Args:
        config (dict): A dictionary containing configuration parameters.
                       Expected keys include "connection_type".

    Raises:
        KeyError: If the "connection_type" key is not found in the config dictionary.
        AttributeError: If the connection_type value does not have a lower() method.

    Comments:
        - This function currently supports handling for ADLS connection type.
        - It logs the start of the handling process and calls the appropriate function
          based on the connection type.
    """
    log_info("START HANDLING EXECUTION BEFORE TASK RUN...")

    connection_type = config.get("connection_type")

    if connection_type.lower() in [
        ConnectionType.ADLS.value.lower(),
        ConnectionType.File.value.lower(),
        ConnectionType.S3.value.lower(),
    ]:
        create_iceberg_table_using_spark(config)


def create_iceberg_table_using_spark(config: dict):
    """
    Creates an Iceberg table using Spark.

    This function prepares the necessary parameters and executes a request to create an Iceberg table
    using a Spark job. It retrieves configuration details, constructs the request parameters, and
    calls the agent_helper to execute the query.

    Args:
        config (dict): Configuration dictionary containing connection and asset details.

    Returns:
        dict: The response from the agent_helper after executing the query.

    Raises:
        Exception: If there is an error during the execution of the query.
    """
    try:
        log_info("CREATING ICEBERG TABLE USING SPARK...")

        # Retrieve asset and asset properties from the configuration
        external_storage_config = get_external_storage_config(config)
        spark_catalog = external_storage_config.get("spark_catalog", "")
        spark_namespace = external_storage_config.get("spark_namespace", "")

        asset = config.get("asset")
        asset_properties = asset.get("properties")
        asset_properties = asset_properties if asset_properties else {}
        is_incremental = config.get("is_incremental", False)
        is_custom_fingerprint = asset.get("is_custom_fingerprint", False)

        # Get Attributes List
        attributes = get_attributes_list(config)
        attributes = attributes if attributes else []
        asset_properties.update({"attributes": attributes})

         # Get Livy Spark configuration
        dag_info = config.get("dag_info", {})
        livy_spark_config = dag_info.get("livy_spark_config", {})
        livy_spark_config = livy_spark_config if livy_spark_config else {}
        log_info(f"Livy Spark Config: {livy_spark_config}")
        livy_server_url = livy_spark_config.get("livy_url")
        livy_driver_file_path = livy_spark_config.get("drivers_path")
        iceberg_catalog = livy_spark_config.get("catalog")
        iceberg_namespace = livy_spark_config.get("namespace")
        spark_conf = livy_spark_config.get("spark_config", {})
        spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

        # Set default values for iceberg_catalog and iceberg_namespace
        iceberg_catalog = spark_catalog if spark_catalog else iceberg_catalog
        iceberg_namespace = spark_namespace if spark_namespace else iceberg_namespace

        if not livy_server_url or not iceberg_catalog:
            log_error(
                "Livy Server URL or Iceberg Catalog is not configured properly in the DAG info.", 
                "Livy Server URL or Iceberg Catalog is not configured properly in the DAG info."
            )
            raise Exception(
                "Livy Server URL or Iceberg Catalog is not configured properly in the DAG info."
            )

        # Add Incremental Config Details
        is_incremental = False
        if is_incremental:
            incremental_config = config.get("incremental_config", {})
            watermark = config.get("watermark")
            if watermark and incremental_config:
                incremental_config.update({"watermark": watermark})
                asset_properties.update({"incremental_config": incremental_config})

        # Get the PostgreSQL connection
        pg_connection = get_postgres_connection(config)

        # Prepare method name and parameters for the request
        method_name = "execute"
        parameters = dict(
            request_type="create_iceberg",
            request_params=dict(
                livy_server_url=livy_server_url,
                livy_driver_file_path=livy_driver_file_path,
                iceberg_catalog=iceberg_catalog,
                iceberg_namespace=iceberg_namespace,
                external_storage_config=external_storage_config,
                spark_conf=spark_conf,
                **asset_properties,
            ),
        )

        # Execute the query using agent_helper
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name=method_name,
            parameters=parameters,
        )
        api_response = api_response if api_response else {}
        log_info(f"API Response : {api_response}")

        api_response = api_response.get("response", {})
        if api_response.get("status") == "success":
            log_info("IceBerg Table Created Successfully")
        else:
            log_error("Iceberg Table Creation Failed:", api_response.get("message", ''))
            raise Exception(
                f"Iceberg Table Creation Failed: {api_response.get('message', '')}"
            )
    except Exception as e:
        log_error("Create IceBerg Table Failed Error:", e)
        raise e


def get_external_storage_config(config: dict) -> dict:
    """
    Retrieves the external storage configuration from the provided config dictionary.
    Args:
        config (dict): Configuration dictionary containing settings and DAG info.
    Returns:
        dict: A dictionary containing the external storage configuration.
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

    external_storage = general_settings.get("external_storage")
    external_storage = (
        json.loads(external_storage, default=str)
        if external_storage and isinstance(external_storage, str)
        else external_storage
    )
    external_storage = external_storage if external_storage else {}
    return external_storage


def prepare_agent_config(
    config: dict, query: str, method_name: str, parameters: dict
) -> tuple:
    """
    Prepares the agent configuration for executing a query.

    Args:
        config (dict): Configuration dictionary containing connection and asset details.
        query (str): The SQL query to be executed.
        method_name (str): The method name to be used for execution.
        parameters (dict): Additional parameters for the execution.

    Returns:
        tuple: A tuple containing the modified query, method name, and parameters.
    """

    # Get Connection Type
    connection_type = config.get("connection_type")

    # Get Connection Credentials
    credentials = config.get("connection", {}).get("credentials")
    credentials = credentials if credentials else {}

    # Get Asset Properties
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_properties = asset.get("properties") if asset.get("properties") else {}
    asset_properties = asset_properties if asset_properties else {}
    

    # Check if the connection type is ADLS and the platform type is SPARK
    if ((
        connection_type == ConnectionType.ADLS.value
        and credentials.get("platform_type", "") == SPARK and method_name not in ["read_file", "assets"]
    ) or (connection_type in [ConnectionType.File.value, ConnectionType.S3.value]) and method_name not in ["read_file", "assets"]):
        
        if not asset_properties:
            # Use SQL to implement the asset filtering and iceberg_table_name matching logic
            connection_id = config.get("connection", {}).get("id")
            if connection_id:
                # Query assets matching connection_id, is_active, is_delete, and non-null properties
                with get_postgres_connection(config) as connection:
                    with connection.cursor() as cursor:
                        asset_query = f"""
                            SELECT *
                            FROM core.asset
                            WHERE connection_id = '{connection_id}'
                            AND is_active = TRUE
                            AND is_delete = FALSE
                            AND properties IS NOT NULL
                        """
                        cursor = execute_query(connection, cursor, asset_query)
                        assets = fetchall(cursor)
                        for asset_row in assets:
                            asset_id = asset_row.get("id")
                            properties = asset_row.get("properties")
                            properties_dict = properties if properties else {}
                            iceberg_table_name = properties_dict.get('iceberg_table_name', '') if properties_dict else ''
                            if iceberg_table_name and str(iceberg_table_name) in query:
                                asset = asset_row
                                break

        asset_properties = asset.get("properties") if asset.get("properties") else {}
        asset_properties = asset_properties if asset_properties else {}
        # Get External Storage Config
        external_storage_config = get_external_storage_config(config)
        spark_catalog = external_storage_config.get("spark_catalog", "")
        spark_namespace = external_storage_config.get("spark_namespace", "")

        # Get Iceberg Table Name and Asset Technical Name
        iceberg_table_name = (
            asset_properties.get("iceberg_table_name") if asset_properties else ""
        )
        asset_technical_name = asset.get("technical_name")

        # Ensure iceberg_table_name is a string to avoid TypeError
        iceberg_table_name = iceberg_table_name if iceberg_table_name is not None else ""
        iceberg_table_name = str(iceberg_table_name)

        # Get Livy Spark configuration
        dag_info = config.get("dag_info", {})
        livy_spark_config = dag_info.get("livy_spark_config", {})
        livy_spark_config = livy_spark_config if livy_spark_config else {}
        livy_server_url = livy_spark_config.get("livy_url")
        livy_driver_file_path = livy_spark_config.get("drivers_path")
        iceberg_catalog = livy_spark_config.get("catalog")
        iceberg_namespace = livy_spark_config.get("namespace")
        spark_conf = livy_spark_config.get("spark_config", {})
        spark_conf = (json.loads(spark_conf) if spark_conf and isinstance(spark_conf, str) else spark_conf)

        # Set default values for iceberg_catalog and iceberg_namespace
        iceberg_catalog = spark_catalog if spark_catalog else iceberg_catalog
        iceberg_namespace = spark_namespace if spark_namespace else iceberg_namespace

        # Construct the fully qualified Iceberg table name
        iceberg_cat_np = f"{iceberg_catalog}.{iceberg_namespace}"
        if iceberg_cat_np not in iceberg_table_name and "." not in iceberg_table_name:
            full_iceberg_table_name = f"{iceberg_cat_np}.{iceberg_table_name}"
        else:
            full_iceberg_table_name = iceberg_table_name

        method_name = "execute"
        parameters = dict(
            request_type="execute_query_by_spark",
            request_params=dict(
                livy_server_url=livy_server_url,
                livy_driver_file_path=livy_driver_file_path,
                iceberg_catalog=iceberg_catalog,
                iceberg_namespace=iceberg_namespace,
                external_storage_config=external_storage_config,
                spark_conf=spark_conf,
                **asset_properties,
            ),
        )

        # Convert query to lowercase and replace the table name
        if full_iceberg_table_name not in query:
            query = query.replace(asset_technical_name, full_iceberg_table_name)

    if method_name in ["read_file", "assets"] and connection_type in [ConnectionType.S3.value, ConnectionType.ADLS.value]:
        method_name = "execute"
   
    return query, method_name, parameters


def get_attributes_list(config: dict) -> list:
    """
    Returns the list of selected attributes for the given asset
    """
    try:
        asset_id = config.get("asset_id")
        
        # Determine the asset condition based on whether asset_id is present
        if not asset_id or asset_id == "None" or asset_id is None:
            asset_condition = "asset.id is null"
        else:
            asset_condition = f"asset.id='{asset_id}'"
            
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select attribute.id, attribute.name, attribute.datatype
                from core.attribute
                join core.asset on asset.id = attribute.asset_id
	 			join core.data on data.asset_id = asset.id
                where {asset_condition}
                and attribute.is_selected=True
                order by attribute.created_date asc
            """
            cursor = execute_query(connection, cursor, query_string)
            attributes = fetchall(cursor)
        return attributes
    except Exception as e:
        raise e


def delete_metrics(
    config: dict,
    run_id: str,
    measure_id: str = None,
    measure_ids: str = None,
    asset_id: str = None,
    attribute_id: str = None,
    attribute_ids: str = None,
    level: str = None,
):
    """
    Deletes metrics for the given asset with optional filters for run_id, attribute_id, and measure_ids.
    """
    try:
        filters = []
        filter_condition = ""
        if asset_id:
            filters.append(f"asset_id = '{asset_id}'")
        if attribute_id:
            filters.append(f"attribute_id = '{attribute_id}'")
        if attribute_ids:
            filters.append(f"attribute_id IN ({attribute_ids})")
        if measure_id:
            filters.append(f"measure_id = '{measure_id}'")
        if measure_ids:
            filters.append(f"measure_id IN ({measure_ids})")
        if level:
            filters.append(f"level = '{level}'")

        if filters:
            filter_condition += " AND " + " AND ".join(filters)
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                WITH metrics AS (
                    SELECT * 
                    FROM core.metrics 
                    WHERE run_id = '{run_id}' {filter_condition}
                ), 
                issues AS (
                    SELECT * 
                    FROM core.issues 
                    WHERE metrics_id IN (SELECT id FROM metrics)
                ), 
                delete_issue_attachment AS (
                    DELETE 
                    FROM core.issue_attachment 
                    WHERE issue_id IN (SELECT id FROM issues)
                ), 
                delete_issue_events AS (
                    DELETE 
                    FROM core.events 
                    WHERE issue_id IN (SELECT id FROM issues) 
                    OR metrics_id IN (SELECT id FROM metrics)
                ), 
                delete_issue_work_log AS (
                    DELETE 
                    FROM core.issue_work_log 
                    WHERE issue_id IN (SELECT id FROM issues)
                ), 
                delete_issue_watch AS (
                    DELETE 
                    FROM core.issue_watch 
                    WHERE issue_id IN (SELECT id FROM issues)
                ), 
                delete_issue_comments AS (
                    DELETE 
                    FROM core.issue_comments 
                    WHERE issue_id IN (SELECT id FROM issues)
                ), 
                delete_user_mapping AS (
                    DELETE 
                    FROM core.user_mapping 
                    WHERE issue_id IN (SELECT id FROM issues)
                ),
				delete_associated_issues AS (
                    DELETE 
                    FROM core.associated_issues 
                    WHERE metrics_id IN (SELECT id FROM metrics) or issue_id IN (SELECT id FROM issues)
                ),
				delete_issues AS (
                    DELETE 
                    FROM core.issues 
                    WHERE id IN (SELECT id FROM issues)
                )
                DELETE 
                FROM core.metrics 
                WHERE id IN (SELECT id FROM metrics)
            """
            cursor = execute_query(connection, cursor, query_string)
        return True
    except Exception as e:
        log_error("Error in delete_metrics:", e)
        raise e


def get_technical_name(
    name: str,
    include_integer: bool = True,
    include_dollar: bool = False,
    include_special_char: bool = False,
) -> str:
    """
    Returns the technical name for the given name
    """
    technical_name = ""
    if not name:
        return technical_name

    if include_dollar:
        regex = "[^A-Za-z_0-9_$]" if include_integer else "[^A-Za-z_$]"
    elif include_special_char:
        regex = (
            "[^A-Za-z_0-9_$.!@#$%^&*/']"
            if include_integer
            else "[^A-Za-z_$.!@#$%^&*/']"
        )
    else:
        regex = "[^A-Za-z_0-9]" if include_integer else "[^A-Za-z_]"
    technical_name = re.sub(regex, "_", name.strip()).strip()
    return str(technical_name)