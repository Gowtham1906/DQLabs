import re
import os
import json
import math
import boto3
import datetime
import sqlglot
from sqlglot import exp

# import pymssql
import pandas as pd  # s3select connector
from PIL import Image
from io import BytesIO
import requests
import base64

# import pyodbc
from decimal import Decimal
from dqlabs.app_constants.dq_constants import (
    NUMERIC,
    DATE,
    TEXT,
    BIT,
    MEASURE_CATEGORIES,
    APP_NAME,
    DIRECT_QUERY_BASE_TABLE_LABEL,
    JSON,
)
from dqlabs.app_helper.crypto_helper import decrypt
import logging
from dqlabs.enums.connection_types import ConnectionType

from dqlabs.app_helper.db_helper import fetchone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

rule_separator = "___"


def log_info(message):
    logger.info(message)


def log_error(message: str, error: Exception):
    logger.error(f"{message} : {str(error)}", exc_info=True)


def convert_to_lower(input_data, sorting: bool = False):
    """
    Converts all the keys in a dict into lower case
    """
    if isinstance(input_data, list):
        input = []
        for data in input_data:
            data = {key.lower(): value for key, value in data.items()}
            input.append(data)
        input_data = input
    if isinstance(input_data, dict):
        input_data = {key.lower(): value for key, value in input_data.items()}
        if sorting:
            input_data = dict(sorted(input_data.items()))
    return input_data


def get_attribute_label(
    attribute: str,
    include_separator: bool = True,
    include_integer: bool = True,
    is_lower: bool = True,
    is_behavioral: bool = False,
):
    """
    Returns the attribute label for combined queries
    """
    global rule_separator

    attribute = attribute if attribute else ""
    attribute_label = f"{attribute}" if attribute else ""

    """
        This code we added purpose some attribute starting character numeric that time faild profile
    """
    if attribute_label:
        if attribute_label[0].isdigit():
            include_integer = False
        if is_behavioral:
            include_integer = True

    regex = "[^A-Za-z_0-9]" if include_integer else "[^A-Za-z_]"
    attribute = attribute.strip().lower() if is_lower else attribute.strip()
    attribute_label = re.sub(regex, "_", attribute)
    attribute_label = (
        f"{attribute_label}{rule_separator}"
        if include_separator
        else f"{attribute_label}"
    )

    return attribute_label


def transform_mongodb_field_names(results: dict, attribute_name: str) -> dict:
    """
    Transform MongoDB result field names to include attribute prefix.
    """
    if not results or not attribute_name:
        return results
    
    attribute_label = get_attribute_label(attribute_name)
    transformed = {}
    
    for key, value in results.items():
        if not key.startswith(attribute_label):
            new_key = f"{attribute_label}{key}"
            transformed[new_key] = value
        else:
            transformed[key] = value
    
    return transformed


def get_rule_separator():
    """
    Returns the rule separator value
    """
    global rule_separator
    return rule_separator


def get_derived_type(datatypes: dict, source_type: str) -> str:
    """
    Returns the derived datatype for the given source datatype
    """
    derived_type = ""
    if not source_type:
        return derived_type
    for key, value in datatypes.items():
        if source_type.upper() not in value:
            continue
        derived_type = key
        break
    return derived_type


def get_derived_type_category(
    derived_type: str, is_freshness_column: bool = False
) -> str:
    """
    Returns the category for the given derived datatype
    """
    if is_freshness_column and derived_type == "time":
        return None
    if derived_type in ["integer", "numeric"]:
        return NUMERIC
    elif derived_type in ["date", "datetime", "datetimeoffset", "time"]:
        return DATE
    elif derived_type in ["text"]:
        return TEXT
    elif derived_type in ["bit"]:
        return BIT
    elif derived_type in ["json"]:
        return JSON
    else:
        return None


def format_freshness(value: int):
    """
    Format freshness value from seconds into hour/min/sec format
    """
    # Handle None or invalid values
    if value is None:
        return "N/A"
    
    try:
        value = int(value)
    except (ValueError, TypeError):
        return "N/A"

    is_negative = value < 0
    value = abs(value)  # Work with absolute value for calculation
    days = math.floor(value / (3600 * 24))
    hours = math.floor(value % (3600 * 24) / 3600)
    minutes = math.floor(value % 3600 / 60)
    seconds = math.floor(value % 60)
    _hours = (days * 24) + hours

    if days >= 4:
        result = f"{days}d {hours}h {minutes}m" if hours else f"{days}d"
    else:
        if _hours > 0 and _hours <= 95:
            result = f"{_hours}h {minutes}m {seconds}s" if minutes else f"{_hours}h"
        elif _hours < 1:
            if 0 < minutes <= 59:
                result = f"{minutes}m {seconds}s" if seconds else f"{minutes}m"
            elif minutes == 60:
                result = f"{minutes}m"
            else:
                result = f"{seconds}s"
    return f"-{result}" if is_negative else result


def calculate_weightage_score(score: int, weightage: int = 100):
    """
    Calculate Weightage Based measure score
    """
    return score * weightage / 100


def get_client_origin(config: dict):
    """
    Get Client Origin
    """
    client_domain = config.get("domain")
    client_domain = (
        client_domain if client_domain else os.environ.get("DEFAULT_CLIENT_DOMAIN")
    )
    origin = os.environ.get("DQLABS_CLIENT_ENDPOINT")
    origin = origin.replace("<domain>", client_domain)
    return origin


def get_client_domain(config: dict):
    """
    Get Client Domain
    """
    client_domain = config.get("domain")
    client_domain = (
        client_domain if client_domain else os.environ.get("DEFAULT_CLIENT_DOMAIN")
    )
    if client_domain:
        client_domain = re.sub("[^A-Za-z0-9]", "_", client_domain.strip().lower())
    return client_domain


def get_aws_credentials(config: dict):
    """Get the aws credentials for athena connector for triggering airflow jobs"""
    connection_type = config.get("connection_type")
    secret_access_key = json.loads(
        config.get("connection").get("airflow_connection_object").get("extra")
    )
    # s3 select aws credentials
    if connection_type == ConnectionType.S3Select.value:
        aws_credentials = {
            "aws_access_key_id": decrypt(
                config.get("connection").get("credentials").get("awsaccesskey")
            ),
            "aws_secret_access_key": decrypt(
                secret_access_key.get("aws_secret_access_key")
            ),
            "region_name": config.get("connection").get("credentials").get("region"),
        }
    else:
        aws_credentials = {
            "aws_access_key_id": decrypt(
                config.get("connection").get("credentials").get("awsaccesskey")
            ),
            "aws_secret_access_key": decrypt(
                secret_access_key.get("aws_secret_access_key")
            ),
            "region_name": config.get("connection").get("credentials").get("region"),
            "s3_staging_dir": config.get("connection")
            .get("credentials")
            .get("s3stagingdir"),
            "catalog_name": config.get("connection").get("credentials").get("database"),
            "work_group": config.get("connection").get("credentials").get("workgroup"),
        }
    return aws_credentials


def get_synapse_credentials(config: dict):
    credentials = config.get("connection").get("credentials")
    authentication_type = credentials.get("authentication_type")
    if authentication_type == "Username and Password":
        synapse_credentials = {
            "server": credentials.get("server"),
            "database": credentials.get("database"),
            "username": decrypt(credentials.get("user")),
            "password": decrypt(credentials.get("password")),
            "authentication_type": credentials.get("authentication_type"),
        }
    else:
        synapse_credentials = {
            "server": credentials.get("server"),
            "database": credentials.get("database"),
            "client_id": decrypt(credentials.get("client_id")),
            "client_secret": decrypt(credentials.get("client_secret")),
            "tenant_id": credentials.get("tenant_id"),
            "authentication_type": credentials.get("authentication_type"),
        }

    return synapse_credentials


def get_hive_credentials(config: dict):
    """Run hive queries through pyhive"""
    hive_credentials = {
        "host": config.get("connection").get("credentials").get("host"),
        "port": config.get("connection").get("credentials").get("port"),
        "database": config.get("connection").get("credentials").get("database"),
        "username": decrypt(config.get("connection").get("credentials").get("user")),
        "password": decrypt(
            config.get("connection").get("credentials").get("password")
        ),
    }
    return hive_credentials


def get_db2_credentials(config: dict):
    """Run db2 queries through idm_db"""
    db2_credentials = {
        "hostname": config.get("connection").get("credentials").get("host"),
        "port": config.get("connection").get("credentials").get("port"),
        "database": config.get("connection").get("credentials").get("database"),
        "uid": decrypt(config.get("connection").get("credentials").get("user")),
        "pwd": decrypt(config.get("connection").get("credentials").get("password")),
    }
    return db2_credentials


def get_windows_auth_connection(config: dict):
    """Get connection for mssql with windows authentication"""
    credentials = config.get("connection").get("credentials")
    credentials = credentials if credentials else {}
    # connection = None
    # auth_type = credentials.get("authentication_type")
    is_read_only = credentials.get("is_read_only")
    is_read_only = is_read_only if is_read_only else False
    # # print(f"""calling pymssql with read_only set to {is_read_only}""")
    # if auth_type and auth_type.lower() == "windows authentication":
    #     connection = pymssql.connect(
    #         host=credentials.get("server"),
    #         user=decrypt(credentials.get("user")),
    #         password=decrypt(credentials.get("password")),
    #         database=credentials.get("database"),
    #         read_only=is_read_only,
    #         appname=APP_NAME,
    #         timeout=180,
    #     )
    # return connection


def get_advanced_measure_names():
    advanced_measures = []
    for key, values in MEASURE_CATEGORIES.items():
        if key in ["length", "value", "health", "user_defined_patterns"]:
            continue
        advanced_measures.extend(values)
    # add length/value range for measure profiling
    advanced_measures.extend(["length_range", "value_range"])  # dql-963
    health_measures = MEASURE_CATEGORIES.get("health")
    for measure in health_measures:
        if measure in advanced_measures:
            advanced_measures.remove(measure)
    return advanced_measures


def check_measure_result(measure: dict, dq_score):
    pass_criteria_result = measure.get("pass_criteria_result", "")
    pass_criteria_result = pass_criteria_result if pass_criteria_result else ""
    allow_score = measure.get("allow_score", False)
    enable_pass_criteria = measure.get("enable_pass_criteria", False)

    if allow_score and enable_pass_criteria:
        pass_criteria_threshold = measure.get("pass_criteria_threshold", 100)
        pass_criteria_condition = measure.get("pass_criteria_condition", ">=")
        dq_score = dq_score if dq_score else 0

        if pass_criteria_condition == "==" and dq_score == pass_criteria_threshold:
            pass_criteria_result = "Pass"
        elif pass_criteria_condition == "!=" and dq_score != pass_criteria_threshold:
            pass_criteria_result = "Pass"
        elif pass_criteria_condition == ">" and dq_score > pass_criteria_threshold:
            pass_criteria_result = "Pass"
        elif pass_criteria_condition == "<" and dq_score < pass_criteria_threshold:
            pass_criteria_result = "Pass"
        elif pass_criteria_condition == "<=" and dq_score <= pass_criteria_threshold:
            pass_criteria_result = "Pass"
        elif pass_criteria_condition == ">=" and dq_score >= pass_criteria_threshold:
            pass_criteria_result = "Pass"
        else:
            pass_criteria_result = "Fail"
    return pass_criteria_result




def get_database_name(config: dict):
    connection = config.get("connection")
    connection = connection if connection else {}
    credentials = connection.get("credentials")
    credentials = (
        json.loads(credentials)
        if credentials and isinstance(credentials, str)
        else credentials
    )
    credentials = credentials if credentials else {}
    db_name = credentials.get("database")
    db_name = db_name if db_name else ""
    return db_name


def prepare_databricks_query(config: dict, query: str) -> str:
    """Cleanup the databricks query mode for Airflow usage"""
    database = config.get("connection").get("credentials").get("database")
    query_mode = config.get("asset").get("query")
    query = query.lower()
    # map the pattern and identify the select statment to be altered
    database_pattern = re.compile(r"from\s+(\w+\.\w+\.\w+)", re.IGNORECASE)
    database_match = database_pattern.findall(query)
    schema_pattern = re.compile(r"from\s+(\w+\.\w+)", re.IGNORECASE)
    schema_match = schema_pattern.findall(query)
    new_query_mode = None
    if database_match:
        database, schema, table = database_match[0].split(".")
        new_query_mode = (
            query_mode.replace(database, f"`{database}`")
            .replace(schema, f"`{schema}`")
            .replace(table, f"`{table}`")
        )
    elif schema_match:
        schema, table = schema_match[0].split(".")
        altered_schema = f"`{database}`.`{schema}`"
        new_query_mode = query_mode.replace(schema, altered_schema).replace(
            table, f"`{table}`"
        )

    if new_query_mode:
        query = query.replace(query_mode, new_query_mode)
    return query


def cleanup_query_whitespace(query: str, table_name: str):
    """condition for space in select query"""
    if "select".lower() in query.lower():
        select_index = query.lower().find("select")
        from_index = query.lower().find("from")

        # Check if there is a space between "SELECT" and "FROM"
        if select_index != -1 and from_index != -1:
            space_between_select_and_from = query[
                select_index + len("SELECT") : from_index
            ]
            if space_between_select_and_from.isspace():
                query = f"select * from {table_name}"

    if (
        query
        and table_name
        and not re.search(r"\bselect\b", query.lower(), re.I)
        and not re.search(r"\bshow\b", query.lower(), re.I)
        and not re.search(r"\bdrop\b", query.lower(), re.I)
        and not re.search(r"\bcreate\b", query.lower(), re.I)
        and not re.search(r"\bdescribe\b", query.lower(), re.I)
        and not re.search(r"\banalyze\b", query.lower(), re.I)
    ):
        query = f"select {query} from {table_name}"
    return query


def get_alias_name(name: str) -> str:
    """ "
    Return Alias Name
    """
    return name.replace(" ", "_")


def remove_decimal_categorical_column(key_columns: list) -> list:
    """
    Remove decimal point from numerical key columns - DQL-4214

    Input: key_columns = ['India', '632484.0']
    Ouput : ['India', '632484']
    """
    # Iterate through the list
    for i, item in enumerate(key_columns):
        # Check if the item is a float
        if "." in item:
            try:
                # Convert to float and then to integer
                key_columns[i] = str(int(float(item)))
            except:
                key_columns[i] = str(item)
        elif item.isdigit():
            # Convert to integer directly if it's a digit
            key_columns[i] = str(int(item))
        else:
            # Skip the element if it cannot be converted to an integer
            pass
    return key_columns


def get_max_workers(connection_type: ConnectionType = None):
    """
    Returns maximum parallel threads to use for parallel processing
    """
    workers = int(os.cpu_count() - 2)
    if connection_type == ConnectionType.Denodo:
        workers = 4
    workers = workers if workers else 2
    return workers


def check_is_direct_query(config: dict):
    """
    Check whether the given query is of type direct query asset or not
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    base_query = asset.get("query")
    asset_type = asset.get("type")
    asset_type = str(asset_type).lower() if asset_type else ""
    asset_view_type = asset.get("view_type")
    asset_view_type = str(asset_view_type).lower() if asset_view_type else ""

    is_direct_query_measure = (
        asset
        and base_query
        and asset_type == "query"
        and asset_view_type == "direct query"
    )
    config.update(
        {
            "is_direct_query_asset": is_direct_query_measure,
            "base_table_query": base_query,
        }
    )
    return is_direct_query_measure

def process_direct_query(query: str, connection_type: str):
    """
    Process the direct query to replace the proper base table with it's alias
    """
    if not query:
        return query

    base_table_key = DIRECT_QUERY_BASE_TABLE_LABEL
    if connection_type and connection_type == ConnectionType.Oracle.value:
        base_table_key_alias = f"{base_table_key} dqbase"
    else:
        base_table_key_alias = f"{base_table_key} as dqbase"

    if base_table_key not in query:
        return query
    
    # Extract comments and replace them with placeholders
    single_line_comments = []
    block_comments = []

    def slc_replacer(match):
        comment = match.group(0)
        single_line_comments.append(comment)
        return f"__SLC_{len(single_line_comments) - 1}__"

    def block_replacer(match):
        comment = match.group(0)
        block_comments.append(comment)
        return f"__BLOCK_{len(block_comments) - 1}__"

    # Replace inline comments with placeholders on new lines
    query = re.sub(r'(?:--|//).*', slc_replacer, query)
    query = re.sub(r'/\*[\s\S]*?\*/', block_replacer, query)

    base_table_queries = query.split()

    base_table_indices = []
    for i in range(0, len(base_table_queries)):
        if base_table_queries[i] == base_table_key:
            base_table_indices.append(i)

    for index in base_table_indices:
        if index < 0:
            continue
        if index == (len(base_table_queries) - 1):
            base_table_queries[index] = base_table_key_alias
            continue

        alias = str(base_table_queries[index + 1]).lower()
        has_alias = alias == "as" or alias not in [
            "where",
            "on",
            "group",
            "order",
            "limit",
            ")",
        ]
        if has_alias:
            continue
        base_table_queries[index] = base_table_key_alias
    query = " ".join(base_table_queries)

    final_query = query
    for idx, comment in enumerate(block_comments):
        final_query = final_query.replace(f"__BLOCK_{idx}__", f"\n{comment}\n")
    for idx, comment in enumerate(single_line_comments):
        final_query = final_query.replace(f"__SLC_{idx}__", f"\n{comment}\n")

    log_info((f"Processed Direct Query: {final_query}"))
    return final_query


def parse_numeric_value(value):
    """
    Parsing numeric values by removing scientific notation,
    trailing zero's after decimal etc.,
    """
    if not value:
        return value
    try:
        if type(value) == int:
            value = int(value) if value else 0
            return value
        if type(value) == float:
            value = float(value) if value else 0

        if re.match("^[+-]?[0-9]+.[0]+$", str(value)):
            value = int(value) if value else 0

        if "e" in str(value).lower():
            value = Decimal(format(value, ".9f"))
            if re.match("^[+-]?[0-9]+.[0]+$", str(value)):
                value = int(value)
        return value
    except:
        return value


def get_pipeline_status(status: str) -> str:
    """
    Change Pipeline Status Value into Common Format
    """
    pipeline_status = None
    if not status:
        return pipeline_status

    if status.lower() in ["success", "pass", "info", "complete"]:
        pipeline_status = "success"
    elif status.lower() in ["skipped"]:
        pipeline_status = "skipped"
    elif status.lower() in ["warning", "upstream_failed", "warn"]:
        pipeline_status = "warning"
    elif status.lower() in ["cancelled"]:
        pipeline_status = "cancelled"
    elif status.lower() in ["waiting"]:
        pipeline_status = "waiting"
    else:
        pipeline_status = "failed"
    return pipeline_status


def parse_teradata_columns(
    columns, queries, column_key: str = "column_name", datatype_key: str = "data_type"
):
    columns_schema = []
    teradata_datatypes = dict(queries.get("teradata_datatypes", {}))
    for column in columns:
        column_name = column.get("column name")
        column_name = column_name.strip() if column_name else ""
        if not column_name:
            continue
        column_type = column.get("type")
        column_type = column_type.strip().upper() if column_type else ""
        if column_type in ["CO", "BO", "BF", "BV", "UT"]:
            continue
        datatype = teradata_datatypes.get(column_type, "")
        columns_schema.append(
            {
                column_key: column_name,
                datatype_key: datatype,
                "description": "",
                "is_null": bool(column.get("nullable") == "N"),
                "is_primary_key": bool(column.get("primary?") == "S"),
            }
        )
    columns_schema.sort(key=lambda x: x[column_key])
    return columns_schema


def extract_select_statement(query_string: str) -> str:
    """
    Extracts the SELECT statement from the given query string.
    """
    cte_pattern = re.compile(r"^\s*WITH\s+.*?\)\s*SELECT", re.IGNORECASE | re.DOTALL)
    match = cte_pattern.search(query_string)
    query = None

    if match:
        # Find the position of the first SELECT statement after the CTE
        select_start = match.end() - len("SELECT")
        query = query_string[select_start:].strip()
    else:
        # If no CTE is found, check if there's a standalone SELECT statement
        select_pattern = re.compile(r"^\s*SELECT", re.IGNORECASE)
        match = select_pattern.search(query_string)
        if match:
            query = query_string[match.start() :].strip()

    query = query.replace(";", "") if query and query.endswith(";") else query
    return query


def fetch_connection_date_format(
    connection_type: str, custom_fingerprint_date: str
) -> str:
    """
    Fetches the correct date format string based on the user's input and connection type.

    Args:
        connection_type (str): The type of database connection (e.g., MySQL).
        custom_fingerprint_date (str): The user-defined date format string.

    Returns:
        str: The corresponding date format for the given connection type. If no mapping is found, returns the input string.
    """
    mapped_fingerprint_date = custom_fingerprint_date

    if connection_type == ConnectionType.MySql.value:
        date_format_map = {
            "YYYYMMDD": "%Y%m%d",
            "YYYY-MM-DD": "%Y-%m-%d",
            "DD-MM-YYYY": "%d-%m-%Y",
            "YYYY-MM-DD HH:MI:SS": "%Y-%m-%d %H:%i:%s",
            "DDMMYYYY": "%d%m%Y",
        }
        mapped_fingerprint_date = date_format_map.get(
            custom_fingerprint_date, custom_fingerprint_date
        )

    return mapped_fingerprint_date


def extract_notation_from_query(query: str) -> list:
    """
    Extracts unique placeholders enclosed in angle brackets (<>) from a given SQL query string.

    Args:
        query (str): The SQL query string to extract placeholders from.

    Returns:
        list: A sorted list of unique placeholders found in the query. If the query is empty or no placeholders
                are found, an empty list is returned.

    Example:
        >>> extract_notation_from_query("SELECT * FROM table WHERE column = <value> AND id = <id>")
        ['id', 'value']
    """
    if "<table_name>" in query:
        query = query.replace("<table_name>", "")
    pattern = r"<([^<>]+)>"
    all_placeholders = []
    if not query:
        return all_placeholders
    matches = re.findall(pattern, query)
    all_placeholders.extend(matches)
    unique_placeholders = sorted(set(all_placeholders))
    return unique_placeholders


def load_env_vars(env_vars: list):
    """
    Parse .env-style bytes and set environment variables.
    """
    for env in env_vars:
        if not env or env.strip().startswith("#"):
            continue
        if "=" not in env:
            continue
        key, value = env.split("=", 1)
        os.environ[key.strip()] = value.strip()


def extract_table_name(tablename, schema_only=False):
    schema = ""
    db_name = ""
    query_table = tablename.split(".")
    if len(query_table) == 3:
        schema = (
            query_table[1]
            .replace('"', "")
            .replace("'", "")
            .replace("[", "")
            .replace("]", "")
        )
        db_name = (
            query_table[0]
            .replace('"', "")
            .replace("'", "")
            .replace("[", "")
            .replace("]", "")
        )
    if len(query_table) == 2 and schema_only:
        schema = (
            query_table[0]
            .replace('"', "")
            .replace("'", "")
            .replace("[", "")
            .replace("]", "")
        )
    query_table = query_table[-1]
    query_table = (
        query_table.replace('"', "").replace("'", "").replace("[", "").replace("]", "")
    )
    return schema, db_name, query_table

def get_default_datatype( column, sample_data, datatype_mappings):
        datatype = get_column_dq_datatype(column, sample_data)
        datatype = datatype if datatype else "string"
        datatype = (
            datatype_mappings.get(datatype)
            if datatype in datatype_mappings
            else datatype
        )
        datatype = str(datatype).lower() if datatype else ""
        return datatype

def get_column_dq_datatype(
    column_name: str, sample_data: list, conver_lower: bool = False
) -> str:
    """
    Get the column datatype for the given column using given sample data
    """
    datatype = "string"
    column_key = column_name if column_name else ""
    if conver_lower:
        column_key = column_name.lower() if column_name else ""

    if not column_key:
        return datatype
    sample_data = sample_data if sample_data else []
    column_data = list(
        map(
            lambda row: row.get(column_key) if row.get(column_key) is not None else None,
            sample_data,
        )
    )
    column_data = column_data if column_data else []
    column_data_types = [detect_type_from_value(column) for column in column_data]
    column_data_type_count = {
        column_data_type: column_data_types.count(column_data_type)
        for column_data_type in column_data_types
    }
    ordered_column_data_type = dict(
        sorted(column_data_type_count.items(), key=lambda item: item[1], reverse=True)
    )
    major_datatype = (
        list(ordered_column_data_type.keys())[0] if ordered_column_data_type else ""
    )
    major_datatype = str(major_datatype) if major_datatype else ""

    default_types = ["datetime", "int", "float", "bool", "date", "integer"]
    if major_datatype:
        for default_type in default_types:
            if default_type != major_datatype:
                continue
            datatype = default_type
            break
    else:
        datatype = "string"

    datatype = datatype if datatype else "string"
    if datatype == "int":
        datatype = "integer"
        if (
            "float" in ordered_column_data_type
            and ordered_column_data_type.get("float") > 0
        ):
            datatype = "numeric"
    elif datatype == "float":
        datatype = "numeric"
    elif datatype == "bool":
        datatype = "boolean"
    return datatype

def get_server_endpoint():
    """
    Prepare Server Endpoint
    """
    server_endpoint = os.environ.get("DQLABS_SERVER_ENDPOINT")
    if "http://localhost:8000" in server_endpoint:
        # # Use this to connect local server from local airflow container
        server_endpoint = server_endpoint.replace(
            "http://localhost:8000", "http://host.docker.internal:8000"
        )
    return server_endpoint

def delete_target_file(target_path: list):
    server_endpoint = get_server_endpoint()
    url = f"{server_endpoint}/api/connection/delete_target/"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        'files': target_path
    }
    requests.post(url, data=json.dumps(data), headers=headers)

def detect_type_from_value(value):
    if value is None:
        return 'string'

    if not isinstance(value, str):
        if isinstance(value, bool):
            return 'bool'
        elif isinstance(value, int):
            return 'int'
        elif isinstance(value, float):
            return 'float'
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return 'timestamp' if isinstance(value, datetime.datetime) else 'date'
        return 'string'

    value = value.strip()
    if not value:
        return 'string'

    # Boolean
    if re.fullmatch(r'(?i)^(true|false|yes|no|t|f|y|n|1|0)$', value):
        return 'bool'

    # Integer detection
    if re.fullmatch(r'^[+-]?\d+$', value.replace(',', '')):
        return 'int'

    # Numeric detection
    if re.fullmatch(r'^[+-]?(?:\d{1,3}(?:,\d{3})*|\.\d+|\d+\.\d*)(?:[eE][+-]?\d+)?$', 
                value.replace(',', '')):
        return 'float'

    # Date/Time detection
    date_formats = [
        r'^\d{4}-\d{2}-\d{2}$',
        r'^\d{2}/\d{2}/\d{4}$',
        r'^\d{4}/\d{2}/\d{2}$',
        r'^\d{2}-\d{2}-\d{4}$',
        r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}',
        r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}',
        r'^\d{2}/\d{2}/\d{4} \d{1,2}:\d{2}:\d{2}',
        r'^\d{2}-\d{2}-\d{4} \d{1,2}:\d{2}:\d{2}',
        r'^\d{1,2}-[A-Za-z]{3}-\d{4}',
        r'^\d{1,2}/[A-Za-z]{3}/\d{4}',
    ]

    for pattern in date_formats:
        if re.fullmatch(pattern, value, re.IGNORECASE):
            if ':' in value or 'T' in value:
                return 'timestamp'
            return 'date'

    # Time without date
    if re.fullmatch(r'^\d{1,2}:\d{2}(?::\d{2})?(?: [AP]M)?$', value, re.IGNORECASE):
        return 'time'

    return 'string'


def get_column_alias_with_function(query: str, connection_type: str) -> dict:
    if connection_type in [ConnectionType.MSSQL.value, ConnectionType.Synapse.value]:
        connection_type = 'tsql'
    elif connection_type in [ConnectionType.Redshift_Spectrum.value, ConnectionType.Athena.value]:
        connection_type = 'redshift'
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.Redshift.value,
        ConnectionType.Snowflake.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Databricks.value,
        ConnectionType.Oracle.value,
        ConnectionType.MySql.value,
    ]:
        connection_type = connection_type.lower()
    parsed = sqlglot.parse_one(query, read=connection_type)
    function_aliases = {}

    for expression in parsed.find_all(exp.Select):
        for selectable in expression.expressions:
            if isinstance(selectable, exp.Alias):
                # Check if the aliased expression contains any function
                if selectable.this.find(exp.Func):
                    alias_name = selectable.alias
                    original_expression = selectable.this.sql()
                    function_aliases[alias_name] = original_expression

            elif isinstance(selectable, exp.Func):
                # Handle unnamed function expressions (less common)
                if selectable.alias:
                    function_aliases[selectable.alias] = selectable.sql()
    return function_aliases 