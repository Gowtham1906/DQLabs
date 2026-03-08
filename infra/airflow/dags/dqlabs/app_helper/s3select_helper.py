import json
import csv
import boto3
import re
import pandas as pd
from pandasql import sqldf

# create mock cursor for s3select asset
from unittest.mock import Mock, MagicMock
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from dqlabs.app_helper.dq_helper import get_aws_credentials

from dqlabs.app_helper.log_helper import log_error, log_info


def identify_csv_delimiter(
    s3_client: object, bucket_name: str, file_name: str, sample_size: int = 1024
):
    """Automatically detect the delimiter if its a csv file"""
    s3_client = s3_client
    # Download a portion of the file
    response = s3_client.get_object(
        Bucket=bucket_name, Key=file_name, Range="bytes=0-{}".format(sample_size)
    )

    try:
        # Read the content
        content = response["Body"].read().decode("utf-8")

        # Use csv.Sniffer to deduce the delimiter
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(content).delimiter

        return delimiter
    except Exception as e:
        delimiter = ","
        return delimiter


def map_s3_attr_index(query, cols):
    # Create a mapping of column names to attr_index
    col_map = {col["column_name"]: col["attr_index"] for col in cols}

    # Function to replace column name with attr_index
    def replace_col_name(match):
        col_name = match.group(0)
        return col_map.get(col_name, col_name)

    # Replace column names in the query
    pattern = re.compile(
        r"\b(" + "|".join(re.escape(col["column_name"]) for col in cols) + r")\b"
    )
    mapped_query = pattern.sub(replace_col_name, query)

    return mapped_query


def map_s3_json_attribute(query, cols):
    # Create a mapping from uppercase to the appropriate column name in the `cols` list
    column_mapping = {col["column_name"]: col["column_name"].lower() for col in cols}
    # Regular expression to find column names enclosed in double quotes
    pattern = re.compile(r's\."(.*?)\"')

    # Function to replace column names if they are in the mapping dictionary
    def replace_column(match):
        column_name = match.group(1)
        return f's."{column_mapping.get(column_name, column_name)}"'

    # Substitute the column names in the query
    result_query = pattern.sub(replace_column, query)
    return result_query


def extract_column_names(query):
    parsed = sqlparse.parse(query)
    statement = parsed[0]

    # Initialize an empty list to hold column names
    column_names = []

    # Flag to check if we are in the SELECT clause
    in_select = False

    for token in statement.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            in_select = True
        elif in_select:
            if token.ttype is Keyword:
                in_select = False
            elif isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    column_names.append(identifier.get_real_name())
            elif isinstance(token, Identifier):
                column_names.append(token.get_real_name())

    return column_names


def extract_aliases(query):
    # Pattern to match alias names in the SELECT clause
    alias_pattern = re.compile(r"\bAS\s+(\w+)(?![^()]*\))", re.IGNORECASE)

    # Find all matches of the alias pattern
    aliases = alias_pattern.findall(query)

    return aliases


def get_s3_client(config):
    """
    Singleton Pattern for s3 client: This pattern ensures that only one instance of the S3 client is created and reused across multiple function calls.

    """
    # Specify the AWS credentials and region
    aws_credentials = get_aws_credentials(config)

    if not hasattr(get_s3_client, "client"):
        session = boto3.session.Session()
        get_s3_client.client = session.client(
            "s3",
            aws_access_key_id=aws_credentials.get("aws_access_key_id"),
            aws_secret_access_key=aws_credentials.get("aws_secret_access_key"),
            region_name=aws_credentials.get("region_name"),
        )
    return get_s3_client.client


def clean_json_query(query: str):
    """Clean up json filetype queries"""
    query = query.replace("s3object", "dataframe")
    # condition for char length
    if "char_length" in query.lower():
        query = query.replace("CHAR_LENGTH", "LENGTH")
    return query


def parse_s3_query_response(
    result: list, config: dict, query_string: str, s3columns: list
):
    """
    Parsing the S3 Select Query response to map to respective columns defined in the query
    """
    is_list = result and isinstance(result, list)
    result = [result] if not is_list else result
    properties = config.get("asset").get("properties")
    file_type = properties.get("file_type")
    measure_type = None
    measure = config.get("measure", "")
    if measure:
        measure_type = measure.get("type", "")

    if file_type == "json":
        columns = extract_aliases(query_string)

    # The regular expression pattern
    alias_pattern = r"\bAS\b"
    # Create Column Description for Cursor
    if result:
        # Condition only for custom conditional measure
        if measure_type and (measure_type == "custom"):
            columns = extract_aliases(query_string)
        elif "*" in query_string:
            columns = s3columns
        elif re.findall(alias_pattern, query_string, re.IGNORECASE):
            columns = extract_aliases(query_string)
        else:
            columns = extract_column_names(query_string)

    # map the columns to the result data from s3 select
    for index in range(0, len(columns)):
        for row in result:
            col_name = f"col_{index}"
            column_name = columns[index] if index <= len(columns) - 1 else col_name
            row[column_name] = row.pop(col_name)
    result = result[0] if not is_list else result
    return result


def run_s3select_query(config: dict, query_string: str, s3columns: list) -> object:
    """Get the aws credentials for s3select connector for triggering airflow jobs"""

    # Create the s3 boto3 client
    s3_client = get_s3_client(config)
    measure_type = None

    # for custom measures
    measures = config.get("measure", "")

    if measures:
        measure_type = measures.get("type", "")

    # Get the file properties
    properties = config.get("asset").get("properties")
    s3_bucket_name = config.get("connection").get("credentials").get("bucket")
    s3_bucket_name = s3_bucket_name.strip()
    s3_file_name = properties.get("name", "")
    file_type = properties.get("file_type")

    s3_folder_path = properties.get("folder", "")
    s3_key = f"{s3_folder_path}/{s3_file_name}" if s3_folder_path else s3_file_name

    asset_properties = config.get("asset").get("properties")
    is_header = asset_properties.get("is_header")
    header = "USE" if is_header else "NONE"
    query = f"""{query_string}"""

    # Creating a default mock cursor object for s3select only
    cursor = MagicMock()

    if file_type == "csv":
        delimiter = identify_csv_delimiter(s3_client, s3_bucket_name, s3_key)
        params = {
            "Bucket": s3_bucket_name,
            "Key": s3_key,
            "ExpressionType": "SQL",
            "Expression": query,
            "InputSerialization": {
                "CSV": {
                    "FileHeaderInfo": header,
                    "FieldDelimiter": delimiter,
                    "RecordDelimiter": "\n",
                    "AllowQuotedRecordDelimiter": True,
                }
            },
            "OutputSerialization": {
                "CSV": {
                    "QuoteEscapeCharacter": "\\",
                    "RecordDelimiter": "\n",
                    "FieldDelimiter": ",",
                    "QuoteCharacter": '"',
                }
            },
        }

        response = s3_client.select_object_content(**params)
        rows = ""

        objects = None
        for event in response["Payload"]:
            if "Records" in event:
                records = event["Records"]["Payload"].decode("utf-8")
                if file_type == "csv":
                    rows = records.strip().split("\r\n")
                    log_info(("rows", rows))
                    if "count(*)" in query_string:
                        objects = [rows[0]]
                    else:
                        # Convert the API results into a mock cursor
                        if file_type == "csv":
                            objects = [tuple(row.split(delimiter)) for row in rows]
                        else:
                            objects = [tuple(row.split(",")) for row in rows]

                    if len(records.strip().split(",")) == 1:
                        # Define the behavior for fetchone()
                        cursor.fetchone = MagicMock(
                            return_value=objects if objects else None
                        )
                    else:
                        cursor.fetchall = MagicMock(return_value=objects)
                        cursor.fetchone = MagicMock(
                            return_value=objects if objects else None
                        )

    elif file_type == "json":
        response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        json_data = json.loads(content)
        dataframe = pd.DataFrame.from_records(json_data)
        try:

            # setup the json dataframe
            df_query = clean_json_query(query)

            # Execute Query
            result_df = sqldf(df_query, locals())
            # fetch the conditional result
            # Format the output as requested
            objects = [tuple(map(str, row)) for row in result_df.values]

            if len(objects[0]) == 1:
                cursor.fetchone = MagicMock(return_value=objects if objects else None)
            else:
                cursor.fetchall = MagicMock(return_value=objects)
                cursor.fetchone = MagicMock(return_value=objects if objects else None)

            # Create custom query
            columns = extract_aliases(query_string)
            cursor.description = columns
            return cursor
        except Exception as e:
            raise Exception(f"JsonObject Error : {e}")

    # get_s3columns = get_s3select_attributes(config)
    get_s3columns = s3columns

    # The regular expression pattern
    alias_pattern = r"\bAS\b"
    # Create Column Description for Cursor
    if objects:
        # Condition only for custom conditional measure
        if measure_type and (measure_type == "custom"):
            columns = extract_aliases(query_string)
            cursor.description = columns
            return cursor

        if "count(*)" in query_string.lower():
            cursor.description = [("total_rows")]
        elif "*" in query_string:
            # Get all the attributes possible
            columns = [(col) for col in get_s3columns]
            cursor.description = columns
        elif re.findall(alias_pattern, query, re.IGNORECASE):
            columns = extract_aliases(query)
            cursor.description = columns
        else:
            columns = extract_column_names(query_string)
            cursor.description = columns

    log_info(("Successfully created cursor for {query_string}"))

    return cursor
