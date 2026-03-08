"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import re
import json
import os
import copy
import pandas as pd
import numpy as np
from uuid import uuid4
from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries

from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.dag_helper import get_postgres_connection

from dqlabs.utils.extract_workflow import save_measure, get_queries

from dqlabs.app_constants.dq_constants import FAILED, PASSED
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.dag_helper import execute_native_query, get_query_lookup_string
from dqlabs.utils.range import get_range_measures, update_range_measure
from dqlabs.utils.profile import to_list
from dqlabs.enums.connection_types import ConnectionType
from detect_delimiter import detect
from chardet.universaldetector import UniversalDetector
from dqlabs.utils.lookup_api import APIDataValidator


def update_table_name_incremental_conditions(invalid_select_query):
    try:
        validate_query = (
            copy.deepcopy(invalid_select_query)
            .replace("\n", "")
            .replace("\t", "")
            .replace(" ", "")
        )
        if "wherecasewhen" in validate_query.lower():
            invalid_select_query = re.split(
                "case", invalid_select_query, flags=re.IGNORECASE
            )
            if len(invalid_select_query) == 2:
                casequery = invalid_select_query[1]
                parent_table_name = "t1"

                replace_value = [
                    {
                        "old": r"\bAND\s+(?!t1\.)(\w+)\s+IS\s+NOT\s+NULL\b",
                        "new": rf"AND {parent_table_name}.\1 IS NOT NULL",
                    },
                    {
                        "old": r"\bAND\s+(?!t1\.)`(\w+)`\s+IS\s+NOT\s+NULL\b",
                        "new": rf"AND {parent_table_name}.`\1` IS NOT NULL",
                    },
                    {
                        "old": r"\bAND\s+(?!t1\.)'(\w+)'\s+IS\s+NOT\s+NULL\b",
                        "new": rf"AND {parent_table_name}.'\1' IS NOT NULL",
                    },
                    {
                        "old": r'\bAND\s+(?!t1\.)"(\w+)"\s+IS\s+NOT\s+NULL\b',
                        "new": rf'AND {parent_table_name}."\1" IS NOT NULL',
                    },
                    {
                        "old": r"\bcast\(`(\w+)`\s+as\b",
                        "new": rf"cast({parent_table_name}.`\1` as",
                    },
                    {
                        "old": r"\bcast\('(\w+)'\s+as\b",
                        "new": rf"cast({parent_table_name}.'\1' as",
                    },
                    {
                        "old": r"\bcast\((\w+)\s+as\b",
                        "new": rf"cast({parent_table_name}.\1 as",
                    },
                    {
                        "old": r'\bcast\("(\w+)"\s+as\b',
                        "new": rf'cast({parent_table_name}."\1" as',
                    },
                    {
                        "old": r"\bcast\(\s*(?!t1\.)\[(\w+)\]\s+as\b",
                        "new": rf"cast({parent_table_name}.[\1] as",
                    },
                    {
                        "old": r"\bWHEN\s+(?!t1\.)`(\w+)`\s+IS\s+NULL\b",
                        "new": rf"WHEN {parent_table_name}.`\1` IS NULL",
                    },
                    {
                        "old": r"\bWHEN\s+(?!t1\.)'(\w+)'\s+IS\s+NULL\b",
                        "new": rf"WHEN {parent_table_name}.'\1' IS NULL",
                    },
                    {
                        "old": r"\bWHEN\s+(?!t1\.)(\w+)\s+IS\s+NULL\b",
                        "new": rf"WHEN {parent_table_name}.\1 IS NULL",
                    },
                    {
                        "old": r"\bWHEN\s+(?!t1\.)\[(\w+)\]\s+IS\s+NULL\b",
                        "new": rf"WHEN {parent_table_name}.[\1] IS NULL",
                    },
                    {
                        "old": r'\bWHEN\s+(?!t1\.)"(\w+)"\s+IS\s+NULL\b',
                        "new": rf'WHEN {parent_table_name}."\1" IS NULL',
                    },
                    {
                        "old": r"\bCONVERT\(datetime,\s*(?!t1\.)\[(\w+)\]\)",
                        "new": rf"CONVERT(datetime, {parent_table_name}.[\1])",
                    },
                    {
                        "old": r"\bTRIM\(\s*(?!t1\.)\"(\w+)\"\)",
                        "new": rf'TRIM({parent_table_name}."\1")',
                    },
                    {
                        "old": r"\bTO_TIMESTAMP\(\s*(?!t1\.)`(\w+)`\)",
                        "new": rf"TO_TIMESTAMP({parent_table_name}.`\1`)",
                    },
                    {
                        "old": r"\bTO_TIMESTAMP\(\s*(?!t1\.)(\w+)\)",
                        "new": rf"TO_TIMESTAMP({parent_table_name}.\1)",
                    },
                    {
                        "old": r"\bTO_TIMESTAMP\(\s*(?!t1\.)'(\w+)'\)",
                        "new": rf"TO_TIMESTAMP({parent_table_name}.'\1')",
                    },
                    {
                        "old": r"\bTO_TIMESTAMP\(\s*(?!t1\.)\"(\w+)\"\)",
                        "new": rf"TO_TIMESTAMP({parent_table_name}.\"\1\")",
                    },
                    {
                        "old": r"\bSAFE_CAST\(\s*(?!t1\.)`(\w+)`\)",
                        "new": rf"SAFE_CAST({parent_table_name}.`\1`)",
                    },
                    {
                        "old": r"\b(IS\s+NULL\s+OR)\s+(?!t1\.)\"(\w+)\"\b",
                        "new": rf'\1 {parent_table_name}."\2"',
                    },
                ]

                for element in replace_value:
                    casequery = re.sub(
                        element.get("old"),
                        element.get("new"),
                        casequery,
                        flags=re.IGNORECASE,
                    )

                invalid_select_query = f"""{invalid_select_query[0]} case {casequery}"""
            else:
                invalid_select_query = " case ".join(invalid_select_query)
        return invalid_select_query
    except Exception as e:
        return invalid_select_query


def generate_lookup_source_large_datatype(config: dict):
    connection_type = config.get("connection_type", "").lower()
    if connection_type == ConnectionType.Snowflake.value:
        return f"""Text"""
    elif connection_type == ConnectionType.MSSQL.value:
        return f"""varchar(max)"""
    elif connection_type == ConnectionType.Synapse.value:
        return f"""VARCHAR(8000)"""
    if connection_type == ConnectionType.Athena.value:
        return f"""VARCHAR"""
    elif connection_type == ConnectionType.Redshift.value:
        return f"""VARCHAR(MAX)"""
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        return f"""VARCHAR(MAX)"""
    elif connection_type == ConnectionType.Oracle.value:
        return f"""VARCHAR2(4000)"""
    elif connection_type == ConnectionType.MySql.value:
        return f"""TEXT"""
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        return f"""Text"""
    elif connection_type == ConnectionType.BigQuery.value:
        return f"""string"""
    elif connection_type == ConnectionType.Denodo.value:
        return f"""text"""
    elif connection_type == ConnectionType.Databricks.value:
        return f"""string"""
    elif (connection_type == ConnectionType.Db2.value) or (
        connection_type == ConnectionType.DB2IBM.value
    ):
        return f"""CLOB"""
    elif (
        connection_type == ConnectionType.SapHana.value
        or connection_type == ConnectionType.Hive.value
    ):
        return f"""VARCHAR(5000)"""
    elif connection_type == ConnectionType.Teradata.value:
        return f"""varchar(5000)"""
    else:
        return f"""Text"""


def generate_lookup_source_datatype(config: dict):
    connection_type = config.get("connection_type", "").lower()
    if connection_type == ConnectionType.Snowflake.value:
        return f"""Text"""
    elif connection_type == ConnectionType.MSSQL.value:
        return f"""NVARCHAR(2000)"""
    elif connection_type == ConnectionType.Synapse.value:
        return f"""NVARCHAR(2000)"""
    if connection_type == ConnectionType.Athena.value:
        return f"""VARCHAR"""
    elif connection_type == ConnectionType.Redshift.value:
        return f"""VARCHAR(MAX)"""
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        return f"""VARCHAR(MAX)"""
    elif connection_type == ConnectionType.Oracle.value:
        return f"""VARCHAR2(2000)"""
    elif connection_type == ConnectionType.MySql.value:
        return f"""TEXT"""
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        return f"""Text"""
    elif connection_type == ConnectionType.BigQuery.value:
        return f"""string"""
    elif connection_type == ConnectionType.Denodo.value:
        return f"""text"""
    elif connection_type == ConnectionType.Databricks.value:
        return f"""string"""
    elif (connection_type == ConnectionType.Db2.value) or (
        connection_type == ConnectionType.DB2IBM.value
    ):
        return f"""VARCHAR(1000)"""
    elif (
        connection_type == ConnectionType.SapHana.value
        or connection_type == ConnectionType.Hive.value
    ):
        return f"""VARCHAR(1000)"""
    elif connection_type == ConnectionType.Teradata.value:
        return f"""varchar(5000)"""
    else:
        return f"""Text"""


def generate_lookup_query_connectionbased(config: dict, lookup_assets: dict):
    connection_type = config.get("connection_type", "").lower()
    if connection_type == ConnectionType.Snowflake.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'
    elif connection_type == ConnectionType.MSSQL.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f"[{db_name}].[{schema}].[{table}]"
    elif connection_type == ConnectionType.Synapse.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"[{schema}].[{table}]"
    if connection_type == ConnectionType.Athena.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'
    elif connection_type == ConnectionType.Redshift.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'
    elif connection_type == ConnectionType.Redshift_Spectrum.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'
    elif connection_type == ConnectionType.Oracle.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.MySql.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"{schema}.{table}"
    elif connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'
    elif connection_type == ConnectionType.MongoDB.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"{schema}.{table}"
    elif connection_type == ConnectionType.BigQuery.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"`{schema}`.`{table}`"
    elif connection_type == ConnectionType.Denodo.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.Databricks.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        return f"`{db_name}`.`{schema}`.`{table}`"
    elif (connection_type == ConnectionType.Db2.value) or (
        connection_type == ConnectionType.DB2IBM.value
    ):
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.SapHana.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.Teradata.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema.strip()}"."{table}"' if schema else f'"{table}"'
    elif connection_type == ConnectionType.Hive.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f"`{db_name}`.`{schema}`.`{table}`"
        return f"`{schema}`.`{table}`" if schema else f"`{table}`"
    elif connection_type == ConnectionType.SapEcc.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"{table}"
    elif connection_type == ConnectionType.Salesforce.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.SalesforceMarketing.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f'"{schema}"."{table}"'
    elif connection_type == ConnectionType.SalesforceDataCloud.value:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        return f"{table}"
    else:
        table = lookup_assets.get("lookup_table")
        schema = lookup_assets.get("profile_schema_name")
        db_name = lookup_assets.get("profile_database_name")
        if db_name:
            return f'"{db_name}"."{schema}"."{table}"'


def get_lookup_table_name(config: dict, name: str):
    connection_type = config.get("connection_type", "").lower()
    if (
        connection_type == ConnectionType.Databricks.value
        or connection_type == ConnectionType.Teradata.value
        or connection_type == ConnectionType.MySql.value
    ):
        name = name.lower()
        name = name.replace(" ", "_")
    return name


def get_lookup_profile_database(config: dict):
    profile_database_name = config.get("profile_database_name")
    profile_database_name = profile_database_name if profile_database_name else ""
    profile_schema_name = config.get("profile_schema_name")
    profile_schema_name = profile_schema_name if profile_schema_name else ""

    connection_details = config.get("connection")
    connection_details = connection_details if connection_details else {}
    asset = config.get("asset")
    asset = asset if asset else {}
    database = ""
    if connection_details:
        credentials = connection_details.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}
        database = credentials.get("database")
        database = database if database else ""
    properties = asset.get("properties", {})
    properties = json.loads(properties) if isinstance(properties, str) else properties
    properties = properties if properties else {}

    database_name = properties.get("database")
    database = database_name if database_name else database
    database = profile_database_name if profile_database_name else database
    schema = properties.get("schema")
    schema = profile_schema_name if profile_schema_name else schema
    base_table_name = config.get("table_name", "")

    if config.get("connection_type") == ConnectionType.Snowflake.value:
        base_table_name_value = base_table_name.split(".")
        if len(base_table_name_value) != 3:
            base_table_name = f""""{database}".{base_table_name}"""
    return database, schema, base_table_name


def check_lookup_process_details(
    config: dict, lookupMetadata: dict, database: str, schema: str
):
    librarie_upload_details = {}
    librarie_upload_updatedetails = {}
    psql_connection = get_postgres_connection(config)
    with psql_connection.cursor() as cursor:
        librarie_query = f"""
            select * from core.libraries_upload_details 
            where 
            libraries_id='{lookupMetadata.get("id", "")}' 
            and connection_id='{config.get("connection_id", "")}'
            and uploaddatabase='{database}'
            and uploadschema='{schema}'
            and is_updated=false
        """
        cursor = execute_query(psql_connection, cursor, librarie_query)
        librarie_upload_details = fetchone(cursor)

        librarie_query = f"""
            select * from core.libraries_upload_details 
            where 
            libraries_id='{lookupMetadata.get("id", "")}' 
            and connection_id='{config.get("connection_id", "")}'
            and uploaddatabase='{database}'
            and uploadschema='{schema}'
            and is_updated=true
        """
        cursor = execute_query(psql_connection, cursor, librarie_query)
        librarie_upload_updatedetails = fetchone(cursor)

    return librarie_upload_details, librarie_upload_updatedetails


def entry_after_lookup_process(
    config: dict,
    librarie_upload_details: dict,
    librarie_upload_updatedetails: dict,
    database: str,
    schema: str,
    lookup_id: str,
):
    psql_connection = get_postgres_connection(config)
    if not librarie_upload_details and not librarie_upload_updatedetails:
        with psql_connection.cursor() as cursor:
            lib_input = []
            query_input = (
                str(uuid4()),
                config.get("connection_id", ""),
                lookup_id,
                database,
                schema,
                False,
                False,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            lib_input.append(query_param)

            lib_input = split_queries(lib_input)

            for input_values in lib_input:
                try:

                    query_input = ",".join(input_values)
                    insert_query_string = f"""insert into core.libraries_upload_details(id,connection_id,libraries_id,
                                                uploaddatabase,uploadschema,is_updated,is_deleted,created_date,modified_date) 
                                                values {query_input}"""

                    cursor = execute_query(psql_connection, cursor, insert_query_string)

                except Exception as e:
                    log_error("insert libraries mapping ", e)
    else:
        if librarie_upload_updatedetails:
            with psql_connection.cursor() as cursor:
                update_query = f"""
                    update core.libraries_upload_details set is_updated=false where id='{librarie_upload_updatedetails.get('id')}'
                """
                cursor = execute_query(psql_connection, cursor, update_query)


def get_connection_details(config: dict, id: str):
    psql_connection = get_postgres_connection(config)
    connection_metadata = {}
    with psql_connection.cursor() as cursor:
        query = f"""
            select * from core.connection where id='{id}'
        """
        cursor = execute_query(psql_connection, cursor, query)
        connection_metadata = fetchone(cursor)
    return connection_metadata


def execute_lookup_table_measure(
    measure: dict, config: dict, default_queries: dict, lookup_metadata: dict
):
    try:
        status = PASSED
        core_connection_id = config.get("core_connection_id")
        lookup_properties = measure.get("properties", {})
        lookupMetadata = lookup_properties.get("lookupMetadata", {})
        lookup_id = lookupMetadata.get("id", "")
        asset = config.get("asset", {})
        asset = asset if asset else {}
        lookup_table_name = get_lookup_table_name(
            config,
            f"""lookup_{lookup_properties.get("lookupOption", "")}_{lookup_id.replace("-","")}""",
        )
        error_msg = f""""""

        lookup_table_properties = lookup_metadata.get("properties", {})
        lookup_tableConfig = lookup_table_properties.get("tableConfig", [])
        lookup_tableConfig_column = lookup_table_properties.get("columns", [])
        if lookup_tableConfig:
            lookup_tableConfig = lookup_tableConfig[0]
            lookup_tableConfig_connection = lookup_tableConfig.get("connection", [])
            if lookup_tableConfig_connection:
                lookup_tableConfig_connection = lookup_tableConfig_connection[0]
            lookup_tableConfig_assets = lookup_tableConfig.get("assets", [])
            if lookup_tableConfig_assets:
                lookup_tableConfig_assets = lookup_tableConfig_assets[0]

            if not lookup_tableConfig_connection or not lookup_tableConfig_assets:
                raise "Source Connection Config or Asset Config Missing"

            """
            common code for profiling database
            """
            database, schema, base_table_name = get_lookup_profile_database(config)
            librarie_upload_details = {}
            lookup_result_query = (
                    measure.get("semantic_query", {})
                    if not measure.get("query") or (
                        measure.get("type") == "semantic"
                        and measure.get("level") == "term"
                        and measure.get("category") == "lookup"
                    )
                    else measure.get("query")
            )

            if str(asset.get("view_type", "")).lower() == "direct query":
                if (
                    "<lookup_table_name>" not in lookup_result_query
                    and "<table_name>" not in lookup_result_query
                ):
                    librarie_upload_details = {"connection": "same"}
                else:
                    lookup_result_query = lookup_result_query.replace(
                        "<table_name>", ""
                    )
            else:
                if (
                    "<lookup_table_name>" not in lookup_result_query
                    and "<table_name>" not in lookup_result_query
                ):
                    librarie_upload_details = {"connection": "same"}

            if not librarie_upload_details:
                """
                Get Data from source table here
                """

                connection_config = get_connection_details(
                    config, lookup_tableConfig_connection.get("id")
                )
                if not connection_config:
                    error_msg = "Lookup Table Config Deleted Please check it."
                    raise Exception("Lookup Table Config Deleted Please check it.")

                check_lookup_table_isactive = connection_config.get("is_active")

                if not check_lookup_table_isactive:
                    error_msg = "Lookup Table Config Deactivate Please check it."
                    raise Exception("Lookup Table Config Deactivate Please check it.")

                table_credentials = connection_config.get("credentials")

                lookup_source_config = {
                    "core_connection_id": core_connection_id,
                    "connection_type": connection_config.get("type"),
                    "source_connection_id": connection_config.get(
                        "airflow_connection_id"
                    ),
                    "table_name": f""" "{table_credentials.get("database")}"."{lookup_tableConfig_assets.get("schema")}"."{lookup_tableConfig_assets.get("name")}" """,
                    "connection": connection_config,
                }

                column_list = ""
                static_col = ""

                source_datatype = generate_lookup_source_datatype(
                    {"connection_type": connection_config.get("type").lower()}
                )
                special_string = (
                    "`"
                    if connection_config.get("type").lower()
                    in [
                        ConnectionType.BigQuery.value,
                        ConnectionType.Databricks.value,
                        ConnectionType.MySql.value,
                        ConnectionType.Hive.value,
                    ]
                    else '"'
                )
                for col in lookup_tableConfig_column:
                    column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
                    static_col = (
                        f"""{static_col}{special_string}{col}{special_string} ,"""
                    )
                column_list = column_list[:-1]
                static_col = static_col[:-1]
                column_list = f"""{column_list}"""
                static_col = f"""{static_col}"""

                lookup_assets = {
                    "core_connection_id": core_connection_id,
                    "profile_schema_name": lookup_tableConfig_assets.get("schema"),
                    "profile_database_name": (
                        table_credentials.get("database")
                        if table_credentials.get("database")
                        else lookup_tableConfig_assets.get("database")
                    ),
                    "lookup_table": lookup_tableConfig_assets.get("name"),
                }
                lookup_full_table_name = generate_lookup_query_connectionbased(
                    {"connection_type": connection_config.get("type").lower()},
                    lookup_assets,
                )

                result_query = f"""select {static_col} from {lookup_full_table_name} group by {static_col}"""
                lookup_source_config.update({"sub_category": "LOOKUP"})
                result, _ = execute_native_query(
                    lookup_source_config,
                    result_query,
                    None,
                    False,
                    is_list=True,
                    convert_lower=False,
                    parameters= {"run_query": True}
                )

                special_string = (
                    "`"
                    if config.get("connection_type", "").lower()
                    in [
                        ConnectionType.BigQuery.value,
                        ConnectionType.Databricks.value,
                        ConnectionType.MySql.value,
                        ConnectionType.Hive.value,
                    ]
                    else '"'
                )

                df = pd.DataFrame.from_dict(result)
                df = df.replace(np.nan, "")
                df = df.drop_duplicates()

                column_list = ""
                static_col = ""

                source_datatype = generate_lookup_source_datatype(config)
                # iterating the columns
                for col in df.columns:
                    column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
                    static_col = (
                        f"""{static_col}{special_string}{col}{special_string} ,"""
                    )
                column_list = column_list[:-1]
                static_col = static_col[:-1]
                column_list = f"""{column_list}"""
                static_col = f"""{static_col}"""

                default_queries = get_queries(config)
                lookup_process_queries = default_queries.get("lookup_process", {})
                create_table_query = lookup_process_queries.get("create_table")
                insert_table_query = lookup_process_queries.get("insert_table")
                drop_table_query = lookup_process_queries.get("drop_table")
                is_exists = True

                if (
                    config.get("connection_type", "").lower()
                    == ConnectionType.Teradata.value
                ):
                    query_string = default_queries.get("failed_rows").get(
                        "metadata_table_exists"
                    )
                    query_string = query_string.replace("<db_name>", schema).replace(
                        "<table_name>", lookup_table_name
                    )
                    table_metadata, _ = execute_native_query(
                        config, query_string, None, True
                    )
                    is_exists = bool(table_metadata)
                if (
                    config.get("connection_type", "").lower()
                    in [
                        ConnectionType.Oracle.value,
                        ConnectionType.Db2.value,
                        ConnectionType.DB2IBM.value,
                        ConnectionType.Teradata.value,
                        ConnectionType.Redshift.value,
                        ConnectionType.Redshift_Spectrum.value,
                        ConnectionType.Hive.value,
                    ]
                    and is_exists
                ):
                    drop_table_query = (
                        drop_table_query.replace("<database_name>", database)
                        .replace("<schema_name>", schema)
                        .replace("<table_name>", f"""{lookup_table_name}""")
                    )
                    execute_native_query(
                        config, drop_table_query, None, True, no_response=True
                    )

                create_table_query = (
                    create_table_query.replace("<database_name>", database)
                    .replace("<schema_name>", schema)
                    .replace("<table_name>", f"""{lookup_table_name}""")
                    .replace("<query_string>", column_list)
                )
                config.update({"sub_category": "LOOKUP"})
                execute_native_query(
                    config, create_table_query, None, True, no_response=True
                )
                insert_query = (
                    insert_table_query.replace("<database_name>", database)
                    .replace("<schema_name>", schema)
                    .replace("<table_name>", f"""{lookup_table_name}""")
                    .replace("<columns>", static_col)
                )
                orginal_value = f""""""
                df_length = len(df)
                for index, row in df.iterrows():
                    index_value = index + 1
                    insert_value = "values"
                    if config.get("connection_type", "").lower() in [
                        ConnectionType.Synapse.value,
                        ConnectionType.Oracle.value,
                        ConnectionType.SapHana.value,
                    ]:
                        special_value = ""
                        final_value = "UNION ALL"
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.Oracle.value
                        ):
                            special_value = f"""FROM DUAL"""
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.SapHana.value
                        ):
                            special_value = f"""from dummy"""
                            final_value = "UNION"
                        string_list = [
                            f"""'{str(element)}'""" for element in row.values
                        ]
                        insert_value = ""
                        orginal_value = (
                            f"""{orginal_value} SELECT {str(string_list[0])} {special_value} {final_value}"""
                            if len(string_list) == 1
                            else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} {final_value}"""
                        )
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.SapHana.value
                        ):
                            orginal_value = f"""{orginal_value} ALL"""
                    else:
                        string_list = [
                            str(element).replace("'", "") for element in row.values
                        ]
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.Teradata.value
                        ):
                            query_part = ""
                            if len(string_list) == 1:
                                query_part = f"{insert_value} ('{string_list[0]}');"
                            else:
                                if str(tuple(string_list)) not in orginal_value:
                                    query_part = (
                                        f"{insert_value} {str(tuple(string_list))};"
                                    )
                            orginal_value = f"{orginal_value} {insert_query.replace('<insert_query>', query_part)}"
                        else:
                            orginal_value = (
                                f"""{orginal_value}('{str(string_list[0])}'),"""
                                if len(string_list) == 1
                                else f"""{orginal_value}{str(tuple(string_list))},"""
                            )
                    if df_length == index_value:
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.Teradata.value
                        ):
                            execute_native_query(
                                config, orginal_value, None, True, no_response=True
                            )
                        else:
                            orginal_value = (
                                orginal_value[:-1]
                                if insert_value
                                else orginal_value[:-9]
                            )
                            insert_data = insert_query.replace(
                                "<insert_query>", f"""{insert_value} {orginal_value}"""
                            )
                            execute_native_query(
                                config, insert_data, None, True, no_response=True
                            )
                        orginal_value = f""""""
                    elif index_value % 998 == 0:
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.Teradata.value
                        ):
                            execute_native_query(
                                config, orginal_value, None, True, no_response=True
                            )
                        else:
                            orginal_value = (
                                orginal_value[:-1]
                                if insert_value
                                else orginal_value[:-9]
                            )
                            insert_data = insert_query.replace(
                                "<insert_query>", f"""{insert_value} {orginal_value}"""
                            )
                            execute_native_query(
                                config, insert_data, None, True, no_response=True
                            )
                        orginal_value = f""""""

            """
            Exxecute Final Query of lookup
            """
            if (
                config.get("connection_type", "").lower()
                == ConnectionType.SalesforceDataCloud.value
            ):
                lookup_table_name = lookup_tableConfig_assets.get("name")
                
            lookup_assets = {
                "profile_schema_name": schema,
                "profile_database_name": database,
                "lookup_table": lookup_table_name,
            }
            lookup_full_table_name = generate_lookup_query_connectionbased(
                config, lookup_assets
            )

            count_query = f"""count(*) as count"""
            if (
                config.get("connection_type", "").lower()
                == ConnectionType.Teradata.value
            ):
                count_query = f"""count(*) as total_count"""

            lookup_result_query = (
                lookup_result_query.replace("<table_name>", base_table_name)
                .replace("<lookup_table_name>", lookup_full_table_name)
                .replace("<query_string>", count_query)
            )
            lookup_result_query = get_query_lookup_string(
                config, default_queries, lookup_result_query, is_full_query=True
            )
            config.update({"sub_category": "LOOKUP"})
            lookup_result_query = update_table_name_incremental_conditions(
                lookup_result_query
            )
            config.update({"query_string": lookup_result_query})
            measure_result, _ = execute_native_query(
                config, lookup_result_query, None, False, convert_lower=False
            )
            # entry_after_lookup_process(config, librarie_upload_details, librarie_upload_updatedetails, database, schema, lookup_id)
        else:
            error_msg = "Lookup Table Config Not Available."
            raise Exception("Lookup Table Config Not Available.")
    except Exception as e:
        measure_result = {}
        status = FAILED
        error_msg = str(e)
        log_error(f"Failed on execute measure : {str(e)}", e)
    finally:
        return status, measure_result, config, error_msg


def execute_lookup_measure(measure: dict, config: dict, default_queries: dict):
    """
    Executes the given measure for the given attribute
    """
    try:
        status = PASSED
        measure_result = {}
        lookup_properties = measure.get("properties", {})
        asset = config.get("asset", {})
        asset = asset if asset else {}
        lookupMetadata = lookup_properties.get("lookupMetadata", {})
        lookup_id = lookupMetadata.get("id", "")
        lookup_table_name = get_lookup_table_name(
            config,
            f"""lookup_{lookup_properties.get("lookupOption", "")}_{lookup_id.replace("-","")}""",
        )
        temp_lookup_table_name = f"""lookup_{lookup_properties.get("lookupOption", "")}_{str(uuid4()).replace("-","")}"""
        connection = get_postgres_connection(config)
        lookup_metadata = {}
        config.update({"sub_category": "LOOKUP"})
        with connection.cursor() as cursor:
            query = f"""
                select type, source, "authorization", throttle_limit, delimiter, properties from core.libraries where id='{lookup_id}' and is_active=true limit 1
            """
            cursor = execute_query(connection, cursor, query)
            lookup_metadata = fetchone(cursor)

        if lookup_metadata:
            configured_lookup_type = lookup_metadata.get("type")
            if configured_lookup_type == "TABLE":
                status, measure_result, config, error_msg = (
                    execute_lookup_table_measure(
                        measure, config, default_queries, lookup_metadata
                    )
                )
                if status == FAILED:
                    raise Exception(f"""{str(error_msg)}""")
            elif configured_lookup_type == "FILE":
                s3_path = lookup_metadata.get("properties")
                s3_path = s3_path.get("s3_file_url", "")
                file_extension = s3_path.split(".")[-1].lower()
                general_settings = config.get("dag_info", {}).get("settings")
                storage_settings = (
                    general_settings.get("storage") if general_settings else {}
                )
                storage_settings = (
                    json.loads(storage_settings)
                    if storage_settings and isinstance(storage_settings, str)
                    else storage_settings
                )
                storage_settings = storage_settings if storage_settings else {}

                storage = storage_settings.get("storage")
                storage_config = storage if storage else {}
                storage_type = storage_config.get("type")
                storage_type = str(storage_type).lower() if storage_type else "s3"

                if os.name.lower() == "nt":
                    local_path = f"""{temp_lookup_table_name}.{file_extension}"""
                else:
                    local_path = f"""{os.getenv("HOME")}/{temp_lookup_table_name}.{file_extension}"""

                """
                Profile Database Details Here 
                """
                database, schema, base_table_name = get_lookup_profile_database(config)
                lookup_result_query = (
                    measure.get("semantic_query", {})
                    if not measure.get("query") or (
                        measure.get("type") == "semantic"
                        and measure.get("level") == "term"
                        and measure.get("category") == "lookup"
                    )
                    else measure.get("query")
)

                librarie_upload_details, librarie_upload_updatedetails = (
                    check_lookup_process_details(
                        config, lookupMetadata, database, schema
                    )
                )

                if s3_path:
                    """
                    Download File From S3
                    """
                    if not librarie_upload_details:
                        storage_service = get_storage_service(config.get("dag_info"))
                        storage_service.download_file(
                            s3_path, config.get("dag_info"), local_path
                        )

                        default_queries = get_queries(config)
                        lookup_process_queries = default_queries.get(
                            "lookup_process", {}
                        )
                        create_table_query = lookup_process_queries.get("create_table")
                        insert_table_query = lookup_process_queries.get("insert_table")
                        drop_table_query = lookup_process_queries.get("drop_table")

                        column_list = ""
                        static_col = ""
                        if file_extension == "xlsx":
                            try:
                                df = pd.read_excel(local_path, engine="openpyxl")
                            except Exception as e:
                                df = pd.read_excel(local_path)
                        elif file_extension == "xls":
                            df = pd.read_excel(local_path)
                        elif file_extension == "tsv":
                            df = pd.read_csv(local_path, sep="\t")
                        elif file_extension == "csv" or file_extension == "txt":
                            default_encoding = "utf-8"
                            delimiter_result = UniversalDetector()
                            delimiter_result.reset()
                            with open(local_path, "rb") as csv_file:
                                _ = csv_file.readline()
                                data_line = csv_file.readline()
                                delimiter_result.feed(data_line)
                                delimiter_result.close()
                            delimiter_result = (
                                delimiter_result.result
                                if delimiter_result.result
                                else {}
                            )
                            encoding_type = delimiter_result.get(
                                "encoding", default_encoding
                            )
                            encoding_type = (
                                encoding_type if encoding_type else default_encoding
                            )
                            delimiter = detect(data_line.decode(encoding_type))
                            delimiter = delimiter if delimiter else ","
                            if (
                                lookup_metadata.get("delimiter")
                                and lookup_metadata.get("delimiter") != delimiter
                            ):
                                delimiter = lookup_metadata.get("delimiter")
                            try:
                                df = pd.read_csv(
                                    local_path,
                                    sep=delimiter,
                                    skip_blank_lines=False,
                                    dtype=str,
                                    escapechar="\\",
                                    encoding=default_encoding,
                                )
                            except:
                                try:
                                    df = pd.read_csv(
                                        local_path,
                                        sep=delimiter,
                                        skip_blank_lines=False,
                                        dtype=str,
                                        escapechar="\\",
                                        encoding=encoding_type,
                                    )
                                except:
                                    df = pd.read_csv(
                                        local_path,
                                        sep=delimiter,
                                        skip_blank_lines=False,
                                        dtype=str,
                                        escapechar="\\",
                                        encoding="ISO-8859-1",
                                    )
                        df = df.replace(np.nan, "")
                        df = df.drop_duplicates()
                        source_datatype = generate_lookup_source_datatype(config)
                        special_string = (
                            "`"
                            if config.get("connection_type", "").lower()
                            in [
                                ConnectionType.BigQuery.value,
                                ConnectionType.Databricks.value,
                                ConnectionType.MySql.value,
                                ConnectionType.Hive.value,
                            ]
                            else '"'
                        )
                        # iterating the columns
                        for col in df.columns:
                            column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
                            static_col = f"""{static_col}{special_string}{col}{special_string} ,"""
                        column_list = column_list[:-1]
                        static_col = static_col[:-1]
                        column_list = f"""{column_list}"""
                        static_col = f"""{static_col}"""
                        is_exists = True
                        if (
                            config.get("connection_type", "").lower()
                            == ConnectionType.Teradata.value
                        ):
                            query_string = default_queries.get("failed_rows").get(
                                "metadata_table_exists"
                            )
                            query_string = query_string.replace(
                                "<db_name>", schema
                            ).replace("<table_name>", lookup_table_name)
                            table_metadata, _ = execute_native_query(
                                config, query_string, None, True
                            )
                            is_exists = bool(table_metadata)

                        if (
                            config.get("connection_type", "").lower()
                            in [
                                ConnectionType.Oracle.value,
                                ConnectionType.Db2.value,
                                ConnectionType.DB2IBM.value,
                                ConnectionType.Teradata.value,
                                ConnectionType.Redshift.value,
                                ConnectionType.Hive.value,
                            ]
                            and is_exists
                        ):
                            drop_table_query = (
                                drop_table_query.replace("<database_name>", database)
                                .replace("<schema_name>", schema)
                                .replace("<table_name>", f"""{lookup_table_name}""")
                            )
                            execute_native_query(
                                config, drop_table_query, None, True, no_response=True
                            )

                        create_table_query = (
                            create_table_query.replace("<database_name>", database)
                            .replace("<schema_name>", schema)
                            .replace("<table_name>", f"""{lookup_table_name}""")
                            .replace("<query_string>", column_list)
                        )
                        execute_native_query(
                            config, create_table_query, None, True, no_response=True
                        )
                        insert_query = (
                            insert_table_query.replace("<database_name>", database)
                            .replace("<schema_name>", schema)
                            .replace("<table_name>", f"""{lookup_table_name}""")
                            .replace("<columns>", static_col)
                        )
                        orginal_value = f""""""
                        df_length = len(df)
                        for index, row in df.iterrows():
                            index_value = index + 1
                            insert_value = "values"
                            if config.get("connection_type", "").lower() in [
                                ConnectionType.Synapse.value,
                                ConnectionType.Oracle.value,
                                ConnectionType.SapHana.value,
                            ]:
                                special_value = ""
                                final_value = "UNION ALL"
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.Oracle.value
                                ):
                                    special_value = f"""FROM DUAL"""
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.SapHana.value
                                ):
                                    special_value = f"""from dummy"""
                                    final_value = "UNION"
                                string_list = [f"""'{str(value).replace("'", "''")}'""" if value and str(value).strip() != '' else f"""NULL""" for value in row.values]
                                insert_value = ""
                                orginal_value = (
                                    f"""{orginal_value} SELECT {str(string_list[0])} {special_value} {final_value}"""
                                    if len(string_list) == 1
                                    else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} {final_value}"""
                                )
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.SapHana.value
                                ):
                                    orginal_value = f"""{orginal_value} ALL"""
                            else:
                                string_list = [
                                    str(element).replace("'", "")
                                    for element in row.values
                                ]
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.Teradata.value
                                ):
                                    query_part = ""
                                    if len(string_list) == 1:
                                        query_part = (
                                            f"{insert_value} ('{string_list[0]}');"
                                        )
                                    else:
                                        if str(tuple(string_list)) not in orginal_value:
                                            query_part = f"{insert_value} {str(tuple(string_list))};"
                                    orginal_value = f"{orginal_value} {insert_query.replace('<insert_query>', query_part)}"
                                else:
                                    orginal_value = (
                                        f"""{orginal_value}('{str(string_list[0])}'),"""
                                        if len(string_list) == 1
                                        else f"""{orginal_value}{str(tuple(string_list))},"""
                                    )
                            if df_length == index_value:
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.Teradata.value
                                ):
                                    execute_native_query(
                                        config,
                                        orginal_value,
                                        None,
                                        True,
                                        no_response=True,
                                    )
                                else:
                                    orginal_value = (
                                        orginal_value[:-1]
                                        if insert_value
                                        else orginal_value[:-9]
                                    )
                                    insert_data = insert_query.replace(
                                        "<insert_query>",
                                        f"""{insert_value} {orginal_value}""",
                                    )
                                    execute_native_query(
                                        config,
                                        insert_data,
                                        None,
                                        True,
                                        no_response=True,
                                    )
                                orginal_value = f""""""
                            elif index_value % 998 == 0:
                                if (
                                    config.get("connection_type", "").lower()
                                    == ConnectionType.Teradata.value
                                ):
                                    execute_native_query(
                                        config,
                                        orginal_value,
                                        None,
                                        True,
                                        no_response=True,
                                    )
                                else:
                                    orginal_value = (
                                        orginal_value[:-1]
                                        if insert_value
                                        else orginal_value[:-9]
                                    )
                                    insert_data = insert_query.replace(
                                        "<insert_query>",
                                        f"""{insert_value} {orginal_value}""",
                                    )
                                    execute_native_query(
                                        config,
                                        insert_data,
                                        None,
                                        True,
                                        no_response=True,
                                    )
                                orginal_value = f""""""
                    lookup_assets = {
                        "profile_schema_name": schema,
                        "profile_database_name": database,
                        "lookup_table": lookup_table_name,
                    }
                    lookup_full_table_name = generate_lookup_query_connectionbased(
                        config, lookup_assets
                    )
                    count_query = f"""count(*) as count"""
                    if (
                        config.get("connection_type", "").lower()
                        == ConnectionType.Teradata.value
                    ):
                        count_query = f"""count(*) as total_count"""
                    if str(asset.get("view_type", "")).lower() == "direct query":
                        lookup_result_query = lookup_result_query.replace(
                            "<table_name>", ""
                        )
                    lookup_result_query = (
                        lookup_result_query.replace("<table_name>", base_table_name)
                        .replace("<lookup_table_name>", lookup_full_table_name)
                        .replace("<query_string>", count_query)
                    )
                    lookup_result_query = get_query_lookup_string(
                        config, default_queries, lookup_result_query, is_full_query=True
                    )
                    lookup_result_query = update_table_name_incremental_conditions(
                        lookup_result_query
                    )
                    config.update({"query_string": lookup_result_query})
                    measure_result, _ = execute_native_query(
                        config, lookup_result_query, None, False, convert_lower=False
                    )

                    entry_after_lookup_process(
                        config,
                        librarie_upload_details,
                        librarie_upload_updatedetails,
                        database,
                        schema,
                        lookup_id,
                    )
            elif configured_lookup_type == "API":
                apidata:APIDataValidator = APIDataValidator(config, lookup_metadata, measure)
                measure_result = apidata.run_validation()
        else:
            raise "Lookup Library Is Missing"
    except Exception as e:
        measure_result = {}
        status = FAILED
        log_error(f"Failed on execute measure : {str(e)}", e)
        raise e
    finally:
        try:
            if not librarie_upload_details:
                os.remove(local_path)
        except Exception as e:
            pass
        save_measure(status, measure_result, config)
