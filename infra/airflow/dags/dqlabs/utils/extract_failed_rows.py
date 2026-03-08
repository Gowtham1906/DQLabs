"""
Migration Notes From V2 to V3:
Migrations Completed
"""

import json
import ast
import re
import time
import os
from copy import deepcopy
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
    execute_native_query,
    get_query_lookup_string,
)
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    get_database_name,
    get_derived_type,
    parse_teradata_columns,
)
from dqlabs.app_helper.log_helper import log_error
from dqlabs.utils import get_last_runs
from dqlabs.utils.extract_workflow import (
    get_attribute_metadata,
    get_range_value,
    get_queries,
)
from dqlabs.app_helper.connection_helper import get_attribute_names
from dqlabs.app_constants.dq_constants import (
    DEFAULT_MAX_RUNS_TO_STORE,
    DEFAULT_MAX_RUNS_TO_STORE_TYPE,
    FAILED_ROWS_METADATA_TABLES,
    MEASURE_METADATA,
    EXPORT_GROUP_FAILED_ROWS_TABLES,
    REPORT_EXPORT_GROUP_INDIVIDUAL,
    REPORT_EXPORT_GROUP_MEASURES,
    REPORT_EXPORT_GROUP_ASSET_MEASURES,
    REPORT_EXPORT_GROUP_DOMAINS,
)
from dqlabs.utils import get_general_settings
from dqlabs.utils.lookup_process import (
    get_lookup_profile_database,
    generate_lookup_query_connectionbased,
    generate_lookup_source_datatype,
    update_table_name_incremental_conditions,
)
from dqlabs.utils.connections import get_dq_connections_database
from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query
from dqlabs.app_helper.dag_helper import get_incremental_query_string
from dqlabs.app_helper.agent_helper import get_vault_data
from dqlabs.app_helper.crypto_helper import decrypt


def get_failed_rows_table_name(config: dict):
    """
    Returns the table for the given asset
    """
    table_name = ""
    level = config.get("level")
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    asset_alias_name = asset.get("alias")
    asset_alias_name = asset_alias_name if asset_alias_name else ""
    asset_name = asset.get("technical_name")
    asset_name = asset_name if asset_name else ""
    measure = config.get("measure")
    measure = measure if measure else {}
    measure_id = measure.get("id")
    measure_name = measure.get("name")
    measure_name = measure_name if measure_name else ""
    measure_alias_name = measure.get("alias")
    measure_alias_name = measure_alias_name if measure_alias_name else ""

    name = asset_alias_name if asset_alias_name else asset_name
    if level == "measure":
        name = measure_alias_name if measure_alias_name else measure_name
    if name and len(name) > 25:
        name = name[:20]
    id = measure_id if level == "measure" else asset_id
    failed_rows_table = f"""{name}_{id}"""
    table_name = str(re.sub("[^A-Za-z0-9]", "_", failed_rows_table)).lower()
    return table_name


def get_failed_rows_tablename_by_export_group(
    config: dict, export_group: str, level: str
):
    """
    Get the failed rows table name based on export group
    """
    table_name = ""
    if export_group == REPORT_EXPORT_GROUP_MEASURES and level == "measure":
        table_name = EXPORT_GROUP_FAILED_ROWS_TABLES[0]
    elif export_group == REPORT_EXPORT_GROUP_ASSET_MEASURES:
        if level == "measure":
            table_name = EXPORT_GROUP_FAILED_ROWS_TABLES[0]
        else:
            table_name = EXPORT_GROUP_FAILED_ROWS_TABLES[1]
    elif export_group == REPORT_EXPORT_GROUP_DOMAINS:
        metadata = config.get("metadata")
        metadata = metadata if metadata else {}
        domains = metadata.get("domains")
        domains = (
            json.loads(domains) if domains and isinstance(domains, str) else domains
        )
        domains = domains if domains else []
        domains = list(sorted(domains, key=lambda d: d["order"]))
        domains = [domain for domain in domains if domain.get("name")]
        domain = domains[0] if domains else ""
        if domain:
            domain_name = domain.get("name") if domain else ""
            domain_name = (
                str(re.sub("[^A-Za-z0-9]", "_", domain_name)) if domain_name else ""
            )
            domain_table_name = str(domain_name).upper()
            if domain_table_name and len(domain_table_name) > 35:
                domain_table_name = domain_table_name[:25]
            table_name = f"DOMAIN_{domain_table_name}_DATA"

        if not table_name:
            if level == "measure":
                table_name = EXPORT_GROUP_FAILED_ROWS_TABLES[0]
            else:
                table_name = EXPORT_GROUP_FAILED_ROWS_TABLES[1]
    return table_name


def get_measures(config: dict, exception_outlier: bool = False):
    """
    Get measure for failed rows
    """
    level = config.get("level")
    asset_id = config.get("asset_id")
    export_category = config.get("export_category")
    attribute_id = None
    measure_id = None
    if str(level).lower() == "attribute":
        attributes = config.get("attributes")
        attributes = attributes if attributes else []
        attributes = [attribute.get("id") for attribute in attributes if attribute]
        attribute_id = attributes[0] if attributes else None
    elif str(level).lower() == "measure":
        attribute_id = None
        asset_id = None
        measure_id = config.get("measure_id")

    filter_queries = []
    if asset_id:
        filter_queries.append(f" mes.asset_id='{asset_id}' ")
    if attribute_id:
        filter_queries.append(f" mes.attribute_id='{attribute_id}' ")
    if measure_id:
        filter_queries.append(f" mes.id='{measure_id}' ")
    if export_category == "custom":
        # Include only regular custom/semantic measures for all connections (including Snowflake)
        # Constraint-based measures (null_count, distinct_count) are excluded from custom category
        filter_queries.append(
            f" base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup', 'parameter', 'cross_source') "
        )
    elif export_category == "auto":
        # Include only default measures, exclude constraint-based measures for all connections
        filter_queries.append(f" base.is_default = True AND base.technical_name NOT IN ('null_count', 'distinct_count') ")
    
    if exception_outlier:
        filter_queries.append(f" mes.is_positive = false and  base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup', 'parameter', 'cross_source')")

    measures = []
    if str(level).lower() != "measure" and not asset_id:
        return measures

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        filter_query = " and ".join(filter_queries)
        filter_query = f" and {filter_query}" if filter_query else ""
        query_string = f"""
            select mes.*, base.type, base.category, base.query, base.properties, base.is_default, base.level, base.term_id, base.name as measure_name
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.allow_score=True and lower(mes.status)='verified'
            and (mes.is_aggregation_query=False or (mes.is_aggregation_query=True and mes.has_failed_rows_query=True)) and mes.is_export = True and mes.is_delete = False and mes.is_active = True
            and mes.last_run_date is not null
            {filter_query}
        """
        cursor = execute_query(connection, cursor, query_string)
        measures = fetchall(cursor)
    return measures


def is_schema_exists(config: dict, schema_query: str):
    """
    Check is the schema exists or not in the destination database
    """
    destination_connection_object = config.get("destination_connection_object")
    destination_config = config.get("destination_config", {})
    connection_type = destination_config.get("connection_type")
    connection_type = connection_type.lower() if connection_type else ""
    schema_name = config.get("failed_rows_schema")
    database = config.get("destination_database")
    source_connection = None
    if connection_type in [
        ConnectionType.Teradata.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Hive.value,
    ]:
        return True

    if (
        connection_type == ConnectionType.Databricks.value
        and database == "hive_metastore"
    ):
        hive_queries = get_queries({**config, "connection_type": connection_type})
        schema_query = (
            hive_queries.get("hive_metastore", {})
            .get("failed_rows", "")
            .get("schema", "")
        )
        connection_config = destination_config if destination_config else config
        schema_result, _ = execute_native_query(
            connection_config, schema_query, source_connection, is_list=True
        )
        schema_result = convert_to_lower(schema_result)
        has_schema = any(
            obj.get("databasename") == schema_name for obj in schema_result
        )
        return has_schema

    has_schema = len(schema_query.strip()) == 0
    if schema_query and schema_name:
        current_database = database
        if not current_database:
            if destination_connection_object:
                connection = destination_connection_object
            connection_credentials = connection.get("credentials")
            connection_credentials = (
                connection_credentials if connection_credentials else {}
            )
            current_database = connection_credentials.get("database")
            current_database = f"{current_database}" if current_database else ""

        if connection_type == ConnectionType.Snowflake.value and not current_database:
            destination_connection_database = get_dq_connections_database(
                config, connection.get("id")
            )
            current_database = destination_connection_database.get("name")

        if current_database:
            schema_query = (
                schema_query.replace("<db_name>", str(current_database).lower())
                .replace("<schema_name>", schema_name.lower())
                .replace("<database_name>", current_database)
            )
            try:
                if connection_type == ConnectionType.Snowflake.value:
                    destination_config = {**destination_config, "database_name": current_database}
                connection_config = destination_config if destination_config else config
                schema_result, _ = execute_native_query(
                    connection_config, schema_query, source_connection
                )
                schema_result = schema_result if schema_result else {}
                if schema_result:
                    schema_result = convert_to_lower(schema_result)
                schema_count = schema_result.get("schema_count") if schema_result else 0
                schema_count = schema_count if schema_count else 0
                has_schema = schema_count > 0
            except Exception as error:
                log_error("No schema found", error)
                has_schema = False
    return has_schema


def is_table_exists(
    config: dict,
    queries: dict,
    table_name: str,
    destination_connection=None,
    database=None,
    schema=None,
):
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

    if database and schema:
        database_name = database
        schema_name = schema

    metadata_table_exists_query = queries.get("metadata_table_exists")
    if metadata_table_exists_query and connection_type in [
        ConnectionType.Teradata.value,
        ConnectionType.DB2IBM.value,
    ]:
        query_string = (
            metadata_table_exists_query.replace("<table_name>", table_name)
            .replace("<schema_name>", schema_name)
            .replace("<db_name>", database_name)
        )
        metadata_table, native_connection = execute_native_query(
            destination_config, query_string, destination_connection
        )
        if native_connection:
            destination_connection = native_connection
        is_exists = bool(metadata_table)
    return is_exists, destination_connection


def create_base_tables(config: dict, queries: dict, is_metadata: bool = False):
    """
    Create base tables for storing failed rows
    """
    connection_type = config.get("connection_type")
    destination_config = config.get("destination_config")
    create_failed_rows_table = queries.get("create_failed_rows_table")
    if not create_failed_rows_table:
        raise Exception("Cannot save invalid rows for this connector")

    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    if (
        destination_config.get("connection_type") == ConnectionType.DB2IBM.value
        and not database_name
    ):
        database_name = "sample"

    failed_rows_table = config.get("failed_rows_table")
    destination_connection = None
    if is_metadata:
        for metadata_table in FAILED_ROWS_METADATA_TABLES:
            key = f"create_{metadata_table.lower()}"
            metadata_table_query = queries.get(key)
            if not metadata_table_query:
                continue

            metadata_table_query = (
                metadata_table_query.replace("<schema_name>", schema_name)
                .replace("<failed_rows_table>", failed_rows_table)
                .replace("<database_name>.", database_name)
            )
            is_exists, destination_connection = is_table_exists(
                config, queries, metadata_table, destination_connection
            )
            if not is_exists:
                _, native_connection = execute_native_query(
                    destination_config,
                    metadata_table_query,
                    destination_connection,
                    True,
                    no_response=True,
                )
                if not destination_connection and native_connection:
                    destination_connection = native_connection
        metadata_params = {
        "destination_config": destination_config,
        "queries": queries,
        "destination_connection": destination_connection,
        }
        # update custom fields
        update_custom_field_columns(config, metadata_params)
        update_new_metadata_columns(config, metadata_params)
    else:
        create_failed_rows_table = (
            create_failed_rows_table.replace("<schema_name>", schema_name)
            .replace("<failed_rows_table>", failed_rows_table)
            .replace("<database_name>.", database_name)
        )
        is_exists, destination_connection = is_table_exists(
            config, queries, failed_rows_table, destination_connection
        )
        if not is_exists:
            execute_native_query(
                destination_config,
                create_failed_rows_table,
                destination_connection,
                True,
                no_response=True,
            )


def get_failed_row_query(config: dict, measure: dict, default_queries: dict, is_direct_query_asset: bool = False):
    """
    Returns failed rows query for a given measure
    """
    query_string = ""
    failed_rows_metadata = {}
    has_failed_rows_query = False
    pattern_query = ""
    try:
        failed_row_queries = default_queries.get("failed_rows", {})
        failed_row_queries = failed_row_queries if failed_row_queries else {}
        if not failed_row_queries:
            return query_string, failed_rows_metadata, has_failed_rows_query

        asset = config.get("asset", {})
        asset = asset if asset else {}
        primary_attributes = asset.get("primary_columns", [])
        primary_attributes = (
            json.loads(primary_attributes, default=str)
            if isinstance(primary_attributes, str)
            else primary_attributes
        )
        primary_attributes = primary_attributes if primary_attributes else []
        connection_type = config.get("connection", {}).get("type")

        input_values = []
        category = measure.get("category", "")
        category = category.lower() if category else ""
        attribute_id = measure.get("attribute_id")
        measure_type = measure.get("type", "")
        measure_name = measure.get("technical_name")
        level = config.get("level")
        metadata = {}
        if attribute_id:
            metadata = get_attribute_metadata(config, attribute_id)
            metadata = metadata if metadata else {}

        if measure_type.lower() == "frequency":
            attribute = config.get("attribute")
            attribute = attribute if attribute else {}
            if category == "pattern":
                is_positive = measure and measure.get("is_positive")
                is_auto_discovered_patterns = measure_name in [
                    "short_pattern",
                    "long_pattern",
                ]
                is_short_pattern = False
                patterns = metadata.get("user_defined_patterns", [])
                if measure_name == "short_pattern":
                    is_short_pattern = True
                    patterns = metadata.get("short_universal_patterns", [])
                elif measure_name == "long_pattern":
                    patterns = metadata.get("universal_patterns", [])
                patterns = patterns if patterns else []
                patterns = (
                    json.loads(patterns, default=str)
                    if isinstance(patterns, str)
                    else patterns
                )
                patterns = patterns if patterns else []
                valid_patterns = [
                    pattern
                    for pattern in patterns
                    if pattern and pattern.get("is_valid") == (not is_positive)
                ]
                has_no_patterns = not valid_patterns
                valid_patterns = patterns if not valid_patterns else valid_patterns
                for pattern in valid_patterns:
                    pattern.update(
                        {
                            "auto_discovered_pattern": is_auto_discovered_patterns,
                            "is_short_pattern": is_short_pattern,
                            "is_positive_measure": is_positive,
                            "measure_name": measure_name,
                        }
                    )
                pattern_query = prepare_patterns_query(
                    connection_type,
                    valid_patterns,
                    default_queries,
                    is_positive,
                    has_no_patterns,
                )
                pattern_query = pattern_query if pattern_query else ""
                input_values = [pattern_query] if pattern_query else []
            elif category == "length":
                min_length = metadata.get("min_length")
                max_length = metadata.get("max_length")
                min_length = get_range_value(str(min_length)) if min_length else None
                max_length = get_range_value(str(max_length)) if max_length else None
                input_values = [str(min_length), str(max_length)]
                if min_length is None or max_length is None:
                    input_values = []
            elif category == "enum":
                enums = metadata.get("value_distribution", [])
                enums = enums if enums else []
                enums = (
                    json.loads(enums, default=str) if isinstance(enums, str) else enums
                )
                enums = enums if enums else []
                escape_character = "'"
                enum_values = [
                    f"""'{enum.get("enum_value").replace(escape_character, "")}'"""
                    for enum in enums
                    if enum and enum.get("is_valid")
                ]
                enum_value = ", ".join(enum_values)
                if "'NULL'" in enum_value:
                    enum_value = enum_value.replace("'NULL'", "'DQ_NULL'")
                elif "'E'" in enum_value:
                    enum_value = enum_value.replace("'E'", "''")
                enum_value = f"({enum_value})" if enum_value else ""
                input_values = [enum_value] if enum_value else []
            elif category == "range":
                value1, value2 = None, None
                if measure_name == "length_range":
                    min_length = metadata.get("min_length")
                    max_length = metadata.get("max_length")
                    value1 = (
                        get_range_value(str(min_length))
                        if min_length is not None
                        else None
                    )
                    value2 = (
                        get_range_value(str(max_length))
                        if max_length is not None
                        else None
                    )

                if measure_name == "value_range":
                    min_value = metadata.get("min_value")
                    max_value = metadata.get("max_value")
                    value1 = (
                        get_range_value(str(min_value))
                        if min_value is not None
                        else None
                    )
                    value2 = (
                        get_range_value(str(max_value))
                        if max_value is not None
                        else None
                    )

                input_values = [str(value1), str(value2)]
                if value1 is None or value2 is None:
                    input_values = []
        if category == "pattern" and not pattern_query:
            return pattern_query, failed_rows_metadata, has_failed_rows_query

        table_name = (
            config.get("table_name")
            if connection_type != ConnectionType.S3Select.value
            else "s3object s"
        )
        measure_input = {
            "table_name": table_name,
            "schema": config.get("schema"),
            "primary_attributes": primary_attributes,
            "input_values": input_values,
            "connection_type": connection_type,
            "attribute": metadata,
            "asset": config.get("asset"),
            "connection": config.get("connection"),
            "config": config,
        }
        query_string, failed_rows_metadata, has_failed_rows_query = (
            __prepare_query_string(measure, measure_input, failed_row_queries)
        )

        if category == "query" and level in ["asset", "attribute"] and not is_direct_query_asset:
            query_string = get_incremental_query_string(config, default_queries, query_string, category=category)

        if category not in ["query", "parameter"] and not has_failed_rows_query:
            if query_string:
                query_string = get_query_lookup_string(
                    config, default_queries, query_string, is_full_query=True, category=category
                )

        if measure_name in ("distinct_count", "duplicate"):
            if is_direct_query_asset and connection_type in [ConnectionType.Databricks.value, ConnectionType.Redshift.value, ConnectionType.Redshift_Spectrum.value, ConnectionType.Athena.value]:
                query_string = query_string.replace(
                    "<distinct_alias_attribute>", ""
                ).replace("<distinct_alias>", "")
            else :
                query_string = query_string.replace(
                    "<distinct_alias_attribute>", "t."
                ).replace("<distinct_alias>", "t")
    except Exception as e:
        log_error("get_failed_row_query", e)
    finally:
        return query_string, failed_rows_metadata, has_failed_rows_query


def convert_to_regex_pattern(pattern: str, is_sql: bool, is_short: bool = False, is_athena: bool = False):
    if pattern in ["NULL", "E", ""]:
        pattern = ""
        return pattern
        
    # Athena-specific optimizations
    if is_athena:
        return _optimize_pattern_for_athena(pattern, is_short)

    pattern_delimiter = "+" if is_short else ""
    if is_sql:
        pattern_delimiter = "%" if is_short else ""

    special_characters = re.findall("[^A-Z]", pattern)
    special_characters = list(set(special_characters)) if special_characters else []
    for char in special_characters:
        if char in ["[", "]", "(", ")"]:
            pattern = pattern.replace(char, f"[\\{char}]")
        else:
            pattern = pattern.replace(char, f"[{char}]")
    pattern = (
        pattern.replace("A", f"[A-Za-z]{pattern_delimiter}")
        .replace("N", f"[0-9]{pattern_delimiter}")
        .replace("S", f"[ ]{pattern_delimiter}")
    )

    if is_sql:
        pattern = pattern.replace("Q", f"""[\\\"''"]{pattern_delimiter}""")
    else:
        pattern = pattern.replace("Q", f"""[''""]{pattern_delimiter}""")

    return pattern


def _optimize_pattern_for_athena(pattern: str, is_short: bool = False) -> str:
    """
    Optimize regex patterns specifically for Athena to prevent query length issues
    """
    if not pattern:
        return pattern
        
    # For UUID-like patterns, use a simplified generic UUID regex
    if _is_uuid_like_pattern(pattern):
        return r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    
    # For other patterns, use simplified character classes
    pattern_delimiter = "+" if is_short else ""
    
    # Simplified pattern conversion for Athena
    optimized_pattern = (
        pattern.replace("[", r"\[")
        .replace("]", r"\]")
        .replace("(", r"\(")
        .replace(")", r"\)")
        .replace("A", f"[A-Za-z]{pattern_delimiter}")
        .replace("N", f"[0-9]{pattern_delimiter}")
        .replace("S", f"[ ]{pattern_delimiter}")
        .replace("Q", f"['\"]")  # Simplified quote pattern
    )
    
    # Ensure pattern has anchors
    if not optimized_pattern.startswith("^"):
        optimized_pattern = f"^{optimized_pattern}"
    if not optimized_pattern.endswith("$"):
        optimized_pattern = f"{optimized_pattern}$"
        
    return optimized_pattern


def _is_uuid_like_pattern(pattern: str) -> bool:
    """
    Check if pattern looks like a UUID pattern
    """
    if not pattern:
        return False
    # Check for UUID-like patterns with multiple character classes and hyphens
    uuid_indicators = ['-', 'N', 'A']
    return (len(pattern) > 20 and 
            all(indicator in pattern for indicator in uuid_indicators) and
            pattern.count('-') >= 3)


def _optimize_patterns_for_athena(patterns: list) -> list:
    """
    Group and optimize similar patterns for Athena
    """
    if not patterns:
        return patterns
        
    # Group patterns by type
    uuid_patterns = []
    other_patterns = []
    
    for pattern_measure in patterns:
        pattern = pattern_measure.get("pattern", "")
        if _is_uuid_like_pattern(str(pattern)):
            uuid_patterns.append(pattern_measure)
        else:
            other_patterns.append(pattern_measure)
    
    # Keep only one representative UUID pattern and top other patterns
    optimized = []
    if uuid_patterns:
        # Use the first UUID pattern as representative
        uuid_rep = uuid_patterns[0].copy()
        uuid_rep["pattern"] = "NNNNNNN-NNNN-NNNN-NNNN-NNNNNNNNNNNN"  # Generic UUID pattern
        optimized.append(uuid_rep)
    
    # Add up to 3 other patterns
    optimized.extend(other_patterns[:3])
    
    return optimized


def prepare_patterns_query(
    connection_type: str,
    patterns: list,
    defaul_queries: dict,
    is_positive: bool,
    has_no_patterns: bool,
):
    queries = []
    failed_rows_queries = defaul_queries.get("failed_rows")
    failed_rows_queries = failed_rows_queries if failed_rows_queries else {}
    attribute_queries = failed_rows_queries.get("attribute", {})

    pattern_condition = attribute_queries.get("pattern_condition")
    null_pattern_condition = attribute_queries.get("null_pattern_condition")
    empty_pattern_condition = attribute_queries.get("empty_pattern_condition")
    space_pattern_condition = attribute_queries.get("space_pattern_condition")

    null_or_empty_patterns = []
    is_not_empty = False
    is_not_null = False
    patterns_query = ""
    measure_name = ""
    
    # Athena-specific optimization: limit pattern count and optimize regex
    is_athena = connection_type and connection_type.lower() == ConnectionType.Athena.value
    max_patterns = 5 if is_athena else len(patterns)  # Limit patterns for Athena
    
    # Group similar patterns for optimization
    if is_athena and len(patterns) > max_patterns:
        patterns = _optimize_patterns_for_athena(patterns[:max_patterns])
    
    if(connection_type and (connection_type.lower() == (ConnectionType.SalesforceMarketing.value) or connection_type.lower() == (ConnectionType.Salesforce.value))):
        return patterns_query
    for pattern_measure in patterns[:max_patterns]:
        is_auto_discovered = pattern_measure.get("auto_discovered_pattern", False)
        is_short_pattern = pattern_measure.get("is_short_pattern", False)
        pattern = pattern_measure.get("pattern")
        is_valid = pattern_measure.get("is_valid")
        if not measure_name:
            measure_name = pattern_measure.get("measure_name")
        not_condition = ""
        if not is_positive:
            not_condition = "" if is_valid else "NOT"
        else:
            not_condition = "" if not is_valid else "NOT"
        pattern = pattern if pattern else ""
        pattern_query = None

        if is_auto_discovered and pattern:
            if pattern == "NULL":
                pattern_query = deepcopy(null_pattern_condition)
                pattern_query = pattern_query.replace("<not_condition>", not_condition)
                is_not_null = bool(not_condition)
                null_or_empty_patterns.append(f"({pattern_query})")
                continue
            elif pattern == "E":
                pattern_query = deepcopy(empty_pattern_condition)
                pattern_query = pattern_query.replace("<not_condition>", not_condition)
                is_not_empty = bool(not_condition)
                null_or_empty_patterns.append(f"({pattern_query})")
                continue
            elif re.match("^[S]+$", pattern) and space_pattern_condition:
                pattern_query = deepcopy(empty_pattern_condition)
                pattern_query = pattern_query.replace("<not_condition>", not_condition)
                is_not_empty = bool(not_condition)
                null_or_empty_patterns.append(f"({pattern_query})")
                continue

        is_sql = (
            connection_type and connection_type.lower() == (ConnectionType.MSSQL.value)
        ) or (
            connection_type
            and connection_type.lower() == (ConnectionType.Synapse.value)
        )
        if is_sql:
            input_pattern = {}
            if is_auto_discovered:
                input_pattern_value = convert_to_regex_pattern(
                    pattern, is_sql, is_short_pattern, is_athena
                )
                input_pattern = {
                    "connector": "and",
                    "not": False,
                    "rules": [
                        {
                            "values": [input_pattern_value],
                            "operator": {"id": "match", "label": "matches"},
                        }
                    ],
                }
            else:
                input_pattern = pattern.get("sql_pattern")
            pattern_query = __process_sql_pattern_rule_group(
                defaul_queries, input_pattern, pattern_query
            )
            if pattern_query:
                queries.append(f"{not_condition} ({pattern_query})")
        else:
            if is_auto_discovered:
                pattern_query = convert_to_regex_pattern(
                    pattern, is_sql, is_short_pattern, is_athena
                )
            else:
                pattern_query = pattern.get("posix_pattern")
            condition = deepcopy(pattern_condition)
            condition = condition.replace("<value1>", pattern_query)
            if condition:
                queries.append(f"({not_condition} {condition})")

    if queries or null_or_empty_patterns:
        conditional_operator = "and" if (is_not_null or is_not_empty) else "or"
        empty_query = f" {conditional_operator} ".join(null_or_empty_patterns)
        pattern_condition = "or" if not has_no_patterns else "and"
        patterns_query = f" {pattern_condition} ".join(queries)
        patterns_query = f"({patterns_query})" if patterns_query else ""
        if empty_query:
            conditional_operator = "and" if ("not" in empty_query.lower()) else "or"
            patterns_query = (
                f"{conditional_operator} {patterns_query}" if patterns_query else ""
            )
            patterns_query = f"{empty_query} {patterns_query}"
    return patterns_query


def __process_sql_pattern_rule_group(
    query_config: dict, cRule: dict, query: str
) -> str:
    """
    Prepares pattern Rule Group Query
    """
    custom_rule_query = query_config.get("custom_rule_query")
    ruleQuery = ""
    connector = cRule.get("connector", None)
    not_condition = cRule.get("not", False)
    rules = cRule.get("rules", [])

    if connector and len(rules) > 0:
        connector_query = custom_rule_query.get(connector, None)
        query_condition = []
        if connector_query:
            for rule in rules:
                rule_condition_query = __process_sql_pattern_rule(
                    query_config, rule, query
                )
                if rule_condition_query:
                    query_condition.append(rule_condition_query)

            if len(query_condition) > 0:
                ruleQuery = f" {connector_query} ".join(query_condition)
    if query:
        ruleQuery = f"""{query} {ruleQuery}"""

    if not_condition:
        not_query = custom_rule_query.get("not", None)
        ruleQuery = f"""{not_query} ({ruleQuery})"""
    return ruleQuery


def __process_sql_pattern_rule(query_config: dict, cRule: dict, query: str) -> str:
    """
    Prepares Pattern Rule Query
    """
    conditionQuery = None
    custom_rule_query = query_config.get("custom_rule_query")
    connector = cRule.get("connector", None)
    rules = cRule.get("rules", [])
    operator = cRule.get("operator", None)
    values = cRule.get("values", [])
    ruleQuery = None

    if connector and len(rules) > 0:
        ruleQuery = __process_sql_pattern_rule_group(query_config, cRule, query)

    if operator:
        operator_id = operator.get("id", "")
        conditionQuery = custom_rule_query.get(operator_id, None)
        if conditionQuery:
            if len(values) > 0:
                for index, value in enumerate(values):
                    conditionQuery = conditionQuery.replace(
                        f"""<value{index+1}>""", value
                    )
            query = f"""{conditionQuery}"""
    if ruleQuery:
        query = f"""( {ruleQuery} )"""
    return query


def __prepare_query_string(
    measure: dict, measure_input: dict, failed_row_queries: dict
):
    """
    Prepare the failed rows query
    """
    query_string = ""
    failed_rows_metadata = {}
    has_failed_rows_query = False
    config = measure_input.get("config")
    config = config if config else {}
    is_same_source = config.get("is_same_source")
    queue_id = config.get("queue_id")
    queue_id = queue_id if queue_id else None
    connection_id = config.get("connection_id")
    connection_id = connection_id if connection_id else None
    delimiter = config.get("delimiter")
    delimiter = delimiter if delimiter else "|"
    asset_id = config.get("asset_id")
    asset_id = asset_id if asset_id else None
    attribute_id = measure.get("attribute_id")
    attribute_id = attribute_id if attribute_id else None
    measure_id = measure.get("id")
    measure_id = measure_id if measure_id else None
    hash_key_single = failed_row_queries.get("hash_key_single")
    # update for length and value range
    threshold_constraints = {}
    threshold_data = {}
    threshold_format = "invalid_dict"
    threshold_constraints = measure.get("threshold_constraints", {})
    range_condition = ""
    input_values = measure_input.get("input_values", [])
    input_values = input_values if input_values else []
    # measure_to_skip_values = ["distinct_count", "duplicate_count", "duplicate"]
    dq_connection = config.get("connection")
    dq_connection = dq_connection if dq_connection else {}

    attribute = measure_input.get("attribute")
    attribute = attribute if attribute else {}
    connection = measure_input.get("connection")
    connection = connection if connection else {}
    connection_type = measure_input.get("connection_type")
    asset = measure_input.get("asset")
    asset = asset if asset else {}
    asset_name = asset.get("technical_name")

    table_name = measure_input.get("table_name")
    table_name = table_name if table_name else ""

    if str(asset.get("view_type", "")).lower() == "direct query":
        table_name = f"""({asset.get("query")}) as direct_query_table"""
        if connection_type and str(connection_type).lower() in [
            ConnectionType.Oracle.value.lower(),
            ConnectionType.BigQuery.value.lower(),
        ]:
            table_name = f"""({asset.get("query")})"""

    attributes = measure_input.get("primary_attributes")
    
    # For SAP ECC, use column_name instead of name
    if connection_type == ConnectionType.SapEcc.value:
        identifier_attributes = [attribute.get("name") or attribute.get("column_name") for attribute in attributes]
    else:
        identifier_attributes = [attribute.get("name") for attribute in attributes]
    identifier_attributes = identifier_attributes if identifier_attributes else []
    is_positive = measure.get("is_positive", False)
    if connection_type == ConnectionType.SapEcc.value:
        attribute_name = (attribute.get("name") or attribute.get("column_name")) if attribute else ""
    else:
        attribute_name = attribute.get("name") if attribute else ""
    attribute_name = attribute_name if attribute_name else ""
    attribute_names = []
    if attribute_name:
        attribute_names = get_attribute_names(connection_type, [attribute_name])

    measure_name = measure.get("technical_name")
    measure_type = measure.get("type")
    measure_category = measure.get("category", "")
    measure_category = measure_category if measure_category else ""
    level = measure.get("level")
    is_asset_level = level == "asset"
    not_condition = "NOT" if is_positive else ""
    if measure_name in ["length_range", "value_range"]:
        not_condition = "NOT" if not is_positive else ""

    if measure_category and measure_category.lower() == "pattern":
        not_condition = ""

    if is_asset_level:
        attribute_name = ""
        query_string = ""
        if measure_name == "duplicate_count":
            query_string = failed_row_queries.get("asset").get(measure_name)
        elif measure_type in ["custom", "semantic"]:
            connection_type = config.get("connection_type")
            query_string, has_failed_rows_query = __get_custom_failed_query(
                measure, failed_row_queries, asset_name, connection_type
            )
    elif level == "measure":
        connection_type = config.get("connection_type")
        query_string, has_failed_rows_query = __get_custom_failed_query(
            measure, failed_row_queries, asset_name, connection_type
        )
    else:
        if not attribute_name:
            return query_string, failed_rows_metadata, has_failed_rows_query

        attribute_queries = failed_row_queries.get("attribute")
        query_type = measure_name
        if measure_type == "frequency":
            query_type = (
                measure_name if measure_category == "range" else measure_category
            )
            query_string = attribute_queries.get(query_type)
        elif measure_name == "null_count":
            query_string = attribute_queries.get(measure_name)
            not_condition = "" if not is_positive else "NOT"
            query_string = query_string.replace("<not_condition>", not_condition)
        elif measure_name == "zero_values":
            query_string = attribute_queries.get(measure_name)
            not_condition = "" if not is_positive else "NOT"
            zero_values_negate_condition = attribute_queries.get(
                "zero_values_negate_condition"
            )
            negate_condition = zero_values_negate_condition if not_condition else ""
            query_string = query_string.replace(
                "<not_condition>", not_condition
            ).replace("<negate_condition>", negate_condition)
        elif measure_type in ["custom", "semantic"]:
            connection_type = config.get("connection_type")
            query_string, has_failed_rows_query = __get_custom_failed_query(
                measure, failed_row_queries, asset_name, connection_type
            )
        else:
            query_string = attribute_queries.get(query_type)

    query_string = query_string if query_string else ""
    primary_attributes = ""
    alias_attributes = ""
    partition_by = ""
    hashkey = ""
    key_attributes = []
    if attributes:

        
        # For SAP ECC, use column_name instead of name
        if connection_type == ConnectionType.SapEcc.value:
            primary_attributes = [attribute.get("name") or attribute.get("column_name") for attribute in attributes]
        else:
            primary_attributes = [attribute.get("name") for attribute in attributes]
        primary_attributes = get_attribute_names(connection_type, primary_attributes)
        if len(attributes) == 1:
            hashkey = primary_attributes[0]
            key_attributes = [hashkey]
            if hash_key_single:
                hashkey = hash_key_single.replace("<attribute>", hashkey)
        elif len(attributes) > 1:
            key_attributes = deepcopy(primary_attributes)
            hash_key_query = failed_row_queries.get("hash_key")
            if hash_key_query:
                hash_key_attributes = ", ".join(primary_attributes)
                hash_key_query = hash_key_query.replace(
                    "<attributes>", hash_key_attributes
                )
            hashkey = hash_key_query

        if attribute_names:
            source_attribute_name = attribute_names[0]
            if source_attribute_name not in primary_attributes:
                primary_attributes.append(source_attribute_name)
            if source_attribute_name not in key_attributes:
                key_attributes.append(source_attribute_name)

        primary_attributes = ", ".join(primary_attributes)
        partition_by = primary_attributes

    attribute_id = attribute_id if not is_asset_level and attribute_id else None
    failed_attributes = key_attributes
    status = "VALID" if (measure_category in ["query", "parameter"] and is_positive) else "INVALID"
    status = "INVALID" if has_failed_rows_query else status
    failed_rows_metadata = {
        "connection_id": connection_id,
        "connection_name": connection.get("name") if connection else "",
        "asset_id": asset_id,
        "asset_name": asset.get("name") if asset else "",
        "attribute_id": attribute_id,
        "attribute_name": (attribute.get("name") or attribute.get("column_name")) if attribute and connection_type == ConnectionType.SapEcc.value else (attribute.get("name") if attribute else ""),
        "measure_id": measure_id,
        "measure_name": measure.get("measure_name") if measure else "",
        "run_id": queue_id,
        "measure_condition": status,
        "domains": "",
        "products": "",
        "applications": "",
        "terms": "",
        "tags": "",
        "exportrow_id": "",
        "measure_data": "",
        "failed_row_data": "",
        "is_summarised": "",
        "created_date": "",
    }

    failed_attributes = ", ".join(failed_attributes)
    failed_attributes = failed_attributes if failed_attributes else "*"
    if query_string and measure_type.lower() == "frequency":
        value1, value2 = "", ""
        if input_values:
            if len(input_values) > 1:
                value1, value2 = input_values[0], input_values[1]
            else:
                value1, value2 = input_values[0], ""

        if (measure_category in ["pattern", "enum"] and not value1) or (
            measure_category not in ["pattern", "enum"] and not (value1 and value2)
        ):
            query_string = ""

        # Passing the exact string with the escape characters preserved to avoid special character query failure
        if (
            connection_type.lower() == ConnectionType.Databricks.value.lower()
            and "\\\\" in value1
        ):
            value1 = repr(value1).strip('"').strip("'")
        
        if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
            try:
                if isinstance(value1, str) and value1.strip().startswith('(') and value1.strip().endswith(')'):
                    tuple_value = ast.literal_eval(value1)
                    if isinstance(tuple_value, tuple):
                        value1 = json.dumps(list(tuple_value))
                    elif isinstance(tuple_value, list):
                        value1 = json.dumps(tuple_value)
            except (ValueError, SyntaxError, TypeError):
                pass

        query_string = query_string.replace("<value1>", value1).replace(
            "<value2>", value2
        )

    primary_attributes = primary_attributes if primary_attributes else "*"
    if is_same_source:
        failed_attributes = f"<metadata_attributes> <query_attributes>"

    if attribute_id and asset_id and ((measure_name == "duplicate") or (measure_category and measure_category.lower() not in ['query', 'conditional'])):
        query_string = prepare_json_attribute_flatten_query(
            config,
            attribute_id,
            query_string,
            "sub_query"
        )
        
    query_string = (
        query_string.replace("<table_name>", table_name)
        .replace("<attribute>", attribute_name)
        .replace("<alias_attributes>", alias_attributes)
        .replace("<not_condition>", not_condition)
        .replace("<partition_by>", partition_by)
        .replace("<attributes>", primary_attributes)
        .replace("<hash_key>", hashkey)
        .replace("<failed_attributes>", failed_attributes)
        .replace("<limit_condition>", "")
        .replace("<not_null_condition>", "NOT" if not not_condition else "")
        .replace("<range_condition>", range_condition)
    )
    return query_string, failed_rows_metadata, has_failed_rows_query


def __cast_value(connection_type: str, attributename: str):
    if connection_type and connection_type.lower() == ConnectionType.BigQuery.value:
        attributename = f""" CAST("{attributename}" as STRING)"""
    return attributename


def __get_custom_failed_query(
    measure: dict, failed_row_queries: dict, asset_name: str, connection_type: str = None
) -> str:
    has_failed_rows_query = False
    measure_type = measure.get("type")
    is_positive = measure.get("is_positive", False)
    measure_category = measure.get("category", "")
    query_string = ""
    if measure_type == "custom":
        query_string = measure.get("query")
    elif measure_type == "semantic":
        query_string = measure.get("semantic_query")
    query_string = query_string if query_string else ""

    if query_string and connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
        if measure_category in ["conditional", "lookup"]:
            try:
                pipeline = json.loads(query_string) if isinstance(query_string, str) else query_string
                if isinstance(pipeline, list):
                    filtered_pipeline = [
                        stage for stage in pipeline
                        if not (isinstance(stage, dict) and "$count" in stage)
                    ]
                    
                    should_invert = False
                    if is_positive:
                        should_invert = True
                    
                    if should_invert:
                        for i, stage in enumerate(filtered_pipeline):
                            if isinstance(stage, dict) and "$match" in stage:
                                match_condition = stage["$match"]
                                if isinstance(match_condition, dict):
                                    inverted_match = {}
                                    for field, condition in match_condition.items():
                                        if field.startswith("$"):  # $expr or other operators
                                            inverted_match[field] = {"$not": condition}
                                        else:
                                            if isinstance(condition, dict):
                                                if measure_category == "lookup" and "$exists" in condition and condition["$exists"] is True:
                                                    inverted_match[field] = {"$exists": False}
                                                else:
                                                    inverted_match[field] = {"$not": condition}
                                            else:
                                                inverted_match[field] = {"$ne": condition}
                                    filtered_pipeline[i] = {"$match": inverted_match}
                    
                    # Convert back to JSON string
                    query_string = json.dumps(filtered_pipeline)
            except (json.JSONDecodeError, TypeError):
                # If not valid JSON, leave as is
                pass
    if query_string and measure_category == "conditional":
        if connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower():
            pass
        else:
            queries = re.split("( from )", query_string, flags=re.IGNORECASE)
            failed_query_to_replace = "SELECT <failed_attributes>"
            if queries:
                queries[0] = failed_query_to_replace
            query_string = " ".join(queries)
            where_filter = " WHERE "
            if "<incremental_filter>" in query_string:
                where_filter = " WHERE <incremental_filter> "
            constraints = query_string.split(where_filter)
            constraints[-1] = f"{where_filter} <not_condition> ({constraints[-1]})"
            query_string = " ".join(constraints)

    measure_properties = measure.get("properties")
    measure_properties = (
        json.loads(measure_properties)
        if isinstance(measure_properties, str)
        else measure_properties
    )
    measure_properties = measure_properties if measure_properties else {}
    input_params = measure_properties.get("input_params", {})
    if query_string and measure_category == "parameter" and input_params:
        for key, value in input_params.items():
            parameter = f"<{key}>"
            if isinstance(value, list):
                formatted_values = ", ".join(f"'{v}'" for v in value)
                replacement = f"({formatted_values})"
            else:
                replacement = f"('{value}')"
            
            query_string = query_string.replace(parameter, replacement)
    failed_rows_query = measure_properties.get("failed_rows_query")
    failed_rows_query = failed_rows_query if failed_rows_query else {}
    is_enabled = failed_rows_query.get("is_enabled", False)
    query = failed_rows_query.get("query")
    if is_enabled and query:
        has_failed_rows_query = True
        query_string = query

    is_aggregation_query = measure.get("is_aggregation_query")
    if not is_positive and not (is_enabled and query) and is_aggregation_query:
        has_failed_rows_query = False
        query_string = ""

    query_string = (
        query_string.replace(";", "") if query_string.endswith(";") else query_string
    )
    return query_string, has_failed_rows_query


def refresh_failed_rows(
    config: dict,
    failed_row_queries: dict,
    source_connection: object,
    destination_config: object,
):
    """
    Refresh the failed rows and keeps the minimum no.of data
    """
    failed_rows_table = config.get("failed_rows_table")
    report_settings = config.get("report_settings")
    if not report_settings and "dag_info" in config:
        dag_info = config
        if "dag_info" in config:
            dag_info = config.get("dag_info")
            dag_info = dag_info if dag_info else {}

        general_settings = dag_info.get("settings", {})
        general_settings = general_settings if general_settings else {}
        general_settings = (
            json.loads(general_settings, default=str)
            if isinstance(general_settings, str)
            else general_settings
        )

        report_settings = general_settings.get("reporting")
        report_settings = report_settings if report_settings else {}
        if report_settings:
            config.update({"report_settings": report_settings})

    report_settings = (
        json.loads(report_settings, default=str)
        if report_settings and isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}
    max_runs_to_store = report_settings.get(
        "max_store_period_limit", DEFAULT_MAX_RUNS_TO_STORE
    )
    max_runs_to_store = (
        int(max_runs_to_store) if max_runs_to_store else DEFAULT_MAX_RUNS_TO_STORE
    )
    max_runs_to_store_type = report_settings.get(
        "max_store_period_type", DEFAULT_MAX_RUNS_TO_STORE_TYPE
    )
    max_runs_to_store_type = (
        max_runs_to_store_type
        if max_runs_to_store_type
        else DEFAULT_MAX_RUNS_TO_STORE_TYPE
    )

    if isinstance(max_runs_to_store, str):
        max_runs_to_store = int(max_runs_to_store)
        max_runs_to_store = (
            max_runs_to_store if max_runs_to_store else DEFAULT_MAX_RUNS_TO_STORE
        )

    level = config.get("level")
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    measure = config.get("measure")
    measure = measure if measure else {}
    measure_id = measure.get("id")
    attribute = config.get("attribute")
    attribute = attribute if attribute else {}
    attribute_id = attribute.get("id")
    if level == "measure":
        asset_id = None
        attribute_id = None
        measure_id = config.get("measure_id")
    else:
        measure_id = None

    connection = get_postgres_connection(config)
    last_runs = get_last_runs(
        connection,
        asset_id,
        limit=max_runs_to_store,
        limit_type=max_runs_to_store_type,
        attribute_id=attribute_id,
        measure_id=measure_id,
    )
    last_runs = [last_runs] if last_runs and isinstance(last_runs, dict) else last_runs
    last_runs = last_runs if last_runs else []

    run_ids = ""
    if last_runs:
        last_runs = list(map(lambda run: f"""'{run.get("run_id")}'""", last_runs))
        run_ids = ", ".join(last_runs)
        run_ids = f"{run_ids}"

    if not run_ids:
        return

    connection_type = config.get("connection_type")
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"

    delete_history_query = failed_row_queries.get("delete_history")
    query_string = (
        deepcopy(delete_history_query)
        .replace("<run_ids>", run_ids)
        .replace("<schema_name>", schema_name)
        .replace("<failed_rows_table>", failed_rows_table)
        .replace("<database_name>.", database_name)
    )
    delete_filters = []
    if level == "measure":
        delete_filters.append(f""" MEASURE_ID='{measure_id}' """)
    elif level == "asset":
        delete_filters.append(f""" ASSET_ID='{asset_id}' """)
    elif level == "attribute":
        delete_filters.append(f""" ATTRIBUTE_ID='{attribute_id}' """)
    delete_filter_query = " and ".join(delete_filters)
    delete_filter_query = f" and {delete_filter_query}" if delete_filter_query else ""
    query_string = (
        f"{query_string} {delete_filter_query}" if delete_filter_query else query_string
    )
    execute_native_query(
        destination_config, query_string, source_connection, True, no_response=True
    )


def update_failed_rows_stat(config: dict):
    """
    Update failed rows statistics for the given asset
    """
    level = config.get("level")
    level = str(level).lower() if level else ""
    asset = config.get("asset")
    asset_id = asset.get("id") if asset else None
    attribute_id = None
    if (level != "measure" and not (asset_id)) or (level == "measure"):
        return

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        # Prepare failed rows count for asset level
        asset_valid_invalid_query = f"""
            select max(case when mes.is_active and mes.allow_score then (case when mes.is_positive then mes.invalid_rows else mes.valid_rows end) else 0 end) as invalid_rows,
            max(mes.row_count) - max(case when mes.is_active and mes.allow_score then (case when mes.is_positive then mes.invalid_rows else mes.valid_rows end) else 0 end) as valid_rows
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            where mes.asset_id = '{asset_id}' 
        """
        cursor = execute_query(connection, cursor, asset_valid_invalid_query)
        asset_invalid_records = fetchone(cursor)

        query_string = f"""
            update core.data set invalid_rows=0, valid_rows=0 where asset_id='{asset_id}'
        """
        execute_query(connection, cursor, query_string)
        if asset_invalid_records:
            invalid_rows = asset_invalid_records.get("invalid_rows")
            invalid_rows = int(invalid_rows) if invalid_rows else 0
            valid_rows = asset_invalid_records.get("valid_rows")
            valid_rows = int(valid_rows) if valid_rows else 0

            query_string = f"""
                update core.data set invalid_rows={invalid_rows}, valid_rows={valid_rows}
                where asset_id='{asset_id}'
            """
            execute_query(connection, cursor, query_string)

        # Prepare failed rows count for attribute level

        attribute_invalid_count_query = f"""
            select mes.attribute_id,
                max(case when mes.is_active and mes.allow_score then (case when mes.is_positive then mes.invalid_rows else mes.valid_rows end) else 0 end) as invalid_rows,
                max(mes.row_count) - max(case when mes.is_active and mes.allow_score then (case when mes.is_positive then mes.invalid_rows else mes.valid_rows end) else 0 end) as valid_rows
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            join core.attribute on attribute.id=mes.attribute_id
            where mes.asset_id = '{asset_id}' and mes.attribute_id is not null
            group by mes.attribute_id
        """
        cursor = execute_query(connection, cursor, attribute_invalid_count_query)
        attribute_invalid_records = fetchall(cursor)
        # reset invalid rows into 0 for the current run
        query_string = f"""
            update core.attribute set invalid_rows=0,valid_rows=0
            where asset_id='{asset_id}'
        """
        execute_query(connection, cursor, query_string)

        for row in attribute_invalid_records:
            attribute_id = row.get("attribute_id")
            if not attribute_id or attribute_id == "None":
                continue
            invalid_rows = row.get("invalid_rows")
            invalid_rows = int(invalid_rows) if invalid_rows else 0
            valid_rows = row.get("valid_rows")
            valid_rows = int(valid_rows) if valid_rows else 0

            query_string = f"""
                update core.attribute
                set invalid_rows={invalid_rows}, valid_rows={valid_rows}
                where id='{attribute_id}'
            """
            execute_query(connection, cursor, query_string)


def update_failed_rows_table_name(
    config: dict,
    table_name: str,
    is_reset: bool,
    export_group: str = REPORT_EXPORT_GROUP_INDIVIDUAL,
):
    """
    Update failed rows table name to the given asset
    """
    if not table_name:
        return
    level = config.get("level")

    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.data set failed_rows_table='{table_name}', reset_failed_rows_table={is_reset}
            where asset_id='{asset_id}'
        """
        if export_group == REPORT_EXPORT_GROUP_DOMAINS:
            query_string = f"""
                update core.data set domain_failed_rows_table='{table_name}'
                where asset_id='{asset_id}'
            """

        if level == "measure":
            measure_id = config.get("measure_id")
            query_string = f"""
                update core.measure set failed_rows_table='{table_name}', reset_failed_rows_table={is_reset}
                where id='{measure_id}'
            """
            if export_group == REPORT_EXPORT_GROUP_DOMAINS:
                query_string = f"""
                    update core.measure set domain_failed_rows_table='{table_name}'
                    where id='{measure_id}'
                """
        execute_query(connection, cursor, query_string)


def insert_connection_metadata(config: dict, queries: dict, destination_connection):
    """
    Insert the connection metadata for the current run
    """
    connection = config.get("connection")
    current_date = queries.get("current_date")
    current_date = str(current_date.split("AS")[0]).strip()
    queue_id = config.get("queue_id")

    connection_psql = get_postgres_connection(config)
    with connection_psql.cursor() as cursor:
        query_string = f"""
            select id, name, type, is_valid, created_date
            from core.connection
            where id='{connection.get("id")}' and is_delete='false' and is_default='false'
        """
        cursor = execute_query(connection_psql, cursor, query_string)
        connection_metadata_info = fetchone(cursor)
        connection_metadata_info = (
            connection_metadata_info if connection_metadata_info else {}
        )

    created_date = connection_metadata_info.get("created_date")
    created_date = created_date if created_date else "NULL"
    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.MSSQL.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Db2.value,
        ConnectionType.DB2IBM.value,
        ConnectionType.Synapse.value,
    ]:
        created_date = str(created_date).split(".")[0].strip()

    if (
        config.get("destination_connection_object").get("type").lower()
        == ConnectionType.Oracle.value
        and str(created_date) != "NULL"
    ):
        created_date = (
            f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
            if str(created_date) != "NULL"
            else "NULL"
        )
    else:
        created_date = f"'{created_date}'" if str(created_date) != "NULL" else "NULL"

    connection_metadata = [
        f"""'{connection.get("id")}'""",
        f"""'{connection.get("name")}'""",
        f"""'{connection.get("type")}'""",
        "'Valid'" if connection.get("is_valid") else "'Deprecated'",
        f"'{queue_id}'",
        created_date,
    ]
    insert_connection_metadata = queries.get("insert_connection_metadata")
    destination_connection = execute_failed_rows_metadata_query(
        config, connection_metadata, insert_connection_metadata, destination_connection
    )
    return destination_connection


def insert_asset_metadata(
    config: dict, queries: dict, destination_connection, destination_queries
):
    """
    Insert the connection metadata for the current run
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    dest_conn_type = config.get("destination_connection_object").get("type").lower()
    default_column = [
        "CONNECTION_ID",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "ASSET_ID",
        "TABLE_NAME",
        "ALIAS_NAME",
        "DQ_SCORE",
        "TOTAL_RECORDS",
        "PASSED_RECORDS",
        "FAILED_RECORDS",
        "ISSUE_COUNT",
        "ALERT_COUNT",
        "STATUS",
        "IDENTIFIER_KEY",
        "RUN_ID",
        "CONVERSATIONS",
        "DESCRIPTION",
        "STEWARD_USER_ID",
        "STEWARD_USERS",
        "DOMAIN",
        "PRODUCT",
        "LAST_RUN_TIME",
        "APPLICATIONS",
        "TERMS",
        "CREATED_DATE",
    ]

    if not asset_id:
        return

    metadata = {}
    last_job_detail = {}
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with comments as (
        select conversations.asset_id as asset_id,conversations.id as conversation_id, comment,conversations.title,conversations.created_date as created_at, rating,
        case when users.first_name is not null then concat(users.first_name,' ', users.last_name) else users.email end as username
        from core.conversations 
        join core.users on users.id=conversations.created_by_id
        where conversations.asset_id='{asset_id}' and conversations.level='asset'
      )
        select con.id as connection_id, con.name as connection_name, con.type as connection_type,
            con.is_valid, con.credentials, ast.status, ast.created_date,
            ast.id as asset_id, ast.name as asset_name, ast.properties, 
            ast.score as asset_score, data.row_count as asset_total_rows, data.invalid_rows as asset_invalid_rows,
            ast.issues as asset_issues, ast.alerts as asset_alerts, data.primary_columns, ast.description,
            coalesce(json_agg(distinct users.id) filter(where users.id is not null and user_mapping.level='asset'), '[]') as steward_users_id,
            coalesce(json_agg(distinct users.email) filter(where users.id is not null and user_mapping.level='asset'), '[]') as steward_users,
            coalesce(json_agg(distinct domain.technical_name) filter (where domain.id is not null), '[]') domains,
            coalesce(json_agg(distinct product.technical_name) filter (where product.id is not null), '[]') product,
            coalesce(json_agg(distinct application.name) filter (where application.id is not null), '[]') applications,
            coalesce(json_agg(distinct term.technical_name) filter (where term.id is not null), '[]') as terms,
            coalesce(json_agg(distinct jsonb_build_object('comment',comments.comment,'title', comments.title,'created_at', comments.created_at, 'rating',comments.rating, 'username',comments.username)) filter (where conversation_id is not null), '[]') as conversation
            from core.asset as ast
            join core.data on data.asset_id = ast.id
            join core.connection as con on con.id=ast.connection_id
        join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
        left join core.user_mapping on user_mapping.asset_id=ast.id
        left join core.users on users.id=user_mapping.user_id
        left join core.domain_mapping as vdom on vdom.asset_id=ast.id
        left join core.product_mapping as vpt on vpt.asset_id=ast.id
        left join core.application_mapping as vapp on vapp.asset_id=ast.id
        left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
        left join core.domain on domain.id=vdom.domain_id
        left join core.product on product.id=vpt.product_id
        left join core.application on application.id=vapp.application_id
        left join core.terms as term on term.id=vterms.term_id 
        left join comments on comments.asset_id=ast.id
            where ast.id='{asset_id}'
      group by con.id, ast.id, data.id
        """
        cursor = execute_query(connection, cursor, query_string)
        metadata = fetchone(cursor)
        metadata = metadata if metadata else {}

        job_query_string = f"""
            select queue.start_time from core.request_queue as rq
            join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category not in ('metadata', 'export_failed_rows')
            where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
      order by queue.created_date desc limit 1
        """
        cursor = execute_query(connection, cursor, job_query_string)
        last_job_detail = fetchone(cursor)
        last_job_detail = last_job_detail if last_job_detail else {}
    if not metadata:
        return

    connection_type = metadata.get("connection_type")
    delimiter = config.get("delimiter")
    current_date = queries.get("current_date")
    current_date = str(current_date.split("AS")[0]).strip()
    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.MSSQL.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Synapse.value,
    ]:
        current_date = str(current_date.split(".")[0]).strip()
    queue_id = config.get("queue_id")

    connection_credentials = metadata.get("credentials")
    connection_credentials = (
        json.loads(connection_credentials)
        if isinstance(connection_credentials, str)
        else connection_credentials
    )
    connection_credentials = connection_credentials if connection_credentials else {}

    asset_properties = metadata.get("properties")
    asset_properties = (
        json.loads(asset_properties)
        if isinstance(asset_properties, str)
        else asset_properties
    )
    asset_properties = asset_properties if asset_properties else {}
    schema = asset_properties.get("schema")
    schema = schema if schema else ""

    if connection_type == ConnectionType.Teradata.value:
        schema = ""

    database = asset_properties.get("database", "")

    primary_attributes = metadata.get("primary_columns")
    primary_attributes = (
        json.loads(primary_attributes)
        if isinstance(primary_attributes, str)
        else primary_attributes
    )
    primary_attributes = primary_attributes if primary_attributes else []

    
    # For SAP ECC, use column_name instead of name
    if connection_type == ConnectionType.SapEcc.value:
        primary_attributes = [attribute.get("name") or attribute.get("column_name") for attribute in primary_attributes]
    else:
        primary_attributes = [attribute.get("name") for attribute in primary_attributes]
    attributes = get_attribute_names(connection_type, primary_attributes)
    identifier_key = f"{delimiter}".join(attributes)
    valid_records = metadata.get("asset_total_rows") - metadata.get(
        "asset_invalid_rows"
    )

    steward_users = metadata.get("steward_users")
    steward_users = convert_json_string(steward_users, "", ",")
    steward_users_id = metadata.get("steward_users_id")
    steward_users_id = convert_json_string(steward_users_id, "", ",")

    domains = metadata.get("domains")
    domains = convert_json_string(domains, "", ",")
    domains = domains.replace("'", "''") if domains else ""
    product = metadata.get("product")
    product = convert_json_string(product, "", ",")
    product = product.replace("'", "''") if product else ""
    applications = metadata.get("applications")
    applications = convert_json_string(applications, "", ",")
    applications = applications.replace("'", "''") if applications else ""
    terms = metadata.get("terms")
    terms = convert_json_string(terms, "", ",")
    terms = terms.replace("'", "''") if terms else ""
    last_run_time = (
        last_job_detail.get("start_time")
        if last_job_detail.get("start_time")
        else "NULL"
    )
    created_date = (
        metadata.get("created_date") if metadata.get("created_date") else "NULL"
    )

    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.MSSQL.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Db2.value,
        ConnectionType.DB2IBM.value,
        ConnectionType.Synapse.value,
    ]:
        created_date = str(created_date).split(".")[0].strip()
        last_run_time = str(last_run_time).split(".")[0].strip()

    if (
        config.get("destination_connection_object").get("type").lower()
        == ConnectionType.Oracle.value
    ):
        created_date = (
            f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
            if str(created_date) != "NULL"
            else "NULL"
        )
        last_run_time = (
            f"""TO_TIMESTAMP_TZ('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
            if str(last_run_time) != "NULL"
            else "NULL"
        )
    else:
        created_date = f"'{created_date}'" if str(created_date) != "NULL" else "NULL"
        last_run_time = f"'{last_run_time}'" if str(last_run_time) != "NULL" else "NULL"

    conversations = metadata.get("conversation")
    comments = []
    for conversation in conversations:
        comment_text = ""
        comment = conversation.get("comment", {})
        comment = json.loads(comment)
        comment_block = comment.get("blocks", [])
        for block in comment_block:
            comment_text += block["text"] + "\n"
        comment_text = comment_text.replace("'", "") if comment_text else ""
        comments.append(
            {
                "title": conversation.get("title"),
                "rating": conversation.get("rating"),
                "created_at": conversation.get("created_at"),
                "username": conversation.get("username"),
                "comment": comment_text,
            }
        )
    comments = json.dumps(comments) if comments else ""
    comments = comments.replace("'", "") if comments else ""

    custom_fields = []
    with connection.cursor() as cursor:
        query_string = f"""
            select
                fields.name,
                field_property.value
            from
                core.fields
                left join core.field_property on field_property.field_id = fields.id
                and field_property.asset_id = '{asset_id}'
                and fields.is_active is true
            where value is not null and level='Asset'
        """
        cursor = execute_query(connection, cursor, query_string)
        custom_fields = fetchall(cursor)
        custom_fields = custom_fields if custom_fields else []

    description = metadata.get("description")
    description = description.replace('"', "").replace("'", "''") if description else ""
    asset_name = metadata.get("asset_name")
    asset_name = asset_name.replace('"', "").replace("'", "''") if asset_name else ""
    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.Databricks.value
    ]:
        domains = domains.replace("''", "\\'") if domains else ""
        terms = terms.replace("''", "\\'") if terms else ""
        applications = applications.replace("''", "\\'") if applications else ""
        product = product.replace("''", "\\'") if product else ""
        asset_name = asset_name.replace("''", "\\'") if asset_name else ""
        description = description.replace("''", "\\'") if description else ""

    asset_score = metadata.get("asset_score")
    asset_score = asset_score if asset_score else "null"

    asset_metadata = [
        f"""'{metadata.get("connection_id")}'""",
        f"'{database}'" if database else "''",
        f"'{schema}'" if schema else "''",
        f"""'{metadata.get("asset_id")}'""",
        f"'{asset_name}'",
        f"'{asset_name}'",
        asset_score,
        metadata.get("asset_total_rows"),
        valid_records,
        metadata.get("asset_invalid_rows"),
        metadata.get("asset_issues"),
        metadata.get("asset_alerts"),
        f"""'{metadata.get("status")}'""",
        f"'{identifier_key}'",
        f"'{queue_id}'",
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
    insert_asset_metadata = queries.get("insert_asset_metadata")
    fetch_source_table_schema_query = queries.get("fetch_table_schema")
    fetch_source_table_schema_query = (
        fetch_source_table_schema_query if fetch_source_table_schema_query else ""
    )
    source_assets_column_input = []
    diff_list = []
    source_measure_columm = []
    extract_custom_field_name = []

    """ Databricks Connection column name need ` string we added dynamically """
    destination_connection_object = config.get("destination_connection_object")
    insert_value = '"'
    if destination_connection_object.get("type").lower() in [
        ConnectionType.Databricks.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MySql.value,
    ]:
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            if (
                config.get("failed_rows_database").replace(".", "").replace("`", "")
                == "hive_metastore"
            ):
                fetch_source_table_schema_query = (
                    destination_queries.get("hive_metastore", {})
                    .get("failed_rows", {})
                    .get("fetch_table_schema", "")
                )
            fetch_source_table_schema_query = (
                fetch_source_table_schema_query.replace(
                    "<database_name>",
                    config.get("failed_rows_database")
                    .replace(".", "")
                    .replace("`", ""),
                )
                .replace("<schema_name>", config.get("failed_rows_schema"))
                .replace("<table_name>", "ASSET_METADATA")
            )

            source_assets_column_input, _ = execute_native_query(
                config.get("destination_config"),
                fetch_source_table_schema_query,
                None,
                is_list=True,
            )
            source_assets_column_input = convert_to_lower(source_assets_column_input)

            if (
                config.get("failed_rows_database").replace(".", "").replace("`", "")
                == "hive_metastore"
            ):
                source_assets_column_input = [
                    {"column_name": obj["col_name"], "data_type": obj["data_type"]}
                    for obj in source_assets_column_input
                ]

            if source_assets_column_input:
                for source_measure_column in source_assets_column_input:
                    extract_custom_field_name.append(
                        source_measure_column.get("column_name")
                    )
            diff_list = list(set(extract_custom_field_name) - set(default_column))

        insert_value = "`"

    if custom_fields:
        for entry in custom_fields:
            value = entry.get("value")
            source_measure_columm.append(entry.get("name"))
            if isinstance(value, str):
                value = value.replace('"', "").replace("'", "")
            asset_metadata.append(f"'{value}'")

        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            if diff_list:
                diff_list = list(set(diff_list) - set(source_measure_columm))
                for entry in diff_list:
                    asset_metadata.append(f"''")
                    custom_fields.append({"name": entry})

        custom_fields_columns = ", ".join(
            [
                f'{insert_value}{entry["name"].upper() if dest_conn_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value] else entry["name"]}{insert_value}'
                for entry in custom_fields
            ]
        )
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value,
            ConnectionType.BigQuery.value,
        ]:
            insert_asset_metadata = insert_asset_metadata.replace(
                "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
            )
        else:
            insert_asset_metadata = insert_asset_metadata.replace(
                "CREATED_DATE", f"CREATED_DATE, {custom_fields_columns}"
            )
    else:
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            for entry in diff_list:
                asset_metadata.append(f"''")

            custom_fields_columns = ", ".join(
                [f"{insert_value}{entry}{insert_value}" for entry in diff_list]
            )
            insert_asset_metadata = (
                insert_asset_metadata.replace(
                    "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
                )
                if custom_fields_columns
                else insert_asset_metadata
            )
    destination_connection = execute_failed_rows_metadata_query(
        config, asset_metadata, insert_asset_metadata, destination_connection
    )
    return destination_connection


def insert_attribute_metadata(
    config: dict, queries: dict, destination_connection, destination_queries
):
    """
    Insert the connection metadata for the current run
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    destination_connection_object = config.get("destination_connection_object")
    dest_conn_type = destination_connection_object.get("type").lower()
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
    if not asset_id:
        return

    attribute_metadata = []
    last_job_detail = {}
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select ast.id as asset_id, attr.id as attribute_id, attr.name as attribute_name,attr.status, attr.score as attribute_score,
            attr.alerts as attribute_alerts, attr.issues as attribute_issues,attr.description,term.technical_name as term,
            attr.row_count as attribute_total_rows, attr.invalid_rows as attribute_invalid_rows,attr.is_blank,
            attr.min_length,attr.max_length,attr.derived_type as datatype,attr.min_value,attr.max_value,attr.is_null,
            attr.is_primary_key as is_primary,attr.is_unique,coalesce(json_agg(distinct tags.technical_name) filter (where tags.id is not null), '[]') as tags,
            attr.created_date
            from core.asset as ast
            join core.connection as con on con.id=ast.connection_id
            join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
            left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
            left join core.tags_mapping as vtags on vtags.attribute_id=attr.id
			left join core.terms as term on term.id=vterms.term_id 
            left join core.tags as tags on tags.id=vtags.tags_id
            where ast.id='{asset_id}' and attr.is_active=true
            group by ast.id,attr.id,term.id
        """
        cursor = execute_query(connection, cursor, query_string)
        attribute_metadata = fetchall(cursor)
        attribute_metadata = attribute_metadata if attribute_metadata else []
        job_query_string = f"""
            select queue.start_time, queue.status from core.request_queue as rq
            join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category = 'profile'
            where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
            order by queue.created_date desc limit 1
        """
        cursor = execute_query(connection, cursor, job_query_string)
        last_job_detail = fetchone(cursor)
        last_job_detail = last_job_detail if last_job_detail else {}
    if not attribute_metadata:
        return

    current_date = queries.get("current_date")
    current_date = str(current_date.split("AS")[0]).strip()
    if dest_conn_type in [
        ConnectionType.MSSQL.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Synapse.value,
    ]:
        current_date = str(current_date.split(".")[0]).strip()
    queue_id = config.get("queue_id")

    job_status = (
        last_job_detail.get("status") if last_job_detail.get("status") else "Completed"
    )
    destination_config = config.get("destination_config")
    connection_type = destination_config.get("connection_type")

    custom_fields_dict = {}
    attribute_ids_list = [metadata.get("attribute_id") for metadata in attribute_metadata]
    attribute_ids_list_str = ", ".join([f"'{attribute_id}'" for attribute_id in attribute_ids_list])
    with connection.cursor() as cursor:
        query_string = f"""
            select
                fields.name,
                field_property.value,
                field_property.attribute_id
            from
                core.fields
                left join core.field_property on field_property.field_id = fields.id
                and field_property.attribute_id in ({attribute_ids_list_str})
                and fields.is_active is true
            where value is not null and level='Attribute'
        """
        cursor = execute_query(connection, cursor, query_string)    
        custom_fields_list = fetchall(cursor)
        custom_fields_list = custom_fields_list if custom_fields_list else []
    
    # Group custom fields by attribute_id
    custom_fields_dict = {}
    for field in custom_fields_list:
        attr_id = field.get("attribute_id")
        if attr_id:
            if attr_id not in custom_fields_dict:
                custom_fields_dict[attr_id] = []
            custom_fields_dict[attr_id].append({
                "name": field.get("name"),
                "value": field.get("value")
            })

    insert_value = '"'

    insert_attribute_metadata = queries.get("insert_attribute_metadata")
    fetch_source_table_schema_query = queries.get("fetch_table_schema")
    source_attributes_column_input = []
    diff_list = []
    source_attribute_columm = []
    extract_custom_field_name = []
    if dest_conn_type in [
        ConnectionType.Databricks.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MySql.value,
    ]:
        if dest_conn_type in [ConnectionType.Databricks.value]:
            if (
                config.get("failed_rows_database").replace(".", "").replace("`", "")
                == "hive_metastore"
            ):
                fetch_source_table_schema_query = (
                    destination_queries.get("hive_metastore", {})
                    .get("failed_rows", {})
                    .get("fetch_table_schema", "")
                )
            fetch_source_table_schema_query = (
                fetch_source_table_schema_query.replace(
                    "<database_name>",
                    config.get("failed_rows_database")
                    .replace(".", "")
                    .replace("`", ""),
                )
                .replace("<schema_name>", config.get("failed_rows_schema"))
                .replace("<table_name>", "ATTRIBUTE_METADATA")
            )

            source_attributes_column_input, _ = execute_native_query(
                config.get("destination_config"),
                fetch_source_table_schema_query,
                None,
                is_list=True,
            )
            source_attributes_column_input = convert_to_lower(
                source_attributes_column_input
            )

            if (
                config.get("failed_rows_database").replace(".", "").replace("`", "")
                == "hive_metastore"
            ):
                source_attributes_column_input = [
                    {"column_name": obj["col_name"], "data_type": obj["data_type"]}
                    for obj in source_attributes_column_input
                ]

            if source_attributes_column_input:
                for source_measure_column in source_attributes_column_input:
                    extract_custom_field_name.append(
                        source_measure_column.get("column_name")
                    )
            diff_list = list(set(extract_custom_field_name) - set(default_column))

        insert_value = "`"

    for metadata in attribute_metadata:
        attribute_id = metadata.get("attribute_id")
        last_run_time = (
            last_job_detail.get("start_time")
            if last_job_detail.get("start_time")
            else "NULL"
        )
        attribute_invalid_rows = metadata.get("attribute_invalid_rows", 0) if metadata.get("attribute_invalid_rows", 0) else 0
        attribute_total_rows = metadata.get("attribute_total_rows", 0) if metadata.get("attribute_total_rows", 0) else 0
        attribute_valid_rows = attribute_total_rows - attribute_invalid_rows
        attribute_name = metadata.get("attribute_name")
        attribute_name = (
            attribute_name.replace('"', "").replace("'", "''") if attribute_name else ""
        )
        custom_fields = custom_fields_dict.get(attribute_id, [])
        tags = metadata.get("tags")
        tags = convert_json_string(tags, "", ",")
        tags = tags.replace("'", "''") if tags else ""
        description = metadata.get("description")
        description = (
            description.replace('"', "").replace("'", "''") if description else ""
        )
        term = metadata.get("term")
        term = term.replace("'", "''") if term else ""
        term = term if term else ""
        if config.get("destination_connection_object").get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            attribute_name = (
                attribute_name.replace("''", "\\'") if attribute_name else ""
            )
            term = term.replace("''", "\\'") if term else ""
            tags = tags.replace("''", "\\'") if tags else ""
            description = description.replace("''", "\\'") if description else ""
        min_value = metadata.get("min_value")
        min_value = min_value if min_value else ""
        if min_value and isinstance(min_value, str):
            min_value = min_value.replace("\\", "\\\\").replace("'", "''")
        max_value = metadata.get("max_value")
        max_value = max_value if max_value else ""
        if max_value and isinstance(max_value, str):
            max_value = max_value.replace("\\", "\\\\").replace("'", "''")
        min_length = metadata.get("min_length")
        min_length = min_length if min_length else ""
        max_length = metadata.get("max_length")
        max_length = max_length if max_length else ""
        is_primary = metadata.get("is_primary") if metadata.get("is_primary") else False
        created_date = (
            metadata.get("created_date") if metadata.get("created_date") else "NULL"
        )

        attribute_score = metadata.get("attribute_score")
        attribute_score = attribute_score if attribute_score else "null"

        if dest_conn_type in [
            ConnectionType.MSSQL.value,
            ConnectionType.BigQuery.value,
            ConnectionType.Db2.value,
            ConnectionType.DB2IBM.value,
            ConnectionType.Synapse.value,
        ]:
            created_date = str(created_date).split(".")[0].strip()
            last_run_time = str(last_run_time).split(".")[0].strip()

        if dest_conn_type == ConnectionType.Oracle.value:
            created_date = (
                f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
                if str(created_date) != "NULL"
                else "NULL"
            )
            last_run_time = (
                f"""TO_TIMESTAMP_TZ('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
                if str(last_run_time) != "NULL"
                else "NULL"
            )

        else:
            created_date = (
                f"'{created_date}'" if str(created_date) != "NULL" else "NULL"
            )
            last_run_time = (
                f"'{last_run_time}'" if str(last_run_time) != "NULL" else "NULL"
            )

        if connection_type in [
            ConnectionType.BigQuery.value,
            ConnectionType.SapHana.value,
            ConnectionType.Postgres.value,
            ConnectionType.AlloyDB.value,
        ]:
            is_primary = bool(is_primary)
        else:
            is_primary = int(is_primary)

        if not metadata.get("datatype", ""):
            min_value = ""
            max_value = ""

        datatype = metadata.get("datatype") if metadata.get("datatype") else "TEXT"

        attribute_metadata_input = [
            f"""'{metadata.get("asset_id")}'""",
            f"""'{attribute_id}'""",
            f"'{attribute_name}'",
            attribute_score,
            metadata.get("attribute_total_rows"),
            attribute_valid_rows,
            metadata.get("attribute_invalid_rows"),
            metadata.get("attribute_issues"),
            metadata.get("attribute_alerts"),
            f"""'{metadata.get("status")}'""",
            f"""'{queue_id}'""",
            f"'{description}'",
            f"'{term}'",
            f"'{min_length}'",
            f"'{max_length}'",
            f"'{datatype}'",
            f"'{min_value}'",
            f"'{max_value}'",
            is_primary,
            f"'{tags}'",
            last_run_time,
            f"'{job_status}'",
            created_date,
        ]
        insert_attribute_metadata = queries.get("insert_attribute_metadata")
        source_attribute_columm = []

        if custom_fields:
            for entry in custom_fields:
                value = entry.get("value")
                source_attribute_columm.append(entry.get("name"))
                if isinstance(value, str):
                    value = value.replace('"', "").replace("'", "")
                attribute_metadata_input.append(f"'{value}'")

            if dest_conn_type in [ConnectionType.Databricks.value]:
                if diff_list:
                    diff_list_col = list(set(diff_list) - set(source_attribute_columm))
                    for entry in diff_list_col:
                        attribute_metadata_input.append(f"''")
                        custom_fields.append({"name": entry})

            custom_fields_columns = ", ".join(
                [
                    f'{insert_value}{entry["name"].upper() if dest_conn_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value] else entry["name"]}{insert_value}'
                    for entry in custom_fields
                ]
            )
            if dest_conn_type in [
                ConnectionType.Databricks.value,
                ConnectionType.BigQuery.value,
            ]:
                insert_attribute_metadata = insert_attribute_metadata.replace(
                    "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
                )
            else:
                insert_attribute_metadata = insert_attribute_metadata.replace(
                    "CREATED_DATE", f"CREATED_DATE, {custom_fields_columns}"
                )
        else:
            if dest_conn_type in [ConnectionType.Databricks.value]:
                for entry in diff_list:
                    attribute_metadata_input.append(f"''")

                custom_fields_columns = ", ".join(
                    [f"{insert_value}{entry}{insert_value}" for entry in diff_list]
                )
                insert_attribute_metadata = (
                    insert_attribute_metadata.replace(
                        "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
                    )
                    if custom_fields_columns
                    else insert_attribute_metadata
                )
        destination_connection = execute_failed_rows_metadata_query(
            config,
            attribute_metadata_input,
            insert_attribute_metadata,
            destination_connection,
        )
    return destination_connection


def insert_measure_metadata(
    config: dict,
    queries: dict,
    measure: dict,
    destination_connection,
    destination_queries,
):
    """
    Insert the connection metadata for the current run
    """
    measure_id = measure.get("id")
    queue_id = config.get("queue_id")
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    destination_config = config.get("destination_config")
    connection_type = destination_config.get("connection_type")
    default_column = [
        "CONNECTION_ID",
        "ASSET_ID",
        "ATTRIBUTE_ID",
        "MEASURE_ID",
        "MEASURE_NAME",
        "MEASURE_DIMENSION",
        "MEASURE_WEIGHTAGE",
        "MEASURE_TYPE",
        "MEASURE_THRESHOLD",
        "MEASURE_QUERY",
        "TOTAL_RECORDS_SCOPE",
        "TOTAL_RECORDS_SCOPE_QUERY",
        "DQ_SCORE",
        "TOTAL_RECORDS",
        "PASSED_RECORDS",
        "FAILED_RECORDS",
        "STATUS",
        "RUN_STATUS",
        "DOMAIN",
        "PRODUCT",
        "APPLICATION",
        "COMMENT",
        "RUN_ID",
        "RUN_DATE",
        "MEASURE_STATUS",
        "IS_ACTIVE",
        "PROFILE",
        "CREATED_DATE",
    ]

    if not measure_id:
        return

    metadata = {}
    queue_id = config.get("queue_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with max_start_time AS (
                SELECT mes.id as measure_id, max(queue_detail.start_time) AS max_start_time
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and queue_detail.category!='export_failed_rows'
                group by mes.id
            ),
            custom_measure_status AS (
                select mes.id, queue_detail.status
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and mes.id=queue_detail.measure_id
                where queue_detail.status is not null and queue_detail.category!='export_failed_rows'
                group by mes.id, queue_detail.status 
            )
            select mes.connection_id as connection_id, ast.id as asset_id, attr.id as attribute_id,
            mes.id as measure_id, base.name as measure_name, mes.weightage, base.category, mes.created_date,
            mes.row_count as measure_total_count, mes.valid_rows AS measure_valid_count, mes.invalid_rows AS measure_invalid_count, 
            mes.score as measure_score, dim.name as dimension_name, mes.is_positive, custom.status as custom_measure_run_status,
            coalesce(json_agg(distinct domain.technical_name) filter (where domain.id is not null), '[]') domains,
            coalesce(json_agg(distinct product.technical_name) filter (where product.id is not null), '[]') product,
            coalesce(json_agg(distinct application.name) filter (where application.id is not null), '[]') applications,
            queue_detail.status as last_run_status, queue_detail.start_time as run_date, mes.status as measure_status, base.description, mes.is_active, base.profile
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            left join core.asset as ast on ast.id=mes.asset_id
            left join core.dimension as dim on dim.id=mes.dimension_id
            left join core.attribute as attr on attr.id=mes.attribute_id
            left join core.domain_mapping as vdom on (vdom.asset_id=ast.id or vdom.measure_id=mes.id)
            left join core.product_mapping as vpt on (vpt.asset_id=ast.id or vpt.measure_id=mes.id)
            left join core.application_mapping as vapp on (vapp.asset_id=ast.id or vapp.measure_id=mes.id)
            left join core.domain on domain.id = vdom.domain_id
            left join core.product on product.id = vpt.product_id
            left join core.application on application.id = vapp.application_id
            left join core.request_queue_detail as queue_detail on mes.last_run_id=queue_detail.queue_id
      		left join custom_measure_status as custom on mes.id=custom.id
            join max_start_time ON mes.id = max_start_time.measure_id AND (queue_detail.start_time = max_start_time.max_start_time or (queue_detail.start_time is null and max_start_time.max_start_time is null))
            where mes.id='{measure_id}' and mes.connection_id is not null
            group by mes.connection_id, ast.id, attr.id, mes.id, queue_detail.start_time, base.name, base.category, 
            dim.name, queue_detail.status, base.description, mes.created_date, custom.status, base.profile
        """
        cursor = execute_query(connection, cursor, query_string)
        metadata = fetchone(cursor)
        metadata = metadata if metadata else {}
    if not metadata:
        return

    custom_fields = []
    with connection.cursor() as cursor:
        query_string = f"""
            select
                fields.name,
                field_property.value
            from
                core.fields
                left join core.field_property on field_property.field_id = fields.id
                and field_property.measure_id = '{measure_id}'
                and fields.is_active is true
            where value is not null and level='Measure'
        """
        cursor = execute_query(connection, cursor, query_string)
        custom_fields = fetchall(cursor)
        custom_fields = custom_fields if custom_fields else []

    current_date = queries.get("current_date")
    current_date = str(current_date.split("AS")[0]).strip()
    level = config.get("level")
    max_limit = config.get("max_row_limit")
    max_limit = int(max_limit) if isinstance(max_limit, str) else max_limit

    comment_text = "''"
    measure_invalid_count = metadata.get("measure_invalid_count")
    if max_limit and measure_invalid_count > max_limit:
        comment_text = f"'Partial subset is inserted based on FAILED_ROW_COUNT of {str(max_limit)}'"

    measure_properties = measure.get("properties")
    measure_properties = (
        json.loads(measure_properties)
        if isinstance(measure_properties, str)
        else measure_properties
    )
    measure_properties = measure_properties if measure_properties else {}
    conditional_scoring = measure_properties.get("conditional_scoring")
    conditional_scoring = conditional_scoring if conditional_scoring else {}
    is_total_records_scope = conditional_scoring.get("is_enabled")
    is_total_records_scope = is_total_records_scope if is_total_records_scope else False
    is_measure_active = measure.get("is_active")
    is_measure_active = is_measure_active if is_measure_active else False
    total_records_scope_query = conditional_scoring.get("query")
    total_records_scope_query = (
        total_records_scope_query if total_records_scope_query else "''"
    )
    drift_threshold = measure.get("drift_threshold")
    measure_technical_name = measure.get("technical_name")
    if (
        connection_type.lower() == ConnectionType.Oracle.value.lower()
        and measure_technical_name in ("short_pattern", "long_pattern", "length", "enum") 
        and measure.get("type") == "frequency"
    ):
        drift_threshold = {}
    drift_threshold = (
        json.dumps(drift_threshold, default=str) if drift_threshold else ""
    )
    measure_name = (
        str(metadata.get("measure_name")).replace("'", "''")
        if metadata.get("measure_name")
        else ""
    )
    asset_id = metadata.get("asset_id")
    asset_id = f"""'{asset_id}'""" if asset_id else "NULL"

    attribute_id = metadata.get("attribute_id")
    attribute_id = f"""'{attribute_id}'""" if attribute_id else "NULL"

    domain = metadata.get("domains")
    domain = convert_json_string(domain, "", ",")
    domain = domain.replace("'", "''") if domain else ""

    product = metadata.get("product")
    product = convert_json_string(product, "", ",")
    product = product.replace("'", "''") if product else ""

    application = metadata.get("applications")
    application = convert_json_string(application, "", ",")
    application = application.replace("'", "''") if application else ""

    last_run_status = metadata.get("last_run_status")
    custom_measure_run_status = metadata.get("custom_measure_run_status")

    last_run_status = (
        custom_measure_run_status if custom_measure_run_status else last_run_status
    )
    last_run_status = last_run_status if last_run_status else "Not Run"

    comment = metadata.get("description")
    comment = (
        comment.replace('"', "").replace("'", "\\'").replace("`", "") if comment else ""
    )

    run_date = metadata.get("run_date") if metadata.get("run_date") else "NULL"
    created_date = (
        metadata.get("created_date") if metadata.get("created_date") else "NULL"
    )
    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.MSSQL.value,
        ConnectionType.BigQuery.value,
        ConnectionType.Db2.value,
        ConnectionType.DB2IBM.value,
        ConnectionType.MySql.value,
        ConnectionType.Synapse.value,
    ]:
        run_date = str(run_date).split(".")[0].strip()
        created_date = str(created_date).split(".")[0].strip()

    if (
        config.get("destination_connection_object").get("type").lower()
        == ConnectionType.Oracle.value
    ):
        created_date = (
            f"""TO_TIMESTAMP_TZ('{created_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
            if str(created_date) != "NULL"
            else "NULL"
        )
        run_date = (
            f"""TO_TIMESTAMP_TZ('{run_date}', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')"""
            if str(run_date) != "NULL"
            else "NULL"
        )
    else:
        created_date = f"'{created_date}'" if str(created_date) != "NULL" else "NULL"
        run_date = f"'{run_date}'" if str(run_date) != "NULL" else "NULL"

    measure_status = metadata.get("measure_status")
    measure_status = measure_status if measure_status else ""

    is_total_records_scope = int(is_total_records_scope)
    is_measure_active = int(is_measure_active)
    weigtage = float(metadata.get("weightage")) if metadata.get("weightage") else 100
    dimension_name = (
        str(metadata.get("dimension_name")).replace("'", "''")
        if metadata.get("dimension_name")
        else ""
    )
    if config.get("destination_connection_object").get("type").lower() in [
        ConnectionType.Databricks.value
    ]:
        domain = domain.replace("''", "\\'") if domain else ""
        product = product.replace("''", "\\'") if product else ""
        application = application.replace("''", "\\'") if application else ""
        measure_name = measure_name.replace("''", "\\'") if measure_name else ""
        dimension_name = dimension_name.replace("''", "\\'") if dimension_name else ""
        comment = comment.replace("''", "\\'") if comment else ""
    query = (
        str(measure.get("query")).replace("'", "''")
        if measure.get("type") != "semantic"
        else str(measure.get("semantic_query")).replace("'", "''")
    )
    total_records_query = total_records_scope_query.replace("'", "''")
    if connection_type in [ConnectionType.BigQuery.value]:
        is_total_records_scope = bool(is_total_records_scope)
        query = str(measure.get("query")).replace("\n", " ").replace("\r", " ")
        total_records_query = total_records_scope_query.replace("\n", " ").replace(
            "\r", " "
        )
        is_measure_active = bool(is_measure_active)
    elif connection_type in [
        ConnectionType.SapHana.value,
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        is_total_records_scope = bool(is_total_records_scope)
        is_measure_active = bool(is_measure_active)

    double_escap_character = '"'
    single_escap_character = "'"
    empty_string = ""
    score = metadata.get("measure_score")
    score = score if score else "null"
    measure_metadata_input = [
        f"""'{metadata.get("connection_id")}'""",
        asset_id,
        attribute_id,
        f"""'{metadata.get("measure_id")}'""",
        f"""'{measure_name}'""",
        f"""'{dimension_name}'""",
        weigtage,
        f"""'{metadata.get("category")}'""" if metadata.get("category") else "''",
        f"""'{drift_threshold}'""",
        f"""'{query.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string)}'""",
        is_total_records_scope,
        f"""{f"'{total_records_query.replace(single_escap_character, empty_string)}'" if total_records_query else f''}""",
        score,
        metadata.get("measure_total_count"),
        metadata.get("measure_valid_count"),
        metadata.get("measure_invalid_count"),
        "'Valid'" if metadata.get("is_positive") else "'Invalid'",
        f"""'{last_run_status.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string)}'""",
        f"'{domain}'",
        f"'{product}'",
        f"'{application}'",
        f"""'{comment}'""",
        f"""'{queue_id}'""",
        run_date,
        f"""'{measure_status.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string)}'""",
        is_measure_active,
        f"""'{metadata.get("profile")}'""" if metadata.get("profile") else "''",
        created_date,
    ]

    for table in FAILED_ROWS_METADATA_TABLES:
        if table != MEASURE_METADATA:
            continue
        delete_metadata_query = queries.get("delete_metadata_record")
        delete_metadata_query = (
            delete_metadata_query.replace("<schema_name>", schema_name)
            .replace("<table_name>", table)
            .replace("<database_name>.", database_name)
            .replace("<run_id>", queue_id)
        )
        delete_metadata_query = (
            f""" {delete_metadata_query} and MEASURE_ID='{measure_id}' """
        )
        _, native_connection = execute_native_query(
            destination_config,
            delete_metadata_query,
            destination_connection,
            no_response=True,
        )
        if not destination_connection and native_connection:
            destination_connection = native_connection

    insert_measure_metadata = queries.get("insert_measure_metadata")
    fetch_source_table_schema_query = queries.get("fetch_table_schema")
    if (
        connection_type == ConnectionType.Databricks.value
        and database_name.replace(".", "").replace("`", "") == "hive_metastore"
    ):
        fetch_source_table_schema_query = (
            destination_queries.get("hive_metastore", {})
            .get("failed_rows", {})
            .get("fetch_table_schema", "")
        )

    fetch_source_table_schema_query = (
        fetch_source_table_schema_query if fetch_source_table_schema_query else ""
    )
    source_measure_column_input = []
    source_measure_columm = []
    diff_list = []
    extract_custom_field_name = []

    """ Databricks Connection column anme need ` string we added dynamically """
    destination_connection_object = config.get("destination_connection_object")
    insert_value = '"'
    if destination_connection_object.get("type").lower() in [
        ConnectionType.Databricks.value,
        ConnectionType.BigQuery.value,
        ConnectionType.MySql.value,
    ]:
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            fetch_source_table_schema_query = (
                fetch_source_table_schema_query.replace(
                    "<database_name>", database_name.replace(".", "").replace("`", "")
                )
                .replace("<schema_name>", schema_name)
                .replace("<table_name>", table)
            )

            source_measure_column_input, _ = execute_native_query(
                config.get("destination_config"),
                fetch_source_table_schema_query,
                None,
                is_list=True,
            )
            source_measure_column_input = convert_to_lower(source_measure_column_input)

            if database_name.replace(".", "").replace("`", "") == "hive_metastore":
                source_measure_column_input = [
                    {"column_name": obj["col_name"], "data_type": obj["data_type"]}
                    for obj in source_measure_column_input
                ]

            if source_measure_column_input:
                for source_measure_column in source_measure_column_input:
                    extract_custom_field_name.append(
                        source_measure_column.get("column_name")
                    )
            diff_list = list(set(extract_custom_field_name) - set(default_column))

        insert_value = "`"

    if custom_fields:
        for entry in custom_fields:
            value = entry.get("value")
            source_measure_columm.append(entry.get("name"))
            if isinstance(value, str):
                value = value.replace('"', "").replace("'", "")
            measure_metadata_input.append(f"'{value}'")

        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            if diff_list:
                diff_list = list(set(diff_list) - set(source_measure_columm))
                for entry in diff_list:
                    measure_metadata_input.append(f"''")
                    custom_fields.append({"name": entry})

        custom_fields_columns = ", ".join(
            [
                f'{insert_value}{entry["name"].upper() if connection_type in [ConnectionType.Db2.value, ConnectionType.DB2IBM.value] else entry["name"]}{insert_value}'
                for entry in custom_fields
            ]
        )
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value,
            ConnectionType.BigQuery.value,
        ]:
            insert_measure_metadata = insert_measure_metadata.replace(
                "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
            )
        else:
            insert_measure_metadata = insert_measure_metadata.replace(
                "CREATED_DATE", f"CREATED_DATE, {custom_fields_columns}"
            )
    else:
        if destination_connection_object.get("type").lower() in [
            ConnectionType.Databricks.value
        ]:
            for entry in diff_list:
                measure_metadata_input.append(f"''")

            custom_fields_columns = ", ".join(
                [f"{insert_value}{entry}{insert_value}" for entry in diff_list]
            )
            insert_measure_metadata = (
                insert_measure_metadata.replace(
                    "`CREATED_DATE`", f"`CREATED_DATE`, {custom_fields_columns}"
                )
                if custom_fields_columns
                else insert_measure_metadata
            )

    destination_connection = execute_failed_rows_metadata_query(
        config, measure_metadata_input, insert_measure_metadata, destination_connection
    )
    return destination_connection


def insert_failed_rows_metadata(
    config: dict, queries: dict, destination_connection, destination_queries
):
    """
    Insert measure metadata into failed rows metatables
    """
    connection_type = config.get("connection_type")
    level = config.get("level")
    queue_id = config.get("queue_id")
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    destination_config = config.get("destination_config")

    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"

    # Delete the record for current run if exists any.
    destination_connection = None
    for table in FAILED_ROWS_METADATA_TABLES:
        if table == MEASURE_METADATA:
            continue
        delete_metadata_query = queries.get("delete_metadata_record")
        delete_metadata_query = (
            delete_metadata_query.replace("<schema_name>", schema_name)
            .replace("<table_name>", table)
            .replace("<database_name>.", database_name)
            .replace("<run_id>", queue_id)
        )
        _, native_connection = execute_native_query(
            destination_config,
            delete_metadata_query,
            destination_connection,
            no_response=True,
        )
        if not destination_connection and native_connection:
            destination_connection = native_connection

    destination_connection = insert_connection_metadata(
        config, queries, destination_connection
    )
    if level != "measure":
        destination_connection = insert_asset_metadata(
            config, queries, destination_connection, destination_queries
        )
        destination_connection = insert_attribute_metadata(
            config, queries, destination_connection, destination_queries
        )


def execute_failed_rows_metadata_query(
    config: dict, input_data: list, query: str, destination_connection
):
    connection_type = config.get("connection_type")
    schema_name = config.get("failed_rows_schema")
    database_name = config.get("failed_rows_database")
    failed_rows_table = config.get("failed_rows_table")
    destination_config = config.get("destination_config")
    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"
    if input_data and query:
        query = (
            query.replace("<schema_name>", schema_name)
            .replace("<failed_rows_table>", failed_rows_table)
            .replace("<database_name>.", database_name)
        )
        input_data = [
            "NULL" if value == "NULL" or value is None else str(value)
            for value in input_data
        ]

        """ synapse database Insert Method is Differe so we added conditions here """
        destination_connection_object = config.get("destination_connection_object")

        if destination_connection_object.get("type").lower() in [
            ConnectionType.Synapse.value
        ]:
            query_input = f"""{",".join(input_data)}"""
            query_string = f"{query} SELECT {query_input}"
        else:
            query_input = f"""({",".join(input_data)})"""
            query_string = f"{query} VALUES {query_input}"
        _, native_connection = execute_native_query(
            destination_config,
            query_string,
            destination_connection,
            True,
            no_response=True,
        )
        if not destination_connection and native_connection:
            destination_connection = native_connection
    return destination_connection


def alter_table_schema(
    config: dict,
    queries: dict,
    source_queries: dict,
    input_data: list,
    destination_connection,
    destination_queries
):
    """
    Alters the table schema if any new columns identified in the failed row query
    """
    check_selected_attributes = config.get("check_selected_attributes")
    asset_attributes = config.get("asset_attributes")
    asset_attributes = asset_attributes if asset_attributes else []
    summarized_failed_rows_columns = queries.get("failed_rows_columns", {})
    
    # Get connection type from config
    destination_config = config.get("destination_config")
    connection_type = destination_config.get("connection_type") if destination_config else ""
    connection_type = connection_type.lower() if connection_type else ""

    base_schema = config.get("schema")
    base_database = config.get("database_name")
    
    # For SAP ECC, use column_name instead of name
    if connection_type == ConnectionType.SapEcc.value:
        asset_attribute_names = [
            attribute.get("column_name").lower()
            for attribute in asset_attributes
            if attribute and attribute.get("column_name")
        ]
        if not asset_attribute_names:
            asset_attribute_names = [
                attribute.get("name").lower()
                for attribute in asset_attributes
                if attribute and attribute.get("name")
            ]
    else:
        asset_attribute_names = [
            attribute.get("attribute_name").lower()
            for attribute in asset_attributes
            if attribute and attribute.get("attribute_name")
        ]
        if not asset_attribute_names:
            asset_attribute_names = [
                attribute.get("name").lower()
                for attribute in asset_attributes
                if attribute and attribute.get("name")
            ]

    dq_datatypes = source_queries.get("dq_datatypes")
    max_column_limit = config.get("max_column_limit")
    is_same_source = config.get("is_same_source")
    level = config.get("level")
    max_column_limit = int(max_column_limit) if max_column_limit else 20
    if not input_data:
        return []

    row_data = input_data[0] if len(input_data) > 0 else {}
    
    # Get source_connection_type first (needed for MongoDB→SQL check)
    source_connection_type = config.get("connection_type")
    source_table = config.get("table_technical_name")
    database_name = config.get("failed_rows_database")
    destination_connection_object = config.get("destination_connection_object")
    destination_config = config.get("destination_config")
    schema_name = config.get("failed_rows_schema")
    database = config.get("destination_database")
    
    source_connection_type_lower = source_connection_type.lower() if source_connection_type else ""
    is_mongo_to_sql = source_connection_type_lower == "mongo" and connection_type in ["snowflake", "postgres", "redshift"]
    
    if not row_data:
        if is_mongo_to_sql and asset_attribute_names:
            pass
        else:
            return []

    if schema_name:
        if database:
            database_name = database
        if not database_name:
            if destination_connection_object:
                connection = destination_connection_object
            connection_credentials = connection.get("credentials")
            connection_credentials = (
                connection_credentials if connection_credentials else {}
            )
            database_name = connection_credentials.get("database")
            database_name = f"{database_name}" if database_name else ""

        if connection_type == ConnectionType.Snowflake.value and not database_name:
            destination_connection_database = get_dq_connections_database(
                config, destination_connection_object.get("id")
            )
            database_name = destination_connection_database.get("name")

    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"

    source_database = database_name
    source_schema = config.get("failed_rows_schema")

    destination_database = config.get("destination_database")
    failed_rows_table = config.get("failed_rows_table")
    fetch_column_query = queries.get("fetch_failed_rows_table_schema")

    if (
        connection_type == ConnectionType.Databricks.value
        and database_name.replace(".", "").replace("`", "") == "hive_metastore"
    ):
        fetch_column_query = (
            destination_queries.get("hive_metastore", {})
            .get("failed_rows", {})
            .get("fetch_failed_rows_table_schema")
        )

    fetch_column_query = fetch_column_query if fetch_column_query else ""
    fetch_source_table_schema_query = queries.get("fetch_table_schema")
    fetch_source_table_schema_query = (
        fetch_source_table_schema_query if fetch_source_table_schema_query else ""
    )
    datatypes_mapping = queries.get("datatypes_mapping")
    derivedtype_mapping = queries.get("derivedtype_mapping")
    alter_failed_rows_table = queries.get("alter_failed_rows_table")
    destination_config = config.get("destination_config")
    drop_column_query = queries.get("drop_column")
    alter_column_query = queries.get("alter_column")

    source_temp_table_name = config.get("temp_table_name")
    if source_temp_table_name:
        source_database = destination_database
        source_schema = schema_name
        source_table = source_temp_table_name

    fetch_column_query = (
        fetch_column_query.replace("<failed_rows_table>", failed_rows_table)
        .replace(
            "<target_database_name>",
            (
                database_name.replace(".", "")
                if database_name.endswith(".")
                else database_name
            ),
        )
        .replace("<database_name>", destination_database)
        .replace("<schema_name>", schema_name)
    )
    drop_column_query = (
        drop_column_query.replace("<table_name>", failed_rows_table)
        .replace(
            "<target_database_name>",
            (
                database_name.replace(".", "")
                if database_name.endswith(".")
                else database_name
            ),
        )
        .replace("<database_name>", destination_database)
        .replace("<schema_name>", schema_name)
    )
    alter_column_query = (
        alter_column_query.replace("<table_name>", failed_rows_table)
        .replace(
            "<target_database_name>",
            (
                database_name.replace(".", "")
                if database_name.endswith(".")
                else database_name
            ),
        )
        .replace("<database_name>", destination_database)
        .replace("<schema_name>", schema_name)
    )

    if fetch_source_table_schema_query and source_schema and source_table:
        source_database = source_database if source_database else ""
        source_schema = source_schema if source_schema else ""
        source_table = source_table.replace("'", "''") if source_table else ""
        fetch_source_table_schema_query = (
            fetch_source_table_schema_query.replace("<database_name>", base_database)
            .replace("<schema_name>", base_schema)
            .replace("<table_name>", source_table)
        )
    else:
        fetch_source_table_schema_query = ""
    if connection_type == ConnectionType.Redshift.value:
        fetch_source_table_schema_query = fetch_source_table_schema_query if fetch_source_table_schema_query and is_same_source else ""
    else:
        fetch_source_table_schema_query = (
            fetch_source_table_schema_query
            if fetch_source_table_schema_query
            and str(level).lower() != "measure"
            and is_same_source
            else ""
        )
    source_column_input = []
    if fetch_source_table_schema_query:
        if config.get("destination_connection_object").get("type").lower() in [
            ConnectionType.BigQuery.value
        ]:
            time.sleep(20)
        source_column_input, _ = execute_native_query(
            config, fetch_source_table_schema_query, None, is_list=True
        )
        source_column_input = convert_to_lower(source_column_input)

    existing_columns = []
    failed_rows_columns = []
    if fetch_column_query:
        if config.get("destination_connection_object").get("type").lower() in [
            ConnectionType.BigQuery.value
        ]:
            time.sleep(20)
        existing_column_input, _ = execute_native_query(
            destination_config, fetch_column_query, destination_connection, is_list=True
        )
        failed_rows_columns = existing_column_input if existing_column_input else []
        existing_column_input = convert_to_lower(existing_column_input)

        # Hive-specific transformation: convert col_name to column_name for consistency
        if connection_type == ConnectionType.Hive.value:
            transformed_columns = []
            for column in existing_column_input:
                if isinstance(column, dict):
                    # Add column_name key with col_name value for consistency
                    column["column_name"] = column.get("col_name")
                    transformed_columns.append(column)
                else:
                    transformed_columns.append(column)
            existing_column_input = transformed_columns

        if (
            connection_type == ConnectionType.Databricks.value
            and database_name.replace(".", "").replace("`", "") == "hive_metastore"
        ):
            existing_column_input = [
                {"column_name": obj["col_name"], "data_type": obj["data_type"]}
                for obj in existing_column_input
            ]
        elif connection_type == ConnectionType.Teradata.value:
            existing_column_input = parse_teradata_columns(
                existing_column_input, destination_queries
            )

    existing_columns = [
        str(column.get("column_name")).lower() for column in existing_column_input
    ]
    source_colums = deepcopy(existing_columns)

    row_data_keys = list(row_data.keys()) if row_data else []
    use_asset_attributes = False
    if is_mongo_to_sql and not row_data_keys and asset_attribute_names:
        use_asset_attributes = True
        
    index = 0
    keys_to_process = asset_attribute_names if use_asset_attributes else row_data_keys
    
    for key in keys_to_process:
        if index >= max_column_limit:
            break

        index = index + 1
        
        if use_asset_attributes:
            value = row_data.get(key) if row_data else None
            if value is None:
                for row_key in row_data_keys:
                    if str(row_key).lower() == str(key).lower():
                        value = row_data.get(row_key)
                        break
        else:
            value = row_data.get(key) if key in row_data else None
        # Check for case-insensitive column existence across all database types
        if is_same_source:
            existing_col_names = [col.get("column_name", "") for col in existing_column_input]
            column_exists = key in existing_col_names
        else:
            existing_col_names = [col.get("column_name", "").upper() for col in existing_column_input]
            column_exists = key.upper() in existing_col_names

        # Rename existing failed rows column was not matched with the key if any
        existing_failed_rows_column = next((
                col for col in failed_rows_columns if col.get("column_name", "").lower() == key.lower()
            ), None)
        failed_row_column_name = existing_failed_rows_column.get("column_name") if existing_failed_rows_column else None
        if existing_failed_rows_column and failed_row_column_name.lower() != key.lower():
            alter_query = (alter_column_query.replace("<source_column>", failed_row_column_name)
                        .replace("<target_column>", key))
            execute_native_query(
                destination_config, alter_query, destination_connection, True, no_response=True,
            )
            # If the column exists in existing_column_input, update its column_name to key
            for col in existing_column_input:
                if col.get("column_name", "").lower() == failed_row_column_name.lower():
                    col["column_name"] = key
                    break
            continue
            
        if column_exists or str(key).lower() == "row_number":
            continue

        if not use_asset_attributes and check_selected_attributes and str(key).lower() not in asset_attribute_names:
            continue

        datatype = None
        if source_column_input:
            source_column = next(
                (
                    source_column
                    for source_column in source_column_input
                    if source_column.get("column_name")
                    and str(source_column.get("column_name")).lower()
                    == str(key).lower()
                ),
                None,
            )
            if source_column:
                native_datatype = source_column.get("data_type")
                native_datatype = native_datatype if native_datatype else ""
                if native_datatype:
                    derived_type = get_derived_type(dq_datatypes, native_datatype)
                    native_datatype = str(derived_type).lower() if derived_type else ""
                    if native_datatype in ["bit", "flag"]:
                        datatype = "boolean"
                    elif native_datatype in ["integer", "numeric"]:
                        datatype = str(derived_type).lower()
                    elif native_datatype in ["date", "time", "datetime"]:
                        datatype = (
                            str(derived_type).lower()
                            if str(derived_type).lower() in datatypes_mapping
                            else "string"
                        )
                    else:
                        if native_datatype:
                            datatype = "string"

        if "java" and "AS400JDBCClobLocator" in str(type(value)):
            value = value.getSubString(1, int(value.length()))
        elif "java" and "java.math.BigDecimal" in str(type(value)):
            value = 0
        if not datatype and value is not None:
            row_item = next(
                (
                    row
                    for row in input_data
                    if row.get(key) is not None and row.get(key) != ""
                ),
                None,
            )
            if row_item:
                value = row_item.get(key)

            datatype = "string"
            if isinstance(value, str) or value is None or value == "":
                datatype = "string"
            elif re.match("^[-]?[0-9]+([.]?[0-9]+)*$", str(value)):
                datatype = "numeric"
                if re.match("^[-]?[0-9]+$", str(value)):
                    datatype = "integer"
            elif str(value).lower() in ["true", "false"]:
                if connection_type == ConnectionType.BigQuery.value and source_connection_type == ConnectionType.Snowflake.value:
                    datatype = "string"
                else:
                    datatype = "boolean"
            else:
                datatype = "string"
        if (
            (
                connection_type == ConnectionType.Redshift.value
                or connection_type == ConnectionType.Redshift_Spectrum.value
            )
            and value != ""
            and type(value) == bool
        ):
            datatype = "boolean"
        native_derived_datatype = derivedtype_mapping.get(datatype)
        native_datatype = datatypes_mapping.get(datatype)
        if not native_datatype:
            native_datatype = datatypes_mapping.get("string")
        if not native_derived_datatype:
            native_derived_datatype = derivedtype_mapping.get("string")
        # Format column name according to database-specific conventions
        uppercase_dbs = [ConnectionType.Oracle.value]
        databricks_dbs = ["databricks", ConnectionType.Databricks.value]


        if connection_type in uppercase_dbs:
            formatted_column_name = key.upper()
        elif connection_type in databricks_dbs:
            formatted_column_name = f"`{key}`"  # Databricks needs backticks in ALTER TABLE
        else:
            formatted_column_name = key

        alter_query = deepcopy(alter_failed_rows_table)
        alter_query = (
            alter_query.replace("<table_name>", failed_rows_table)
            .replace("<database_name>", database_name)
            .replace("<schema_name>", schema_name)
            .replace("<col_name>", formatted_column_name)
            .replace("<col_type>", native_datatype)
        )
        try:
            if config.get("destination_connection_object").get("type").lower() in [
                ConnectionType.BigQuery.value
            ]:
                time.sleep(20)
            execute_native_query(
                destination_config,
                alter_query,
                destination_connection,
                True,
                no_response=True,
            )
        except Exception as e:
            log_error(f"Add column issue fixed{str(e)}", e)

        # Use the formatted column name for consistency if it contain quotes or backticks
        final_column_name = formatted_column_name
        # Remove quotes/backticks for storage since get_attribute_names will add them back
        if final_column_name and final_column_name[0] in ('"', '`') and final_column_name[0] == final_column_name[-1]:
            final_column_name = final_column_name[1:-1]

        source_colums.append(final_column_name)
        existing_column_input.append(
            {"column_name": final_column_name, "data_type": native_derived_datatype}
        )
    columns = list(map(lambda column: column.get("column_name"), existing_column_input))
    distinct_columns = list(set(columns))

    distinct_column_input = []
    for column in distinct_columns:
        existing_column = next(
            (
                column_input
                for column_input in existing_column_input
                if column_input.get("column_name") == column
            ),
            None,
        )
        if not existing_column:
            continue
        distinct_column_input.append(existing_column)

    for summarized_column in summarized_failed_rows_columns.keys():
        if any(summarized_column.lower() == (col.get("column_name", "").lower()) for col in existing_column_input):
            continue
        summarized_datatype = summarized_failed_rows_columns[summarized_column]
        failed_row_alter_query = deepcopy(alter_failed_rows_table)

        if connection_type == ConnectionType.Databricks.value:
            column_name = f"`{summarized_column}`"  # Databricks needs backticks in ALTER TABLE
        else:
            column_name = summarized_column

        failed_row_alter_query = (
            failed_row_alter_query.replace("<table_name>", failed_rows_table)
            .replace("<database_name>", database_name)
            .replace("<schema_name>", schema_name)
            .replace("<col_name>", column_name)
            .replace("<col_type>", summarized_datatype)
        )
        try:
            if config.get("destination_connection_object").get("type").lower() in [
                ConnectionType.BigQuery.value
            ]:
                time.sleep(20)
            execute_native_query(
                destination_config,
                failed_row_alter_query,
                destination_connection,
                True,
                no_response=True,
            )
        except Exception as e:
            log_error(f"Add summarized column failed {str(e)}", e)

    # Use the actual column name for postgresql
    if row_data and connection_type in [
        ConnectionType.Postgres.value,
        ConnectionType.AlloyDB.value,
    ]:
        input_columns = list(row_data.keys())
        input_columns = input_columns if input_columns else []

        for column in input_columns:
            if not column:
                continue

            input_column = next(
                (
                    distinct_column
                    for distinct_column in distinct_column_input
                    if distinct_column
                    and distinct_column.get("column_name") == str(column).lower()
                ),
                None,
            )
            if not input_column:
                continue
            input_column.update({"column_name": column})
    return distinct_column_input



def get_metadata(config: dict, measure: dict = {}):
    """
    Return the metadata for the given asset or measure
    """
    level = config.get("level")
    query_string = ""
    metadata = {}
    if str(level).lower() == "measure":
        measure_id = config.get("measure_id")
        query_string = f"""
            select mes.id, COALESCE(json_agg(DISTINCT jsonb_build_object('id',domain.id,'name',domain.technical_name, 'order', mdom.id)) FILTER (WHERE domain.id IS NOT NULL), '[]') AS domains,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id',application.id,'name',application.name)) FILTER (WHERE application.id IS NOT NULL), '[]') as applications,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id',product.id,'name',product.technical_name)) FILTER (WHERE product.id IS NOT NULL), '[]') as products
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            left join core.domain_mapping as mdom on mdom.measure_id=mes.id
            left join core.application_mapping as mapp on mapp.measure_id=mes.id
            left join core.product_mapping as pmap on pmap.measure_id=mes.id
            left join core.domain on domain.id=mdom.domain_id
            left join core.application on application.id=mapp.application_id
            left join core.product on product.id=pmap.product_id
            where base.level='measure' and mes.id='{measure_id}'
            group by mes.id
        """
    else:
        asset_id = config.get("asset_id")
        
        # Determine the asset condition based on whether asset_id is present
        if not asset_id or asset_id == "None" or asset_id is None:
            asset_condition = "asset.id is null"
        else:
            asset_condition = f"asset.id='{asset_id}'"
            
        query_string = f"""
            select asset.id, COALESCE(json_agg(DISTINCT jsonb_build_object('id',domain.id,'name',domain.technical_name, 'order', vdom.id)) FILTER (WHERE domain.id IS NOT NULL), '[]') AS domains,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id',application.id,'name',application.name)) FILTER (WHERE application.id IS NOT NULL), '[]') as applications,
            COALESCE(json_agg(DISTINCT jsonb_build_object('id',product.id,'name',product.technical_name)) FILTER (WHERE product.id IS NOT NULL), '[]') as products
            from core.asset
            left join core.domain_mapping as vdom on vdom.asset_id=asset.id
            left join core.product_mapping as pmap on pmap.asset_id=asset.id
            left join core.application_mapping as vapp on vapp.asset_id=asset.id
            left join core.domain on domain.id=vdom.domain_id 
            left join core.product on product.id=pmap.product_id
            left join core.application on application.id=vapp.application_id
            where {asset_condition}
            group by asset.id
        """
        attribute_id = measure.get("attribute_id")
        if attribute_id and asset_id:
            query_string = f"""
                select attribute.id,
                COALESCE(json_agg(DISTINCT jsonb_build_object('id',domain.id,'name',domain.technical_name, 'order', vdom.id)) FILTER (WHERE domain.id IS NOT NULL), '[]') AS domains,
                COALESCE(json_agg(DISTINCT jsonb_build_object('id',product.id,'name',product.technical_name)) FILTER (WHERE product.id IS NOT NULL), '[]') AS products,
                COALESCE(json_agg(DISTINCT jsonb_build_object('id',application.id,'name',application.name)) FILTER (WHERE application.id IS NOT NULL), '[]') as applications,
                COALESCE(json_agg(DISTINCT jsonb_build_object('id',tags.id,'name',tags.technical_name)) FILTER (WHERE tags.id IS NOT NULL), '[]') as tags,
                COALESCE(json_agg(DISTINCT jsonb_build_object('id',term.id,'name',term.technical_name)) FILTER (WHERE term.id IS NOT NULL), '[]') as terms
                from core.attribute
				join core.asset on asset.id = attribute.asset_id
				left join core.tags_mapping as amtags on amtags.attribute_id=attribute.id
                left join core.tags as tags on tags.id=amtags.tags_id
                left join core.domain_mapping as vdom on vdom.asset_id=asset.id
                left join core.application_mapping as vapp on vapp.asset_id=asset.id
                left join core.product_mapping as pmap on pmap.asset_id=asset.id
                left join core.domain on domain.id=vdom.domain_id
                left join core.application on application.id=vapp.application_id
                left join core.product on product.id=pmap.product_id
				left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
                left join core.terms as term on term.id=terms_mapping.term_id and terms_mapping.approved_by is not null
                where attribute.id='{attribute_id}' and asset.id='{asset_id}'
                group by attribute.id
            """
    if not query_string:
        return metadata

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor = execute_query(connection, cursor, query_string)
        metadata = fetchone(cursor)
    metadata = metadata if metadata else {}
    return metadata


def update_metadata(failed_rows_metadata, metadata):
    """
    Appends the metadata fields into the failed rows table
    """
    terms = metadata.get("terms")
    terms = json.loads(terms) if terms and isinstance(terms, str) else terms
    terms = terms if terms else []

    tags = metadata.get("tags")
    tags = json.loads(tags) if tags and isinstance(tags, str) else tags
    tags = tags if tags else []

    domains = metadata.get("domains")
    domains = json.loads(domains) if domains and isinstance(domains, str) else domains
    domains = domains if domains else []

    products = metadata.get("products")
    products = (
        json.loads(products) if products and isinstance(products, str) else products
    )
    products = products if products else []

    applications = metadata.get("applications")
    applications = (
        json.loads(applications)
        if applications and isinstance(applications, str)
        else applications
    )
    applications = applications if applications else []

    if terms:
        term_names = [term.get("name") for term in terms if term.get("name")]
        term_names = list(set(term_names)) if term_names else []
        term_name = ",".join(term_names) if term_names else ""
        term_name = term_name if term_name else ""
        failed_rows_metadata.update({"terms": term_name})

    if tags:
        tag_names = [tag.get("name") for tag in tags if tag.get("name")]
        tag_names = list(set(tag_names)) if tag_names else []
        tag_name = ",".join(tag_names) if tag_names else ""
        tag_name = tag_name if tag_name else ""
        failed_rows_metadata.update({"tags": tag_name})

    if domains:
        domain_names = [domain.get("name") for domain in domains if domain.get("name")]
        domain_names = list(set(domain_names)) if domain_names else []
        domain_name = ",".join(domain_names) if domain_names else ""
        domain_name = domain_name if domain_name else ""
        failed_rows_metadata.update({"domains": domain_name})

    if applications:
        application_names = [
            application.get("name")
            for application in applications
            if application.get("name")
        ]
        application_names = list(set(application_names)) if application_names else []
        application_name = ",".join(application_names) if application_names else ""
        application_name = application_name if application_name else ""
        failed_rows_metadata.update({"applications": application_name})

    if products:
        product_names = [
            product.get("name") for product in products if product.get("name")
        ]
        product_names = list(set(product_names)) if product_names else []
        product_name = ",".join(product_names) if product_names else ""
        product_name = product_name if product_name else ""
        failed_rows_metadata.update({"products": product_name})
    return failed_rows_metadata


def rename_table(config: dict, queries: dict, destination_connection=None):
    """
    Rename failed rows table
    """
    rename_table_query = queries.get("rename_table")
    if not rename_table_query:
        return

    try:
        destination_config = config.get("destination_config")
        connection_type = destination_config.get("connection_type")
        connection_type = connection_type.lower() if connection_type else ""
        schema_name = config.get("failed_rows_schema")
        failed_rows_table = config.get("failed_rows_table")
        new_failed_rows_table = get_failed_rows_table_name(config)
        if failed_rows_table != new_failed_rows_table:
            rename_table_query = (
                rename_table_query.replace("<table_name>", failed_rows_table)
                .replace("<new_table_name>", new_failed_rows_table)
                .replace("<schema_name>", schema_name)
            )
            _, native_connection = execute_native_query(
                destination_config,
                rename_table_query,
                destination_connection,
                True,
                no_response=True,
            )
            if not destination_connection and native_connection:
                destination_connection = native_connection
            update_failed_rows_table_name(config, new_failed_rows_table, False)
    except Exception as e:
        log_error(f" rename table - Failed to rename table - Error {str(e)}", e)


def has_limit(connection_type: str, query: str):
    """
    Check the query has limit statement or not
    """
    if connection_type == ConnectionType.MSSQL.value:
        return " top " in query.lower() or " fetch next " in query.lower()
    elif connection_type == ConnectionType.Synapse.value:
        return " sample " in query.lower()
    elif connection_type == ConnectionType.Oracle.value:
        return " fetch first " in query.lower()
    else:
        return " limit " in query.lower()


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

def lookup_processing_query(
    config: dict,
    measure: dict,
    invalid_select_query,
    invalid_select_query_with_limit,
    limit_condition_query,
    measure_export_row_limit,
    is_same_source,
):
    invalid_select_query = invalid_select_query.replace("inner join", "left join")
    invalid_select_query_with_limit = invalid_select_query_with_limit.replace(
        "inner join", "left join"
    )
    query = invalid_select_query.split("t2 on")
    base_condition = query[1].strip()
    query = base_condition.split("t2.")

    conditions = "not"
    # if not measure.get("is_positive", True):
    #     conditions = ""

    t2_table_attr = f""""""
    for idx, x in enumerate(query):
        if idx > 0:
            result = re.match('("[a-zA-Z0-9]+ [a-zA-Z0-9])', x)
            if result:
                attribute_value = x.split('" ')
                current_attribute_value = (
                    attribute_value[0]
                    if attribute_value[0][-1] == '"'
                    else f'''{attribute_value[0]}"'''
                )
                t2_table_attr = (
                    f"""{t2_table_attr}t2.{current_attribute_value} is not null and """
                )
            else:
                attribute_value = x.split(" ")
                t2_table_attr = (
                    f"""{t2_table_attr}t2.{attribute_value[0]} is not null and """
                )
    t2_table_attr = t2_table_attr[:-5]

    if " where " in invalid_select_query.lower():
        validate_query = (
            deepcopy(invalid_select_query)
            .replace("\n", "")
            .replace("\t", "")
            .replace(" ", "")
        )
        if "wherecasewhen" in validate_query.lower():
            invalid_select_query = update_table_name_incremental_conditions(
                invalid_select_query
            )
        elif "to_timestamp" in invalid_select_query:
            invalid_select_query = invalid_select_query.replace(
                " WHERE to_timestamp(", " WHERE to_timestamp(t1."
            )

    if " where " in base_condition.lower():
        base_condition = base_condition.split(" WHERE ")
        base_condition = base_condition[0]
        invalid_select_query = f"""{invalid_select_query} and {conditions} ({base_condition} and {t2_table_attr})"""
        invalid_select_query_with_limit = f"""{invalid_select_query} and {conditions} ({base_condition} and {t2_table_attr})"""
    else:
        invalid_select_query = f"""{invalid_select_query} where {conditions} ({base_condition} and {t2_table_attr})"""
        invalid_select_query_with_limit = f"""{invalid_select_query_with_limit} where {conditions} ({base_condition} and {t2_table_attr})"""

    invalid_select_query = invalid_select_query.replace("<query_string>", "t1.*")

    invalid_select_query_with_limit = invalid_select_query.replace(
        "<query_string>", "t1.*"
    )

    """ Lookup synapse database order cause issue so re set empty here """
    destination_connection_object = config.get("destination_connection_object", {})

    if destination_connection_object and destination_connection_object.get("type").lower() in [
        ConnectionType.Synapse.value
    ]:
        limit_condition_query = ""

    connection_type = config.get("connection_type", "")
    from dqlabs.utils.export_failed_rows.measure_helpers import merge_limit_query
    invalid_select_query_with_limit = merge_limit_query(
        invalid_select_query_with_limit, limit_condition_query, connection_type
    )
    invalid_select_query = merge_limit_query(
        invalid_select_query, limit_condition_query, connection_type
    )
    invalid_select_query_with_limit = (
        invalid_select_query_with_limit.replace(
            "<count>", str(10)
        )
        .replace("<metadata_attributes>", "")
        .replace("<query_attributes>", "*")
    )

    invalid_select_query = (
        invalid_select_query.replace("<count>", str(measure_export_row_limit))
        .replace("<metadata_attributes>", "")
        .replace("<query_attributes>", "*")
    )

    lookup_group_query = "t1.*"

    if "<lookup_table_name>" in invalid_select_query:
        database, schema, base_table_name = get_lookup_profile_database(config)
        lookup_properties = measure.get("properties", {})
        lookupMetadata = lookup_properties.get("lookupMetadata", {})
        lookup_id = lookupMetadata.get("id", "")
        lookup_table_name = f"""lookup_{lookup_properties.get("lookupOption", "")}_{lookup_id.replace("-","")}"""
        lookup_table_name = get_lookup_table_name(config, lookup_table_name)
        connection_type = config.get("connection_type")
        if connection_type ==ConnectionType.SalesforceDataCloud.value:
            lib_asset = lookup_properties.get("libasset_name","")
            lookup_table_name = lib_asset
        lookup_assets = {
            "profile_schema_name": schema,
            "profile_database_name": database,
            "lookup_table": lookup_table_name,
        }
        lookup_full_table_name = generate_lookup_query_connectionbased(
            config, lookup_assets
        )

        invalid_select_query = invalid_select_query.replace(
            "<lookup_table_name>", lookup_full_table_name
        )

        invalid_select_query_with_limit = invalid_select_query_with_limit.replace(
            "<lookup_table_name>", lookup_full_table_name
        )

    return invalid_select_query, invalid_select_query_with_limit, lookup_group_query


def convert_json_string(value: str, property_name: str = "", delimeter: str = ""):
    value = json.loads(value) if isinstance(value, str) else value
    value = value if value else []
    if property_name:
        value = [x.get(property_name) for x in value]
    if value:
        value = [x for x in value if x]
    value = f"{delimeter}".join(value) if delimeter and value else ""
    return value


def get_measure_fields(config: dict):
    """
    Returns the Custom Fields for the measure
    """
    measure_custom_fields = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select name from core.fields where level='Measure'
        """
        cursor = execute_query(connection, cursor, query_string)
        measure_custom_fields = fetchall(cursor)
    return measure_custom_fields


def get_asset_fields(config: dict):
    """
    Returns the Custom Fields for the Asset
    """
    custom_fields = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = (
            f"select name from core.fields where level='Asset'"
        )
        cursor = execute_query(connection, cursor, query_string)
        custom_fields = fetchall(cursor)
    return custom_fields


def update_custom_field_columns(config: dict, params: dict):
    """
    Update Asset Custom Field Columns
    """
    try:
        destination_config = params.get("destination_config")
        destination_connection = params.get("destination_connection")
        failed_rows_query = params.get("queries")
        level = params.get("level")
        destination_connection_object = config.get("destination_connection_object")
        schema_name = config.get("failed_rows_schema")
        database_name = config.get("metadata_database")
        connection_type = config.get("connection_type")
        if connection_type == ConnectionType.DB2IBM.value and not database_name:
            database_name = "sample"
        if (
            destination_connection_object.get("type") == ConnectionType.Snowflake.value
            and not database_name
        ):
            destination_connection_database = get_dq_connections_database(
                config, destination_config.get("connection", {}).get("id")
            )
            database_name = destination_connection_database.get("name")
        # get all new custom fields
        custom_fields = get_new_custom_fields(config)
        if not custom_fields:
            return

        # get all existing columns
        existing_columns_query = failed_rows_query.get("existing_metadata_columns")
        if not existing_columns_query:
            return
        existing_columns_query = existing_columns_query.replace(
            "<schema_name>", schema_name
        ).replace("<database_name>", database_name)
        existing_fields, _ = execute_native_query(
            destination_config,
            existing_columns_query,
            destination_connection,
            is_list=True,
        )

        if (
            connection_type == ConnectionType.Databricks.value
            and database_name == "hive_metastore"
        ):
            existing_fields = []
            hive_queries = get_queries({**config, "connection_type": connection_type})
            existing_columns_query = (
                hive_queries.get("hive_metastore", {})
                .get("failed_rows", {})
                .get("existing_metadata_columns", "")
            )
            meta_tables = ["ASSET_METADATA", "ATTRIBUTE_METADATA", "MEASURE_METADATA"]
            for meta_table in meta_tables:
                hive_columns_query = (
                    existing_columns_query.replace("<schema_name>", schema_name)
                    .replace("<database_name>", database_name)
                    .replace("<table_name>", meta_table)
                )
                hive_existing_fields, _ = execute_native_query(
                    destination_config,
                    hive_columns_query,
                    destination_connection,
                    is_list=True,
                )
                hive_existing_fields = convert_to_lower(hive_existing_fields)
                hive_columns = [
                    {"column_name": obj["col_name"], "table_name": meta_table}
                    for obj in hive_existing_fields
                ]
                existing_fields.extend(hive_columns)

        existing_fields = convert_to_lower(existing_fields)
        existing_metadata_columns = deepcopy(existing_fields) if existing_fields else []
        params.update({"existing_metadata": existing_metadata_columns})

        metadata_table_schema = {}
        for column in existing_fields:
            key = column.get("table_name").lower()
            value = column.get("column_name")
            existing_values = metadata_table_schema.get(key)
            existing_values = existing_values if existing_values else []
            existing_values.append(value)
            existing_values = list(set(existing_values)) if existing_values else []
            metadata_table_schema.update({key: existing_values})
        existing_fields = metadata_table_schema if metadata_table_schema else {}
        source_datatype = generate_lookup_source_datatype(
            {"connection_type": destination_connection_object.get("type").lower()}
        )
        destination_connection_type = destination_connection_object.get("type")

        for custom_field in custom_fields:
            level = custom_field.get("level").lower()
            fields = custom_field.get("names")
            fields = list(fields) if fields else []
            if not fields:
                continue

            metadata_table_name = f"""{level}_metadata"""
            column_update_query = failed_rows_query.get(f"alter_{metadata_table_name}")
            if not column_update_query:
                continue
            level_existing_fields = existing_fields.get(metadata_table_name)

            if not level_existing_fields:
                continue

            pending_fields = [
                field for field in fields if field not in level_existing_fields
            ]

            if not pending_fields:
                continue

            if destination_connection_type == ConnectionType.Snowflake.value:
                pending_attribute_names = get_attribute_names(
                    destination_connection_type, pending_fields
                )
                new_column_names = ", \n".join(
                    [
                        f"{attribute_name} {source_datatype}"
                        for attribute_name in pending_attribute_names
                    ]
                )
                update_query = (
                    column_update_query.replace("<schema_name>", schema_name)
                    .replace("<database_name>", database_name)
                    .replace("<columns_str>", new_column_names)
                )
                try:
                    execute_native_query(
                        destination_config,
                        update_query,
                        destination_connection,
                        True,
                        no_response=True,
                    )
                except Exception as e:
                    log_error(f"Add column issue fixed{str(e)}", e)
            else:
                column_update_query = column_update_query.replace(
                    "<schema_name>", schema_name
                ).replace("<database_name>", database_name)
                for column_name in pending_fields:
                    attribute_names = get_attribute_names(
                        destination_connection_type, [column_name]
                    )
                    if destination_connection_type == ConnectionType.DB2IBM.value:
                        column_name = f""""{attribute_names[0]}" {source_datatype}"""
                    else:
                        column_name = f"{attribute_names[0]} {source_datatype}"
                    update_query = column_update_query.replace(
                        "<columns_str>", column_name
                    )
                    try:
                        execute_native_query(
                            destination_config,
                            update_query,
                            destination_connection,
                            True,
                            no_response=True,
                        )
                    except Exception as e:
                        log_error(f"Add column issue fixed{str(e)}", e)

        # update the flag for all custom_fields
        update_fields = sum([item.get("names") for item in custom_fields], [])
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            update_fields = [f"'{column}'" for column in update_fields if column]
            fields = ",".join(update_fields)
            query_string = (
                f"UPDATE core.fields SET is_created=true where name IN ({fields})"
            )
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Failed update_custom_field_columns {str(e)}", e)


def update_new_metadata_columns(config: dict, params: dict):
    """
    Update Asset, Attribute and Measure Metadata Columns
    """
    try:
        existing_metadata_fields = params.get("existing_metadata")
        existing_metadata_fields = (
            existing_metadata_fields if existing_metadata_fields else []
        )
        destination_config = params.get("destination_config")
        destination_connection = params.get("destination_connection")
        destination_connection_type = destination_config.get("connection_type")
        failed_rows_query = params.get("queries")
        schema_name = config.get("failed_rows_schema")
        # report_settings = config.get("dag_info").get("report_settings")
        database_name = config.get("metadata_database")
        connection_type = config.get("connection_type")
        if (
            destination_connection_type == ConnectionType.DB2IBM.value
            and not database_name
        ):
            database_name = "sample"
        if (
            destination_connection_type == ConnectionType.Snowflake.value
            and not database_name
        ):
            destination_connection_database = get_dq_connections_database(
                config, destination_config.get("connection", {}).get("id")
            )
            database_name = destination_connection_database.get("name")
        database = database_name.replace('"', "")

        levels = ["asset", "attribute", "measure"]
        existing_columns_query = failed_rows_query.get("existing_metadata_columns")
        if not existing_metadata_fields:
            if not existing_columns_query:
                return
            existing_columns_query = existing_columns_query.replace(
                "<schema_name>", schema_name
            ).replace("<database_name>", database)
            existing_metadata_fields, _ = execute_native_query(
                destination_config,
                existing_columns_query,
                destination_connection,
                is_list=True,
            )

            if (
                destination_connection_type == ConnectionType.Databricks.value
                and database == "hive_metastore"
            ):
                existing_metadata_fields = []
                hive_queries = get_queries(
                    {**config, "connection_type": destination_connection_type}
                )
                existing_columns_query = (
                    hive_queries.get("hive_metastore", {})
                    .get("failed_rows", {})
                    .get("existing_metadata_columns", "")
                )
                meta_tables = [
                    "ASSET_METADATA",
                    "ATTRIBUTE_METADATA",
                    "MEASURE_METADATA",
                ]
                for meta_table in meta_tables:
                    hive_columns_query = (
                        existing_columns_query.replace("<schema_name>", schema_name)
                        .replace("<database_name>", database_name)
                        .replace("<table_name>", meta_table)
                    )
                    hive_existing_fields, _ = execute_native_query(
                        destination_config,
                        hive_columns_query,
                        destination_connection,
                        is_list=True,
                    )
                    hive_existing_fields = convert_to_lower(hive_existing_fields)
                    hive_columns = [
                        {"column_name": obj["col_name"], "table_name": meta_table}
                        for obj in hive_existing_fields
                    ]
                    existing_metadata_fields.extend(hive_columns)

            existing_metadata_fields = convert_to_lower(existing_metadata_fields)

        for level in levels:
            level = level.lower()
            metadata_table_name = f"{level}_metadata"
            existing_fields = list(
                filter(
                    lambda column: column.get("table_name").lower()
                    == metadata_table_name,
                    existing_metadata_fields,
                )
            )
            existing_fields = [column.get("column_name") for column in existing_fields]
            columns = failed_rows_query.get(f"{level}_metadata_columns", {})
            if not columns:
                continue

            column_update_query = failed_rows_query.get(f"alter_{level}_metadata")
            if not column_update_query:
                continue
            column_update_query = column_update_query.replace(
                "<schema_name>", schema_name
            ).replace("<database_name>", database_name)
            column_names = list(columns.keys())
            for column_name in column_names:
                if not any(
                    existing.lower() == column_name.lower()
                    for existing in existing_fields
                ):
                    datatype = columns[column_name]
                    attribute_names = get_attribute_names(
                        destination_connection_type, [column_name]
                    )
                    column_name = f"{attribute_names[0]} {datatype}"
                    update_query = column_update_query.replace(
                        "<columns_str>", column_name
                    )
                    execute_native_query(
                        destination_config,
                        update_query,
                        destination_connection,
                        True,
                        no_response=True,
                    )
    except Exception as e:
        log_error(f"Failed update_new_metadata_columns - {str(e)}", e)


def get_new_custom_fields(config):
    """
    Return all custom fields which are not created yet/shifted to metadata tables
    """
    custom_fields = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"select LOWER(level) as level, json_agg(name) as names from core.fields where is_active=true group by level"
        cursor = execute_query(connection, cursor, query_string)
        custom_fields = fetchall(cursor)
    return custom_fields


def get_unsupported_attributes(config: dict):
    """
    Return columns which has unsupported datatypes
    """
    unsupported_datatypes = [
        "variant",
        "object",
        "geography",
        "geometry",
        "varbinary",
        "sql_variant",
        "json",
        "struct",
        "map",
        "array",
        "blob",
        "binary",
        "super",
        "bindata"
    ]
    unsupported_columns = []
    connection = get_postgres_connection(config)
    if config.get("asset_id", None):
        with connection.cursor() as cursor:
            query_string = f"""
                select name from core.attribute
                where asset_id = '{config.get('asset_id')}' and is_active = true and is_delete = false
                and lower(datatype) in ({', '.join([f"'{unsupported_datatype}'" for unsupported_datatype in unsupported_datatypes])})
            """
            cursor = execute_query(connection, cursor, query_string)
            unsupported_columns = fetchall(cursor)
            if unsupported_columns:
                unsupported_columns = [
                    column.get("name").lower() for column in unsupported_columns
                ]
    return unsupported_columns

def get_connection_metadata(config: dict):
    """
    Insert the connection metadata for the current run
    """
    connection = config.get("connection")
    queue_id = config.get("queue_id")

    connection_psql = get_postgres_connection(config)
    with connection_psql.cursor() as cursor:
        query_string = f"""
            select id, name, type, is_valid, created_date
            from core.connection
            where id='{connection.get("id")}' and is_delete='false' and is_default='false'
        """
        cursor = execute_query(connection_psql, cursor, query_string)
        connection_data = fetchone(cursor)
        connection_data = connection_data if connection_data else {}
        if connection_data:
            connection_data = {
                "CONNECTION_ID": connection_data.get("id"),
                "CONNECTION_NAME": connection_data.get("name"),
                "DATASOURCE": connection_data.get("type"),
                "STATUS": "Valid" if connection_data.get("is_valid") else "Deprecated",
                "CREATED_DATE": str(connection_data.get("created_date")),
                "RUN_ID": queue_id
            }
    return [connection_data]

def get_asset_metadata(config:dict):
    connection = config.get("connection")
    queue_id = config.get("queue_id")
    delimeter = config.get("delimeter")
    asset_id = config.get("asset_id")

    # Determine the asset condition based on whether asset_id is present
    if not asset_id or asset_id == "None" or asset_id is None:
        asset_condition = "ast.id is null"
        comments_condition = "conversations.asset_id is null"
    else:
        asset_condition = f"ast.id='{asset_id}'"
        comments_condition = f"conversations.asset_id='{asset_id}'"

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            with comments as (
            select conversations.asset_id as asset_id,conversations.id as conversation_id, comment,conversations.title,conversations.created_date as created_at, rating,
            case when users.first_name is not null then concat(users.first_name,' ', users.last_name) else users.email end as username
            from core.conversations 
            join core.users on users.id=conversations.created_by_id
            where {comments_condition} and conversations.level='asset'
        )
            select con.id as connection_id, con.name as connection_name, con.type as connection_type,
                con.is_valid, con.credentials, ast.status, ast.created_date,
                ast.id as asset_id, ast.name as asset_name, ast.properties, 
                ast.score as asset_score, data.row_count as asset_total_rows, data.invalid_rows as asset_invalid_rows,
                ast.issues as asset_issues, ast.alerts as asset_alerts, data.primary_columns, ast.description,
                coalesce(string_agg(distinct users.id::TEXT, ','), '') as steward_users_id,
                coalesce(string_agg(distinct users.email::TEXT, ','), '') as steward_users,
                coalesce(string_agg(distinct domain.technical_name::TEXT, ','), '') domains,
                coalesce(string_agg(distinct product.technical_name::TEXT, ','), '')  product,
                coalesce(string_agg(distinct application.name::TEXT, ','), '') applications,
                coalesce(string_agg(distinct term.technical_name::TEXT, ','), '')  as terms,
                coalesce(json_agg(distinct jsonb_build_object('comment',comments.comment,'title', comments.title,'created_at', comments.created_at, 'rating',comments.rating, 'username',comments.username)) filter (where conversation_id is not null), '[]') as conversation
                from core.asset as ast
                join core.data on data.asset_id = ast.id
                join core.connection as con on con.id=ast.connection_id
            join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
            left join core.user_mapping on user_mapping.asset_id=ast.id
            left join core.users on users.id=user_mapping.user_id
            left join core.domain_mapping as vdom on vdom.asset_id=ast.id
            left join core.product_mapping as vpt on vpt.asset_id=ast.id
            left join core.application_mapping as vapp on vapp.asset_id=ast.id
            left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
            left join core.domain on domain.id=vdom.domain_id
            left join core.product on product.id=vpt.product_id
            left join core.application on application.id=vapp.application_id
            left join core.terms as term on term.id=vterms.term_id 
            left join comments on comments.asset_id=ast.id
                where {asset_condition}
        group by con.id, ast.id, data.id
        """
        cursor = execute_query(connection, cursor, query_string)
        metadata = fetchone(cursor)
        metadata = metadata if metadata else {}

        job_query_string = f"""
            select queue.start_time from core.request_queue as rq
            join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category not in ('metadata', 'export_failed_rows')
            where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
            order by queue.created_date desc limit 1
        """
        cursor = execute_query(connection, cursor, job_query_string)
        last_job_detail = fetchone(cursor)
        last_job_detail = last_job_detail if last_job_detail else {}

        if not metadata:
            return []
        
        conversations = metadata.get("conversation")
        comments = []
        for conversation in conversations:
            comment_text = ""
            comment = conversation.get("comment", {})
            comment = json.loads(comment)
            comment_block = comment.get("blocks", [])
            for block in comment_block:
                comment_text += block["text"] + "\n"
            comment_text = comment_text.replace("'", "") if comment_text else ""
            comments.append(
                {
                    "title": conversation.get("title"),
                    "rating": conversation.get("rating"),
                    "created_at": conversation.get("created_at"),
                    "username": conversation.get("username"),
                    "comment": comment_text,
                }
            )
        comments = json.dumps(comments) if comments else ""
        comments = comments.replace("'", "") if comments else ""

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

        asset_total_rows = metadata.get("asset_total_rows")
        asset_total_rows = asset_total_rows if asset_total_rows else 0
        asset_invalid_rows = metadata.get("asset_invalid_rows")
        asset_invalid_rows = asset_invalid_rows if asset_invalid_rows else 0
        passed_records = asset_total_rows - asset_invalid_rows

        asset_metadata = {
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

        # Get Custom Field Value
        custom_fields = []
        with connection.cursor() as cursor:
            query_string = f"""
                select
                    fields.name,
                    field_property.value
                from
                    core.fields
                    left join core.field_property on field_property.field_id = fields.id
                    and field_property.asset_id = '{asset_id}'
                    and fields.is_active is true
                where value is not null and level='Asset'
            """
            cursor = execute_query(connection, cursor, query_string)
            custom_fields = fetchall(cursor)
            custom_fields = custom_fields if custom_fields else []

        
        for field in custom_fields:
            value = field.get("value")
            name = field.get("name")
            if isinstance(value, str):
                value = value.replace('"', "").replace("'", "")
            asset_metadata.update({name: value})
        asset_metadata = [asset_metadata]
    return asset_metadata
    

def get_attribute_metadata_data(config: dict):
    asset_id = config.get("asset_id")
    
    # Determine the asset condition based on whether asset_id is present
    if not asset_id or asset_id == "None" or asset_id is None:
        asset_condition = "ast.id is null"
    else:
        asset_condition = f"ast.id='{asset_id}'"
        
    attribute_list = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select ast.id as asset_id, attr.id as attribute_id, attr.name as attribute_name,attr.status, attr.score as attribute_score,
            attr.alerts as attribute_alerts, attr.issues as attribute_issues,attr.description,term.technical_name as term,
            attr.row_count as attribute_total_rows, attr.invalid_rows as attribute_invalid_rows,attr.is_blank,
            attr.min_length,attr.max_length,attr.derived_type as datatype,attr.min_value,attr.max_value,attr.is_null,
            attr.is_primary_key as is_primary,attr.is_unique, coalesce(string_agg(distinct tags.technical_name::TEXT, ','), '') tags,
            attr.created_date
            from core.asset as ast
            join core.connection as con on con.id=ast.connection_id
            join core.attribute as attr on attr.asset_id=ast.id and attr.is_selected=true
            left join core.terms_mapping as vterms on vterms.attribute_id = attr.id and vterms.approved_by is not null
            left join core.tags_mapping as vtags on vtags.attribute_id=attr.id
            left join core.terms as term on term.id=vterms.term_id 
            left join core.tags as tags on tags.id=vtags.tags_id
            where {asset_condition} and attr.is_active=true
            group by ast.id,attr.id,term.id
        """
        cursor = execute_query(connection, cursor, query_string)
        attribute_metadata = fetchall(cursor)
        attribute_metadata = attribute_metadata if attribute_metadata else []
        job_query_string = f"""
            select queue.start_time, queue.status from core.request_queue as rq
            join core.request_queue_detail as queue on queue.queue_id=rq.id and queue.category = 'profile'
            where rq.asset_id='{asset_id}' and queue.status in ('Failed','Completed')
            order by queue.created_date desc limit 1
        """
        cursor = execute_query(connection, cursor, job_query_string)
        last_job_detail = fetchone(cursor)
        last_job_detail = last_job_detail if last_job_detail else {}

        if not attribute_metadata:
            return []
    
        for metadata in attribute_metadata:
            attribute_id = metadata.get("attribute_id")
            description = metadata.get("description")
            description = description.replace('"', "").replace("'", "''") if description else ""
            min_value = metadata.get("min_value")
            min_value = min_value if min_value else ""
            if min_value and isinstance(min_value, str):
                min_value = min_value.replace("\\", "\\\\").replace("'", "''")
            max_value = metadata.get("max_value")
            max_value = max_value if max_value else ""
            if max_value and isinstance(max_value, str):
                max_value = max_value.replace("\\", "\\\\").replace("'", "''")
            is_primary = metadata.get("is_primary") if metadata.get("is_primary") else False
            is_primary = int(is_primary)
            created_date = metadata.get("created_date") if metadata.get("created_date") else "NULL"
            if not metadata.get("datatype", ""):
                min_value = ""
                max_value = ""

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
                "STATUS": metadata.get("status"),
                "RUN_ID": config.get("queue_id"),
                "DESCRIPTION": description,
                "TERM": metadata.get("term") if metadata.get("term") else "",
                "MIN_LENGTH": metadata.get("min_length") if metadata.get("min_length") else "",
                "MAX_LENGTH": metadata.get("max_length") if metadata.get("max_length") else "",
                "DATATYPE": metadata.get('datatype') if metadata.get('datatype') else "TEXT",
                "MIN_VALUE": min_value,
                "MAX_VALUE": max_value,
                "IS_PRIMARY": is_primary,
                "TAGS": metadata.get("tags"),
                "LAST_RUN_TIME": str(last_job_detail.get("start_time")) if last_job_detail.get("start_time") else "NULL",
                "JOB_STATUS": last_job_detail.get("status") if last_job_detail.get("status") else "Completed",
                "CREATED_DATE": str(created_date),
            }

            custom_fields = []
            query_string = f"""
                select
                    fields.name,
                    field_property.value
                from
                    core.fields
                    left join core.field_property on field_property.field_id = fields.id
                    and field_property.attribute_id = '{attribute_id}'
                    and fields.is_active is true
                where value is not null and level='Attribute'
            """
            cursor = execute_query(connection, cursor, query_string)
            custom_fields = fetchall(cursor)
            custom_fields = custom_fields if custom_fields else []

            for field in custom_fields:
                value = field.get("value")
                name = field.get("name")
                if isinstance(value, str):
                    value = value.replace('"', "").replace("'", "")
                attribute.update({name: value})
            attribute_list.append(attribute)
    return attribute_list

def get_measure_metadata(config: dict, measures: list):
    connection = get_postgres_connection(config)
    measure_list = []
    with connection.cursor() as cursor:
        measures = (f"""({','.join(f"'{w}'" for w in measures)})""") if measures else ""
        query_string = f"""
            with max_start_time AS (
                SELECT mes.id as measure_id, max(queue_detail.start_time) AS max_start_time
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and queue_detail.category!='export_failed_rows'
                group by mes.id
            ),
            custom_measure_status AS (
                select mes.id, queue_detail.status
                from core.measure as mes
                left join core.request_queue_detail as queue_detail on mes.last_run_id = queue_detail.queue_id and mes.id=queue_detail.measure_id
                where queue_detail.status is not null and queue_detail.category!='export_failed_rows'
                group by mes.id, queue_detail.status 
            )
            select mes.connection_id as connection_id, ast.id as asset_id, attr.id as attribute_id,
            mes.id as measure_id, base.name as measure_name, mes.weightage, base.category, mes.created_date,
            mes.row_count as measure_total_count, mes.valid_rows AS measure_valid_count, mes.invalid_rows AS measure_invalid_count, 
            mes.score as measure_score, dim.name as dimension_name, mes.is_positive, custom.status as custom_measure_run_status,
            coalesce(string_agg(distinct domain.technical_name::TEXT, ','), '') domains,
            coalesce(string_agg(distinct product.technical_name::TEXT, ','), '')  product,
            coalesce(string_agg(distinct application.name::TEXT, ','), '') applications,
            coalesce(string_agg(distinct term.technical_name::TEXT, ','), '') terms,
            coalesce(string_agg(distinct tags.technical_name::TEXT, ','), '') tags,
            queue_detail.status as last_run_status, queue_detail.start_time as run_date, mes.status as measure_status, base.description, mes.is_active, base.profile,
            base.type, base.query, mes.semantic_query, mes.drift_threshold, base.properties
            from core.measure as mes
            join core.base_measure as base on base.id=mes.base_measure_id
            left join core.asset as ast on ast.id=mes.asset_id
            left join core.dimension as dim on dim.id=mes.dimension_id
            left join core.attribute as attr on attr.id=mes.attribute_id
            left join core.domain_mapping as vdom on (vdom.asset_id=ast.id or vdom.measure_id=mes.id)
            left join core.product_mapping as vpt on (vpt.asset_id=ast.id or vpt.measure_id=mes.id)
            left join core.application_mapping as vapp on (vapp.asset_id=ast.id or vapp.measure_id=mes.id)
            left join core.domain on domain.id = vdom.domain_id
            left join core.product on product.id = vpt.product_id
            left join core.application on application.id = vapp.application_id
            left join core.request_queue_detail as queue_detail on mes.last_run_id=queue_detail.queue_id
            left join custom_measure_status as custom on mes.id=custom.id
            left join core.terms_mapping on terms_mapping.attribute_id = attr.id
            left join core.terms as term on term.id=terms_mapping.term_id and terms_mapping.approved_by is not null
            left join core.tags_mapping as amtags on ((attr.id is not null and amtags.attribute_id = attr.id and amtags.level='attribute') or (attr.id is null and ast.id is not null and amtags.asset_id = ast.id and amtags.level='asset'))
            left join core.tags as tags on tags.id=amtags.tags_id
            join max_start_time ON mes.id = max_start_time.measure_id AND (queue_detail.start_time = max_start_time.max_start_time or (queue_detail.start_time is null and max_start_time.max_start_time is null))
            where mes.id in {measures} and mes.connection_id is not null
            group by mes.connection_id, ast.id, attr.id, mes.id, queue_detail.start_time, base.name, base.category, 
            dim.name, queue_detail.status, base.description, mes.created_date, custom.status, base.profile, base.type,
            base.query, base.properties
        """
        cursor = execute_query(connection, cursor, query_string)
        measure_list = fetchall(cursor)
        measure_list = measure_list if measure_list else []
    return measure_list


def get_summary_measure_data(config: dict, measures: list):
    connection = get_postgres_connection(config)
    measure_list = []
    with connection.cursor() as cursor:
        measures = (f"""({','.join(f"'{w}'" for w in measures)})""") if measures else ""
        query_string = f"""
                   WITH max_start_time AS (
                    SELECT mes.id AS measure_id, MAX(queue_detail.start_time) AS max_start_time
                    FROM core.measure AS mes
                    LEFT JOIN core.request_queue_detail AS queue_detail ON mes.last_run_id = queue_detail.queue_id AND queue_detail.category != 'export_failed_rows'
                    GROUP BY mes.id
                ),
                custom_measure_status AS (
                    SELECT mes.id, queue_detail.status
                    FROM core.measure AS mes
                    LEFT JOIN core.request_queue_detail AS queue_detail ON mes.last_run_id = queue_detail.queue_id AND mes.id = queue_detail.measure_id
                    WHERE queue_detail.status IS NOT NULL AND queue_detail.category != 'export_failed_rows'
                    GROUP BY mes.id, queue_detail.status 
                )
                SELECT 
                    mes.id AS measure_id, base.name AS measure_name, 
                    mes.weightage, base.category, mes.created_date, mes.row_count AS total_count, mes.valid_rows AS valid_count, 
                    mes.invalid_rows AS invalid_count, mes.score AS score, dim.name AS dimension_name, mes.is_positive, 
                    queue_detail.status AS last_run_status, queue_detail.start_time AS run_date, 
                    mes.status AS measure_status, base.description, mes.is_active,base.type,
                    -- Attribute fields
                    attr.id AS attribute_id, attr.name AS attribute_name,
                    -- Metric fields
    
                    m.query AS metric_query, m.message AS message,
                    -- Metadata fields
                    COALESCE(string_agg(distinct domain.technical_name::TEXT, ','), '') AS domains,
                    COALESCE(string_agg(distinct product.technical_name::TEXT, ','), '') AS products,
                    COALESCE(string_agg(distinct application.name::TEXT, ','), '') AS applications,
                    COALESCE(string_agg(distinct term.technical_name::TEXT, ','), '') AS terms,
                    COALESCE(string_agg(distinct tags.technical_name::TEXT, ','), '') AS tags
                FROM core.measure AS mes
                JOIN core.base_measure AS base ON base.id = mes.base_measure_id
                LEFT JOIN core.asset AS ast ON ast.id = mes.asset_id
                LEFT JOIN core.dimension AS dim ON dim.id = mes.dimension_id
                LEFT JOIN core.attribute AS attr ON attr.id = mes.attribute_id
                LEFT JOIN core.domain_mapping AS vdom ON (vdom.asset_id = ast.id OR vdom.measure_id = mes.id)
                LEFT JOIN core.product_mapping AS vpt ON (vpt.asset_id = ast.id OR vpt.measure_id = mes.id)
                LEFT JOIN core.application_mapping AS vapp ON (vapp.asset_id = ast.id OR vapp.measure_id = mes.id)
                LEFT JOIN core.domain ON domain.id = vdom.domain_id
                LEFT JOIN core.product ON product.id = vpt.product_id
                LEFT JOIN core.application ON application.id = vapp.application_id
                LEFT JOIN core.request_queue_detail AS queue_detail ON mes.last_run_id = queue_detail.queue_id
                LEFT JOIN custom_measure_status AS custom ON mes.id = custom.id
                LEFT JOIN core.terms_mapping ON terms_mapping.attribute_id = attr.id
                LEFT JOIN core.terms AS term ON term.id = terms_mapping.term_id AND terms_mapping.approved_by IS NOT NULL
                LEFT JOIN core.tags_mapping AS amtags ON (
                    (attr.id IS NOT NULL AND amtags.attribute_id = attr.id AND amtags.level = 'attribute') OR 
                    (attr.id IS NULL AND ast.id IS NOT NULL AND amtags.asset_id = ast.id AND amtags.level = 'asset')
                )
                LEFT JOIN core.tags AS tags ON tags.id = amtags.tags_id
                LEFT JOIN core.metrics AS m ON m.measure_id = mes.id AND m.run_id = mes.last_run_id AND m.is_measure = true
                JOIN max_start_time ON mes.id = max_start_time.measure_id AND (
                    queue_detail.start_time = max_start_time.max_start_time OR 
                    (queue_detail.start_time IS NULL AND max_start_time.max_start_time IS NULL)
                )
                WHERE mes.id IN {measures} AND mes.connection_id IS NOT NULL
                GROUP BY 
                    mes.connection_id, ast.id, attr.id, attr.name, mes.id, base.name, base.category, dim.name, queue_detail.status, 
                    base.description, mes.created_date, custom.status, base.profile, base.type, base.query, base.properties, 
                    queue_detail.start_time, mes.row_count, mes.valid_rows, mes.invalid_rows, mes.score, mes.is_positive, 
                    mes.status, mes.is_active, mes.semantic_query, mes.drift_threshold,
                    -- Metric fields in GROUP BY
                    m.id, m.organization_id, m.connection_id, m.asset_id, m.attribute_id, m.run_id, m.airflow_run_id, 
                    m.attribute_name, m.measure_name, m.level, m.value, m.weightage, m.total_count, m.valid_count, 
                    m.invalid_count, m.valid_percentage, m.invalid_percentage, m.score, m.status, m.is_archived, m.query, 
                    m.message, m.allow_score, m.is_drift_enabled, m.is_measure, m.is_active, m.is_delete, m.parent_attribute_id, 
                    m.created_date
                    """
        cursor = execute_query(connection, cursor, query_string)
        measure_list = fetchall(cursor)
        measure_list = measure_list if measure_list else []
        
        # Clean up metric_query field - remove excessive escaping
        for measure in measure_list:
            if isinstance(measure, dict) and measure.get('metric_query'): 
                # Remove excessive backslash escaping that can occur during JSON serialization
                metric_query = measure['metric_query']
                if isinstance(metric_query, str):
                    # Use regex to clean up all backslash escaping patterns
                    # Remove all backslash escaping from double quotes
                    metric_query = re.sub(r'\\+"', '"', metric_query)
                    # Clean up any remaining excessive backslashes
                    metric_query = re.sub(r'\\{2,}', '', metric_query)
                    measure['metric_query'] = metric_query
    return measure_list


def prepare_measure_metadata(config: dict, measures:list):
    queue_id = config.get('queue_id')
    measure_metadata = []
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        for metadata in measures:
            measure_id = metadata.get("measure_id")
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
            measure = {
                "CONNECTION_ID": metadata.get("connection_id"),
                "ASSET_ID": metadata.get("asset_id") if  metadata.get("asset_id") else '',
                "ATTRIBUTE_ID": metadata.get("attribute_id") if metadata.get("attribute_id") else '',
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
                "TOTAL_RECORDS": metadata.get("measure_total_count"),
                "PASSED_RECORDS": metadata.get("measure_valid_count"),
                "FAILED_RECORDS": metadata.get("measure_invalid_count"),
                "STATUS": "Valid" if metadata.get("is_positive") else "Invalid",
                "RUN_STATUS": last_run_status.replace(double_escap_character, empty_string).replace(single_escap_character, empty_string).replace("`", empty_string),
                "DOMAIN": metadata.get("domains"),
                "PRODUCT": metadata.get("product"),
                "APPLICATION": metadata.get("applications"),
                "COMMENT": comment,
                "RUN_ID": queue_id,
                "RUN_DATE": str(metadata.get("run_date")) if metadata.get("run_date") else None,
                "MEASURE_STATUS": measure_status,
                "IS_ACTIVE": is_measure_active,
                "PROFILE": metadata.get("profile"),
                "CREATED_DATE": str(metadata.get("created_date")),
            }
            query_string = f"""
                select
                    fields.name,
                    field_property.value
                from
                    core.fields
                    left join core.field_property on field_property.field_id = fields.id
                    and field_property.measure_id = '{measure_id}'
                    and fields.is_active is true
                where value is not null and level='Measure'
            """
            cursor = execute_query(connection, cursor, query_string)
            custom_fields = fetchall(cursor)
            custom_fields = custom_fields if custom_fields else []

            for field in custom_fields:
                value = field.get("value")
                name = field.get("name")
                if isinstance(value, str):
                    value = value.replace('"', "").replace("'", "")
                measure.update({name: value})
            measure_metadata.append(measure)
    return measure_metadata

def save_to_storage(config: dict, data: dict):
    import io

    dag_info = config.get("dag_info", {})
    queue_id = config.get("queue_id", "")
    queue_id = re.sub("[^A-Za-z0-9]", "_", queue_id.strip())
    file_name = f"failed_rows_{queue_id}.json"
    file_path = f"reports/{file_name}"
    try:
        storage_service = get_storage_service(dag_info)

        # Use in-memory buffer to avoid file write permission issues
        json_buffer = io.BytesIO()
        json_str = json.dumps(data, default=str)
        json_buffer.write(json_str.encode("utf-8"))
        json_buffer.seek(0)

        file_url = storage_service.upload_file(json_buffer, file_path, dag_info)
        return file_url
    except Exception as e:
        log_error('Save failed rows to storage failed', e)
        return None

def delete_temp_metadata(config:dict):
    dag_info = config.get("dag_info", {})
    queue_id = config.get("queue_id", "")
    queue_id = re.sub("[^A-Za-z0-9]", "_", queue_id.strip())
    file_name = f"failed_rows_{queue_id}.json"
    file_path = f"reports/{file_name}"
    storage_service = get_storage_service(dag_info)
    storage_service.delete_file(file_path, dag_info)

def get_export_failed_rows_job_config(config: dict, is_failed_rows: bool = True):
    # Prepare External Storage Configuration
    dag_info = config.get("dag_info", {})
    livy_spark_config = dag_info.get("livy_spark_config", {})
    livy_spark_config = livy_spark_config if livy_spark_config else {}
    namespace = livy_spark_config.get("export_namespace", "export")
    if not is_failed_rows:
        namespace = livy_spark_config.get("namespace", "db")
    catalog = livy_spark_config.get("catalog", "spark_catalog")
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
                "iceberg_warehouse": f"s3a://{bucket_name}/{folder}"
            }
        elif provider == "disc":
            local_directory = external_storage_settings.get("local_directory", "")
            if local_directory and not local_directory.endswith("/"):
                local_directory += "/"

            external_credentials = {
                **external_storage_settings,
                "local_directory": external_storage_settings.get("local_directory", ""),
                "iceberg_warehouse": f"{local_directory}"
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
                "iceberg_warehouse": f"abfss://{external_storage_settings.get('container')}@{external_storage_settings.get('storage_account_name')}.dfs.core.windows.net/{folder}"
            }
        elif provider == "gcp":
            folder = external_storage_settings.get("directory", "").strip("/")
            if external_storage_settings.get("is_vault_enabled", False):
                pg_connection = get_postgres_connection(config)
                keyjson = get_vault_data(config, pg_connection, external_storage_settings)
                keyjson = {k: decrypt(v) for k, v in keyjson.items()}
                external_storage_settings.update({"keyjson": keyjson})
            if folder:
                folder = folder if folder.endswith("/") else folder + "/"
            external_credentials = {
                "project_id": external_storage_settings.get("project_id"),
                "gcp_bucket_name": external_storage_settings.get("gcp_bucket_name"),
                "keyjson": external_storage_settings.get("keyjson"),
                "base_url": f"https://storage.googleapis.com/{external_storage_settings.get('gcp_bucket_name')}/",
                "iceberg_warehouse": f"gs://{external_storage_settings.get('gcp_bucket_name')}/{folder}"
            }
    else:
        external_credentials = {
            "bucket_name": os.environ.get("S3_BUCKET"),
            "access_key": os.environ.get("S3_ACCESS_KEY"),
            "secret_key": os.environ.get("S3_SECRET_KEY"),
            "region": os.environ.get("S3_REGION"),
            "iceberg_warehouse": f"s3a://{os.environ.get('S3_BUCKET')}/{folder}"
        }

    external_credentials = {
        **external_credentials,
        "provider": provider,
        "iceberg_catalog": catalog,
        "iceberg_schema": namespace,
    }
    return external_credentials

def get_retention_run_history(config: dict):
    """
    Refresh the failed rows and keeps the minimum no.of data
    """
    report_settings = config.get("report_settings")
    if not report_settings and "dag_info" in config:
        dag_info = config
        if "dag_info" in config:
            dag_info = config.get("dag_info")
            dag_info = dag_info if dag_info else {}

        general_settings = dag_info.get("settings", {})
        general_settings = general_settings if general_settings else {}
        general_settings = (
            json.loads(general_settings, default=str)
            if isinstance(general_settings, str)
            else general_settings
        )

        report_settings = general_settings.get("reporting")
        report_settings = report_settings if report_settings else {}
        if report_settings:
            config.update({"report_settings": report_settings})

    report_settings = (
        json.loads(report_settings, default=str)
        if report_settings and isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}
    max_runs_to_store = report_settings.get(
        "max_store_period_limit", DEFAULT_MAX_RUNS_TO_STORE
    )
    max_runs_to_store = (
        int(max_runs_to_store) if max_runs_to_store else DEFAULT_MAX_RUNS_TO_STORE
    )
    max_runs_to_store_type = report_settings.get(
        "max_store_period_type", DEFAULT_MAX_RUNS_TO_STORE_TYPE
    )
    max_runs_to_store_type = (
        max_runs_to_store_type
        if max_runs_to_store_type
        else DEFAULT_MAX_RUNS_TO_STORE_TYPE
    )

    if isinstance(max_runs_to_store, str):
        max_runs_to_store = int(max_runs_to_store)
        max_runs_to_store = (
            max_runs_to_store if max_runs_to_store else DEFAULT_MAX_RUNS_TO_STORE
        )

    level = config.get("level")
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    measure = config.get("measure")
    measure = measure if measure else {}
    measure_id = measure.get("id")
    attribute = config.get("attribute")
    attribute = attribute if attribute else {}
    attribute_id = attribute.get("id")
    if level == "measure":
        asset_id = None
        attribute_id = None
        measure_id = config.get("measure_id")
    else:
        measure_id = None

    connection = get_postgres_connection(config)
    last_runs = get_last_runs(
        connection,
        asset_id,
        limit=max_runs_to_store,
        limit_type=max_runs_to_store_type,
        attribute_id=attribute_id,
        measure_id=measure_id,
    )
    last_runs = [last_runs] if last_runs and isinstance(last_runs, dict) else last_runs
    last_runs = last_runs if last_runs else []
    last_runs = [run.get('run_id') for run in last_runs]
    return last_runs


def get_deprecated_columns(config: dict):
    """
    Get the deprecated columns for the given config
    """
    deprecated_columns = []
    asset_id = config.get("asset_id")
    asset_id = asset_id if asset_id else ""
    if asset_id:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select name from core.attribute
                where status='Deprecated' and asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            deprecated_columns = fetchall(cursor)
            deprecated_columns = deprecated_columns if deprecated_columns else []
            deprecated_columns = [column.get("name") for column in deprecated_columns]
    return deprecated_columns

def update_summarized_columns(config: dict, queries: dict, destination_queries: dict, destination_connection: dict):
    """
    Update the summarized columns for the given config
    """
    # Get connection type from config
    destination_config = config.get("destination_config")
    connection_type = destination_config.get("connection_type") if destination_config else ""
    connection_type = connection_type.lower() if connection_type else ""


    destination_connection_object = config.get("destination_connection_object")
    destination_database = config.get("destination_database")
    failed_rows_table = config.get("failed_rows_table")
    fetch_column_query = queries.get("fetch_failed_rows_table_schema")
    database_name = config.get("failed_rows_database")
    database = config.get("destination_database")
    schema_name = config.get("failed_rows_schema")
    columns = queries.get("failed_rows_columns", {})

    if not columns:
        return

    if schema_name:
        if database:
            database_name = database
        if not database_name:
            if destination_connection_object:
                connection = destination_connection_object
            connection_credentials = connection.get("credentials")
            connection_credentials = (
                connection_credentials if connection_credentials else {}
            )
            database_name = connection_credentials.get("database")
            database_name = f"{database_name}" if database_name else ""

        if connection_type == ConnectionType.Snowflake.value and not database_name:
            destination_connection_database = get_dq_connections_database(
                config, destination_connection_object.get("id")
            )
            database_name = destination_connection_database.get("name")

    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"

    fetch_column_query = queries.get("fetch_failed_rows_table_schema")

    if (
        connection_type == ConnectionType.Databricks.value
        and database_name.replace(".", "").replace("`", "") == "hive_metastore"
    ):
        fetch_column_query = (
            destination_queries.get("hive_metastore", {})
            .get("failed_rows", {})
            .get("fetch_failed_rows_table_schema")
        )

    fetch_column_query = fetch_column_query if fetch_column_query else ""
    fetch_column_query = (
        fetch_column_query.replace("<failed_rows_table>", failed_rows_table)
        .replace(
            "<target_database_name>",
            (
                database_name.replace(".", "")
                if database_name.endswith(".")
                else database_name
            ),
        )
        .replace("<database_name>", destination_database)
        .replace("<schema_name>", schema_name)
    )

    existing_columns = []
    if fetch_column_query:
        if config.get("destination_connection_object").get("type").lower() in [
            ConnectionType.BigQuery.value
        ]:
            time.sleep(20)
        existing_columns, _ = execute_native_query(
            destination_config, fetch_column_query, destination_connection, is_list=True
        )
        existing_columns = [column.get("column_name") for column in existing_columns]


    column_update_query = queries.get("alter_failed_rows_table")
    if not column_update_query:
        return

    column_update_query = column_update_query.replace(
        "<schema_name>", schema_name
    ).replace("<database_name>", database_name).replace("<table_name>", failed_rows_table)

    for column in columns:
        if not any(
            existing.lower() == column.lower()
            for existing in existing_columns
        ):
            datatype = columns[column]
            update_query = column_update_query.replace(
                "<col_name>", column
            ).replace("<col_type>", datatype)
            execute_native_query(
                destination_config,
                update_query,
                destination_connection,
                True,
                no_response=True,
            )
    