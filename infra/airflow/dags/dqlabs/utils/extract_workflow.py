"""
    Migration Notes From V2 to V3:
    Pending:True
    Need to Fix Below function based on version model changes
    change_version
"""

import json
import re
import pendulum
import pandas as pd
from copy import deepcopy
from uuid import uuid4
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall, split_queries
from dqlabs.utils.event_capture import save_sync_event

from dqlabs.app_helper.dq_helper import (
    convert_to_lower,
    get_attribute_label,
    get_derived_type_category,
    calculate_weightage_score,
    check_measure_result,
    process_direct_query,
    parse_numeric_value,
    extract_select_statement,
    extract_notation_from_query,
    transform_mongodb_field_names,
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    get_query_string,
    delete_metrics
)
from dqlabs.app_constants.dq_constants import (
    ATTRIBUTE_LENGTH_RANGE_COLUMNS,
    ATTRIBUTE_RANGE_COLUMNS,
    BEHAVIORAL,
    BUSINESS_RULES,
    CUSTOM,
    STATISTICS,
    LOOKUP,
    CUSTOM_MEASURE_CATEGORIES,
    DAYS,
    FAILED,
    FREQUENCY,
    HEALTH,
    DISTRIBUTION,
    HOURS,
    MINUTES,
    MONTHS,
    PASSED,
    PENDING,
    PROPERTIES,
    QUERY,
    WEEKS,
    SEMANTIC_MEASURE,
    RANGE,
    CONDITIONAL,
    PARAMETER,
    DIRECT_QUERY_BASE_TABLE_LABEL,
    CROSS_SOURCE
)
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.range import (
    get_range_measures,
    update_range_measure,
    fetch_semantic_term_measure_attribute_name,
)
from dqlabs.utils.profile import to_list
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.enums.approval_status import ApprovalStatus
from dqlabs.utils import is_scoring_enabled
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query
from dqlabs.app_helper.dag_helper import get_incremental_query_string
from dqlabs.app_helper.sql_group_parse import convert_query_to_group_by, append_where_to_query

def get_selected_attributes(config: dict, attribute_id: str = "", job_type: str = "") -> list:
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
            
        attribute_condition = (
            f" and attribute.id='{attribute_id}' " if attribute_id else ""
        )
        
        # For profile job, we need to get the attributes that are selected and profile is true
        if job_type == 'profile':
            attribute_condition = f" {attribute_condition} and attribute.profile = true "

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select attribute.*, attribute.row_count
                from core.attribute
                join core.asset on asset.id = attribute.asset_id
	 			join core.data on data.asset_id = asset.id
                where {asset_condition}
                and attribute.is_selected=True and lower(attribute.status)!='{ApprovalStatus.Deprecated.value.lower()}'
                {attribute_condition}
            """
            cursor = execute_query(connection, cursor, query_string)
            if attribute_id:
                attributes = fetchone(cursor)
            else:
                attributes = fetchall(cursor)
        return attributes
    except Exception as e:
        raise e


def get_queries(config: dict) -> str:
    """
    Returns the default native queries for given connection type
    """
    try:
        connection_type = config.get("connection_type")
        connection = get_postgres_connection(config)
        queries = {}

        # Handle Spark Queries for Below Mentioned Connection Types
        if connection_type in [ConnectionType.ADLS.value, ConnectionType.File.value, ConnectionType.S3.value]:
            connection_type = ConnectionType.Spark.value

        with connection.cursor() as cursor:
            query_string = f"""
                select queries from core.connection_type
                where lower(type) = '{connection_type.lower()}'
            """
            cursor = execute_query(connection, cursor, query_string)
            queries = fetchone(cursor)
            queries = queries.get("queries", {})
        return queries
    except Exception as e:
        raise e


def get_metadata_query(
    query_type: str,
    queries: dict,
    config: dict,
    native_query: bool = False,
    replace_temp_table: bool = False,
    attribute="",
) -> str:
    """
    Returns the metadata query for given connection type
    """
    try:
        log_info(("query_type", query_type))
        asset = config.get("asset")
        asset = asset if asset else {}
        is_incremental = config.get("is_incremental", False)
        asset = asset if asset else {}
        filter_query = ""
        source_filter_query = asset.get("filter_query", None)
        schema_name = config.get("schema")
        connection = config.get("connection")
        connection = connection if connection else {}
        connection_type = connection.get("type")
        if source_filter_query:
            filter_query = f"{source_filter_query}"
        source_asset_filter_query = ""
        if source_filter_query:
            source_asset_filter_query = f" {source_filter_query} AND "
        is_filtered = asset.get("is_filtered")
        has_temp_table = config.get("has_temp_table")
        table_name = config.get("table_technical_name")
        table_name = (
            table_name.replace("'", "\\'")
            if query_type not in ["primary_key"]
            else table_name
        )
        if query_type in ["total_rows", "duplicate", "freshness"]:
            if query_type == "duplicate" and connection_type == ConnectionType.Synapse.value:
                qualified_table_name = table_name
            else:
                qualified_table_name = config.get("table_name")
            table_name = qualified_table_name if qualified_table_name else table_name

        log_info(("connection_type", connection_type))
        connection_type = connection_type.lower() if connection_type else ""
        if (
            connection_type == ConnectionType.Redshift.value
            and query_type == "primary_key"
        ):
            table_name = table_name.replace("'", "''")
        credentials = connection.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}
        db_name = credentials.get("database")
        if not db_name:
            db_name = config.get("database_name")
        db_name = db_name if db_name else ""

        asset = config.get("asset")
        asset = asset if asset else {}
        is_incremental = config.get("is_incremental", False)
        asset_type = asset.get("type")
        if (
            asset
            and asset_type
            and str(asset_type).lower() == "query"
            and not has_temp_table
        ):
            profile_database_name = config.get("profile_database_name")
            profile_database_name = (
                profile_database_name if profile_database_name else ""
            )
            profile_schema_name = config.get("profile_schema_name")
            profile_schema_name = profile_schema_name if profile_schema_name else ""

            properties = asset.get("properties", {})
            properties = (
                json.loads(properties) if isinstance(
                    properties, str) else properties
            )
            properties = properties if properties else {}

            database = properties.get("database")
            database = database if database else ""
            database = profile_database_name if profile_database_name else database
            db_name = database if database else db_name
            schema = properties.get("schema")
            schema_name = profile_schema_name if profile_schema_name else schema

        if (has_temp_table and replace_temp_table) or (
            str(asset.get("view_type", "")).lower() == "direct query"
        ):
            table_name = config.get("temp_table_name")
            schema_name = config.get("temp_schema_name")
            db_name = config.get("temp_database_name")

        if (has_temp_table and replace_temp_table):
            table_name = config.get("temp_view_table_name")

        if connection_type == ConnectionType.Teradata.value:
            schema_name = db_name

        schema_condition = queries.get("schema_condition") if queries else ""
        if query_type == "query_table":
            querymode_table_query = queries.get(
                "metadata", {}).get(query_type, {})
            metadata_queries = []
            for key in querymode_table_query:
                metadata_query = querymode_table_query.get(key)
                if not native_query and metadata_query:
                    location = credentials.get("location")  # for bigquery
                    location = location if location else ""
                    metadata_query = prepare_query_string(
                        metadata_query,
                        schema_name,
                        table_name,
                        database_name=db_name,
                        schema_condition=schema_condition,
                        location=location,
                    )
                    metadata_queries.append(metadata_query)
            return metadata_queries
        elif (
            connection_type == ConnectionType.Databricks.value
            and db_name == "hive_metastore"
            and query_type
            in ("table", "view", "attributes", "total_queries", "watermark_datatype")
        ):
            metadata_query = (
                queries.get("hive_metastore", {})
                .get("metadata", {})
                .get(query_type, "")
                if queries
                else ""
            )
        else:
            metadata_query = (
                queries.get("metadata", {}).get(
                    query_type, "") if queries else ""
            )
        if not native_query and metadata_query:
            location = credentials.get("location")  # for bigquery
            location = location if location else ""
            metadata_query = prepare_query_string(
                metadata_query,
                schema_name,
                table_name,
                database_name=db_name,
                schema_condition=schema_condition,
                location=location,
            )
        if attribute and query_type in ["freshness", "duplicate"]:
            metadata_query = metadata_query.replace("<attribute>", attribute)
        metadata_query = get_query_string(
            config, queries, metadata_query, is_full_query=True
        )
        return metadata_query
    except Exception as e:
        raise e


def __get_default_queries(
    queries: dict, query_type: str, derived_type: str = "", check_numerics: bool = False
) -> str:
    """
    Returns the native queries for the given type.
    """
    default_queries = {}
    connector_queries = queries
    queries = deepcopy(queries)
    queries = queries.get(query_type)
    basic_queries = (
            queries.get("common", {})
            if queries else queries
        )
    if query_type in ["statistics", "distribution"]:
            advanced_queries = connector_queries.get("advanced", {})
            advanced_queries = advanced_queries if advanced_queries else {}
            basic_queries = advanced_queries.get("text", {})
            basic_queries.update(advanced_queries.get("numeric", {}))

    if query_type == "distribution":
        queries = connector_queries.get("health", {})
        basic_queries.update(queries.get("common", {}))

    if derived_type:
        derived_category = get_derived_type_category(derived_type.lower())
        if derived_category:
            derived_type_queries = queries.get(derived_category, {})
            basic_queries.update(derived_type_queries)

        # Datatype Sense Queries
        if check_numerics and str(derived_type).lower() == "text":
            datatype_sense = queries.get("datatype_sense", {})
            if datatype_sense:
                basic_queries.update(datatype_sense)

    default_queries = basic_queries
    return default_queries


def get_measures(
    config: dict, measure_type: str, level: str = "attribute", attribute_id: str = None
) -> str:
    """
    Returns the list of active measures for the given asset
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        measure_condition = (
            f" and base.type = '{measure_type}' " if measure_type else ""
        )
        if measure_type == HEALTH:
            measure_condition = f""" and base.type = '{DISTRIBUTION}' and lower(base.category) = '{HEALTH}' """
        elif measure_type in [BUSINESS_RULES, SEMANTIC_MEASURE]:
            level = ""
            measure_condition = """ and base.type in ('custom', 'semantic') and lower(base.category) in ('conditional', 'query', 'lookup')
                    and (base.query is not null or (base.term_id is not null and mes.semantic_query is not null))
                """
        elif measure_type == BEHAVIORAL:
            level = ""
            measure_condition = """ and base.type = 'custom' and lower(base.category) = 'behavioral'
                    and base.query is not null
                """

        attribute_query = ""
        if attribute_id:
            attribute_query = f" and mes.attribute_id='{attribute_id}' "

        level_condition = ""
        if level:
            level_condition = (
                f" and base.level in ('attribute', 'term')"
                if level == "attribute"
                else " and base.level = 'asset' "
            )

        asset_query = f" and mes.asset_id='{asset_id}'" if asset_id else ""
        measures = []
        with connection.cursor() as cursor:
            query_string = f"""
                select mes.id as id, base.id as base_measure_id, base.technical_name as name, base.query, base.properties, base.type, base.level, base.category, base.derived_type,
                mes.allow_score, mes.is_drift_enabled, mes.attribute_id, mes.asset_id, mes.is_positive, mes.drift_threshold, base.term_id, mes.semantic_measure, mes.semantic_query,
                mes.weightage, mes.enable_pass_criteria, mes.pass_criteria_threshold, mes.pass_criteria_condition
                from core.measure as mes
                join core.base_measure as base on base.id = mes.base_measure_id
                left join core.attribute as att on att.id = mes.attribute_id
                where mes.is_active=True
                and (att.id is null or (att.id is not null and att.profile = true and att.is_selected = true))
                {asset_query}
                {measure_condition} 
                {level_condition}
                {attribute_query}
            """
            cursor = execute_query(connection, cursor, query_string)
            measures = fetchall(cursor)
        return measures
    except Exception as e:
        raise e


def get_iteration(list_count, max_limit):
    """
    Returns the no.of iteration to process all the sellected attributes
    """
    iteration = 0
    if list_count:
        iteration = int(list_count / max_limit)
        remaining = int(list_count % max_limit)
        if remaining:
            iteration = iteration + 1
    return iteration


def __get_combined_queries_mongo(mongo_queries_with_labels: list) -> list:
    """
    Prepares combined MongoDB aggregation query using $facet operator
    to combine multiple statistical measure pipelines into a single query.
    """
    max_attributes_to_query = 50
    total_attributes = len(mongo_queries_with_labels)
    iteration = 0
    if total_attributes:
        iteration = get_iteration(total_attributes, max_attributes_to_query)
    
    queries_to_execute = []
    for i in range(0, iteration):
        attribute_index = i * max_attributes_to_query
        batch_queries = mongo_queries_with_labels[
            attribute_index: (attribute_index + max_attributes_to_query)
        ]
        
        if not batch_queries:
            continue
        
        facet_stage = {"$facet": {}}
        for query_pipeline, label in batch_queries:
            if isinstance(query_pipeline, str):
                try:
                    query_pipeline = json.loads(query_pipeline)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            if isinstance(query_pipeline, list):
                facet_stage["$facet"][label] = query_pipeline
            else:
                facet_stage["$facet"][label] = [query_pipeline]
        
        final_pipeline = [facet_stage]
        
        project_stage = {"$project": {}}
        for _, label in batch_queries:
            project_stage["$project"][label] = {"$arrayElemAt": ["$" + label, 0]}
        
        final_pipeline.append(project_stage)
        
        query_string = json.dumps(final_pipeline)
        queries_to_execute.append(query_string)
    
    return queries_to_execute


def __get_combined_queries(attribute_queries: dict) -> list:
    """
    Prepares combined query from list of measures for set of attributes
    based on no.of attributes to process
    """
    max_attributes_to_query = 50
    total_attributes = len(attribute_queries)
    queries = [attribute_queries]
    if isinstance(attribute_queries, dict):
        queries = [value for _, value in attribute_queries.items()]
    iteration = 0
    if total_attributes:
        iteration = get_iteration(total_attributes, max_attributes_to_query)
    queries_to_execute = []
    for i in range(0, iteration):
        attribute_index = i * max_attributes_to_query
        attribute_queries = queries[
            attribute_index: (attribute_index + max_attributes_to_query)
        ]
        attribute_queries = [
            query for sub_queries in attribute_queries for query in sub_queries
        ]
        query_string = ", ".join(attribute_queries)
        queries_to_execute.append(query_string)
    return queries_to_execute


def should_run_constraint_measures(config: dict, attribute: dict) -> bool:
    """
    Check if we should run constraint-based measures for Snowflake
    regardless of profile toggle state
    """
    try:
        connection_type = config.get("connection_type", "").lower()
        if connection_type != "snowflake":
            return False
            
        # Check if attribute has constraint information
        is_null = attribute.get("is_null")  # False = NOT NULL constraint
        is_unique = attribute.get("is_unique")  # True = UNIQUE constraint
        
        # Run constraint measures if we have constraint information
        return (is_null is False) or (is_unique is True)
        
    except Exception as e:
        log_error("should_run_constraint_measures", e)
        return False


def get_constraint_measures_for_attribute(config: dict, attribute: dict, all_measures: list) -> list:
    """
    Get the list of real constraint-based measures that should run for an attribute,
    including measures that may have is_active=False
    """
    constraint_measures = []
    
    is_null = attribute.get("is_null")  # False = NOT NULL constraint
    is_unique = attribute.get("is_unique")  # True = UNIQUE constraint
    attribute_id = attribute.get("id")
    
    # First, try to find measures from the regular list (is_active=True)
    attribute_measures = [m for m in all_measures if m.get("attribute_id") == attribute_id]
    
    # If we need constraint measures but don't find them in active measures,
    # query the database directly to get them even if is_active=False
    needed_measures = []
    if is_null is False:
        needed_measures.append("null_count")
    if is_unique is True:
        needed_measures.append("distinct_count")
    
    if needed_measures:
        try:
            from dqlabs.app_helper.db_helper import get_postgres_connection, fetchall, execute_query
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                measure_names = "', '".join(needed_measures)
                query = f"""
                    SELECT mes.id as id, base.id as base_measure_id, base.technical_name as name, 
                           base.query, base.properties, base.type, base.level, base.category, base.derived_type,
                           mes.allow_score, mes.is_drift_enabled, mes.attribute_id, mes.asset_id, 
                           mes.is_positive, mes.drift_threshold, base.term_id, mes.semantic_measure, 
                           mes.semantic_query, mes.weightage, mes.enable_pass_criteria, 
                           mes.pass_criteria_threshold, mes.pass_criteria_condition, mes.is_active
                    FROM core.measure as mes
                    JOIN core.base_measure as base on base.id = mes.base_measure_id
                    WHERE mes.attribute_id = '{attribute_id}'
                      AND base.technical_name IN ('{measure_names}')
                      AND mes.is_delete = false and mes.is_active = true
                """
                cursor = execute_query(connection, cursor, query)
                db_measures = fetchall(cursor)
                
                for measure in db_measures:
                    measure_name = measure.get("name")
                    is_active = measure.get("is_active", False)
                    
                    # Add null_count measure for NOT NULL constraints
                    if is_null is False and measure_name == "null_count":
                        # Force the measure to be active for constraint-based execution
                        measure["is_active"] = True
                        measure["allow_score"] = True  # Enable scoring for constraint measures
                        measure["constraint_based"] = True  # Mark as constraint-based
                        constraint_measures.append(measure)
                    
                    # Add distinct_count measure for UNIQUE constraints  
                    if is_unique is True and measure_name == "distinct_count":
                        # Force the measure to be active for constraint-based execution
                        measure["is_active"] = True
                        measure["allow_score"] = True  # Enable scoring for constraint measures
                        measure["constraint_based"] = True  # Mark as constraint-based
                        constraint_measures.append(measure)
                        
        except Exception as e:
            # Fallback to regular measures if database query fails
            if is_null is False:
                null_count_measure = next((m for m in attribute_measures if m.get("name") == "null_count"), None)
                if null_count_measure:
                    constraint_measures.append(null_count_measure)
            
            if is_unique is True:
                distinct_count_measure = next((m for m in attribute_measures if m.get("name") == "distinct_count"), None)
                if distinct_count_measure:
                    constraint_measures.append(distinct_count_measure)
    
    return constraint_measures


def get_health_measures(
    measure_type: str,
    config: dict,
    default_queries: dict,
    selected_attribute_id: str = "",
):
    """
    Returns the list of measure queries for all the selected
    attributes available in an asset
    """
    datatype_sense_queries = default_queries.get(measure_type.lower()).get(
        "datatype_sense", {}
    )
    datatype_sense_queries = datatype_sense_queries if datatype_sense_queries else {}
    datatype_sense_keys = list(datatype_sense_queries.keys())
    datatype_sense_keys = datatype_sense_keys if datatype_sense_keys else []
    check_numerics = config.get(
        "profile_settings", {}).get("check_numerics", False)
    selected_attributes = config.get("attributes", [])
    selected_attributes = selected_attributes if selected_attributes else []
    selected_attributes = [
        attribute.get("id") for attribute in selected_attributes if attribute
    ]
    measures = get_measures(config, measure_type.lower())
    attributes = get_selected_attributes(config)
    attributes = attributes if attributes else []
    attributes = [
        attr
        for attr in attributes
        if attr.get("datatype") and attr.get("datatype", "").lower() not in ["geometry", "geography", "vector","bindata"]
    ]
    connection_type = config.get("connection_type").lower()
    if selected_attributes:
        attributes = list(
            filter(
                lambda attribute: attribute.get(
                    "id") in selected_attributes, attributes
            )
        )
        measures = list(
            filter(
                lambda measures: measures.get(
                    "attribute_id") in selected_attributes,
                measures,
            )
        )
    total_records = 0
    attribute_queries = {}
    measure_level_queries = {}
    for attribute in attributes:
        # Check if profile is enabled OR if we need constraint-based measures for Snowflake
        profile_enabled = attribute.get("profile", False)
        needs_constraint_measures = should_run_constraint_measures(config, attribute)
        
        if not profile_enabled and not needs_constraint_measures:
            log_info(("Profile not enabled for attribute",  attribute.get("name")))
            continue
        elif not profile_enabled and needs_constraint_measures:
            pass

        attribute_name = attribute.get("name")
        attribute_id = attribute.get("id")
        attribute_datatype = attribute.get("datatype", "").lower()
        if selected_attribute_id and attribute_id != selected_attribute_id:
            continue
        if not total_records:
            total_records = int(attribute.get("row_count", 0))
        derived_type = attribute.get("derived_type", "").lower()
        attribute_measure_queries = __get_default_queries(
            default_queries, measure_type, derived_type, check_numerics
        )
        attribute_measures = list(
            filter(
                lambda measure: measure.get("attribute_id") == attribute_id,
                measures,
            )
        )
        
        # Add constraint-based measures for Snowflake if needed
        if needs_constraint_measures:
            constraint_measures = get_constraint_measures_for_attribute(config, attribute, measures)
            if constraint_measures:
                # If profile is disabled, only use constraint measures
                if not profile_enabled:
                    attribute_measures = constraint_measures
                else:
                    # If profile is enabled, add constraint measures to existing ones
                    attribute_measures.extend(constraint_measures)
        queries = []
        """ 
        Extract all the measure where not null filter will added 
        -- Condition valid for only Snowflake & SapHana Connectors as part of DQL- 848 & DQL- 849
        """
        not_null_queries = []
        not_null_filter_measures = [
            "min_value",
            "max_value",
            "zero_values",
            "min_length",
            "max_length",
        ]
        for measure in attribute_measures:
            measure_name = measure.get("name")
            query = attribute_measure_queries.get(measure_name)
            if not query:
                continue
            attribute_label = get_attribute_label(attribute_name)
            if connection_type in [ConnectionType.Salesforce.value, ConnectionType.SalesforceMarketing.value] and attribute_datatype in ["boolean", "char"]:
                # For Salesforce,SalesforceMarketing boolean attributes, we need to use the picklist query
                continue
            query = prepare_query_string(query, attribute=attribute_name)
            if connection_type == ConnectionType.MongoDB.value and (not query or query.strip() == ""):
                continue
            alias_name = f"{attribute_label}{measure_name}"
            
            if connection_type == ConnectionType.MongoDB.value:
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records,
                    is_full_query=True, attribute_name=attribute_name
                )
            else:
                if connection_type == ConnectionType.Oracle.value:
                    query = f'{query} AS "{alias_name}"'
                else:
                    query = f"""{query} AS {alias_name}"""
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records, attribute_name=attribute_name
                )
            measure_level_queries.update({alias_name: measure_level_query})
            # We can remove the connection type condition once all the connectors are updated with not null default query
            if measure_name in not_null_filter_measures and (
                connection_type
                in [
                    ConnectionType.Snowflake.value,
                    ConnectionType.SapHana.value,
                    ConnectionType.Athena.value,
                    ConnectionType.BigQuery.value,
                    ConnectionType.Databricks.value,
                    ConnectionType.Db2.value,
                    ConnectionType.DB2IBM.value,
                    ConnectionType.EmrSpark.value,
                    ConnectionType.MSSQL.value,
                    ConnectionType.MySql.value,
                    ConnectionType.Oracle.value,
                    ConnectionType.Postgres.value,
                    ConnectionType.Redshift.value,
                    ConnectionType.Redshift_Spectrum.value,
                    ConnectionType.Synapse.value,
                    ConnectionType.Teradata.value,
                    ConnectionType.AlloyDB.value,
                    ConnectionType.SapEcc.value,
                    ConnectionType.Salesforce.value,
                    ConnectionType.SalesforceMarketing.value,
                    ConnectionType.SalesforceDataCloud.value,
                ]
            ):
                not_null_queries.append(
                    query
                )  # Replace 'pass' with your intended logic
            else:
                if connection_type == ConnectionType.MongoDB.value:
                    queries.append(measure_level_query)
                else:
                    queries.append(query)

        # Datatype Sense Queries
        for key in datatype_sense_keys:
            if key not in attribute_measure_queries:
                continue
            query = attribute_measure_queries.get(key)
            if not query:
                continue
            attribute_label = get_attribute_label(attribute_name)
            query = prepare_query_string(query, attribute=attribute_name)
            if connection_type == ConnectionType.MongoDB.value and (not query or query.strip() == ""):
                continue
            alias_name = f"{attribute_label}{key}"
            
            if connection_type == ConnectionType.MongoDB.value:
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records,
                    is_full_query=True, attribute=attribute
                )
            else:
                if connection_type == ConnectionType.Oracle.value:
                    query = f'{query} AS "{alias_name}"'
                else:
                    query = f"""{query} AS {alias_name}"""
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records,
                    attribute=attribute
                )
            measure_level_queries.update({alias_name: measure_level_query})
            if connection_type == ConnectionType.MongoDB.value:
                queries.append(measure_level_query)
            else:
                queries.append(query)

        # Fetch the not-null queries if applicable
        not_null_query_string = ""
        if not_null_queries:
            not_null_queries = [
                query for query in not_null_queries if query and len(query.strip()) > 0
            ]
            not_null_query_string = (
                ", ".join(not_null_queries) if not_null_queries else ""
            )
            not_null_query_string = (
                not_null_query_string if not_null_query_string else ""
            )

        queries = [query for query in queries if query and len(
            query.strip()) > 0]
        
        if connection_type == ConnectionType.MongoDB.value:
            if not queries:
                continue
            attribute_queries.update({attribute_id: queries})
        else:
            query_string = ", ".join(queries) if queries else ""
            query_string = query_string if query_string else ""
            if query_string:
                final_query = get_query_string(
                    config,
                    default_queries,
                    query_string,
                    total_records,
                    attribute_name=attribute_name
                )
                # Ensure the value associated with each attribute_id is a list of strings
                # so that the new query can be appended without errors
                attribute_queries.update({attribute_id: [final_query]})

        if not_null_query_string:
            not_null_query_string = get_query_string(
                config,
                default_queries,
                not_null_query_string,
                total_records,
                attribute_name=attribute_name,
                query_type=HEALTH,
            )
            # Ensure attribute_queries[attribute_id] is a list, then append the query string
            if attribute_id not in attribute_queries:
                attribute_queries[attribute_id] = []

            attribute_queries[attribute_id].append(not_null_query_string)

    return attribute_queries, attributes, measures, measure_level_queries


def prepare_advanced_measure_queries(
    measure_type: str, config: dict, default_queries: dict
):
    """
    Returns the list of measure queries for all the selected
    attributes available in an asset
    """
    attribute = config.get("attribute")
    attribute_id = attribute.get("id")
    attribute_name = attribute.get("name")
    attribute_label = get_attribute_label(attribute_name)
    advanced_queries = default_queries.get("advanced")
    advanced_measure_queries = advanced_queries.get("text", {})
    advanced_measure_queries.update(advanced_queries.get("numeric", {}))
    table_name = config.get("table_name")
    asset = config.get("asset")
    asset = asset if asset else {}
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_type = connection.get("type", "")
    if str(asset.get("view_type", "")).lower() == "direct query":
        table_name = f"""({asset.get("query")}) as direct_query_table"""
        if connection_type and str(connection_type).lower() in [
            ConnectionType.Oracle.value.lower(),
            ConnectionType.BigQuery.value.lower(),
        ]:
            table_name = f"""({asset.get("query")})"""
    if connection_type == ConnectionType.SalesforceDataCloud.value:
        table_name= asset.get("name")

    measures = get_measures(config, measure_type.lower(),
                            "attribute", attribute_id)
    total_records = 0
    if not total_records:
        total_records = int(attribute.get("row_count", 0))

    attribute_queries = []
    full_queries = []
    measure_level_queries = {}
    is_mongo = connection_type and connection_type.lower() == ConnectionType.MongoDB.value.lower()
    mongo_queries_with_labels = []
    
    for measure in measures:
        measure_name = measure.get("name")
        query = advanced_measure_queries.get(measure_name)
        if not query:
            continue

        label = f"{attribute_label}{measure_name}"
        if isinstance(query, dict):
            query = query.get("query")
            if query:
                # Prepare the query for JSON attributes
                query = prepare_json_attribute_flatten_query(config, attribute_name, query, "sub_query")

                query = query.replace("<attribute_label>", label)
                query = prepare_query_string(
                    query, attribute=attribute_name, table_name=table_name
                )
                measure_level_queries.update({label: query})
                full_queries.append(query)
        else:
            if connection_type and connection_type in [ConnectionType.Oracle.value]:
                label = f'"{label}"'
            
            if is_mongo:
                query = prepare_query_string(query, attribute=attribute_name)
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records,
                    attribute_name=attribute_name
                )
                measure_level_queries.update({label: measure_level_query})
                try:
                    query_pipeline = json.loads(measure_level_query)
                    if isinstance(query_pipeline, list):
                        mongo_queries_with_labels.append((query_pipeline, label))
                    else:
                        mongo_queries_with_labels.append(([query_pipeline], label))
                except (json.JSONDecodeError, TypeError):
                    continue
            else:
                query = f"""{query} AS {label}"""
                query = prepare_query_string(query, attribute=attribute_name)
                measure_level_query = get_query_string(
                    config, default_queries, query, total_records,
                    attribute_name=attribute_name
                )
                measure_level_queries.update({label: measure_level_query})
                attribute_queries.append(query)

    if is_mongo and mongo_queries_with_labels:
        queries = __get_combined_queries_mongo(mongo_queries_with_labels)
        measure_queries = []
        for query in queries:
            if not query:
                continue
            query_string = get_query_string(
                config,
                default_queries,
                query,
                total_records,
                attribute_name=attribute_name,
                query_type=STATISTICS,
            )
            measure_queries.append(query_string)
    else:
        queries = __get_combined_queries(attribute_queries)
        measure_queries = []
        for query in queries:
            query_type = (
                STATISTICS if "ROUND" in query else ""
            )  # fetch the statistics queries
            if not query:
                continue
            query_string = get_query_string(
                config,
                default_queries,
                query,
                total_records,
                attribute_name=attribute_name,
                query_type=query_type,
            )
            measure_queries.append(query_string)

    if full_queries:
        measure_queries.extend(full_queries)
    return measure_queries, measures, measure_level_queries


def get_distribution_measure_name(measure_name: str):
    name = measure_name
    if measure_name == "null_count":
        name = "null"
    elif measure_name == "blank_count":
        name = "empty"
    elif measure_name == "space_count":
        name = "space"
    elif measure_name == "zero_values":
        name = "zero"
    return name


def update_metrics(measures: dict, config: dict) -> None:
    """
    Updates the current measure values into postgres
    """
    attributes = config.get("attributes", [])
    active_measures = config.get("measures", [])
    asset_id = config.get("asset_id")
    run_id = config.get("run_id")
    total_records = config.get("total_records")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query = f"""
            select attribute.*, terms_mapping.term_id, terms_mapping.approved_by  from core.attribute
            left join core.terms_mapping on terms_mapping.attribute_id = attribute.id
            where attribute.asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query)
        metadata = fetchall(cursor)

        previous_metadata = {}
        for data in metadata:
            attribute_id = data.get("id")
            metadata = data.get("metadata")
            metadata = metadata if metadata else {}
            if not attribute_id:
                continue
            term_id = data.get("term_id")
            term_approval_id = data.get("approved_by")
            has_approved_terms = term_id and term_approval_id
            previous_metadata.update(
                {attribute_id: {**metadata, "has_approved_terms": has_approved_terms}}
            )

        # Update attribute level measure properties
        for attribute in attributes:
            attribute_name = attribute.get("name")
            attribute_id = attribute.get("id")
            derived_type = attribute.get("derived_type")
            attribute_label = get_attribute_label(attribute_name)
            updated_properties = attribute.get("updated_properties", {})
            updated_properties = (
                json.loads(updated_properties)
                if isinstance(updated_properties, str)
                else updated_properties
            )
            updated_properties = updated_properties if updated_properties else {}
            has_numeric_values = False
            if str(derived_type).lower() == "text":
                non_numeric_value_key = f"{attribute_label}has_numeric"
                non_numeric_count = measures.get(non_numeric_value_key, 0)
                has_numeric_values = non_numeric_count <= 0

            attribute_metadata = previous_metadata.get(attribute_id)
            attribute_metadata = attribute_metadata if attribute_metadata else {}
            has_approved_terms = attribute_metadata.get("has_approved_terms")
            if has_approved_terms:
                continue

            attribute_measures = list(
                filter(
                    lambda measure: measure.get(
                        "attribute_id") == attribute_id,
                    active_measures,
                )
            )

            metadata_fields = [f""" last_run_id = '{run_id}' """]
            completeness_distribution = {
                "null": 0,
                "space": 0,
                "empty": 0,
                "non_empty": 0,
            }
            total_empty_count = 0
            for measure in attribute_measures:
                measure_name = measure.get("name")
                distribution_measure_name = get_distribution_measure_name(
                    measure_name)
                key = f"{attribute_label}{measure_name}"
                value = measures.get(key, 0)
                if measure_name.lower() in ATTRIBUTE_RANGE_COLUMNS:
                    value = parse_numeric_value(value)
                if (
                    measure_name.lower() in ATTRIBUTE_RANGE_COLUMNS
                    and derived_type
                    and derived_type.lower() == "bit"
                ):
                    value = str(value) if value is not None else ""
                elif str(derived_type).lower() not in ["integer", "numeric"]:
                    value = str(value) if value else ""
                else:
                    value = value if value else 0
                attribute_metadata.update({measure_name: value})

                is_updated_mannualy = False
                if updated_properties:
                    for category, measure_names in PROPERTIES.items():
                        if measure_name in measure_names:
                            is_updated_mannualy = bool(
                                updated_properties.get(category))

                if is_updated_mannualy:
                    continue

                if measure_name.lower() in ATTRIBUTE_RANGE_COLUMNS:
                    if str(derived_type).lower() in ["integer", "numeric"]:
                        value = get_range_value(value)
                        value = value if value else 0
                    elif str(derived_type).lower() == "text" and has_numeric_values:
                        value = get_range_value(value)
                        value = value if value else 0
                    elif str(derived_type).lower() == "text" and not has_numeric_values:
                        value = ""
                    attribute_metadata.update({measure_name: value})
                    value = str(value) if value is not None else ""
                    value = value.replace("'", "''")
                    value = re.sub(r"\x00", "", value)
                    metadata_fields.append(f""" {measure_name} = '{value}' """)

                if measure_name in ATTRIBUTE_LENGTH_RANGE_COLUMNS:
                    value = value if value else 0
                    metadata_fields.append(f""" {measure_name} = {value} """)

                if measure_name in [
                    "null_count",
                    "distinct_count",
                    "blank_count",
                    "space_count",
                    "zero_values",
                ]:
                    value = int(value) if value else 0
                    metadata_fields.append(
                        f""" {measure_name} = {str(value)} """)

                if distribution_measure_name in completeness_distribution:
                    value = int(value) if value else 0
                    total_empty_count = total_empty_count + value
                    completeness_distribution.update(
                        {distribution_measure_name: value})

            metadata_fields.append(f"has_numeric_values={has_numeric_values}")
            non_empty_count = total_records - total_empty_count
            non_empty_count = non_empty_count if non_empty_count > 0 else 0
            completeness_distribution.update({"non_empty": non_empty_count})
            completeness = (
                completeness_distribution if completeness_distribution else {}
            )
            completeness = to_list(completeness)
            completeness = json.dumps(completeness, default=str)
            completeness = completeness.replace("'", "''")
            metadata_fields.append(f"completeness='{completeness}'")

            metadata_fields_query = ", ".join(metadata_fields)
            if metadata_fields_query:
                metadata_fields_query = f", {metadata_fields_query}"
            metadata_properties = json.dumps(attribute_metadata, default=str)
            metadata_properties = metadata_properties.replace("'", "''")
            query = f"""
                update core.attribute set metadata='{metadata_properties}' {metadata_fields_query}
                where asset_id='{asset_id}' and id='{attribute_id}'
            """
            cursor = execute_query(connection, cursor, query)


def get_connection_config(config: dict) -> dict:
    """
    Returns the connection object for the given connection id.
    """
    try:
        source_connection_id = config.get("connection_id")
        connection = get_postgres_connection(config)
        source_connection = {}
        with connection.cursor() as cursor:
            query = f"""
                select * from core.connection
                where id = '{source_connection_id}' and is_active=True
                and is_delete=False
            """
            cursor = execute_query(connection, cursor, query)
            source_connection = fetchone(cursor)
        return source_connection
    except Exception as e:
        log_error(str(e), e)


def get_asset(config: dict) -> dict:
    """
    Returns the asset object for the given asset id.
    """
    try:
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        asset = {}
        with connection.cursor() as cursor:
            query = f"""
                select asset.*, data.primary_columns, data.row_count
                from core.asset as asset
                join core.data on data.asset_id = asset.id
                where asset.id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query)
            asset = fetchone(cursor)
        return asset
    except Exception as e:
        log_error(str(e), e)


def get_start_date(schedule: dict) -> str:
    """
    Returns the start date and timeszone for the given schedule
    """
    time_zone = None
    start_time = None
    if not schedule:
        return (start_time, time_zone)

    time_zone = schedule.get("timezone")
    start_date = schedule.get("start_date")
    start_time = pendulum.from_format(
        start_date, "MM-DD-YYYY HH:mm", tz=time_zone)
    schedule_type = schedule.get("type", "").lower()
    duration = schedule.get("duration")
    duration = int(duration) if duration else 0

    if schedule_type == MINUTES:
        start_time.add(minutes=-duration)
    elif schedule_type == HOURS:
        start_time.add(hours=-duration)
    elif schedule_type == DAYS:
        start_time.add(days=-duration)
    elif schedule_type == WEEKS:
        start_time.add(weeks=-duration)
    elif schedule_type == MONTHS:
        start_time.add(months=-duration)

    start_time = start_time.format("MM-DD-YYYY HH:mm")
    return start_time, time_zone


def get_schedule_interval(config: dict) -> dict:
    """
    Returns the connection object for the given connection id.
    """
    connection_id = config.get("connection_id")
    asset_id = config.get("asset_id")
    connection = get_postgres_connection(config)
    connection_schedule = None
    asset_schedule = None
    with connection.cursor() as cursor:
        query = f"""
            select schedules.* from core.schedules
            join core.schedules_mapping on schedules_mapping.schedule_id = schedules.id
            join core.asset on asset.connection_id = schedules_mapping.connection_id
            where schedules_mapping.connection_id = '{connection_id}' and asset.is_active = true and asset.is_delete = false
            and schedules_mapping.asset_id is null
        """
        cursor = execute_query(connection, cursor, query)
        connection_schedule = fetchone(cursor)

        query = f"""
            select schedules.* from core.schedules
            join core.schedules_mapping on schedules_mapping.schedule_id = schedules.id
            join core.asset on asset.id = schedules_mapping.asset_id
            where schedules_mapping.connection_id = '{connection_id}' and asset.is_active = true and asset.is_delete = false
            and schedules_mapping.asset_id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query)
        asset_schedule = fetchone(cursor)

    schedule = None
    if asset_schedule:
        schedule = asset_schedule
    if not schedule and connection_schedule:
        schedule = connection_schedule

    start_date, timezone = get_start_date(schedule)
    interval = schedule.get("cron_format") if schedule else None
    interval = interval if interval else None
    schedule_interval = {
        "schedule_interval": interval,
        "timezone": timezone,
        "start_date": start_date,
        "schedule": schedule,
    }
    return schedule_interval


def execute_measure(measure: dict, config: dict, default_queries: dict):
    """
    Executes the given measure for the given attribute
    """
    try:
        status = PENDING
        connection_type = config.get("connection_type")
        attribute = config.get("attribute")
        attribute = attribute if attribute else {}
        is_aggregation_query = measure.get("is_aggregation_query")
        measure_category = measure.get("category").lower()
        type = measure.get("type").lower()
        measure_id = measure.get("id")
        level = measure.get("level")
        is_direct_query_asset = config.get("is_direct_query_asset")
        base_table_query = config.get("base_table_query")
        is_custom_query = measure_category == QUERY
        is_positive = measure.get("is_positive")
        
        if measure_category == CROSS_SOURCE:
            from dqlabs.utils.cross_source import prepare_cross_source_query
            query_string = prepare_cross_source_query(config, measure, is_create_table=False)
        else:
            query_string = prepare_query(
            measure, config, default_queries, attribute)
            
        # Exception Outlier
        if type == "custom" and not is_positive:
            from dqlabs.utils.export_failed_rows.exception_outlier import get_marked_normal_outlier_values, prepare_marked_normal_where_clause
            exception_connection_type = connection_type
            if measure_category == CROSS_SOURCE:
                exception_connection_type = ConnectionType.Spark.value
            marked_normal_outlier_values = get_marked_normal_outlier_values(config, measure_id)
            outlier_where_clause = prepare_marked_normal_where_clause(config, marked_normal_outlier_values, exception_connection_type)
            if outlier_where_clause:
                if connection_type != ConnectionType.MongoDB.value:
                    query_string = append_where_to_query(query_string, outlier_where_clause)


        source_connection = None
        native_connection = None
        if not query_string:
            if connection_type == ConnectionType.MongoDB.value:
                return {}, None
            raise Exception("Undefined query string.")
        measure_result = {}
        config.update({"sub_category": measure_category})
        if measure_category == CONDITIONAL or is_aggregation_query:
            config.update({"query_string": query_string})
            if measure_category == CROSS_SOURCE:
                from dqlabs.utils.cross_source import execute_cross_source_query
                measure_result = execute_cross_source_query(config, query_string)
            else:
                is_list_for_query = (
                    connection_type == ConnectionType.MongoDB.value and measure_category == QUERY
                )
                
                measure_result, native_connection = execute_native_query(
                    config, query_string, source_connection, is_list=is_list_for_query
                )
            if not source_connection:
                source_connection = native_connection
            measure_result = convert_to_lower(measure_result)
            
            if connection_type == ConnectionType.MongoDB.value and measure_result and measure_category == QUERY:
                if isinstance(measure_result, list):
                    measure_count = len(measure_result)
                elif isinstance(measure_result, dict):
                    measure_count = measure_result.get("total_rows", 1 if measure_result else 0)
                else:
                    measure_count = 0
                
                if level == "asset":
                    attribute_name = ""
                attribute_label = get_attribute_label(attribute_name)
                measure_name = measure.get("name", "") if measure else ""
                key = f"{attribute_label}{str(measure_name).lower()}"
                measure_result = {key: measure_count}
            elif connection_type == ConnectionType.MongoDB.value and measure_result:
                attribute_name = attribute.get("name", "") if attribute else ""
                if attribute_name:
                    measure_result = transform_mongodb_field_names(measure_result, attribute_name)
            
            measure_result = measure_result if measure_result else {}
        else:
            is_count_only = True
            connection = config.get("connection")
            connection = connection if connection else {}
            connection_type = connection.get("type", "")
            config.update({"query_string": query_string})
            if connection_type and connection_type in [
                ConnectionType.MSSQL.value,
                ConnectionType.Databricks.value,
                ConnectionType.ADLS.value,
                ConnectionType.File.value,
                ConnectionType.SalesforceDataCloud.value,
                ConnectionType.S3.value
            ]:
                existing_select_query = extract_select_statement(query_string)
                if existing_select_query:
                    alias = (
                        " as dq_cte_count_query"
                        if connection_type
                        and str(connection_type).lower()
                        not in [
                            ConnectionType.Oracle.value.lower(),
                            ConnectionType.BigQuery.value.lower(),
                        ]
                        else ""
                    )
                    new_select_query = f"""
                        SELECT COUNT(*) as total_rows FROM ({existing_select_query}){alias}
                    """
                    query_string = query_string.replace(
                        existing_select_query, new_select_query
                    )
                    is_count_only = new_select_query not in query_string

            if measure_category == CROSS_SOURCE:
                from dqlabs.utils.cross_source import execute_cross_source_query
                measure_count = execute_cross_source_query(
                    config, query_string, is_count_only=is_count_only
                )
                if isinstance(measure_count, list):
                    measure_count = measure_count[0] if measure_count else {}
            else:
                is_list_for_mongo_query = (
                    connection_type == ConnectionType.MongoDB.value and measure_category == QUERY
                )
                
                measure_count, native_connection = execute_native_query(
                    config, query_string, source_connection, 
                    is_count_only=is_count_only,
                    is_list=is_list_for_mongo_query
                )
                
                if connection_type == ConnectionType.MongoDB.value and measure_category == QUERY:
                    if isinstance(measure_count, list):
                        measure_count = len(measure_count)
                    elif isinstance(measure_count, dict):
                        measure_count = measure_count.get("total_rows", 1 if measure_count else 0)
                    else:
                        measure_count = 0

            if (
                measure_count
                and isinstance(measure_count, dict)
                and "total_rows" in measure_count
            ):
                measure_count = measure_count.get("total_rows")
                measure_count = int(measure_count) if measure_count else 0

            if not source_connection and native_connection:
                source_connection = native_connection

            attribute_name = attribute.get("name") if attribute else ""
            if level == "asset":
                attribute_name = ""
            attribute_label = get_attribute_label(attribute_name)
            measure_name = measure.get("name") if measure else ""
            key = f"{attribute_label}{str(measure_name).lower()}"
            measure_result = {key: measure_count}

        if measure_category in CUSTOM_MEASURE_CATEGORIES:
            measure_properties = measure.get("properties")
            measure_properties = (
                json.loads(measure_properties)
                if isinstance(measure_properties, str)
                else measure_properties
            )
            measure_properties = measure_properties if measure_properties else {}
            conditional_scoring = measure_properties.get("conditional_scoring")
            conditional_scoring = conditional_scoring if conditional_scoring else {}
            is_enabled = conditional_scoring.get("is_enabled", False)
            query = conditional_scoring.get("query")
            if measure_category == CROSS_SOURCE:
                from dqlabs.utils.cross_source import prepare_cross_source_query
                query = prepare_cross_source_query(config, measure, query=query, is_create_table=False)
            if is_enabled and query:
                if is_direct_query_asset and base_table_query:
                    query = process_direct_query(query, connection_type.lower())
                    query = query.replace(
                        DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                    )

                if measure_category == "query" and level in ["asset", "attribute"] and not is_direct_query_asset:
                    query = get_incremental_query_string(config, default_queries, query)

                if measure_category == CROSS_SOURCE:
                    from dqlabs.utils.cross_source import execute_cross_source_query
                    result = execute_cross_source_query(config, query)
                else:
                    result, native_connection = execute_native_query(
                        config, query, source_connection, is_list=True,parameters= {"run_query": True}
                    )
                result = [result] if result else []

                conditional_count = 0
                for rows in result:
                    if len(rows) == 1:
                        conditional_measure_result = rows[0]
                        conditional_count = list(
                            conditional_measure_result.values())[0]
                        conditional_count = (
                            conditional_count if conditional_count else 0
                        )
                        break
                    conditional_count = conditional_count + len(rows)

                try:
                    conditional_count = int(conditional_count)
                except:
                    conditional_count = 0
                if conditional_count:
                    config.update(
                        {"conditional_scoring_count": conditional_count})
        status = PASSED
    except Exception as e:
        measure_result = {}
        status = FAILED
        log_error(f"Failed on execute measure : {str(e)}", e)
        raise e
    finally:
        save_measure(status, measure_result, config)


def execute_measures(measure_type: str, config: dict, default_queries: dict):
    """
    Executes the given measure for the given attribute
    """

    queries, measures, measure_level_queries = prepare_advanced_measure_queries(
        measure_type, config, default_queries
    )
    queries = queries if queries else []
    source_connection = None

    measure_result = {}
    if queries:
        for query in queries:
            attribute_measures, native_connection = execute_native_query(
                config, query, source_connection
            )
            if not source_connection and native_connection:
                source_connection = native_connection
            if attribute_measures:
                attribute_measures = convert_to_lower(attribute_measures)
                measure_result.update(attribute_measures)

    if measure_result:
        config.update({"measure_level_queries": measure_level_queries})
        attribute_metrics = save_measures(
            measure_type, measures, measure_result, config
        )
        if measure_type == STATISTICS and attribute_metrics:
            update_attribute_statistcs(config, attribute_metrics)


def __get_advanced_queries(queries: dict):
    """
    Return all the advanced queries
    """
    adv_queries = deepcopy(queries.get("advanced", {}))
    advanced_queries = {}
    for _, value in adv_queries.items():
        advanced_queries.update(value)
    return advanced_queries


def get_range_value(value: str):
    if value is None:
        return value
    try:
        if "." in str(value):
            value = float(value) if value else 0
        else:
            value = int(value) if value else 0
    except:
        value = None
    return value


def prepare_query(measure: dict, config: dict, default_queries: dict, attribute: dict):
    """
    Returns the list of measure queries for all the selected
    attributes available in an asset
    """

    log_info(("measure",measure))
    log_info(("config",config))
    attribute_name = attribute.get("name")
    attribute_label = get_attribute_label(attribute_name)
    measure_name = measure.get("name")
    measure_type = measure.get("type").lower()
    level = measure.get("level")
    measure_category = measure.get("category").lower()
    total_records = int(attribute.get("row_count", 0))
    asset = config.get("asset")
    asset = asset if asset else {}
    log_info(("asset",asset))
    connection = config.get("connection")
    connection = connection if connection else {}
    connection_type = connection.get("type", "")
    if level == "asset":
        total_records = int(asset.get("row_count", 0))

    query_string = ""
    isfullquery = False
    if measure_type == FREQUENCY and measure_category == RANGE:
        attribute_id = attribute.get("id")
        table_name = config.get("table_name")
        queries = default_queries.get("range")
        properties = measure.get("properties")
        properties = properties if properties else {}
        properties = (
            json.loads(properties)
            if properties and isinstance(properties, str)
            else properties
        )
        properties = properties if properties else {}
        query_type = properties.get("type")
        is_auto = properties.get("is_auto")
        threshold_constraints = properties.get("threshold_constraints")
        threshold_constraints = threshold_constraints if threshold_constraints else {}
        query = queries.get(query_type)
        attribute_metadata = get_attribute_metadata(config, attribute_id)

        min_value, max_value = 0, 0
        if query_type == "value":
            min_value = attribute_metadata.get("min_value")
            max_value = attribute_metadata.get("max_value")
            min_value = get_range_value(min_value)
            max_value = get_range_value(max_value)
        elif query_type == "length":
            min_value = attribute_metadata.get("min_length")
            max_value = attribute_metadata.get("max_length")
            min_value = get_range_value(min_value)
            max_value = get_range_value(max_value)

        if (min_value is None) or (max_value is None):
            return query_string

        if not is_auto:
            if not threshold_constraints:
                threshold_constraints = {
                    "condition": "isNotBetween",
                    "priority": "High",
                }
            threshold_constraints.update(
                {"value": min_value, "value2": max_value})
            update_range_measure(config, measure, threshold_constraints)
        query_string = prepare_query_string(
            query, table_name=table_name, attribute=attribute_name
        )
        query_string = query_string.replace("<value1>", str(min_value)).replace(
            "<value2>", str(max_value)
        )
        query_string = get_query_string(
            config, default_queries, query_string, is_full_query=True
        )
    elif measure_type not in [CUSTOM, SEMANTIC_MEASURE]:
        queries = __get_advanced_queries(default_queries)
        query = queries.get(measure_name)

        if isinstance(query, dict):
            query = query.get("query")
            isfullquery = True

        if not query:
            return query_string

        # Prepare the query for JSON attributes
        query = prepare_json_attribute_flatten_query(config, attribute_name, query, "sub_query")
        
        query = prepare_query_string(query, attribute=attribute_name)

        if isfullquery:
            query = query.replace("<attribute_label>", attribute_label)
        else:
            query = f"""{query} AS {attribute_label}{measure_name}"""

        query_string = get_query_string(
            config, default_queries, query, total_records)
    else:
        category = measure.get("category", "").lower()
        if category not in CUSTOM_MEASURE_CATEGORIES:
            return query_string

        measure_name = measure.get("name")
        query = measure.get("query")
        semantic_query = measure.get("semantic_query")
        if measure_type == SEMANTIC_MEASURE and semantic_query:
            query = semantic_query
            # Replace << with < only if not preceded by '-'
            query = re.sub(r'(?<!-)(<<)', '<', query)
            # Replace >> with > only if not followed by '>'
            query = re.sub(r'(>>)(?!>)', '>', query)
            
            # Notation replacement
            notation_list = extract_notation_from_query(query)
            for notation in notation_list:
                asset_id = asset.get("id", "")
                semantic_attribute_name = attribute_name
                try:
                    semantic_attribute_name = (
                        fetch_semantic_term_measure_attribute_name(
                            config,
                            term_name=notation,
                            asset_id=asset_id,
                            attribute_name=attribute_name,
                        )
                    )
                    query = query.replace(f"<{notation}>", semantic_attribute_name)
                except Exception as e:
                    log_info(
                        f"Failed to fetch semantic term measure attribute name: {str(e)}"
                    )
                    semantic_attribute_name = attribute_name
                if notation != 'attribute_label':
                    query = query.replace(f"<{notation}>", '')
                log_info(("query", query))
        query = query if query else ""
        table_name = config.get("table_name")
        if str(asset.get("view_type", "")).lower() == "direct query":
            table_name = f"""({asset.get("query")}) as direct_query_table"""
            if connection_type and str(connection_type).lower() in [
                ConnectionType.Oracle.value.lower(),
                ConnectionType.BigQuery.value.lower(),
            ]:
                table_name = f"""({asset.get("query")})"""

        measure_key = f"{attribute_label}{measure_name}"
        if category == CONDITIONAL:
            measure_key = measure_key[:60]
            
            if connection_type == ConnectionType.MongoDB.value:
                connector = config.get("connector")
                if connector and hasattr(connector, "prepare_custom_query"):
                    property_data = measure.get("property")
                    if property_data:
                        try:
                            custom_query = connector.prepare_custom_query(
                                property_data,
                                is_rule_assignee=False
                            )
                            if custom_query:
                                return custom_query
                        except Exception as e:
                            # Fall through to default processing
                            pass

        if not category == CONDITIONAL:
            # Prepare the query for JSON attributes
            query = prepare_json_attribute_flatten_query(config, attribute_name, query, "sub_query")
        
        query = query.replace("<attribute_label>", measure_key)
        query_string = prepare_query_string(
            query, table_name=table_name, attribute=attribute_name
        )
        if category not in [ QUERY ,PARAMETER ]:
            query_string = get_query_string(
                config, default_queries, query_string, is_full_query=True
            )

        if category in [QUERY, PARAMETER]:
            properties = measure.get("properties")
            properties = properties if properties else {}
            properties = (
                json.loads(properties)
                if properties and isinstance(properties, str)
                else properties
            )
            properties = properties if properties else {}
            input_params = properties.get("input_params", {})
            if category == PARAMETER:
                if input_params:
                    for key, value in input_params.items():
                        parameter = f"<{key}>"
                        if isinstance(value, list):
                            formatted_values = ", ".join(f"'{v}'" for v in value)
                            replacement = f"({formatted_values})"
                        else:
                            replacement = f"('{value}')"
                        
                        query_string = query_string.replace(parameter, replacement)
            is_direct_query_asset = config.get("is_direct_query_asset")
            base_table_query = config.get("base_table_query")

            if category == "query" and level in ["asset", "attribute"] and not is_direct_query_asset:
                query_string = get_incremental_query_string(config, default_queries, query_string)

            if is_direct_query_asset and base_table_query:
                query_string = process_direct_query(query_string, connection_type.lower())
                query_string = query_string.replace(
                    DIRECT_QUERY_BASE_TABLE_LABEL, f"({base_table_query})"
                )

    return query_string


def save_measure(status: str, measure_result: dict, config: dict) -> None:
    """
    Write the basic measures for this run into postgres db
    """
    attribute = config.get("attribute")
    attribute = attribute if attribute else {}
    attribute_name = attribute.get("name")
    measure = config.get("measure")
    measure = measure if measure else {}
    measure_category = measure.get("category")
    level = measure.get("level")
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    conditional_scoring_count = config.get("conditional_scoring_count")
    connection = get_postgres_connection(config)
    executed_query = config.get("query_string")
    executed_query = executed_query if executed_query else ""
    executed_query = executed_query.strip().replace("'", "''")
    if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
        executed_query = executed_query.replace(
            f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
        )

    is_measure_level = level == "measure" and not asset_id
    total_records = int(attribute.get("row_count", 0)) if attribute else 0
    properties = measure.get("properties")
    properties = properties if properties else {}
    properties = (
        json.loads(properties)
        if properties and isinstance(properties, str)
        else properties
    )
    properties = properties if properties else {}
    input_params = properties.get("input_params", {})
    if level == "asset":
        attribute_name = ""
        asset = config.get("asset")
        asset = asset if asset else {}
        total_records = int(asset.get("row_count", 0)) if asset else 0

    if conditional_scoring_count:
        total_records = (
            int(conditional_scoring_count)
            if conditional_scoring_count
            else total_records
        )
    with connection.cursor() as cursor:
        measure_id = measure.get("id")
        attribute_id = attribute.get("id")
        attribute_condition = f""" and attribute_id= '{attribute_id}' """
        if level == "asset":
            attribute_id = None
            attribute_condition = ""
        if is_measure_level:
            attribute_id = None
            attribute_condition = ""

        select_string = f""" 
            select id from core.metrics 
            where run_id='{run_id}'
            and measure_id = '{measure_id}'{attribute_condition}
        """

        cursor = execute_query(connection, cursor, select_string)
        metrics_details = fetchone(cursor)
        metrics_id = None
        if metrics_details:
            metrics_id = metrics_details.get("id", "")

        attribute_label = (
            get_attribute_label(attribute_name) if not is_measure_level else ""
        )
        measure_name = measure.get("name")
        is_positive = measure.get("is_positive", False)
        allow_score = is_scoring_enabled(
            config, measure.get("allow_score", False))
        is_drift_enabled = measure.get("is_drift_enabled", False)
        measure_type = measure.get("type", "").lower()
        category = measure.get("category", "")
        category = category.lower() if category else ""

        key = f"{attribute_label}{str(measure_name).lower()}"
        if measure_category == CONDITIONAL:
            key = key[:60]
        if measure_category == PARAMETER and input_params:
            key_values = [
                ",".join(v) if isinstance(v, list) else str(v)
                for v in input_params.values()
            ]
            measure_name = " || ".join(key_values)
        value = 0
        if status == PASSED:
            if (
                measure_result
                and measure_type in [CUSTOM, SEMANTIC_MEASURE, FREQUENCY]
                and category in [QUERY, PARAMETER, RANGE, LOOKUP, CROSS_SOURCE]
            ):
                for dict_key, value in measure_result.items():
                    if isinstance(value, int) or isinstance(value, float):
                        key = dict_key
                        break
            value = measure_result.get(key, 0)
            value = int(value) if value else 0
            value = parse_numeric_value(value)

        weightage = measure.get("weightage", 100)
        weightage = int(weightage) if weightage else 100
        valid_count = 0
        invalid_count = 0
        valid_percentage = 0
        invalid_percentage = 0
        score = None
        is_archived = False

        if status == PASSED and total_records and allow_score:
            valid_count = value
            if measure_category and measure_category.lower() == "range":
                valid_count = total_records - value

            valid_count = valid_count if is_positive else (
                total_records - valid_count)
            invalid_count = total_records - valid_count
            valid_percentage = float(valid_count / total_records * 100)
            invalid_percentage = float(100 - valid_percentage)
            score = valid_percentage
            score = calculate_weightage_score(score, weightage)
            score = 100 if score > 100 else score
            score = 0 if score < 0 else score

        query_input = (
            str(uuid4()),
            organization_id,
            connection_id,
            asset_id,
            attribute_id,
            measure_id,
            run_id,
            airflow_run_id,
            attribute_name,
            measure_name,
            level,
            str(value),
            weightage,
            total_records,
            valid_count,
            invalid_count,
            valid_percentage,
            invalid_percentage,
            score,
            status,
            is_archived,
            executed_query,
            allow_score,
            is_drift_enabled,
            True,
            True,
            False,
            attribute.get("parent_attribute_id", None),
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals},CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")
        try:
            if not metrics_id:
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id, attribute_name,measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,is_active, is_delete, parent_attribute_id, created_date)
                    values {query_param}
                """
                cursor = execute_query(
                    connection, cursor, attribute_insert_query)
            else:
                score = "null" if score is None else score
                attribute_insert_query = f"""
                    update core.metrics
                    set 
                    level = '{level}',
                    value = '{str(value)}',
                    weightage = {weightage},
                    total_count = {total_records},
                    valid_count = {valid_count},
                    invalid_count = {invalid_count},
                    valid_percentage = {valid_percentage},
                    invalid_percentage = {invalid_percentage},
                    score = {score},
                    status = '{status}',
                    is_archived = {is_archived},
                    query = '{executed_query}',
                    allow_score = {allow_score},
                    is_drift_enabled = {is_drift_enabled}
                    where id='{metrics_id}'
                """
                cursor = execute_query(
                    connection, cursor, attribute_insert_query)

            pass_criteria_result = check_measure_result(measure, score)
            score = "null" if score is None else score
            update_measure_score_query = f"""
                update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count}, result = '{pass_criteria_result}'
                where id='{measure_id}'{attribute_condition}
            """
            cursor = execute_query(
                connection, cursor, update_measure_score_query)
        except Exception as e:
            log_error("Save Measures: inserting new metric", e)


def save_measures(
    measure_type: str, measures: list, measure_result: dict, config: dict
) -> None:
    """
    Write the basic measures for this run into postgres db
    """
    attribute = config.get("attribute")
    asset_id = config.get("asset_id")
    connection_type = config.get("connection_type")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    measure_level_queries = config.get("measure_level_queries", {})
    measure_level_queries = measure_level_queries if measure_level_queries else {}
    connection = get_postgres_connection(config)
    attribute_metrics = {}

    total_records = int(attribute.get("row_count", 0)) if attribute else 0
    with connection.cursor() as cursor:
        measure_results = []
        for measure in measures:
            measure_id = measure.get("id")
            attribute_id = attribute.get("id")
            
            delete_metrics(
                config,
                run_id=run_id,
                measure_id=measure_id,
                attribute_id=attribute_id,
            )

            attribute_name = attribute.get("name")
            attribute_label = (
                get_attribute_label(attribute_name) if attribute_name else ""
            )
            measure_name = measure.get("name")
            is_positive = measure.get("is_positive", False)
            allow_score = is_scoring_enabled(
                config, measure.get("allow_score", False))
            is_drift_enabled = measure.get("is_drift_enabled", False)
            category = measure.get("category", "")
            category = category.lower() if category else ""

            level = measure.get("level")
            key = f"{attribute_label}{str(measure_name).lower()}"
            value = measure_result.get(key, 0)
            executed_query = measure_level_queries.get(key, "")
            executed_query = executed_query if executed_query else ""
            executed_query = executed_query.strip().replace("'", "''")

            if config.get("has_temp_table", False) and config.get("table_name", "") and config.get("temp_view_table_name", ""):
                executed_query = executed_query.replace(
                    f'''{config.get("temp_view_table_name", "").strip()}''', f'''{config.get("table_name", "").strip()}'''
                )

            if "java" and "java.math.BigDecimal" in str(type(value)):
                value = float(str(value))
            value = value if value else 0
            if measure_type == STATISTICS:
                value = parse_numeric_value(value)
                metric = attribute_metrics.get(attribute_id)
                metric = metric if metric else {}
                metric_value = get_range_value(value)
                metric.update({str(measure_name): metric_value})
                if "min_value" not in metric:
                    min_value = attribute.get("min_value")
                    min_value = get_range_value(min_value)
                    metric.update({"min_value": min_value})
                if "max_value" not in metric:
                    max_value = attribute.get("max_value")
                    max_value = get_range_value(max_value)
                    metric.update({"max_value": max_value})
                if max_value:
                    min_value = min_value if min_value else 0
                    metric.update({"range": (max_value - min_value)})
                attribute_metrics.update({attribute_id: metric})
                # add not null condition for statitstics measures
                if executed_query:
                    not_null_condition = (
                        ""
                        if connection_type == ConnectionType.SapHana.value
                        and measure_name == "skewness"
                        else f" WHERE {attribute_name} IS NOT NULL"
                    )
                    executed_query += not_null_condition
            weightage = measure.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None
            is_archived = False

            if total_records and allow_score:
                valid_count = value
                valid_count = (
                    valid_count if is_positive else (
                        total_records - valid_count)
                )
                invalid_count = total_records - valid_count
                valid_percentage = float(valid_count / total_records * 100)
                invalid_percentage = float(100 - valid_percentage)
                score = valid_percentage
                score = calculate_weightage_score(score, weightage)
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                # score = round(score, 2)

            query_input = (
                str(uuid4()),
                organization_id,
                connection_id,
                asset_id,
                attribute_id,
                measure_id,
                run_id,
                airflow_run_id,
                attribute_name,
                measure_name,
                level,
                str(value),
                weightage,
                total_records,
                valid_count,
                invalid_count,
                valid_percentage,
                invalid_percentage,
                score,
                PASSED,
                is_archived,
                executed_query,
                allow_score,
                is_drift_enabled,
                True,
                True,
                False,
                attribute.get("parent_attribute_id", None),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            measure_results.append(query_param)

            try:
                pass_criteria_result = check_measure_result(measure, score)
                score = "null" if score is None else score
                update_measure_score_query = f"""
                    update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, score={score}, failed_rows=null, row_count = {total_records}, valid_rows = {valid_count}, invalid_rows = {invalid_count} , result = '{pass_criteria_result}'
                    where id='{measure_id}'
                """
                cursor = execute_query(
                    connection, cursor, update_measure_score_query)

            except Exception as e:
                pass

        measures_input = split_queries(measure_results)
        for input_values in measures_input:
            try:
                input_value = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id,attribute_name, measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,is_active,is_delete, parent_attribute_id, created_date)
                    values {input_value}
                """
                cursor = execute_query(
                    connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("Save Measures: inserting new metric", e)
    return attribute_metrics


def update_attribute_row_count(config: dict, attribute: dict = None) -> list:
    """
    Returns the list of selected attributes for the given asset
    """
    try:
        asset_id = config.get("asset_id")
        if not attribute:
            attribute = config.get("attribute", {})
        attribute_id = attribute.get("id")
        if not attribute_id:
            return

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select row_count from core.data
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            version = fetchone(cursor)
            row_count = version.get("row_count", 0)
            row_count = row_count if row_count else 0
            attribute.update({"row_count": row_count})
        config.update({"attribute": attribute})
    except Exception as e:
        raise e


def update_attribute_statistcs(config: dict, statistics: dict) -> list:
    """
    Returns the list of selected attributes for the given asset
    """
    try:

        if not statistics:
            return
        asset_id = config.get("asset_id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for attribute_id, attribute_statistics in statistics.items():
                attribute_statistics = (
                    attribute_statistics if attribute_statistics else {}
                )
                if "min_value" in attribute_statistics:
                    min_value = attribute_statistics.get("min_value")
                    attribute_statistics.update({"q0": min_value})
                if "max_value" in attribute_statistics:
                    max_value = attribute_statistics.get("max_value")
                    attribute_statistics.update({"q4": max_value})
                if "median" in attribute_statistics:
                    median = attribute_statistics.get("median")
                    attribute_statistics.update({"q2": median})
                if "q1" in attribute_statistics and "q3" in attribute_statistics:
                    q1 = attribute_statistics.get("q1")
                    q1 = q1 if q1 else 0
                    q3 = attribute_statistics.get("q3")
                    q3 = q3 if q3 else 0
                    iqr = q3 - q1
                    iqr = iqr if iqr > 0 else 0
                    attribute_statistics.update({"iqr": iqr})

                for key in attribute_statistics:
                    value = attribute_statistics.get(key)
                    if isinstance(value, str):
                        value = get_range_value(value)
                        attribute_statistics.update({key: value})
                attribute_statistics = json.dumps(
                    attribute_statistics, default=str)
                attribute_statistics = attribute_statistics.replace("'", "''")
                query_string = f"""
                    update core.attribute set statistics='{attribute_statistics}'
                    where asset_id='{asset_id}'
                    and id='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        raise e


def get_attribute_metadata(config: dict, attribute_id: str) -> list:
    """
    Returns the list of selected attributes for the given asset
    """
    try:
        attribute = {}
        asset_id = config.get("asset_id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select * from core.attribute
                where asset_id='{asset_id}'
                and is_selected=True and id='{attribute_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            attribute = fetchone(cursor)
        return attribute
    except Exception as e:
        raise e


def execute_range_measures(config: dict, default_queries: dict) -> None:
    """
    Execute attribute level patterns
    """
    attribute = config.get("attribute")
    attribute_id = attribute.get("id")
    measures = get_range_measures(config, attribute_id)
    if not measures:
        return

    for measure in measures:
        config.update({"measure": deepcopy(measure)})
        execute_measure(measure, config, default_queries)


def get_deprecated_attributes(config: dict, asset_id: str) -> list:
    """
    Get the list of all the deprecated attributes for the given asset
    """
    connection = get_postgres_connection(config)
    attributes = []
    with connection.cursor() as cursor:
        query_string = f"""
            select * from core.attribute
            where status='{ApprovalStatus.Deprecated.value}' and asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        attributes = fetchall(cursor)
    return attributes


def is_deprecated(config: dict, default_queries: dict, attributes: list = []):
    is_deprecated_asset = False
    connection_type = config.get("connection_type")
    deprecated_attributes = []
    pending_attributes = []
    deprecated_columns_query = default_queries.get("deprecated_columns", "")
    profile_database_name = config.get("profile_database_name")
    profile_database_name = profile_database_name if profile_database_name else ""
    profile_schema_name = config.get("profile_schema_name")
    profile_schema_name = profile_schema_name if profile_schema_name else ""

    connection = config.get("connection")
    connection = connection if connection else {}
    asset_status = config.get("asset_status")
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    existing_deprecated_attributes = []
    log_info(("asset_id", asset_id))
    if asset_id:
        existing_deprecated_attributes = get_deprecated_attributes(
            config, asset_id)
    view_type = asset.get("view_type") if asset else ""
    view_type = str(view_type).lower() if view_type else "table"
    asset_type = asset.get("type") if asset else ""
    asset_type = asset_type.lower() if asset_type else ""
    is_query_mode = bool(asset_type.lower() == "query")
    table_name = asset.get("technical_name").replace("'", "\\'")
    if (connection_type in [ConnectionType.Snowflake.value, ConnectionType.BigQuery.value, ConnectionType.MSSQL.value]):
        table_name = asset.get("name").replace("'", "\\'")
    database = ""
    if connection:
        credentials = connection.get("credentials")
        credentials = (
            json.loads(credentials)
            if credentials and isinstance(credentials, str)
            else credentials
        )
        credentials = credentials if credentials else {}
        database = credentials.get("database")
        database = database if database else ""

    properties = asset.get("properties", {})
    properties = json.loads(properties) if isinstance(
        properties, str) else properties
    properties = properties if properties else {}

    database_name = properties.get("database")
    database = database_name if database_name else database
    schema = properties.get("schema")
    schema = schema if schema else ""

    columns = []
    if not is_query_mode:
        if connection_type == ConnectionType.Hive.value or (
            connection_type == ConnectionType.Databricks.value
            and database_name == "hive_metastore"
        ):
            table_name = table_name.strip().upper() if table_name else ""
            schema = config.get("schema")
            schema = schema.strip().upper() if schema else ""

        if (
            connection_type == ConnectionType.Databricks.value
            and database_name == "hive_metastore"
        ):
            deprecated_columns_query = default_queries.get("hive_metastore", {}).get(
                "deprecated_columns", ""
            )

        query = (
            deprecated_columns_query.replace("<database_name>", database)
            .replace("<table_name>", table_name)
            .replace("<schema_name>", schema)
        )
        columns, _ = execute_native_query(config, query, None, is_list=True)
        if connection_type == ConnectionType.Salesforce.value and not columns:
            pg_connection = get_postgres_connection(config)
            query_string = (
            deprecated_columns_query.replace("<table_name>", table_name)
            )
            columns = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= query_string),
            )
        if connection_type == ConnectionType.SalesforceDataCloud.value and not columns:
            pg_connection = get_postgres_connection(config)
            table_name = asset.get("name")
            object_id = properties.get("object_id")
            asset_type = properties.get("asset_type")
            columns = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes",table_name = table_name,asset_type = asset_type, object_id = object_id),
            )
        if connection_type == ConnectionType.SalesforceMarketing.value and not columns:
            pg_connection = get_postgres_connection(config)
            query_string = (
            deprecated_columns_query.replace("<table_name>", table_name)
            )
            columns = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= query_string),
            )
        columns = convert_to_lower(columns)

        if (
                connection_type == ConnectionType.Hive.value
                or connection_type == ConnectionType.ADLS.value
                or connection_type == ConnectionType.File.value
                or connection_type == ConnectionType.S3.value
                or (
                    connection_type == ConnectionType.Databricks.value
                    and database_name == "hive_metastore"
                )
            ):
            columns = [
                {
                    "column_name": column.get("col_name"),
                    "datatype": column.get("data_type"),
                }
                for column in columns
                if column
            ]
        if columns and connection_type == ConnectionType.SalesforceMarketing.value:
            columns = [
                {
                    "column_name": column.get("columnname"),
                    "datatype": column.get("datatypename"),
                    "is_null": True if column.get("isnullable", 0) == 1 else False,
                    "description": column.get("description", "")
                }
                for column in columns
            ]
        if connection_type == ConnectionType.MongoDB.value:
            is_deprecated_asset = len(columns) > 0  # Has deprecated columns = deprecated
        else:
            is_deprecated_asset = len(columns) == 0  # Empty = table doesn't exist = deprecated
    else:
        try:
            default_query_with_limit = default_queries.get(
                "default_query_limit")
            database = profile_database_name if profile_database_name else database
            schema = profile_schema_name if profile_schema_name else schema
            query = asset.get("query")
            columns = []
            if default_query_with_limit:
                query_with_alias = f"({query}) as direct_query_table"
                if connection_type in [
                    ConnectionType.Oracle.value,
                    ConnectionType.BigQuery.value,
                ]:
                    query_with_alias = f"({query})"
                asset_query = (
                    default_query_with_limit.replace("<count>", "10")
                    .replace("<query>", "*")
                    .replace("<table_name>", query_with_alias)
                )
                data, _ = execute_native_query(config, asset_query)
                data = data if data else {}
                columns = list(data.keys()) if data else []
                columns = [{"column_name": column}
                           for column in columns if column]
        except Exception as error:
            is_deprecated_asset = bool(
                error
                and hasattr(error, "message")
                and "does not exist" in error.message
            )

    if not is_deprecated_asset and attributes and columns:
        deprecated_attributes = []
        for attribute in attributes:
            attribute_name = attribute.get("name")
            attribute_name = attribute_name.lower() if attribute_name else ""
            json_attribute_parent_id = attribute.get("parent_attribute_id")
            has_attribute = next(
                (
                    column
                    for column in columns
                    if column.get("column_name")
                    and (
                        column.get("column_name").lower() == attribute_name.lower()
                        or json_attribute_parent_id is not None
                    )
                ),
                None,
            )
            if has_attribute:
                continue
            deprecated_attributes.append(deepcopy(attribute))

        pending_attributes = []
        if existing_deprecated_attributes and columns:
            for attribute in existing_deprecated_attributes:
                attribute_name = attribute.get("name")
                attribute_name = attribute_name.lower() if attribute_name else ""
                has_attribute = next(
                    (
                        column
                        for column in columns
                        if column.get("column_name")
                        and column.get("column_name").lower() == attribute_name
                    ),
                    None,
                )
                if not has_attribute:
                    continue

                if attribute.get("attribute_id"):
                    pending_attributes.append(attribute.get("attribute_id"))

    if (
        (
            (
                asset_status
                and asset_status == ApprovalStatus.Deprecated.value
                and not is_deprecated_asset
            )
            or (
                asset_status
                and asset_status != ApprovalStatus.Deprecated.value
                and is_deprecated_asset
            )
        )
        or len(pending_attributes) > 0
        or len(deprecated_attributes) > 0
    ):
        change_version(config)

    if pending_attributes:
        revert_deprecated_attribute_status(config, pending_attributes)

    if (
        not is_deprecated_asset
        and asset_status
        and asset_status == ApprovalStatus.Deprecated.value
    ):
        revert_deprecated_asset_status(config)
    return is_deprecated_asset, deprecated_attributes


def deprecate_asset(config: dict):
    """
    Deprecate all the asset related attributes and it's measures
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    if not asset_id:
        return

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.asset set status='{ApprovalStatus.Deprecated.value}'
            where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        query_string = f"""
            update core.attribute set status='{ApprovalStatus.Deprecated.value}', is_primary_key = false
            where asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        query_string = f"""
            update core.measure set status='{ApprovalStatus.Deprecated.value}'
            where asset_id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        # create_version_history(config, ApprovalStatus.Deprecated.value)
        
        sync_data = {
            "asset": asset_id,
            "event": "sync",
            "action": "deprecated",
            "event_type": "sync",
            "properties": {
                "type": "asset_sync",
                "status": "deprecated"
            },
            "source": "airflow"
        }
        save_sync_event(config, sync_data)


def deprecate_attributes(config: dict, attribute_ids: list):
    """
    Deprecate all the asset related attributes and it's measures
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    if not attribute_ids:
        return

    attribute_id = [f"'{str(id)}'" for id in attribute_ids if id]
    attribute_id = ", ".join(attribute_id) if attribute_id else ""
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.attribute set status='{ApprovalStatus.Deprecated.value}', is_primary_key = false
            where asset_id='{asset_id}' and attribute.id in ({attribute_id}) and attribute.parent_attribute_id is null
        """
        cursor = execute_query(connection, cursor, query_string)

        query_string = f"""
            update core.measure
            set status='{ApprovalStatus.Deprecated.value}'
            from core.attribute
            where core.measure.attribute_id = core.attribute.id
              and core.measure.asset_id='{asset_id}'
              and core.measure.attribute_id in ({attribute_id})
              and core.attribute.parent_attribute_id is null
        """
        cursor = execute_query(connection, cursor, query_string)

    sync_data = {
        "asset": asset_id,
        "event": "sync",
        "action": "deprecated",
        "event_type": "sync",
        "properties": {
            "type": "attribute_sync",
            "attribute_ids": attribute_ids
        },
        "source": "airflow",
    }
    save_sync_event(config, sync_data)

def revert_deprecated_asset_status(config: dict):
    """
    Revert all the asset, attributes and it's measures status to Pending
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.asset set status='{ApprovalStatus.Pending.value}'
            where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
    revert_deprecated_attribute_status(config, [])

    sync_data = {
        "asset": asset_id,
        "event": "sync",
        "action": "restored",
        "event_type": "sync",
        "properties": {
            "type": "asset_sync",
            "status": "restored"
        },
        "source": "airflow"
    }
    save_sync_event(config, sync_data)


def revert_deprecated_attribute_status(config: dict, attribute_ids: list):
    """
    Revert all the asset related attributes and it's measures status to Pending
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    update_version_history = bool(attribute_ids)

    attribute_id = [f"'{str(id)}'" for id in attribute_ids if id]
    attribute_id = ", ".join(attribute_id) if attribute_id else ""

    attribute_condition = ""
    measure_attribute_condition = ""
    if attribute_id:
        attribute_condition = f"and id in ({attribute_id})"
        measure_attribute_condition = f"and mes.attribute_id in ({attribute_id})"

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.attribute set status='{ApprovalStatus.Pending.value}'
            where asset_id='{asset_id}' {attribute_condition}
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update default system (OOB) measure status
        query_string = f"""
            with measures as (
                select mes.id from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                where mes.asset_id='{asset_id}' {measure_attribute_condition}
                and base.is_default=True and mes.status='{ApprovalStatus.Deprecated.value}'
            )
            update core.measure set status='{ApprovalStatus.Verified.value}'
            where asset_id='{asset_id}' {attribute_condition}
            and id in (select id from measures)
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update custom measure status
        query_string = f"""
            with measures as (
                select mes.id from core.measure as mes
                join core.base_measure as base on base.id=mes.base_measure_id
                where mes.asset_id='{asset_id}' {measure_attribute_condition}
                and base.is_default=False and mes.status='{ApprovalStatus.Deprecated.value}'
            )
            update core.measure set status='{ApprovalStatus.Pending.value}'
            where asset_id='{asset_id}' {attribute_condition}
            and id in (select id from measures)
        """
        cursor = execute_query(connection, cursor, query_string)

        if attribute_ids:
            sync_data = {
                "asset": asset_id,
                "event": "sync",
                "action": "restored",
                "event_type": "sync",
                "properties": {
                    "type": "attribute_sync",
                    "status": "restored",
                    "attribute_ids": attribute_ids if attribute_ids else []
                },
                "source": "airflow"
            }
            save_sync_event(config, sync_data)


def __get_version_max_value(version_no: int) -> float:
    value = "9.9"
    if version_no == 2:
        value = "9.9.9"
    elif version_no == 3:
        value = "9.9.9.9"
    elif version_no == 4:
        value = "9.9.9.9.9"
    return value


def __update_major_version(version: str, versioning_digit: int) -> str:
    """
    Update the first part of the version (major update), reset the rest to zero.
    For example: 0.0.0.2 -> 1.0.0.0
    """
    version_parts = [int(v) for v in version.split(".")]
    while len(version_parts) < versioning_digit:
        version_parts.append(0)

    # Update the first part (major version)
    version_parts[0] += 1

    # Reset all other parts to zero after the first part
    for i in range(1, len(version_parts)):
        version_parts[i] = 0

    updated_version = ".".join(map(str, version_parts))
    return updated_version


def compare_versions(max_version, version_number):
    
    if not max_version or not version_number:
        return False
    if max_version == version_number:
        return False
    # Split the versions by '.' and convert each part to an integer
    v1_parts = list(map(int, max_version.split(".")))
    v2_parts = list(map(int, version_number.split(".")))
    status = True

    # Compare each part of the version
    for part1, part2 in zip(v1_parts, v2_parts):
        if part1 < part2:
            status = False
            return status
    return status


def change_version(config: dict, is_major_version=False):
    """
    Update the version when changing the status
    """
    asset = config.get("asset")
    created_by = asset.get("created_by")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    if not asset_id:
        return

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

    versioning = general_settings.get("versioning")
    versioning = versioning if versioning else {}
    versioning = (
        json.loads(versioning, default=str)
        if isinstance(versioning, str)
        else versioning
    )
    versioning = versioning if versioning else {}

    is_version_active = versioning.get("is_active", False)

    connection = get_postgres_connection(config)

    with connection.cursor() as cursor:
        version_query = f"""
            select * from core.version where asset_id='{asset_id}' and is_active='true' limit 1
        """
        cursor = execute_query(connection, cursor, version_query)
        existing_version = fetchone(cursor)
        existing_version = existing_version if existing_version else {}
        versioing_digit = existing_version.get("versioning_digit")
        existing_versioing_id = existing_version.get("id")
        max_version_count = __get_version_max_value(versioing_digit)
        version_no = existing_version.get("version")

        if is_version_active and compare_versions(max_version_count, version_no):
            version_number = __update_major_version(
                version=version_no, versioning_digit=versioing_digit
            )

            create_version = f"""
            insert into core.version (
                id, version, versioning_digit, is_active, is_delete, created_by, updated_by, created_date, asset_id, is_latest
            ) 
            values
            ( '{str(uuid4())}', '{version_number}', {versioing_digit}, true, false,
            '{created_by}', '{created_by}', CURRENT_TIMESTAMP, '{asset_id}', false )
            """
            cursor = execute_query(connection, cursor, create_version)

            update_version = f"""
                update core.version set is_active=False, is_latest=False
                where id='{existing_versioing_id}' and asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, update_version)


def create_version_history(
    config: dict,
    version_type: str = "",
    sub_type: str = "",
    value: str = "",
    attribute_id: str = "",
    is_audit: bool = True,
):
    """
    Create a version history
    """
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_id = asset.get("id")
    created_by = asset.get("created_by")
    attribute_id = attribute_id if attribute_id else None

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        version_query = f"""
            select * from core.version where asset_id='{asset_id}' and is_active='true' limit 1
        """
        cursor = execute_query(connection, cursor, version_query)
        existing_version = fetchone(cursor)
        version_id = existing_version.get("id")
        if not version_id:
            return

        query_input = (
            str(uuid4()),
            version_type,
            sub_type,
            value,
            is_audit,
            asset_id,
            version_id,
            attribute_id,
            True,
            False,
            created_by,
        )
        input_literals = ", ".join(["%s"] * len(query_input))
        query_param = cursor.mogrify(
            f"({input_literals},CURRENT_TIMESTAMP)",
            query_input,
        ).decode("utf-8")

        create_version_history = f"""
            insert into core.version_history (id, type, sub_type, value, is_audit, asset_id,
            version_id, attribute_id, is_active, is_delete, created_by, created_date)
            values {query_param}
        """
        cursor = execute_query(connection, cursor, create_version_history)


def update_asset_run_id(run_id: str, config: dict) -> None:
    """
    Updates the recent row count into metadata tables
    """
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    run_id = config.get("run_id")

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.asset set last_run_id='{run_id}'
            where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)


def update_recent_run_alert(config: dict, selected_attributes: list = []):
    """
    Update the recent run alerts for asset and attribute
    """
    level = config.get("level")
    level = str(level).lower() if level else ""
    queue_id = config.get("queue_id")
    asset_id = config.get("asset_id")
    attribute_id = config.get("attribute_id")
    if level == "measure":
        return

    attribute_queries = []
    attribute_filter_condition = ""
    if level == "attribute" and attribute_id:
        attribute_queries.append(f"metrics.attribute_id='{attribute_id}'")
    if attribute_queries:
        attribute_filter_condition = " and ".join(attribute_queries)

    if len(attribute_queries) > 0:
        attribute_filter_condition = " and " + attribute_filter_condition

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if level == "asset":
            query_string = f"""
                select asset_id, sum(
                    case when lower(drift_status) in ('high', 'medium', 'low') then 1 else 0 end
                ) as alert_count,
                max(created_date) as created_date
                from core.metrics
                where asset_id='{asset_id}' and run_id='{queue_id}'
                group by asset_id
            """
            cursor.execute(query_string)
            asset_alert = fetchone(cursor)
            asset_alert = asset_alert if asset_alert else {}

            alert_count = asset_alert.get("alert_count")
            asset = config.get("asset", {})
            asset = asset if asset else {}

            asset_last_runs = asset.get("last_runs", [])
            asset_last_runs = (
                json.loads(asset_last_runs)
                if asset_last_runs and isinstance(asset_last_runs, str)
                else asset_last_runs
            )
            asset_last_runs = asset_last_runs if asset_last_runs else []
            asset_last_runs = list(
                filter(lambda x: x.get("run_id") != queue_id, asset_last_runs)
            )
            asset_last_runs.insert(
                0,
                {
                    "asset_id": asset_id,
                    "run_id": queue_id,
                    "alert_count": alert_count,
                    "status": (
                        "alert" if alert_count and alert_count > 0 else "no_alert"
                    ),
                    "created_date": asset_alert.get("created_date"),
                },
            )
            if len(asset_last_runs) > 7:
                asset_last_runs = asset_last_runs[:7]

            query_input = (
                asset_id,
                json.dumps(asset_last_runs, default=str),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals})",
                query_input,
            ).decode("utf-8")
            input_values = [query_param]

            # update asset level last run values
            try:
                query_input = ",".join(input_values)
                last_run_insert_query = f"""
                    update core.asset as asset set last_runs = to_json(c.last_runs::text)
                    from (values {query_input}) as c(asset_id, last_runs)
                    where c.asset_id = asset.id::text;
                """
                cursor = execute_query(
                    connection, cursor, last_run_insert_query)
            except Exception as e:
                log_error("update last run alerts: asset level", e)
                raise e

        query_string = f"""
            select attribute_id, sum(
                case when lower(drift_status) in ('high', 'medium', 'low') then 1 else 0 end
            ) as alert_count,
            max(created_date) as created_date
            from core.metrics
            where asset_id='{asset_id}' and run_id='{queue_id}'
            and attribute_id is not null {attribute_filter_condition}
            group by attribute_id
        """
        cursor.execute(query_string)
        attribute_alerts = fetchall(cursor)
        attribute_alerts = attribute_alerts if attribute_alerts else []
        if not selected_attributes:
            selected_attribute = attribute_id if level == "attribute" else ""
            selected_attributes = get_selected_attributes(
                config, selected_attribute)
            selected_attributes = (
                [selected_attributes]
                if isinstance(selected_attributes, dict)
                else selected_attributes
            )

        attribute_last_run_values = []
        for attribute_alert in attribute_alerts:
            attribute_id = attribute_alert.get("attribute_id")
            alert_count = attribute_alert.get("alert_count")
            attribute = next(
                (
                    attribute
                    for attribute in selected_attributes
                    if attribute.get("id") == attribute_id
                ),
                None,
            )
            attribute = attribute if attribute else {}
            last_runs = attribute.get("last_runs", [])
            last_runs = (
                json.loads(last_runs)
                if last_runs and isinstance(last_runs, str)
                else last_runs
            )
            last_runs = last_runs if last_runs else []
            last_runs = list(filter(lambda x: x.get(
                "run_id") != queue_id, last_runs))
            last_runs.insert(
                0,
                {
                    "asset_id": asset_id,
                    "attribute_id": attribute_id,
                    "run_id": queue_id,
                    "alert_count": alert_count,
                    "status": "alert" if alert_count > 0 else "no_alert",
                    "created_date": attribute_alert.get("created_date"),
                },
            )
            if len(last_runs) > 7:
                last_runs = last_runs[:7]

            query_input = (
                attribute_id,
                json.dumps(last_runs, default=str),
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals})",
                query_input,
            ).decode("utf-8")
            attribute_last_run_values.append(query_param)

        # update attribute level last run values
        attribute_last_run_values = split_queries(attribute_last_run_values)
        for input_values in attribute_last_run_values:
            try:
                query_input = ",".join(input_values)
                last_run_insert_query = f"""
                    update core.attribute as attribute set last_runs = to_json(c.last_runs::text)
                    from (values {query_input}) as c(attribute_id, last_runs)
                    where c.attribute_id = attribute.id::text;
                """
                cursor = execute_query(
                    connection, cursor, last_run_insert_query)
            except Exception as e:
                log_error("update last run alerts: attribute level", e)
                raise e


def remove_depricated_primary_keys(
    config: dict, primary_keys: list, source_attribute: list
):
    primary_columns = [
        primary_key
        for primary_key in primary_keys
        if primary_key.get("name", "") in source_attribute
    ]
    primary_key_columns = json.dumps(primary_columns, default=str)
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            update core.data set primary_columns='{primary_key_columns}'
            where asset_id='{asset_id}' and connection_id='{connection_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

    return primary_columns

def get_table_schema(
       config, default_queries, table_name: str, schema_name: str = "", db_name: str = "", **kwargs
    ):
        query_string = default_queries.get("attributes")
        primary_key_query = default_queries.get("primary_key")
        connection_type = config.get("connection_type")
        if connection_type in [ConnectionType.MSSQL.value, ConnectionType.Redshift.value, ConnectionType.Athena.value]:
            schema_condition = (
                f"AND UPPER(T.TABLE_SCHEMA) = '{schema_name.strip().upper()}'"
                if schema_name
                else ""
            )
            query_string = (
                query_string.replace("<table_name>", table_name.strip().upper())
                .replace("<schema_condition>", schema_condition)
                .replace("<database_name>", db_name)
            )
            schema, _ = execute_native_query(
                            config, query_string, connection_type, is_list=True
                    )
            primary_keys = []
            if primary_key_query:
                primary_key_query = (
                    primary_key_query.replace("<table_name>", table_name.strip().upper())
                    .replace("<schema_condition>", schema_condition)
                    .replace("<database_name>", db_name)
                )
                primary_keys, _ = execute_native_query(
                            config, primary_key_query, connection_type, is_list=True
                    )

            primary_key_columns = []
            for primary_key in primary_keys:
                primary_key_columns.append(primary_key.get("column_name"))

            for column in schema:
                column_name = column.get("column_name")
                column.update({"is_primary_key": (column_name in primary_key_columns)})
            return schema

        if connection_type == ConnectionType.Snowflake.value:
            table_name = table_name.replace("'", "''")
            schema_condition = (
                f"AND T.TABLE_SCHEMA = '{schema_name.strip()}'" if schema_name else ""
            )
            query_string = (
                query_string.replace("<table_name>", table_name.strip().upper())
                .replace("<schema_condition>", schema_condition)
                .replace("<database_name>", db_name)
            )
            schema, _ = execute_native_query(
                            config, query_string, connection_type, is_list=True
                    )
            primary_keys = []
            if primary_key_query:
                if schema_name:
                    table_name = table_name.replace("''", "'")
                    table_name = f""" "{db_name}"."{schema_name}"."{table_name}" """
                primary_key_query = primary_key_query.replace(
                    "<table_name>", table_name.strip()
                )
                try:
                    primary_keys, _ = execute_native_query(
                            config, primary_key_query, connection_type, is_list=True
                    )  
                except:
                    primary_keys = []

            primary_key_columns = []
            for primary_key in primary_keys:
                primary_key_columns.append(primary_key.get("column_name"))

            for column in schema:
                column_name = column.get("column_name")
                column.update({"is_primary_key": (column_name in primary_key_columns)})
            return schema
        if connection_type == ConnectionType.Databricks.value:
            describe_string = (
                f""" describe `<database_name>`.`<schema_name>`.`<table_name>`"""
            )
            describe_string = (
                describe_string.replace("<table_name>", table_name)
                .replace("<schema_name>", schema_name)
                .replace("<database_name>", db_name)
            )
            response, _ = execute_native_query(
                            config, describe_string, connection_type, is_list=True
                )
            df = pd.DataFrame.from_records(response)
            if db_name.lower() == "hive_metastore":
                if "is_watermark" in kwargs:
                    return response
                else:
                    for _, column in enumerate(response):
                        column["column_name"] = column.pop("col_name")
                        column["datatype"] = column.pop("data_type")
                        column["description"] = column.pop("comment")
                        column["is_primary_key"] = False
                        column["schema"] = schema_name
                        column["database"] = db_name
                return response
            else:
                df["col_name"] = df["col_name"].apply(lambda x: x.upper())
                result_columns = df["col_name"]
                if len(result_columns) == 1:
                    result_columns = list(result_columns)
                    result_columns.append(result_columns[0])
                describe_columns = tuple(result_columns)
                schema_condition = (
                    f"AND UPPER(C.TABLE_SCHEMA) = UPPER('{schema_name.strip()}')"
                    if schema_name
                    else ""
                )
                table_name = table_name.replace("'", "\\'")
                query_string = (
                    query_string.replace("<table_name>", table_name.strip())
                    .replace("<schema_condition>", schema_condition)
                    .replace("<database_name>", db_name)
                )
                query_string = query_string.replace(";", "")
                query_string = (
                    f""" {query_string} AND UPPER(C.COLUMN_NAME) IN {describe_columns}"""
                )
                schema, _ = execute_native_query(
                            config, query_string, connection_type, is_list=True
                    )
                primary_keys = []
                if primary_key_query:
                    primary_key_query = primary_key_query.replace(
                        "<table_name>", table_name.strip()
                    ).replace("<schema_name>", schema_name)
                    primary_keys, _ = execute_native_query(
                            config, primary_key_query, connection_type, is_list=True
                    )

                primary_key_columns = []
                for primary_key in primary_keys:
                    primary_key_columns.append(primary_key.get("column_name"))

                for column in schema:
                    column_name = column.get("column_name")
                    column.update({"is_primary_key": (column_name in primary_key_columns)})
            return schema
        if connection_type == ConnectionType.Oracle.value:
            schema_condition = (
                f"AND UPPER(T.OWNER) = '{schema_name}'" if schema_name else ""
            )
            query_string = (
                query_string.replace("<table_name>", table_name.strip())
                .replace("<schema_condition>", schema_condition)
                .replace("<database_name>", db_name)
            )
            schema, _ = execute_native_query(
                            config, query_string, connection_type, is_list=True
                    )

            primary_keys = []
            if primary_key_query:
                primary_key_query = (
                    primary_key_query.replace("<table_name>", table_name.strip())
                    .replace("<schema_condition>", schema_condition)
                    .replace("<db_name>", db_name)
                )

                primary_keys, _ = execute_native_query(
                            config, primary_key_query, connection_type, is_list=True
                    )

            primary_key_columns = []
            for primary_key in primary_keys:
                primary_key_columns.append(primary_key.get("column_name"))
            primary_key_columns = (
                list(set(primary_key_columns)) if primary_key_columns else []
            )

            for column in schema:
                column_name = column.get("column_name")
                column.update({"is_primary_key": (column_name in primary_key_columns)})
            return schema

def get_merge_grouping_measure_queries(
    queries: dict,
) -> dict:
    """
    Returns the native queries for the given type.
    """
    default_queries = {} 
    queries = deepcopy(queries)
    basic_queries = queries.get("health", {})
    advanced_queries = queries.get("advanced", {})
   
    # INSERT_YOUR_CODE
    def flatten_queries(queries):
        result = {}
        for parent_key, subdict in queries.items():
            if isinstance(subdict, dict):
                for k, v in subdict.items():
                    if k in result:
                        result[f"{parent_key}_{k}"] = v
                    else:
                        result[k] = v
        return result

    default_queries = flatten_queries(basic_queries)
    default_queries.update(flatten_queries(advanced_queries))
    return default_queries

def prepare_grouping_measure_query(query: str, measure_name: str, connection_type: str, attribute_name: str) -> str:
    """
    Returns the native queries for the given type.
    """
    attribute_label = get_attribute_label(attribute_name)
    query = prepare_query_string(query, attribute=attribute_name)
    alias_name = f"{attribute_label}{measure_name}"
    if connection_type == ConnectionType.Oracle.value:
        query = f'{query} {alias_name}'
    else:
        query = f"""{query} AS {alias_name}"""
    return query


def prepare_custom_grouping_measure_query(config: dict, measures: list, default_queries: dict, group_by_column: str, asset_query: str):
    """
    Returns the grouping measure query string for the given measures
    """
    join_clause = []
    select_clause = [f"asset_query.{group_by_column}", "count"]
    queries = [asset_query]
    asset = config.get("asset", "")
    asset_query_string = asset.get("query", "")
    for measure in measures:
        query = measure.get("query")
        measure_name = measure.get("name")
        if measure.get('category') == CONDITIONAL:
            attribute = {
                "attribute_name": measure.get("attribute_name"), 
                "attribute_id": measure.get("attribute_id")
            }
            query = prepare_query(measure, config, default_queries, attribute)
        group_by_query = convert_query_to_group_by(query, group_by_column, [f"count(*) AS {measure_name}"])
        group_by_query = group_by_query.replace(";", "")
        queries.append(f"{measure_name} AS (\n        {group_by_query}\n)")
        select_clause.append(measure_name)
        join_clause.append(f"""left join {measure_name} on ((asset_query.{group_by_column} = {measure_name}.{group_by_column}) or (asset_query.{group_by_column} is null and {measure_name}.{group_by_column} is null))""")
    
    # Prepare Final Query String
    queries = ",\n".join(queries)
    join_query = "\n  ".join(join_clause)
    select_columns = ",".join(select_clause)
    query_string = f"""
        {queries}
        select {select_columns}
        from asset_query
        {join_query}
    """
    if query_string and DIRECT_QUERY_BASE_TABLE_LABEL in query_string:
        query_string = query_string.replace("<base_table>", f"({asset_query_string})")
    return query_string

def is_description_manually_updated(config: dict, attribute_id = None) -> bool:
    """
    Checks if the description of the attribute is manually updated
    """
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        # If attribute_id is provided, check attribute properties
        if attribute_id:
            query_string = f"""
                select updated_properties ->> 'is_description_manually_updated' as is_description_manually_updated 
                from core.attribute where id = '{attribute_id}'
            """        
        # Check asset properties
        else:
            query_string = f"""
                select properties ->> 'is_description_manually_updated' as is_description_manually_updated 
                from core.asset where id = '{config.get("asset_id")}'
            """
        cursor = execute_query(connection, cursor, query_string)    
        result = fetchone(cursor)
        return result.get("is_description_manually_updated", False) if result else False
    return False
    
        