import json
import pandas as pd
import uuid
from uuid import uuid4
from datetime import datetime
from sql_metadata import Parser
from copy import deepcopy
from dqlabs.app_helper.dq_helper import (
    get_derived_type,
    convert_to_lower,
    get_derived_type_category,
    calculate_weightage_score,
    check_measure_result,
    check_is_direct_query,
    parse_numeric_value,
    parse_teradata_columns,
    extract_table_name,
    get_default_datatype,
    get_column_alias_with_function
)
from dqlabs.app_helper.connection_helper import (
    get_primary_key,
    get_databricks_datasize,
    get_synapse_datasize,
    get_databricks_freshness,
)
from dqlabs.app_helper.db_helper import execute_query, fetchall, split_queries, fetchone
from dqlabs.utils.extract_workflow import (
    get_measures,
    get_metadata_query,
    get_queries,
    is_deprecated,
    deprecate_asset,
    deprecate_attributes,
    change_version,
    create_version_history,
    update_asset_run_id,
    update_recent_run_alert,
    remove_depricated_primary_keys,
    get_table_schema
)
from dqlabs.app_helper.lineage_helper import save_snowflake_lineage, save_databricks_lineage
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    prepare_query_string,
    prepare_nosql_pipeline,
    get_native_connection,
    get_query_string,
    handle_execution_before_task_run,
    delete_metrics,
    get_technical_name
)
from dqlabs.utils.patterns import (
    check_is_positive,
    get_default_patterns,
    get_existing_patterns,
)
from dqlabs.utils.semantic_tags import (
    get_tag_duplicates,
    get_tag_column_list,
    get_selected_attributes,
    get_selected_asset,
    get_parent_id,
    get_source_tags,
    get_dqlabs_tags,
    get_col_tags,
    get_tag_name,
    get_attribute_name,
)
from dqlabs.tasks.observe import extract_metadata
from dqlabs.utils.observe import (
    get_selected_assets,
)
from dqlabs.app_helper.json_attribute_helper import prepare_json_attribute_flatten_query

from dqlabs.app_constants.dq_constants import (
    ASSET,
    FRESHNESS,
    DUPLICATES,
    RELIABILITY,
    PASSED,
    DEPRECATED_ASSET_ERROR,
    WATERMARK_COLUMN_MISSING_ERROR,
    VARIANT,
    JSON,
    ARRAY,
    OBJECT,
)
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.tasks.check_alerts import check_alerts
from dqlabs.tasks.update_threshold import update_threshold
from dqlabs.utils.query_mode import create_view
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.tasks.scoring import update_scores
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.profile import get_temp_table_query
from dqlabs.utils import extract_workflow, is_scoring_enabled
from dqlabs.utils.extract_workflow import is_description_manually_updated
from dqlabs.app_helper import agent_helper
from dqlabs.utils.observe import get_default_measures
from dqlabs.enums.approval_status import ApprovalStatus
from dqlabs.utils.event_capture import save_sync_event

def update_primary_columns(
    config: dict, attributes: dict, primary_columns: list
) -> None:
    """
    Updates the primary key columns
    """
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    primary_columns = primary_columns if primary_columns else []
    existing_primary_columns = asset.get("primary_columns", [])
    new_primary_columns = []
    existing_primary_columns = (
        existing_primary_columns if existing_primary_columns else []
    )
    existing_primary_column = list(
        set(map(lambda column: column.get("name"), existing_primary_columns))
    )
    connection = get_postgres_connection(config)
    primary_column = (
        list(set(map(lambda column: column.get("name"), primary_columns)))
        if primary_columns
        else []
    )
    if not primary_column:
        return

    with connection.cursor() as cursor:
        for column in primary_column:
            if column in existing_primary_column:
                continue

            attribute = attributes.get(column)
            if not attribute:
                continue
            attribute_id = attribute.get("id")
            derived_type = attribute.get("derived_type")
            new_primary_columns.append(
                {
                    "id": attribute_id,
                    "name": column,
                    "derived_type": derived_type,
                }
            )
        if new_primary_columns:
            primary_key_column = json.dumps(new_primary_columns, default=str)
            query_string = f"""
                    update core.data set primary_columns='{primary_key_column}'
                    where asset_id='{asset_id}'
                """
            cursor = execute_query(connection, cursor, query_string)

            attribute_ids = [
                f"'{columns.get('id')}'" for columns in new_primary_columns
            ]
            attribute_query_string = f"""
                    update core.attribute set is_primary_key = true
                    where id in ({', '.join(attribute_ids)})    
            """

            cursor = execute_query(connection, cursor, attribute_query_string)


def save_datatype_patterns(config: dict, attributes: dict) -> None:
    """
    Stores the patterns into both postgres
    """
    organization_id = config.get("organization_id")
    asset_id = config.get("asset_id")
    existing_patterns = get_existing_patterns(config)
    default_patterns = get_default_patterns(config)
    connection = get_postgres_connection(config)

    pattern_measures = []
    with connection.cursor() as cursor:
        for _, attribute in attributes.items():
            attribute_id = attribute.get("id")

            is_deleted = attribute.get("is_deleted")
            if is_deleted:
                continue

            has_patterns = existing_patterns.get(attribute_id, [])
            if has_patterns:
                continue
            derived_type = attribute.get("derived_type")
            derived_types = [derived_type, "all"]
            patterns = list(
                filter(
                    lambda pattern: any(
                        derived_type in pattern.get("derived_type", [])
                        for derived_type in derived_types
                    ),
                    default_patterns,
                )
            )
            attribute_patterns = existing_patterns.get(attribute_id)
            attribute_patterns = list(attribute_patterns) if attribute_patterns else []

            for pattern in patterns:
                is_positive = check_is_positive(pattern, attribute)
                pattern_id = pattern.get("id")
                if attribute_patterns and pattern_id in attribute_patterns:
                    continue
                query_input = (
                    str(uuid4()),
                    pattern.get("technical_name"),
                    pattern.get("is_drift_enabled", False),
                    is_positive,
                    pattern.get("allow_score", False),
                    pattern.get("is_active", False),
                    False,
                    True,
                    asset_id,
                    attribute_id,
                    pattern_id,
                    organization_id,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                pattern_measures.append(query_param)

        # create each pattern as a separate measure
        patterns_input = split_queries(pattern_measures)
        for input_values in patterns_input:
            try:
                query_input = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.measure (id, technical_name,
                    is_drift_enabled, is_positive, allow_score,
                    is_active, is_delete, is_auto, asset_id, attribute_id,
                    base_measure_id, organization_id, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("extract metadata: inserting attribute level patterns", e)


def save_asset_metadata(config: dict, metadata: dict):
    """
    Stores the asset level metadata into postgres
    """
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    existing_description = config.get("asset", {}).get("description")
    run_id = config.get("run_id")
    airflow_run_id = config.get("airflow_run_id")
    asset_measures = get_measures(config, RELIABILITY, ASSET)
    connection = get_postgres_connection(config)
    connection_type = config.get("connection_type")
    source_connection = ""
    table_name = config.get("table_name")
    measure_level_queries = config.get("measure_level_queries", {})
    measure_level_queries = measure_level_queries if measure_level_queries else {}

    with connection.cursor() as cursor:
        measure_input_values = []
        metadata_query = []
        asset_metadata = []
        # Prepare asset level measures for insertion
        for measure in asset_measures:
            measure_name = measure.get("name")
            measure_id = measure.get("id")
            is_positive = measure.get("is_positive", False)
            allow_score = is_scoring_enabled(config, measure.get("allow_score", False))
            is_drift_enabled = measure.get("is_drift_enabled", False)
            executed_query = ""

            # Delete metrics for the same run id
            delete_metrics(
                config,
                run_id=run_id,
                measure_id=measure_id,
                level="asset",
            )

            total_count = 0
            level = measure.get("level")
            value = metadata.get(measure_name, 0)
            executed_query = measure_level_queries.get("metadata", "")
            if measure_name in measure_level_queries:
                executed_measure_query = measure_level_queries.get(measure_name, "")
                executed_query = (
                    executed_measure_query if executed_measure_query else executed_query
                )
            executed_query = executed_query if executed_query else ""
            if (
                config.get("has_temp_table", False)
                and config.get("table_name", "")
                and config.get("temp_view_table_name", "")
            ):
                executed_query = executed_query.replace(
                    f"""{config.get("temp_view_table_name", "").strip()}""",
                    f"""{config.get("table_name", "").strip()}""",
                )
            # executed_query = executed_query.strip().replace("'", "''") # this will replace filter values in double quotes which doesnt work
            value = value if value else 0
            if measure_name == "freshness":
                value = parse_numeric_value(value)
                value = f"{str(value)}"
            metadata_field = f""" {measure_name} = {value} """
            if metadata_field not in metadata_query:
                metadata_query.append(metadata_field)

            if measure_name in ["row_count", "duplicate_count"]:
                total_count = metadata.get("row_count")
                total_count = total_count if total_count else 0

            weightage = measure.get("weightage", 100)
            weightage = int(weightage) if weightage else 100
            valid_count = 0
            invalid_count = 0
            valid_percentage = 0
            invalid_percentage = 0
            score = None
            is_archived = False

            if total_count and allow_score:
                valid_count = value
                valid_count = (
                    valid_count if is_positive else (total_count - valid_count)
                )
                invalid_count = total_count - valid_count
                valid_percentage = float(valid_count / total_count * 100)
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
                None,
                measure_id,
                run_id,
                airflow_run_id,
                "",
                measure_name,
                level,
                str(value),
                weightage,
                total_count,
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
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals}, CURRENT_TIMESTAMP)",
                query_input,
            ).decode("utf-8")
            measure_input_values.append(query_param)

            pass_criteria_result = check_measure_result(measure, score)
            score = "null" if score is None else score
            update_measure_score_query = f"""
                update core.measure set last_run_id='{run_id}', last_run_date=CURRENT_TIMESTAMP, 
                  score={score}, failed_rows=null, row_count = {total_count}, valid_rows = {valid_count},
                  invalid_rows = {invalid_count}, result = '{pass_criteria_result}'
                where id='{measure_id}'
            """
            cursor = execute_query(connection, cursor, update_measure_score_query)

        measures_input = split_queries(measure_input_values)
        for input_values in measures_input:
            try:
                query_input = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.metrics (id, organization_id, connection_id, asset_id, attribute_id,
                    measure_id, run_id, airflow_run_id, attribute_name, measure_name, level, value, weightage, total_count,
                    valid_count, invalid_count, valid_percentage, invalid_percentage, score, status, is_archived,
                    query, allow_score, is_drift_enabled, is_measure,  is_active,is_delete, created_date)
                    values {query_input}
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
            except Exception as e:
                log_error("extract metadata : inserting new metrics", e)

        data_size = metadata.get("table_size", "")
        table_description = ""
        if (
            connection_type == ConnectionType.Databricks.value
            and config.get("type") == "TABLE"
            and config.get("database_name") != "hive_metastore"
        ):
            grant_query = f""" SHOW GRANT ON TABLE {table_name} ;"""
            grant_response, _ = execute_native_query(
                config, grant_query, source_connection, is_list=True
            )
            grant_response = convert_to_lower(grant_response)
            get_privileges = (
                grant_response[0].get("actiontype") if grant_response else ""
            )
            if ("all privileges" in get_privileges.lower()) or (
                "modify" in get_privileges.lower()
            ):
                analyze_query = f""" ANALYZE TABLE {table_name} COMPUTE STATISTICS;"""
                try:
                    execute_native_query(
                        config, analyze_query, source_connection, no_response=True
                    )
                except Exception as e:
                    # Handle the permission denied
                    print(f"An error occurred: {e}")
                describe_query = f"""DESCRIBE EXTENDED {table_name}"""
                try:
                    response, _ = execute_native_query(
                        config, describe_query, source_connection, is_list=True
                    )
                except Exception as e:
                    # Handle the permission denied
                    print(f"An error occurred: {e}")
                    response = None
                data_size = get_databricks_datasize(response) if response else 0
                if response:
                    table_description = next((d for d in response if d.get("col_name") == "Comment"), None)
                    table_description = table_description.get("data_type") if table_description else existing_description
        if connection_type == ConnectionType.MySql.value:
            table_description = metadata.get("description","") if metadata else ""
            table_description = table_description if table_description else existing_description
        # elif connection_type == ConnectionType.Synapse.value and "TABLE" in config.get(
        #     "type"
        # ):
        #     table_name = table_name.replace("[", "").replace("]", "")
        #     analyze_query = f""" EXEC sp_spaceused '{table_name}';"""
        #     response, _ = execute_native_query(
        #         config, analyze_query, source_connection, is_list=True
        #     )
        #     data_size = get_synapse_datasize(response)
        properties = config.get("asset", {})
        asset_properties = config.get("asset", {}).get("properties", {})
        last_run_id = properties.get("last_run_id")
        if last_run_id:
            # Get existing old_properties or create new one
            old_properties = asset_properties.get("old_properties", {})
            if not isinstance(old_properties, dict):
                old_properties = {}
            # Update properties in old_properties only if they don't exist
            if "issues" not in old_properties:
                old_properties["issues"] = properties.get("issues", 0)
            if "row_count" not in old_properties:
                old_properties["row_count"] = asset_properties.get("row_count", 0)
            if "column_count" not in old_properties:
                old_properties["column_count"] = asset_properties.get("column_count", 0)
            if "score" not in old_properties:
                old_properties["score"] = properties.get("score", None)
            if "alerts" not in old_properties:
                old_properties["alerts"] = properties.get("alerts", 0)

            asset_properties["old_properties"] = old_properties
            asset_properties = json.dumps(asset_properties, default=str)
            asset_properties = asset_properties.replace("'", "''")
            query_string = f"""
                    update core.asset set properties = '{asset_properties}'
                    where id='{asset_id}'
                """
            cursor = execute_query(connection, cursor, query_string)
        data_size = data_size if data_size else ""
        if data_size:
            asset_metadata.append(f""" data_size = {data_size} """)

        total_query_count = metadata.get("total_query_count", 0)
        total_query_count = total_query_count if total_query_count else 0
        if total_query_count:
            asset_metadata.append(f""" queries = {total_query_count} """)
        op = {}
        if metadata_query : 
            for item in metadata_query:
                key, value = map(str.strip, item.split('='))
                op[key] = value
            properties = op
            asset_properties = config.get("asset", {}).get("properties", {})
            asset_properties.update({
                                        "row_count": properties.get("row_count", 0),
                                        "column_count": properties.get("column_count", 0),
                                    })
            asset_properties = json.dumps(asset_properties, default=str)
            asset_properties = asset_properties.replace("'", "''")
        if table_description and not is_description_manually_updated(config):
            table_description = table_description.replace("'", "''")
            query_string = f"""
                    update core.asset set properties = '{asset_properties}', description = '{table_description}'
                    where id='{asset_id}'
                """
        else:
            query_string = f"""
                    update core.asset set properties = '{asset_properties}'
                    where id='{asset_id}'
                """
        cursor = execute_query(connection, cursor, query_string)
        if asset_metadata:
            asset_metadata = list(set(asset_metadata)) if asset_metadata else []
            query = ", ".join(asset_metadata)
            query_string = f"""
                update core.asset set {query}
                where id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

        if metadata:
            keys_to_update = ['row_count', 'column_count', 'freshness', 'duplicate_count']
            update_parts = []
            for key in keys_to_update:
                if key in metadata:
                    value = metadata[key]
                    if value is None:
                        update_parts.append(f"{key}=NULL")
                    else:
                        update_parts.append(f"{key}={value}")
            if update_parts:
                query = ", ".join(update_parts)
                query_string = f"""
                    update core.data set {query}
                    where asset_id='{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)


def save_attribute_metadata(
    config: dict, asset_metadata: dict, metadata: list, attributes: dict, is_direct_query_asset: bool
) -> None:
    """
    Stores the metadata into postgres
    """
    datatypes = config.get("datatypes")
    total_records = asset_metadata.get("row_count", 0)
    total_records = total_records if total_records else 0
    
    default_queries = get_queries(config)
    
    # Get the total_rows query once if we have default_queries
    row_count_query = None
    if default_queries:
        row_count_query = default_queries.get("metadata", {}).get("total_rows")
    
    # Check if we need to fetch constraint info for Snowflake
    connection_type = config.get("connection_type", "").lower()
    # Always refresh constraint info for Snowflake to keep it in sync with database changes
    needs_constraint_info = (
        connection_type == "snowflake" and 
        metadata
    )
    
    # Fetch constraint information for Snowflake if needed
    primary_key_columns = set()
    unique_key_columns = set()
    
    if needs_constraint_info and not is_direct_query_asset:
        try:
            database_name = config.get("database_name", "")
            schema_name = config.get("schema", "")
            table_name = config.get("table_technical_name", "")
            
            if database_name and schema_name and table_name:
                # Get primary keys
                pk_query = f'SHOW PRIMARY KEYS IN TABLE "{database_name}"."{schema_name}"."{table_name}"'
                pk_result, _ = execute_native_query(config, pk_query, None, is_list=True)
                if pk_result:
                    primary_key_columns = {row.get("column_name", "").upper() for row in pk_result if row.get("column_name")}
                
                # Get unique keys  
                uk_query = f'SHOW UNIQUE KEYS IN TABLE "{database_name}"."{schema_name}"."{table_name}"'
                uk_result, _ = execute_native_query(config, uk_query, None, is_list=True)
                if uk_result:
                    unique_key_columns = {row.get("column_name", "").upper() for row in uk_result if row.get("column_name")}
        except Exception as e:
            pass

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        attribute_metadata_input = []
        for attribute_metadata in metadata:
            attribute_name = attribute_metadata.get("name")
            if not attribute_name:
                continue
            existing_attribute = attributes.get(attribute_name, {})
            existing_attribute = existing_attribute if existing_attribute else {}
            attribute_id = existing_attribute.get("id")
            if not attribute_id:
                continue

            is_deleted = existing_attribute.get("is_deleted")
            if is_deleted:
                continue

            datatype = attribute_metadata.get("datatype")
            derived_type = ""
            if datatype:
                derived_type = get_derived_type(datatypes, datatype)
            if connection_type not in [ConnectionType.Databricks.value, ConnectionType.MySql.value]:
                query_string = f"""
                    select last_run_id from core.attribute
                    where id='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                exisitng_attribute_data = fetchone(cursor)
                existing_id = (
                    exisitng_attribute_data.get("last_run_id")
                    if exisitng_attribute_data
                    else ""
                )
                # Check if we should skip due to existing last_run_id
                should_skip = existing_id and not needs_constraint_info
                if should_skip:
                    continue

            is_null = attribute_metadata.get("is_null")
            is_blank = attribute_metadata.get("is_blank")
            is_unique = attribute_metadata.get("is_unique")
            is_primary_key = attribute_metadata.get("is_primary_key")
            is_foreign_key = attribute_metadata.get("is_foreign_key")
            description = attribute_metadata.get("description")
            description = description if description else existing_attribute.get("description")
            if is_description_manually_updated(config, attribute_id):
                description = existing_attribute.get("description")
            
            # Always refresh constraint info from database for Snowflake to keep it in sync
            if needs_constraint_info and attribute_name:
                column_name_upper = attribute_name.upper()
                # Get current constraint state from database
                current_is_primary_key = column_name_upper in primary_key_columns
                current_is_unique = column_name_upper in unique_key_columns
                
                # Check if constraints have changed
                old_is_primary_key = attribute_metadata.get('is_primary_key')
                old_is_unique = attribute_metadata.get('is_unique')
                
                # Always update constraint info from current database state
                is_primary_key = current_is_primary_key
                is_unique = current_is_unique
                # Update the attribute_metadata dictionary to include the refreshed constraint info
                attribute_metadata['is_primary_key'] = is_primary_key
                attribute_metadata['is_unique'] = is_unique
            
            is_null = True if is_null else False
            is_blank = True if is_blank else False
            is_unique = True if is_unique else False
            is_primary_key = True if is_primary_key else False
            is_foreign_key = True if is_foreign_key else False
            attributes.get(attribute_name, {}).update(
                {
                    "datatype": datatype,
                    "derived_type": derived_type,
                    "is_null": is_null,
                    "is_blank": is_blank,
                }
            )
            modified_query = prepare_json_attribute_flatten_query(
                config,
                attribute_name,
                row_count_query,
                "sub_query"
            )

            if row_count_query != modified_query:
                modified_query = get_query_string(
                    config,
                    default_queries,
                    modified_query,
                    is_full_query=True,
                )
                source_connection = config.get("source_connection")
                row_count_data, _ = execute_native_query(
                    config, modified_query, source_connection
                )
                if row_count_data:
                    row_count_data = convert_to_lower(row_count_data)
                    updated_row_count = row_count_data.get("total_rows") if row_count_data else 0
                    if updated_row_count:
                        total_records = updated_row_count

            query_input = (
                attribute_id,
                description,
                is_null,
                is_blank,
                is_unique,
                is_foreign_key,
                is_primary_key,
                derived_type,
                datatype,
                total_records,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            attribute_metadata_input.append(query_param)

        attributes_metadata_input = split_queries(attribute_metadata_input)
        for input_values in attributes_metadata_input:
            try:
                query_input = ",".join(input_values)
                query_string = f"""
                    update core.attribute
                    set
                        description=update_payload.description,
                        is_null=update_payload.is_null,
                        is_blank=update_payload.is_blank,
                        is_unique=update_payload.is_unique,
                        is_forgien_key=update_payload.is_forgien_key,
                        is_primary_key=update_payload.is_primary_key,
                        derived_type=update_payload.derived_type,
                        datatype=update_payload.datatype,
                        row_count=update_payload.row_count,
                        modified_date=update_payload.modified_date
                    from (
    					 values {query_input}
    				) as update_payload(attribute_id, description,is_null, is_blank, is_unique, is_forgien_key, is_primary_key, derived_type,datatype, row_count, modified_date)
    				where
                        attribute.id::text = update_payload.attribute_id::text
                """
                cursor = execute_query(connection, cursor, query_string)
            except Exception as e:
                log_error("save attribute: inserting new attributes", e)

        # Enable/disable constraint-based measures after attribute updates
        manage_constraint_measures(config, connection, cursor)


def detect_constraint_changes(config: dict, connection, cursor):
    """
    Detect constraint changes by comparing current state with stored constraint history
    Returns dictionary of constraint changes per attribute
    """
    try:
        asset_id = config.get("asset_id")
        if not asset_id:
            log_error("No asset_id provided for constraint change detection")
            return {}
        
        # Validate asset_id is a valid UUID format
        try:
            from uuid import UUID
            UUID(asset_id)
        except (ValueError, TypeError):
            log_error(f"Invalid asset_id format: {asset_id}")
            return {}
            
        # Get current asset properties
        query_string = """
            SELECT properties FROM core.asset 
            WHERE id = %s
        """
        cursor.execute(query_string, (asset_id,))
        result = cursor.fetchone()
        
        if not result:
            return {}
            
        asset_properties = result[0] if result[0] else {}
        constraint_history = asset_properties.get("constraint_history", {})
        
        # Get current constraint state for all attributes
        current_constraints = {}
        query_string = """
            SELECT id, name, is_unique, is_null, is_primary_key 
            FROM core.attribute 
            WHERE asset_id = %s AND is_delete = false
        """
        cursor.execute(query_string, (asset_id,))
        attributes = cursor.fetchall()
        
        for attr in attributes:
            attr_id, attr_name, is_unique, is_null, is_primary_key = attr
            current_constraints[attr_name] = {
                "id": attr_id,
                "is_unique": is_unique or False,
                "is_null": is_null if is_null is not None else True,
                "is_primary_key": is_primary_key or False
            }
        
        # Compare with stored constraint history to detect changes
        constraint_changes = {}
        
        for attr_name, current_state in current_constraints.items():
            previous_state = constraint_history.get(attr_name, {}).get("previous_state", {})
            
            # Detect changes
            changes = {}
            
            # Check UNIQUE constraint changes
            prev_unique = previous_state.get("is_unique", False)
            curr_unique = current_state["is_unique"]
            if prev_unique != curr_unique:
                changes["unique_changed"] = {
                    "from": prev_unique,
                    "to": curr_unique,
                    "action": "added" if curr_unique else "removed"
                }
            
            # Check NOT NULL constraint changes  
            prev_null = previous_state.get("is_null", True)
            curr_null = current_state["is_null"]
            if prev_null != curr_null:
                changes["null_changed"] = {
                    "from": prev_null,
                    "to": curr_null,
                    "action": "removed" if not curr_null else "added"  # NOT NULL removed = is_null false
                }
            
            # Check PRIMARY KEY constraint changes
            prev_pk = previous_state.get("is_primary_key", False)
            curr_pk = current_state["is_primary_key"]
            if prev_pk != curr_pk:
                changes["primary_key_changed"] = {
                    "from": prev_pk,
                    "to": curr_pk,
                    "action": "added" if curr_pk else "removed"
                }
            
            if changes:
                constraint_changes[attr_name] = {
                    "attribute_id": current_state["id"],
                    "changes": changes,
                    "current_state": current_state,
                    "previous_state": previous_state
                }
        
        log_info(f"Detected constraint changes for {len(constraint_changes)} attributes")
        return constraint_changes
        
    except Exception as e:
        log_error(f"Error detecting constraint changes: {str(e)}")
        return {}


def update_constraint_history(config: dict, connection, cursor, current_constraints: dict):
    """
    Update the constraint history in asset properties
    """
    try:
        asset_id = config.get("asset_id")
        if not asset_id:
            log_error("No asset_id provided for constraint history update")
            return
        
        # Validate asset_id is a valid UUID format
        try:
            from uuid import UUID
            UUID(asset_id)
        except (ValueError, TypeError):
            log_error(f"Invalid asset_id format: {asset_id}")
            return
        
        if not current_constraints:
            log_info("No current constraints provided for history update")
            return
            
        # Get current asset properties
        query_string = """
            SELECT properties FROM core.asset 
            WHERE id = %s
        """
        cursor.execute(query_string, (asset_id,))
        result = cursor.fetchone()
        asset_properties = result[0] if result and result[0] else {}
        # Update constraint history
        constraint_history = {}
        current_timestamp = datetime.now().isoformat()
        
        for attr_name, constraint_state in current_constraints.items():
            constraint_history[attr_name] = {
                "previous_state": {
                    "is_unique": constraint_state["is_unique"],
                    "is_null": constraint_state["is_null"], 
                    "is_primary_key": constraint_state["is_primary_key"]
                },
                "last_updated": current_timestamp
            }
        
        asset_properties["constraint_history"] = constraint_history
        
        # Update asset properties using parameterized query
        properties_json = json.dumps(asset_properties, default=str)
        properties_json = properties_json.replace("'", "''")
        query_string = """
            UPDATE core.asset 
            SET properties = %s
            WHERE id = %s
        """
        cursor.execute(query_string, (properties_json, asset_id))
        
        log_info(f"Updated constraint history for {len(constraint_history)} attributes")
        
    except Exception as e:
        log_error(f"Error updating constraint history: {str(e)}")


def manage_constraint_measures(config: dict, connection, cursor):
    """
    Manage constraint-based measures for Snowflake connections with smart override logic:
    - Detect constraint changes using stored constraint history
    - Override manual settings only when constraints are added or removed
    - Respect user manual configurations when no constraint changes occur
    - Maintain constraint history for future comparisons
    """
    try:
        connection_type = config.get("connection_type", "").lower()
        if connection_type != "snowflake":
            return
            
        asset_id = config.get("asset_id")
        if not asset_id:
            return
        
        # Step 1: Detect constraint changes
        constraint_changes = detect_constraint_changes(config, connection, cursor)
        
        # Step 2: Get current constraint state for history update
        current_constraints = {}
        query_string = """
            SELECT id, name, is_unique, is_null, is_primary_key 
            FROM core.attribute 
            WHERE asset_id = %s AND is_delete = false
        """
        cursor.execute(query_string, (asset_id,))
        attributes = cursor.fetchall()
        
        for attr in attributes:
            attr_id, attr_name, is_unique, is_null, is_primary_key = attr
            current_constraints[attr_name] = {
                "id": attr_id,
                "is_unique": is_unique or False,
                "is_null": is_null if is_null is not None else True,
                "is_primary_key": is_primary_key or False
            }
        
        # Step 3: Apply smart override logic based on constraint changes
        measures_updated = 0
        
        for attr_name, change_info in constraint_changes.items():
            attribute_id = change_info["attribute_id"]
            changes = change_info["changes"]
            
            # Handle UNIQUE constraint changes
            if "unique_changed" in changes:
                unique_change = changes["unique_changed"]
                action = unique_change["action"]
                
                if action == "added":
                    # UNIQUE constraint added - Enable distinct_count measure (override manual settings)
                    query = """
                        UPDATE core.measure 
                        SET is_active = true, allow_score = true, modified_date = CURRENT_TIMESTAMP
                        WHERE technical_name = 'distinct_count' 
                          AND asset_id = %s
                          AND attribute_id = %s
                    """
                    cursor.execute(query, (asset_id, attribute_id))
                    measures_updated += cursor.rowcount
                    log_info(f"Enabled distinct_count measure for {attr_name} (UNIQUE constraint added)")
                    
                elif action == "removed":
                    # UNIQUE constraint removed - Disable distinct_count measure (override manual settings)
                    query = """
                        UPDATE core.measure 
                        SET is_active = false, allow_score = false, modified_date = CURRENT_TIMESTAMP
                        WHERE technical_name = 'distinct_count' 
                          AND asset_id = %s
                          AND attribute_id = %s
                    """
                    cursor.execute(query, (asset_id, attribute_id))
                    measures_updated += cursor.rowcount
                    log_info(f"Disabled distinct_count measure for {attr_name} (UNIQUE constraint removed)")
            
            # Handle NOT NULL constraint changes
            if "null_changed" in changes:
                null_change = changes["null_changed"]
                action = null_change["action"]
                
                if action == "removed":
                    # NOT NULL constraint added (is_null changed from true to false) - Enable null_count measure
                    query = """
                        UPDATE core.measure 
                        SET is_export = true, is_active = true, allow_score = true, modified_date = CURRENT_TIMESTAMP
                        WHERE technical_name = 'null_count' 
                          AND asset_id = %s
                          AND attribute_id = %s
                    """
                    cursor.execute(query, (asset_id, attribute_id))
                    measures_updated += cursor.rowcount
                    log_info(f"Enabled null_count measure for {attr_name} (NOT NULL constraint added)")
                    
                elif action == "added":
                    # NOT NULL constraint removed (is_null changed from false to true) - Disable null_count measure
                    query = """
                        UPDATE core.measure 
                        SET is_export = false, is_active = false, allow_score = false, modified_date = CURRENT_TIMESTAMP
                        WHERE technical_name = 'null_count' 
                          AND asset_id = %s
                          AND attribute_id = %s
                    """
                    cursor.execute(query, (asset_id, attribute_id))
                    measures_updated += cursor.rowcount
                    log_info(f"Disabled null_count measure for {attr_name} (NOT NULL constraint removed)")
        
        # Step 4: Handle first-time setup (no constraint history exists)
        if not constraint_changes:
            # Enable measures for existing constraints (respects manual settings)
            # This handles the case when constraint history doesn't exist yet
            
            # Enable distinct_count for attributes with UNIQUE constraints (only if not manually configured)
            distinct_count_query = """
                UPDATE core.measure 
                SET is_active = true, allow_score = true
                WHERE technical_name = 'distinct_count' 
                  AND asset_id = %s
                  AND attribute_id IN (
                    SELECT id FROM core.attribute 
                    WHERE is_unique = true 
                      AND asset_id = %s
                  )
                  AND (last_run_date IS NULL OR modified_date IS NULL OR modified_date <= created_date)
            """
            cursor.execute(distinct_count_query, (asset_id, asset_id))
            measures_updated += cursor.rowcount
            
            # Enable null_count for attributes with NOT NULL constraints (only if not manually configured)
            null_count_query = """
                UPDATE core.measure 
                SET is_export = true, is_active = true, allow_score = true
                WHERE technical_name = 'null_count' 
                  AND asset_id = %s
                  AND attribute_id IN (
                    SELECT id FROM core.attribute 
                    WHERE is_null = false 
                      AND asset_id = %s
                  )
                  AND (last_run_date IS NULL OR modified_date IS NULL OR modified_date <= created_date)
            """
            cursor.execute(null_count_query, (asset_id, asset_id))
            measures_updated += cursor.rowcount
        
        # Step 5: Update constraint history for future comparisons
        update_constraint_history(config, connection, cursor, current_constraints)
        
        log_info(f"Smart constraint management completed: {measures_updated} measures updated, {len(constraint_changes)} constraint changes detected")
        
    except Exception as e:
        log_error(f"Error managing constraint measures: {str(e)}")


def save_attributes(config: dict, metadata: list, is_direct_query_asset: bool) -> dict:
    """
    Writes the new attributes and its measures into postgresql
    """
    attributes: dict = {}
    deprecated_attributes: dict = {}
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.SapEcc.value:
        source_attributes = list(
            map(lambda attribute: attribute.get("column_name", "").strip(), metadata)
        )
    else:
        source_attributes = list(
            map(lambda attribute: attribute.get("name").strip(), metadata)
        )
    source_attributes = [attribute for attribute in source_attributes if attribute]
    selected_attributes = extract_workflow.get_selected_attributes(config)
    selected_attributes = selected_attributes if selected_attributes else []
    connection = get_postgres_connection(config)
    selected_flag = True if is_direct_query_asset else False
    new_attributes = []
    deleted_attributes = []

    attribute_list = []
    with connection.cursor() as cursor:
        # Get the attributes that are already configured for processing
        try:
            query_string = f"""
                select id, trim(technical_name) as name, is_selected, status, description from core.attribute
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            attribute_list = fetchall(cursor)
            attributes = {
                attribute.get("name").strip(): {
                    "id": attribute.get("id"),
                    "is_selected": attribute.get("is_selected"),
                    "status": attribute.get("status"),
                    "description": attribute.get("description"),
                }
                for attribute in attribute_list
                if attribute and str(attribute.get("status")).lower() != "deprecated"
            }
            deprecated_attributes = {
                attribute.get("name").strip(): {
                    "id": attribute.get("id"),
                    "is_selected": attribute.get("is_selected"),
                    "status": attribute.get("status"),
                    "description": attribute.get("description"),
                }
                for attribute in attribute_list
                if attribute and str(attribute.get("status")).lower() == "deprecated"
            }
        except Exception as e:
            log_error("extract metadata: fetching existing attributes", e)

        # Insert the new attributes if any - Bulk Insert
        new_attributes = [
            attribute for attribute in source_attributes if attribute not in attributes
        ]
        if new_attributes:
            case_changed_attributes = [ 
            (source_attr, matched_attr)
            for source_attr in source_attributes
            for matched_attr in attributes
            if source_attr.lower() == matched_attr.lower() and source_attr != matched_attr
             ]
            
            new_attributes = [
                attribute for attribute in source_attributes
                if attribute.lower() not in [a.lower() for a in attributes]
            ]
            duplicate_case_changes = []
            unique_case_changes = []
            
            for source_attr, matched_attr in case_changed_attributes:
                # Count how many times source_attr.lower() appears in source_attributes
                count = sum(1 for attr in source_attributes if attr.lower() == source_attr.lower())
                if count > 1:
                    duplicate_case_changes.append((source_attr, matched_attr))
                else:
                    unique_case_changes.append((source_attr, matched_attr))
            
            # Update case_changed_attributes to only include unique cases
            case_changed_attributes = unique_case_changes
            new_attributes.extend([source_attr for source_attr, _ in duplicate_case_changes])

            if case_changed_attributes:
                for source_attr, matched_attr in case_changed_attributes:
                    source_attr_technical = get_technical_name(source_attr, include_special_char=True)
                    attribute_insert_query = f"""
                            UPDATE core.attribute set name = '{source_attr}', technical_name = '{source_attr_technical}'
                            where asset_id='{asset_id}' and name = '{matched_attr}'
                        """
                    cursor = execute_query(connection, cursor, attribute_insert_query)
            
        deleted_attributes = [
            attribute
            for attribute in attributes
            if attribute.lower() not in (attr.lower() for attr in source_attributes)
        ]
        new_inserted_attribute = new_attributes if new_attributes else []
        deleted_attribute_ids = []
        for attribute in deleted_attributes:
            is_selected_attribute = next(
                (attr for attr in selected_attributes if attr.get("name") == attribute),
                None,
            )
            if not is_selected_attribute:
                continue
            attribute_metadata = attributes.get(attribute)
            attribute_id = attribute_metadata.get("id")
            if not attribute_id:
                continue
            deleted_attribute_ids.append(attribute_id)

        if deleted_attribute_ids:
            deprecate_attributes(config, deleted_attribute_ids)

        asset_attributes = list(attributes.keys())
        attributes_input_values = []
        deprecated_attribute_values = []

        for attribute in new_attributes:
            if attribute in asset_attributes:
                continue

            if attribute in deprecated_attributes:
                deprecated_attribute_values.append(attribute)
                continue

            query_input = (
                str(uuid4()),
                attribute,
                attribute,
                selected_flag,
                asset_id,
                connection_id,
                organization_id,
                True,
                True,
                False,
                False,
                "Completed",
                "Pending", # status,
                True,
            )
            input_literals = ", ".join(["%s"] * len(query_input))
            query_param = cursor.mogrify(
                f"({input_literals},CURRENT_TIMESTAMP)", query_input
            ).decode("utf-8")
            attributes_input_values.append(query_param)

        new_attribute_ids = []
        if deprecated_attribute_values:
            deprecated_attribute_value = [
                f"'{attribute}'"
                for attribute in deprecated_attribute_values
                if attribute
            ]
            deprecated_attribute_value = (
                ", ".join(deprecated_attribute_value)
                if deprecated_attribute_value
                else ""
            )
            deprecated_attribute_value = (
                f"({deprecated_attribute_value})" if deprecated_attribute_value else ""
            )
            if deprecated_attribute_value:
                attribute_insert_query = f"""
                    UPDATE core.attribute set status='Pending'
                    where asset_id='{asset_id}' and name in {deprecated_attribute_value}
                    RETURNING id, technical_name as name, is_selected, status
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
                updated_new_attributes = fetchall(cursor)
                update_new_attribute_list = []
                if updated_new_attributes:
                    attribute_list = {
                        attribute.get("name"): {
                            "id": attribute.get("id"),
                            "is_selected": attribute.get("is_selected"),
                            "status": attribute.get("status"),
                            "is_new": True,
                        }
                        for attribute in updated_new_attributes
                    }
                    update_new_attribute_list = [attribute.get("id") for attribute in updated_new_attributes]
                    attributes.update(attribute_list)
                    new_attribute_ids.extend(
                        [attribute.get("id") for attribute in updated_new_attributes]
                    )
                if update_new_attribute_list:
                    measure_attributes = f"""({','.join(f"'{i}'" for i in update_new_attribute_list)})"""
                    
                    # Update AUTO measures to Verified
                    auto_measure_query = f"""
                        with measures as (
                            select mes.id from core.measure as mes
                            join core.base_measure as base on base.id=mes.base_measure_id
                            where mes.asset_id='{asset_id}'
                            and mes.attribute_id in {measure_attributes}
                            and base.is_default=True and mes.status='Deprecated'
                        )
                        update core.measure set status='Verified'
                        where id in (select id from measures)
                    """
                    execute_query(connection, cursor, auto_measure_query)
                    
                    # Update CUSTOM measures to Pending
                    custom_measure_query = f"""
                        with measures as (
                            select mes.id from core.measure as mes
                            join core.base_measure as base on base.id=mes.base_measure_id
                            where mes.asset_id='{asset_id}'
                            and mes.attribute_id in {measure_attributes}
                            and base.is_default=False and mes.status='Deprecated'
                        )
                        update core.measure set status='Pending'
                        where id in (select id from measures)
                    """
                    execute_query(connection, cursor, custom_measure_query)

                    sync_data = {
                        "asset": asset_id,
                        "event": "sync",
                        "action": "restored",
                        "event_type": "sync",
                        "properties": {
                            "type": "attribute_sync",
                            "status": "restored",
                            "attribute_ids": update_new_attribute_list
                        },
                        "source": "airflow",
                    }
                    save_sync_event(config, sync_data)


        attributes_input = split_queries(attributes_input_values)
        for input_values in attributes_input:
            try:
                query_input = ",".join(input_values)
                attribute_insert_query = f"""
                    insert into core.attribute (id, name, technical_name, is_selected, asset_id, connection_id,
                    organization_id, is_active, is_semantic_enabled, is_primary_key, is_delete, run_status, status, profile, created_date)
                    values {query_input} returning id, technical_name as name, is_selected, status
                """
                cursor = execute_query(connection, cursor, attribute_insert_query)
                new_attributes = fetchall(cursor)
                if new_attributes:
                    attribute_list = {
                        attribute.get("name"): {
                            "id": attribute.get("id"),
                            "is_selected": attribute.get("is_selected"),
                            "status": attribute.get("status"),
                            "is_new": True,
                        }
                        for attribute in new_attributes
                    }
                    attributes.update(attribute_list)
                    new_attribute_ids.extend(
                        [attribute.get("id") for attribute in new_attributes]
                    )

                    # Update the parent attribute id for the json attributes
                    for attribute in new_attributes:
                        json_attribute = next(
                            (attr for attr in metadata if attr.get("name") == attribute.get('name')), None
                        )
                        parent_attribute_name = json_attribute.get("parent_attribute_name")
                        if parent_attribute_name:
                            parent_attribute = attributes.get(parent_attribute_name)
                            if parent_attribute:
                                datatypes = config.get("datatypes")
                                derived_type = get_derived_type(datatypes, json_attribute.get("datatype"))
                                attribute_insert_query = f"""
                                    UPDATE core.attribute set 
                                        parent_attribute_id='{parent_attribute.get("id")}', level='{json_attribute.get("level")}', 
                                        key_path='{json_attribute.get("key_path")}', datatype='{json_attribute.get("datatype")}',
                                        derived_type='{derived_type}', is_selected='{True}'
                                    where asset_id='{asset_id}' and id='{attribute.get("id")}'
                                """
                                execute_query(connection, cursor, attribute_insert_query)
                                attribute_for_measures = {
                                    'id': attribute.get("id"),
                                    'derived_type': derived_type
                                }
                                create_default_measures_for_attribute(config, attribute_for_measures)

            except Exception as e:
                log_error("extract metadata: inserting new attributes", e)

        if deleted_attribute_ids or new_attribute_ids:
            change_version(config)
            for att_id in deleted_attribute_ids:
                create_version_history(
                    config,
                    version_type="delete_attribute",
                    sub_type="delete_attribute",
                    value="",
                    attribute_id=att_id,
                )

            for att_id in new_attribute_ids:
                create_version_history(
                    config,
                    version_type="create_attribute",
                    sub_type="create_attribute",
                    value="",
                    attribute_id=att_id,
                )

    return attributes, new_inserted_attribute

def create_default_measures_for_attribute(config, attribute):
    
    asset_id = config.get("asset_id")
    connection_id = config.get("connection_id")
    organization_id = config.get("organization_id")
    default_measures = get_default_measures(config)
    attribute_level_measures = [
        m for m in default_measures 
        if m.get("level") == "attribute"
    ]
    # Prepare for database operations
    org_settings = config["dag_info"]["settings"]
    profile_settings = config["dag_info"]["settings"]["profile"]
    profile_settings = (
        json.loads(profile_settings)
        if isinstance(profile_settings, str)
        else profile_settings
    )
    profile_settings = profile_settings if profile_settings else {}
    check_numerics = profile_settings.get("check_numerics", False)
    connection = get_postgres_connection(config)
    input_measures = []
    try:
        with connection.cursor() as cursor:
            attribute_id = attribute["id"]
            derived_type = attribute.get("derived_type", "").lower()
            for measure in attribute_level_measures:
                # Check if measure applies to this attribute type
                measure_types = measure.get("derived_type", []) or []
                if not isinstance(measure_types, list):
                    measure_types = [measure_types]
                    
                measure_types = [t.lower() for t in measure_types]
                
                if "all" not in measure_types and derived_type not in measure_types:
                    continue
                
                # Handle numeric measures for text attributes
                technical_name = measure.get("technical_name", "")
                if (
                    not check_numerics
                    and technical_name in ["min_value", "max_value"]
                    and derived_type == "text"
                ):
                    is_active = False
                else:
                    is_active = bool(measure.get("is_active", True))
                
                # Process measure properties
                properties = measure.get("properties") or {}
                properties = (
                    json.loads(properties)
                    if properties and isinstance(properties, str)
                    else properties
                )
                # Handle range measures
                threshold_constraints = None
                if (measure.get("category") or "").lower() == "range":
                    threshold_constraints = properties.get("threshold_constraints")
                    if threshold_constraints:
                        threshold_constraints = json.dumps(threshold_constraints)
                
                # Create measure entry
                measure_id = str(uuid.uuid4())
                query_input = (
                    measure_id,
                    organization_id,
                    connection_id,
                    asset_id,
                    attribute_id,
                    technical_name,
                    measure.get("threshold"),
                    measure.get("is_positive"),
                    measure.get("is_drift_enabled"),
                    measure.get("allow_score"),
                    100,                                # weightage
                    bool(measure.get("allow_export")),
                    False,                              # enable_pass_criteria
                    100,                                # pass_criteria_threshold
                    ">=",                               # pass_criteria_condition
                    True,                               # is_validated
                    is_active,
                    measure.get("is_delete", False),
                    True,                               # is_auto
                    measure.get("is_aggregation_query", False),
                    measure.get("id"),
                    measure.get("dimension_id"),
                    threshold_constraints,
                    ApprovalStatus.Verified.value,
                )

                # Format for bulk insert
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)",
                    query_input,
                ).decode("utf-8")
                input_measures.append(query_param)

            # Insert in batches
            if input_measures:
                batches = split_queries(input_measures, 1000)
                for batch in batches:
                    if not batch:
                        continue
                    query = f"""
                        INSERT INTO core.measure (
                            id, organization_id, connection_id, asset_id, attribute_id,
                            technical_name, threshold, is_positive, is_drift_enabled, allow_score, 
                            weightage, is_export, enable_pass_criteria, pass_criteria_threshold, 
                            pass_criteria_condition, is_validated, is_active, is_delete,
                            is_auto, is_aggregation_query, base_measure_id, dimension_id, 
                            threshold_constraints, status, created_date
                        ) VALUES {','.join(batch)}
                    """
                    execute_query(connection, cursor, query)
                connection.commit()


    except Exception as e:
        log_error("create_default_measures_for_attribute: error getting connection", e)
    
def save_semantic_tags(config: dict, metadata: dict):
    """
    Stores the semantic tags from source into postgres
    """
    organization_id = config.get("organization_id")
    connection = get_postgres_connection(config)
    # asset = config.get("asset", {})
    asset_id = config.get("asset_id")
    tags_dataframe = pd.DataFrame.from_records(metadata)
    if "connection_type" in config.keys():
        connection_name = config.get("connection_type")
    connection_name = connection_name if connection_name else "dqlabs"
    with connection.cursor() as cursor:
        # Check if duplicate measures exist for the semantic tags
        duplicate_list = []
        """ Get the duplicate count for each """
        tags_duplicate_count = get_tag_duplicates(connection, cursor, metadata)
        tags_duplicate_count = tags_duplicate_count if tags_duplicate_count else {}
        if tags_duplicate_count:
            for key in tags_duplicate_count.keys():
                if tags_duplicate_count.get(key) >= 1:
                    duplicate_list.append(key)
            # Get the asset_id from both the asset and columns to generate parent id
            asset_name = get_selected_asset(connection, cursor, asset_id)
            _asset_tags = tags_dataframe[
                tags_dataframe["tag_name"] == asset_name
            ].to_dict("list")
            _columns_tags = tags_dataframe[
                tags_dataframe["tag_name"] != asset_name
            ].to_dict("list")
            asset_tags = _asset_tags.get("tag_name", 0)
            columns_tags = _columns_tags.get("tag_name", 0)
            if asset_tags:
                for idx, tag_name in enumerate(asset_tags):
                    if tag_name == asset_name:
                        if tag_name not in duplicate_list:
                            if "description" in _asset_tags.keys():
                                tag_description = _asset_tags.get("description", None)[
                                    idx
                                ]
                            else:
                                tag_description = None
                            color = "#64AAEF"
                            query_input = (
                                str(uuid4()),
                                tag_name,
                                tag_name,
                                tag_description,
                                color,
                                organization_id,
                                connection_name,
                            )
                            # Used to update 'attribute_metadata_tags' table
                            tags_id = query_input[0]
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals}, CURRENT_TIMESTAMP)",
                                query_input,
                            ).decode("utf-8")
                            measure_input_values = []
                            measure_input_values.append(query_param)
                            measures_input = split_queries(measure_input_values)
                            for input_values in measures_input:
                                try:
                                    query_input = ",".join(input_values)
                                    tags_insert_query = f"""
                                        insert into core.tags (id, name, technical_name, description,color,
                                        organization_id,db_name,created_date)
                                        values {query_input}
                                    """

                                    cursor = execute_query(
                                        connection, cursor, tags_insert_query
                                    )
                                except Exception as e:
                                    log_error(
                                        "semantics metadata : inserting new metrics", e
                                    )
            if columns_tags:
                for idx, tag_name in enumerate(columns_tags):
                    col_tags = get_col_tags(connection, cursor, config)
                    col_tags_list = col_tags.get("col_tag_name", [])
                    if tag_name not in col_tags_list:
                        if "description" in _columns_tags.keys():
                            tag_description = _columns_tags.get("description", None)[
                                idx
                            ]
                        else:
                            tag_description = None
                        color = "#64AAEF"
                        # display_name = '.'.join([asset_name, tag_name])
                        display_name = tag_name
                        parent_id = get_parent_id(connection, cursor, asset_name)
                        parent_id = parent_id if parent_id else None
                        query_input = (
                            str(uuid4()),
                            tag_name,
                            display_name,
                            tag_description,
                            color,
                            organization_id,
                            parent_id,
                            connection_name,
                        )
                        # Used to update 'attribute_metadata_tags' table
                        tags_id = query_input[0]
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)",
                            query_input,
                        ).decode("utf-8")
                        measure_input_values = []
                        measure_input_values.append(query_param)
                        measures_input = split_queries(measure_input_values)
                        for input_values in measures_input:
                            try:
                                query_input = ",".join(input_values)
                                tags_insert_query = f"""
                                    insert into core.tags (id, name, technical_name, description,color,
                                    organization_id,parent_id,db_name,created_date)
                                    values {query_input}
                                """
                                cursor_output = execute_query(
                                    connection, cursor, tags_insert_query
                                )
                            except Exception as e:
                                log_error(
                                    "semantics metadata : inserting new metrics", e
                                )
                        # Query to update display name as single tag could be used across multiple assets/attributes
                        query_string = f"""
                                        update core.tags set technical_name='{tag_name}'
                                        where name='{tag_name}'
                                        """
                        _cursor = execute_query(connection, cursor, query_string)

                        """ Query to update attributemetadata_id and tags_id"""

                        if "column_name" in _columns_tags.keys():
                            tag_column = _columns_tags.get("column_name")[idx]
                            if tag_column:
                                attribute_id = get_selected_attributes(
                                    connection, cursor, tag_column, asset_id
                                )
                                query_input_tags = (
                                    "attribute",
                                    attribute_id,
                                    tags_id,
                                )
                                tag_input_literals = ", ".join(
                                    ["%s"] * len(query_input_tags)
                                )
                                tag_query_param = cursor.mogrify(
                                    f"({tag_input_literals})",
                                    query_input_tags,
                                ).decode("utf-8")
                                tag_measure_input_values = []
                                tag_measure_input_values.append(tag_query_param)
                                tag_measures_input = split_queries(
                                    tag_measure_input_values
                                )
                                for tag_input_values in tag_measures_input:
                                    try:
                                        tag_query_input = ",".join(tag_input_values)
                                        tags_insert_query = f"""
                                            insert into core.tags_mapping(level,attribute_id,tags_id)
                                            values {tag_query_input} RETURNING id
                                        """
                                        cursor = execute_query(
                                            connection, cursor, tags_insert_query
                                        )
                                    except Exception as e:
                                        log_error(
                                            "attribute metadata : inserting new tags", e
                                        )
                    else:  # if tag in duplicate list check if column is duplicate
                        """Get only common tags that are not yet mapped to a column"""
                        column_name = _columns_tags.get("column_name")[idx]
                        column_dict = get_tag_column_list(connection, cursor, config)
                        # Get all the tag and column details from dqlabs
                        concat_list = column_dict.get("concat_tag_name", 0)
                        if concat_list:
                            col_name = ".".join([tag_name, column_name])
                            if col_name not in concat_list:
                                # table_id = random.randint(100, 999)
                                attribute_id = get_selected_attributes(
                                    connection, cursor, column_name, asset_id
                                )
                                tags_id = get_parent_id(connection, cursor, tag_name)
                                query_input_tags = (
                                    "attribute",
                                    attribute_id,
                                    tags_id,
                                )
                                tag_input_literals = ", ".join(
                                    ["%s"] * len(query_input_tags)
                                )
                                tag_query_param = cursor.mogrify(
                                    f"({tag_input_literals})",
                                    query_input_tags,
                                ).decode("utf-8")
                                tag_measure_input_values = []
                                tag_measure_input_values.append(tag_query_param)
                                tag_measures_input = split_queries(
                                    tag_measure_input_values
                                )
                                for tag_input_values in tag_measures_input:
                                    try:
                                        tag_query_input = ",".join(tag_input_values)
                                        tags_insert_query = f"""
                                            insert into core.tags_mapping(level,attribute_id,tags_id)
                                            values {tag_query_input} RETURNING id
                                        """
                                        cursor = execute_query(
                                            connection, cursor, tags_insert_query
                                        )
                                    except Exception as e:
                                        log_error(
                                            "attribute metadata : inserting new tags", e
                                        )


def get_delete_attributemetadata(config: dict, schema_tags_metadata: dict):
    """Get all the attributes that have been unmapped by user in dqlabs portal"""
    """ Delete mapped attributes and tags if deleted in native source"""
    snowflake_tag_keys, dqlabs_tag_keys, drop_tags = list(), list(), list()
    connection = get_postgres_connection(config)
    """ Get the database name from the credentials"""
    _database = config.get("connection").get("airflow_connection_object").get("extra")
    database_name = json.loads(_database).get("database")
    database_name = '"{}"'.format(database_name)
    df_snowflake_tags = pd.DataFrame.from_records(schema_tags_metadata)
    if not df_snowflake_tags.empty:
        df_snowflake_tags["TAG_KEY"] = df_snowflake_tags["TAG_KEY"].str.upper()
        snowflake_tag_keys = list(df_snowflake_tags["TAG_KEY"].values)
    with connection.cursor() as cursor:
        """Function to delete from backend if some tag is not present in source"""
        dqlabs_dict = get_dqlabs_tags(connection, cursor, config, database_name)
        df_dqlabs = pd.DataFrame.from_records(dqlabs_dict)
        if not df_dqlabs.empty:
            df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.replace('"', "")
            df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.upper()
            dqlabs_tag_keys = list(df_dqlabs["tag_key"].values)
        if snowflake_tag_keys and dqlabs_tag_keys:
            # Get only the unique tags that are in dqlabs but not in source
            drop_tags = list(set(snowflake_tag_keys) - set(dqlabs_tag_keys))
        return drop_tags


def delete_semantic_tags(
    config: dict, schema_tags_metadata: dict, tag_key: str, droptag_list: list = None
):
    """Delete mapped attributes and tags if deleted in native source"""
    snowflake_tag_keys, dqlabs_tag_keys, snowflake_tags = list(), list(), list()
    connection = get_postgres_connection(config)
    """ Get the database name from the credentials"""
    _database = config.get("connection").get("airflow_connection_object").get("extra")
    database_name = json.loads(_database).get("database")
    database_name = '"{}"'.format(database_name)
    df_snowflake_tags = pd.DataFrame.from_records(schema_tags_metadata)
    if not df_snowflake_tags.empty:
        df_snowflake_tags["TAG_KEY"] = df_snowflake_tags["TAG_KEY"].str.upper()
        snowflake_tag_keys = list(df_snowflake_tags["TAG_KEY"].values)
        snowflake_tags = list(df_snowflake_tags["TAG_NAME"].values)
    with connection.cursor() as cursor:
        """Function to delete from backend if some tag is not present in source"""
        dqlabs_dict = get_dqlabs_tags(connection, cursor, config, database_name)
        df_dqlabs = pd.DataFrame.from_records(dqlabs_dict)
        if not df_dqlabs.empty:
            df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.replace('"', "")
            df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.upper()
            dqlabs_tag_keys = list(df_dqlabs["tag_key"].values)
        if snowflake_tag_keys and dqlabs_tag_keys:
            # Get only the unique tags that are in dqlabs but not in source
            drop_tags = []
            if tag_key == "dqlabs":
                drop_tags = list(set(dqlabs_tag_keys) - set(snowflake_tag_keys))
                if drop_tags:
                    for tag_key in drop_tags:
                        _tag_id = df_dqlabs.loc[
                            df_dqlabs["tag_key"] == tag_key, "tag_id"
                        ].tolist()
                        if _tag_id:
                            # Get the first element of the list tag_id
                            _tag_id = _tag_id[0]
                        attribute_id = df_dqlabs.loc[
                            df_dqlabs["tag_key"] == tag_key, "attribute_id"
                        ].tolist()
                        if attribute_id:
                            # Get the first element of the list attribute_id
                            attribute_id = attribute_id[0]
                        # drop data from attribute_metadata_tags table first(foreign key dependency)
                        if _tag_id and attribute_id:
                            query_string = f"""
                                delete from core.tags_mapping
                                where attribute_id = '{attribute_id}' and tags_id='{_tag_id}'
                            """
                            cursor = execute_query(connection, cursor, query_string)
            elif tag_key == "snowflake":
                if droptag_list:
                    for tag_key in droptag_list:
                        _tag_id = df_dqlabs.loc[
                            df_dqlabs["tag_key"] == tag_key, "tag_id"
                        ].tolist()
                        if _tag_id:
                            # Get the first element of the list tag_id
                            _tag_id = _tag_id[0]
                        attribute_id = df_dqlabs.loc[
                            df_dqlabs["tag_key"] == tag_key, "attribute_id"
                        ].tolist()
                        if attribute_id:
                            # Get the first element of the list attribute_id
                            attribute_id = attribute_id[0]
                        # drop data from attribute_metadata_tags table first(foreign key dependency)
                        if _tag_id and attribute_id:
                            query_string = f"""
                            delete from core.tags_mapping
                            where attribute_id = '{attribute_id}' and tags_id='{_tag_id}'
                            """
                            cursor = execute_query(connection, cursor, query_string)

        # Delete all the unmapped tags from backend
        # if tag_key == 'dqlabs':
        #     unused_tags = get_unused_tags(connection, cursor)
        #     unused_tags_list = unused_tags.get("tag_id", [])
        #     if unused_tags_list:
        #         for tag_id in unused_tags_list:
        #             try:
        #                 query = f"""
        #                         delete from core.tags
        #                         where parent_id='{tag_id}';
        #                         """
        #                 cursor_tag = execute_query(
        #                     connection, cursor, query)
        #                 query_string = f"""
        #                                 delete from core.tags
        #                                 where id='{tag_id}';
        #                                 """
        #                 cursor_tag = execute_query(
        #                     connection, cursor, query_string)
        #             except Exception as e:
        #                 log_error("attribute metadata : inserting new tags", e)


def push_dqlabs_to_source(
    config: object, tag_list: list = None, schema_tags_metadata: dict = None
):
    """
    Get all the new tags if any for the asset from dqlabs portal
    and push it back to the source database
    """
    connection = get_postgres_connection(config)
    default_queries = get_queries(config)
    source_connection = None
    table_name = config.get("technical_name")
    schema_name = config.get("schema")
    schema_condition = (
        default_queries.get("schema_condition") if default_queries else ""
    )
    """ Get the database name from the credentials"""
    _database = config.get("connection").get("airflow_connection_object").get("extra")
    database_name = json.loads(_database).get("database")

    with connection.cursor() as cursor:
        native_tag_details = get_source_tags(connection, cursor, config)
        if native_tag_details:
            native_df = pd.DataFrame.from_records(native_tag_details)
            tags_list = list(set(native_df["tag_name"].tolist()))
            tags_dict = {
                tag: (list(native_df.loc[native_df["tag_name"] == tag, "column_name"]))
                for tag in tags_list
            }
            for tag_name in tags_list:
                if tag_name not in tag_list:
                    try:
                        create_tags = get_metadata_query(
                            "create_source_tags", default_queries, config
                        )
                        create_source_query = prepare_query_string(
                            create_tags,
                            schema_name=schema_name,
                            database_name=database_name,
                            schema_condition=schema_condition,
                            tag_name=tag_name,
                        )
                        execute_native_query(
                            config,
                            create_source_query,
                            source_connection,
                            no_response=True,
                        )
                    except Exception as e:
                        log_error(
                            "Privileges Error : Insufficient priveleges to work on current schema",
                            e,
                        )
                for column in tags_dict.get(tag_name):
                    attribute_name = column
                    try:
                        push_tags_source = get_metadata_query(
                            "push_tags_to_source", default_queries, config
                        )
                        push_tags_query = prepare_query_string(
                            push_tags_source,
                            schema_name=schema_name,
                            table_name=table_name,
                            attribute=attribute_name,
                            database_name=database_name,
                            schema_condition=schema_condition,
                            tag_name=tag_name,
                        )

                        execute_native_query(
                            config, push_tags_query, source_connection, no_response=True
                        )
                    except Exception as e:
                        log_error(
                            "Privileges Error : Insufficient priveleges to work on current schema",
                            e,
                        )
                    # Depreceated this method for the current version
                    # query_string = f"""
                    # update core.tags set native_query_run='True'
                    # where name='{tag_name}'
                    # """
                    # cursor = execute_query(
                    #     connection, cursor, query_string)

        # Unset tags in source if un-mapped from dqlabs environment
        if schema_tags_metadata:
            dqlabs_tag_keys, snowflake_tag_keys = list(), list()
            database_name = '"{}"'.format(database_name)
            df_snowflake_tags = pd.DataFrame.from_records(schema_tags_metadata)
            # only check tags for the current asset
            if "OBJECT_NAME" in df_snowflake_tags.columns:
                df_snowflake_tags = df_snowflake_tags[
                    df_snowflake_tags["OBJECT_NAME"] == table_name
                ]
            if not df_snowflake_tags.empty:
                df_snowflake_tags["TAG_KEY"] = df_snowflake_tags["TAG_KEY"].str.upper()
                snowflake_tag_keys = list(df_snowflake_tags["TAG_KEY"].values)
            dqlabs_dict = get_dqlabs_tags(connection, cursor, config, database_name)
            df_dqlabs = pd.DataFrame.from_records(dqlabs_dict)
            if not df_dqlabs.empty:
                df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.replace('"', "")
                df_dqlabs["tag_key"] = df_dqlabs["tag_key"].str.upper()
                dqlabs_tag_keys = list(df_dqlabs["tag_key"].values)
            if snowflake_tag_keys and dqlabs_tag_keys:
                # Get only the unique tags that are in dqlabs but not in source
                drop_tags = list(set(snowflake_tag_keys) - set(dqlabs_tag_keys))
                if drop_tags:
                    for tag in drop_tags:
                        try:
                            database_name = tag.split(".")[0]
                            schema_name = tag.split(".")[1]
                            table_name = tag.split(".")[2]
                            sf_attribute_name = tag.split(".")[3]
                            sf_tag_name = tag.split(".")[4]
                            attribute_name = get_attribute_name(
                                connection, cursor, sf_attribute_name
                            )
                            attribute_name = attribute_name.get("attribute_name", [])
                            if attribute_name:
                                attribute_name = attribute_name[0]
                            tag_name = get_tag_name(connection, cursor, sf_tag_name)
                            tag_name = tag_name.get("tag_name", [])
                            if tag_name:
                                tag_name = tag_name[0]
                            if tag_name and attribute_name:
                                untag_tags = get_metadata_query(
                                    "alter_tags_source", default_queries, config
                                )
                                alter_tags_query = prepare_query_string(
                                    untag_tags,
                                    schema_name=schema_name,
                                    table_name=table_name,
                                    attribute=attribute_name,
                                    database_name=database_name,
                                    schema_condition=schema_condition,
                                    tag_name=tag_name,
                                )
                                execute_native_query(
                                    config,
                                    alter_tags_query,
                                    source_connection,
                                    no_response=True,
                                )
                        except Exception as e:
                            log_error(
                                "TagID Error : TagID already dropped from snowflake", e
                            )


def push_tags(
    config,
    source_connection,
    default_queries,
    connection_type,
):
    snowflake_account_details = {}
    semantic_tag_dict = {}
    semantic_name, semantic_description, semantic_column = [], [], []
    semantic_tags = get_metadata_query("tags", default_queries, config)
    schema_tags = get_metadata_query("get_schema_tags", default_queries, config)
    organization_tags = get_metadata_query(
        "show_organizations", default_queries, config
    )
    all_tags = get_metadata_query("show_tags", default_queries, config)

    # Snowflake Push Semantic Job
    push_snowflake_tags_flag = None
    snowflake_edition = "STANDARD"  # default edition
    if connection_type.upper() == ConnectionType.Snowflake.value.upper():
        push_snowflake_tags_flag = (
            config.get("connection").get("credentials").get("push_semantic_tags")
        )

    # Get the snowflake account details - Edition (Standard, Enterprise, Virtual Private Snowflake (VPS), Business Critical)
    if (
        push_snowflake_tags_flag
        and connection_type.upper() == ConnectionType.Snowflake.value.upper()
    ):
        connection_account = config.get("connection").get("credentials").get("account")
        snowflake_account = connection_account.split(".")[0]
        if snowflake_account:
            try:
                all_editions, _ = execute_native_query(
                    config, organization_tags, source_connection, is_list=True
                )
                for accounts in all_editions:
                    account_edition = accounts.get("edition")
                    account_locator = accounts.get("account_locator")
                    if account_locator.upper() == snowflake_account.upper():
                        snowflake_edition = account_edition

                    snowflake_account_details.update(
                        {account_locator: snowflake_edition}
                    )
                    snowflake_edition = snowflake_account_details.get(
                        snowflake_account.upper(), "STANDARD"
                    )
            except Exception as e:
                snowflake_edition = "STANDARD"
                log_error("snowflake account doesn't have neccessary rights", e)

        """ save_semantic_tags should always run after attribute_metadata job is run"""
        if snowflake_edition not in ["STANDARD", "READER"]:
            if semantic_tags:
                semantic_schema, droptag_list = list(), list()
                semantic_tag_metadata, _ = execute_native_query(
                    config, semantic_tags, source_connection, is_list=True
                )
                semantic_tag_metadata = convert_to_lower(semantic_tag_metadata)
                if semantic_tag_metadata:
                    for semantic_dict in semantic_tag_metadata:
                        semantic_name.append(semantic_dict.get("tag_name"))
                        semantic_description.append(semantic_dict.get("tag_comment"))
                        semantic_column.append(semantic_dict.get("column_name"))
                        semantic_schema.append(semantic_dict.get("tag_schema"))
                    (
                        semantic_tag_dict.update({"tag_name": semantic_name})
                        if semantic_name
                        else []
                    )
                    (
                        semantic_tag_dict.update({"description": semantic_description})
                        if semantic_description
                        else []
                    )
                    (
                        semantic_tag_dict.update({"column_name": semantic_column})
                        if semantic_column
                        else []
                    )
                    (
                        semantic_tag_dict.update({"schema_name": semantic_schema})
                        if semantic_schema
                        else []
                    )
                    schema_tags_metadata = None
                    if schema_tags_metadata:
                        droptag_list = get_delete_attributemetadata(
                            config, schema_tags_metadata
                        )
                    save_semantic_tags(config, semantic_tag_dict)
                    if droptag_list:
                        delete_semantic_tags(
                            config,
                            schema_tags_metadata,
                            tag_key="snowflake",
                            droptag_list=droptag_list,
                        )

    """ Get all the tags from snowflake for the current schema and database"""
    combined_tags_list = []
    schema_tags_metadata = []
    if snowflake_edition not in ["STANDARD", "READER"]:
        if all_tags and schema_tags:
            all_tag, _ = execute_native_query(
                config, all_tags, source_connection, is_list=True
            )
            all_tags_list = [tag.get("name") for tag in all_tag]
            _database = (
                config.get("connection").get("airflow_connection_object").get("extra")
            )
            database_name = json.loads(_database).get("database")
            schema_name = config.get("schema")
            schema_condition = (
                default_queries.get("schema_condition") if default_queries else ""
            )
            schema_tags_query = prepare_query_string(
                schema_tags,
                schema_name=schema_name,
                database_name=database_name,
                schema_condition=schema_condition,
            )
            schema_tags_metadata, _ = execute_native_query(
                config, schema_tags_query, source_connection, is_list=True
            )
            schema_tags_list = [tag.get("TAG_NAME") for tag in schema_tags_metadata]
            combined_tags_list = list(set(all_tags_list + schema_tags_list))

    # Push from dqlabs to source -- Run only when tags for both the schema and table is availaible
    if snowflake_edition not in ["STANDARD", "READER"]:
        if schema_tags_metadata:
            delete_semantic_tags(config, schema_tags_metadata, tag_key="dqlabs")
        if push_snowflake_tags_flag:
            push_dqlabs_to_source(config, combined_tags_list, schema_tags_metadata)


def get_asset_metadata(config: dict, source_connection, **kwargs) -> None:
    asset_metadata = {}
    asset = config.get("asset")
    asset = asset if asset else {}
    is_query_mode = kwargs.get("is_query_mode")
    querymode_table_query = kwargs.get("querymode_table_query")
    asset_query = kwargs.get("asset_query")
    connection_type = kwargs.get("connection_type")
    measure_level_queries = kwargs.get("measure_level_queries")

    # Populate row count and column count
    if is_query_mode and querymode_table_query:
        for query in querymode_table_query:
            query_metadata, native_connection = execute_native_query(
                config, query, source_connection
            )
            if not source_connection and native_connection:
                source_connection = native_connection
            query_metadata = convert_to_lower(query_metadata)
            query_metadata = query_metadata if query_metadata else {}
            if query_metadata:
                asset_metadata.update(**query_metadata)
    else:
        try:
            response = None
            if asset_query and connection_type != ConnectionType.S3Select.value:
                measure_level_queries.update({"metadata": asset_query})
                response, native_connection = execute_native_query(
                    config,
                    asset_query,
                    source_connection,
                    is_list=(connection_type == ConnectionType.Hive.value),
                )
                if not source_connection and native_connection:
                    source_connection = native_connection
        except Exception as e:
            log_error("Asset Query Error :", e)
            pass


        if connection_type == ConnectionType.Hive.value:
            asset_metadata.update(
                {
                    "row_count": 0,
                    "table_size": 0,
                    "freshness": 0,
                }
            )

        elif connection_type == ConnectionType.S3Select.value:
            properties = asset.get("properties")
            properties = (
                json.loads(properties)
                if properties and isinstance(properties, str)
                else properties
            )
            properties = properties if properties else {}
            connection = config.get("connection")
            connection = connection if connection else {}
            credentials = connection.get("credentials")
            credentials = (
                json.loads(credentials)
                if credentials and isinstance(credentials, str)
                else credentials
            )
            credentials = credentials if credentials else {}
            s3_bucket_name = credentials.get("bucket")
            if not s3_bucket_name:
                s3_bucket_name = credentials.get("bucket_name")
            s3_bucket_name = s3_bucket_name.strip()

            s3_folder_path = credentials.get("folder")
            if not s3_folder_path:
                s3_folder_path = credentials.get("folder_path")
            s3_folder_path = s3_folder_path if s3_folder_path else ""
            s3_file_name = asset.get("name")
            s3_file_name = s3_file_name if s3_file_name else ""
            s3_key = (
                f"{s3_folder_path}/{s3_file_name}" if s3_folder_path else s3_file_name
            )

            response, _ = execute_native_query(
                config,
                "",
                source_connection,
                method_name="execute",
                parameters=dict(
                    method_name="get_metadata", file_name=s3_file_name, key=s3_key
                ),
            )
            response = response if response else {}
            s3_file_size = properties.get("file_size", "")
            response.update({"table_size": s3_file_size})
            asset_metadata.update({**response})
        else:
            asset_metadata = convert_to_lower(response)
            asset_metadata = asset_metadata if asset_metadata else {}
    return asset_metadata


def get_row_count(
    config, source_connection, default_queries, row_count_query, **kwargs
):
    asset_metadata = kwargs.get("asset_metadata")
    connection_type = kwargs.get("connection_type")
    measure_level_queries = kwargs.get("measure_level_queries")

    if row_count_query and connection_type != ConnectionType.S3Select.value:
        row_count_query = get_query_string(
            config, default_queries, row_count_query, is_full_query=True
        )
        measure_level_queries.update({"row_count": row_count_query})
        
        row_count_data, _ = execute_native_query(
            config, row_count_query, source_connection
        )
        row_count_data = convert_to_lower(row_count_data)
        row_count = row_count_data.get("total_rows") if row_count_data else 0
        row_count = row_count if row_count else 0
        asset_metadata.update({"row_count": row_count})

def get_direct_query_attributes(config, default_queries, asset):
        """
        Process the query and extracts the attribute details for direct query asset
        """
        organization_id = str(asset.get("organization_id"))
        connection_id = str(asset.get("connection_id"))
        asset_id = str(asset.get("id"))
        failed_rows_queries = default_queries.get("failed_rows", {})
        failed_rows_queries = failed_rows_queries if failed_rows_queries else {}
        datatype_mappings = failed_rows_queries.get("datatypes_mapping", {})
        datatype_mappings = datatype_mappings if datatype_mappings else {}
        dq_data_types = default_queries.get("dq_datatypes", {})
        dq_data_types = dq_data_types if dq_data_types else {}

        query = asset.get("query") if asset.get("query") else ""
        connection_type = config.get("connection_type")

        default_query_with_limit = default_queries.get(
            "default_query_limit")
        if default_query_with_limit:
            asset_query = f"({query}) as direct_query_table"
            if connection_type and str(connection_type).lower() in [
                ConnectionType.Oracle.value.lower(),
                ConnectionType.BigQuery.value.lower(),
            ]:
                asset_query = f"({query})"
            asset_query = (
                default_query_with_limit.replace("<count>", "10")
                .replace("<query>", "*")
                .replace("<table_name>", asset_query)
            )
            sample_data, _ = execute_native_query(
                config, asset_query, connection_type, is_list=True, convert_lower=False
            )
        sample_data = sample_data if sample_data else []
        asset_data = sample_data[0] if len(sample_data) > 0 else {}
        asset_data = asset_data if asset_data else {}

        query_tables = []
        query_tables_aliases = {}
        parsed_columns = []
        parsed_column_aliases = {}
        function_aliases = {}  # Initialize function_aliases before try-except block
        try:
            parser = Parser(query)
            query_tables = parser.tables
            query_tables_aliases = parser.tables_aliases
            parsed_columns = parser.columns

            parsed_columns = parsed_columns if parsed_columns else []
            parsed_columns = [
                column
                for column in parsed_columns
                if column and str(column).lower() != "top"
            ]
            parsed_column_aliases = parser.columns_aliases
            parsed_column_aliases = (
                parsed_column_aliases if parsed_column_aliases else {}
            )
            function_aliases = get_column_alias_with_function(query, connection_type)
            parsed_column_aliases = {k: v for k, v in parsed_column_aliases.items() if k not in function_aliases}
        except Exception as e:
            log_error(f"Failed to parsing the query - {query}. Error: {str(e)}", e)

        query_columns = list(asset_data.keys()) if asset_data else []
        lower_query_columns = list(map(lambda column: column.lower(), query_columns))
        if query_tables_aliases:
            for _, tables in query_tables_aliases.items():
                if not tables:
                    continue
                if isinstance(tables, str):
                    query_tables.append(tables)
                else:
                    query_tables.extend(tables)
        query_tables = list(set(query_tables)) if query_tables else []
        if not query_columns:
            log_error(f"No columns defined in the given query", None)

        table_schema = {}
        query_attributes = {}
        has_multi_table = len(query_tables) > 1
        if query_tables:

            # Fetch table schema and map direct query columns
            for table_name in query_tables:
                if table_name not in table_schema:
                    schema, db_name, query_table = extract_table_name(
                        table_name,
                        schema_only=connection_type
                        in [ConnectionType.BigQuery.value, ConnectionType.SapHana.value,ConnectionType.Oracle.value],
                    )
                    
                    table_attributes = get_table_schema(config, default_queries, query_table, schema, db_name) if query_table and schema else []
                    table_attributes = convert_to_lower(table_attributes)
                    if table_attributes:
                        table_schema.update({table_name: table_attributes})

                table_attributes = table_schema.get(table_name)
                table_attributes = table_attributes if table_attributes else []
                parsed_attributes = {}
                parsed_table_attributes = (
                    list(
                        filter(
                            lambda column: str(column).startswith(f"{table_name}."),
                            parsed_columns,
                        )
                    )
                    if parsed_columns
                    else []
                )
                expanded_columns = []
                for col in parsed_table_attributes:
                    if col.endswith(".*"):
                        # Replace table_name.* with all columns from table_schema
                        if table_schema.get(table_name):
                            expanded_columns.extend([f"{table_name}.{c['column_name']}" for c in table_schema[table_name]])
                        else:
                            print(f"Warning: No table schema found for {table_name} to expand *")
                    else:
                        expanded_columns.append(col)
                parsed_table_attributes = expanded_columns
                if has_multi_table:
                    parsed_attributes = {}
                    for column in parsed_table_attributes:
                        column_name = column.replace(f"{table_name}.", "")
                        if str(column_name).lower() not in lower_query_columns:
                            continue
                        parsed_attributes.update(
                            {
                                str(column_name).lower(): {
                                    "column_name": column,
                                    "table_name": table_name,
                                }
                            }
                        )
                else:
                    parsed_table_attributes = deepcopy(query_columns)
                    parsed_attributes = {
                        str(column).lower(): {
                            "column_name": column,
                            "table_name": table_name,
                        }
                        for column in parsed_table_attributes
                    }

                for column in parsed_attributes:
                    table_attribute = next(
                        (
                            table_attribute
                            for table_attribute in table_attributes
                            if table_attribute
                            and str(table_attribute.get("column_name", "")).lower()
                            == column
                        ),
                        None,
                    )
                    if not table_attribute:
                        continue

                    attribute = deepcopy(table_attribute)
                    attribute.update({"is_selected": True})
                    query_attributes.update({column: attribute})

            # Identify datatype for alias columns
            if parsed_column_aliases:
                for (
                    alias_column,
                    source_columns,
                ) in parsed_column_aliases.items():
                    if (
                        not source_columns
                        or str(alias_column).lower() in query_attributes
                        or str(alias_column).lower() not in lower_query_columns
                    ):
                        continue

                    datatype = ""
                    alias_attribute_datatypes = []
                    source_columns = (
                        [source_columns]
                        if isinstance(source_columns, str)
                        else source_columns
                    )
                    source_columns = source_columns if source_columns else []

                    # pre-process the source columns
                    processed_source_columns = []
                    for column in source_columns:
                        if isinstance(source_columns, list):
                            processed_source_columns.extend(column)
                        else:
                            processed_source_columns.append(column)

                    for column in source_columns:
                        tokens = column.split(".")
                        column_name = tokens[-1]
                        if has_multi_table:
                            table_name = column.replace(f".{column_name}", "")
                        else:
                            table_name = query_tables[0] if query_tables else ""

                        alias_table_attributes = table_schema.get(table_name)
                        alias_table_attributes = (
                            alias_table_attributes if alias_table_attributes else []
                        )
                        table_attribute = next(
                            (
                                table_attribute
                                for table_attribute in alias_table_attributes
                                if table_attribute
                                and str(table_attribute.get("column_name", "")).lower()
                                == str(column_name).lower()
                            ),
                            None,
                        )
                        if not table_attribute:
                            continue

                        column_datatype = table_attribute.get("datatype")
                        column_datatype = column_datatype if column_datatype else ""
                        if column_datatype:
                            alias_attribute_datatypes.append(column_datatype)

                    alias_attribute_datatypes = (
                        list(set(alias_attribute_datatypes))
                        if alias_attribute_datatypes
                        else []
                    )
                    alias_attribute_datatypes = (
                        alias_attribute_datatypes if alias_attribute_datatypes else []
                    )
                    if len(alias_attribute_datatypes) == 1:
                        datatype = alias_attribute_datatypes[0]
                    elif len(alias_attribute_datatypes) > 1:
                        column_types = []
                        for column_type in alias_attribute_datatypes:
                            for key, value in dq_data_types.items():
                                if column_type.upper() not in value:
                                    continue
                                column_types.append(key.lower())
                                break

                        datatype = ""
                        index = -1
                        if "text" in column_types:
                            index = column_types.index("text")
                        elif "numeric" in column_types:
                            index = column_types.index("numeric")
                        elif "integer" in column_types:
                            index = column_types.index("integer")
                        elif "bit" in column_types:
                            index = column_types.index("bit")

                        if index > -1:
                            datatype = alias_attribute_datatypes[index]
                        datatype = datatype if datatype else ""

                    if not datatype:
                        continue

                    attribute = dict(
                        column_name=alias_column,
                        datatype=datatype,
                        description="",
                        schema="",
                        is_primary_key=False,
                        is_selected=True,
                    )
                    query_attributes.update({str(alias_column).lower(): attribute})

        if function_aliases:
            function_aliases = convert_to_lower(function_aliases)
            query_attributes = {k: v for k, v in query_attributes.items() if k.lower() not in function_aliases}
        # to get correct datatypes for unmapped columns
        snowflake_query_schema = {}
        if (
            connection_type
            and str(connection_type).upper() == ConnectionType.Snowflake.value.upper()
        ):
            unmapped_columns = [
                col for col in query_columns
                if not query_attributes or str(col).lower() not in [
                    str(k).lower() for k in query_attributes.keys()
                ]
            ]
            if unmapped_columns and table_schema:
                # Match unmapped columns to tables - e.g. SELECT t2.* columns come from t2's table
                for col in unmapped_columns:
                    col_lower = str(col).lower()
                    for table_name, table_attrs in table_schema.items():
                        table_attr = next(
                            (
                                attr for attr in table_attrs
                                if attr and str(attr.get("column_name", "")).lower() == col_lower
                            ),
                            None,
                        )
                        if table_attr:
                            col_type = table_attr.get("datatype") or table_attr.get("DATA_TYPE")
                            if col_type:
                                snowflake_query_schema[col_lower] = str(col_type).split("(")[0].upper()
                            break
        for column in query_columns:
            if query_attributes and str(column).lower() in query_attributes:
                continue

            # For Snowflake: use schema from query result when available
            if snowflake_query_schema and str(column).lower() in snowflake_query_schema:
                datatype = snowflake_query_schema[str(column).lower()]
                datatype = (
                    datatype_mappings.get(datatype.lower())
                    if datatype and datatype.lower() in datatype_mappings
                    else datatype
                )
            else:
                datatype = get_default_datatype(
                    column, sample_data, datatype_mappings
                )
            datatype = str(datatype).upper() if datatype else ""
            datatype = datatype.split("(")[0] if datatype else ""
            datatype = datatype.upper() if datatype else ""
            attribute = dict(
                column_name=column,
                datatype=datatype,
                description="",
                schema="",
                is_primary_key=False,
                is_selected=True,
            )
            query_attributes.update({column: attribute})
        attributes = list(query_attributes.values()) if query_attributes else []
        attributes = attributes if attributes else []
        return attributes


def get_column_count(
    config: dict, source_connection, default_queries, attribute_query, **kwargs
) -> None:
    # Populate columns and it's constrains
    asset_metadata = kwargs.get("asset_metadata")
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    measure_level_queries = kwargs.get("measure_level_queries")
    metadata = kwargs.get("metadata")
    table_name = config.get("table_name")
    watermark = config.get("watermark", "")
    if attribute_query:
        measure_level_queries.update({"column_count": attribute_query})
        response, _ = execute_native_query(
            config, attribute_query, source_connection, is_list=True
        )
        response = response if response else []

        # Bind JSON Attributes to the metadata
        json_schema_query = default_queries.get("json_process", "")
        if json_schema_query:
            for attribute in response:
                if attribute.get("datatype").lower() in [VARIANT, JSON, ARRAY, OBJECT]:
                    json_attribute_query = json_schema_query.get("json_attributes", "")
                    json_attribute_query = prepare_query_string(json_attribute_query, attribute=attribute.get("name"))
                    json_attribute_query = get_query_string(config, default_queries, json_attribute_query, is_full_query=True)
                    json_response, _ = execute_native_query(config, json_attribute_query, source_connection, is_list=True)
                    for json_attribute in json_response:
                        attribute_info = [
                            {
                                "name": json_attribute.get("key_name"),
                                "key_path": json_attribute.get("key_path"),
                                "is_null": True,
                                "datatype": json_attribute.get("datatype"),
                                "description": json_attribute.get("comment, "),
                                "level": json_attribute.get("level"),
                                "parent_attribute_name": attribute.get("name"),
                            }
                        ]
                        response.extend(attribute_info)

        if (
            connection_type == ConnectionType.Hive.value
            or connection_type == ConnectionType.ADLS.value
            or connection_type == ConnectionType.File.value
            or connection_type == ConnectionType.S3.value
            or (
                connection_type == ConnectionType.Databricks.value
                and config.get("database_name", "") == "hive_metastore"
            )
        ):
            for attribute in response:
                attribute_info = [
                    {
                        "name": attribute.get("col_name"),
                        "is_null": True,
                        "datatype": attribute.get("data_type"),
                        "description": attribute.get("comment"),
                    }
                ]
                metadata.extend(attribute_info)
            hive_column_count = len(response)
            asset_metadata.update({"column_count": hive_column_count})
        elif connection_type == ConnectionType.Teradata.value:
            metadata = convert_to_lower(response)
            metadata = parse_teradata_columns(
                response, default_queries, "name", "datatype"
            )
            asset_metadata.update({"column_count": len(metadata)})
        elif connection_type == ConnectionType.Salesforce.value:
            pg_connection = get_postgres_connection(config)
            log_info(("attr 1594", attribute_query))
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= attribute_query),
            )
            for data in response:
                metadata.append({
                    "name": data.get("column_name"),
                    "datatype": data.get("datatype"),
                    "is_null": data.get("is_null", True),
                    "description": data.get("description", ""),
                })
            metadata = convert_to_lower(metadata)
            asset_metadata.update({"column_count": len(metadata)})
        elif connection_type == ConnectionType.SalesforceMarketing.value:
            pg_connection = get_postgres_connection(config)
            for data in response:
                metadata.append({
                    "name": data.get("columnname"),
                    "datatype": data.get("datatypename"),
                    "is_null": True if data.get("isnullable", 0) == 1 else False,
                    "description": data.get("description", ""),
                })
            metadata = convert_to_lower(metadata)
            asset_metadata.update({"column_count": len(metadata)})
        else:
            metadata = convert_to_lower(response)
            asset_metadata.update({"column_count": len([col for col in metadata if not col.get("parent_attribute_name")])})
    elif is_direct_query_asset:
        metadata = extract_workflow.get_selected_attributes(config)
        metadata = (
            [
                dict(
                    name=attribute.get("name"),
                    is_null=attribute.get("is_null"),
                    description=attribute.get("description"),
                    datatype=attribute.get("datatype"),
                )
                for attribute in metadata
                if attribute
            ]
            if metadata
            else []
        )
        asset_metadata.update({"column_count": len([col for col in metadata if not col.get("parent_attribute_name")])})
    elif (
        connection_type == ConnectionType.Databricks.value
        and config.get("type") == "TABLE"
    ):
        column_describe_query = f""" describe {table_name} ;"""
        measure_level_queries.update({"column_count": column_describe_query})
        response, _ = execute_native_query(
            config, column_describe_query, source_connection, is_list=True
        )
        df = pd.DataFrame.from_records(response)
        df["col_name"] = df["col_name"].apply(lambda x: x.upper())
        result_columns = list(df["col_name"])
        _column_count = len(result_columns)
        asset_metadata.update({"column_count": _column_count})
    else:
        if connection_type == ConnectionType.S3Select.value:
            connection = get_postgres_connection(config)
            asset_id = config.get("asset_id")
            with connection.cursor() as cursor:
                query_string = f"""
                    select name as col_name,
                    datatype,
                    description
                    from core.attribute
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                attributes = fetchall(cursor)
                for attribute in attributes:
                    attribute_info = [
                        {
                            "name": attribute.get("col_name"),
                            "is_null": True,
                            "datatype": attribute.get("datatype"),
                            "description": attribute.get("description"),
                        }
                    ]
                    metadata.extend(attribute_info)
                s3_column_count = len(attributes)
                asset_metadata.update({"column_count": s3_column_count})
        elif connection_type == ConnectionType.SalesforceDataCloud.value:
            pg_connection = get_postgres_connection(config)
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", table_name = config.get("asset").get("name"), asset_type=config.get("asset").get("properties").get("asset_type"),object_id=config.get("asset").get("properties").get("object_id")),
            )
            metadata = []
            for attribute in response:
                attribute_info = [
                        {
                            "name": attribute.get("column_name"),
                            "is_null": True,
                            "datatype": attribute.get("datatype"),
                            "description": attribute.get("description"),
                        }
                    ]
                metadata.extend(attribute_info)
            metadata = convert_to_lower(metadata)
            asset_metadata.update({"column_count": len(metadata)})
        elif connection_type.lower() == "mongo":
            pg_connection = get_postgres_connection(config)
            
            database_name = config.get("schema", "")
            if isinstance(database_name, list):
                database_name = database_name[0] if database_name else ""
            
            asset = config.get("asset", {})
            collection_name = asset.get("technical_name") or asset.get("name", "")
                        
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(
                    method_name="get_table_schema",
                    database=database_name,
                    collection=collection_name
                ),
            )            
            metadata = []
            if response and isinstance(response, list):
                for attribute in response:
                    metadata.append({
                        "name": attribute.get("column_name") or attribute.get("name"),
                        "is_null": True,  # MongoDB fields are nullable by default
                        "datatype": attribute.get("datatype") or "STRING",
                        "description": attribute.get("description", ""),
                    })
            metadata = convert_to_lower(metadata)
            asset_metadata.update({"column_count": len(metadata)})
    return metadata


def get_freshness(config, source_connection, default_queries, **kwargs):
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_metadata = kwargs.get("asset_metadata")
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    view_type = config.get("asset", {}).get("view_type", None)
    measure_level_queries = kwargs.get("measure_level_queries")
    table_name = kwargs.get("table_name")
    schema_name = kwargs.get("schema_name")
    metadata = kwargs.get("metadata")

    freshness_column = config.get("watermark", "")
    is_custom_fingerprint = asset.get("is_custom_fingerprint")
    custom_fingerprint_date_format = None
    if is_custom_fingerprint:
        custom_fingerprint = config.get("custom_fingerprint")
        custom_fingerprint_value = custom_fingerprint.get("value", None) if custom_fingerprint else None
        custom_fingerprint_date_format = custom_fingerprint.get("format", None) if custom_fingerprint else None
        if custom_fingerprint_value:
            freshness_column = custom_fingerprint_value

        if not custom_fingerprint_date_format:
            is_custom_fingerprint = False
            custom_fingerprint_date_format = ""
            freshness_column = (
                freshness_column if freshness_column else custom_fingerprint_value
            )

    freshness = asset_metadata.get("freshness", None)
    freshness = freshness if freshness else None

    # Databricks doesn't provide metadata freshness, so processing it via describe history query
    if (
        connection_type == ConnectionType.Databricks.value
        and config.get("type") == "TABLE"
    ):
        table_name = config.get("table_name")
        describe_history_query = f"""describe detail {table_name};"""
        measure_level_queries.update({"freshness": describe_history_query})
        response, _ = execute_native_query(
            config, describe_history_query, source_connection, is_list=True
        )
        freshness = get_databricks_freshness(response) if response else freshness
    elif (
        connection_type == ConnectionType.MSSQL.value
        and not freshness
        and not is_custom_fingerprint
        and view_type != "Direct Query"
    ):
        try:
            metadata_freshness_query = get_metadata_query(
                "metadata_freshness", default_queries, config, False, True
            )
            metadata_freshness_query = prepare_query_string(
                metadata_freshness_query, schema_name=schema_name, table_name=table_name
            )
            measure_level_queries.update({"freshness": metadata_freshness_query})
            freshness_metadata, _ = execute_native_query(
                config, metadata_freshness_query, source_connection
            )
            freshness_metadata = convert_to_lower(freshness_metadata)
            freshness = (
                freshness_metadata.get("freshness", None) if freshness_metadata else None
            )
        except Exception as e:
            log_error("Error getting freshness from metadata_freshness query", e)
            freshness = None

    # Populate freshness value based on date column on the table
    if (
        not freshness
        and not freshness_column
        and not is_custom_fingerprint
        and not view_type
    ):
        date_columns = list(
            filter(
                lambda column: get_derived_type_category(
                    (column.get("datatype") or "").lower(), is_freshness_column=True
                )
                == "date",
                metadata,
            )
        )
        if date_columns:
            freshness_column = date_columns[0].get("name")

    if is_custom_fingerprint or view_type in ["View", "Direct Query"]:
        freshness = None

    is_primay_fingerprint = not is_custom_fingerprint or (
        is_custom_fingerprint and not custom_fingerprint_date_format
    )
    
    if is_primay_fingerprint:
        if (
            connection_type
            not in (
                ConnectionType.Athena.value,
                ConnectionType.EmrSpark.value,
            )
            and not freshness
            and freshness_column
        ):
            if connection_type and connection_type.lower() == "mongo":
                is_valid_primary_fingerprint = False
                if metadata and freshness_column:
                    for column in metadata:
                        if column.get("name") == freshness_column:
                            datatype = (column.get("datatype") or "").lower()
                            if get_derived_type_category(datatype, is_freshness_column=True) == "date":
                                is_valid_primary_fingerprint = True
                            break
                
                if not is_valid_primary_fingerprint:
                    freshness = None
                else:
                    freshness_query = get_metadata_query(
                        "freshness",
                        default_queries,
                        config,
                        False,
                        True,
                        attribute=freshness_column,
                    )
                    freshness_query = prepare_nosql_pipeline(
                        freshness_query, attribute=freshness_column
                    )
            else:
                freshness_query = get_metadata_query(
                    "freshness",
                    default_queries,
                    config,
                    False,
                    True,
                    attribute=freshness_column,
                )
                freshness_query = prepare_query_string(
                    freshness_query, attribute=freshness_column, table_name=table_name
                )
                
                freshness_query = get_query_string(
                    config,
                    default_queries,
                    freshness_query,
                    is_full_query=True,
                )
                measure_level_queries.update({"freshness": freshness_query})
                freshness_metadata, _ = execute_native_query(
                    config, freshness_query, source_connection
                )
                freshness_metadata = convert_to_lower(freshness_metadata)
                if freshness_metadata:
                    freshness = freshness_metadata.get("freshness", 0)
                else:
                    freshness = 0
        elif (
            connection_type
            in [ConnectionType.Athena.value, ConnectionType.EmrSpark.value]
            and not is_direct_query_asset
        ):
            database_name = config.get("schema")
            table_name = config.get("technical_name")
            method_name = (
                "execute"
                if connection_type == ConnectionType.EmrSpark.value
                else "get_asset_freshness"
            )
            response, _ = execute_native_query(
                config,
                "",
                source_connection,
                method_name=method_name,
                parameters=dict(
                    method_name="get_asset_freshness",
                    database_name=database_name,
                    table_name=table_name,
                ),
            )
            response = response if response else {}
            freshness = response.get("freshness")
            freshness = freshness if freshness else freshness
        else:
            if (
                connection_type
                in [ConnectionType.Databricks.value, ConnectionType.Redshift.value]
                and view_type in ["Table", "View", "Direct Query"]
                and is_custom_fingerprint
            ):
                freshness_query = get_metadata_query(
                    "freshness",
                    default_queries,
                    config,
                    False,
                    True,
                    attribute=freshness_column,
                )
                
                if connection_type and connection_type.lower() == "mongo":
                    freshness_query = prepare_nosql_pipeline(
                        freshness_query, attribute=freshness_column
                    )
                else:
                    freshness_query = prepare_query_string(
                        freshness_query, attribute=freshness_column, table_name=table_name
                    )
                
                freshness_query = get_query_string(
                    config,
                    default_queries,
                    freshness_query,
                    is_full_query=True,
                )
                measure_level_queries.update({"freshness": freshness_query})
                freshness_metadata, _ = execute_native_query(
                    config, freshness_query, source_connection
                )
                if freshness_metadata:
                    freshness = freshness_metadata.get("freshness", 0)
                else:
                    freshness = 0
    else:
        custom_fingerprint_freshness_query = default_queries.get(
            "custom_fingerprint_freshness"
        )
        if (
            connection_type == ConnectionType.Teradata.value
            and len(custom_fingerprint_date_format) > 10
        ):
            custom_fingerprint_freshness_query = default_queries.get(
                "custom_fingerprint_freshness_datetime"
            )
        freshness_query = get_query_string(
            config,
            default_queries,
            custom_fingerprint_freshness_query,
            is_full_query=True,
        )
        measure_level_queries.update({"freshness": freshness_query})
        freshness_metadata, _ = execute_native_query(
            config, freshness_query, source_connection
        )
        freshness_metadata = convert_to_lower(freshness_metadata)
        if freshness_metadata:
            freshness = freshness_metadata.get("freshness", 0)
        else:
            freshness = 0

    freshness = freshness if freshness else None
    asset_metadata.update({"freshness": freshness})


def get_duplicate_count(config, source_connection, default_queries, **kwargs):
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_metadata = kwargs.get("asset_metadata")
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    measure_level_queries = kwargs.get("measure_level_queries")
    metadata = kwargs.get("metadata")
    table_name = kwargs.get("table_name")
    primary_key_query = kwargs.get("primary_key_query")
    total_queries = kwargs.get("total_queries")
    primary_keys = kwargs.get("primary_keys")

    # Populate duplicate values based on primary keys
    if not primary_keys:
        keys = list(
            filter(lambda column: column.get("is_primary_key", False), metadata)
        )
        if keys:
            primary_keys = keys

    if not primary_keys and primary_key_query:
        primary_keys, _ = execute_native_query(
            config, primary_key_query, source_connection, is_list=True
        )
        if primary_keys:
            primary_keys = [
                {
                    "name": (
                        key.get("column_name")
                        if key.get("column_name")
                        else key.get("COLUMN_NAME")
                    )
                }
                for key in primary_keys
            ]

    source_attribute = []
    if primary_keys and metadata:
        if connection_type == ConnectionType.SapEcc.value:
            source_attribute = list(
                map(lambda attribute: attribute.get("COLUMN_NAME", "").strip(), metadata)
            )
        else:
            source_attribute = list(
                map(lambda attribute: attribute.get("name").strip(), metadata)
            )
        primary_keys = remove_depricated_primary_keys(
            config, primary_keys, source_attribute
        )

    if primary_keys:
        primary_key = get_primary_key(
            connection_type.lower(), primary_keys, source_attribute
        )
        duplicate_query = get_metadata_query(
            "duplicate", default_queries, config, False, True, attribute=primary_key
        )
        
        if connection_type and connection_type.lower() == "mongo":
            duplicate_query = prepare_nosql_pipeline(
                duplicate_query, attribute=primary_key
            )
        else:
            duplicate_query = prepare_query_string(
                duplicate_query, attribute=primary_key, table_name=table_name
            )
        
        duplicate_query = get_query_string(
            config, default_queries, duplicate_query, is_full_query=True
        )
        if is_direct_query_asset:
            direct_query_metadata = default_queries.get("direct_query_metadata", {})
            direct_query_metadata = (
                direct_query_metadata if direct_query_metadata else {}
            )
            duplicate_query = direct_query_metadata.get("duplicate")
            duplicate_query = total_queries.replace("<table_query>", asset.get("query"))
        measure_level_queries.update({"duplicate_count": duplicate_query})
        duplicate_metadata, _ = execute_native_query(
            config, duplicate_query, source_connection
        )
        duplicate_metadata = convert_to_lower(duplicate_metadata)
        duplicate = duplicate_metadata.get("duplicate_count", 0) if duplicate_metadata else 0
        duplicate = duplicate if duplicate else 0
        asset_metadata.update({"duplicate_count": duplicate})

def check_watermark_column(
    config: dict, source_connection, default_queries, attribute_query, **kwargs
) -> dict:
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    measure_level_queries = kwargs.get("measure_level_queries")
    metadata = kwargs.get("metadata")
    table_name = config.get("table_name")
    watermark = config.get("watermark", "")

    if attribute_query:
        response, _ = execute_native_query(
            config, attribute_query, source_connection, is_list=True
        )
        response = response if response else []

        if (
            connection_type == ConnectionType.Hive.value
            or connection_type == ConnectionType.ADLS.value
            or connection_type == ConnectionType.File.value
            or connection_type == ConnectionType.S3.value
            or (
                connection_type == ConnectionType.Databricks.value
                and config.get("database_name", "") == "hive_metastore"
            )
        ):
            current_attribute_names = {value.get("col_name", "") for value in response if value.get("col_name")}
        elif connection_type == ConnectionType.Teradata.value:
            metadata = convert_to_lower(response)
            metadata = parse_teradata_columns(
                response, default_queries, "name", "datatype"
            )
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
        elif connection_type == ConnectionType.Salesforce.value:
            pg_connection = get_postgres_connection(config)
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= attribute_query),
            )
            current_attribute_names = {value.get("column_name", "") for value in response if value.get("column_name")}
        elif connection_type == ConnectionType.SalesforceMarketing.value:
            pg_connection = get_postgres_connection(config)
            current_attribute_names = {value.get("columnname", "") for value in response if value.get("columnname")}
        else:
            metadata = convert_to_lower(response)
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif is_direct_query_asset:
        metadata = extract_workflow.get_selected_attributes(config)
        current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif (
        connection_type == ConnectionType.Databricks.value
        and config.get("type") == "TABLE"
    ):
        column_describe_query = f""" describe {table_name} ;"""
        response, _ = execute_native_query(
            config, column_describe_query, source_connection, is_list=True
        )
        df = pd.DataFrame.from_records(response)
        df["col_name"] = df["col_name"].apply(lambda x: x.upper())
        current_attribute_names = list(df["col_name"])

    # Validate watermark column exists in current metadata
    if watermark and current_attribute_names:
        if watermark not in current_attribute_names:
            asset_id = config.get("asset_id")
            # Raise exception to fail the job when watermark column is missing
            error_message = WATERMARK_COLUMN_MISSING_ERROR.format(
                column_name=watermark, 
                asset_id=asset_id
            )
            raise Exception(error_message)

    return config

def check_fingerprint_column(
    config: dict, source_connection, default_queries, attribute_query, **kwargs
) -> dict:
    """
    Validates if the configured custom fingerprint column exists in the current table schema.
    If the fingerprint column is missing, it automatically disables custom fingerprint settings
    and allows the system to fall back to watermark column or default behavior.
    
    Similar to check_watermark_column but for custom fingerprint validation.
    """
    
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    measure_level_queries = kwargs.get("measure_level_queries")
    metadata = kwargs.get("metadata")
    table_name = config.get("table_name")
    asset_id = config.get("asset_id")
    
    # Get custom fingerprint configuration
    asset = config.get("asset", {})
    is_custom_fingerprint = asset.get("is_custom_fingerprint", False)
    custom_fingerprint = config.get("custom_fingerprint", {})
    fingerprint_column = custom_fingerprint.get("value", "") if custom_fingerprint else ""
    
    # Only validate if custom fingerprint is enabled and column is specified
    if not is_custom_fingerprint or not fingerprint_column:
        return config
        
    current_attribute_names = set()
        
    if attribute_query:
        try:
            response, _ = execute_native_query(
                config, attribute_query, source_connection, is_list=True
            )
            response = response if response else []
        except Exception as e:
            response = []

        if (
            connection_type == ConnectionType.Hive.value
            or connection_type == ConnectionType.ADLS.value
            or connection_type == ConnectionType.File.value
            or connection_type == ConnectionType.S3.value
            or (
                connection_type == ConnectionType.Databricks.value
                and config.get("database_name", "") == "hive_metastore"
            )
        ):
            current_attribute_names = {value.get("col_name", "") for value in response if value.get("col_name")}
        elif connection_type == ConnectionType.Teradata.value:
            metadata = convert_to_lower(response)
            metadata = parse_teradata_columns(
                response, default_queries, "name", "datatype"
            )
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
        elif connection_type == ConnectionType.Salesforce.value:
            pg_connection = get_postgres_connection(config)
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= attribute_query),
            )
            current_attribute_names = {value.get("column_name", "") for value in response if value.get("column_name")}
        elif connection_type == ConnectionType.SalesforceMarketing.value:
            pg_connection = get_postgres_connection(config)
            current_attribute_names = {value.get("columnname", "") for value in response if value.get("columnname")}
        else:
            metadata = convert_to_lower(response)
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif is_direct_query_asset:
        metadata = extract_workflow.get_selected_attributes(config)
        current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif (
        connection_type == ConnectionType.Databricks.value
        and config.get("type") == "TABLE"
    ):
        column_describe_query = f""" describe {table_name} ;"""
        try:
            response, _ = execute_native_query(
                config, column_describe_query, source_connection, is_list=True
            )
            df = pd.DataFrame.from_records(response)
            df["col_name"] = df["col_name"].apply(lambda x: x.upper())
            current_attribute_names = set(df["col_name"])
        except Exception as e:
            pass
    else:
        if metadata:
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    # Validate fingerprint column exists in current metadata
    
    if fingerprint_column and current_attribute_names:        
        if fingerprint_column not in current_attribute_names:
            
            # Find similar columns for suggestions
            similar_columns = []
            fingerprint_lower = fingerprint_column.lower()
            for col in current_attribute_names:
                col_lower = col.lower()
                # Simple similarity check
                if fingerprint_lower in col_lower or col_lower in fingerprint_lower:
                    similar_columns.append(col)
                elif len(set(fingerprint_lower) & set(col_lower)) / max(len(fingerprint_lower), len(col_lower)) > 0.6:
                    similar_columns.append(col)
                        
            asset_id = config.get("asset_id")
            
            try:
                connection = get_postgres_connection(config)
                with connection.cursor() as cursor:
                    # Only update core.data table (custom_fingerprint is stored here)
                    query = f"""UPDATE core.data 
                        SET custom_fingerprint = NULL
                        WHERE asset_id = '{asset_id}'
                    """
                    cursor.execute(query)
                    
                    # Update asset table to disable is_custom_fingerprint
                    asset_query = f"""UPDATE core.asset 
                        SET is_custom_fingerprint = FALSE
                        WHERE id = '{asset_id}'
                    """
                    cursor.execute(asset_query)
                    
                    connection.commit()
            except Exception as e:
                # Try alternative approach - update only core.data if asset table doesn't have the column
                try:
                    connection = get_postgres_connection(config)
                    with connection.cursor() as cursor:
                        query = f"""UPDATE core.data 
                            SET custom_fingerprint = NULL
                            WHERE asset_id = '{asset_id}'
                        """
                        cursor.execute(query)
                        connection.commit()
                except Exception as e2:
                    pass
            
            # Update asset configuration to disable custom fingerprint
            asset.update({"is_custom_fingerprint": False})
            config.update({
                "custom_fingerprint": None,
                "asset": asset
            })
            
            warning_msg = f"WARNING: Custom fingerprint column '{fingerprint_column}' not found in table schema. "
            if similar_columns:
                warning_msg += f"Similar columns found: {similar_columns}. "
            warning_msg += f"Disabled custom fingerprint for asset {asset_id}. System will fall back to watermark column if available."
    return config

def check_filter_columns(
    config: dict, source_connection, default_queries, attribute_query, **kwargs
) -> dict:
    """
    Validates if the columns used in filter conditions exist in the current table schema.
    If filter columns are missing, it automatically disables filter settings
    and allows the system to continue without filtering.
    
    Similar to check_fingerprint_column but for filter validation.
    """
    connection_type = kwargs.get("connection_type")
    is_direct_query_asset = kwargs.get("is_direct_query_asset")
    metadata = kwargs.get("metadata")
    table_name = config.get("table_name")
    asset_id = config.get("asset_id")
    if not connection_type:
        connection_type = config.get("connection_type")
    
    # Get filter configuration
    asset = config.get("asset", {})
    is_filtered = asset.get("is_filtered", False)
    filter_properties = asset.get("filter_properties", {})
    filter_query = asset.get("filter_query", "")
    
    # Only validate if filtering is enabled and filter properties exist
    if not is_filtered or not filter_properties:
        return config
    
    # Extract column names used in filter rules
    filter_columns = set()
    rules = filter_properties.get("rules", [])
    
    for rule in rules:
        if isinstance(rule, dict) and "attribute" in rule:
            attribute = rule.get("attribute", {})
            if isinstance(attribute, dict):
                # Get column name from attribute label or id
                column_name = attribute.get("label", "") or attribute.get("id", "")
                if column_name:
                    filter_columns.add(column_name)
    
    if not filter_columns:
        return config
    
    # Get current table columns (reuse logic from fingerprint validation)
    current_attribute_names = set()
    
    if attribute_query:
        try:
            response, _ = execute_native_query(
                config, attribute_query, source_connection, is_list=True
            )
            response = response if response else []
        except Exception as e:
            response = []

        if (
            connection_type == ConnectionType.Hive.value
            or connection_type == ConnectionType.ADLS.value
            or connection_type == ConnectionType.File.value
            or connection_type == ConnectionType.S3.value
            or (
                connection_type == ConnectionType.Databricks.value
                and config.get("database_name", "") == "hive_metastore"
            )
        ):
            current_attribute_names = {value.get("col_name", "") for value in response if value.get("col_name")}
        elif connection_type == ConnectionType.Teradata.value:
            metadata = convert_to_lower(response)
            metadata = parse_teradata_columns(
                response, default_queries, "name", "datatype"
            )
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
        elif connection_type == ConnectionType.Salesforce.value:
            pg_connection = get_postgres_connection(config)
            response = agent_helper.execute_query(
                config,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name="get_attributes", query= attribute_query),
            )
            current_attribute_names = {value.get("column_name", "") for value in response if value.get("column_name")}
        elif connection_type == ConnectionType.SalesforceMarketing.value:
            pg_connection = get_postgres_connection(config)
            current_attribute_names = {value.get("columnname", "") for value in response if value.get("columnname")}
        else:
            metadata = convert_to_lower(response)
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif is_direct_query_asset:
        metadata = extract_workflow.get_selected_attributes(config)
        current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    elif (
        connection_type == ConnectionType.Databricks.value
        and config.get("type") == "TABLE"
    ):
        column_describe_query = f""" describe {table_name} ;"""
        try:
            response, _ = execute_native_query(
                config, column_describe_query, source_connection, is_list=True
            )
            df = pd.DataFrame.from_records(response)
            df["col_name"] = df["col_name"].apply(lambda x: x.upper())
            current_attribute_names = set(df["col_name"])
        except Exception as e:
            pass
    else:
        if metadata:
            current_attribute_names = {value.get("name", "") for value in metadata if value.get("name")}
    # Validate filter columns exist in current metadata
    missing_columns = filter_columns - current_attribute_names
    
    if missing_columns:
        
        # Find similar columns for suggestions
        similar_columns = {}
        for missing_col in missing_columns:
            similar = []
            missing_lower = missing_col.lower()
            for col in current_attribute_names:
                col_lower = col.lower()
                # Simple similarity check
                if missing_lower in col_lower or col_lower in missing_lower:
                    similar.append(col)
                elif len(set(missing_lower) & set(col_lower)) / max(len(missing_lower), len(col_lower)) > 0.6:
                    similar.append(col)
            if similar:
                similar_columns[missing_col] = similar
        
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                # Update core.asset table - filter data is stored here, not in core.data
                asset_query = f"""UPDATE core.asset 
                    SET filter_properties = NULL, filter_query = NULL, is_filtered = FALSE
                    WHERE id = '{asset_id}'
                """
                cursor.execute(asset_query)
                connection.commit()
        except Exception as e:
            # Try fallback approach - update only is_filtered if other columns don't exist
            try:
                connection = get_postgres_connection(config)
                with connection.cursor() as cursor:
                    query = f"""UPDATE core.asset 
                        SET is_filtered = FALSE
                        WHERE id = '{asset_id}'
                    """
                    cursor.execute(query)
                    connection.commit()
            except Exception as e2:
                pass
        
        # Update asset configuration to disable filtering
        asset.update({"is_filtered": False})
        config.update({
            "filter_properties": None,
            "filter_query": None,
            "asset": asset
        })
        
        warning_msg = f"WARNING: Filter columns {sorted(missing_columns)} not found in table schema. "
        if similar_columns:
            suggestions = []
            for missing, similar in similar_columns.items():
                suggestions.append(f"'{missing}' -> {similar}")
            warning_msg += f"Similar columns found: {', '.join(suggestions)}. "
        warning_msg += f"Disabled filtering for asset {asset_id}. System will continue without filtering."
    return config


def extract_table_metadata(config: dict, **kwargs) -> None:
    is_completed = check_task_status(config, kwargs)
    if is_completed:
        return

    source_connection = None
    # Update the asset state
    run_id = config.get("queue_id")
    asset = config.get("asset")
    asset = asset if asset else {}
    asset_properties = asset.get("properties", {})
    asset_properties = asset_properties if asset_properties else {}
    asset_database = config.get("database_name")
    asset_schema = config.get("schema")
    watermark = config.get("watermark", "")
    if not asset_database and asset_properties:
        asset_database = asset_properties.get("database")
    if not asset_schema and asset_properties:
        asset_schema = asset_properties.get("schema")
    asset_database = asset_database if asset_database else ""
    asset_schema = asset_schema if asset_schema else ""
    config.update({"database_name": asset_database, "schema": asset_schema})

    is_incremental = config.get("is_incremental", False)
    task_config = get_task_config(config, kwargs)
    # Variable in semantic tag mapping
    measure_level_queries = {}
    connection_type = config.get("connection_type")
    is_direct_query_asset = check_is_direct_query(config)

    update_queue_detail_status(
        config, ScheduleStatus.Running.value, task_config=task_config
    )
    update_queue_status(config, ScheduleStatus.Running.value, True)

    # Handle Some Jobs Before the Task Start Run
    handle_execution_before_task_run(config)

    # Get Reports Settings
    report_settings = config.get("dag_info").get("report_settings")
    report_settings = (
        json.loads(report_settings, default=str)
        if report_settings and isinstance(report_settings, str)
        else report_settings
    )
    report_settings = report_settings if report_settings else {}
    schema_name = report_settings.get("schema")
    schema_name = schema_name if schema_name else "DQLABS"
    database_name = report_settings.get("database")
    database_name = database_name if database_name else ""
    connection_type = config.get("connection_type")
    if connection_type == ConnectionType.DB2IBM.value and not database_name:
        database_name = "sample"

    primary_keys = config.get("primary_columns", [])
    default_queries = get_queries(config)
    datatypes = default_queries.get("dq_datatypes", {})
    failed_rows_queries = default_queries.get("failed_rows", {})
    failed_rows_queries = failed_rows_queries if failed_rows_queries else {}
    datatype_mappings = failed_rows_queries.get("datatypes_mapping", {})
    datatype_mappings = datatype_mappings if datatype_mappings else {}
    config.update(
        {
            "failed_rows_schema": schema_name,
            "destination_database": database_name,
            "airflow_run_id": str(kwargs.get("dag_run").run_id),
            "datatypes": datatypes,
        }
    )
    if connection_type not in [
        ConnectionType.S3Select.value,
        ConnectionType.ADLS.value,
        ConnectionType.File.value,
        ConnectionType.S3.value,
        ConnectionType.SalesforceDataCloud.value,
        ConnectionType.MongoDB.value
    ]:
        is_asset_deprecated, _ = is_deprecated(config, default_queries)
        if is_asset_deprecated:
            deprecate_asset(config)
            raise Exception(DEPRECATED_ASSET_ERROR)

    view_type = asset.get("view_type") if asset else ""
    view_type = str(view_type).lower() if view_type else "table"
    asset_type = asset.get("type") if asset else ""
    asset_type = asset_type.lower() if asset_type else ""
    asset_measures = get_measures(config, RELIABILITY, ASSET)
    asset_measures = asset_measures if asset_measures else []
    asset_measure_names = [measure.get("name", '') for measure in asset_measures if measure.get("name") is not None]

    is_query_mode = bool(asset_type.lower() == "query")
    if is_query_mode:
        source_connection = create_view(config, default_queries)

    source_connection = (
        get_native_connection(config)
        if connection_type.lower()
        not in [ConnectionType.Redshift.value, ConnectionType.Redshift_Spectrum.value]
        else None
    )
    source_connection = (
        source_connection
        if source_connection
        and connection_type.lower()
        in [ConnectionType.Snowflake.value, ConnectionType.Denodo.value]
        else None
    )

    # Creating a temp table for views or query
    create_temp_table_query = get_temp_table_query(config, default_queries)
    if create_temp_table_query:
        execute_native_query(
            config, create_temp_table_query, source_connection, no_response=True
        )

    table_name = config.get("table_name")
    has_temp_table = config.get("has_temp_table")
    if has_temp_table:
        table_name = config.get("temp_view_table_name")

    attribute_query = get_metadata_query("attributes", default_queries, config)
    if not is_query_mode:
        config = check_watermark_column(config, source_connection, default_queries, attribute_query, **kwargs)
        config = check_fingerprint_column(config, source_connection, default_queries, attribute_query, **kwargs)
        config = check_filter_columns(config, source_connection, default_queries, attribute_query, **kwargs)

    asset_query = get_metadata_query("table", default_queries, config)
    total_queries = get_metadata_query("total_queries", default_queries, config)
    primary_key_query = get_metadata_query("primary_key", default_queries, config)
    querymode_table_query = get_metadata_query("query_table", default_queries, config)
    if asset_type == "view" or is_query_mode and view_type == "view":
        asset_query = get_metadata_query("view", default_queries, config)

    row_count_query = get_metadata_query("total_rows", default_queries, config)
    if is_direct_query_asset:
        query_tables = Parser(asset.get("query")).tables
        query_table_list = []
        query_schema_list = []
        if query_tables:
            for query_table in query_tables:
                query_table = query_table.split(".")
                db_schema = ""
                # db_name = ""
                if len(query_table) == 3:
                    db_schema = (
                        query_table[1]
                        .replace('"', "")
                        .replace("'", "")
                        .replace("[", "")
                        .replace("]", "")
                    )
                query_table_list.append(query_table[-1].upper())
                query_schema_list.append(db_schema.upper())
        asset_query = None
        primary_key_query = None
        querymode_table_query = None
        attribute_query = None
        direct_query_metadata = default_queries.get("direct_query_metadata", {})
        total_queries = direct_query_metadata.get("total_rows")
        total_queries = total_queries.replace("<table_query>", asset.get("query"))
        row_count_query = total_queries
    metadata = []
    # Get Asset Metadata
    asset_metadata = get_asset_metadata(
        config,
        source_connection,
        is_query_mode=is_query_mode,
        querymode_table_query=querymode_table_query,
        asset_query=asset_query,
        connection_type=connection_type,
        measure_level_queries=measure_level_queries,
    )
    asset_metadata = asset_metadata if asset_metadata else {}
    get_row_count(
        config,
        source_connection,
        default_queries,
        row_count_query,
        asset_metadata=asset_metadata,
        connection_type=connection_type,
        measure_level_queries=measure_level_queries,
    )
    metadata = get_column_count(
        config,
        source_connection,
        default_queries,
        attribute_query,
        asset_metadata=asset_metadata,
        connection_type=connection_type,
        is_direct_query_asset=is_direct_query_asset,
        measure_level_queries=measure_level_queries,
        metadata=metadata,
    )
    if FRESHNESS in asset_measure_names:
        get_freshness(
            config,
            source_connection,
            default_queries,
            asset_metadata=asset_metadata,
            is_direct_query_asset=is_direct_query_asset,
            connection_type=connection_type,
            measure_level_queries=measure_level_queries,
            metadata=metadata,
            table_name=table_name,
            schema_name=schema_name,
        )
    if DUPLICATES in asset_measure_names:
        get_duplicate_count(
            config,
            source_connection,
            default_queries,
            asset_metadata=asset_metadata,
            connection_type=connection_type,
            is_direct_query_asset=is_direct_query_asset,
            measure_level_queries=measure_level_queries,
            metadata=metadata,
            table_name=table_name,
            primary_keys=primary_keys,
            primary_key_query=primary_key_query,
            total_queries=total_queries,
        )
    if metadata:
        update_asset_run_id(run_id, config)
        if not is_direct_query_asset:
            attributes, _ = save_attributes(config, metadata, is_direct_query_asset)
            config.update({"measure_level_queries": measure_level_queries})
            save_asset_metadata(config, asset_metadata)
            save_attribute_metadata(config, asset_metadata, metadata, attributes, is_direct_query_asset)
            update_primary_columns(config, attributes, primary_keys)
        else:
            direct_query_attributes = get_direct_query_attributes(config, default_queries, asset)
            converted_metadata = [
                {
                    "name": attr.get("column_name", "").strip(),
                    "datatype": attr.get("datatype"),
                    "is_null": False,
                    "description": attr.get("description", "")
                }
                for attr in direct_query_attributes
            ]
            if converted_metadata:
                attributes, new_inserted_attribute = save_attributes(config, converted_metadata, is_direct_query_asset)
                if direct_query_attributes:
                    asset_metadata["column_count"] = len(direct_query_attributes)
                save_asset_metadata(config, asset_metadata)
                save_attribute_metadata(config, asset_metadata, converted_metadata, attributes, is_direct_query_asset)
                update_primary_columns(config, attributes, primary_keys)

                if new_inserted_attribute:
                    connection = get_postgres_connection(config)
                    asset_id = config.get("asset_id")

                    with connection.cursor() as cursor:
                        names_str = ", ".join(f"'{attr}'" for attr in new_inserted_attribute)
                        query = f"""
                            SELECT id, name, derived_type
                            FROM core.attribute
                            WHERE asset_id = '{asset_id}'
                            AND name IN ({names_str})
                        """
                        cursor = execute_query(connection, cursor, query)
                        results = fetchall(cursor)
                        for attr in results:
                            attribute_for_measures = {
                                'id': attr['id'],
                                'derived_type': attr['derived_type']
                            }
                            create_default_measures_for_attribute(config, attribute_for_measures)

    update_scores(config)
    # Check for the alerts for the current run
    config.update({"is_asset": True, "job_type": RELIABILITY, "measure_id": None, "asset_metadata": asset_metadata})
    is_observe = config['connection']['credentials'].get('observe', False)
    if connection_type in [ConnectionType.Snowflake.value, ConnectionType.Databricks.value, ConnectionType.Hive.value] and is_observe and asset_type.lower() != "query" :
        database = asset_database
        observe_queries = default_queries.get("observe", {})
        observe_queries = observe_queries if observe_queries else {}
        if not observe_queries:
            error_message = (
                f"Observability not supported for connection type: {connection_type}"
            )
            raise ValueError(error_message)
        table_name = asset.get("name", "")
        quoted_table_names = f"'{table_name}'"

        selected_asset_schema_filter = observe_queries.get(
                "selected_asset_schema_filter"
            )
        schema_filter_query = deepcopy(selected_asset_schema_filter)
        schema_filter_query = (
            schema_filter_query.replace("<schema_name>", asset_schema)
            .replace("<selected_tables>", quoted_table_names)
            .replace("<database_name>", database)
        )
        schema_filter_query = (
            f" AND {schema_filter_query}" if schema_filter_query else ""
        )
        selected_assets = get_selected_assets(config)
        extract_metadata(
            config,
            database,
            source_connection,
            observe_queries,
            is_reliability=True,
            schema_filter_query=schema_filter_query,
            selected_assets=selected_assets,
        )
    check_alerts(config)
    # # Update the threshold value based on the drift config
    update_threshold(config, is_seasonal=True)

    # Semantic Tags Push
    if not is_incremental:
        push_tags(
            config,
            source_connection,
            default_queries,
            connection_type,
        )

    # Handle Snowflake Lineage
    if connection_type.upper() == ConnectionType.Snowflake.value.upper():
        save_snowflake_lineage(config)

    # Handle Databricks Lineage
    if connection_type.upper() == ConnectionType.Databricks.value.upper():
        save_databricks_lineage(config)

    # update request queue status
    is_triggered = config.get("is_triggered")
    if not is_triggered:
        create_queue_detail(config)
    update_queue_detail_status(config, ScheduleStatus.Completed.value)
    update_queue_status(config, ScheduleStatus.Completed.value)
    update_recent_run_alert(config)
