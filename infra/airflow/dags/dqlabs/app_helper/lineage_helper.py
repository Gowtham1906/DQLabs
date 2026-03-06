import json
import re
from uuid import uuid4
from collections import defaultdict
from dqlabs.app_helper import agent_helper
from openai import OpenAI
from textwrap import dedent

# Heplers
from dqlabs.app_helper.db_helper import (
    execute_query,
    split_queries,
    fetchone,
    split_queries,
    fetchall,
)
from dqlabs.app_helper.pipeline_helper import get_pipeline_tests, update_pipeline_run_detail_telemetry
from dqlabs.app_helper.dag_helper import execute_native_query, get_postgres_connection
from dqlabs.app_constants.dq_constants import (
    ASSET_GROUP_REPORT,
    ASSET_TYPE_PIPE,
    ASSET_TYPE_VIEW,
    ASSET_TYPE_PROCEDURE,
    ASSET_TYPE_TASK,
    ASSET_GROUP_PIPELINE
)
from dqlabs.utils.extract_workflow import get_queries, get_metadata_query
from dqlabs.utils.event_capture import save_alert_event
from dqlabs.utils.integrations.servicenow import send_servicenow_alerts

from dqlabs.tasks.workflow.actions.jira import create_issue as create_jira_issue
from dqlabs.tasks.workflow.actions.service_now import create_issue as create_servicenow_issue
# Log
from dqlabs.app_helper.log_helper import log_error, log_info

# Import Enums
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.integrations.servicenow import get_servicenow_config,  prepare_servicenow_request_data, get_servicenow_response
from dqlabs.app_helper.dq_helper import format_freshness
import os
from dqlabs.tasks.check_alerts import update_issues_status, generate_llm_based_alert_message
# from dqlabs.tasks.check_alerts import update_issues_status

def save_lineage(config: dict, type: str, lineage: dict, asset_id: str):
    """
    Save Lineage
    """
    try:
        lineage = lineage if lineage else {}
        relations = lineage.get("relations", [])
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clear Existing Lineage Details
            query_string = f"""
                delete from core.lineage
                where
                    source_asset_id = '{asset_id}' or target_asset_id = '{asset_id}'
                    and is_auto = true
            """
            cursor = execute_query(connection, cursor, query_string)

            # Save New Lineage Details
            insert_objects = []
            for relation in relations:
                query_input = (
                    uuid4(),
                    relation.get("source_connection_id") if relation.get("source_connection_id") else connection_id,
                    relation.get("source_asset_id") if relation.get("source_asset_id") else asset_id,
                    relation.get("srcTableId"),
                    relation.get("srcTableColName", ""),
                    relation.get("target_connection_id") if relation.get("target_connection_id") else connection_id,
                    relation.get("target_asset_id") if relation.get("target_asset_id") else asset_id,
                    relation.get("tgtTableId"),
                    relation.get("tgtTableColName", ""),
                    type,
                    True,
                    True,
                    False,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.lineage(
                            id, source_connection_id, source_asset_id, source_entity, source_entity_column,
                            target_connection_id, target_asset_id, target_entity, target_entity_column,
                            type, is_auto, is_active, is_delete, created_date
                        ) values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error("Lineage Insert Failed  ", e)
    except Exception as e:
        log_error(str(e), e)
        raise e


def save_lineage_entity(config: dict, tables: list, asset_id: str, is_entity_pk_id: bool = False):
    """
    Save Lineage Entity
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clear Existing Lineage Details
            query_string = f"""
                delete from core.lineage_entity where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # Save New Lineage Details
            insert_objects = []
            for table in tables:
                entity_pk_id = table.get("lineage_entity_id") if is_entity_pk_id and 'lineage_entity_id' in table else uuid4()
                # Handle properties field - ensure it's valid JSON
                properties = table.get("properties", "")
                if not properties or properties.strip() == "":
                    properties = "{}"  # Empty JSON object
                
                query_input = (
                    entity_pk_id,
                    asset_id,
                    table.get("database", ""),
                    table.get("schema",""),
                    table.get("name", ""),
                    table.get("entity_name"),
                    table.get("connection_type"),
                    json.dumps(table.get("fields", [])),
                    properties,
                    True,
                    False,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.lineage_entity(
                            id, asset_id, database, schema, name,  entity_name, connection_type,
                            columns, properties, is_active, is_delete, created_date
                        ) values {query_input}
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("Lineage Entity Insert Failed  ", e)
                    raise e
    except Exception as e:
        log_error(str(e), e)
        raise e


def save_snowflake_lineage(config: dict) -> list:
    """
    Returns the lineage information of snowflake for selected asset
    """
    try:
        connection = config.get("connection")
        connection = connection if connection else {}
        connection_config = config.get("connection").get("credentials")
        lineage_is_enabled = (
            connection_config.get(
                "lineage", None) if connection_config else None
        )
        if not lineage_is_enabled:
            return

        asset = config.get("asset", None)
        asset_type = asset.get("type").lower() if asset else "table"
        asset_id = asset.get("id")
        properties = asset.get("properties", {})
        properties = (
            json.loads(properties) if isinstance(
                properties, str) else properties
        )
        properties = properties if properties else {}
        table_name = asset.get("technical_name")
        database_name = properties.get("database")
        schema_name = properties.get("schema")
        default_queries = get_queries(config)
        lineage_query = default_queries.get("lineage", {})

        if lineage_query and asset and connection:
            if asset_type == ASSET_TYPE_VIEW:
                table_properties = {
                    "table_name": table_name,
                    "database_name": database_name,
                    "schema_name": schema_name,
                }
                query = prepare_snowflake_view_lineage_query(
                    config, default_queries, table_properties
                )
            elif asset_type == ASSET_TYPE_TASK:
                query = lineage_query.get("task")
                query = f"{query} IN SCHEMA {schema_name}"
            elif asset_type == ASSET_TYPE_PIPE:
                query = (
                    lineage_query.get("pipe")
                    .replace("<pipe_name>", table_name)
                    .replace("<database_name>", database_name)
                    .replace("<schema_name>", schema_name)
                )
            elif asset_type == ASSET_TYPE_PROCEDURE:
                query = (
                    lineage_query.get("procedure")
                    .replace("<database_name>", database_name)
                    .replace("<procedure_name>", table_name)
                )
            else:
                lineage_query = lineage_query.get("table")
                query = (
                    lineage_query.replace("<database_name>", database_name)
                    .replace("<schema_name>", schema_name)
                    .replace("<table_name>", table_name)
                )
            data, _ = execute_native_query(config, query, is_list=True)
            if data:
                if asset_type == ASSET_TYPE_TASK:
                    tables, relations = prepare_snowflake_task_lineage(
                        data, table_name, asset_id
                    )
                    tables = tables if tables else []
                elif asset_type == ASSET_TYPE_PIPE:
                    attribute_query = default_queries.get("attributes")
                    tables, relations = prepare_snowflake_pipe_lineage(
                        config, lineage_query, data, asset_id, attribute_query
                    )
                elif asset_type == ASSET_TYPE_PROCEDURE:
                    definition = data[0].get("procedure_definition")
                    tables, relations = prepare_procedure_lineage(
                        definition, asset_id, asset_id
                    )
                else:
                    tables, relations = prepare_snowflake_lineage(
                        data, asset_id)

                if relations:
                    lineage = {"tables": tables, "relations": relations}
                    save_lineage(config, "pipeline", lineage, asset_id)
                    # if asset_type != ASSET_TYPE_TASK:
                    #     map_asset_with_lineage(config, lineage, "pipeline")
                if tables:
                    save_lineage_entity(config, tables, asset_id)
    except Exception as e:
        log_error(str(e), e)


def prepare_snowflake_pipe_lineage(
    config: dict, lineage_query: dict, data: dict, asset_id: str, attribute_query: str
):
    """
    Prepare Snowflake Pipe Lineage
    """
    tables = []
    relations = []
    pipe_data = data[0]

    # Query Extraction
    query_definition = pipe_data.get("definition", "")
    table_pattern = r"COPY INTO\s+([\w.]+)"
    stage_pattern = r"FROM\s+@([\w.]+)"
    table_name = re.search(table_pattern, query_definition).group(1)
    stage_name = re.search(stage_pattern, query_definition).group(1)
    table_detail = table_name.split(".")
    database_name = pipe_data.get("pipe_catalog")
    schema_name = pipe_data.get("pipe_schema")

    # Staget Detail
    stage_query = lineage_query.get("stage", "")
    stage_query = stage_query.replace("<database_name>", database_name).replace(
        "<stage_name>", stage_name.lower()
    )
    stage_detail, _ = execute_native_query(config, stage_query)
    tables = []
    if len(table_detail) == 3:
        schema_name = table_detail[1]
        table_name = table_detail[2]
        database_name = table_detail[0]
    elif len(table_detail) == 2:
        schema_name = table_detail[0]
        table_name = table_detail[1]

    attributes = []
    if attribute_query and database_name and schema_name:
        try:
            attribute_query = (
                attribute_query.replace("<database_name>", database_name)
                .replace("<table_name>", table_name.strip())
                .replace("<schema_condition>", f"AND T.TABLE_SCHEMA = '{schema_name}'")
            )
            attribute_list, _ = execute_native_query(
                config, attribute_query, is_list=True
            )
            attribute_list = attribute_list if attribute_list else []
            for attribute in attribute_list:
                attributes.append(
                    {
                        "id": attribute.get("column_name"),
                        "name": attribute.get("column_name"),
                        "type": "column",
                        "r_column": attribute.get("column_name"),
                        "is_source": False,
                    }
                )
        except Exception as e:
            log_error("Get Pipeline Attributes failed ", e)

    # Relation creation
    stage_entity_name = f"{database_name}.{schema_name}.{stage_name}"
    table_entity_name = f"{database_name}.{schema_name}.{table_name}"
    tables = [
        {
            "database": database_name,
            "schema": schema_name,
            "name": stage_name.upper(),
            "entity_name": stage_entity_name,
            "connection_type": ConnectionType.Snowflake.value.lower(),
            "fields": [],
        },
        {
            "database": database_name,
            "schema": schema_name,
            "name": table_name.upper(),
            "entity_name": table_entity_name,
            "connection_type": ConnectionType.Snowflake.value.lower(),
            "fields": attributes,
        },
    ]
    if stage_detail:
        tables.append(
            {
                "database": stage_detail.get("state_catalog", ""),
                "schema": stage_detail.get("stage_schema"),
                "name": stage_detail.get("stage_url"),
                "entity_name": stage_detail.get("stage_url"),
                "connection_type": ConnectionType.Snowflake.value.lower(),
                "fields": [],
            }
        )
        relations = [
            {
                "srcTableId": stage_detail.get("stage_url"),
                "tgtTableId": stage_entity_name,
            },
            {"srcTableId": stage_entity_name, "tgtTableId": asset_id},
            {"srcTableId": asset_id, "tgtTableId": table_entity_name},
        ]
    else:
        relations = [
            {"srcTableId": stage_entity_name, "tgtTableId": asset_id},
            {"srcTableId": asset_id, "tgtTableId": table_entity_name},
        ]
    return tables, relations


def prepare_snowflake_task_lineage(tasks: list, task_name: str, asset_id: str):
    """
    Prepare Snowflake Task Lineage
    """
    tables = []
    relations = []
    task_list = []
    visited = set()
    stack = [task_name]
    task_links = defaultdict(set)
    task_mapping = {}
    task_name_mapping = {}

    # Prepare Task Mapping
    for task in tasks:
        task_relations = json.loads(task.get("task_relations", {})).get(
            "Predecessors", []
        )
        relation = task_relations[0] if task_relations else ""
        task.update({"relation_name": relation})
        current_task_name = (
            f"{task['database_name']}.{task['schema_name']}.{task['name']}"
        )
        task_mapping[current_task_name] = task
        task_name_mapping[task["name"]] = task
        for link in task_relations:
            short_pred = task_mapping.get(link, {}).get("name", link)
            task_links[task["name"]].add(short_pred)
            task_links[short_pred].add(task["name"])

    while stack:
        current = stack.pop()
        if current not in visited:
            visited.add(current)
            current_task = task_name_mapping.get(current, {})
            entity_name = f"{current_task.get('database_name')}.{current_task.get('schema_name')}.{current_task.get('name')}"
            current_task.update({"entity_name": entity_name})
            if current_task:
                if current_task.get("relation_name"):
                    relation_name = current_task.get("relation_name")
                    task_relation = task_mapping.get(relation_name, {})
                    current_task.update(
                        {
                            "relation_id": (
                                task_relation.get(
                                    "id") if task_relation else ""
                            )
                        }
                    )
                task_list.append(current_task)
            stack.extend(task_links[current] - visited)

    # Prepare Lineage Table, Relation
    if len(task_list) == 1:
        relations = [{"srcTableId": task_list[0].get("id"), "tgtTableId": ""}]
    else:
        for task in task_list:
            if task.get("name") != task_name:
                tables.append(
                    {
                        "database": task.get("database_name"),
                        "schema": task.get("schema_name"),
                        "name": task.get("name"),
                        "entity_name": task.get("id"),
                        "connection_type": ConnectionType.Snowflake.value.lower(),
                        "fields": [],
                    }
                )
            if task.get("relation_id"):
                relations.append(
                    {
                        "srcTableId": task.get("relation_id"),
                        "tgtTableId": task.get("id"),
                    }
                )
    return tables, relations


def prepare_snowflake_lineage(data: list, asset_id: str):
    """
    Prepare Snowflake Lineage
    """
    tables = []
    relations = []
    for table in data:
        is_source_table = table.get("is_source_table", False)
        source_uri = (
            table.get("source_name", None)
            if is_source_table
            else table.get("target_name", None)
        )
        target_uri = (
            table.get("target_name", None)
            if is_source_table
            else table.get("source_name", None)
        )
        if source_uri and target_uri:
            source_uri = source_uri.replace('"', "")
            table_details = source_uri.split(".")

            if_table_exists = next(
                (x for x in tables if x.get("id") == source_uri), None
            )
            if not if_table_exists:
                tables.append(
                    {
                        "id": source_uri,
                        "database": table_details[0] if table_details[0] else "",
                        "schema": table_details[1] if len(table_details) > 1 else "",
                        "name": (
                            table_details[2] if len(
                                table_details) > 2 else source_uri
                        ),
                        "entity_name": source_uri,
                        "isCollapse": False,
                        "is_source": is_source_table,
                        "connection_type": ConnectionType.Snowflake.value.lower(),
                        "fields": [],
                    }
                )

    for column in data:
        is_source_table = column.get("is_source_table", False)
        source_uri = (
            column.get("source_name", None)
            if is_source_table
            else column.get("target_name", None)
        )
        target_uri = (
            column.get("target_name", None)
            if is_source_table
            else column.get("source_name", None)
        )
        if source_uri and target_uri:
            source_uri = source_uri.replace('"', "")
            if_table_exists = next(
                (x for x in tables if x.get("id") == source_uri), None
            )
            if if_table_exists:
                fields = if_table_exists.get("fields", [])
                if is_source_table:
                    if_column_exists = next(
                        (
                            x
                            for x in fields
                            if x.get("id")
                            == column.get("source_column", "").replace('"', "")
                        ),
                        None,
                    )
                    if not if_column_exists:
                        if column.get("source_column"):
                            fields.append(
                                {
                                    "id": column.get("source_column", "").replace(
                                        '"', ""
                                    ),
                                    "name": column.get("source_column", "").replace(
                                        '"', ""
                                    ),
                                    "type": "column",
                                    "is_source": is_source_table,
                                    "r_column": column.get("target_column", "").replace(
                                        '"', ""
                                    ),
                                }
                            )
                            if_table_exists.update({"fields": fields})
                        relations.append(
                            {
                                "srcTableId": if_table_exists.get("id"),
                                "tgtTableId": asset_id,
                                "srcTableColName": column.get(
                                    "source_column", ""
                                ).replace('"', ""),
                                "tgtTableColName": column.get(
                                    "target_column", ""
                                ).replace('"', ""),
                            }
                        )
                else:
                    if_column_exists = next(
                        (
                            x
                            for x in fields
                            if x.get("id")
                            == column.get("target_column", "").replace('"', "")
                        ),
                        None,
                    )
                    if not if_column_exists:
                        if column.get("target_column"):
                            fields.append(
                                {
                                    "id": column.get("target_column", "").replace(
                                        '"', ""
                                    ),
                                    "name": column.get("target_column", "").replace(
                                        '"', ""
                                    ),
                                    "type": "column",
                                    "is_source": is_source_table,
                                    "r_column": column.get("source_column", "").replace(
                                        '"', ""
                                    ),
                                }
                            )
                            if_table_exists.update({"fields": fields})
                        relations.append(
                            {
                                "srcTableId": asset_id,
                                "tgtTableId": if_table_exists.get("id"),
                                "srcTableColName": column.get(
                                    "target_column", ""
                                ).replace('"', ""),
                                "tgtTableColName": column.get(
                                    "source_column", ""
                                ).replace('"', ""),
                            }
                        )

    return tables, relations


def prepare_snowflake_view_lineage_query(
    config: dict, queries: dict, table_properties: dict
):
    """
    Prepare Snowflake view lineage query
    """

    # Get Asset Properties
    table_name = table_properties.get("table_name")
    database_name = table_properties.get("database_name")
    schema_name = table_properties.get("schema_name")
    table_name = f"{database_name}.{schema_name}.{table_name}"

    # Get Attributes
    attribute_query = get_metadata_query("attributes", queries, config)
    attributes, _ = execute_native_query(config, attribute_query, is_list=True)
    lineage_query = queries.get("lineage", {}).get("view", "")
    query_list = []
    if attributes:
        for attribute in attributes:
            attribute_name = f"{table_name}.{attribute.get('name')}"
            attribute_lineage_query = lineage_query.replace(
                "<column_name>", attribute_name
            ).replace("<source_name>", table_name)
            upstream_query = attribute_lineage_query.replace(
                "<direction>", "UPSTREAM")
            downstream_query = attribute_lineage_query.replace(
                "<direction>", "DOWNSTREAM"
            )
            query_list.append(upstream_query)
            query_list.append(downstream_query)
    query = " UNION ALL ".join(query_list)
    return query


def save_reports_lineage(config: dict, lineage: dict):
    """
    Save Tableau / PowerBi Reports Lineage
    """
    try:
        connection = config.get("connection")
        connection = connection if connection else {}
        asset = config.get("asset", None)
        asset_id = asset.get("id")

        tables = lineage.get("tables", [])

        # Save Report External Tables
        external_tables = [item for item in tables if item["level"] == 2]
        for table in external_tables:
            table.update({"entity_name": table.get("table_id")})
            if config.get("connection_type", "") == ConnectionType.Sigma.value:
                table.update({"entity_name": table.get("table_name")})

        if external_tables:
            save_lineage_entity(config, external_tables, asset_id)

        # Save Report Views
        views = [item for item in tables if item["level"] == 3]
        if views:
            lineage.update({"tables": views})

        # Save Lineage
        save_lineage(config, "report", lineage, asset_id)

    except Exception as e:
        log_error(str(e), e)
        raise e


def get_asset_by_db_schema_name(config: dict, table: dict) -> list:
    try:
        db_name = table.get("database", "")
        schema_name = table.get("schema", "")
        table_name = table.get("name", "")
        connection = get_postgres_connection(config)

        condition_query = f"""
                and lower(asset.name) = lower('{table_name}')
                and lower(asset.properties->>'database') = lower('{db_name}')
            """
        if schema_name:
            condition_query = f"""
                and lower(asset.name) = lower('{table_name}')
                and lower(asset.properties->>'schema') = lower('{schema_name}')
                and lower(asset.properties->>'database') = lower('{db_name}')
            """
        with connection.cursor() as cursor:
            query_string = f"""
                select asset.id, asset.name, connection.id as connection_id
                from core.asset
                join core.data on data.asset_id = asset.id
                join core.connection on connection.id = asset.connection_id
                where asset.is_active = true and asset.is_delete = false
                {condition_query}
                order by asset.id asc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            assets = fetchall(cursor)
            return assets if assets else []
    except Exception as e:
        log_error(
            "Asset Mapping Logic : Get Asset By Database,Schema and Name Failed ", e
        )
        raise e


def save_asset_lineage_mapping(
    config: dict,
    group_type: str,
    pipeline_report_field: dict,
    assets: list,
    connection_type: bool = False,
):
    try:
        source_id = pipeline_report_field.get(
            "entity_name" if connection_type else "id", ""
        )
        if config.get("connection_type", "") == ConnectionType.Sigma.value:
            source_id = pipeline_report_field.get("name", "")
        direction = pipeline_report_field.get("type", "downstream")
        source_asset = config.get("asset", None)
        asset_lineage_input = []
        asset_group = config.get("asset", {}).get("group", "")
        if source_asset:
            source_asset_id = source_asset.get("id")
            connection = get_postgres_connection(config)
            with connection:
                with connection.cursor() as cursor:
                    for asset in assets:
                        associated_asset_id = asset.get("id")
                        check_existing_query = f"""
                                select id from core.associated_asset
                                where source_id = '{source_id}'
                                and source_asset_id = '{source_asset_id}'
                            """
                        cursor = execute_query(
                            connection, cursor, check_existing_query)
                        task_source_id = source_id
                        is_sink = True
                        asset_connection_type = config.get("connection", {}).get("type")
                        # Marking sink as false for BI connectors as pipeline_telemetry is supported for pipeline assets only
                        is_sink = True if asset_group != ASSET_GROUP_REPORT  else False
                        if asset_connection_type == ConnectionType.Airbyte.value.lower():
                            task_source_id = pipeline_report_field.get("task_id")
                            is_sink = pipeline_report_field.get("dataset_type") == "sink"
                        elif asset_connection_type == ConnectionType.ADF.value.lower():
                            task_source_id = pipeline_report_field.get("source_id")
                            is_sink = pipeline_report_field.get("dataset_type") == "sink"
                        elif asset_connection_type == ConnectionType.Coalesce.value.lower():
                            task_source_id = pipeline_report_field.get("entity_name")
                        
                        
                        update_pipeline_run_detail_telemetry(config, associated_asset_id, task_source_id, is_sink)
                        existing_map_data = fetchone(cursor)
                        if existing_map_data:
                            continue
                        
                        if group_type.lower() == "data":
                            final_source_asset_id = associated_asset_id
                            final_source_id = associated_asset_id
                            final_associated_asset_id = source_asset_id
                            final_associate_id = source_asset_id
                        else:
                            # Normal mapping
                            final_source_asset_id = source_asset_id
                            final_source_id = source_id
                            final_associated_asset_id = associated_asset_id
                            final_associate_id = associated_asset_id

                        if group_type.lower() == "data":
                            final_source_asset_id = associated_asset_id
                            final_source_id = associated_asset_id
                            final_associated_asset_id = source_asset_id
                            final_associate_id = source_asset_id
                        else:
                            # Normal mapping
                            final_source_asset_id = source_asset_id
                            final_source_id = source_id
                            final_associated_asset_id = associated_asset_id
                            final_associate_id = associated_asset_id

                        query_input = (
                            str(uuid4()),
                            final_source_asset_id,
                            final_source_id,
                            final_associated_asset_id,
                            final_associate_id,
                            True,
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        # Order: id, source_asset_id, source_id, associated_asset_id, associate_id, is_auto, modified_date, direction
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP, %s)", query_input + (direction,)
                        ).decode("utf-8")
                        asset_lineage_input.append(query_param)

                    asset_lineage_input = split_queries(asset_lineage_input)
                    for input_values in asset_lineage_input:
                        try:
                            query_input = ",".join(input_values)
                            query_string = f"""
                                    insert into core.associated_asset(
                                        id, source_asset_id, source_id, associated_asset_id, associate_id, is_auto, modified_date, direction
                                    ) values {query_input}
                                    RETURNING id
                                """
                            cursor = execute_query(
                                connection, cursor, query_string)
                        except Exception as e:
                            log_error("insert asset mapping ", e)
                            raise e

                    for asset in assets:
                        associated_asset_id = asset.get("id")                       
                        log_info(f"Associated asset ID: {associated_asset_id}")
                        try:
                            data = {
                                'source_asset': source_asset_id,
                                'associated_asset': associated_asset_id
                            }
                            update_associated_asset_descriptions(config, data)
                        except Exception as e:
                            log_error(f"Failed to update associated asset descriptions for asset {associated_asset_id}", e)
                            # Continue processing other assets even if one fails

    except Exception as e:
        log_error("Tableau Get Asset By Database,Schema and Name Failed ", e)
        raise e


def update_report_pipeline_propagations(config: dict, asset: dict) -> str:
    try:
        if asset:
            # Handling Propagation Logic
            if asset.get("group") == "pipeline":
                update_pipeline_propagations(config, asset)
            else:
                # Update Stats by Report level
                update_reports_propagations(config, asset)

    except Exception as e:
        log_error(f"Pipeline and Reports Connector - Update DQ Score", e)
        raise e


def apply_propagation_logic(
    config: dict, source_asset: dict, data: dict, return_dq_score: bool = False
):
    try:
        update_query = []

        updated_score = data.get("score") if data else None
        updated_test_score = data.get("test_score") if data else None
        updated_alerts = data.get("alerts", 0) if data else 0
        updated_issues = data.get("issues", 0) if data else 0
        updated_pipeline_alerts = data.get('pipeline_alerts', 0) if data else 0
        updated_pipeline_issues = data.get('pipeline_issues', 0) if data else 0

        updated_alerts = updated_alerts if updated_alerts else 0
        updated_issues = updated_issues if updated_issues else 0

        # Check Propagate Logic
        c_credentials = source_asset.get("credentials", None)
        if not c_credentials:
            s_connection = config.get("connection", {})
            c_credentials = s_connection.get("credentials", {})

        c_credentials = (
            json.loads(c_credentials)
            if isinstance(c_credentials, str)
            else c_credentials
        )
        propagate_score = c_credentials.get("propagate_score", "none")
        propagate_alert = c_credentials.get("propagate_alerts", "none")
        propagate_issue = c_credentials.get("propagate_issue", "none")

        # Pipeline Propogate Alerts
        propagate_alerts_failure = c_credentials.get(
            "propagate_alerts_failure", False)
        propagate_alerts_warning = c_credentials.get(
            "propagate_alerts_warning", False)

        # Pipeline Propogate Issues
        propagate_issues_warning = c_credentials.get(
            "propagate_issue_warning", False)
        propagate_issues_failure = c_credentials.get(
            "propagate_issue_failure", False)

        # Propagate Scores
        if propagate_score == "table":
            score = "null" if updated_score is None else updated_score
            update_query.append(f""" score={score} """)
        if propagate_score == "test":
            score = "null" if updated_test_score is None else updated_test_score
            update_query.append(f""" score={score} """)
        elif propagate_score == "none":
            update_query.append(f""" score = null """)

        # Propagate Alerts
        if propagate_alert == "table":
            update_query.append(f""" alerts = {updated_alerts} """)
        elif propagate_alert == 'pipeline' and (propagate_alerts_failure or propagate_alerts_warning):
            update_query.append(f""" alerts = {updated_pipeline_alerts} """)
        elif propagate_alert == "none":
            update_query.append(f""" alerts = 0 """)

        # Propagate Alerts
        if propagate_issue == "table":
            update_query.append(f""" issues = {updated_issues} """)
        elif propagate_issue == 'pipeline' and (propagate_issues_warning or propagate_issues_failure):
            update_query.append(f""" issues = {updated_pipeline_issues} """)
        elif propagate_issue == "none":
            update_query.append(f""" issues = 0 """)

        if return_dq_score == True:
            updated_score = updated_score if updated_score else 0
            return updated_score, updated_alerts, updated_issues
        else:
            return update_query
    except Exception as e:
        log_error(f"Pipeline and Reports Apply Propatation Logic Failed ", e)
        raise e


def update_pipeline_task_propagations(config: dict, asset: dict) -> str:
    try:
        source_asset_id = asset.get("id", None)
        c_credentials = asset.get("credentials", None)
        if not c_credentials:
            s_connection = config.get("connection", {})
            c_credentials = s_connection.get("credentials", {})

        c_credentials = (
            json.loads(c_credentials)
            if isinstance(c_credentials, str)
            else c_credentials
        )
        alert_propagation_type = c_credentials.get("propagate_alerts")
        connection_type = config.get("connection", {}).get("type")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clear Asset Metrics
            query_string = f"""
                update core.asset 
                set score=NULL, alerts=0, issues=0
                where asset.id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # Clear Tasks Metrics
            query_string = f"""
                update core.pipeline_tasks 
                set score=NULL, alerts=0, issues=0
                where pipeline_tasks.asset_id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # List Tasks
            query_string = f"""
                select
                    distinct lineage.target_entity, source_id, pipeline_tasks.id
                from core.pipeline_tasks
                left join core.lineage on lineage.target_entity = pipeline_tasks.source_id
                where pipeline_tasks.asset_id =  '{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_tasks = fetchall(cursor)
            pipeline_tasks = pipeline_tasks if pipeline_tasks else []

            for task in pipeline_tasks:
                # Initialize lineage_join and source_id_condition with default values
                lineage_join = (
                    "join core.lineage on lineage.source_asset_id = asset.id "
                    "and (lineage.source_entity = pipeline_tasks.source_id or lineage.target_entity = pipeline_tasks.source_id)"
                )
                source_id_condition = f"associated_asset.source_id = '{task.get('source_id')}'"
                if connection_type in [ConnectionType.Airbyte.value.lower()]:
                    lineage_query = f"""
                        select distinct target_entity
                        from core.lineage
                        where source_entity = '{task.get("source_id")}'
                        and source_asset_id = '{source_asset_id}'
                    """
                    cursor = execute_query(connection, cursor, lineage_query)
                    target_entities = [row["target_entity"] for row in fetchall(cursor)] if cursor else []
                     # Step 2: Also get source_entity values that map to those target_entities (reverse lookup)
                    source_entities_from_targets = set()
                    if target_entities:
                        reverse_lineage_query = f"""
                            select distinct source_entity
                            from core.lineage
                            where target_entity = '{task.get("source_id")}'
                            and source_asset_id = '{source_asset_id}'
                        """
                        cursor = execute_query(connection, cursor, reverse_lineage_query)
                        source_entities_from_targets = {row["source_entity"] for row in fetchall(cursor)} if cursor else set()
                    
                    # Step 3: Find associated asset IDs for those source entities and target entities
                    source_ids = set()
                    all_entities = set(target_entities) | source_entities_from_targets
                    for entity in all_entities:
                        associated_asset_query = f"""
                            select source_id
                            from core.associated_asset
                            where source_asset_id = '{source_asset_id}'
                            and source_id::text = '{entity}'
                        """
                        cursor = execute_query(connection, cursor, associated_asset_query)
                        source_ids.update([row['source_id'] for row in fetchall(cursor)] if cursor else [])
                    all_source_ids =  source_ids 
                    source_ids_list = ", ".join(f"'{sid}'" for sid in all_source_ids)

                    lineage_join = (
                        "join core.lineage on (lineage.source_asset_id = asset.id or lineage.target_asset_id = asset.id) "
                        "and (lineage.source_entity = pipeline_tasks.source_id or lineage.target_entity = pipeline_tasks.source_id)"
                    )
                    if source_ids_list:
                        source_id_condition = f"associated_asset.source_id IN ({source_ids_list})"
                if connection_type in [ConnectionType.Dbt.value.lower()]:
                    lineage_query = f"""
                        select distinct source_entity
                        from core.lineage
                        where target_entity = '{task.get("source_id")}'
                        and target_asset_id = '{source_asset_id}'
                    """
                    cursor = execute_query(connection, cursor, lineage_query)
                    source_entities = [row["source_entity"] for row in fetchall(cursor)] if cursor else []

                    # Step 2: Find associated asset IDs for those source entities
                    source_ids = set()
                    for source_entity in source_entities:
                        associated_asset_query = f"""
                            select source_id
                            from core.associated_asset
                            where source_asset_id = '{source_asset_id}'
                            and associated_asset_id::text = '{source_entity}'
                        """
                        cursor = execute_query(connection, cursor, associated_asset_query)
                        source_ids.update([row['source_id'] for row in fetchall(cursor)] if cursor else [])
                        all_source_ids = {task.get("source_id")} | source_ids
                        source_ids_list = ", ".join(f"'{sid}'" for sid in all_source_ids)

                        lineage_join = (
                            "join core.lineage on (lineage.source_asset_id = asset.id or lineage.target_asset_id = asset.id) "
                            "and (lineage.source_entity = pipeline_tasks.source_id or lineage.target_entity = pipeline_tasks.source_id)"
                        )

                        source_id_condition = f"associated_asset.source_id IN ({source_ids_list})"

                # Associated Asset Propagation
                query_string = f"""
                    select
                        avg(asset.score) as score,
                        sum(asset.alerts) as alerts,
                        sum(asset.issues) as issues
                    from core.asset
                    join core.connection on connection.id = asset.connection_id
                    where asset.group = 'data'
                    and connection.is_active = true and connection.is_delete = false
                    and asset.is_active = true and asset.is_delete = false
                    and asset.id in 
                    (   
                        select
                            distinct
                            associated_asset.associated_asset_id
                        from core.asset
                        join core.pipeline on pipeline.asset_id = asset.id
                        join core.pipeline_tasks on pipeline_tasks.asset_id = asset.id
                        {lineage_join}
                        join core.associated_asset on associated_asset.source_asset_id=asset.id
                        where asset.id = '{source_asset_id}' and {source_id_condition}
                    )
                """
                cursor = execute_query(connection, cursor, query_string)
                propation_data = fetchone(cursor)
                propation_data = propation_data if propation_data else {}

                # Tests Based Score Propagation ast Task Level
                tests_propagation_query = f"""
                    select 
                         case when failed_count=0 then 100 else ((total-failed_count)/total::float)*100 end  as score
                    from (
                        select 
                            count(*) total, 
                            sum(case when status = 'failed' then 1 else 0 end) failed_count
                        from core.pipeline_tests
                        where asset_id = '{source_asset_id}' and depends_on ?  '{task.get("source_id")}'
                    ) x
                """
                cursor = execute_query(
                    connection, cursor, tests_propagation_query)
                test_propation_data = fetchone(cursor)
                propation_data.update(
                    {
                        "test_score": (
                            test_propation_data.get("score", 0)
                            if test_propation_data
                            else 0
                        )
                    }
                )

                propogation_task_condition = f""" and metrics.task_id='{str(task.get("id"))}' """
                if connection_type in [ConnectionType.Snowflake.value.lower()]:
                    propogation_task_condition = ""

                # Pipeline Based Propagation for Task Level
                pipeline_propagation_query = f"""
                        select
                            count( distinct case when lower(metrics.drift_status) in ('high', 'medium', 'low') then  metrics.id end) as alerts,
                            count(distinct case when issues.id is not null then issues.id end) as issues
                        from core.metrics
                        left join core.issues on issues.metrics_id = metrics.id
                        where metrics.asset_id = '{source_asset_id}' 
                        {propogation_task_condition}
                    """
                cursor = execute_query(
                    connection, cursor, pipeline_propagation_query)
                pipeline_propation_data = fetchone(cursor)
                propation_data.update({
                    "pipeline_alerts": pipeline_propation_data.get('alerts', 0) if pipeline_propation_data else 0,
                    "pipeline_issues": pipeline_propation_data.get('issues', 0) if pipeline_propation_data else 0
                })

                update_query = apply_propagation_logic(
                    config, asset, propation_data)

                if update_query:
                    update_query = " , ".join(update_query)
                    query_string = f"""
                        update core.pipeline_tasks set {update_query}
                        where 
                        pipeline_tasks.asset_id = '{source_asset_id}'
                        and pipeline_tasks.id = '{task.get("id")}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

            # Update Stats By Pipeline Job Level
            query_string = f"""
                SELECT
                    ROUND(avg(pipeline_tasks.score)::numeric, 2) score,
                    sum(pipeline_tasks.alerts) alerts,
                    sum(pipeline_tasks.issues) issues
                FROM core.pipeline_tasks 
                where pipeline_tasks.asset_id = '{source_asset_id}'
                    and pipeline_tasks.is_delete = false and pipeline_tasks.is_active = true
                    and pipeline_tasks.is_selected = true
                group by pipeline_tasks.asset_id
            """
            cursor = execute_query(connection, cursor, query_string)
            propation_data = fetchone(cursor)
            propation_data = propation_data if propation_data else {}

            # Tests Based Score Propagation ast Asset Level
            tests_propagation_query = f"""
                select 
                    case when failed_count=0 then 100 else ((total-failed_count)/total::float)*100 end  as score
                from (
                    select 
                        count(*) total, 
                        sum(case when status = 'failed' then 1 else 0 end) failed_count
                    from core.pipeline_tests
                    where asset_id = '{source_asset_id}'
                ) x
            """
            cursor = execute_query(connection, cursor, tests_propagation_query)
            test_propation_data = fetchone(cursor)
            propation_data.update(
                {
                    "test_score": (
                        test_propation_data.get("score", 0)
                        if test_propation_data
                        else 0
                    )
                }
            )

            # Pipeline Based Propagation for Asset level
            pipeline_propagation_query = f"""
                    SELECT
                    sum(pipeline_tasks.alerts) alerts,
                    sum(pipeline_tasks.issues) issues
                FROM core.pipeline_tasks 
                where pipeline_tasks.asset_id = '{source_asset_id}'
                    and pipeline_tasks.is_delete = false and pipeline_tasks.is_active = true
                    and pipeline_tasks.is_selected = true
                group by pipeline_tasks.asset_id
            """
            cursor = execute_query(connection, cursor, pipeline_propagation_query)
            pipeline_propation_data = fetchone(cursor)
            propation_data.update({
                "pipeline_alerts": pipeline_propation_data.get('alerts', 0) if pipeline_propation_data else 0,
                "pipeline_issues": pipeline_propation_data.get('issues', 0) if pipeline_propation_data else 0
            })

            update_query = apply_propagation_logic(
                config, asset, propation_data)

            if update_query:
                update_query = " , ".join(update_query)
                query_string = f"""
                        update core.asset set {update_query}
                        where 
                        asset.id ='{source_asset_id}'
                    """
                cursor = execute_query(connection, cursor, query_string)
                if alert_propagation_type != 'pipeline':
                    update_metrics_table_propagations(config, source_asset_id)

    except Exception as e:
        log_error(f"Pipeline Update Propagation Stats Failed", e)
        raise e
    
    
def update_metrics_table_propagations(config: dict, source_asset_id: str):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            WITH issues AS (SELECT id FROM core.issues WHERE asset_id = '{source_asset_id}' and measure_id is null),
                 metrics AS (SELECT id FROM core.metrics WHERE asset_id = '{source_asset_id}' and measure_id is null),
                 delete_events AS (DELETE FROM core.events WHERE issue_id IN (SELECT id FROM issues)),
                 delete_notification_logs AS (DELETE FROM core.notification_logs WHERE issues_id IN (SELECT id FROM issues)),
                 delete_issue_comments AS (DELETE FROM core.issue_comments WHERE issue_id IN (SELECT id FROM issues)),
                 delete_user_mapping AS (DELETE FROM core.user_mapping WHERE issue_id IN (SELECT id FROM issues)),
                 delete_issue_attachment AS (DELETE FROM core.issue_attachment WHERE issue_id IN (SELECT id FROM issues)),
                 delete_issue_work_log AS (DELETE FROM core.issue_work_log WHERE issue_id IN (SELECT id FROM issues)),
                 delete_issue_watch AS (DELETE FROM core.issue_watch WHERE issue_id IN (SELECT id FROM issues)),
                 delete_associated_issues AS (DELETE FROM core.associated_issues WHERE issue_id IN (SELECT id FROM issues)),
                 delete_issues AS (DELETE FROM core.issues WHERE id IN (SELECT id FROM issues)),
                 delete_events_metrics AS (DELETE FROM core.events WHERE metrics_id IN (SELECT id FROM metrics)),
                 delete_notification_logs_metrics AS (DELETE FROM core.notification_logs WHERE metric_history_id IN (SELECT id FROM metrics)),
                 delete_issues_metrics AS (DELETE FROM core.issues WHERE metrics_id IN (SELECT id FROM metrics)),
                 delete_associated_issues_metrics AS (DELETE FROM core.associated_issues WHERE metrics_id IN (SELECT id FROM metrics))
            DELETE FROM core.metrics WHERE id IN (SELECT id FROM metrics);
        """
        cursor = execute_query(connection, cursor, query_string)




def update_pipeline_transformation_propagations(config: dict, asset: dict) -> str:
    try:
        source_asset_id = asset.get("id", None)
        c_credentials = asset.get("credentials", None)
        if not c_credentials:
            s_connection = config.get("connection", {})
            c_credentials = s_connection.get("credentials", {})

        c_credentials = (
            json.loads(c_credentials)
            if isinstance(c_credentials, str)
            else c_credentials
        )
        alert_propagation_type = c_credentials.get("propagate_alerts")
        alert_propagation_type = (
            str(alert_propagation_type).lower() if alert_propagation_type else ""
        )
        is_failure = c_credentials.get("propagate_alerts_failure")
        is_warning = c_credentials.get("propagate_alerts_warning")
        connection_type = config.get("connection", {}).get("type")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clear Asset Metrics
            query_string = f"""
                update core.asset 
                set score=NULL, alerts=0, issues=0
                where asset.id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # Clear Tasks Metrics
            query_string = f"""
                update core.pipeline_transformations
                set score=NULL, alerts=0, issues=0
                where pipeline_transformations.asset_id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # List Transformations
            query_string = f"""
                select
                    id, source_id
                from core.pipeline_transformations
                where pipeline_transformations.asset_id = '{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_transformations = fetchall(cursor)

            for transformation in pipeline_transformations:
                query_string = f"""
                    select
                        asset.score,
                        asset.alerts,
                        asset.issues
                    from core.associated_asset
                    join core.asset on asset.id = associated_asset.associated_asset_id
                    join core.connection on connection.id = asset.connection_id
                    where asset.group = 'data'
                        and connection.is_active = true and connection.is_delete = false
                        and asset.is_active = true and asset.is_delete = false
                        and associated_asset.source_asset_id = '{source_asset_id}'
                        and associated_asset.source_id = '{transformation.get("source_id")}'
                """
                cursor = execute_query(connection, cursor, query_string)
                propation_data = fetchone(cursor)
                propation_data = propation_data if propation_data else {}

                update_query = apply_propagation_logic(
                    config, asset, propation_data)
                if alert_propagation_type == "pipeline" and (is_failure or is_warning) and connection_type in [ConnectionType.Snowflake.value.lower()]:
                    pipeline_propagation_query = f"""
                            select
                                count( distinct case when lower(metrics.drift_status) in ('high', 'medium', 'low') then  metrics.id end) as alerts,
                                count(distinct case when issues.id is not null then issues.id end) as issues
                            from core.metrics
                            left join core.issues on issues.metrics_id = metrics.id
                            where metrics.asset_id = '{source_asset_id}' 
                        """
                    cursor = execute_query(
                        connection, cursor, pipeline_propagation_query)
                    pipeline_propation_data = fetchone(cursor)
                    propation_data.update({
                        "pipeline_alerts": pipeline_propation_data.get('alerts', 0) if pipeline_propation_data else 0,
                        "pipeline_issues": pipeline_propation_data.get('issues', 0) if pipeline_propation_data else 0
                    })

                    update_query = apply_propagation_logic(
                        config, asset, propation_data)
                update_query = " , ".join(update_query)
                query_string = f"""
                    update core.pipeline_transformations set {update_query}
                    where 
                    pipeline_transformations.asset_id = '{source_asset_id}'
                    and pipeline_transformations.id = '{transformation.get("id")}'
                """
                cursor = execute_query(connection, cursor, query_string)

            # Update Stats By Pipeline Job Level
            query_string = f"""
                SELECT
                    ROUND(avg(pipeline_transformations.score)::numeric, 2) score,
                    sum(pipeline_transformations.alerts) alerts,
                    sum(pipeline_transformations.issues) issues
                FROM core.pipeline_transformations 
                where pipeline_transformations.asset_id = '{source_asset_id}'
                    and pipeline_transformations.is_delete = false and pipeline_transformations.is_active = true
                group by pipeline_transformations.asset_id
            """
            cursor = execute_query(connection, cursor, query_string)
            propation_data_query = fetchone(cursor)
            if alert_propagation_type == "pipeline" and (is_failure or is_warning) and connection_type in [ConnectionType.Snowflake.value.lower()]:
                propation_data_query = propation_data_query if propation_data_query else {}
                propation_data.update({
                        "pipeline_alerts": propation_data_query.get('alerts', 0) if propation_data_query else 0,
                        "pipeline_issues": propation_data_query.get('issues', 0) if propation_data_query else 0
                    })
                update_query = apply_propagation_logic(
                config, asset, propation_data)
            else:
                 update_query = apply_propagation_logic(
                config, asset, propation_data_query)
            if update_query:
                update_query = " , ".join(update_query)
                query_string = f"""
                        update core.asset set {update_query}
                        where 
                        asset.id ='{source_asset_id}'
                    """
                cursor = execute_query(connection, cursor, query_string)
                if alert_propagation_type !='pipeline':
                    update_metrics_table_propagations(config, source_asset_id)

    except Exception as e:
        log_error(f"Pipeline Update Propagation Stats Failed", e)
        raise e


def update_pipeline_pipe_propagations(config: dict, asset: dict) -> str:
    try:
        source_asset_id = asset.get("id", None)
        c_credentials = asset.get("credentials", None)
        if not c_credentials:
            s_connection = config.get("connection", {})
            c_credentials = s_connection.get("credentials", {})

        c_credentials = (
            json.loads(c_credentials)
            if isinstance(c_credentials, str)
            else c_credentials
        )
        alert_propagation_type = c_credentials.get("propagate_alerts")
        alert_propagation_type = (
            str(alert_propagation_type).lower() if alert_propagation_type else ""
        )
        is_failure = c_credentials.get("propagate_alerts_failure")
        is_warning = c_credentials.get("propagate_alerts_warning")
        connection_type = config.get("connection", {}).get("type")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clear Asset Metrics
            query_string = f"""
                update core.asset 
                set score=NULL, alerts=0, issues=0
                where asset.id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            query_string = f"""
                select
                    avg(asset.score) as score,
                    sum(asset.alerts) as alerts,
                    sum(asset.issues) as issues
                from core.associated_asset
                join core.asset on asset.id = associated_asset.associated_asset_id
                join core.connection on connection.id = asset.connection_id
                where asset.group = 'data'
                    and connection.is_active = true and connection.is_delete = false
                    and asset.is_active = true and asset.is_delete = false
                    and associated_asset.source_asset_id = '{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            propation_data = fetchone(cursor)
            propation_data = propation_data if propation_data else {}

            update_query = apply_propagation_logic(
                config, asset, propation_data)
            if alert_propagation_type == "pipeline" and (is_failure or is_warning) and connection_type in [ConnectionType.Snowflake.value.lower(), ConnectionType.Databricks.value.lower(), ConnectionType.ADF.value.lower(), ConnectionType.Synapse.value.lower(),ConnectionType.Coalesce.value.lower(), ConnectionType.SalesforceDataCloud.value.lower(), ConnectionType.Hive.value.lower(), ConnectionType.SalesforceMarketing.value.lower(), ConnectionType.Airbyte.value.lower()]:
                pipeline_propagation_query = f"""
                            select
                                count( distinct case when lower(metrics.drift_status) in ('high', 'medium', 'low') then  metrics.id end) as alerts,
                                count(distinct case when issues.id is not null then issues.id end) as issues
                            from core.metrics
                            left join core.issues on issues.metrics_id = metrics.id
                            where metrics.asset_id = '{source_asset_id}' 
                        """
                cursor = execute_query(
                    connection, cursor, pipeline_propagation_query)
                pipeline_propation_data = fetchone(cursor)
                propation_data.update({
                    "pipeline_alerts": pipeline_propation_data.get('alerts', 0) if pipeline_propation_data else 0,
                    "pipeline_issues": pipeline_propation_data.get('issues', 0) if pipeline_propation_data else 0
                })
                update_query = apply_propagation_logic(
                        config, asset, propation_data)
                
            if connection_type == ConnectionType.Coalesce.value:
                  # Tests Based Score Propagation ast Asset Level
                tests_propagation_query = f"""
                    select 
                        case when failed_count=0 then 100 else ((total-failed_count)/total::float)*100 end  as score
                    from (
                        select 
                            count(*) total, 
                            sum(case when status = 'failed' then 1 else 0 end) failed_count
                        from core.pipeline_tests
                        where asset_id = '{source_asset_id}'
                    ) x
                """
                cursor = execute_query(connection, cursor, tests_propagation_query)
                test_propation_data = fetchone(cursor)
                propation_data.update(
                    {
                        "test_score": (
                            test_propation_data.get("score", 0)
                            if test_propation_data
                            else 0
                        )
                    }
                )
                update_query = apply_propagation_logic(
                        config, asset, propation_data)


            if update_query:
                update_query = " , ".join(update_query)
                query_string = f"""
                    update core.asset set {update_query}
                    where 
                    asset.id ='{source_asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                if alert_propagation_type !='pipeline':
                    update_metrics_table_propagations(config, source_asset_id)

    except Exception as e:
        log_error(f"Pipeline Update Propagation Stats Failed", e)
        raise e


def update_pipeline_propagations(config: dict, asset: dict, connection_type: str = None) -> str:
    try:
        asset_type = asset.get("type").lower()
        
        # Check if task-level alerts are enabled for ADF
        c_credentials = asset.get("credentials", None)
        if not c_credentials:
            s_connection = config.get("connection", {})
            c_credentials = s_connection.get("credentials", {})
        
        c_credentials = (
            json.loads(c_credentials)
            if isinstance(c_credentials, str)
            else c_credentials
        )
        propagate_alerts = c_credentials.get("propagate_alerts", "table")
        propagate_alerts_failure = c_credentials.get("propagate_alerts_failure", False)
        propagate_alerts_warning = c_credentials.get("propagate_alerts_warning", False)
        
        # For ADF connections, if task-level alerts are enabled, use task propagation
        if (asset_type == ASSET_GROUP_PIPELINE and 
            (connection_type == ConnectionType.ADF.value.lower() or connection_type == ConnectionType.SalesforceDataCloud.value.lower()) and
            propagate_alerts == "pipeline" and 
            (propagate_alerts_failure or propagate_alerts_warning)):
            update_pipeline_task_propagations(config, asset)
            update_pipeline_pipe_propagations(config, asset)
        elif asset_type == ASSET_TYPE_PROCEDURE:
            update_pipeline_transformation_propagations(config, asset)
        elif asset_type == ASSET_TYPE_PIPE:
            update_pipeline_pipe_propagations(config, asset)
        elif (asset_type == ASSET_GROUP_PIPELINE) and (connection_type and connection_type in [ConnectionType.Databricks.value.lower(), ConnectionType.ADF.value.lower(), ConnectionType.Synapse.value.lower(), ConnectionType.SalesforceDataCloud.value.lower(), ConnectionType.SalesforceMarketing.value.lower()]):
            update_pipeline_pipe_propagations(config, asset)
        else:
            update_pipeline_task_propagations(config, asset)

    except Exception as e:
        log_error(f"Pipeline Update Propagation Stats Failed", e)
        raise e

def generate_reports_score(config: dict, connection_type: str, asset_id: str = None, worksheet_id: str = None):
        """
        Get the average score of all attributes.
        Returns a single average score value for the report or reports_views.
        """
        type_condition = ''
        asset = None
        if asset_id:
            if asset and asset.connection and asset.connection.type == ConnectionType.Tableau.value:
                type_condition = "and reports_views.type = 'Sheet'" if asset_id else ""

        worksheet_filter = f"where reports_views.id = '{worksheet_id}'" if worksheet_id else ""
        asset_filter = f"where reports_columns.asset_id = '{asset_id}'" if asset_id else ""

        asset_filter_condition = ""
        asset_check_condition = ""

        if connection_type == ConnectionType.Sigma.value:
            asset_filter_condition = f"""
                        AND (
                            LOWER(COALESCE(assoc_ast.technical_name, assoc_ast.name, assoc_ast.properties ->> 'name', '')) = LOWER(COALESCE(reports_columns.table_name, ''))
                            OR LOWER(COALESCE(assoc_ast.name, '')) = LOWER(COALESCE(reports_columns.table_name, ''))
                        )
                        AND (
                            reports_columns.schema IS NULL
                            OR LOWER(COALESCE(assoc_ast.properties ->> 'schema', '')) = LOWER(reports_columns.schema)
                        )
                        AND (
                            reports_columns.database IS NULL
                            OR LOWER(COALESCE(assoc_ast.properties ->> 'database', '')) = LOWER(reports_columns.database)
                        )
            """
            asset_check_condition = f"""
                JOIN core.asset assoc_ast ON assoc_ast.id = aa.associated_asset_id
            """

        query_string = f"""
            with attributes as (
                select
                    -- Get the specific attribute score for this reports_column
                    COALESCE((
                        SELECT attr.score
                        FROM core.associated_asset aa
                        {asset_check_condition}
                        JOIN core.attribute attr ON aa.associate_id::uuid = attr.asset_id
                        WHERE aa.source_asset_id = reports_columns.asset_id
                        AND attr.name ilike reports_columns.name
                        {asset_filter_condition}
                        LIMIT 1
                    ), (
                        SELECT AVG(attr.score)
                        FROM jsonb_to_recordset((reports_columns.report_column_properties -> 'used_attributes')::jsonb) as ua(name text)
                        JOIN core.attribute attr ON attr.name = ua.name
                        JOIN core.associated_asset aa ON aa.associate_id::uuid = attr.asset_id
                        WHERE aa.source_asset_id = reports_columns.asset_id
                    )) as score
                from core.reports_columns
                join core.reports_views on reports_views.id = reports_columns.report_view_id {type_condition}
                {asset_filter} {worksheet_filter}
            )
            select 
                ROUND(AVG(score)::decimal, 2) as average_score
            from attributes
            where score is not null
        """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor.execute(query_string)
            response = fetchone(cursor)
        
        return response.get("average_score") if response else 0


def update_reports_propagations(config: dict, asset: dict) -> str:
    try:
        source_asset_id = asset.get("id", None)
        connection_type = config.get("connection", {}).get("type")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Clear Asset Metrics
            query_string = f"""
                update core.asset 
                set score=NULL, alerts=0, issues=0
                where asset.id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # Clear Views Metrics
            query_string = f"""
                update core.reports_views 
                set score=NULL, alerts=0, issues=0
                where reports_views.asset_id ='{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            # List Views
            query_string = f"""
                select 
                    distinct 
                    reports_views.id,
                    reports_views.source_id
                from core.reports_views
                join core.lineage on lineage.target_entity = reports_views.source_id
                where reports_views.asset_id = '{source_asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            reports_views = fetchall(cursor)

            for view in reports_views:

                query_string = f"""
                    select
                        sum(asset.alerts) as alerts,
                        sum(asset.issues) as issues
                    from core.asset
                    join core.connection on connection.id = asset.connection_id
                    where asset.group = 'data'
                    and connection.is_active = true and connection.is_delete = false
                    and asset.is_active = true and asset.is_delete = false
                    and asset.id in 
                    (   select
                                associated_asset.associated_asset_id
                        from core.asset
                        join core.reports on reports.asset_id = asset.id
                        join core.reports_views on reports_views.asset_id = asset.id
                        join core.lineage on lineage.source_asset_id = asset.id and lineage.target_entity = reports_views.source_id
                        join core.associated_asset on associated_asset.source_asset_id=asset.id 
                        and associated_asset.source_id = lineage.source_entity
                        where asset.id = '{source_asset_id}' and reports_views.id = '{view.get("id")}'
                    )
                """

                cursor = execute_query(connection, cursor, query_string)
                propation_data = fetchone(cursor)
                if propation_data:
                    propation_data.update({
                        "score": generate_reports_score(config, connection_type, worksheet_id=view.get("id"))
                    })
                    update_query = apply_propagation_logic(
                        config, asset, propation_data)

                    if update_query:
                        update_query = " , ".join(update_query)
                        query_string = f"""
                            update core.reports_views set {update_query}
                            where 
                            reports_views.id ='{view.get("id")}'
                        """
                        cursor = execute_query(connection, cursor, query_string)

                # Update Stats By Reports Job Level
                query_string = f"""
                    select
                        sum(asset.alerts) as alerts,
                        sum(asset.issues) as issues
                    from core.associated_asset
                    join core.asset on asset.id = associated_asset.associated_asset_id
                    join core.connection on connection.id = asset.connection_id
                    where asset.group = 'data'
                        and connection.is_active = true and connection.is_delete = false
                        and asset.is_active = true and asset.is_delete = false
                        and associated_asset.source_asset_id = '{source_asset_id}'
                    group by associated_asset.source_asset_id
                """
                cursor = execute_query(connection, cursor, query_string)
                propation_data = fetchone(cursor)
                if propation_data:
                    propation_data.update({
                        "score": generate_reports_score(config, connection_type, asset_id=source_asset_id)
                    })
                    update_query = apply_propagation_logic(
                        config, asset, propation_data)

                    if update_query:
                        update_query = " , ".join(update_query)
                        query_string = f"""
                                update core.asset set {update_query}
                                where 
                                asset.id ='{source_asset_id}'
                            """
                        cursor = execute_query(connection, cursor, query_string)

    except Exception as e:
        log_error(f"Reports Update Propagation Stats Failed", e)
        raise e


def clean_column_names(columns):
    """
    Cleans up extracted column strings by removing unwanted characters,
    SQL operators, and placeholders.
    """
    clean_columns = []
    for col in columns:
        col = re.sub(r"--.*", "", col)
        col = re.sub(r"=.*", "", col)  # Remove everything after `=`
        col = re.sub(r"\s*\+\s*\?", "", col)  # Remove `+ ?` placeholders
        col = col.strip()  # Trim whitespace
        if col.upper() not in ["WHERE", "SET", "RETURNING", "AND", "OR"] and col:
            clean_columns.append(col)
    return clean_columns


def extract_procedure_query(sql):
    target_info = []
    source_info = []
    column_lineage = []
    try:
        # Patterns for identifying tables and columns
        target_table_pattern = r"(INSERT INTO|UPDATE)\s+([a-zA-Z0-9_.]+)"
        source_table_pattern = r"(FROM|JOIN)\s+([a-zA-Z0-9_.]+)(?:\s+([a-zA-Z0-9_]+))?"
        insert_column_pattern = r"INSERT INTO\s+[a-zA-Z0-9_.]+\s*\(\s*([\s\S]+?)\s*\)"
        update_column_pattern = r"SET\s+([\s\S]*?)(\s+WHERE|\s*$)"
        select_column_pattern = r"SELECT\s+([\s\S]+?)\s+FROM"

        # Extract target tables
        target_tables = re.findall(target_table_pattern, sql, re.IGNORECASE)
        insert_columns = re.findall(insert_column_pattern, sql, re.IGNORECASE)
        update_clause_match = re.search(
            update_column_pattern, sql, re.IGNORECASE)

        # Extract source tables and columns
        source_tables = re.findall(source_table_pattern, sql, re.IGNORECASE)
        select_columns = re.findall(select_column_pattern, sql, re.IGNORECASE)

        # Initialize final results
        target_info = []
        source_info = []

        # Process target tables
        for i, match in enumerate(target_tables):
            table_name = match[1]
            table_data = {"table": table_name, "columns": []}
            if insert_columns and i < len(insert_columns):
                table_data["columns"] = clean_column_names(
                    insert_columns[i].split(","))
            target_info.append(table_data)

        # Process update columns
        if update_clause_match:
            table_name = re.search(
                r"UPDATE\s+([a-zA-Z0-9_.]+)", sql, re.IGNORECASE
            ).group(1)
            set_clause = update_clause_match.group(1)
            columns = re.findall(r"([a-zA-Z0-9_]+)\s*=", set_clause)
            columns = clean_column_names(columns)
            table_exists = False
            for target in target_info:
                if target["table"] == table_name:
                    table_exists = True
                    target["columns"] = list(
                        set(target["columns"]).union(columns))
                    break
            if not table_exists:
                target_info.append({"table": table_name, "columns": columns})

        # Process source tables and columns
        alias_map = {}
        for match in source_tables:
            table_name = match[1]
            alias = match[2] if len(match) > 2 else None
            source_info.append({"table": table_name, "columns": []})
            if alias:
                alias_map[alias] = table_name

        source_column_list = []
        for match in select_columns:
            source_column_list.extend(clean_column_names(match.split(",")))

        # Map source columns to the correct source table using aliases
        for col in source_column_list:
            if col == "*":
                continue
            if "." in col:
                alias, column = col.split(".")
                table_name = alias_map.get(alias, alias)
                for source in source_info:
                    if source["table"] == table_name:
                        if column not in source["columns"]:
                            source["columns"].append(column)
                        break
            else:
                if source_info and col not in source_info[0]["columns"]:
                    source_info[0]["columns"].append(col)

        # Map columns based on the query
        select_clause = re.search(select_column_pattern, sql, re.IGNORECASE)
        if select_clause:
            select_columns = clean_column_names(
                select_clause.group(1).split(","))
            for target in target_info:
                for i, target_col in enumerate(target["columns"]):
                    if i < len(select_columns):
                        source_col = select_columns[i]
                        if source_col == "*":
                            continue
                        if "." in source_col:
                            source_table, source_column = source_col.split(".")
                            source_table = alias_map.get(
                                source_table, source_table)
                        else:
                            source_table = source_info[0]["table"]
                            source_column = source_col
                        column_lineage.append(
                            {
                                "source_name": source_table,
                                "source_column": source_column,
                                "target_name": target["table"],
                                "target_column": target_col,
                            }
                        )

    except Exception as e:
        log_error("Procedure Lineage extraction failed", e)
    finally:
        return {
            "target_info": target_info,
            "source_info": source_info,
            "column_lineage": column_lineage,
        }


def prepare_procedure_lineage(definition: str, asset_id: str, asset_name: str):
    """
    Prepares the lineage information for a given procedure definition.
    """

    lineage_data = extract_procedure_query(definition)
    target_info = lineage_data.get("target_info", [])
    source_info = lineage_data.get("source_info", [])
    column_lineage = lineage_data.get("column_lineage", [])

    tables = prepare_tables(target_info, source_info)
    lineage_columns = prepare_lineage_columns(
        column_lineage, source_info, target_info, asset_name)
    relations = prepare_relations(lineage_columns, tables, asset_id)
    return tables, relations


def prepare_tables(target_info, source_info):
    """
    Prepare Lineage Tables For Using Target and Source Tables
    """
    tables = []
    for target_table in target_info:
        existing_table = next((x for x in tables if x.get(
            "id") == target_table.get("table")), None)
        if not existing_table:
            table_details = target_table.get("table").split(".")
            tables.append({
                "id": target_table.get("table"),
                "database": table_details[0] if len(table_details) > 2 else "",
                "schema": table_details[1] if len(table_details) > 1 else "",
                "name": table_details[2] if len(table_details) > 2 else target_table.get("table"),
                "entity_name": target_table.get("table"),
                "connection_type": ConnectionType.Snowflake.value.lower(),
                "fields": [],
            })
    for source_table in source_info:
        existing_table = next((x for x in tables if x.get(
            "id") == source_table.get("table")), None)
        if not existing_table:
            table_details = source_table.get("table").split(".")
            tables.append({
                "id": source_table.get("table"),
                "database": table_details[0] if len(table_details) > 2 else "",
                "schema": table_details[1] if len(table_details) > 1 else "",
                "name": table_details[2] if len(table_details) > 2 else source_table.get("table"),
                "entity_name": source_table.get("table"),
                "connection_type": ConnectionType.Snowflake.value.lower(),
                "fields": [],
            })
    return tables


def prepare_lineage_columns(column_lineage, source_info, target_info, asset_name):
    """
    Prepare Lineage Columns
    """
    lineage_columns = []
    if column_lineage:
        source_tables = source_info if source_info else target_info
        for table in source_tables:
            column_lineage.append({
                "source_name": asset_name,
                "source_column": "",
                "target_name": table.get("table"),
                "target_column": "",
                "is_source_table": True
            })
        lineage_columns = column_lineage
    elif source_info and target_info:
        if all(not src.get("columns") for src in source_info) and all(not tgt.get("columns") for tgt in target_info):
            for target in target_info:
                target_table = target["table"]
                for source in source_info:
                    source_table = source["table"]
                    if not lineage_columns:
                        lineage_columns.append({
                            "source_name": asset_name,
                            "source_column": "",
                            "target_name": source_table,
                            "target_column": "",
                            "is_source_table": True
                        })
                    lineage_columns.append({
                        "source_name": source_table,
                        "source_column": "",
                        "target_name": target_table,
                        "target_column": ""
                    })
        else:
            for target in target_info:
                target_table = target["table"]
                target_columns = target["columns"]
                for source in source_info:
                    source_table = source["table"]
                    source_columns = source["columns"]
                    if not lineage_columns:
                        lineage_columns.append({
                            "source_name": asset_name,
                            "source_column": "",
                            "target_name": source_table,
                            "target_column": "",
                            "is_source_table": True
                        })
                    max_columns = max(len(source_columns), len(target_columns))
                    for i in range(max_columns):
                        src_col = source_columns[i] if i < len(
                            source_columns) else ""
                        tgt_col = target_columns[i] if i < len(
                            target_columns) else ""
                        lineage_columns.append({
                            "source_name": source_table,
                            "source_column": src_col,
                            "target_name": target_table,
                            "target_column": tgt_col
                        })
    elif source_info and not target_info:
        for source in source_info:
            source_table = source["table"]
            source_columns = source["columns"]
            if not lineage_columns:
                lineage_columns.append({
                    "source_name": asset_name,
                    "source_column": "",
                    "target_name": source_table,
                    "target_column": "",
                    "is_source_table": True
                })
            for src_col in source_columns:
                lineage_columns.append(
                    {
                        "source_name": source_table,
                        "source_column": src_col,
                        "target_name": "",
                        "target_column": "",
                    }
                )
    elif not source_info and target_info:
        for target in target_info:
            target_table = target["table"]
            target_columns = target["columns"]
            if not lineage_columns:
                lineage_columns.append(
                    {
                        "source_name": asset_name,
                        "source_column": "",
                        "target_name": target_table,
                        "target_column": "",
                        "is_source_table": True,
                    }
                )
            for tgt_col in target_columns:
                lineage_columns.append({
                    "source_name": "",
                    "source_column": "",
                    "target_name": target_table,
                    "target_column": tgt_col
                })
    return lineage_columns


def prepare_relations(lineage_columns, tables, asset_id):
    """
    Prepare Relations
    """
    relations = []
    if lineage_columns:
        for column in lineage_columns:
            source_name = column.get("source_name")
            source_column_name = column.get("source_column")
            target_name = column.get("target_name")
            target_column_name = column.get("target_column")
            relations.append({
                "srcTableId": source_name if source_name else asset_id,
                "srcTableColName": source_column_name,
                "tgtTableId": target_name,
                "tgtTableColName": target_column_name
            })
            if target_column_name:
                target_table = next(
                    (x for x in tables if x.get("id") == target_name), None)
                if target_table:
                    fields = target_table.get("fields", [])
                    existing_field = next(
                        (x for x in fields if x.get("id") == target_column_name), None)
                    if not existing_field:
                        fields.append(
                            {
                                "id": target_column_name,
                                "name": target_column_name,
                                "type": "column",
                                "is_source": False,
                                "r_column": source_column_name,
                            }
                        )
                    target_table["fields"] = fields
            if source_column_name:
                source_table = next(
                    (x for x in tables if x.get("id") == source_name), None)
                if source_table:
                    fields = source_table.get("fields", [])
                    existing_field = next(
                        (x for x in fields if x.get("id") == source_column_name), None)
                    if not existing_field:
                        fields.append({
                            "id": source_column_name,
                            "name": source_column_name,
                            "type": "column",
                            "is_source": True,
                            "r_column": target_column_name
                        })
                    source_table['fields'] = fields
    return relations


def map_asset_with_lineage(config: dict, lineage: dict, group: str):
    try:
        tables = lineage.get("tables", [])
        tables = [item for item in tables if item.get("level", 0) == 2]
        for table in tables:
            db_name = table.get("database", "")
            table_name = table.get("name", "")
            schema_name = table.get("schema", "")
            if group == "report" and db_name and table_name:
                assets = get_asset_by_db_schema_name(config, table)
                if assets and len(assets) > 0:
                    save_asset_lineage_mapping(config, "report", table, assets)
            elif db_name and table_name and schema_name:
                assets = get_asset_by_db_schema_name(config, table)
                if assets and len(assets) > 0:
                    save_asset_lineage_mapping(config, group, table, assets)

    except Exception as e:
        log_error("Lineage Failed ", e)
        raise e


def update_report_last_runs(config: dict):
    """
    Update the last runs for the asset based on run history
    """
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            WITH run_history AS (
                SELECT
                    temp.asset_id AS run_history_id,
                    jsonb_agg(
                        jsonb_build_object(
                            'last_run_date', temp.modified_date,
                            'status', temp.status
                        ) ORDER BY temp.modified_date DESC
                    ) AS last_runs
                FROM (
                    SELECT
                        rqd.modified_date,
                        rqd.status,
                        rq.asset_id,
                        row_number() OVER (PARTITION BY rq.asset_id ORDER BY rqd.modified_date DESC) AS rn
                    FROM core.request_queue_detail rqd
                    JOIN core.request_queue rq
                        ON rqd.queue_id = rq.id
                    WHERE rq.asset_id = '{asset_id}'
                ) AS temp
                WHERE temp.rn <= 7
                GROUP BY temp.asset_id
            )
            UPDATE core.asset a
            SET last_runs = rh.last_runs
            FROM run_history rh
            WHERE a.id = rh.run_history_id
            AND a.id = '{asset_id}';
        """
        try:
            cursor = execute_query(connection, cursor, query_string)
        except Exception as e:
            log_error("update last runs for asset", e)
            connection.rollback()
            raise e


def handle_alerts_issues_propagation(config: dict, run_id: str, execute_state_pipe: str = None):

    # Check Propagate Logic
    asset = config.get("asset", {})
    asset_id = asset.get("id")

    c_credentials = asset.get("credentials", None)
    if not c_credentials:
        s_connection = config.get("connection", {})
        c_credentials = s_connection.get("credentials", {})

    c_credentials = (
        json.loads(c_credentials)
        if isinstance(c_credentials, str)
        else c_credentials
    )

    alert_propagation_type = c_credentials.get("propagate_alerts")
    alert_propagation_type = (
        str(alert_propagation_type).lower() if alert_propagation_type else ""
    )
    is_failure = c_credentials.get("propagate_alerts_failure")
    is_warning = c_credentials.get("propagate_alerts_warning")
    if alert_propagation_type == "pipeline" and (is_failure or is_warning):
        create_progations_alerts(
            config,
            c_credentials,
            asset_id,
            run_id,
            execute_state_pipe
        )
    update_issues_status(config)

def generate_sql_error_suggestions(task_data: dict):
        """
        Generate AI-powered suggestions for fixing SQL compilation errors.
        
        Args:
            error_data (dict): Contains error message and compiled SQL code
            sentence_limit (int): Maximum number of sentences in the response
            
        Returns:
            str: AI-generated suggestions for fixing the SQL error
        """
        try:
            client = OpenAI(api_key= os.environ.get("OPENAI_API_KEY"))
            error_message = task_data.get("error", "")
            compiled_code = task_data.get("compiled_code", "")
            model_name = task_data.get("source_id", "")
            system_prompt = dedent(
                "You are an expert SQL debugging specialist. "
                "Given a SQL compilation error and the problematic query, analyze the error and provide specific, actionable suggestions to fix it. "
                "Focus on identifying syntax errors, typos, missing keywords, incorrect table/column references, and other common SQL issues. "
                f"Limit your response to {6} sentence{'s' if 6 != 1 else ''}. "
                "Return only the suggestions in markdown format."
            )
            
            user_prompt = f"""**MODEL:** {model_name}

            **ERROR MESSAGE:**
            {error_message}

            **PROBLEMATIC SQL CODE:**
            ```sql
            {compiled_code}
            ```

            Please analyze this SQL compilation error and provide specific suggestions to fix it."""
                
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.2,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
            )
            
            response_content = response.choices[0].message.content if hasattr(response, 'choices') and response.choices else ""
            response_content = response_content.replace("```markdown", "").replace("```", "").replace("**Suggestions:**", "").replace("**SQL Fix:**", "").replace("'", "").strip()
            return response_content
            
        except Exception as e:
            log_error(f"Generate SQL Error Suggestions Failed ", e)
            raise e

def generate_tests_sql_error_suggestions(tests_list: list):
        """
        Generate AI-powered suggestions for multiple SQL test failures in batch.
        
        Args:
            tests_list (list): List of test dictionaries containing error messages and SQL queries
            
        Returns:
            str: AI-generated recommendations for all tests in a single response
        """
        try:
            client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
            
            system_prompt = dedent("""
                You are an expert SQL data quality analyst specializing in test failure analysis.
                
                Your task is to analyze SQL test failures and provide specific, actionable recommendations.
                
                For each test failure, you need to:
                1. Analyze the error message and understand what the test was expecting
                2. Examine the SQL query to understand the data condition being tested
                3. Provide a one-liner recommendation explaining why the test failed and what action to take
                
                Key analysis patterns:
                - If error contains "Got X results, configured to fail if != 0": The test expects zero records but found X records
                - If error contains "Got 0 results, configured to fail if == 0": The test expects records but found none
                - If error contains "Got X results, configured to fail if != Y": The test expects Y records but found X
                - Analyze the WHERE clause to understand the data condition being tested
                
                Output format:
                Number each recommendation and start with the test name (source_id) followed directly by a space and the explanation (no colon):
                "Test_Name natural explanation of the issue and suggested action."
                "Test_Name natural explanation of the issue and suggested action."
                
                Do not use single or double quotes for column names, table names, or any other identifiers.
                Do not use colons or any punctuation after the test name. Keep each recommendation under 100 characters and focus on the root cause and immediate action needed.
                """).strip()
            
            # Build the user prompt with all tests
            user_prompt = "Analyze the following SQL test failures and provide recommendations:\n\n"
            
            for test_data in tests_list:
                error_message = test_data.get("test_error", "")
                compiled_code = test_data.get("test_compiled_code", "")
                test_name = test_data.get("test_name", "")
                
                user_prompt += f"Test: {test_name}\n"
                user_prompt += f"Error: {error_message}\n"
                user_prompt += f"SQL Query:\n```sql\n{compiled_code}\n```\n\n"
            
            user_prompt += "Provide one-liner recommendations for each test failure."
                
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
            )
            
            response_content = response.choices[0].message.content if hasattr(response, 'choices') and response.choices else ""
            return response_content.strip()
            
        except Exception as e:
            log_error(f"Generate Batch SQL Error Suggestions Failed", e)
            raise e

def create_progations_alerts(config: dict, c_credentials: dict, asset_id: str, run_id: str, execute_state_pipe: str = None):
    try:
        configured_status = []
        pipeline_tests = []
        test_tasks_filter = ""

        is_failure = c_credentials.get("propagate_alerts_failure")
        if is_failure:
            configured_status.append("failed")

        is_warning = c_credentials.get("propagate_alerts_warning")
        if is_warning:
            configured_status.append("warning")
        alert_propagation_type = c_credentials.get("propagate_alerts")
        alert_propagation_type = (
            str(alert_propagation_type).lower() if alert_propagation_type else ""
        )

        connection_type = config.get("connection", {}).get("type")
        configured_status = f"""({','.join(f"'{w}'" for w in configured_status)})"""
        drift_status = ""
        if connection_type == ConnectionType.Dbt.value or connection_type == ConnectionType.Coalesce.value:
            pipeline_tests = get_pipeline_tests(config, asset_id, run_id, configured_status)
            if isinstance(pipeline_tests, str):
                pipeline_tests = json.loads(pipeline_tests)
            if pipeline_tests:
                task_ids = list(pipeline_tests.keys())
                test_tasks_filter = f"""or pipeline_tasks.id in ({','.join(f"'{w}'" for w in task_ids)})""" if task_ids else ""
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pipeline_tasks.*, pipeline.name as pipeline_name, connection.organization_id,
                connection.name as connection_name, metrics.id as metrics_id
                from core.pipeline_tasks
                join core.connection on connection.id=pipeline_tasks.connection_id
                join core.pipeline on pipeline.id = pipeline_tasks.pipeline_id
                left join core.metrics on metrics.task_id = pipeline_tasks.id and metrics.pipeline_run_id = pipeline_tasks.run_id
                where pipeline_tasks.asset_id='{asset_id}' and pipeline_tasks.run_id='{run_id}'
                and (lower(pipeline_tasks.status) in {configured_status}
                {test_tasks_filter})
            """
                
            if connection_type.lower() in [ConnectionType.Databricks.value, ConnectionType.Hive.value, ConnectionType.SalesforceMarketing.value] and alert_propagation_type == "pipeline":
                query_string = f"""
                select pipeline.name as pipeline_name, connection.organization_id,metrics.id as metrics_id,
                    pipeline.id as type_unique_id,
                    connection.id as connection_id,
                    pipeline.status as status,
                    connection.name as connection_name,
					asset.type as pipeline_type
                    from core.pipeline
                    join core.connection on connection.id=pipeline.connection_id
                    left join core.metrics  on metrics.asset_id = pipeline.asset_id and metrics.pipeline_run_id = pipeline.run_id
                    left join core.asset on asset.id=pipeline.asset_id
                    where pipeline.asset_id='{asset_id}'
                    and lower(pipeline.status) in {configured_status}
                """

            if connection_type in [ConnectionType.Snowflake.value.lower()] and alert_propagation_type == "pipeline":
                asset = config.get("asset", {})
                asset_type = asset.get("type").lower()
                if asset_type in [ASSET_TYPE_PROCEDURE]:  
                    query_string = f"""
                     select pipeline_transformations.id as type_unique_id,pipeline_id,pipeline.name as pipeline_name, connection.organization_id,metrics.id as metrics_id,
                        connection.id as connection_id,
                        pipeline.status as status,
                        connection.name as connection_name,
						asset.type as pipeline_type
                        from core.pipeline
                        join core.connection on connection.id=pipeline.connection_id
                        left join core.metrics  on metrics.pipeline_run_id = pipeline.run_id
						left join core.pipeline_transformations on pipeline_transformations.pipeline_id=pipeline.id
						left join core.asset on asset.id=pipeline.asset_id
                        where pipeline.asset_id='{asset_id}'
                        and lower(pipeline.status) in {configured_status}
                    """
                elif asset_type == ASSET_TYPE_PIPE:
                     query_string = f"""
                     select pipeline.id as type_unique_id,pipeline.name as pipeline_name, connection.organization_id,metrics.id as metrics_id,
                        connection.id as connection_id,
                        pipeline.status as status,
                        connection.name as connection_name,
						asset.type as pipeline_type
                        from core.pipeline
                        join core.connection on connection.id=pipeline.connection_id
                        left join core.metrics  on metrics.pipeline_run_id = pipeline.run_id
						left join core.asset on asset.id=pipeline.asset_id
                        where pipeline.asset_id='{asset_id}'
                        and lower(pipeline.status) in {configured_status}
                    """
                else:
                    query_string = f"""
                    select pipeline_tasks.id ,pipeline.name as pipeline_name, connection.organization_id,metrics.id as metrics_id,
                        connection.id as connection_id,
                        pipeline.status as status,
                        connection.name as connection_name
                        from core.pipeline
                        join core.connection on connection.id=pipeline.connection_id
                        join core.pipeline_tasks on pipeline.id = pipeline_tasks.pipeline_id
                        left join core.metrics  on metrics.pipeline_run_id = pipeline.run_id
                        where pipeline.asset_id='{asset_id}'
                        and lower(pipeline.status) in {configured_status}
                    """

            if connection_type.lower() in [ConnectionType.ADF.value.lower()] and alert_propagation_type == "pipeline":
                query_string = f"""
                select pipeline_tasks.*, pipeline.name as pipeline_name, connection.organization_id,
                connection.name as connection_name, metrics.id as metrics_id
                from core.pipeline_tasks
                join core.connection on connection.id=pipeline_tasks.connection_id
                join core.pipeline on pipeline.id = pipeline_tasks.pipeline_id
                join core.pipeline_runs on pipeline_runs.source_id = '{run_id}' and pipeline_runs.asset_id = pipeline_tasks.asset_id
                left join core.metrics on metrics.task_id = pipeline_tasks.id and metrics.pipeline_run_id = pipeline_tasks.run_id
                where pipeline_tasks.asset_id='{asset_id}' and pipeline_tasks.run_id = pipeline_runs.source_id
                and (lower(pipeline_tasks.status) in {configured_status})
                """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_tasks = fetchall(cursor)
            pipeline_tasks = pipeline_tasks if pipeline_tasks else []

            # Collect metrics_ids for batch LLM update
            metrics_ids_for_llm_update = []
            # Store mapping of metrics_id to (job_name, task_name) for LLM context
            metrics_context_map = {}

            for pipeline_task in pipeline_tasks:
                sql_error_suggestions = ""
                metrics_id = pipeline_task.get("metrics_id")
                if metrics_id:
                    continue
                job_name = pipeline_task.get("pipeline_name")
                task_id = pipeline_task.get("id", None)
                tests = pipeline_task.get("tests", None)
                type_unique_id = pipeline_task.get("type_unique_id", None)
                type_is = pipeline_task.get("pipeline_type", None)
                task_name = pipeline_task.get("name", "")
                task_status = pipeline_task.get("status", None)
                if not task_status or task_status.lower() == "none":
                    continue
                if connection_type == ConnectionType.Dbt.value:
                    
                    failed_tests = pipeline_tests.get(task_id, []) if pipeline_tests else {}
                    if failed_tests:
                        sql_error_suggestions = generate_tests_sql_error_suggestions(failed_tests)
                    else:
                        sql_error_suggestions = generate_sql_error_suggestions(pipeline_task)
                if sql_error_suggestions:
                    sql_error_suggestions = '\n'.join(line.strip() for line in sql_error_suggestions.split('\n') if line.strip())
                test_status = None
                task_status_title = (
                    "failure"
                    if task_status and str(task_status).lower() == "failed"
                    else task_status
                )
                connection_id = pipeline_task.get("connection_id")
                organization_id = pipeline_task.get("organization_id")
                task_status = str(task_status).lower() if task_status else ""
                if is_failure and task_status == "failed":
                    drift_status = "High"
                elif is_warning and task_status == "warning":
                    drift_status = "Medium"

                if connection_type == ConnectionType.Dbt.value and task_status == "success" and pipeline_tests:
                    drift_status, task_status_title, test_status = prepare_dbt_test_alert_info(pipeline_tests, task_id, is_failure, is_warning)
                if connection_type == ConnectionType.Coalesce.value and pipeline_tests:
                    drift_status, task_status_title, test_status = prepare_coalesce_test_alert_info(pipeline_tests, task_id, is_failure, is_warning)
                message = f"The task {task_name} on job {job_name} detected a {task_status_title}."
                    
                task_id = f"'{str(task_id)}'" if task_id else 'NULL'
                type_unique_id= f"'{str(type_unique_id)}'" if type_unique_id else 'NULL'
                type_is = f"'{str(type_is)}'" if type_is else 'NULL'
                
                # Get Last Run Info
                query_string = f"""
                    insert into core.metrics (
                        id, measure_name, level, is_measure, drift_status, message, asset_group, pipeline_run_id, ai_sql_recommendations,
                        organization_id, connection_id, asset_id, task_id,type_unique_id,type_is, is_archived, allow_score, is_drift_enabled,
                        is_active, is_delete, created_date
                    )
                    values (
                        gen_random_uuid(), '{task_name}', 'Pipeline', True, '{drift_status}', '{message}', 'pipeline', '{str(run_id)}','{sql_error_suggestions}',
                        '{str(organization_id)}', '{str(connection_id)}', '{str(asset_id)}', {task_id},{type_unique_id},{type_is}, False, False,
                        False, True, False, CURRENT_TIMESTAMP
                    )
                    returning id
                """
                cursor = execute_query(connection, cursor, query_string)
                metrics = fetchone(cursor)
                metrics_id = metrics.get("id") if metrics else None
                metrics_id = metrics_id if metrics_id else None

                if metrics_id:
                    # Collect metrics_id for batch LLM update (will process after loop)
                    metrics_ids_for_llm_update.append(metrics_id)
                    # Store job_name and task_name for this metrics_id
                    metrics_context_map[metrics_id] = {
                        "job_name": job_name,
                        "task_name": task_name
                    }

                    issue_status = []
                    # Create Notification for Pipeline Alerts
                    # save_alert_event(config, [str(metrics_id)], {"type": "new_alert_pipeline"})
                    # # Create ServiceNow Alerts
                    # send_servicenow_alerts(config,metrics_id)

                    issue_propagation_type = c_credentials.get(
                        "propagate_issue"
                    )
                    issue_propagation_type = (
                        str(issue_propagation_type).lower()
                        if issue_propagation_type
                        else ""
                    )
                    is_failure = c_credentials.get(
                        "propagate_issue_failure")
                    if is_failure:
                        issue_status.append("failed")
                    is_warning = c_credentials.get(
                        "propagate_issue_warning")
                    if is_warning:
                        issue_status.append("warning")
                    if (
                        issue_propagation_type == "pipeline"
                        and (is_failure or is_warning)
                        and ((
                            task_status
                            and str(task_status).lower() in issue_status
                        ) or (
                            connection_type == ConnectionType.Dbt.value
                            and test_status
                            and str(test_status).lower() in issue_status
                        ))
                    ):
                        create_progations_issues(config, metrics_id, sql_error_suggestions)

            # Batch update LLM-generated messages for all collected metrics
            if metrics_ids_for_llm_update:
                try:
                    # Fetch all metric data for LLM processing
                    escaped_ids = [str(mid).replace("'", "''") for mid in metrics_ids_for_llm_update]
                    metric_ids_str = "', '".join(escaped_ids)
                    
                    fetch_metrics_query = f"""
                        SELECT query, message, *
                        FROM core.metrics
                        WHERE id IN ('{metric_ids_str}')
                        AND drift_status IN ('High','Medium','Low')
                    """
                    cursor = execute_query(connection, cursor, fetch_metrics_query)
                    all_alert_data = fetchall(cursor)

                    if all_alert_data:
                        # Process all alerts and collect LLM messages for batch update
                        update_values = []
                        for alert_data in all_alert_data:
                            try:
                                # Format alert metadata as a single string (tab-separated like in update_alerts_with_llm_messages)
                                alert_metadata_parts = []
                                for key, value in alert_data.items():
                                    if value is None:
                                        alert_metadata_parts.append("")
                                    elif isinstance(value, (dict, list)):
                                        # Convert dict/list to JSON string
                                        alert_metadata_parts.append(json.dumps(value, default=str))
                                    else:
                                        # Convert to string and escape tabs/newlines
                                        str_value = str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")
                                        alert_metadata_parts.append(str_value)
                                
                                # Get job_name and task_name from context map if available
                                metric_id = alert_data.get("id")
                                context_info = metrics_context_map.get(metric_id, {})
                                job_name_context = context_info.get("job_name")
                                task_name_context = context_info.get("task_name")
                                
                                # Prepend job_name and task_name to alert_metadata_string
                                # Format: "Job name: {job_name}"	"Task name: {task_name}"	<rest of metadata>
                                job_task_prefix_parts = []
                                if job_name_context:
                                    job_task_prefix_parts.append(f"Job name: {job_name_context}")
                                else:
                                    job_task_prefix_parts.append("")
                                if task_name_context:
                                    job_task_prefix_parts.append(f"Task name: {task_name_context}")
                                else:
                                    job_task_prefix_parts.append("")
                                
                                # Build the complete alert_metadata_string with job_name and task_name prepended
                                alert_metadata_parts_with_context = job_task_prefix_parts + alert_metadata_parts
                                alert_metadata_string = "\t".join(alert_metadata_parts_with_context)
                                
                                # Generate LLM-based message using the imported function
                                llm_message = generate_llm_based_alert_message(alert_metadata_string)
                                if llm_message:
                                    if metric_id:
                                        # Prepare tuple for batch update: (metric_id, llm_message)
                                        update_values.append((metric_id, llm_message))
                                        log_info(f"Generated LLM message for pipeline metric_id: {metric_id}")
                                else:
                                    log_info(f"LLM message generation returned None for pipeline metric_id: {alert_data.get('id')}")
                                    
                            except Exception as e:
                                log_error(f"Error processing alert for metric_id {alert_data.get('id')}: {e}", e)
                                continue
                        
                        # Perform batch update if we have any messages to update
                        if update_values:
                            # Build batch update query using UPDATE FROM VALUES pattern
                            update_params = []
                            for metric_id, llm_message in update_values:
                                query_input = (metric_id, llm_message)
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals})",
                                    query_input,
                                ).decode("utf-8")
                                update_params.append(query_param)
                            
                            # Split into batches if needed (using split_queries helper)
                            update_batches = split_queries(update_params)
                            
                            for batch_values in update_batches:
                                try:
                                    query_input = ",".join(batch_values)
                                    batch_update_query = f"""
                                        UPDATE core.metrics
                                        SET message = update_data.message
                                        FROM (
                                            VALUES {query_input}
                                        ) AS update_data(id, message)
                                        WHERE metrics.id::text = update_data.id::text
                                    """
                                    cursor = execute_query(connection, cursor, batch_update_query)
                                    log_info(f"Batch updated {len(batch_values)} pipeline alerts with LLM-generated messages")
                                except Exception as e:
                                    log_error(f"Error in batch update for pipeline LLM messages: {e}", e)
                                    # Continue with next batch even if one fails
                                    continue

                        
                                    
                except Exception as e:
                    log_error(f"Failed to batch update pipeline alerts with LLM messages: {e}", e)
                    # Don't fail the entire process if LLM batch update fails

                try:
                    for id in metrics_ids_for_llm_update:
                        save_alert_event(config, [str(id)], {"type": "new_alert_pipeline"})
                        send_servicenow_alerts(config,id)
                except Exception as e:
                    log_error(f"Error sending alert event: {e}", e)

    except Exception as e:
        log_error(f"Create Pipeline Alerts By Propagations Failed ", e)
        raise e
    
def get_alert_metric(config: dict, alert_id: str, alerts_filter):
    response =[]
    if alerts_filter:
        alerts_filter = '(\'' + '\', \''.join(map(str, alerts_filter)) + '\')'
        alerts_filter_query = f"and lower(metrics.drift_status) in {alerts_filter}"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        query_string = f"""
            select connection.name as "Connection_Name", asset.name as "Asset_Name", attribute.technical_name as "Attribute_Name", 
            string_agg(distinct(application.name),', ') as "Application", string_agg(distinct(domain.name),', ') as "Domain", 
            base_measure.name as "Measure_Name", base_measure.type  as "Measure_Type", base_measure.level as "Measure_Level",
            case when metrics.drift_status='High' then 'critical'
            when metrics.drift_status='Medium' then 'warning'
            when metrics.drift_status='Low' then 'unknown'
            else 'ok' end as status, metrics.message as "Alert_Description", metrics.threshold->>'lower_threshold' as "Lower_Threshold",
            metrics.threshold->>'upper_threshold' as "Upper_Threshold", 
            concat(metrics.threshold->>'lower_threshold',' - ', metrics.threshold->>'upper_threshold') as "Expected", 
            metrics.value as "Actual", metrics.percent_change as "Percent_Change",
            metrics.deviation as "Deviation", metrics.id as "Measure_ID", metrics.id as "Metrics_Id"
            from core.metrics 
            join core.connection on metrics.connection_id=connection.id
            left join core.asset on metrics.asset_id=asset.id
            left join core.attribute on attribute.id=metrics.attribute_id
            left join core.application_mapping on application_mapping.asset_id=asset.id
            left join core.application on application.id=application_mapping.application_id
            left join core.domain_mapping on domain_mapping.asset_id=asset.id
            left join core.domain on domain.id=domain_mapping.domain_id
            left join core.measure on measure.id=metrics.measure_id
            left join core.base_measure on measure.base_measure_id=base_measure.id
            where metrics.id='{alert_id}'{alerts_filter_query}
            group by connection.name,asset.name,attribute.technical_name,base_measure.name,
            base_measure.type,base_measure.level,metrics.drift_status,metrics.message,
            metrics.threshold,metrics.threshold,metrics.value,metrics.percent_change,
            metrics.percent_change,metrics.deviation,metrics.measure_id, metrics.id
        """
        cursor = execute_query(connection, cursor, query_string)
        alert = fetchone(cursor)
        if alert:
            if alert.get("Measure_Name", '') == 'Freshness':
                lt = format_freshness(
                    alert.get("Lower_Threshold", '0'))
                ut = format_freshness(
                    alert.get("Upper_Threshold", '0'))
                formatted_data = {
                    "Lower_Threshold": lt,
                    "Upper_Threshold": ut,
                    "Expected": f"{lt} - {ut}",
                    "Actual": alert(alert.get("Actual", '0'))
                }
                alert = {**alert, **formatted_data}
            response.append(alert)
    return response
    
def get_jira_details(config: dict,issue_id, channel, organization_id) -> dict:
        """
        Get Jira Details for the given issue_id and channel.
        """
        jira = ""
        is_active = ""
        jira_config = ""
        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    WITH issue_details AS (
                            SELECT 
                                i1.is_active,
                                a2.technical_name AS asset_name,
                                m.technical_name AS measure_name,
                                c.name AS connection_name,
                                c.type AS connection_type,
                                d.message AS description,
                                i2.created_date,
                                '{config.get("asset").get("created_by")}' as created_by,
                                i1.config,
                                i2.asset_id as asset_id,
                                i2.metrics_id,
                                i2.issue_id as issue_id,
								null as attribute_id,
								null as measure_id,
                                null as attribute_name,
                                i2.name as name,
                                i2.priority as priority,
                                i2.id as id
                            FROM core.channels ch
                            JOIN core.integrations i1 ON i1.channel_id = ch.id
                            JOIN core.issues i2 ON i1.organization_id = i2.organization_id
                            LEFT JOIN core.metrics d ON i2.metrics_id = d.id
                            LEFT JOIN core.measure m ON m.id = d.measure_id
                            LEFT JOIN core.asset a2 ON i2.asset_id = a2.id
                            LEFT JOIN core.connection c ON c.id = d.connection_id
                            LEFT JOIN core.users u ON u.id = i2.created_by_id
                            WHERE i2.id = '{issue_id}'
                            AND i1.organization_id = '{organization_id}'
                            AND ch.type = '{channel}'
                            AND d.drift_status IN ('High', 'Low', 'Medium')
                        ),
                    mappings AS (
                        SELECT
                            i.asset_id,
                            i.metrics_id,
                            COALESCE(json_agg(DISTINCT jsonb_build_object('id', d.id, 'name', d.technical_name)) 
                                    FILTER (WHERE dm.id IS NOT NULL), '[]') AS domains,
                            COALESCE(json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.technical_name)) 
                                    FILTER (WHERE pm.id IS NOT NULL), '[]') AS products,
                            COALESCE(json_agg(DISTINCT jsonb_build_object('id', a.id, 'name', a.name, 'color', a.color)) 
                                    FILTER (WHERE am.id IS NOT NULL), '[]') AS applications
                        FROM issue_details i
                        LEFT JOIN core.domain_mapping dm ON (dm.asset_id = i.asset_id OR dm.measure_id = i.metrics_id)
                        LEFT JOIN core.domain d ON d.id = dm.domain_id
                        LEFT JOIN core.product_mapping pm ON (pm.asset_id = i.asset_id OR pm.measure_id = i.metrics_id)
                        LEFT JOIN core.product p ON p.id = pm.product_id
                        LEFT JOIN core.application_mapping am ON (am.asset_id = i.asset_id OR am.measure_id = i.metrics_id)
                        LEFT JOIN core.application a ON a.id = am.application_id
                        GROUP BY i.asset_id, i.metrics_id
                    )
                    SELECT 
                        idt.*,
                        mp.domains,
                        mp.products,
                        mp.applications
                    FROM issue_details idt
                    JOIN mappings mp ON mp.asset_id = idt.asset_id AND mp.metrics_id = idt.metrics_id;
                """
                cursor = execute_query(connection, cursor, query_string)
                jira = fetchone(cursor)
                if jira:
                    is_active = jira.get("is_active")
                    jira_config = jira.get("config", {})
                return jira_config, is_active, jira
        except Exception as e:
            log_error(f"Failed to get Jira details for issue {issue_id}", e)
            raise e
        
def send_servicenow_alerts(config, metrics_id):
    try:
        is_active, channel_config = get_servicenow_config(config)
        if not channel_config:
            return

        push_alerts = channel_config.get("push_alerts", '')
        alerts_filter = channel_config.get("alerts_priorities", [])
        if is_active and push_alerts:
            if metrics_id:
                alerts =  get_alert_metric(config, metrics_id, alerts_filter) 
                if alerts:
                    request_data = prepare_servicenow_request_data(
                        config, channel_config, alerts)
                    connection = get_postgres_connection(config)
                    with connection.cursor() as cursor:
                        for alert, request in zip(alerts, request_data):
                            servicenow_response = get_servicenow_response(request, channel_config, "post")
                            
                            if servicenow_response:
                                servicenow_id = servicenow_response.get("number")
                                servicenow_sys_id = servicenow_response.get("sys_id")
                                if servicenow_id:
                                    client_name = os.environ.get("CLIENT_NAME")
                                    client_name = client_name if client_name else "DQLabs"
                                    update_payload = {
                                        "source": client_name,
                                        "description": alert.get('Alert_Description')
                                    }
                                    get_servicenow_response(update_payload, channel_config, "patch", servicenow_sys_id)
                                    # Update the metrics table with the external_id
                                    external_id = f'{{"servicenow_id": "{servicenow_id}"}}'
                                    update_query = f"""
                                        UPDATE core.metrics 
                                        SET external_id = '{external_id}'
                                        WHERE id = '{alert.get("Metrics_Id")}'
                                    """
                                    execute_query(connection, cursor, update_query)
    except Exception as e:
        log_error(
            f"Servicenow send alerts Failed", e)

def create_progations_issues(config: dict, metric_id: str, sql_error_suggestions: str):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                with recent_issue as (
                    select (
                        case when issue_id is not null then(replace(issue_id, 'ISU-', ''))::int else 0 end
                    ) as issue_number
                    from core.issues
                    order by created_date desc
                    limit 1
                ), filtered_alerts as (
                    select
                    metrics.message as name,
                    metrics.message as description,
                    (
                        'ISU-' || (
                            (select issue_number from recent_issue)
                            + row_number() over(partition by metrics.organization_id order by metrics.created_date desc)
                        )
                    ) as issue_id,
                    case when metrics.drift_status is not null then metrics.drift_status else 'Low' end as priority,
                    metrics.organization_id as organization_id,
                    connection.id as connection_id,
                    asset.id as asset_id,
                    metrics.id as metrics_id,
                    True as is_active,
                    False as is_delete,
                    current_timestamp as created_date
                    from core.metrics
                    join core.connection on connection.id=metrics.connection_id
                    join core.asset on asset.id=metrics.asset_id
                    left join core.pipeline_tasks on pipeline_tasks.id = metrics.task_id
                    where metrics.id = '{metric_id}'
                    order by metrics.created_date desc
                )
                insert into core.issues(
                    id, name, description, issue_id, status, priority,
                    organization_id, connection_id, asset_id, metrics_id,
                    is_active, is_delete, created_date
                ) select gen_random_uuid(), name, description, issue_id, 'New', priority,
                    organization_id, connection_id, asset_id, metrics_id,
                    is_active, is_delete, created_date
                from filtered_alerts
            """
            cursor = execute_query(connection, cursor, query_string)
            query_string = f"""
                select id from core.issues
                where metrics_id = '{metric_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)
            issue_id = result.get('id') if result else None
            if issue_id:
                map_pipeline_corelated_alerts(config, issue_id, metric_id)
                update_ai_summary(config, issue_id, sql_error_suggestions)
            jira_config, jira_is_active, jira = get_jira_details(config, issue_id, "jira", config.get("organization_id"))
            if jira_is_active:
                create_jira_issue(config, jira_config, jira, True)
            servicenow_config, servicenow_is_active, servicenow = get_jira_details(config, issue_id, "servicenow", config.get("organization_id"))
            if servicenow_is_active:
                push_issues = servicenow_config.get("push_issues", False)
                issues_priorities = servicenow_config.get("issues_priorities") if (servicenow_config and "issues_priorities" in servicenow_config) else ""
                priority = servicenow.get("priority")
                if servicenow_is_active and push_issues and (priority.lower() in issues_priorities if issues_priorities else True):
                    create_servicenow_issue(config, servicenow_config, servicenow, True)
            
    except Exception as e:
        log_error(f"Create Pipeline Issues By Propagations Failed ", e)
        raise e

def map_pipeline_corelated_alerts(config: dict, issue_id: str, metric_id: str):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select pt.id as task_id, pt.source_id, pt.asset_id, pt.connection_id, metrics.id as metrics_id,
                issues.id as issue_id, issues.issue_id as issue_number
                from core.pipeline_tasks pt
                left join core.metrics on metrics.task_id = pt.id and metrics.task_id is not null
                left join core.issues on issues.metrics_id = metrics.id and issues.id = '{issue_id}'
                where metrics.id = '{metric_id}' and pt.id is not null
            """
            cursor = execute_query(connection, cursor, query_string)
            issue_data = fetchone(cursor)
            linked_entities =[]
            if issue_data:
                params = {
                    "asset_id": issue_data.get("asset_id"),
                    "connection_id": issue_data.get("connection_id"),
                    "entity": issue_data.get("source_id"),
                    "asset_group": "pipeline"
                }
                linked_entities = get_linked_entities(config, params)
                issue_task_id = issue_data.get("task_id","") if issue_data else ""
            if linked_entities:
                for asst in linked_entities:
                    lineage_asset_id = asst.get("asset_id")
                    lineage_task_id = asst.get("redirect_id")
                    lineage_connection_id = asst.get("connection_id")
                    if not lineage_asset_id or not lineage_task_id or not lineage_connection_id:
                        continue
                    if lineage_task_id and str(lineage_task_id) == str(issue_task_id):
                        # ToDo : check for same task
                        continue
                    else:
                        query_string = f""" 
                        select 
                            * from core.metrics where asset_id='{lineage_asset_id}' and connection_id='{lineage_connection_id}' and 
                            task_id='{lineage_task_id}' and drift_status in ('High', 'Low', 'Medium') and created_date >= NOW() - INTERVAL '36 HOUR'  """
                    
                        connection = get_postgres_connection(config)
                        with connection.cursor() as cursor:
                            cursor.execute(query_string)
                            related_metrics = fetchall(cursor)
                            if related_metrics:
                                for metric in related_metrics:
                                    metric_id = metric.get("id")
                                    check_query = f"""
                                         SELECT COUNT(*) as count 
                                         FROM core.associated_issues 
                                         WHERE metrics_id = '{metric_id}' AND issue_id = '{issue_id}'
                                     """
                                    cursor.execute(check_query)
                                    check_result = fetchone(cursor)
                                    associated_issue_exists = check_result.get('count', 0) > 0 if check_result else False
                                     
                                    if not associated_issue_exists:
                                        add_associated_issue(config, issue_data, metric)

    except Exception as e:
        log_error(f"Map Pipeline Corelated Alerts Failed ", e)
        raise e

def add_associated_issue(config: dict, issue, metric):
    """
    Adds an associated issue to the database.
    """

    query = """
        INSERT INTO associated_issues (
            id,
            issue_number,
            issue_id,
            measure_id,
            attribute_id,
            metrics_id,
            asset_id,
            connection_id,
            created_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """
    values = (
        str(uuid4()),
        issue.get("issue_number"),
        issue.get("issue_id"),
        metric.get("measure_id"),
        metric.get("attribute_id"),
        metric.get("id"),
        metric.get("asset_id"),
        metric.get("connection_id"),
    )
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query, values)
    except Exception as e:
        log_error(f"Failed to add associated issue: {e}")

def generate_ai_summary(prompt_data: dict, sentence_limit: int = 4):
    try:
        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        issue_message = prompt_data.get("issue_message", {})
        correlated_alerts = prompt_data.get("pipeline_alerts", {})
        system_prompt = dedent(
            "You are an expert data quality summarizer. "
            "Given the following issue message or correlated alerts details analyze and summarize the root cause, key patterns, alert types, frequency, and drift severity. "
            f"Limit your response to {sentence_limit} sentence{'s' if sentence_limit != 1 else ''}. "
            "Return only the summary in markdown format."
        )
        user_prompt = ""
        if issue_message:
            user_prompt = (
                f"**ISSUE MESSAGE:**\n{issue_message}\n\n")
        if correlated_alerts:
            user_prompt = (
                f"**CORRELATED ALERTS:**\n{json.dumps(correlated_alerts, indent=2)}"
            )
            
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        response_content = response.choices[0].message.content if hasattr(response, 'choices') and response.choices else ""
        response_content = response_content.replace("```markdown", "").replace("```", "").replace("**Summary:**", "").replace("'", "").replace("### Summary", "").strip()
        return response_content
    except Exception as e:
        log_error(f"Generate AI Summary Failed ", e)
        raise e

def update_ai_summary(config: dict, issue_id: str, sql_error_suggestions: str):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""With corelated_alerts as (
                select associated_issues.*,
                asset.group as asset_group,
                metrics.measure_name, metrics.attribute_name, metrics.level, metrics.value, metrics.weightage,
                metrics.total_count, metrics.invalid_count, metrics.invalid_percentage, metrics.score, metrics.status,
                metrics.drift_status, metrics.message, metrics.measure_id, metrics.task_id, metrics.type_unique_id

                from core.associated_issues
                join core.asset on asset.id = associated_issues.asset_id
                join core.metrics on metrics.id = associated_issues.metrics_id
                where associated_issues.issue_id='{issue_id}'
                )

                select issues.*, m.message, asset.name as asset_name, asset.group as asset_group,  
                    (
                        SELECT json_agg(corelated_alerts)
                        FROM corelated_alerts
                    ) AS correlated_alerts_json
                    from core.issues 
                    left join core.metrics m on m.id = issues.metrics_id
                    left join core.asset on asset.id = issues.asset_id
                    where issues.id='{issue_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            issue_data = fetchone(cursor)
            if issue_data:
                prompt_data = {
                    "issue_message": issue_data
                }
                issue_ai_summary = generate_ai_summary(prompt_data, 4)
                pipeline_alerts_response = {}
                correlated_alerts_data = issue_data.get("correlated_alerts_json", [])
                if correlated_alerts_data:
                    for ca in correlated_alerts_data:
                        asset_group = ca.get("asset_group", "")
                        if asset_group == "pipeline":
                            ca_data = {"pipeline_alerts": ca}
                            # Use 2-sentence limit for correlated alerts summary
                            ca_ai_summary = generate_ai_summary(ca_data, 2)
                            pipeline_alerts_response.update({ca.get("id"): ca_ai_summary})
                update_query = f"""
                    UPDATE core.issues
                    SET summary = '{issue_ai_summary}', ai_sql_recommendations = '{sql_error_suggestions}'
                    WHERE id = '{issue_id}'
                """
                execute_query(connection, cursor, update_query)
                if pipeline_alerts_response:
                    for associate_alert_id, summary_data in pipeline_alerts_response.items():
                        update_query = f""" update core.associated_issues set summary = '{summary_data}' where id='{associate_alert_id}' """
                        execute_query(connection, cursor, update_query)

    except Exception as e:
        log_error(f"Update AI Summary Failed ", e)
        raise e

def save_databricks_lineage(config: dict) -> list:
    """
    Returns the lineage information of databricks for selected asset
    """
    try:
        connection = config.get("connection")
        connection = connection if connection else {}
        connection_config = config.get("connection").get("credentials")
        # lineage_is_enabled = (
        #     connection_config.get(
        #         "lineage", None) if connection_config else None
        # )
        # if not lineage_is_enabled:
        #     return
        asset = config.get("asset", None)
        asset_type = asset.get("type").lower() if asset else "table"
        asset_id = asset.get("id")
        properties = asset.get("properties", {})
        properties = (
            json.loads(properties) if isinstance(
                properties, str) else properties
        )
        properties = properties if properties else {}
        table_name = asset.get("technical_name", "")
        database_name = properties.get("database", "")
        schema_name = properties.get("schema", "")
        asset_table = {
            "id": asset_id,
            "name": table_name,
            "entity_name": table_name,
            "type": asset_type,
            "catalog": database_name,
            "schema": schema_name,
        }
        
        params = json.dumps({
            "table_name": f"{database_name}.{schema_name}.{table_name}", # sample data "table_name": "main.easyjet_silver.taxi_raw_records",
            "include_entity_lineage": True
        })
        api_respsone = __get_databricks_response(config, "api/2.0/lineage-tracking/table-lineage", "get", params)

        lineage = prepare_databricks_lineage(api_respsone, asset_table, config)
        if lineage:
            # saving lineage
            save_lineage(config, "pipeline", lineage, asset_id)
            tables = lineage.get("tables", [])
            if tables:
                # save lineage entity (only tables not in DB)
                save_lineage_entity(config, tables, asset_id)
        return lineage
    except Exception as e:
        log_error(str(e), e)
        return None

def __get_databricks_response(config: dict, url: str = "", method_type: str = "get", params=""):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute_databricks_api_request",
            parameters=dict(
                request_url=url, request_type=method_type, request_params=params
            ),
        )
        return api_response
    except Exception as e:
        log_error.error(
            f"Databricks Connector - Get Response Failed - {str(e)}", exc_info=True
        )
    finally:
        return api_response

def prepare_databricks_lineage(lineage_response, asset_table, config=None):
    result = {
        'tables': [],
        'relations': []
    }
    
    if not lineage_response or not (lineage_response.get('downstreams') or lineage_response.get('upstreams')):
        return result
    
    # Add the asset table to the tables list
    result['tables'].append(asset_table)
    

    # Track tables that exist in DB (will be mapped to associated_asset, not lineage_entity)
    tables_in_db = []
    matched_connection_id = None
    matched_asset_id = None
    
    
    # Process downstream tables (asset -> downstream)
    if lineage_response.get('downstreams'):
        for item in lineage_response['downstreams']:
            if 'tableInfo' not in item:
                continue
                
            table_info = item['tableInfo']
            downstream_table = {
                'id': str(uuid4()),
                'name': table_info['name'],
                'entity_name': table_info['name'],
                'type': 'downstream',
                'catalog': table_info['catalog_name'],
                'database': table_info['catalog_name'],
                'schema': table_info['schema_name'],
                'table_type': table_info['table_type'].lower(),
                'lineage_timestamp': table_info.get('lineage_timestamp'),
                'connection_type': ConnectionType.Databricks.value.lower(),
                'level': 2
            }
            table_exists_in_db = False
            matched_asset_id = None
            matched_connection_id = None
            if config:
                assets = get_asset_by_db_schema_name(config, downstream_table)
                if assets and len(assets) > 0:
                    table_exists_in_db = True
                    matched_asset_id = assets[0].get("id")  # Get the first asset's id
                    matched_connection_id = assets[0].get("connection_id")  # Get connection_id if available
                    # Map to associated_asset
                    save_asset_lineage_mapping(config, "data", downstream_table, assets)
                    tables_in_db.append(downstream_table)
            
            # Only add to tables list if NOT in DB (will be saved as lineage_entity)
            if not table_exists_in_db:
                result['tables'].append(downstream_table)
            
            relation = {
                'srcTableId': asset_table['id'],
                'tgtTableId': matched_asset_id if table_exists_in_db else downstream_table['entity_name'],
                'target_asset_id': matched_asset_id,
                'relationship_type': 'feeds',
                'srcEntityName': asset_table['entity_name'],
                'tgtEntityName': downstream_table['entity_name'],
                'lineage_timestamp': downstream_table.get('lineage_timestamp'),
                'srcTableColName': "",
                'tgtTableColName': ""
            }
        if matched_connection_id:
            relation['target_connection_id'] = matched_connection_id

        result['relations'].append(relation)
    
    # Process upstream tables (upstream -> asset)
    if lineage_response.get('upstreams'):
        for item in lineage_response['upstreams']:
            if 'tableInfo' not in item:
                continue
                
            table_info = item['tableInfo']
            upstream_table = {
                'id': str(uuid4()),
                'name': table_info['name'],
                'entity_name': table_info['name'],
                'type': 'upstream',
                'catalog': table_info['catalog_name'],
                'database': table_info['catalog_name'],
                'schema': table_info['schema_name'],
                'table_type': table_info['table_type'].lower(),
                'lineage_timestamp': table_info.get('lineage_timestamp'),
                'connection_type': ConnectionType.Databricks.value.lower(),
                'level': 2
            }
            
            # Check if table exists in DB
            table_exists_in_db = False
            matched_asset_id = None
            if config:
                assets = get_asset_by_db_schema_name(config, upstream_table)
                if assets and len(assets) > 0:
                    table_exists_in_db = True
                    matched_asset_id = assets[0].get("id")
                    matched_connection_id = assets[0].get("connection_id")
                    save_asset_lineage_mapping(config, "data", upstream_table, assets)
                    tables_in_db.append(upstream_table)
            if not table_exists_in_db:
                result['tables'].append(upstream_table)
            
            if not table_exists_in_db:
                relation = {
                    'srcTableId': matched_asset_id if table_exists_in_db else upstream_table['entity_name'],
                    'source_asset_id': matched_asset_id,
                    'tgtTableId': asset_table['id'],
                    'relationship_type': 'feeds',
                    'srcEntityName': upstream_table['entity_name'],
                    'tgtEntityName': asset_table['entity_name'],
                    'lineage_timestamp': upstream_table.get('lineage_timestamp'),
                    'srcTableColName': "",
                    'tgtTableColName': ""
                }
                if matched_connection_id:
                    relation['source_connection_id'] = matched_connection_id
                result['relations'].append(relation)
    
    return result

def prepare_dbt_test_alert_info(pipeline_tests: dict, task_id: str, is_failure: bool, is_warning: bool):
    """
    Prepare the alert information for dbt tests
    """
    try:
        test_status = ""
        drift_status = ""
        pipeline_test = pipeline_tests.get(task_id, {})

        # Determine overall test status
        for test in pipeline_test:
            if test["test_status"].lower() == "failed":
                test_status = "failed"
                break
            elif test["test_status"].lower() == "warning":
                test_status = "warning"

        # Set drift status based on overall test status
        drift_status = (
            "Medium" if is_failure and test_status == "failed" else
            "Low" if is_warning and test_status == "warning" else
            ""
        )
        # Build the message
        task_status_title = " and a ".join(
            f"{t['test_status']} on dbt Test {t['test_name']}"
            for t in pipeline_test
        )
        return drift_status, task_status_title, test_status
    except Exception as e:
        log_error(f"Prepare Dbt Test Alert Info Failed ", e)
        raise e

def prepare_coalesce_test_alert_info(pipeline_tests: dict, task_id: str, is_failure: bool, is_warning: bool):
    """
    Prepare the alert information for dbt tests
    """
    try:
        test_status = ""
        drift_status = ""
        pipeline_test = pipeline_tests.get(task_id, {})

        # Determine overall test status
        for test in pipeline_test:
            if test["test_status"].lower() == "failed":
                test_status = "failed"
                break
            elif test["test_status"].lower() == "warning":
                test_status = "warning"
        # Set drift status based on overall test status
        drift_status = (
            "Medium" if is_failure and test_status == "failed" else
            "Low" if is_warning and test_status == "warning" else
            ""
        )
        # Build the message
        task_status_title = " and a ".join(
            f"{t['test_status']} on Coalesce Test {t['test_name']}"
            for t in pipeline_test
        )
        return drift_status, task_status_title, test_status
    except Exception as e:
        log_error(f"Prepare Coalesce Test Alert Info Failed ", e)
        raise e

def get_lineage_table_columns(
        config: dict, type, asset_id: str, linked_entity: str
    ) -> list:
        columns = []
        if type == "data":
            query = f"""
                select
                    lower(attribute.id::text) as id, 
                    attribute.id as attribute_id,
                    attribute.name as name, 
                    attribute.datatype as data_type, 
                    attribute.score,
                    attribute.alerts
                from core.attribute
                where asset_id::text = '{linked_entity}'
            """
        elif type == "pipeline":
            query = f"""
                select 
                    lower(pipeline_columns.name) as id, 
                    pipeline_columns.name, 
                    pipeline_columns.data_type,
                    attribute.score as score,
                    attribute.alerts as alerts
                from core.pipeline_columns
                join core.pipeline_tasks on pipeline_tasks.id = pipeline_columns.pipeline_task_id
                join core.asset on asset.id = pipeline_tasks.asset_id
                join core.connection on connection.id = asset.connection_id
                left join core.associated_asset on associated_asset.source_asset_id = asset.id 
                    and associated_asset.source_id = pipeline_tasks.source_id
                left join core.attribute on attribute.asset_id = associated_asset.associated_asset_id and attribute.parent_attribute_id is null
                    and lower(attribute.name) = lower(pipeline_columns.name)
                where pipeline_tasks.source_id = '{linked_entity}' and asset.id = '{asset_id}'
            """
        else:
            query = f"""
                select 
                    lower(reports_columns.name) as id, 
                    reports_columns.name, 
                    reports_columns.data_type,
                    avg(attribute.score) as score,
                    sum(attribute.alerts) as alerts
                    from core.reports_columns
                    join core.reports_views on reports_views.id = reports_columns.report_view_id
                    join core.asset on asset.id = reports_views.asset_id
                    join core.connection on connection.id = asset.connection_id
                    left join core.lineage_entity on lineage_entity.asset_id = asset.id 
                    left join core.associated_asset on associated_asset.source_asset_id = asset.id 
                        and associated_asset.source_id = lineage_entity.entity_name
                    left join core.attribute on attribute.asset_id = associated_asset.associated_asset_id and attribute.parent_attribute_id is null
                        and lower(attribute.name) = lower(reports_columns.name)
                where reports_views.source_id = '{linked_entity}' and asset.id = '{asset_id}'
                group by reports_columns.id
            """

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = fetchall(cursor)
        return columns if columns else []

def get_lineage_table(
        config: dict,
        view_by: str,
        lineage_row: dict,
        flow: str,
        asset_id: str,
        linked_entity: str,
        tables: list,
    ) -> list:
        query = None
        type = lineage_row.get("type")
        asset_exists = next(
            (
                x
                for x in tables
                if x["entity"] == linked_entity and x["asset_id"] == asset_id
            ),
            None,
        )

        if not asset_exists:
            query = f"""
                select 
                    LOWER(CONCAT (asset.connection_id, '_', asset.id, '_', asset.id)) as id,
                    asset.id::text as entity, 
                    asset.id as redirect_id,
                    asset.name, 
                    asset.score,
                    asset.name as asset_name, 
                    asset.group as asset_group,
                    connection.id as connection_id, 
                    connection.name as connection_name, 
                    connection.type as connection_type,
                    connection_type_id, 
                    asset.properties ->> 'database' as database,
                    asset.properties ->> 'schema' as schema,
                    NULL as status,
                    NULL::jsonb as properties
                from core.asset
                join core.connection on connection.id = asset.connection_id
                where asset.id::text = '{linked_entity}'
                UNION
                select  
                    LOWER(CONCAT (connection.id, '_', asset.id, '_', pipeline_tasks.source_id)) as id,
                    pipeline_tasks.source_id as entity, 
                    pipeline_tasks.id as redirect_id,
                    pipeline_tasks.name,
                    pipeline_tasks.score, 
                    asset.name as asset_name,
                    asset.group as asset_group,
                    connection.id as connection_id,
                    connection.name as connection_name , 
                    connection.type as connection_type,
                    connection_type_id,
                    pipeline_tasks.database as database,
                    pipeline_tasks.schema as schema,
                    pipeline_tasks.status as status,
                    NULL::jsonb as properties
                from core.pipeline_tasks
                join core.asset on asset.id = pipeline_tasks.asset_id
                join core.connection on connection.id = asset.connection_id
                where pipeline_tasks.source_id = '{linked_entity}' and asset.id = '{asset_id}'
                UNION
                select  
                    LOWER(CONCAT (connection.id, '_', asset.id, '_', pipeline_transformations.source_id)) as id,
                    pipeline_transformations.source_id as entity, 
                    pipeline_transformations.id as redirect_id,
                    pipeline_transformations.name,
                    pipeline_transformations.score, 
                    asset.name as asset_name,
                    asset.group as asset_group,
                    connection.id as connection_id,
                    connection.name as connection_name , 
                    connection.type as connection_type,
                    connection_type_id,
                    pipeline_transformations.database as database,
                    pipeline_transformations.schema as schema,
                    pipeline_transformations.status as status,
                    pipeline_transformations.properties as properties
                from core.pipeline_transformations
                join core.asset on asset.id = pipeline_transformations.asset_id
                join core.connection on connection.id = asset.connection_id
                where pipeline_transformations.source_id = '{linked_entity}' and asset.id = '{asset_id}'
                UNION
                select  
                    LOWER(CONCAT (connection.id, '_', asset.id, '_', reports_views.source_id)) as id,
                    reports_views.source_id as entity, 
                    reports_views.id as redirect_id,
                    reports_views.name,
                    reports_views.score, 
                    asset.name as asset_name,
                    asset.group as asset_group,
                    connection.id as connection_id,
                    connection.name as connection_name , 
                    connection.type as connection_type,
                    connection_type_id,
                    NULL as database,
                    NULL as schema,
                    NULL as status,
                    NULL::jsonb as properties
                from core.reports_views
                join core.asset on asset.id = reports_views.asset_id
                join core.connection on connection.id = asset.connection_id
                where reports_views.source_id = '{linked_entity}' and asset.id = '{asset_id}'
            """
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                cursor.execute(query)
                table = fetchone(cursor)
                if table:
                    fields = []
                    if view_by == "column":
                        fields = get_lineage_table_columns(
                            config,
                            table.get("asset_group"), asset_id, linked_entity
                        )
                    tables.append(
                        {
                            "id": table.get("id"),
                            "entity": linked_entity,
                            "asset_id": asset_id,
                            "redirect_id": table.get("redirect_id"),
                            "connection_id": table.get("connection_id"),
                            "name": table.get("name"),
                            "asset_name": table.get("asset_name", ""),
                            "asset_group": table.get("asset_group", ""),
                            "connection_name": table.get("connection_name", ""),
                            "connection_type": table.get("connection_type", ""),
                            "connection_type_id": table.get("connection_type_id", ""),
                            "database": table.get("database", None),
                            "schema": table.get("schema", None),
                            "score": table.get("score", None),
                            "status": table.get("status", ""),
                            "type": type,
                            "flow": flow,
                            "is_auto": lineage_row.get("is_auto"),
                            "depth": lineage_row.get("depth"),
                            "is_transform": (
                                True
                                if ".transformation" in table.get("entity")
                                else False
                            ),
                            "fields": fields,
                            "is_dataflow": len(linked_entity.split(".")) == 3,
                        }
                    )
        return tables


def prepare_lineage_tables(config: dict, view_by, lineage_list: list) -> list:
        # Prepare Lineage Tables List
        tables = []
        for lineage in lineage_list:
            source_asset_id = lineage.get("source_asset_id")
            source_entity = lineage.get("source_entity")
            target_asset_id = lineage.get("target_asset_id")
            target_entity = lineage.get("target_entity")
            is_export    = lineage.get("is_export", False)

            # Get Source and Target Tables
            tables = get_lineage_table(
                config,
                view_by, lineage, "upstream", source_asset_id, source_entity, tables
            )
            if not is_export:
                tables = get_lineage_table(
                    config,
                    view_by, lineage, "downstream", target_asset_id, target_entity, tables
                )
            tables = get_lineage_entity_table(
                config,
                view_by, "upstream", source_asset_id, source_entity, tables
            )
            if not is_export:
                tables = get_lineage_entity_table(
                    config,
                    view_by, "downstream", target_asset_id, target_entity, tables
                )
        return tables

def get_lineage_entity_table(
        config: dict, view_by: str, flow: str, asset_id: str, linked_entity: str, tables: list
    ) -> list:
        asset_exists = next(
            (
                x
                for x in tables
                if x["entity"] == linked_entity and x["asset_id"] == asset_id
            ),
            None,
        )
        query = None
        if not asset_exists:
            query = f"""
                select 
                    LOWER(CONCAT (asset.connection_id, '_', asset.id, '_', entity_name)) as id,
                    lineage_entity.name, 
                    lineage_entity.connection_type as external_conn_type, 
                    lineage_entity.columns, 
                    lineage_entity.database, 
                    lineage_entity.schema, 
                    lineage_entity.entity_name,
                    asset.group,
                    case when lineage_entity.connection_type is not null then lineage_entity.connection_type else connection.type END AS connection_type,
                    connection_type_id,
                    pipeline_transformations.id as redirect_id,
                    pipeline_transformations.properties::jsonb as pt_properties,
                    COALESCE(lineage.is_auto, False) as is_auto
                from core.lineage_entity
                join core.asset on asset.id = lineage_entity.asset_id
                join core.connection on connection.id = asset.connection_id
                left join core.pipeline_transformations on pipeline_transformations.asset_id = lineage_entity.asset_id
                left join core.lineage on (
                    (lineage.source_asset_id = lineage_entity.asset_id AND lineage.source_entity = lineage_entity.entity_name)
                    OR 
                    (lineage.target_asset_id = lineage_entity.asset_id AND lineage.target_entity = lineage_entity.entity_name)
                ) AND lineage.is_active = true AND lineage.is_delete = false
                where lineage_entity.asset_id='{asset_id}' and entity_name='{linked_entity}'
             """
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                cursor.execute(query)
                table = fetchone(cursor)
                if table:
                    pipeline_transformation = (
                        json.loads(table.get("pt_properties"))
                        if table.get("pt_properties") and isinstance(table.get("pt_properties"), str)
                        else table.get("pt_properties","") or {}
                    )
                    pipeline_transformation_name = pipeline_transformation.get(
                        "dataflow_name", None
                    )
                    is_transform = (
                        table.get("connection_type") == "adf"
                        and table.get("group") == "pipeline"
                        and pipeline_transformation_name is not None
                        and table.get("schema") in ["", None]
                    )
                    is_dataflow = (
                        table.get("connection_type") == "snowflake"
                        and table.get("group") == "pipeline"
                    )
                    fields = []
                    if view_by == "column":
                        columns_data = table.get("columns", "[]")
                        fields = json.loads(columns_data if isinstance(columns_data, str) else "[]")
                    tables.append(
                        {
                            "id": table.get("id", ""),
                            "entity": linked_entity,
                            "asset_id": asset_id,
                            "redirect_id": table.get("redirect_id", ""),
                            "name": table.get("name"),
                            "asset_name": table.get("name", ""),
                            "connection_type": table.get("connection_type", ""),
                            "connection_type_id": table.get("connection_type_id", ""),
                            "external_conn_type": table.get("connection_type", ""),
                            "database": table.get("database", None),
                            "schema": table.get("schema", None),
                            "score": table.get("score", None),
                            "flow": flow,
                            "is_auto": table.get("is_auto", False),
                            "fields": fields,
                            "is_external": True,
                            "asset_group": table.get("group"),
                            "is_dataflow": is_dataflow,
                            "is_transform": is_transform,
                            "dataflow_name": (
                                pipeline_transformation_name.replace("_", " ").title()
                                if pipeline_transformation_name and is_transform
                                else ""
                            ),
                        }
                    )
        return tables



def get_linked_entities(config: dict, data: dict) -> list:
        connection_id = data.get("connection_id", None)
        asset_id = data.get("asset_id", None)
        entity = data.get("entity", None)
        entity_list = []
        entity_names = []
        asset_group = data.get("asset_group", None)
        if asset_group != 'pipeline':
            report_inf = f"""
                    select name from core.lineage_entity where asset_id='{ asset_id}' """
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                cursor.execute(report_inf)
                result = cursor.fetchall()
            entity_names = [row[0] for row in result] if result else []
            if entity_names:
                    entity_list = "', '".join(entity_names)
        condition_1 = f"  (source_connection_id = '{connection_id}' and source_asset_id = '{asset_id}' and source_entity ='{entity}' and source_entity NOT IN ('{entity_list}')) "
        condition_2 = f" (target_connection_id = '{connection_id}' and target_asset_id = '{asset_id}' and target_entity ='{entity}'and target_entity NOT IN ('{entity_list}')) "
        if asset_id == entity:
            if entity_names:
                condition_1 = f"  (source_connection_id = '{connection_id}' and source_asset_id = '{asset_id}' and source_entity NOT IN ('{entity_list}')) "
                condition_2 = f" (target_connection_id = '{connection_id}' and target_asset_id = '{asset_id}' and target_entity NOT IN ('{entity_list}')) "
            else:
                condition_1 = f"  (source_connection_id = '{connection_id}' and source_asset_id = '{asset_id}' ) "
                condition_2 = f" (target_connection_id = '{connection_id}' and target_asset_id = '{asset_id}' ) "
        tables = []
        query_string = f"""
            select
                *
            from core.lineage
            where 
                is_active = true and is_delete = false
                and (
                    {condition_1}
                    or 
                    {condition_2}
                )
        """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor.execute(query_string)
            lineage_list = fetchall(cursor)
            tables = prepare_lineage_tables(config, "column", lineage_list)
            tables = [x for x in tables if x.get("entity") != entity]
        return tables

def get_asset_metric_count(config: dict, asset_id: str) -> int:
    query_string = f"select alerts, issues from core.asset where id = '{asset_id}'"
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        cursor.execute(query_string)
        metrics_count = fetchone(cursor)
        metrics_count = metrics_count if metrics_count else {}
        return metrics_count

def update_asset_metric_count(config: dict, asset_id: str, metrics_count: dict, is_latest: bool, propagate_alerts: str = "") -> None:
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if is_latest and not propagate_alerts != "pipeline":
            query_string = f"select alerts from core.asset where id = '{asset_id}'"
        else:
            if propagate_alerts == "pipeline":
                query_string = f"""SELECT COUNT(*) FILTER ( WHERE LOWER(metrics.drift_status) IN ('high', 'medium', 'low')) AS  alerts
                                    FROM core.metrics
                                    WHERE asset_id = '{asset_id}'
                                    AND (measure_id IS NULL OR EXISTS (
                                            SELECT 1 FROM core.measure 
                                            WHERE id = metrics.measure_id AND last_run_id = metrics.run_id))"""
            else:
                query_string = f"""
                    select count(distinct metrics.id) as alerts
                    from core.metrics
                    join core.measure on measure.id = metrics.measure_id and measure.last_run_id  = metrics.run_id
                    where metrics.asset_id = '{asset_id}' 
                    and lower(metrics.drift_status) in ('high', 'medium', 'low')
                """
        execute_query(connection, cursor, query_string)
        asset_statistics = fetchone(cursor)
        asset_statistics = asset_statistics if asset_statistics else {}
        if asset_statistics:
            asset_alert_count = asset_statistics.get("alerts", 0)
            propagated_alert_count = metrics_count.get("alerts", 0) if metrics_count else 0
            alert_count = asset_alert_count + propagated_alert_count
            update_query_string = f"""
                update core.asset 
                set alerts = {alert_count}
                where id = '{asset_id}'
            """
            execute_query(connection, cursor, update_query_string)
            
def update_associated_asset_descriptions(config: dict, data: dict):
    
    try:
        # Extract data from request
        source_asset_id = data.get('source_asset')
        associated_asset_id = data.get('associated_asset')
        
        if not source_asset_id:
            log_info("No source_asset found in request data")
            return
            
        # Check if the source asset's connection type is 'dbt'
        connection_check_query = f"""
            SELECT c.type as connection_type
            FROM core.asset a
            JOIN core.connection c ON c.id = a.connection_id
            WHERE a.id = '{source_asset_id}'
        """
        
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, connection_check_query)
            connection_result = fetchone(cursor)
            
            if not connection_result or connection_result.get('connection_type') != 'dbt':
                log_info(f"Source asset {source_asset_id} is not connected to dbt (connection type: {connection_result.get('connection_type') if connection_result else 'None'})")
                return            
            # Get associated assets for the given source asset
            query_string = f"""
                SELECT DISTINCT
                    aa.associated_asset_id,
                    aa.source_id,
                    pt.description as pipeline_description
                FROM core.associated_asset aa
                JOIN core.pipeline_tasks pt ON pt.source_id = aa.source_id
                WHERE aa.source_asset_id = '{source_asset_id}'
                AND associated_asset_id = '{associated_asset_id}'
                AND pt.asset_id = '{source_asset_id}'
                  
            """
            cursor = execute_query(connection, cursor, query_string)
            associated_asset = fetchone(cursor)
            if associated_asset:
                
                associated_asset_id = associated_asset.get('associated_asset_id')
                pipeline_description = associated_asset.get('pipeline_description')
                
                if associated_asset_id:
                    # Update the asset description
                    update_query = f"""
                        UPDATE core.asset 
                        SET description = %s, 
                            modified_date = NOW()
                        WHERE id = %s
                        AND (description IS NULL OR TRIM(description) = '')
                    """
                    cursor.execute(update_query, (pipeline_description, associated_asset_id))
            
                # Commit the changes
                connection.commit()
            else:
                log_info(f"No associated assets found with pipeline descriptions for source asset {source_asset_id}")
            
            # Update column descriptions
            update_associated_column_descriptions(config, data, source_asset_id, associated_asset_id)
                
    except Exception as e:
        log_error(f"Error updating associated asset descriptions: {str(e)}", e)
        # Log the error but don't fail the lineage request
        pass


def update_associated_column_descriptions(config: dict, data: dict, source_asset_id: str, associated_asset_id: str):
    try:
        
        # Get columns that need description updates
        query_string = f"""
            SELECT DISTINCT
                at.id as attribute_id,
                at.name as attribute_name,
                pc.description as pipeline_column_description
            FROM core.attribute at
            JOIN core.pipeline_columns pc ON LOWER(pc.name) = LOWER(at.name)
            JOIN core.pipeline_tasks pt ON pc.pipeline_task_id = pt.id
            JOIN core.associated_asset aa ON pt.source_id = aa.source_id
            WHERE pt.asset_id = '{source_asset_id}'
              AND aa.source_asset_id = '{source_asset_id}'
              AND at.asset_id = '{associated_asset_id}'
        """
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            cursor = execute_query(connection, cursor, query_string)
            columns_to_update = fetchall(cursor)
            
            if columns_to_update:
                for column_data in columns_to_update:
                    attribute_id = column_data.get('attribute_id')
                    pipeline_column_description = column_data.get('pipeline_column_description')
                    if attribute_id:
                        # Update the attribute description
                        update_query = f"""
                            UPDATE core.attribute 
                            SET description = %s, 
                                modified_date = NOW()
                            WHERE id = %s
                            AND (description IS NULL OR TRIM(description) = '')
                        """
                        cursor.execute(update_query, (pipeline_column_description, attribute_id))
                
                # Commit the changes
                connection.commit()
                log_info(f"Updated descriptions for {len(columns_to_update)} columns")
            else:
                log_info(f"No columns found with pipeline descriptions for source_asset_id: {source_asset_id}, associated_asset_id: {associated_asset_id}")
                
    except Exception as e:
        log_error(f"Error updating associated column descriptions: {str(e)}", e)
        # Log the error but don't fail the lineage request
        pass


def update_materializes_asset_id(config: dict, materializes: dict, model_name: str = None, task_id: str = None) -> dict:
    """
    Resolve asset_id for a materialized object directly and update the materializes dict.
    Does not rely on get_asset_by_db_schema_name.
    """
    try:
        if not (materializes.get("database") and materializes.get("name")):
            return materializes

        db_name = materializes.get("database", "")
        schema_name = materializes.get("schema", "")
        table_name = materializes.get("name", "")

        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            # Build condition query
            condition_query = f"""
                AND lower(asset.name) = lower('{table_name}')
                AND lower(asset.properties->>'database') = lower('{db_name}')
            """
            if schema_name:
                condition_query += f" AND lower(asset.properties->>'schema') = lower('{schema_name}')"

            # Query to find matching asset
            query = f"""
                SELECT asset.id, asset.name, connection.name AS connection_name
                FROM core.asset
                JOIN core.connection ON connection.id = asset.connection_id
                WHERE asset.is_active = TRUE
                  AND asset.is_delete = FALSE
                  {condition_query}
                ORDER BY asset.id ASC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query)
            assets = fetchall(cursor)

            if assets:
                
                materializes["asset_id"] = assets[0].get("id")
                materializes["connection_name"] = assets[0].get("connection_name")
                updates = {}
                if model_name:
                    dbt_connection = config.get("connection", {})
                    dbt_connection_name = dbt_connection.get("name", "")
                    updates["materialized_by"] = model_name
                    updates["dbt_connection_name"] = dbt_connection_name
                if task_id:
                    updates["task_id"] = task_id

                if updates:
                    update_query = f"""
                        UPDATE core.asset
                        SET properties = properties || '{json.dumps(updates).replace("'", "''")}'::jsonb
                        WHERE id = '{assets[0].get("id")}'
                    """
                    execute_query(connection, cursor, update_query)

        return materializes

    except Exception as e:
        log_error("Failed to update materializes asset_id", e)
        return materializes


def get_task_ids_by_source_ids(config: dict, asset_id: str, source_ids: list) -> list:
    """
    Get task_ids by passing depends_on data as source_id
    """
    try:
        if not source_ids:
            return []
            
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Convert list to comma-separated string for SQL IN clause
            source_ids_str = "', '".join(source_ids)
            
            query_string = f"""
                SELECT id, source_id, name
                FROM core.pipeline_tasks 
                WHERE asset_id = '{asset_id}' 
                AND source_id IN ('{source_ids_str}')
                AND is_active = true 
                AND is_delete = false
            """
            
            cursor = execute_query(connection, cursor, query_string)
            results = fetchall(cursor)
            
            task_ids = []
            for result in results:
                task_ids.append({
                    'task_id': result.get('id'),
                    'source_id': result.get('source_id'),
                    'task_name': result.get('name')
                })
            
            return task_ids
            
    except Exception as e:
        log_error(f"DBT Connector - Get Task IDs By Source IDs Failed", e)
        return []


def dbt_exposures_tag_users_mapping(config: dict, credentials: dict, exposure_properties: dict, asset_id: str, task_ids: list = None):
    """
    Process exposure tags, users, and create tags_mapping for exposure and task dependencies
    """
    try:
        import datetime
        from uuid import uuid4
        
        tags = exposure_properties.get("tags", [])
        owner_data = exposure_properties.get("owner")

        if isinstance(owner_data, dict):
            owners_email = [owner_data.get("email")]
        elif isinstance(owner_data, list):
            owners_email = [owner.get("email") for owner in owner_data if isinstance(owner, dict)]
        else:
            owners_email = []
        users_names = []
        auto_tag_mapping = credentials.get("auto_mapping_tags", False)

        dbt_tags = tags
        exposure_properties["dbt_tags"] = dbt_tags
        exposure_entity_id = exposure_properties.get("exposure_entity_id")
        
        if tags and auto_tag_mapping:
            for tag in tags:
                connection = get_postgres_connection(config)
                with connection.cursor() as cursor:
                    query_string = f"""
                        select name from core.tags where lower(name) = '{tag.lower()}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    tag_data = fetchone(cursor)
                    if not tag_data:
                        tag_name = tag.lower()
                        tag_id = uuid4()
                        tags_inserting_query = f"""
                            insert into core.tags (id, name, technical_name, is_mask_data,description,organization_id, color, source, is_active, is_delete, parent_id, properties, created_date)
                            values ('{tag_id}', '{tag_name}', '{tag_name}', false, '', '{config.get("organization_id")}', '#64AAEF', 'exposure', true, false, null, null, NOW())
                        """
                        cursor = execute_query(connection, cursor, tags_inserting_query)
                    
                    # Insert tags_mapping for this tag and task_ids
                        if tag_id and task_ids:
                            # Delete existing mappings for this tag and exposure
                            delete_query = f"""
                                DELETE FROM core.tags_mapping 
                                WHERE level = 'exposure' AND asset_id = '{asset_id}' AND tags_id = '{tag_id}'
                            """
                            cursor = execute_query(connection, cursor, delete_query)
                            
                            # Insert new mappings for this tag
                            insert_objects = []
                            for task_info in task_ids:
                                query_input = (
                                    uuid4(),
                                    "exposure",  # level
                                    tag_id,  # tags_id (the actual tag ID from core.tags)
                                    asset_id,  # asset_id
                                    None,  # attribute_id (not applicable for exposure mapping)
                                    task_info.get('task_id'),  # task_id
                                    None,  # run_id (not applicable for exposure mapping)
                                    datetime.datetime.now(),
                                    exposure_entity_id if exposure_entity_id else None  # exposure_entity_id
                                )
                                input_literals = ", ".join(["%s"] * len(query_input))
                                query_param = cursor.mogrify(
                                    f"({input_literals})", query_input
                                ).decode("utf-8")
                                insert_objects.append(query_param)
                            
                            if insert_objects:
                                insert_objects = split_queries(insert_objects)
                                for input_values in insert_objects:
                                    try:
                                        query_input = ",".join(input_values)
                                        query_string = f"""
                                            INSERT INTO core.tags_mapping (
                                                id, level, tags_id, asset_id, attribute_id, task_id, run_id, created_date, exposure_entity_id
                                            ) VALUES {query_input}
                                        """
                                        cursor = execute_query(connection, cursor, query_string)
                                    except Exception as e:
                                        log_error('DBT Exposure Task Mapping Insert Failed', e)
        if owners_email:
            for owner_email in owners_email:
                connection = get_postgres_connection(config)
                with connection.cursor() as cursor:
                    query_string = f"""
                        select first_name, last_name from core.users where email = '{owner_email.lower()}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    user_data = fetchone(cursor)
                    if user_data:
                        first_name = user_data.get("first_name")
                        last_name = user_data.get("last_name")
                        user_name = f"{first_name} {last_name}"
                        users_names.append(user_name)
            exposure_properties["user_names"] = users_names
                
    except Exception as e:
        log_error(f"DBT Connector - Dbt Exposures Tag Mapping Failed", e)
        raise e


def map_exposure_with_lineage(config: dict, exposure_url: str, asset_id: str):
    """
    Map exposure with lineage by finding associated assets based on URL
    """
    try:
        from uuid import uuid4
        
        url1 = exposure_url
        url2 = None
        if '.com/' in exposure_url:
            url2 = exposure_url.split('.com/')[1]
        elif '.ai/' in exposure_url:
            url2 = exposure_url.split('.ai/')[1]
        else:
            url2 = exposure_url
        

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, name, asset_id,url from core.reports where (url like '%{url1}%' or url like '%{url2}%')
                """
            cursor = execute_query(connection, cursor, query_string)
            exposures = fetchall(cursor)
            
            if not exposures:
                query_string = f"""
                    select id, name, asset_id,url from core.reports_views where (url like '%{url1}%' or url like '%{url2}%')
                    """
                cursor = execute_query(connection, cursor, query_string)
                exposures = fetchall(cursor)
            
            if exposures:
                # Return list of asset_ids from all matching exposures
                asset_ids = [exposure.get("asset_id") for exposure in exposures if exposure.get("asset_id")]
                return asset_ids
            return []
    except Exception as e:
        log_error(f"DBT Connector - Map Exposure With Lineage Failed", e)
        return []