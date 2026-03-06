"""
Migration Notes From V2 to V3:
Migrations Completed
"""

import datetime
import json
import re
import copy
import sqlglot
from sqlglot import exp
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status, extract_table_name

from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.lineage_helper import (
    save_lineage,
    save_asset_lineage_mapping,
    save_lineage_entity,
    update_pipeline_propagations,
    handle_alerts_issues_propagation,
    get_asset_metric_count, update_asset_metric_count
)
from dqlabs.app_helper import agent_helper
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from uuid import uuid4
from dqlabs.app_constants.dq_constants import ADF_DATASETS_TYPES
from dqlabs.app_helper.pipeline_helper import update_pipeline_last_runs
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks

TASK_CONFIG = None


def extract_adf_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        global TASK_CONFIG
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        connection_metadata = credentials.get("metadata")
        column_relations = []
        transformations = []
        all_pipeline_runs = []
        copy_activity_sql = {}
        pipeline_tasks = []
        latest_run = False
        TASK_CONFIG = config

        is_valid, is_exist, e = __validate_connection_establish(asset_properties)
        if not is_exist:
            raise Exception(
                f"Pipeline - {asset_properties.get('name')} doesn't exist in the datafactory - {asset_properties.get('data_factory')} "
            )
        lineage = {"tables": [], "relations": []}

        if is_valid:
            data = asset_properties if asset_properties else {}
            pipeline_name = asset_properties.get("name")
            pipeline_info, description, stats = get_pipeline_info(
                data, pipeline_name, credentials
            )
            original_pipeline_info = copy.deepcopy(pipeline_info)
            pipeline_id = __get_pipeline_id(config)


            # get pipeline level lineage
            lineage, found_colums, transformations, lookup_entities = __get_pipeline_level_lineage(
                credentials, asset_properties, lineage, connection_metadata
            )
            data.update({"pipeline_info": pipeline_info, "stats": stats})

            # update description
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.asset set description='{description}'
                    where id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

            if connection_metadata.get("tasks"):
                # get column relations
                if found_colums:
                    for column in found_colums:
                        src_table_id = column.get("parent_entity_name")
                        target_table_id = column.get("parent_source_id")
                        if column.get("from_dataset") == "outbound":
                            src_table_id = column.get("parent_source_id")
                            target_table_id = column.get("parent_entity_name")
                        if column.get("from_dataset") == "transform":
                            src_table_id = column.get("parent_source_id")
                            target_table_id = column.get("parent_entity_name")
                        column_relations.append(
                            {
                                "srcTableId": src_table_id,
                                "tgtTableId": target_table_id,
                                "srcTableColName": column.get("name"),
                                "tgtTableColName": column.get("name"),
                            }
                        )
                # delete existing columns
                __delete_columns(config, pipeline_id)

                # save dataflows as tasks in pipeline_task table
                new_tasks, pipeline_tasks = __save_pipeline_dataflows(
                    config,
                    pipeline_info,
                    pipeline_id,
                    asset_id,
                    found_colums,
                    asset_properties,
                )

                # Create Pipeline Task Measures
                if new_tasks:
                    create_pipeline_task_measures(config, new_tasks)


            if connection_metadata.get("runs"):
                # save runs & its details
                result = __save_pipeline_runs(
                    config,
                    pipeline_info,
                    data,
                    asset_id,
                    asset_properties.get("id"),
                    pipeline_id,
                )
                if result:
                    all_pipeline_runs, latest_run, all_runs, copy_activity_sql = result
                # Update Job Run Stats
                __update_pipeline_stats(config)
                # Update Propagations Alerts and Issues Creation and Notifications for Tasks
                if latest_run and all_pipeline_runs:
                    # Get the latest run from database to ensure we have the most recent run_id
                    db_connection = get_postgres_connection(config)
                    with db_connection.cursor() as cursor:
                        query_string = f"""
                            select source_id
                            from core.pipeline_runs
                            where asset_id = '{asset_id}'
                            order by run_end_at desc
                            limit 1
                        """
                        cursor = execute_query(db_connection, cursor, query_string)
                        latest_run_result = fetchone(cursor)
                        latest_run_id = latest_run_result.get("source_id") if latest_run_result else ""
                        if latest_run_id:
                            handle_alerts_issues_propagation(config, latest_run_id)
            if connection_metadata.get("tasks"):
                if copy_activity_sql:
                    lineage = __process_copy_activity_source_lineage(asset_properties, original_pipeline_info, lineage, copy_activity_sql)
                    lineage = __process_copy_activity_sink_lineage(asset_properties, original_pipeline_info, lineage, copy_activity_sql)
                # save lineage entity (including lookup entities)
                lineage_tables = lineage.get("tables", [])
                # Combine lineage_tables with lookup_entities for saving
                all_lineage_entities = lineage_tables + lookup_entities if lookup_entities else lineage_tables
                if all_lineage_entities:
                    save_lineage_entity(config, all_lineage_entities, asset_id, True)

                # save columns
                __save_columns(config, found_colums, pipeline_id, pipeline_tasks)
                # generate Auto Mapping
                __map_asset_with_lineage(config, lineage)

            if connection_metadata.get("transform"):
                # save transformations
                __save_transformations(
                    config,
                    pipeline_info,
                    pipeline_id,
                    asset,
                    transformations,
                    asset_properties,
                    all_pipeline_runs,
                    connection_metadata.get("runs"),
                )

            # save lineage
            lineage["relations"] = lineage.get("relations") + column_relations
            save_lineage(config, "pipeline", lineage, asset_id)

            # save Propagation Values
            update_pipeline_propagations(config, asset, connection_type)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
        update_pipeline_last_runs(config)
        propagate_alerts = credentials.get("propagate_alerts", "table")
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(all_runs, connection_metadata.get("tasks"))
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("ADF Pipeline pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, error=e)
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)


def __get_response(method_name: str, params: dict = {}):
    try:
        global TASK_CONFIG
        pg_connection = get_postgres_connection(TASK_CONFIG)
        api_response = agent_helper.execute_query(
            TASK_CONFIG,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(method_name=method_name, request_params=params),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection_establish(config) -> tuple:
    try:
        is_valid = False
        is_exist = False

        subscription_id = config.get("subscription_id", "")
        resource_group = config.get("resource_group", "")
        factory_name = config.get("data_factory", "")
        pipeline_name = config.get("name", "")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
        )
        response = __get_response("validate_pipeline", params)
        is_exist = bool(response.get("is_exist"))
        is_valid = bool(response.get("is_valid"))
        return (bool(is_valid), bool(is_exist), "")
    except Exception as e:
        log_error("ADF Connector - Validate Connection Failed", e)
        return (is_valid, is_exist, str(e))


def get_pipeline_info(data, pipeline_name: str, credentials: dict) -> tuple:
    try:
        subscription_id = data.get("subscription_id", "")
        resource_grp_name = data.get("resource_group", "")
        data_factory = data.get("data_factory", "")
        no_of_runs = credentials.get("no_of_runs", 30)
        no_of_runs = int(no_of_runs) if type(no_of_runs) == str else no_of_runs
        status_filter = credentials.get("status", "all")
        days_ago = datetime.datetime.now() - datetime.timedelta(days=no_of_runs)
        days_ago = days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_grp_name,
            factory_name=data_factory,
            pipeline_name=pipeline_name,
            last_update_after=days_ago,
            last_update_before=current_timestamp,
            pipeline_status=status_filter,
            pipeline_meta=credentials.get("metadata", {}),
        )
        response = __get_response("get_pipeline_info", params)
        pipeline_info = response.get("pipeline_info")
        description = (
            response.get("description")
            if response.get("description")
            else __prepare_description(
                {
                    "name": pipeline_name,
                    "data_factory": data_factory,
                    "resource_group": resource_grp_name,
                }
            )
        )
        stats = response.get("stats")
    except Exception as e:
        log_error("ADF Connector - Get Pipeline info Failed", e)
    finally:
        return pipeline_info, description, stats


def __process_column_level_lineage(temp, scriptLines):
    # below code for column level mapping
    mappedColumns = []
    mappedData = []
    if scriptLines:
        scriptLines = "".join([x.strip() for x in scriptLines])
        if "mapColumn" in scriptLines:
            splittedLines = scriptLines.split("mapColumn(")[1]
            if ")," in splittedLines:
                mappedColumns = splittedLines.split("),")[0]
                mappedColumns = [x for x in mappedColumns.split(",")]

    # process mapped columns
    if mappedColumns:
        source = [x for x in temp["tables"] if x.get("dataset_type") == "source"]
        sink = [x for x in temp["tables"] if x.get("dataset_type") == "sink"]
        transform_columns = [
            x for x in temp["tables"] if x.get("dataset_type") == "transform"
        ]
        for column in mappedColumns:
            source_id = ""
            sink_id = ""
            source_column_name = ""
            sink_column_name = ""
            source_fields = []
            sink_fields = []
            if source:
                source_id = source[0].get("id", "")
                source_fields = source[0].get("fields", [])

            if sink:
                sink_id = sink[0].get("id", "")
                sink_fields = sink[0].get("fields", [])

            if "=" not in column:
                if source_fields:
                    column_exists = [
                        y for y in source_fields if y.get("name") == column
                    ]
                    if column_exists:
                        source_column_name = column

                if sink_fields:
                    column_exists = [y for y in sink_fields if y.get("name") == column]
                    if column_exists:
                        sink_column_name = column
            else:
                split_names = column.split(" = ")
                source_column_name = split_names[0]
                sink_column_name = split_names[1]
            if source_id and sink_id and source_column_name and sink_column_name:
                mappedData.append(
                    {
                        "srcTableId": source_id,
                        "tgtTableId": sink_id,
                        "srcTableColName": source_column_name,
                        "tgtTableColName": sink_column_name,
                    }
                )
    return mappedData


def __process_dataflow_detail(
    data,
    subscription_id,
    resource_group_name,
    factory_name,
    params,
    parent_table_id=None,
    dataflowGeneralName=None,
    dataflowSourceId=None,
):
    temp = {}
    nested_dataflow_unique_id = str(uuid4())
    temp["id"] = nested_dataflow_unique_id
    temp["source_type"] = "Dataset"
    temp["source_id"] = dataflowSourceId
    temp["entity_name"] = data.get("name")
    temp["name"] = data.get("name")
    temp["tables"] = []
    temp["relations"] = []
    scriptLines = []
    sinks_ids = []
    columns = []
    pool_ids = []
    parent_relations = []
    children_ids = []
    children_relation_objects = []
    sink_columns_ = []
    transform_columns = parse_dataflow_json(data)
    if data.get("properties"):
        typeProperties = data.get("properties").get("typeProperties")
        scriptLines = typeProperties.get("scriptLines")
        transformations = typeProperties.get("transformations")
        temp["transformations"] = transformations
        temp["transformation_scripts"] = scriptLines
        # process for sources & get fields
        if typeProperties.get("sources"):
            for source in typeProperties.get("sources"):
                source_temp = {}
                source_unique_id = str(uuid4())
                children_ids.append(source_unique_id)

                source_temp["id"] = source_unique_id
                source_display_name = (
                    source.get("dataset", {}).get("referenceName", "")
                    if source and source.get("dataset") and source.get("dataset").get("type") == "DatasetReference"
                    else ""
                )
                if not source_display_name:
                    continue

                source_temp["name"] = source_display_name
                source_temp["dataset_name"] = source.get("name")
                source_temp["source_type"] = "Table"
                source_temp["fields"] = []
                source_temp["type"] = "table"
                source_temp["dataset_type"] = "source"
                source_temp["level"] = 3
                source_temp["connection_type"] = "adf"
                source_temp["source_id"] = dataflowSourceId
                source_temp["entity_name"] = f"{source_display_name}.source"

                params["request_url"] = (
                    f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{source_display_name}"""
                )
                dataset_detail = __get_response("execute", params)
                if dataset_detail:
                    dataset_props = dataset_detail.get("properties")
                    if dataset_props:
                        dataset_schema = dataset_props.get("schema")
                        source_temp["dataset_source"] = dataset_props.get("type")
                        source_temp["dataset_properties"] = dataset_props.get(
                            "typeProperties"
                        )
                        if dataset_schema:
                            dataset_table = dataset_props.get("typeProperties").get(
                                "table", ""
                            )
                            source_name = (
                                f"""{source_display_name} ({dataset_table})"""
                                if dataset_table
                                else source_display_name
                            )
                            source_temp["name"] = source_name
                            source_temp["entity_name"] = f"{source_name}.source"

                            temp_unique_schema_id = f"""{dataset_table}_{dataset_props.get("typeProperties").get("schema", "")}_{dataset_props.get('linkedServiceName').get('referenceName')}"""
                            # get dataset linked service detail
                            params["request_url"] = (
                                f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{dataset_props.get("linkedServiceName").get("referenceName")}"""
                            )
                            linked_services_detail = __get_response("execute", params)
                            if linked_services_detail:
                                properties = linked_services_detail.get("properties", {})
                                linked_service_type = properties.get("type", "").lower()
                                if linked_service_type == "snowflakev2":
                                    source_temp["connection_type"] = "snowflake"
                                elif linked_service_type == "sqlserver":
                                    source_temp["connection_type"] = "mssql"

                                source_temp["dataset_linked_service"] = (
                                    linked_services_detail.get("properties").get(
                                        "typeProperties"
                                    )
                                    if linked_services_detail.get("properties")
                                    else {}
                                )
                            updated_source_schema = [
                                dict(
                                    item,
                                    **{
                                        "id": str(uuid4()),
                                        "type": "column",
                                        "column_type": item.get("type", ""),
                                        "score": 0,
                                        "table": dataset_table,
                                        "dataset_name": dataset_props.get(
                                            "linkedServiceName"
                                        ).get("referenceName"),
                                        "schema": dataset_props.get(
                                            "typeProperties"
                                        ).get("schema", ""),
                                        "from_dataset": "inbound",
                                        "dataflow_name": dataflowGeneralName,
                                        "parent_entity_name": source_temp[
                                            "entity_name"
                                        ],
                                        "parent_source_id": dataflowSourceId,
                                        "transformation_name": source.get("name"),
                                        "task_id": dataflowSourceId
                                    },
                                )
                                for item in dataset_schema
                            ]
                            # source_temp["fields"] = []

                            source_temp["fields"] = updated_source_schema
                            columns.extend(updated_source_schema)
                temp["relations"].append(
                    {"srcTableId": source_unique_id, "tgtTableId": ""}
                )
                temp["relations"].append(
                    {
                        "srcTableId": source_temp["entity_name"],
                        "tgtTableId": parent_table_id,
                    }
                )

                children_relation_objects.append(
                    {
                        "id": source_unique_id,
                        "type": "source",
                        "parent": dataflowSourceId,
                        "name": source_temp["entity_name"],
                    }
                )
                if transformations:
                    parent = dataflowSourceId
                    for each_transform in transformations:
                        transform_id = str(uuid4())
                        transform_name = each_transform.get("name", "")
                        ob = {
                            "id": transform_id,
                            "type": "transform",
                            "parent": parent,
                            "name": transform_name,
                        }
                        children_ids.append(transform_id)
                        children_relation_objects.append(ob)
                        
                        # form columns
                        transform_columns = next(
                            (stage["columns"] if "columns" in stage else None for stage in transform_columns if stage["name"] == transform_name),
                            [],
                        )
                        transform_fields = [
                            {
                                "id": str(uuid4()),
                                "type": item.get("type", ""),
                                "name": item.get("name", ""),
                                "column_type": item.get("type", ""),
                                "score": 0,
                                "table": "",
                                "type": "string",
                                "dataset_name": "",
                                "schema": "",
                                "from_dataset": "transform",
                                "dataflow_name": dataflowGeneralName,
                                "parent_entity_name": transform_name,
                                "parent_source_id": transform_name,
                                "transformation_name": transform_name,
                                "lineage_entity_id": transform_id,
                                "task_id": dataflowSourceId
                            }
                            for item in transform_columns
                        ]
                        temp["tables"].append(
                            {
                                "id": transform_id,
                                "name": transform_name,
                                "dataset_name": transform_name,
                                "source_type": "Table",
                                "type": "table",
                                "dataset_type": "transform",
                                "level": 4,
                                "connection_type": "adf",
                                "source_id": dataflowSourceId,
                                "entity_name": transform_name,
                                "dataset_source": "",
                                "lineage_entity_id": transform_id,
                                "fields": transform_fields,
                            }
                        )
                        columns.extend(transform_fields)

                        parent = ob["name"]
                        sink_columns_ = transform_fields
                temp["tables"].append(source_temp)

        # process for sink & get fields if available
        if typeProperties.get("sinks"):
            for sink in typeProperties.get("sinks"):
                sink_temp = {}
                sink_unique_id = str(uuid4())
                sink_temp["id"] = sink_unique_id
                children_ids.append(sink_unique_id)

                sink_display_name = (
                    sink.get("dataset", {}).get("referenceName", "")
                    if sink and sink.get("dataset") and sink.get("dataset").get("type") == "DatasetReference"
                    else ""
                )
                if sink_display_name:
                    sink_temp["name"] = sink_display_name
                    sink_temp["dataset_name"] = sink.get("name")
                    sink_temp["source_type"] = "Table"
                    sink_temp["fields"] = []
                    sink_temp["type"] = "table"
                    sink_temp["dataset_type"] = "sink"
                    sink_temp["level"] = 2
                    sink_temp["connection_type"] = "adf"
                    sink_temp["source_id"] = dataflowSourceId
                    sink_temp["entity_name"] = f"{sink_display_name}.sink"

                    params["request_url"] = (
                        f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{sink_display_name}"""
                    )
                    dataset_detail = __get_response("execute", params)
                    if dataset_detail:
                        sinks_ids.append(sink_unique_id)
                        dataset_props = dataset_detail.get("properties")
                        if dataset_props:
                            dataset_schema = dataset_props.get("schema")
                            sink_temp["dataset_source"] = dataset_props.get("type")
                            sink_temp["dataset_properties"] = dataset_props.get(
                                "typeProperties"
                            )
                            dataset_table = dataset_props.get("typeProperties").get(
                                "table", ""
                            )
                            if isinstance(dataset_table, dict):
                                dataset_table = dataset_table.get("value", "")
                            sink_name = (
                                f"""{sink_display_name} ({dataset_table})"""
                                if dataset_table
                                else sink_display_name
                            )
                            sink_temp["name"] = sink_name
                            sink_temp["entity_name"] = f"{sink_name}.sink"
                            if not dataset_schema:
                                outbound_columns = []
                                for each_column in sink_columns_:
                                    outbound_columns.append({
                                        "id": str(uuid4()),
                                        "table" :  dataset_table,
                                        "dataset_name" : dataset_props.get(
                                            "linkedServiceName"
                                        ).get("referenceName", ""),
                                        "schema" : dataset_props.get(
                                            "typeProperties"
                                        ).get("schema", ""),
                                        "from_dataset" : "outbound",
                                        "dataflow_name" : dataflowGeneralName,
                                        "parent_entity_name" : sink_temp.get("entity_name", ""),
                                        "type": each_column.get("type", ""),
                                        "name": each_column.get("name", ""),
                                        "column_type": each_column.get("type", ""),
                                        "score": 0,
                                        "parent_source_id": transform_name,
                                        "transformation_name": transform_name,
                                        "lineage_entity_id": transform_id,
                                        "task_id": dataflowSourceId
                                    })
                                
                            
                                sink_temp["fields"] = outbound_columns
                                columns.extend(outbound_columns)
                            else:
                                temp_unique__sink_schema_id = f"""{dataset_table}_{dataset_props.get("typeProperties").get("schema", "")}_{dataset_props.get('linkedServiceName').get('referenceName')}"""
                                # get dataset linked service detail
                                params["request_url"] = (
                                    f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{dataset_props.get("linkedServiceName").get("referenceName")}"""
                                )
                                linked_services_detail = __get_response(
                                    "execute", params
                                )
                                if linked_services_detail:
                                    properties = linked_services_detail.get("properties", {})
                                    linked_service_type = properties.get("type", "").lower()
                                    sink_temp["dataset_linked_service"] = (
                                        linked_services_detail.get("properties").get(
                                            "typeProperties"
                                        )
                                        if linked_services_detail.get("properties")
                                        else {}
                                    )
                                    if linked_service_type == "snowflakev2":
                                        sink_temp["connection_type"] = "snowflake"
                                    elif linked_service_type == "sqlserver":
                                        sink_temp["connection_type"] = "mssql"
                                updated_sink_schema = [
                                    dict(
                                        item,
                                        **{
                                            "id": str(uuid4()),
                                            "type": "column",
                                            "column_type": item.get("type", ""),
                                            "score": 0,
                                            "table": dataset_table,
                                            "dataset_name": dataset_props.get(
                                                "linkedServiceName"
                                            ).get("referenceName"),
                                            "schema": dataset_props.get(
                                                "typeProperties"
                                            ).get("schema", ""),
                                            "from_dataset": "outbound",
                                            "dataflow_name": dataflowGeneralName,
                                            "parent_entity_name": sink_temp[
                                                "entity_name"
                                            ],
                                            "parent_source_id": dataflowSourceId,
                                            "transformation_name": sink.get("name", ''),
                                            "task_id": dataflowSourceId
                                        },
                                    )
                                    for item in dataset_schema
                                ]
                                # sink_temp["fields"] = updated_sink_schema
                                outbound_columns = []
                                for each_column in sink_columns_:
                                    outbound_columns.append({
                                        "id": str(uuid4()),
                                        "table" :  dataset_table,
                                        "dataset_name" : dataset_props.get(
                                            "linkedServiceName"
                                        ).get("referenceName", ""),
                                        "schema" : dataset_props.get(
                                            "typeProperties"
                                        ).get("schema", ""),
                                        "from_dataset" : "outbound",
                                        "dataflow_name" : dataflowGeneralName,
                                        "parent_entity_name" : sink_temp.get("entity_name", ""),
                                        "type": each_column.get("type", ""),
                                        "name": each_column.get("name", ""),
                                        "column_type": each_column.get("type", ""),
                                        "score": 0,
                                        "parent_source_id": transform_name,
                                        "transformation_name": transform_name,
                                        "lineage_entity_id": transform_id,
                                        "task_id": dataflowSourceId
                                    })
                                
                                sink_temp["fields"] = outbound_columns
                                # append columns
                                if temp_unique__sink_schema_id not in pool_ids:
                                    pool_ids.append(temp_unique__sink_schema_id)
                                    # columns.extend(updated_sink_schema)
                                    columns.extend(outbound_columns)       
                    children_relation_objects.append(
                        {
                            "id": sink_unique_id,
                            "type": "sink",
                            "parent": dataflowSourceId,
                            "name": sink_temp["entity_name"],
                        }
                    )
                temp["tables"].append(sink_temp)

    new_relations = []
    temp_source_table_id = ""
    for relation in children_relation_objects:
        relation_type = relation.get("type")
        target_table_id = relation.get("parent")
        source_table_id = relation.get("name")
        if relation_type == "source":
            new_relations.append(
                {"tgtTableId": target_table_id, "srcTableId": source_table_id}
            )
        elif relation_type == "transform":
            new_relations.append(
                {"tgtTableId": source_table_id, "srcTableId": target_table_id}
            )
            # Swap values for the next iteration without overriding
            temp_source_table_id = source_table_id
        elif relation_type == "sink":
            new_relations.append(
                {"tgtTableId": source_table_id, "srcTableId": temp_source_table_id}
            )
    temp["relations"] = new_relations
    return temp, columns, children_ids


def __get_asset_by_db_schema_name(
    config, db_name, schema_name, table_name, connection_type
) -> list:
    assets = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select asset.id, asset.name from core.asset
                join core.connection on connection.id = asset.connection_id
                where asset.is_active = true and asset.is_delete = false
                and asset.name = '{table_name}'
                and lower(connection.type) = lower('{connection_type}')
                and asset.properties->>'schema' = '{schema_name}'
                and (asset.properties->>'database' = '{db_name}' OR connection.credentials->>'database' = '{db_name}')
                order by asset.created_date desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            assets = fetchall(cursor)
    except Exception as e:
        log_error("ADF Get Asset By Database, Schema and Name Failed ", e)
    finally:
        return assets


def __map_asset_with_lineage(config, lineage):
    # DQscore Based On Asset Matching
    try:
        table_ids = []
        lineage_dataflow_tables = lineage.get("tables", [])
        if not lineage_dataflow_tables:
            return

        for table in lineage_dataflow_tables:
            if "dataset_type" in table and "dataset_source" in table:
                table_source_type = table.get("dataset_source")
                connection_type = ADF_DATASETS_TYPES.get(table_source_type, "")
                if connection_type:
                    table_properties = table.get("dataset_properties", {})
                    table_schema = table_properties.get("schema", "")
                    table_name = table_properties.get("table", "")
                    if isinstance(table_name, dict):
                        table_name = table_name.get("value", "") if table_name.get("type") != "Expression" else table.get("table", "")
                    if isinstance(table_schema, dict):
                        table_schema = table_schema.get("value", "") if table_schema.get("type") != "Expression" else table.get("schema", "")
                    database = table.get("dataset_linked_service", {}).get(
                        "database", ""
                    )
                    if not database:
                        database = table.get("database", '')
                    if table_name and table_schema and database:
                        unique_id = f"{table_source_type}_{table_schema}_{table_name}_{table.get('id')}"
                        if unique_id not in table_ids:
                            table_ids.append(unique_id)
                            assets = __get_asset_by_db_schema_name(
                                config,
                                database,
                                table_schema,
                                table_name,
                                connection_type,
                            )
                            if assets:
                                save_asset_lineage_mapping(
                                    config, "pipeline", table, assets, True
                                )
    except Exception as e:
        log_error("Error in mapping asset with lineage", e)


def __get_pipeline_level_lineage(
    credentials, asset_properties, lineage, connection_metadata
):
    tables = lineage.get("tables", [])
    relations = lineage.get("relations", [])
    found_colums = []
    transformations = []
    subscription_id = asset_properties.get("subscription_id", "")
    resource_group_name = asset_properties.get("resource_group", "")
    data_factory = asset_properties.get("data_factory", "")
    pipeline_name = asset_properties.get("name", "")

    params = dict(
        subscription_id=subscription_id,
        resource_group=resource_group_name,
        factory_name=data_factory,
        pipeline_name=pipeline_name,
        request_url=f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{data_factory}/pipelines/{pipeline_name}""",
        request_type="get",
        api_version="2018-06-01",
    )
    pipeline_info = __get_response("execute", params)
    pipeline_activities = []
    if pipeline_info.get("properties"):
        pipeline_activities = pipeline_info.get("properties").get("activities")
    if not len(pipeline_activities):
        return lineage, found_colums, transformations, []

    nested_tables_relations = []
    lookup_entities = []  # Collect lookup entities from all activities
    for each_activity in pipeline_activities:

        temp, found_colums, nested_tables_relations, transformations, activity_lookup_entities = __get_pipeline_task_info(
            each_activity,
            pipeline_info,
            connection_metadata,
            subscription_id,
            resource_group_name,
            data_factory,
            params,
            tables,
            nested_tables_relations,
            found_colums,
        )
        # append temp variable to tables list
        tables.append(temp)
        # Collect lookup entities
        if activity_lookup_entities:
            lookup_entities.extend(activity_lookup_entities)

    # process for relations
    if len(tables):
        for each_table in tables:
            dependsOn = each_table.get("dependsOn")
            if dependsOn:
                for each_dependency in dependsOn:
                    parent_dataflow_display_name = each_dependency.get("activity")
                    get_matched_table = [
                        x
                        for x in tables
                        if x.get("name") == parent_dataflow_display_name
                    ]
                    if len(get_matched_table):
                        # Use source_id for consistency - dataflow becomes source, dependent activity becomes target
                        parent_table = get_matched_table[0]
                        src_id = parent_table.get("source_id") if parent_table.get("source_id") else parent_table.get("id")
                        tgt_id = each_table.get("source_id") if each_table.get("source_id") else each_table.get("id")
                        if src_id and tgt_id:
                            relations.append(
                                {
                                    "srcTableId": src_id,
                                    "tgtTableId": tgt_id,
                                }
                            )
    # comment below extend if need to have pop-up lineage back in action
    relations.extend(nested_tables_relations)
    lineage.update({"tables": tables, "relations": relations})
    return lineage, found_colums, transformations, lookup_entities


def __save_transformations(
    config,
    pipeline_info,
    pipeline_id,
    asset,
    transformation_data: list = [],
    pipeline_properties: dict = {},
    all_pipeline_runs: list = [],
    is_run_enabled: bool = False,
):
    # Transformations implementation
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        asset_id = asset.get("id")
        with connection.cursor() as cursor:
            query_string = f"""
                delete from core.pipeline_transformations
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            insert_objects = []
            last_run_detail = all_pipeline_runs[-1] if is_run_enabled and all_pipeline_runs else ()
            for each_task in transformation_data:
                for item in each_task.get("transformations", []):
                    each_task["transformation_name"] = item.get("name", "")
                    if item:
                        source_id = item.get("source_id", "") if "source_id" in item else each_task.get("source_id", "")
                        source_type = "transform"
                        source_id = f"""{source_id}.{item.get("name", "")}.transformation"""
                        query_input = (
                            uuid4(),
                            item.get("name", ""),
                            item.get("description", "") if "description" in item else __prepare_description(dict(each_task, **pipeline_properties), "transformation"),
                            last_run_detail[2] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else "",
                            json.dumps(item.get("properties", {}), default=str).replace("'", "''") if "properties" in item else json.dumps({"dataflow_name": each_task.get("source_id").split(".")[2]}, default=str),
                            source_id,
                            pipeline_id,
                            source_type,
                            asset_id,
                            connection_id,
                            True,
                            False,
                            datetime.datetime.now(),
                            datetime.datetime.now(),
                            last_run_detail[3] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else "",
                            last_run_detail[4] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else "",
                            last_run_detail[5] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else None,
                            last_run_detail[6] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else None,
                            last_run_detail[7] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else 0,
                            last_run_detail[6] if is_run_enabled and last_run_detail and 'is_update' not in last_run_detail else None
                        )

                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals})", query_input
                        ).decode("utf-8")
                        insert_objects.append(query_param)

            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_transformations(
                            id, name, description, run_id, properties, source_id,
                            pipeline_id, source_type, asset_id, connection_id, is_active, is_delete,
                            created_at, updated_at, status, error, run_start_at, run_end_at, 
                            duration, last_run_at
                        ) values {query_input}
                    """
                    cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"ADF Connector - Get Pipeline Transformations", e)
        raise e


def __save_pipeline_runs(
    config: dict,
    pipeline_info: list,
    data: dict,
    asset_id: str,
    source_asset_id: str,
    pipeline_id: str,
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        runs = pipeline_info.get("pipeline_runs", [])
        all_runs = []
        copy_activity_sql = []
        if not connection_id or not runs:
            return
        pipeline_runs = []
        all_pipeline_runs = []
        latest_run = False
        with connection.cursor() as cursor:
            # Clear Existing Runs Details
            insert_objects = []
            for i, item in enumerate(runs):
                run_id = item.get("id")
                source_id = f"""dataflow.{item.get('id').lower()}.{item.get('dequeued_at').lower().replace(" ","_")}"""
                status_humanized = (
                    item.get("status_humanized")
                    if item.get("status_humanized")
                    else item.get("status")
                )
                duration = (
                    item.get("duration")
                    if item.get("duration")
                    else item.get("durationInMs")
                )  # in miliseconds
                duration = duration / 1000 if duration else 0  # in seconds
                triggered_by = item.get("triggered_by", "")
                individual_run = {
                    "id": item.get("id"),
                    "trigger_id": item.get("trigger_id"),
                    "triggered_by": triggered_by,
                    "account_id": item.get("account_id"),
                    "environment_id": item.get("environment_id"),
                    "project_id": item.get("project_id"),
                    "project_name": data.get("project_name", ""),
                    "runGeneratedAt": (
                        item.get("finished_at")
                        if not data.get("runGeneratedAt", "")
                        else item.get("finished_at")
                    ),
                    "environment_name": data.get("environment_name", ""),
                    "job_definition_id": item.get("job_definition_id"),
                    "status": item.get("status"),
                    "status_humanized": status_humanized,
                    "dbt_version": item.get("dbt_version"),
                    "git_branch": item.get("git_branch"),
                    "git_sha": item.get("git_sha"),
                    "status_message": item.get("status_message"),
                    "artifact_s3_path": item.get("artifact_s3_path"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "dequeued_at": item.get("id"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "job": item.get("job"),
                    "environment": item.get("environment"),
                    "run_steps": item.get("run_steps"),
                    "in_progress": item.get("in_progress"),
                    "is_complete": item.get("is_complete"),
                    "is_success": item.get("is_success"),
                    "is_error": item.get("is_error"),
                    "is_cancelled": item.get("is_cancelled"),
                    "duration": duration,
                    "run_duration": item.get("run_duration", ""),
                    "job_id": source_id,
                    "unique_id": source_id,
                    "is_running": item.get("is_running", ""),
                    "href": item.get("href", ""),
                    "uniqueId": source_id,
                    "unique_job_id": item.get("job_id"),
                    "models": [
                        {**each_activity, "uniqueId": source_id}
                        for each_activity in item.get("dataflow_activities", [])
                    ],
                }
                copy_activity_sql = []
                # Only execute for the last loop iteration
                if i == len(runs) - 1:
                    for each_activity in item.get("dataflow_activities", []):
                        if each_activity.get("type") == "Copy":
                            copy_activity_sql.append({
                                "query": each_activity.get("run_input_info", {}).get("source", {}).get("sqlReaderQuery", ''),
                                "activity_name": each_activity.get("name"),
                                "connection_type": each_activity.get("run_input_info", {}).get("source", {}).get("type", ''),
                                "type": each_activity.get("type"),
                                "properties": each_activity
                            })

                # make properties
                properties = individual_run

                # Validating existing runs
                query_string = f"""
                    select id from core.pipeline_runs
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and source_id = '{item.get('job_id')}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)
                existing_run = existing_run.get("id") if existing_run else None
                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{get_pipeline_status(status_humanized.lower())}', 
                            error = '{item.get('error', '')}',
                            run_start_at = {f"'{item.get('started_at')}'" if item.get('started_at') else 'NULL'},
                            run_end_at = {f"'{item.get('finished_at')}'" if item.get('finished_at') else 'NULL'},
                            duration = '{duration}' ,
                            triggered_by = '{triggered_by}',
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": item.get("job_id"),
                            "is_update": True,
                        }
                    )
                    all_pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": item.get("job_id"),
                            "is_update": True,
                        }
                    )
                else:
                    query_input = (
                        uuid4(),
                        item.get("job_id"),
                        run_id,
                        get_pipeline_status(status_humanized.lower()),
                        item.get("error", ""),
                        item.get("started_at") if item.get("started_at") else None,
                        item.get("finished_at") if item.get("finished_at") else None,
                        duration,
                        triggered_by,
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    all_pipeline_runs.append(query_input)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if not latest_run:
                        latest_run = True
                all_runs.append(individual_run)

            insert_objects = split_queries(insert_objects)

            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            triggered_by, properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs_data = fetchall(cursor)
                    pipeline_runs.extend(pipeline_runs_data)
                except Exception as e:
                    log_error("ADF Jobs Runs Insert Failed  ", e)
            # save individual run detail
            __save_runs_details(config, all_runs, pipeline_id, pipeline_runs)
            return all_pipeline_runs, latest_run, all_runs, copy_activity_sql
    except Exception as e:
        log_error(str(e), e)


def __get_pipeline_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            if pipeline:
                return pipeline.get("id")
    except Exception as e:
        log_error(
            f"ADF Connector - Get Pipeline Primary Key Information By Asset ID Failed ",
            e,
        )
        raise e


def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    return next((item[get_key] for item in data if item[filter_key] == value), None)


def __save_columns(
    config: dict, columns: list, pipeline_id: str, pipeline_tasks: list = None
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                delete from core.pipeline_columns
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            if not columns:
                return

            for column in columns:
                properties = {
                    "id": column.get("id"),
                    "table": column.get("table"),
                    "dataset_name": column.get("dataset_name"),
                    "schema": column.get("schema"),
                    "from_dataset": column.get("from_dataset"),
                    "dataflow_name": column.get("dataflow_name"),
                    "transformation_name": column.get("transformation_name", ""),
                }
                pipeline_task_id = get_data_by_key(
                    pipeline_tasks, column.get("task_id")
                )
                query_input = (
                    uuid4(),
                    column.get("name"),
                    column.get("description", ""),
                    column.get("comment"),
                    column.get("column_type"),
                    column.get("tags") if column.get("tags") else [],
                    True,
                    False,
                    pipeline_task_id,
                    pipeline_id,
                    asset_id,
                    connection_id,
                    json.dumps(properties, default=str).replace("'", "''"),
                    column.get("lineage_entity_id", None),
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(f"({input_literals})", query_input).decode(
                    "utf-8"
                )
                insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_columns(
                            id, name, description,  comment, data_type, tags,
                            is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id,
                            properties, lineage_entity_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("ADF Datasets Columns Insert Failed  ", e)
    except Exception as e:
        log_error(f"ADF Connector - Save Tasks Columns By Job ID Failed ", e)
        raise e


def __save_pipeline_dataflows(
    config: dict,
    pipeline_info: dict,
    pipeline_id: str,
    asset_id: str,
    columns: list = [],
    pipeline_properties: dict = {},
) -> None:
    """
    Save / Update Jobs For Pipeline
    """
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        status =''
        last_run_id=''
        error =''
        source_type = "dataflow"
        pipeline_runs = pipeline_info.get("pipeline_runs")
        dataflows = pipeline_info.get("activities")
        if (len(pipeline_runs)>0):
            last_pipeline_run_data = pipeline_runs[-1]
        tasks = []
        pipeline_tasks = []
        if not dataflows:
            return

        with connection.cursor() as cursor:
            for task in dataflows:
                if task.get("type") == "ForEach" and len(task.get("activities",[])) > 0:
                    dataflows.extend(task.get("activities",[]))
                    for act in task.get("activities", []):
                        if act.get("type") == "IfCondition":
                            dataflows.extend(act.get("if_true_activities", []))
                            dataflows.extend(act.get("if_false_activities", []))
                task_type = task.get("type", "").lower()
                task_type = task_type if task_type and task_type != "executedataflow" else "dataflow"
                task_id =f"""{task_type}.{pipeline_info.get('name').lower()}.{task.get('name').lower().replace(" ","_")}"""
                if (len(pipeline_runs)>0):
                    dataflow_activities = last_pipeline_run_data.get('dataflow_activities', [])
                    for activity in dataflow_activities:
                        if activity.get('name', '') == task.get('name',''):
                            task_duration = (
                                activity.get("duration")
                                if activity.get("duration")
                                else activity.get("durationInMs")
                            )  # in miliseconds
                            task_duration = task_duration / 1000 if task_duration else 0
                            status_humanized = (
                                activity.get("status_humanized")
                                if activity.get("status_humanized")
                                else activity.get("status")
                            )
                            run_start_at = (
                                activity.get("executeStartedAt")
                                if activity.get("executeStartedAt")
                                else datetime.datetime.today()
                            )
                            run_end_at = (
                                activity.get("executeCompletedAt")
                                if activity.get("executeCompletedAt")
                                else datetime.datetime.today()
                            )
                            status = activity.get('status', '')  # Get the status of the matching task
                            error_message = activity.get('error', {}).get('message', '')
                            if error_message:
                                pattern = r"Message=([^.]*)"
                                # Search for the pattern in the error_message string
                                match = re.search(pattern, error_message)
                                if match:
                                    error = match.group(1).strip()
                            break
                    last_run_id = last_pipeline_run_data.get('pipeline_run_id','')
                # Validate Existing Task Information
                query_string = f""" 
                    select id from core.pipeline_tasks 
                    where asset_id = '{asset_id}' and source_id = '{task_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get("id") if existing_task else None
                task_policy = task.get("policy", {})
                task_retry = task_policy.get("retry", 0) if task_policy else 0
                properties = {
                    **pipeline_properties,
                    "is_mapped": task.get("is_mapped", ""),
                    "retries": task_retry,
                    "policy": task.get("policy", {}),
                    "compute": task.get("compute", {}),
                    "staging": task.get("staging", {}),
                }
                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            owner = '{task.get('owner', '')}',
                            source_type = '{source_type}',
                            run_start_at = '{run_start_at},',
                            status = '{get_pipeline_status(status_humanized.lower())}',
                            error ='{error.replace("'", "''")}',
                            run_id = '{last_run_id}',
                            run_end_at = '{run_end_at}',
                            duration = '{task_duration}',
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.datetime.now()}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_tasks.append(
                        {
                            "id": existing_task,
                            "source_id": task_id,
                        }
                    )
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        task_id,
                        task.get("name"),
                        task.get("owner", ""),
                        get_pipeline_status(status_humanized.lower()),
                        source_type,
                        last_run_id,
                        error,
                        json.dumps(properties, default=str).replace("'", "''"),
                        run_start_at,
                        run_end_at,
                        task_duration,
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.datetime.now(),
                        (
                            task.get("description")
                            if task.get("description")
                            else __prepare_description(
                                {"task_name": task.get("name"), **pipeline_properties},
                                "task",
                            )
                        ),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    tasks.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            # create each tasks
            tasks_input = split_queries(tasks)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)

                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, status, source_type, run_id, error,  properties, run_start_at,
                        run_end_at, duration, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete,
                        created_at, description)
                        values {query_input}
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(connection, cursor, tasks_insert_query)
                    pipeline_tasks_data = fetchall(cursor)
                    pipeline_tasks.extend(pipeline_tasks_data)
                    
                except Exception as e:
                    log_error("extract properties: inserting tasks level", e)
    except Exception as e:
        log_error(f"ADF Connector - Saving dataflows as tasks ", e)
        raise e
    finally:
        return new_pipeline_tasks, pipeline_tasks

def __save_runs_details(
    config: dict, data: list, pipeline_id: str, pipeline_runs: list = None
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in data:
                for model in run.get("models"):
                    pipeline_run_id = get_data_by_key(
                        pipeline_runs, run.get("unique_job_id")
                    )
                    task_type = model.get("type", "").lower()
                    task_type = task_type if task_type and task_type != "executedataflow" else "dataflow"
                    if pipeline_run_id:
                        source_id = f"""{task_type}.{asset.get('name').lower()}.{model.get('name',"").lower().replace(" ","_")}"""
                        
                        # Check if record exists with both pipeline_run_id and source_id
                        query_string = f"""
                            select id from core.pipeline_runs_detail
                            where
                                pipeline_run_id = '{pipeline_run_id}'
                                and source_id = '{source_id}'
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        existing_detail = fetchone(cursor)
                        existing_detail_id = existing_detail.get("id") if existing_detail else None
                        if existing_detail_id:
                            # Update existing record
                            model_duration = model.get("duration")
                            run_duration = run.get("duration")
                            duration_value = 0
                            if model_duration:
                                duration_value = model_duration / 1000
                            elif run_duration:
                                duration_value = run_duration / 1000
                            
                            run_start_at_value = model.get("executeStartedAt") if model.get("executeStartedAt") else run.get("started_at")
                            run_end_at_value = model.get("executeCompletedAt") if model.get("executeCompletedAt") else run.get("finished_at")
                            
                            query_string = f"""
                                update core.pipeline_runs_detail set
                                    status = '{get_pipeline_status(model.get("status"))}',
                                    error = '{run.get("status_humanized", "").replace("'", "''")}',
                                    source_code = '{run.get("rawSql", "").replace("'", "''") if run.get("rawSql") else ""}',
                                    compiled_code = '{run.get("compiledSql", "").replace("'", "''") if run.get("compiledSql") else ""}',
                                    run_start_at = {f"'{run_start_at_value}'" if run_start_at_value else 'NULL'},
                                    run_end_at = {f"'{run_end_at_value}'" if run_end_at_value else 'NULL'},
                                    duration = '{duration_value}'
                                where id = '{existing_detail_id}'
                            """
                            cursor = execute_query(connection, cursor, query_string)
                        else:
                            # Insert new record
                            model_duration = model.get("duration")
                            run_duration = run.get("duration")
                            duration_value = 0
                            if model_duration:
                                duration_value = model_duration / 1000
                            elif run_duration:
                                duration_value = run_duration / 1000
                            
                            query_input = (
                                uuid4(),
                                run.get("id", ""),
                                source_id,
                                "task",
                                f"""{model.get('name',"").lower().replace(" ","_")}""",
                                get_pipeline_status(model.get("status")),
                                run.get("status_humanized", ""),
                                (
                                    run.get("rawSql", "").replace("'", "''")
                                    if run.get("rawSql")
                                    else ""
                                ),
                                (
                                    run.get("compiledSql", "").replace("'", "''")
                                    if run.get("compiledSql")
                                    else ""
                                ),
                                (
                                    model.get("executeStartedAt")
                                    if model.get("executeStartedAt")
                                    else run.get("started_at")
                                ),
                                (
                                    model.get("executeCompletedAt")
                                    if model.get("executeCompletedAt")
                                    else run.get("finished_at")
                                ),
                                duration_value,
                                True,
                                False,
                                pipeline_run_id,
                                pipeline_id,
                                asset_id,
                                connection_id,
                            )
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals})", query_input
                            ).decode("utf-8")
                            insert_objects.append(query_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs_detail(
                            id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                            run_start_at, run_end_at, duration, is_active, is_delete,
                            pipeline_run_id, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("ADF Runs Details Insert Failed  ", e)
    except Exception as e:
        log_error(f"ADF Connector - Save Runs Details By Run ID Failed ", e)
        raise e


def __update_pipeline_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Get Last Run Info
            query_string = f"""
                select
                    id,
                    source_id,
                    run_end_at,
                    duration,
                    status
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            last_run = last_run if last_run else {}

            # Get Pipeline Stats
            query_string = f"""
                select 
                    pipeline.id,
                    count(distinct pipeline_tasks.id) as tot_tasks,
                    count(distinct pipeline_tests.id) as tot_tests,
                    count(distinct pipeline_columns.id) as tot_columns,
                    count(distinct pipeline_runs.id) as tot_runs
                from pipeline
                left join core.pipeline_tasks  on pipeline_tasks.asset_id = pipeline.asset_id
                left join core.pipeline_tests  on pipeline_tests.asset_id = pipeline.asset_id
                left join core.pipeline_columns on pipeline_columns.asset_id = pipeline.asset_id
                left join core.pipeline_runs on pipeline_runs.asset_id = pipeline.asset_id
                where pipeline.asset_id ='{asset_id}'
                group by pipeline.id
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_stats = fetchone(cursor)
            pipeline_stats = pipeline_stats if pipeline_stats else {}

            # Update Pipeline Status
            query_string = f"""
                select id, properties from core.pipeline
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            properties = report.get("properties", {})
            properties.update(
                {
                    "tot_tasks": pipeline_stats.get("tot_tasks", 0),
                    "tot_tests": pipeline_stats.get("tot_tests", 0),
                    "tot_columns": pipeline_stats.get("tot_columns", 0),
                    "tot_runs": pipeline_stats.get("tot_runs", 0),
                }
            )

            run_id = last_run.get("source_id", "")
            status = last_run.get("status", "")
            last_run_at = last_run.get("run_end_at")
            query_string = f"""
                update core.pipeline set 
                    run_id='{run_id}', 
                    status='{status}', 
                    last_run_at= {f"'{last_run_at}'" if last_run_at else 'NULL'},
                    properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"ADF Connector - Update Run Stats to Job Failed ", e)
        raise e


def __prepare_description(properties, type="job") -> str:
    name = properties.get("name", "")
    project = properties.get("data_factory", "")
    environment = properties.get("resource_group", "")
    description = f"""This {name} pipeline is under the Project {project} and Environment {environment}."""
    if type == "task":
        name = properties.get("task_name", "")
        pipeline = properties.get("name", "")
        description = f"""This {name} task is part of the pipeline {pipeline} and under the Project {project} and Environment {environment}."""
    if type == "transformation":
        task_name = properties.get("entity_name", "")
        transform_name = properties.get("transformation_name", "")
        description = f"""This {transform_name} transformation is part of the task {task_name} which is a part of pipeline {name} and under the Project {project} and Environment {environment}."""
    return description

def parse_dataflow_json(json_data):
    dataflow = json.loads(json_data) if isinstance(json_data, str) else json_data
    dataflow_name = dataflow.get("name", "")
    properties = dataflow.get("properties", {})
    type_properties = properties.get("typeProperties", {})
    sources = {source.get("name"): source for source in type_properties.get("sources", [])}
    sinks = {sink.get("name"): sink for sink in type_properties.get("sinks", [])}
    transformations = {transform.get("name"): transform for transform in type_properties.get("transformations", [])}

    script_lines = type_properties.get("scriptLines", [])

    result = {
        "dataflow_name": dataflow_name,
        "stages": []
    }

    stage_columns = {}

    # Function to extract columns from source and sink blocks
    def extract_columns_from_block(start_idx, end_marker):
        columns = []
        column_types = {}

        j = start_idx
        while j < len(script_lines):
            line = script_lines[j].strip()

            if line == end_marker:
                break

            # Skip empty lines
            if not line:
                j += 1
                continue

            # Remove trailing comma if present
            if line.endswith(','):
                line = line[:-1].strip()

            # Extract column name and type
            col_match = re.match(r'\s*(\w+)\s+as\s+([\w\(\),\.]+)', line)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                columns.append(col_name)
                column_types[col_name] = col_type

            j += 1

        return columns, column_types, j

    # Identify the flow structure by parsing the entire script
    flow_structure = {}
    split_outputs = {}

    # First pass: identify all stages and their relationships
    i = 0
    while i < len(script_lines):
        line = script_lines[i].strip()

        # Skip empty lines
        if not line:
            i += 1
            continue

        # Identify source
        if line.startswith("source("):
            # Find the source name (after ~>)
            j = i
            source_name = None
            while j < len(script_lines) and source_name is None:
                if "~>" in script_lines[j]:
                    source_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if source_name:
                flow_structure[source_name] = {"type": "source", "outputs": []}

            i = j
            continue

        # Identify filter transformations
        filter_match = re.match(r'(\w+)\s+filter\((.*?)\)\s+~>\s*(\w+)', line)
        if filter_match:
            input_name = filter_match.group(1)
            transform_name = filter_match.group(3)

            flow_structure[transform_name] = {"type": "filter", "inputs": [input_name], "outputs": []}

            # Update the input's outputs
            if input_name in flow_structure:
                flow_structure[input_name]["outputs"].append(transform_name)

            i += 1
            continue

        # Identify split transformations
        split_match = re.search(r'^(\w+)\s+split\(([^,]+)', line)
        if split_match:
            input_name = split_match.group(1)
            split_condition = split_match.group(2)
            j = i
            transform_name = None
            while j < len(script_lines) and transform_name is None:
                if "~>" in script_lines[j]:
                    regex = re.search(r'^([^~>]+)~>\s*([^@]+)@\(([^)]+)\)', script_lines[j])
                    transform_name = regex.group(2).strip()
                    output_streams = [s.strip() for s in regex.group(3).split(',')]
                j += 1

            # Record the split outputs
            for stream in output_streams:
                split_output_name = f"{transform_name}@{stream}"
                split_outputs[split_output_name] = {"parent": transform_name, "stream": stream, "inputs": [input_name]}

            flow_structure[transform_name] = {"type": "split", "inputs": [input_name], "outputs": output_streams}

            # Update the input's outputs
            if input_name in flow_structure:
                flow_structure[input_name]["outputs"].append(transform_name)

            i += 1
            continue

        # Identify select transformations
        select_match = re.search(r'(\w+)\s+select\(mapColumn\(', line)
        if select_match:
            input_name = select_match.group(1)

            # Find the select name (after ~>)
            j = i
            transform_name = None
            while j < len(script_lines) and transform_name is None:
                if "~>" in script_lines[j]:
                    transform_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if transform_name:
                flow_structure[transform_name] = {"type": "select", "inputs": [input_name], "outputs": []}

                # Update the input's outputs
                if input_name in flow_structure:
                    flow_structure[input_name]["outputs"].append(transform_name)

            i = j
            continue

        # Identify join transformations
        join_match = re.match(r'(\w+),\s*(\w+)\s+join\(', line)
        if join_match:
            input1 = join_match.group(1)
            input2 = join_match.group(2)

            # Find the join name (after ~>)
            j = i
            transform_name = None
            while j < len(script_lines) and transform_name is None:
                if "~>" in script_lines[j]:
                    transform_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if transform_name:
                flow_structure[transform_name] = {"type": "join", "inputs": [input1, input2], "outputs": []}

                # Update the inputs' outputs
                if input1 in flow_structure:
                    flow_structure[input1]["outputs"].append(transform_name)
                if input2 in flow_structure:
                    flow_structure[input2]["outputs"].append(transform_name)

            i = j
            continue

        # Identify sink
        sink_match = re.search(r'(\w+)\s+sink\(', line)
        if sink_match:
            input_name = sink_match.group(1)

            # Find the sink name (after ~>)
            j = i
            sink_name = None
            while j < len(script_lines) and sink_name is None:
                if "~>" in script_lines[j]:
                    sink_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if sink_name:
                flow_structure[sink_name] = {"type": "sink", "inputs": [input_name], "outputs": []}

                # Update the input's outputs
                if input_name in flow_structure:
                    flow_structure[input_name]["outputs"].append(sink_name)
                elif input_name in split_outputs:
                    # Handle split outputs specially
                    split_parent = split_outputs[input_name]["parent"]
                    if split_parent in flow_structure:
                        flow_structure[sink_name] = {"type": "sink", "inputs": [input_name], "outputs": []}

            i = j
            continue

        # Move to next line for unhandled lines
        i += 1

    # Second pass: extract columns for each stage
    i = 0
    while i < len(script_lines):
        line = script_lines[i].strip()

        # Skip empty lines
        if not line:
            i += 1
            continue

        # Process source blocks
        if line.startswith("source(output("):
            # Find the line with the source name
            j = i
            source_name = None
            while j < len(script_lines) and source_name is None:
                if "~>" in script_lines[j]:
                    source_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if source_name:
                # Extract columns from the output block
                columns, column_types, end_idx = extract_columns_from_block(i + 1, "),")

                # Store columns for this stage
                stage_columns[source_name] = {
                    "columns": columns,
                    "types": column_types
                }

                # Add to result
                if source_name in sources:
                    source_info = sources[source_name]
                    dataset_name = source_info.get("dataset", {}).get("referenceName", "")

                    stage = {
                        "stage_type": "source",
                        "name": source_name,
                        "dataset": dataset_name,
                        "columns": columns,
                        "column_types": column_types
                    }
                    result["stages"].append(stage)

            i = j
            continue

        # Process filter transformations
        filter_match = re.match(r'(\w+)\s+filter\((.*?)\)\s+~>\s*(\w+)', line)
        if filter_match:
            input_name = filter_match.group(1)
            filter_condition = filter_match.group(2).strip()
            transform_name = filter_match.group(3)

            # Filter keeps the same columns as input
            if input_name in stage_columns:
                stage_columns[transform_name] = stage_columns[input_name]

            # Add to result
            stage = {
                "stage_type": "transform_filter",
                "name": transform_name,
                "input_from": input_name,
                "filter_condition": filter_condition,
                "columns": stage_columns.get(transform_name, {}).get("columns", []),
                "column_types": stage_columns.get(transform_name, {}).get("types", {})
            }
            result["stages"].append(stage)

            i += 1
            continue

        # Process split transformations
        split_match = re.search(r'^(\w+)\s+split\(([^,]+)', line)
        if split_match:
            input_name = split_match.group(1)
            split_condition = split_match.group(2)
            j = i
            transform_name = None
            while j < len(script_lines) and transform_name is None:
                if "~>" in script_lines[j]:
                    regex = re.search(r'^([^~>]+)~>\s*([^@]+)@\(([^)]+)\)', script_lines[j])
                    transform_name = regex.group(2).strip()
                    output_streams = [s.strip() for s in regex.group(3).split(',')]
                j += 1
            # Split keeps the same columns as input for each output stream
            if input_name in stage_columns:
                # For the base split transform
                stage_columns[transform_name] = stage_columns[input_name]

                # For each output stream
                for stream in output_streams:
                    split_output_name = f"{transform_name}@{stream}"
                    stage_columns[split_output_name] = stage_columns[input_name]

            # Add to result
            stage = {
                "stage_type": "transform_split",
                "name": transform_name,
                "input_from": input_name,
                "split_condition": split_condition,
                "output_streams": output_streams,
                "columns": stage_columns.get(transform_name, {}).get("columns", []),
                "column_types": stage_columns.get(transform_name, {}).get("types", {})
            }
            result["stages"].append(stage)

            i += 1
            continue

        # Process select transformations
        select_match = re.search(r'(\w+)\s+select\(mapColumn\(', line)
        if select_match:
            input_name = select_match.group(1)

            # Find the end of mapColumn block and the transform name
            j = i
            transform_name = None
            mapColumn_end = False
            mapColumn_lines = []

            while j < len(script_lines) and not (mapColumn_end and transform_name):
                curr_line = script_lines[j].strip()

                if not mapColumn_end and ")" in curr_line:
                    mapColumn_end = True
                    mapColumn_lines.append(curr_line.split(")", 1)[0])
                elif not mapColumn_end:
                    mapColumn_lines.append(curr_line)

                if "~>" in curr_line:
                    transform_name = curr_line.split("~>")[1].strip()

                j += 1

            if transform_name:
                # Extract selected columns
                selected_columns = []
                selected_column_types = {}

                # Process the mapColumn block
                mapColumn_text = " ".join(mapColumn_lines)
                for col_entry in mapColumn_text.split(","):
                    col_entry = col_entry.strip()
                    if not col_entry:
                        continue

                    # Handle both simple columns and renamed columns
                    if "=" in col_entry:  # Renamed column
                        col_name = col_entry.split("=")[0].strip()
                    else:  # Simple column
                        col_name = col_entry

                    selected_columns.append(col_name)

                # Get types from input
                if input_name in stage_columns:
                    input_types = stage_columns[input_name]["types"]
                    for col in selected_columns:
                        if col in input_types:
                            selected_column_types[col] = input_types[col]

                # Store columns for this stage
                stage_columns[transform_name] = {
                    "columns": selected_columns,
                    "types": selected_column_types
                }

                # Add to result
                stage = {
                    "stage_type": "transform_select",
                    "name": transform_name,
                    "input_from": input_name,
                    "columns": selected_columns,
                    "column_types": selected_column_types
                }
                result["stages"].append(stage)

            i = j
            continue

        # Process join transformations
        join_match = re.match(r'(\w+),\s*(\w+)\s+join\(', line)
        if join_match:
            input1 = join_match.group(1)
            input2 = join_match.group(2)

            # Find join name
            j = i
            transform_name = None
            while j < len(script_lines) and transform_name is None:
                if "~>" in script_lines[j]:
                    transform_name = script_lines[j].split("~>")[1].strip()
                j += 1

            if transform_name:
                # Combine columns from both inputs
                combined_columns = []
                combined_types = {}

                # Add columns from first input
                if input1 in stage_columns:
                    for col in stage_columns[input1]["columns"]:
                        combined_columns.append(f"{input1}@{col}")
                        if col in stage_columns[input1]["types"]:
                            combined_types[f"{input1}@{col}"] = stage_columns[input1]["types"][col]

                # Add columns from second input
                if input2 in stage_columns:
                    for col in stage_columns[input2]["columns"]:
                        combined_columns.append(f"{input2}@{col}")
                        if col in stage_columns[input2]["types"]:
                            combined_types[f"{input2}@{col}"] = stage_columns[input2]["types"][col]

                # Store columns for this stage
                stage_columns[transform_name] = {
                    "columns": combined_columns,
                    "types": combined_types
                }

                # Add to result
                stage = {
                    "stage_type": "transform_join",
                    "name": transform_name,
                    "input_from": [input1, input2],
                    "columns": combined_columns,
                    "column_types": combined_types
                }
                result["stages"].append(stage)

            i = j
            continue

        # Process sink blocks
        sink_match = re.search(r'(\w+)\s+sink\(', line)
        if sink_match:
            input_name = sink_match.group(1)

            # Find input block and sink name
            j = i
            in_input_block = False
            sink_name = None
            input_start_idx = None

            while j < len(script_lines) and (input_start_idx is None or sink_name is None):
                curr_line = script_lines[j].strip()

                if curr_line.startswith("input("):
                    input_start_idx = j + 1

                if "~>" in curr_line:
                    sink_name = curr_line.split("~>")[1].strip()

                j += 1

            if sink_name and input_start_idx:
                # Extract columns from sink's input block
                sink_columns, sink_column_types, _ = extract_columns_from_block(input_start_idx, "),")

                # Get input columns and types
                input_columns = []
                input_column_types = {}

                if input_name in stage_columns:
                    input_columns = stage_columns[input_name]["columns"]
                    input_column_types = stage_columns[input_name]["types"]
                elif input_name in split_outputs:
                    # Handle split outputs
                    parent = split_outputs[input_name]["parent"]
                    if parent in stage_columns:
                        input_columns = stage_columns[parent]["columns"]
                        input_column_types = stage_columns[parent]["types"]

                # Use input columns if available, otherwise sink columns
                final_columns = input_columns if input_columns else sink_columns

                # For column types, show conversions between input and sink
                final_column_types = {}
                for col in final_columns:
                    if col in input_column_types:
                        input_type = input_column_types[col]
                        if col in sink_column_types:
                            sink_type = sink_column_types[col]
                            if input_type != sink_type:
                                final_column_types[col] = f"{input_type} -> {sink_type}"
                            else:
                                final_column_types[col] = input_type
                        else:
                            final_column_types[col] = input_type
                    elif col in sink_column_types:
                        final_column_types[col] = sink_column_types[col]

                # Add to result
                if sink_name in sinks:
                    sink_info = sinks[sink_name]
                    dataset_name = sink_info.get("dataset", {}).get("referenceName", "")

                    stage = {
                        "stage_type": "sink",
                        "name": sink_name,
                        "input_from": input_name,
                        "dataset": dataset_name,
                        "columns": final_columns,
                        "column_types": final_column_types
                    }
                    result["stages"].append(stage)

            i = j
            continue

        # Move to next line for unhandled lines
        i += 1

    dataflow_pipelines = []
    for stage in result["stages"]:
        columns = stage["columns"]
        columns_type = stage["column_types"]
        columns_arr = []
        for col in columns:
            column_data_type = "string"
            if col in columns_type:
                column_data_type = columns_type[col]
            columns_arr.append({
                "name": col,
                "type": column_data_type
            })

        dataflow_pipelines.append({
            "name": stage["name"],
            "columns": columns_arr,
        })
    return dataflow_pipelines


def __delete_columns(config: dict, pipeline_id: str):
    # function to delete all existing columns for a pipeline & asset
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                delete from core.pipeline_columns
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"ADF Connector - Delete Columns By Pipeline ID Failed ", e)
        raise e
    
def extract_pipeline_measure(run_history: list, tasks_pull: bool):
    if not run_history:
        return
    
    # Fetch pipeline_name (job_name) from pipeline table
    asset = TASK_CONFIG.get("asset", {})
    asset_id = asset.get("id") if asset else TASK_CONFIG.get("asset_id")
    if not asset_id:
        # Fallback: try to get from asset_id directly in config
        asset_id = TASK_CONFIG.get("asset_id")
    
    connection = get_postgres_connection(TASK_CONFIG)
    pipeline_name = None
    if asset_id:
        with connection.cursor() as cursor:
            pipeline_query = f"""
                select p.name as pipeline_name
                from core.pipeline p
                where p.asset_id = '{asset_id}'
                limit 1
            """
            cursor = execute_query(connection, cursor, pipeline_query)
            pipeline_result = fetchone(cursor)
            pipeline_name = pipeline_result.get("pipeline_name") if pipeline_result else None
    
    run_history = sorted(run_history, key=lambda x: x.get("started_at"), reverse=True)[:2]
    latest_run = run_history[0]
    previous_run = run_history[1] if len(run_history) > 1 else {}
    job_run_detail = {
        "duration": latest_run.get("duration"),
        "last_run_date": latest_run.get("started_at"),
        "previous_run_date": previous_run.get("started_at") if previous_run.get("started_at") else None,
    }
    # Pass pipeline_name (job_name) for asset level measures
    execute_pipeline_measure(TASK_CONFIG, "asset", job_run_detail, job_name=pipeline_name)

    # Get Tasks and execute measure
    if not tasks_pull:
        return
    tasks = get_pipeline_tasks(TASK_CONFIG)
    if not tasks:
        return
    
    latest_run_task = latest_run.get("models", [])
    previous_run_task = previous_run.get("models", [])
    for task in tasks:
        task_name = task.get("name")
        last_task_run_detail = next(
            (t for t in latest_run_task if t.get("name") == task_name), None
        )
        previous_run_detail = next(
            (t for t in previous_run_task if t.get("name") == task_name), None
        )
        if not last_task_run_detail:
            continue
        run_detail = {
            "duration": last_task_run_detail.get("duration") / 1000,
            "last_run_date": last_task_run_detail.get("executeStartedAt"),
            "previous_run_date": previous_run_detail.get("executeStartedAt") if previous_run_detail else None,
        }
        run_detail['is_row_written'] = last_task_run_detail.get("is_row_written")
        run_detail["rows_affected"] = last_task_run_detail.get("rows_count")
        run_detail["rows_inserted"] = last_task_run_detail.get("rows_inserted")
        run_detail["rows_updated"] = last_task_run_detail.get("rows_updated")
        run_detail["rows_deleted"] = last_task_run_detail.get("rows_deleted")
        # Pass pipeline_name (job_name) and task_name for task level measures
        execute_pipeline_measure(TASK_CONFIG, "task", run_detail, task_info=task, job_name=pipeline_name, task_name=task_name)

def __get_pipeline_task_info(
    each_activity,
    pipeline_info,
    connection_metadata,
    subscription_id,
    resource_group_name,
    data_factory,
    params,
    tables,
    nested_tables_relations,
    found_colums,
):
    temp = {}
    transformations = []
    lookup_entities = []  # List to store lookup dataset entities
    activity_type = each_activity.get("type", "")
    temp["dependsOn"] = []
    temp["fields"] = []
    temp["type"] = "table"
    dependsOn = each_activity.get("dependsOn")
    if dependsOn:
        temp["dependsOn"] = dependsOn
    dataflow_unique_id = str(uuid4())
    if activity_type == "ExecuteDataFlow":
        temp = {}
        dataflowGeneralName = each_activity.get("name")
        dataflowSourceId = f"""dataflow.{pipeline_info.get('name').lower()}.{dataflowGeneralName.lower().replace(" ","_")}"""
        temp["id"] = dataflow_unique_id
        temp["source_type"] = "Dataflow"
        temp["fields"] = []
        temp["dependsOn"] = []
        temp["source_id"] = dataflowSourceId

        temp["type"] = "table"
        typeProps = each_activity.get("typeProperties")
        dependsOn = each_activity.get("dependsOn")
        if dependsOn:
            temp["dependsOn"] = dependsOn
        if typeProps:
            dataflowName = typeProps.get("dataflow").get("referenceName", "")
            sourceSinkInfo = typeProps.get("dataflow").get(
                "datasetParameters", {}
            )
            temp["name"] = dataflowGeneralName
            temp["dataflow_name"] = dataflowName
            temp["source_sink"] = sourceSinkInfo
            temp["dataflow"] = []
            temp["childrenL1"] = []
            if dataflowName:
                # connection metadata check for tasks which is dataflows
                if connection_metadata.get("tasks"):
                    # get dataflow detail
                    params["request_url"] = (
                        f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{data_factory}/dataflows/{dataflowName}"""
                    )
                    flowDetail = __get_response("execute", params)
                    if flowDetail:
                        # process dataflow detail
                        dataflow_details, columns, table_children_ids = (
                            __process_dataflow_detail(
                                flowDetail,
                                subscription_id,
                                resource_group_name,
                                data_factory,
                                params,
                                dataflow_unique_id,
                                dataflowName,
                                dataflowSourceId,
                            )
                        )
                        # uncomment below line to get the lineage back to pop-up
                        temp["dataflow"] = dataflow_details
                        if dataflow_details:
                            tables.extend(dataflow_details.get("tables", []))
                            if dataflow_details.get("relations"):
                                nested_tables_relations.extend(
                                    dataflow_details.get("relations")
                                )
                            transformations.append(dataflow_details)
                        if columns:
                            found_colums.extend(columns)

                        if table_children_ids:
                            temp["childrenL1"] = table_children_ids
    elif activity_type == "Lookup":
        # Handle Lookup activity - get dataset information
        activity_name = each_activity.get("name")
        dataflowSourceId = f"""{activity_type.lower()}.{pipeline_info.get('name').lower()}.{activity_name.lower().replace(" ","_")}"""
        temp["id"] = dataflowSourceId
        temp["source_id"] = dataflowSourceId
        temp["source_type"] = "Activity"
        temp["name"] = activity_name
        temp["dataflow_name"] = activity_name
        temp["dataflow"] = []
        temp["childrenL1"] = []
        
        # Get dataset reference from Lookup activity
        type_properties = each_activity.get("typeProperties", {})
        dataset = type_properties.get("dataset", {})
        dataset_reference_name = dataset.get("referenceName", "")
        
        if dataset_reference_name:
            try:
                # Call dataset API to get dataset information
                # Use ADF format: factory_name (not workspace_name) and full resource path
                dataset_params = dict(
                    subscription_id=subscription_id,
                    resource_group=resource_group_name,
                    factory_name=data_factory,
                    pipeline_name=pipeline_info.get('name', ''),
                    request_url=f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{data_factory}/datasets/{dataset_reference_name}""",
                    request_type="get",
                    api_version="2018-06-01",
                )
                
                log_info(f"Fetching dataset information for Lookup activity '{activity_name}': {dataset_reference_name}")
                dataset_response = __get_response("execute", dataset_params)
                
                if dataset_response and dataset_response.get("properties"):
                    dataset_props = dataset_response.get("properties")
                    
                    # Extract dataset information
                    dataset_type = dataset_props.get("type", "")
                    linked_service = dataset_props.get("linkedServiceName", {})
                    linked_service_name = linked_service.get("referenceName", "") if linked_service else ""
                    type_properties_dataset = dataset_props.get("typeProperties", {})
                    schema_name = type_properties_dataset.get("schema", "")
                    table_name = type_properties_dataset.get("table", "")
                    dataset_schema = dataset_props.get("schema", [])
                    
                    # Get linked service to determine connection type
                    connection_type = None
                    if linked_service_name:
                        try:
                            # Use ADF format: factory_name (not workspace_name) and full resource path
                            linked_service_params = dict(
                                subscription_id=subscription_id,
                                resource_group=resource_group_name,
                                factory_name=data_factory,
                                pipeline_name=pipeline_info.get('name', ''),
                                request_url=f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{data_factory}/linkedservices/{linked_service_name}""",
                                request_type="get",
                                api_version="2018-06-01",
                            )
                            linked_service_response = __get_response("execute", linked_service_params)
                            if linked_service_response and linked_service_response.get("properties"):
                                connection_type = linked_service_response.get("properties").get("type", "").lower()
                                if  connection_type == "snowflakev2":
                                     connection_type = "snowflake"
                                elif  connection_type == "sqlserver":
                                     connection_type = "mssql"
                        except Exception as e:
                            log_error(f"Failed to get linked service '{linked_service_name}' for Lookup dataset", e)
                    
                    # Prepare columns from dataset schema
                    fields = []
                    if dataset_schema and isinstance(dataset_schema, list):
                        for col in dataset_schema:
                            fields.append({
                                "name": col.get("name", ""),
                                "type": col.get("type", ""),
                                "precision": col.get("precision"),
                                "scale": col.get("scale")
                            })
                    
                    # Create source_id for the dataset entity (separate from adf activity)
                    dataset_source_id = f"""dataset.{pipeline_info.get('name').lower()}.{dataset_reference_name.lower().replace(" ","_")}"""
                    
                    if fields:
                        lookup_fields = []
                        for field in fields:
                            lookup_fields.append(
                                {
                                  "id": str(uuid4()),
                                  "type": "column",
                                   "name": field.get("name"),
                                   "column_type": field.get("type", ""),
                                    "score": 0,
                                    "table": table_name or dataset_reference_name,
                                    "dataset_name": dataset_reference_name,
                                    "schema": schema_name,
                                    "from_dataset": "inbound",
                                    "dataflow_name": activity_name,
                                    "parent_entity_name": table_name,
                                    "parent_source_id": dataflowSourceId,
                                    "transformation_name": activity_name,
                                    "lineage_entity_id": field.get("lineage_entity_id", None),
                                    "task_id": dataflowSourceId,
                                    "comment": "",
                                    "description": "",
                                    "tags": [],
                                }
                            )
                    found_colums.extend(lookup_fields)
                    # Create lineage_entity entry for the lookup dataset
                    lookup_entity = {
                        "database": "",  # Dataset may not have explicit database
                        "schema": schema_name,
                        "name": table_name or dataset_reference_name,
                        "entity_name": table_name,  # Map to dataset source_id
                        "connection_type": connection_type,
                        "fields": fields,
                        "properties": json.dumps({
                            "dataset_reference_name": dataset_reference_name,
                            "dataset_type": dataset_type,
                            "linked_service_name": linked_service_name,
                            "lookup_activity_name": activity_name,
                            "pipeline_name": pipeline_info.get('name', ''),
                            "type_properties": type_properties_dataset,
                            "dataset_id": dataset_response.get("id", ""),
                            "dataset_name": dataset_response.get("name", "")
                        })
                    }
                    
                    lookup_entities.append(lookup_entity)
                    
                    # Create relation from dataset source to lookup activity
                    # Source: dataset (srcTableId) -> Target: lookup activity (tgtTableId)
                    lookup_relation = {
                        "srcTableId": table_name,
                        "tgtTableId": dataflowSourceId
                    }
                    nested_tables_relations.append(lookup_relation)
                    
                    log_info(f"Created lineage_entity for Lookup '{activity_name}' with dataset '{dataset_reference_name}' (schema: {schema_name}, table: {table_name})")
                    log_info(f"Created relation from dataset '{dataset_source_id}' to lookup activity '{dataflowSourceId}'")
                    
            except Exception as e:
                log_error(f"Failed to get dataset information for Lookup activity '{activity_name}'", e)
    else:
        activity_name = each_activity.get("name")
        dataflowSourceId = f"""{activity_type.lower()}.{pipeline_info.get('name').lower()}.{activity_name.lower().replace(" ","_")}"""
        temp["id"] = dataflowSourceId
        temp["source_id"] = dataflowSourceId
        temp["source_type"] = "Activity"
        temp["name"] = activity_name
        temp["dataflow_name"] = activity_name
        temp["dataflow"] = []
        temp["childrenL1"] = []
    return temp, found_colums, nested_tables_relations, transformations, lookup_entities


def __process_copy_activity_source_lineage(
    config: dict,
    pipeline_info: dict,
    lineage: dict,
    copy_activity_sql: list
) -> None:
    try:
        subscription_id = config.get("subscription_id", "")
        resource_group_name = config.get("resource_group", "")
        factory_name = config.get("data_factory", "")
        pipeline_name = pipeline_info.get("name", "")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group_name,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            request_url=f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelines/{pipeline_name}""",
            request_type="get",
            api_version="2018-06-01",
        )
        children_ids = []
        temp = {"tables": [], "relations": []}
        tables = lineage.get("tables", [])
        relations = lineage.get("relations", [])
        for each_activity in copy_activity_sql:
            database_name = None
            schema_name = None
            table_name = None
            source_connection_type = "adf"
            source_display_name = None
            activity_name = each_activity.get("activity_name")
            activities = pipeline_info.get("activities", [])
            found_activity = None
            dataflowSourceId = f"""{each_activity.get("type").lower()}.{pipeline_name.lower().replace(" ","_")}.{activity_name.lower().replace(" ","_")}"""
            source_sql = each_activity.get("query", '')
            connection_type = each_activity.get("connection_type")
            table_info = None
            if source_sql:
                table_info = get_table_info_from_sql(source_sql, connection_type)
            if table_info:
                database_name = table_info.get("database")
                schema_name = table_info.get("schema")
                table_name = table_info.get("table")
            if not database_name:
                for activity in activities:
                    if activity.get("name") == activity_name:
                        found_activity = activity
                        break
                    if activity.get("type") == "ForEach":
                        found_activity = None
                if found_activity and found_activity.get("type") == 'Copy':
                    input = found_activity.get("inputs")
                    if input and len(input) > 0:
                        source_display_name = input[0].get("referenceName", input[0].get("reference_name", ''))
                if not source_display_name:
                    continue
                params["request_url"] = (
                    f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{source_display_name}"""
                )
                dataset_detail = __get_response("execute", params)
                if dataset_detail:
                    dataset_properties = dataset_detail.get("properties", {})
                    dataset_props = dataset_properties.get("typeProperties", {})
                    if dataset_props:
                        dataset_schema = dataset_props.get("schema")
                        if dataset_schema and not schema_name:
                            schema_name = dataset_schema
                        dataset_table = dataset_props.get("table")
                        if dataset_table and not table_name:
                            table_name = dataset_table
                if not database_name:
                    linked_service_name = dataset_properties.get("linkedServiceName", {}).get("referenceName", "")
                    if linked_service_name:
                        params["request_url"] = (
                            f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{linked_service_name}"""
                        )
                        linked_service_detail = __get_response("execute", params)
                        if linked_service_detail:
                            linked_service_properties = linked_service_detail.get("properties", {})
                            linked_service_type = linked_service_properties.get("type", "").lower()
                            if linked_service_type.lower() == "snowflakev2":
                                source_connection_type = "snowflake"
                            elif linked_service_type.lower() == "sqlserver":
                                source_connection_type = "mssql" 
                            linked_service_props = linked_service_properties.get("typeProperties", {})
                            if linked_service_props:
                                linked_service_database = linked_service_props.get("database")
                                if linked_service_database:
                                    database_name = linked_service_database
                
            if database_name and schema_name and table_name and all(isinstance(x, str) for x in [database_name, schema_name, table_name]):
                source_temp = {}
                source_unique_id = str(uuid4())
                children_ids.append(source_unique_id)

                source_temp["id"] = source_unique_id
                source_temp["name"] = table_name
                source_temp["dataset_name"] = activity_name
                source_temp["source_type"] = "Table"
                source_temp["fields"] = []
                source_temp["type"] = "table"
                source_temp["dataset_type"] = "source"
                source_temp["level"] = 3
                source_temp["connection_type"] = source_connection_type
                source_temp["source_id"] = dataflowSourceId
                source_temp["entity_name"] = f"{table_name}.source"
                source_temp["database"] = database_name
                source_temp["schema"] = schema_name
                source_temp["table"] = table_name

                if not dataset_detail:
                    params["request_url"] = (
                        f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{source_display_name}"""
                    )
                    dataset_detail = __get_response("execute", params)
                if dataset_detail:
                    dataset_props = dataset_detail.get("properties")
                    if dataset_props:
                        dataset_schema = dataset_props.get("schema")
                        source_temp["dataset_source"] = dataset_props.get("type")
                        source_temp["dataset_properties"] = dataset_props.get(
                            "typeProperties"
                        )
                        if dataset_schema:
                            dataset_table = dataset_props.get("typeProperties").get(
                                "table", ""
                            )

                            if not linked_service_detail:
                                # get dataset linked service detail
                                params["request_url"] = (
                                    f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{dataset_props.get("linkedServiceName").get("referenceName")}"""
                                )
                                linked_service_detail = __get_response("execute", params)
                            if linked_service_detail:
                                source_temp["dataset_linked_service"] = (
                                    linked_service_detail.get("properties").get(
                                        "typeProperties"
                                    )
                                    if linked_service_detail.get("properties")
                                    else {}
                                )
                temp["relations"].append(
                    {
                        "srcTableId": source_temp["entity_name"],
                        "tgtTableId": dataflowSourceId,
                    }
                )

                temp["tables"].append(source_temp)
        tables.extend(temp.get("tables", []))
        relations.extend(temp.get("relations", []))
        lineage.update({"tables": tables, "relations": relations})
        return lineage
    except Exception as e:
        log_error(f"ADF Connector - Process Copy Activity Source Lineage Failed ", e)
        raise e

def __process_copy_activity_sink_lineage(
    config: dict,
    pipeline_info: dict,
    lineage: dict,
    copy_activity_sql: list
) -> None:
    try:
        subscription_id = config.get("subscription_id", "")
        resource_group_name = config.get("resource_group", "")
        factory_name = config.get("data_factory", "")
        pipeline_name = pipeline_info.get("name", "")

        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group_name,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            request_url=f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelines/{pipeline_name}""",
            request_type="get",
            api_version="2018-06-01",
        )

        children_ids = []
        temp = {"tables": [], "relations": []}
        tables = lineage.get("tables", [])
        relations = lineage.get("relations", [])

        for each_activity in copy_activity_sql:
            activity_name = each_activity.get("activity_name")
            dataflowSinkId = f"""{each_activity.get("type").lower()}.{pipeline_name.lower().replace(" ","_")}.{activity_name.lower().replace(" ","_")}"""
            
            outputs = []
            sink_display_name = None
            table_name = None
            schema_name = None
            database_name = None
            source_connection_type = "adf"

            # Check for sink details in the activity
            activities = pipeline_info.get("activities", [])
            found_activity = next((act for act in activities if act.get("name") == activity_name), None)

            if found_activity and found_activity.get("type") == 'Copy':
                outputs = found_activity.get("outputs", [])
                if outputs and len(outputs) > 0:
                    sink_display_name = outputs[0].get("referenceName", outputs[0].get("reference_name", ''))

            if not sink_display_name:
                continue

            # Fetch dataset details
            params["request_url"] = f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/datasets/{sink_display_name}"""
            dataset_detail = __get_response("execute", params)
            if dataset_detail:
                dataset_props = dataset_detail.get("properties", {}).get("typeProperties", {})
                schema_name = dataset_props.get("schema")
                table_name = dataset_props.get("table")
                linked_service_name = dataset_detail.get("properties", {}).get("linkedServiceName", {}).get("referenceName", "")

                # Fetch database name from linked service if not present
                if linked_service_name and not database_name:
                    params["request_url"] = f"""{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/linkedservices/{linked_service_name}"""
                    linked_service_detail = __get_response("execute", params)
                    if linked_service_detail:
                        linked_service_properties = linked_service_detail.get("properties", {})
                        linked_service_type = linked_service_properties.get("type", "").lower()
                        if linked_service_type.lower() == "snowflakev2":
                            source_connection_type = "snowflake"
                        elif linked_service_type.lower() == "sqlserver":
                            source_connection_type = "mssql"
                        database_name = linked_service_properties.get("typeProperties", {}).get("database")

            if database_name and schema_name and table_name:
                sink_temp = {}
                sink_unique_id = str(uuid4())
                children_ids.append(sink_unique_id)

                sink_temp.update({
                    "id": sink_unique_id,
                    "name": table_name,
                    "dataset_name": activity_name,
                    "sink_type": "Table",
                    "fields": [],
                    "type": "table",
                    "dataset_type": "sink",
                    "level": 3,
                    "connection_type": source_connection_type,
                    "sink_id": dataflowSinkId,
                    "entity_name": f"{table_name}.sink",
                    "database": database_name,
                    "schema": schema_name,
                    "table": table_name
                })

                # Create relation: copy activity -> sink table
                temp["relations"].append({
                    "srcTableId": dataflowSinkId,
                    "tgtTableId": sink_temp["entity_name"],
                })

                temp["tables"].append(sink_temp)

        tables.extend(temp.get("tables", []))
        relations.extend(temp.get("relations", []))
        lineage.update({"tables": tables, "relations": relations})
        return lineage

    except Exception as e:
        log_error(f"ADF Sink Lineage Error: {str(e)}")


def get_table_info_from_sql(query: str, connection_type: str) -> dict:
    """
    Extract table information from a SQL query.
    
    Args:
        query: SQL query string
        connection_type: Database connection type (e.g., 'SqlServer', 'Snowflake', 'BigQuery')
    
    Returns:
        Dictionary containing database, schema, and table name
    """
    # Map ADF dataset connection types to sqlglot dialects
    connection_type_map = {
        'Snowflake': 'snowflake',
        'BigQuery': 'bigquery',
        'PostgreSql': 'postgres',
        'AmazonRedshift': 'redshift',
        'AmazonRdsForOracle': 'oracle',
        'Oracle': 'oracle',
        'MySql': 'mysql',
        'AzureDatabricks': 'databricks',
        'AmazonAthena': 'redshift'  # Athena uses Presto syntax but redshift is closest in sqlglot
    }
    
    # Get the sqlglot dialect, default to 'tsql' if not found
    dialect = connection_type_map.get(connection_type, 'tsql')
    parsed = sqlglot.parse_one(query, read=dialect)
    table_info = {}

    # Extract table information from the first table found
    for table_expression in parsed.find_all(exp.Table):
        schema, database, table = extract_table_name(str(table_expression), schema_only=True)
        table_info = {
            'database': database,
            'schema': schema,
            'table': table
        }
        # Convert identifiers to strings if they exist
        for key in table_info:
            if table_info[key] and hasattr(table_info[key], 'name'):
                table_info[key] = table_info[key].name
        
        break  # Assuming single table, break after first table found
    
    return table_info