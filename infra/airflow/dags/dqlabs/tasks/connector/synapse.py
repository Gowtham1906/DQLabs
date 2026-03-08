"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""
import datetime
import json
import re
import copy
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.lineage_helper import (handle_alerts_issues_propagation, save_lineage, 
save_asset_lineage_mapping, save_lineage_entity, update_pipeline_propagations,
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
from dqlabs.tasks.connector.adf import get_table_info_from_sql

TASK_CONFIG = None
def extract_synapse_pipeline(config, **kwargs):
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
        TASK_CONFIG = config
        is_valid, is_exist, e = __validate_connection_establish(asset_properties)
        if not is_exist:
            raise Exception(
                f"Pipeline - {asset_properties.get('name')} doesn't exist in the workspace - {asset_properties.get('workspace')} "
            )
        lineage = {"tables": [], "relations": []}
        if is_valid:
            data = asset_properties if asset_properties else {}
            pipeline_name = asset_properties.get("name")
            pipeline_info, description, stats = get_pipeline_info(data, pipeline_name, credentials)
            original_pipeline_info = copy.deepcopy(pipeline_info)
            pipeline_id = __get_pipeline_id(config)
            if not description:
                description = __prepare_description(asset_properties)
            # get pipeline level lineage
            lineage, found_colums, lookup_entities = __get_pipeline_level_lineage(
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
                # save dataflows as tasks in pipeline_task table
                new_tasks = __save_pipeline_dataflows(
                    config, pipeline_info, pipeline_id, asset_id, found_colums, 
                    asset_properties
                )
                if new_tasks:
                    create_pipeline_task_measures(config, new_tasks)
                # get column relations
                if found_colums:
                    for column in found_colums:
                        src_table_id = column.get("parent_entity_name")
                        target_table_id = column.get("parent_source_id")
                        if column.get("from_dataset") == "outbound":
                            src_table_id = column.get("parent_source_id")
                            target_table_id = column.get("parent_entity_name")
                        column_relations.append(
                            {
                                "srcTableId": src_table_id,
                                "tgtTableId": target_table_id,
                                "srcTableColName": column.get("name"),
                                "tgtTableColName": column.get("name")
                            }
                        )
                
            if connection_metadata.get("runs"):
                # save runs & its details
                latest_run, all_runs, copy_activity_sql = __save_pipeline_runs(
                    config, pipeline_info, data, asset_id, asset_properties.get("id"), pipeline_id
                )
                # Update Job Run Stats
                __update_pipeline_stats(config)
                __update_last_run_stats(config)

            if connection_metadata.get("tasks") and copy_activity_sql:
                    lineage = __process_copy_activity_source_lineage(
                        asset_properties, original_pipeline_info, lineage, copy_activity_sql
                    )
                    lineage = __process_copy_activity_sink_lineage(
                        asset_properties, original_pipeline_info, lineage, copy_activity_sql
                    )
            filter_tables =  list(filter(lambda table: table.get(
                        "source_type") != "Dataflow", lineage.get("tables")))
            all_lineage_entities = filter_tables + lookup_entities if lookup_entities else filter_tables
            if all_lineage_entities:
                save_lineage_entity(config, all_lineage_entities, asset_id)
            # generate Auto Mapping
            __map_asset_with_lineage(config, lineage)
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
        asset_id = config.get("asset_id")
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        if latest_run:
            extract_pipeline_measure(all_runs, connection_metadata.get("tasks"))
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)
    except Exception as e:
        log_error("Synapse Pipeline pull Failed", e)
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
        workspace_name = config.get("workspace", "")
        pipeline_name = config.get("name", "")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_name=workspace_name,
            pipeline_name=pipeline_name,
        )
        response = __get_response("validate_pipeline", params)
        is_exist = bool(response.get("is_exist"))
        is_valid = bool(response.get("is_valid"))
        return (bool(is_valid), bool(is_exist), "")
    except Exception as e:
        log_error("Synapse Connector - Validate Connection Failed", e)
        return (is_valid, is_exist, str(e))
    
def get_pipeline_info(data, pipeline_name: str, credentials: dict) -> tuple:
    try:
        subscription_id = data.get("subscription_id", "")
        resource_grp_name = data.get("resource_group", "")
        workspace = data.get("workspace", "")
        no_of_runs = credentials.get("no_of_runs", 30)
        no_of_runs = int(no_of_runs) if type(no_of_runs) == str else no_of_runs
        status_filter = credentials.get("status", "all")
        days_ago = datetime.datetime.now() - datetime.timedelta(days=no_of_runs)
        days_ago = days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_grp_name,
            workspace_name=workspace,
            pipeline_name=pipeline_name,
            last_update_after=days_ago,
            last_update_before=current_timestamp,
            pipeline_status=status_filter,
            pipeline_meta=credentials.get("metadata", {})
        )
        response = __get_response("get_pipeline_info", params)
        pipeline_info = response.get("pipeline_info")
        description = response.get("description")
        stats = response.get("stats")
    except Exception as e:
        log_error("Synapse Connector - Get Pipeline info Failed", e)
    finally:
        return pipeline_info, description, stats

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
                return pipeline.get('id')
    except Exception as e:
        log_error(
            f"Synapse Connector - Get Pipeline Primary Key Information By Asset ID Failed ", e)
        raise e

def __prepare_description(properties, type = "") -> str:
    name = properties.get("name", "")
    resource_group = properties.get("resource_group", "")
    workspace = properties.get("workspace", "")
    if type == "task":
        description = f"""This {properties.get("task_name","")} task is part of the {name} pipeline  and under the Project {workspace} and Environment {resource_group}."""
    else:
        description = f"""This {name} pipeline  is under the Project {workspace} and Environment {resource_group}"""
    return f"{description}".strip()

def __process_dataflow_detail(data, subscription_id, resource_group_name, workspace, params, parent_table_id=None, dataflowGeneralName=None, dataflowSourceId=None):
    
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

    if data.get("properties"):
        typeProperties = data.get("properties").get("typeProperties")
        scriptLines = typeProperties.get("scriptLines")
        temp["transformations"] = typeProperties.get("transformations")
        # process for sources & get fields
        if typeProperties.get("sources"):
            for source in typeProperties.get("sources"):
                source_temp = {}
                source_unique_id = str(uuid4())
                children_ids.append(source_unique_id)
                

                source_temp["id"] = source_unique_id
                source_display_name = (
                    source.get("dataset").get("referenceName")
                    if source.get("dataset") is not None and source.get("dataset").get("type") == "DatasetReference"
                    else ""
                )
                if source_display_name:
                    source_temp["name"] = source_display_name
                    source_temp["dataset_name"] = source.get("name")
                    source_temp["source_type"] = "Table"
                    source_temp["fields"] = []
                    source_temp["type"] = "table"
                    source_temp["dataset_type"] = "source"
                    source_temp["level"] = 3
                    source_temp["connection_type"] = "synapse"
                    source_temp["source_id"] = dataflowSourceId
                    source_temp["entity_name"] = source_display_name
                    params["request_url"] = (
                        f"""datasets/{source_display_name}?api-version=2021-06-01"""
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
                                dataset_table = dataset_props.get("typeProperties").get("table", "")
                                source_name = f"""{source_display_name} ({dataset_table})""" if dataset_table else source_display_name
                                source_temp["name"] = source_name
                                source_temp["entity_name"] = source_name
                                
                                temp_unique_schema_id = f"""{dataset_table}_{dataset_props.get("typeProperties").get("schema", "")}_{dataset_props.get('linkedServiceName').get('referenceName')}"""
                                # get dataset linked service detail
                                params["request_url"] = (
                                    f"""linkedservices/{dataset_props.get("linkedServiceName").get("referenceName")}?api-version=2021-06-01"""
                                )
                                linked_services_detail = __get_response(
                                    "execute", params
                                )
                                if linked_services_detail:
                                    source_temp["dataset_linked_service"] = linked_services_detail.get("properties").get("typeProperties") if linked_services_detail.get("properties") else {}
                                    source_temp["connection_type"] = {
                                        "snowflakev2": "snowflake",
                                        "sqlserver": "mssql"
                                    }.get(linked_services_detail.get("properties", {}).get("type", "").lower(), "synapse")
                                updated_source_schema = [
                                    dict(
                                        item,
                                        **{
                                            'id': str(uuid4()),
                                            'type': 'column',
                                            'column_type': item.get("type", ""),
                                            'score': 0,
                                            'table': dataset_table,
                                            'dataset_name': dataset_props.get("linkedServiceName").get("referenceName"),
                                            'schema': dataset_props.get("typeProperties").get("schema", ""),
                                            'from_dataset': 'inbound',
                                            'dataflow_name': dataflowGeneralName,
                                            'parent_entity_name': source_temp["entity_name"],
                                            'parent_source_id': dataflowSourceId
                                        }
                                    ) for item in dataset_schema
                                ]
                                source_temp["fields"] = updated_source_schema
                                # append columns
                                if temp_unique_schema_id not in pool_ids:
                                    pool_ids.append(temp_unique_schema_id)
                                    columns.extend(updated_source_schema)
                    temp["relations"].append(
                        {"srcTableId": source_unique_id, "tgtTableId": ""}
                    )
                    temp["relations"].append(
                        {"srcTableId": source_temp["entity_name"], "tgtTableId": parent_table_id}
                    )

                    children_relation_objects.append({
                        "id": source_unique_id, 
                        "type": "source",
                        "parent": dataflowSourceId,
                        "name": source_temp["entity_name"]
                    })
                temp["tables"].append(source_temp)

        # process for sink & get fields if available
        if typeProperties.get("sinks"):
            for sink in typeProperties.get("sinks"):
                sink_temp = {}
                sink_unique_id = str(uuid4())
                sink_temp["id"] = sink_unique_id
                children_ids.append(sink_unique_id)
                
                sink_display_name = (
                    sink.get("dataset").get("referenceName")
                    if sink.get("dataset") is not None and sink.get("dataset").get("type") == "DatasetReference"
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
                    sink_temp["connection_type"] = "synapse"
                    sink_temp["source_id"] = dataflowSourceId
                    sink_temp["entity_name"] = sink_display_name

                    params["request_url"] = (
                        f"""datasets/{sink_display_name}?api-version=2021-06-01"""
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
                            if dataset_schema:
                                dataset_table = dataset_props.get("typeProperties").get("table", "")
                                sink_name = f"""{sink_display_name} ({dataset_table})""" if dataset_table else sink_display_name
                                sink_temp["name"] = sink_name
                                sink_temp["entity_name"] = sink_name
                                
                                temp_unique__sink_schema_id = f"""{dataset_table}_{dataset_props.get("typeProperties").get("schema", "")}_{dataset_props.get('linkedServiceName').get('referenceName')}"""
                                # get dataset linked service detail
                                params["request_url"] = (
                                    f"""linkedservices/{dataset_props.get("linkedServiceName").get("referenceName")}?api-version=2021-06-01"""
                                )
                                linked_services_detail = __get_response(
                                    "execute", params
                                )
                                if linked_services_detail:
                                    sink_temp["dataset_linked_service"] = linked_services_detail.get("properties").get("typeProperties") if linked_services_detail.get("properties") else {}
                                    sink_temp["connection_type"] = {
                                        "snowflakev2": "snowflake",
                                        "sqlserver": "mssql"
                                    }.get(linked_services_detail.get("properties", {}).get("type", "").lower(), "synapse")
                                updated_sink_schema = [
                                    dict(
                                        item,
                                        **{
                                            'id': str(uuid4()),
                                            'type': 'column',
                                            'column_type': item.get("type", ""),
                                            'score': 0,
                                            'table': dataset_table,
                                            'dataset_name': dataset_props.get("linkedServiceName").get("referenceName"),
                                            'schema': dataset_props.get("typeProperties").get("schema", ""),
                                            'from_dataset': 'outbound',
                                            'dataflow_name': dataflowGeneralName,
                                            'parent_entity_name': sink_temp["entity_name"],
                                            'parent_source_id': dataflowSourceId
                                        }
                                    ) for item in dataset_schema
                                ]
                                sink_temp["fields"] = updated_sink_schema
                                # append columns
                                if temp_unique__sink_schema_id not in pool_ids:
                                    pool_ids.append(temp_unique__sink_schema_id)
                                    columns.extend(updated_sink_schema)
                    children_relation_objects.append({
                        "id": sink_unique_id, 
                        "type": "sink",
                        "parent": dataflowSourceId,
                        "name": sink_temp["entity_name"]
                    })
                temp["tables"].append(sink_temp)

    new_relations = []
    for relation in children_relation_objects:
        if relation.get("type") == "source":
            new_relations.append({"tgtTableId": relation.get("parent"), "srcTableId": relation.get("name")})
        else:
            new_relations.append({"tgtTableId": relation.get("name"), "srcTableId": relation.get("parent")})
    temp["relations"] = new_relations
    return temp, columns, children_ids

def __get_pipeline_level_lineage(credentials, asset_properties, lineage, connection_metadata):
    tables = lineage.get("tables", [])
    relations = lineage.get("relations", [])
    found_colums = []
    subscription_id = asset_properties.get("subscription_id", "")
    resource_group_name = asset_properties.get("resource_group", "")
    workspace = asset_properties.get("workspace", "")
    pipeline_name = asset_properties.get("name", "")

    params = dict(
        subscription_id=subscription_id,
        resource_group=resource_group_name,
        workspace_name=workspace,
        pipeline_name=pipeline_name,
        request_url=f"""pipelines/{pipeline_name}?api-version=2021-06-01""",
        request_type="get",
    )
    pipeline_info = __get_response("execute", params)
    pipeline_activities = []
    if pipeline_info.get("properties"):
        pipeline_activities = pipeline_info.get("properties").get("activities")
    if not len(pipeline_activities):
        return lineage, found_colums, []

    nested_tables_relations = []
    lookup_entities = []  # Collect lookup entities from all activities
    for each_activity in pipeline_activities:
        
        temp, found_colums, nested_tables_relations, activity_lookup_entities = __get_pipeline_task_info(
            each_activity,
            pipeline_info,
            connection_metadata,
            subscription_id,
            resource_group_name,
            workspace,
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
                        # append relation
                        relations.append(
                            {
                                "srcTableId": get_matched_table[0].get("source_id"),
                                "tgtTableId": each_table.get("source_id"),
                            }
                        )
    # comment below extend if need to have pop-up lineage back in action
    relations.extend(nested_tables_relations)
    lineage.update({"tables": tables, "relations": relations})
    return lineage, found_colums, lookup_entities

def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    return next((item[get_key] for item in data if item[filter_key] == value), None)

def __save_columns(config: dict, columns: list, pipeline_id:str, pipeline_tasks: list = None):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
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
                    "dataflow_name": column.get("dataflow_name")
                }
                pipeline_task_id = get_data_by_key(pipeline_tasks, column.get("parent_source_id"))
                query_input = (
                    uuid4(),
                    column.get('name').upper(),
                    column.get('description', ''),
                    column.get('comment'),
                    column.get('column_type'),
                    column.get("tags") if column.get("tags") else [],
                    True,
                    False,
                    pipeline_task_id,
                    pipeline_id,
                    asset_id,
                    connection_id,
                    json.dumps(properties, default=str).replace("'", "''")
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
                        insert into core.pipeline_columns(
                            id, name, description,  comment, data_type, tags,
                            is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id,
                            properties
                        ) values {query_input} 
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error(
                        'Synapse Datasets Columns Insert Failed  ', e)
    except Exception as e:
        log_error(f"Synapse Connector - Save Tasks Columns By Job ID Failed ", e)
        raise e
    
def calculate_duration(task_data = {}):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    if not task_data.get('start_date') or not task_data.get('end_date'):
        return 0
    start_time = datetime.datetime.strptime(task_data['start_date'][:26] + "Z", fmt)
    end_time = datetime.datetime.strptime(task_data['end_date'][:26] + "Z", fmt)
    duration = end_time - start_time
    duration = duration.total_seconds()
    return duration

def __save_pipeline_dataflows(
    config: dict, pipeline_info: dict, pipeline_id: str, asset_id: str, columns: list = [], pipeline_properties: dict = {}
) -> None:
    """
    Save / Update Jobs For Pipeline
    """
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        last_run_id=''
        error = ''
        source_type = 'dataflow'
        dataflows = pipeline_info.get("activities",{}).get("activities",[])
        pipeline_runs = pipeline_info.get("pipeline_runs")
        tasks = []
        if (len(pipeline_runs)>0):
            last_pipeline_run_data = pipeline_runs[-1]
        if not dataflows:
            return
        
        with connection.cursor() as cursor:
            for task in dataflows:
                if task.get("type") == "ForEach":
                    activities = task.get("typeProperties", {}).get("activities", [])
                    if activities:
                        dataflows.extend(activities)
                        for act in activities:
                            if act.get("type") == "IfCondition":
                                dataflows.extend(act.get("typeProperties", {}).get("ifTrueActivities", []))
                                dataflows.extend(act.get("typeProperties", {}).get("ifFalseActivities", []))

                task_type = task.get("type", "").lower()
                task_type = task_type if task_type and task_type != "executedataflow" else "dataflow"
                task_id =f"""{task_type}.{pipeline_info.get('name').lower()}.{task.get('name').lower().replace(" ","_")}"""
                last_run_data = pipeline_info.get("pipeline_last_run_activites",[])
                task_data ={}
                for data in last_run_data:
                    if data.get('name') == task.get('name'):
                        task_data.update({
                            "run_id": data.get("id"),
                            "start_date": data.get("executeStartedAt"),
                            "end_date": data.get("executeCompletedAt"),
                            "status": data.get("status"),
                        })
                        status = data.get('status', '')  # Get the status of the matching task
                        error_message = data.get('error', {}).get('message', '')
                        try:
                            error_message_json = json.loads(error_message)
                            error = error_message_json.get('Message', '')
                        except (json.JSONDecodeError, TypeError):
                            error = ''
                        last_run_id = last_pipeline_run_data.get('pipeline_run_id','')
                        break
                query_string = f""" 
                    select id from core.pipeline_tasks 
                    where asset_id = '{asset_id}' and source_id = '{task_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get(
                    'id') if existing_task else None
                properties = {
                    **pipeline_properties,
                    "task_name": task.get("name"),
                    "is_mapped": task.get("is_mapped", ""),
                    "retries": task.get("policy", {}).get("retry", 0),
                    "policy":  task.get("policy", {}),
                    "compute":  task.get("compute", {}),
                    "staging":  task.get("staging", {})
                }
                duration = calculate_duration(task_data)
                description = __prepare_description(properties, 'task')
                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            owner = '{task.get('owner', '')}',
                            description = '{description}',
                            run_id = '{last_run_id}',
                            source_type = '{source_type}',
                            error ='{error.replace("'", "''")}',
                            run_start_at = {f"'{task_data.get('start_date')}'" if task_data.get('start_date') else 'NULL'},
                            run_end_at = {f"'{task_data.get('end_date')}'" if task_data.get('end_date') else 'NULL'},
                            status = '{task_data.get('status')}',
                            duration = {duration},
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.datetime.now()}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        task_id,
                        task.get("name"),
                        task.get("owner", ""),
                        description,
                        json.dumps(properties, default=str).replace(
                            "'", "''"),
                        last_run_id,
                        error,
                        task_data.get("start_date", datetime.datetime.today()),
                        task_data.get("end_date", datetime.datetime.today()),
                        source_type,
                        duration,
                        task_data.get("status"),
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.datetime.now(),
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input).decode("utf-8")
                    tasks.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            # create each tasks
            tasks_input = split_queries(tasks)
            for input_values in tasks_input:
                try:
                    query_input = ",".join(input_values)
                    
                    tasks_insert_query = f"""
                        insert into core.pipeline_tasks (id, source_id, name, owner, description, properties, run_id, error, run_start_at,
                        run_end_at, source_type, duration, status, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete, created_at)
                        values {query_input}
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(
                        connection, cursor, tasks_insert_query)
                    pipeline_tasks = fetchall(cursor)
                    # Save Dataflow Columns
                    __save_columns(config, columns, pipeline_id, pipeline_tasks)
                except Exception as e:
                    log_error(
                        "extract properties: inserting tasks level", e)
    except Exception as e:
        log_error(f"Synapse Connector - Saving dataflows as tasks ", e)
        raise e
    finally:
        return new_pipeline_tasks

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
                    table_schema, table_name = table_properties.get("schema", ""), table_properties.get("table", "")
                    database = table.get("dataset_linked_service", {}).get("database", "")
                    if not database and connection_type == "synapse":
                        connection_string = table.get("dataset_linked_service", {}).get("connectionString",'')
                        connection_data = dict(item.split('=') for item in connection_string.split(';') if '=' in item)
                        database = connection_data.get("Initial Catalog", '')
                    if table_name and table_schema and database:
                        unique_id = f"{table_source_type}_{table_schema}_{table_name}_{table.get('id')}"
                        if unique_id not in table_ids:
                            table_ids.append(unique_id)
                            assets = __get_asset_by_db_schema_name(config, database, table_schema, table_name, connection_type)
                            if assets:
                                save_asset_lineage_mapping(config, "pipeline", table, assets, True)
    except Exception as e:
        log_error("Error in mapping asset with lineage", e)


def __get_pipeline_task_info(
    each_activity,
    pipeline_info,
    connection_metadata,
    subscription_id,
    resource_group_name,
    workspace,
    params,
    tables,
    nested_tables_relations,
    found_colums,
):
    temp = {}
    lookup_entities = []  # List to store lookup dataset entities
    activity_type = each_activity.get("type", "")
    temp["dependsOn"] = []
    temp["fields"] = []
    temp["type"] = "table"
    dependsOn = each_activity.get("dependsOn")
    if dependsOn:
        temp["dependsOn"] = dependsOn
    if activity_type == "ExecuteDataFlow":
        dataflow_unique_id = str(uuid4())
        dataflowGeneralName = each_activity.get("name")
        dataflowSourceId = f"""dataflow.{pipeline_info.get('name').lower()}.{dataflowGeneralName.lower().replace(" ","_")}"""
        temp["id"] = dataflow_unique_id
        temp["source_id"] = dataflowSourceId
        temp["source_type"] = "Dataflow"
        typeProps = each_activity.get("typeProperties")
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
                        f"""dataflows/{dataflowName}?api-version=2021-06-01"""
                    )
                    flowDetail = __get_response("execute", params)
                    if flowDetail:
                        # process dataflow detail
                        dataflow_details, columns, table_children_ids = __process_dataflow_detail(flowDetail, subscription_id, resource_group_name, workspace, params, dataflow_unique_id, dataflowName, dataflowSourceId)
                        # uncomment below line to get the lineage back to pop-up
                        # temp["dataflow"] = dataflow_details
                        if dataflow_details:
                            tables.extend(dataflow_details.get("tables", []))
                            if dataflow_details.get("relations"):
                                nested_tables_relations.extend(
                                    dataflow_details.get("relations")
                                )
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
                dataset_params = dict(
                    subscription_id=subscription_id,
                    resource_group=resource_group_name,
                    workspace_name=workspace,
                    pipeline_name=pipeline_info.get('name', ''),
                    request_url=f"datasets/{dataset_reference_name}?api-version=2020-12-01",
                    request_type="get",
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
                            linked_service_params = dict(
                                subscription_id=subscription_id,
                                resource_group=resource_group_name,
                                workspace_name=workspace,
                                pipeline_name=pipeline_info.get('name', ''),
                                request_url=f"linkedservices/{linked_service_name}?api-version=2020-12-01",
                                request_type="get",
                            )
                            linked_service_response = __get_response("execute", linked_service_params)
                            if linked_service_response and linked_service_response.get("properties"):
                                connection_type = linked_service_response.get("properties").get("type", "").lower()
                                if connection_type == "snowflakev2":
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
                    
                    # Create source_id for the dataset entity (separate from lookup activity)
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
    return temp, found_colums, nested_tables_relations, lookup_entities


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
        log_error("Synapse Get Asset By Database, Schema and Name Failed ", e)
    finally:
        return assets

def __save_pipeline_runs(
    config: dict, pipeline_info: list, data: dict, asset_id: str, source_asset_id: str, pipeline_id: str
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    all_runs = []
    copy_activity_sql = []
    latest_run = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        runs = pipeline_info.get("pipeline_runs", [])
        if not connection_id or not runs:
            return
        pipeline_runs = []
        with connection.cursor() as cursor:
            # Clear Existing Runs Details
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            insert_objects = []
            for i, item in enumerate(runs):
                run_id = item.get("id")
                source_id =f"""dataflow.{item.get('id').lower()}.{item.get('dequeued_at').lower().replace(" ","_")}"""
                status_humanized = item.get("status_humanized") if item.get("status_humanized") else item.get("status")
                duration = item.get("duration") if item.get("duration") else item.get("durationInMs") # in miliseconds
                duration = duration/1000 if duration else 0 # in seconds
                individual_run = {
                    "id": item.get("id"),
                    "trigger_id": item.get("trigger_id"),
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
                    "unique_job_id": item.get('job_id'),
                    "models": [
                        {**each_activity, "uniqueId": source_id}
                        for each_activity in item.get("dataflow_activities", [])
                    ],
                }
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
                existing_run = existing_run.get('id') if existing_run else None
                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{get_pipeline_status(status_humanized.lower())}', 
                            error = '{item.get('error', '')}',
                            run_start_at = {f"'{item.get('started_at')}'" if item.get('started_at') else 'NULL'},
                            run_end_at = {f"'{item.get('finished_at')}'" if item.get('finished_at') else 'NULL'},
                            duration = '{duration}' ,
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append({"id": existing_run, "source_id": item.get('job_id')})
                else:
                    query_input = (
                        uuid4(),
                        item.get('job_id'),
                        run_id,
                        get_pipeline_status(status_humanized.lower()),
                        item.get('error', ''),
                        item.get('started_at') if item.get(
                            'started_at') else None,
                        item.get('finished_at') if item.get(
                            'finished_at') else None,
                        duration,
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
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
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                        RETURNING id, source_id;
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                    pipeline_runs = fetchall(cursor)       
                except Exception as e:
                    log_error('Synapse Jobs Runs Insert Failed  ', e)
            # save individual run detail
            __save_runs_details(config, all_runs, pipeline_id, pipeline_runs)
    except Exception as e:
        log_error(str(e), e)
    finally:
        return latest_run, all_runs, copy_activity_sql

def __process_copy_activity_source_lineage(
    config: dict,
    pipeline_info: dict,
    lineage: dict,
    copy_activity_sql: list
):
    """Process Copy activity source dataset lineage for Synapse (workspace API)."""
    try:
        subscription_id = config.get("subscription_id", "")
        resource_group_name = config.get("resource_group", "")
        workspace = config.get("workspace", "")
        pipeline_name = pipeline_info.get("name", "")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group_name,
            workspace_name=workspace,
            pipeline_name=pipeline_name,
            request_url=f"""pipelines/{pipeline_name}?api-version=2021-06-01""",
            request_type="get",
        )
        temp = {"tables": [], "relations": []}
        tables = lineage.get("tables", [])
        relations = lineage.get("relations", [])
        activities = pipeline_info.get("activities", {}).get("activities", [])

        for each_activity in copy_activity_sql:
            database_name = None
            schema_name = None
            table_name = None
            source_connection_type = "synapse"
            source_display_name = None
            activity_name = each_activity.get("activity_name")
            dataflowSourceId = f"""{each_activity.get("type").lower()}.{pipeline_name.lower()}.{activity_name.lower().replace(" ", "_")}"""
            source_sql = each_activity.get("query", "")
            connection_type = each_activity.get("connection_type", "")

            found_activity = None
            for activity in activities:
                if activity.get("name") == activity_name:
                    found_activity = activity
                    break
                if activity.get("type") == "ForEach":
                    foreach_activities = activity.get("typeProperties", {}).get("activities", [])
                    for sub_activity in foreach_activities or []:
                        if sub_activity.get("name") == activity_name:
                            found_activity = sub_activity
                            break
                    if found_activity:
                        break
            if found_activity and found_activity.get("type") == "Copy":
                inputs = found_activity.get("inputs", [])
                if inputs and len(inputs) > 0:
                    source_display_name = inputs[0].get("referenceName", inputs[0].get("reference_name", ""))
            if not source_display_name:
                continue

            table_info = None
            if source_sql and connection_type:
                table_info = get_table_info_from_sql(source_sql, connection_type)
            if table_info:
                database_name = table_info.get("database")
                schema_name = table_info.get("schema")
                table_name = table_info.get("table")

            params["request_url"] = f"""datasets/{source_display_name}?api-version=2020-12-01"""
            dataset_detail = __get_response("execute", params)
            dataset_properties = {}
            if dataset_detail:
                dataset_properties = dataset_detail.get("properties", {})
                dataset_props = dataset_properties.get("typeProperties", {}) or {}
                if not schema_name and dataset_props.get("schema"):
                    schema_name = dataset_props.get("schema")
                if not table_name and dataset_props.get("table"):
                    table_name = dataset_props.get("table")
            if not database_name:
                linked_service_name = (dataset_properties.get("linkedServiceName") or {}).get("referenceName", "")
                if linked_service_name:
                    params["request_url"] = f"""linkedservices/{linked_service_name}?api-version=2020-12-01"""
                    linked_service_detail = __get_response("execute", params)
                    if linked_service_detail:
                        linked_service_properties = linked_service_detail.get("properties", {})
                        linked_service_type = (linked_service_properties.get("type") or "").lower()
                        if linked_service_type == "snowflakev2":
                            source_connection_type = "snowflake"
                        elif linked_service_type == "sqlserver":
                            source_connection_type = "mssql"
                        type_props = linked_service_properties.get("typeProperties", {}) or {}
                        if type_props.get("database"):
                            database_name = type_props.get("database")

            if database_name and schema_name and table_name and all(isinstance(x, str) for x in [database_name, schema_name, table_name]):
                source_temp = {
                    "id": str(uuid4()),
                    "name": table_name,
                    "dataset_name": activity_name,
                    "source_type": "Table",
                    "fields": [],
                    "type": "table",
                    "dataset_type": "source",
                    "level": 3,
                    "connection_type": source_connection_type,
                    "source_id": dataflowSourceId,
                    "entity_name": f"{table_name}.source",
                    "database": database_name,
                    "schema": schema_name,
                    "table": table_name,
                }
                if dataset_detail and dataset_detail.get("properties"):
                    dataset_props = dataset_detail.get("properties", {})
                    source_temp["dataset_source"] = dataset_props.get("type")
                    source_temp["dataset_properties"] = dataset_props.get("typeProperties", {})
                temp["relations"].append({
                    "srcTableId": source_temp["entity_name"],
                    "tgtTableId": dataflowSourceId,
                })
                temp["tables"].append(source_temp)

        tables.extend(temp.get("tables", []))
        relations.extend(temp.get("relations", []))
        lineage.update({"tables": tables, "relations": relations})
        return lineage
    except Exception as e:
        log_error("Synapse Connector - Process Copy Activity Source Lineage Failed", e)
        raise e


def __process_copy_activity_sink_lineage(
    config: dict,
    pipeline_info: dict,
    lineage: dict,
    copy_activity_sql: list
):
    """Process Copy activity sink dataset lineage for Synapse (workspace API)."""
    try:
        subscription_id = config.get("subscription_id", "")
        resource_group_name = config.get("resource_group", "")
        workspace = config.get("workspace", "")
        pipeline_name = pipeline_info.get("name", "")
        params = dict(
            subscription_id=subscription_id,
            resource_group=resource_group_name,
            workspace_name=workspace,
            pipeline_name=pipeline_name,
            request_url=f"""pipelines/{pipeline_name}?api-version=2021-06-01""",
            request_type="get",
        )
        temp = {"tables": [], "relations": []}
        tables = lineage.get("tables", [])
        relations = lineage.get("relations", [])
        activities = pipeline_info.get("activities", {}).get("activities", [])

        for each_activity in copy_activity_sql:
            activity_name = each_activity.get("activity_name")
            dataflowSinkId = f"""{each_activity.get("type").lower()}.{pipeline_name.lower()}.{activity_name.lower().replace(" ", "_")}"""
            sink_display_name = None
            table_name = None
            schema_name = None
            database_name = None
            source_connection_type = "synapse"

            found_activity = next((act for act in activities if act.get("name") == activity_name), None)
            if found_activity and found_activity.get("type") == "Copy":
                outputs = found_activity.get("outputs", [])
                if outputs and len(outputs) > 0:
                    sink_display_name = outputs[0].get("referenceName", outputs[0].get("reference_name", ""))

            if not sink_display_name:
                continue

            params["request_url"] = f"""datasets/{sink_display_name}?api-version=2020-12-01"""
            dataset_detail = __get_response("execute", params)
            if dataset_detail:
                dataset_props = (dataset_detail.get("properties") or {}).get("typeProperties", {}) or {}
                schema_name = dataset_props.get("schema")
                table_name = dataset_props.get("table")
                linked_service_name = (dataset_detail.get("properties") or {}).get("linkedServiceName") or {}
                linked_service_name = linked_service_name.get("referenceName", "") if isinstance(linked_service_name, dict) else ""
                if linked_service_name and not database_name:
                    params["request_url"] = f"""linkedservices/{linked_service_name}?api-version=2020-12-01"""
                    linked_service_detail = __get_response("execute", params)
                    if linked_service_detail:
                        linked_service_properties = (linked_service_detail.get("properties") or {})
                        linked_service_type = (linked_service_properties.get("type") or "").lower()
                        if linked_service_type == "snowflakev2":
                            source_connection_type = "snowflake"
                        elif linked_service_type == "sqlserver":
                            source_connection_type = "mssql"
                        database_name = (linked_service_properties.get("typeProperties") or {}).get("database")

            if database_name and schema_name and table_name:
                sink_temp = {
                    "id": str(uuid4()),
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
                    "table": table_name,
                }
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
        log_error("Synapse Sink Lineage Error", e)
        raise e

def __save_runs_details(config: dict, data: list, pipeline_id: str, pipeline_runs: list = None):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_name = asset.get("name")

        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for run in data:
                for model in run.get("models"):
                    pipeline_run_id = get_data_by_key(pipeline_runs, run.get("unique_job_id"))
                    task_type = model.get("type", "").lower()
                    task_type = task_type if task_type and task_type != "executedataflow" else "dataflow"
                    query_input = (
                        uuid4(),
                        run.get('unique_job_id',''),
                        f"""{task_type}.{asset_name.lower()}.{model.get('name',"").lower().replace(" ","_")}""",
                        'task',
                        model.get('name',"").lower().replace(" ","_"),
                        get_pipeline_status(model.get('status')),
                        run.get('status_humanized', ''),
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
                        model.get('executeStartedAt') if model.get(
                            'executeStartedAt') else run.get('started_at'),
                        model.get('executeCompletedAt') if model.get(
                            'executeCompletedAt') else run.get('finished_at'),
                        int(model.get('duration')/1000) if model.get(
                            'duration') else run.get('duration'),
                        True,
                        False,
                        pipeline_run_id,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
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
                    cursor = execute_query(
                        connection, cursor, query_string)
                except Exception as e:
                    log_error('Synapse Runs Details Insert Failed  ', e)
    except Exception as e:
        log_error(f"Syanpse Connector - Save Runs Details By Run ID Failed ", e)
        raise e

def __update_pipeline_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties", {})
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
            properties = report.get('properties', {})
            properties.update({
                "workspace" : asset_properties.get('workspace', ''),
                "resource_group" : asset_properties.get('resource_group', ''),
                "tot_tasks": pipeline_stats.get('tot_tasks', 0),
                "tot_tests": pipeline_stats.get('tot_tests', 0),
                "tot_columns":  pipeline_stats.get('tot_columns', 0),
                "tot_runs":  pipeline_stats.get('tot_runs', 0)
            })

            run_id = last_run.get('source_id', '')
            status = last_run.get('status', '')
            last_run_at = last_run.get('run_end_at')
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
        log_error(f"Synapse Connector - Update Run Stats to Job Failed ", e)
        raise e


def __update_last_run_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id,status, run_start_at, run_end_at from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_start_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id') 
                status = last_run.get('status')
                last_run_at = last_run.get('run_start_at')
                pipeline_query_string = ""
                pipeline_query_string = f"""
                    update core.pipeline set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                asset = config.get("asset", {})
                handle_alerts_issues_propagation(config, run_id)
    except Exception as e:
        log_error(f"Synapse Pipeline Connector - Update Run Stats to Job Failed ", e)
        raise e

def extract_pipeline_measure(run_history: list, tasks_pull: bool):
    if not run_history:
        return
    
    # Fetch pipeline_name (job_name) from pipeline table
    asset = TASK_CONFIG.get("asset", {})
    asset_id = asset.get("id") if asset else TASK_CONFIG.get("asset_id")
    if not asset_id:
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