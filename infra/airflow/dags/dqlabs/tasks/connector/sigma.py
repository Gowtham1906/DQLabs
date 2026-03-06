"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
import base64
import time
from uuid import uuid4
import time

from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries, fetchall
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import update_asset_run_id
from dqlabs.app_helper.lineage_helper import save_reports_lineage, map_asset_with_lineage, update_reports_propagations, update_report_last_runs
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_constants.dq_constants import SIGMA_CONNECTION_TYPE_MAPPING, SIGMA_CHART_BASED_COLUMN_KEYS
from dqlabs.app_helper import agent_helper
from dqlabs.enums.connection_types import ConnectionType

TASK_CONFIG = None


def extract_sigma_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        global TASK_CONFIG
        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        connection = config.get("connection", {})
        dag_info = config.get("dag_info")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)

        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        workbook_id = asset_properties.get("id")
        credentials = decrypt_connection_config(credentials, connection_type)
        TASK_CONFIG = config

        is_valid, is_exist, workbook, e = __validate_connection_establish(asset_properties)

        if not is_exist:
            raise Exception(
                f"Workbook - {asset_properties.get('name')} doesn't exist in the workspace - {asset_properties.get('path')} "
            )
        
        if is_valid:
            workbook_data = __get_workbook(config, asset_properties)
            extracted_workbook = __extract_workbook_data(config, workbook_id, workbook_data)
            workbook_id = asset_properties.get("id")
            
            # Extract workbook views (pages)
            extracted_workbook = __extract_workbook_views(config, workbook_data, extracted_workbook)
            __save_views(dag_info, config, asset_properties, workbook_data)
            __save_columns(config, extracted_workbook)
            __prepare_lineage(config, extracted_workbook)
            __update_workbook_statistics(config, extracted_workbook)
            __get_sigma_tags(config, workbook_id, workbook)
            
            # Prepare Description
            owner_id = asset_properties.get("owner_id")
            params = dict(
                request_type="get",
                request_url=f"members/{owner_id}",
                access_token = config.get("access_token", "")
            )
            data = __get_response(config, params = params)
            owner_name = f"{data.get('firstName')} {data.get('lastName')}"
            description = __prepare_description(workbook, owner_name)
            # Prepare Search Keys
            search_keys = __prepare_search_key(asset_properties)
            updatedAt=workbook.get('updatedAt','')
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                query_string = f"""
                    update core.asset set description='{description}', search_keys='{search_keys}',
                    properties = jsonb_set(jsonb_set(properties, '{{updatedAt}}', to_jsonb('{updatedAt}'::text), true), '{{owner}}', to_jsonb('{owner_name}'::text), true)
                    where id = '{asset_id}' and is_active=true and is_delete = false
                """
                cursor = execute_query(
                    connection, cursor, query_string)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Sigma Pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value)
    finally:
        update_report_last_runs(config)


def __get_response(config, method_name: str = None, params: dict = None, max_retries: int = 3, retry_delay: float = 1.0):
    """
    Get response from Sigma API with retry logic for Conflict errors.
    
    Args:
        config: Configuration dictionary
        method_name: Optional method name for the API call
        params: Optional parameters dictionary
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Initial delay in seconds for exponential backoff (default: 1.0)
    
    Returns:
        API response data
    
    Raises:
        Exception: If all retry attempts fail
    """
    global TASK_CONFIG
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            pg_connection = get_postgres_connection(TASK_CONFIG)
            api_response = agent_helper.execute_query(
                TASK_CONFIG,
                pg_connection,
                "",
                method_name="execute",
                parameters=dict(method_name=method_name, request_params=params),
            )
            api_response = api_response if api_response else {}
            response = update_access_token(config, api_response)
            return response
        except Exception as e:
            last_exception = e
            error_message = str(e)
            
            # Check if it's a Conflict error that should be retried
            is_conflict_error = "Conflict" in error_message or "conflict" in error_message.lower()
            
            if is_conflict_error and attempt < max_retries - 1:
                # Exponential backoff with jitter
                wait_time = (retry_delay * (2 ** attempt)) + (time.time() % 1)
                log_info(f"Sigma API Conflict error on attempt {attempt + 1}/{max_retries}. Retrying in {wait_time:.2f} seconds. Error: {error_message}")
                time.sleep(wait_time)
                continue
            else:
                # For non-conflict errors or final attempt, raise immediately
                if attempt < max_retries - 1:
                    log_error(f"Sigma API call failed on attempt {attempt + 1}/{max_retries}: {error_message}", e)
                response = {}
                return response
    
    # If we exhausted all retries, raise the last exception
    if last_exception:
        log_error(f"Sigma API call failed after {max_retries} attempts", last_exception)
        raise last_exception


def __validate_connection_establish(config) -> tuple:
    try:
        is_valid = False
        is_exist = False
        workbook = None
        workbook_name = config.get("name", "")
        workbook_path = config.get("path", "")
        params = dict(
                        workbook_name = workbook_name,
                        workbook_path = workbook_path
                    )
        response = __get_response(config, "is_workbook_exists", params = params) 
        is_exist = bool(response.get("is_exist"))
        is_valid = bool(response.get("is_valid"))
        workbook = response.get("workbook")
        return (bool(is_valid), bool(is_exist), workbook , "")
    except Exception as e:
        log_error("Synapse Connector - Validate Connection Failed", e)
        return (is_valid, is_exist, workbook, str(e))

def update_access_token(config, response_data):
    data = response_data.get("response") if response_data.get("response") else ""
    token = response_data.get("access_token") 
    if token : 
        config.update({"access_token" : token}) 
    if data:
        return data
    return ""

def __get_workbook(config: dict, properties: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        workbook_id = properties.get("id")

        if workbook_id:
            params = dict(
                        request_type = "get",
                        request_url = f"workbooks/{workbook_id}/schema",
                        access_token = config.get("access_token")
                    )
            workbook = __get_response(config, params = params)
            if workbook:
                connection = get_postgres_connection(config)
                with connection.cursor() as cursor:
                    tags = workbook.get('tags', [])
                    properties_obj = {
                        "contentUrl": workbook.get("contentUrl", ""),
                        "webpageUrl": workbook.get("webpageUrl", ""),
                        "size": workbook.get("size", ""),
                        "encryptExtracts": workbook.get("encryptExtracts", ""),
                        "dataFreshnessPolicy": workbook.get("dataFreshnessPolicy", "")
                    }
                    properties_obj = json.dumps(
                        properties_obj, default=str).replace("'", "''")
                    query_string = f"""
                        update core.reports set 
                            description = '{workbook.get('description')}', 
                            tags='{tags}', 
                            url = '{properties.get("url")}',
                            properties = '{properties_obj}'
                        where asset_id = '{asset_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                return workbook
            else:
                raise Exception("Invalid Tableau Site Id or Workbook Id")
        else:
            raise Exception("Tableau Site Id or Workbook Id missing")
    except Exception as e:
        log_error("Tableau Get Workbook Failed ", e)
        raise e


def __get_report_table_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.reports
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            report = report.get('id', None) if report else None
            return report
    except Exception as e:
        log_error(str(e), e)
        raise e


def __get_report_view_table(config: dict, source_id) -> dict:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                select 
                    reports_views.id,
                    reports_views.source_id,
                    reports_views.properties,
                    count(distinct reports_columns.id) as tot_columns,
				    count(distinct case when reports_columns.table_name is not null then reports_columns.table_name end) as tot_tables
                from core.reports_views
                left join core.reports_columns on reports_columns.report_view_id = reports_views.id
                where reports_views.asset_id='{asset_id}' and lower(reports_views.source_id) = lower('{source_id}')
                group by reports_views.id
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            return report
    except Exception as e:
        log_error(str(e), e)
        raise e

def __prepare_description(properties, owner_name) -> str:
    name = properties.get("name", "")
    path = properties.get("path", {}).split('/')[0]
    description = f"""Workbook {name} is part of Workspace {path} and owned by  {owner_name}. """
    return f"{description}".strip()

def __prepare_search_key(properties: dict) -> dict:
    keys = properties.values()
    keys = [str(x)for x in keys if x]

    if len(keys) > 0:
        search_keys = " ".join(keys)
    return search_keys

def __save_views(dag_info: dict, config: dict, properties: dict, workbook: dict):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        report_id = __get_report_table_id(config)

        views = workbook.get('pages', {})
        views_ids = list(views.keys())
        new_views = []

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for view in views_ids:
                view_data = views.get(view)
                view_id = view_data.get("id")

                # Validate Existing Reports Views (Sheet / Dashboard) Information
                query_string = f""" 
                    select id from core.reports_views 
                    where asset_id = '{asset_id}' and source_id = '{view_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_view = fetchone(cursor)
                existing_view = existing_view.get(
                    'id') if existing_view else None

                tags = []
                # image_url = __get_sigma_page_image(config, dag_info, {"workbook_id": properties.get("id"), "view_id": view_data.get('id')})
                # image = {
                #     "p_img" : image_url,
                #     "img": image_url
                # }
                image = {}
                folder_path = ''
                properties_obj = {}
                if existing_view:
                    query_string = f"""
                        update core.reports_views set 
                            name = '{view_data.get('title', '')}'
                        where id = '{existing_view}'
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                else:
                    query_input = (
                        str(uuid4()),
                        view_data.get("id"),
                        view_data.get("title"),
                        json.dumps(properties_obj, default=str).replace(
                            "'", "''"),
                        tags,
                        json.dumps(image, default=str).replace("'", "''"),
                        folder_path,
                        str(report_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        False
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input).decode("utf-8")
                    new_views.append(query_param)

            # create each views
            views_input = split_queries(new_views)
            for input_values in views_input:
                try:
                    query_input = ",".join(input_values)
                    insert_query = f"""
                        insert into core.reports_views (id, source_id, name, properties, tags, image,
                        folder_path, report_id, asset_id, connection_id, is_active, is_delete)
                        values {query_input}
                    """
                    cursor = execute_query(
                        connection, cursor, insert_query)
                except Exception as e:
                    log_error(
                        "extract properties: inserting views level", e)
                    raise e

    except Exception as e:
        log_error("Tableau Save View Failed ", e)
        raise e

def __extract_workbook_data(config, workbook_id, workbook_schema):
    workbook = {
            "data_sources": [],
            "databases": [],
            "tables": [],
            "columns": [],
            "used_columns": [],
            "pages": [],
        }
    tables_data = {}
    inode_to_table_mapping = {}
    try:
        # Step 1: Get list of tables used in the workbook
        params = dict(
            request_type="get",
            request_url=f"workbooks/{workbook_id}/sources",
            access_token = config.get("access_token", "")    
        )
        sources_response = __get_response(config, params=params)
        if not sources_response:
            return workbook
            
        # Step 2: Process each table source
        for source in sources_response:
            if source.get("type") == "table":
                inode_id = source.get("inodeId")
                
                # Step 3: Get connection path for the table
                path_params = dict(
                    request_type="get",
                    request_url=f"connections/paths/{inode_id}",
                    access_token = config.get("access_token", "")
                )
                path_response = __get_response(config, params=path_params)
                if not path_response:
                    continue
                    
                connection_id = path_response.get("connectionId")
                path_array = path_response.get("path", [])
                
                # Step 4: Get connection details
                connection_params = dict(
                    request_type="get",
                    request_url=f"connections/{connection_id}",
                    access_token = config.get("access_token", "")
                )
                connection_response = __get_response(config, params=connection_params)
                if not connection_response:
                    continue
                
                # Extract path components (database, schema, table)
                database_name = path_array[0] if len(path_array) > 0 else ""
                schema_name = path_array[1] if len(path_array) > 1 else ""
                table_name = path_array[2] if len(path_array) > 2 else ""
                
                # Create unique identifiers
                connection_name = connection_response.get("name", "")
                connection_type = connection_response.get("type", "")
                mapped_connection_type = SIGMA_CONNECTION_TYPE_MAPPING.get(connection_type, connection_type)
                
                table_params = dict(
                    request_type="get",
                    request_url=f"files/{inode_id}",
                    access_token = config.get("access_token", "")
                )
                table_response = __get_response(config, params=table_params)
                if not table_response:
                    continue

                # Create data source object
                data_source = {
                    "id": connection_id,
                    "name": connection_name
                }
                
                # Check if data source already exists
                existing_data_source = next(
                    (ds for ds in workbook["data_sources"] if ds.get("id") == connection_id), None
                )
                if not existing_data_source:
                    workbook["data_sources"].append(data_source)
                
                # Create database object
                database = {
                    "datasource_id": connection_id,
                    "datasource_name": connection_name,
                    "database_id": f"{connection_id}_{database_name}",
                    "database": database_name,
                    "schema": schema_name,
                    "server": connection_response.get("server", ""),
                    "connection_type": mapped_connection_type,
                    "level": 1
                }
                
                # Check if database already exists
                existing_database = next(
                    (db for db in workbook["databases"] if db.get("database_id") == database["database_id"]), None
                )
                if not existing_database:
                    workbook["databases"].append(database)
                
                tables_data.update({table_response.get("urlId"): database })
            
            if source.get("type") == "data-model":
                data_model_id = source.get("dataModelId")
                element_ids = source.get("elementIds")
                for element_id in element_ids:
                    params = dict(
                        request_type="get", 
                        request_url=f"dataModels/{data_model_id}/elements", 
                        access_token = config.get("access_token", "")
                    )
                    element_response = __get_response(config, params=params)
                    element_response = element_response.get("entries", [])
                    if not element_response:
                        continue
                    for element in element_response:
                        element_reponse_id = element.get("elementId")
                        if element_reponse_id == element_id and element.get("type") == "table":
                            element_response_table_name = element.get("name", "")
                            data_model_source_params = dict(
                                request_type="get", 
                                request_url=f"dataModels/{data_model_id}/sources", 
                                access_token = config.get("access_token", "")
                            )
                            data_model_source_response = __get_response(config, params=data_model_source_params)
                            data_model_source_response = data_model_source_response.get("entries", [])
                            if not data_model_source_response:
                                continue
                            for source in data_model_source_response:
                                source_response_table_id = source.get("tableId")
                                inode_id = source.get("tableId")
                        
                                # Step 3: Get connection path for the table
                                path_params = dict(
                                    request_type="get",
                                    request_url=f"connections/paths/{inode_id}",
                                    access_token = config.get("access_token", "")
                                )
                                path_response = __get_response(config, params=path_params)
                                if not path_response:
                                    continue
                                connection_id = path_response.get("connectionId")
                                path_array = path_response.get("path", [])
                                # Step 4: Get connection details
                                connection_params = dict(
                                    request_type="get",
                                    request_url=f"connections/{connection_id}",
                                    access_token = config.get("access_token", "")
                                )
                                connection_response = __get_response(config, params=connection_params)
                                if not connection_response:
                                    continue
                                # Extract path components (database, schema, table)
                                database_name = path_array[0] if len(path_array) > 0 else ""
                                schema_name = path_array[1] if len(path_array) > 1 else ""
                                table_name = path_array[2] if len(path_array) > 2 else ""
                                # Create unique identifiers
                                if table_name == element_response_table_name:
                                    connection_name = connection_response.get("name", "")
                                    connection_type = connection_response.get("type", "")
                                    mapped_connection_type = SIGMA_CONNECTION_TYPE_MAPPING.get(connection_type, connection_type)
                                    
                                    table_params = dict(
                                        request_type="get",
                                        request_url=f"files/{inode_id}",
                                        access_token = config.get("access_token", "")
                                    )
                                    table_response = __get_response(config, params=table_params)
                                    if not table_response:
                                        continue
                                    # Create data source object
                                    data_source = {
                                        "id": connection_id,
                                        "name": connection_name
                                    }
                                    # Check if data source already exists
                                    existing_data_source = next(
                                        (ds for ds in workbook["data_sources"] if ds.get("id") == connection_id), None
                                    )
                                    if not existing_data_source:
                                        workbook["data_sources"].append(data_source)
                                    # Create database object
                                    database = {
                                        "datasource_id": connection_id,
                                        "datasource_name": connection_name,
                                        "database_id": f"{connection_id}_{database_name}",
                                        "database": database_name,
                                        "schema": schema_name,
                                        "server": connection_response.get("server", ""),
                                        "connection_type": mapped_connection_type,
                                        "level": 1
                                    }
                                    # Check if database already exists
                                    existing_database = next(
                                        (db for db in workbook["databases"] if db.get("database_id") == database["database_id"]), None
                                    )
                                    if not existing_database:
                                        workbook["databases"].append(database)
                                    tables_data.update({table_response.get("urlId"): database })
        
        # Get workbook schema to find all elements
        if workbook_schema:
            # Get all elements from the workbook
            elements = workbook_schema.get("elements", {})
            
            # Process each element to get lineage information and build inode mapping
            for element_id, element_data in elements.items():
                # Get lineage information for this element
                lineage_params = dict(
                    request_type="get",
                    request_url=f"workbooks/{workbook_id}/lineage/elements/{element_id}",
                    access_token = config.get("access_token", "")
                )
                lineage_response = __get_response(config, params=lineage_params)
                if lineage_response and lineage_response.get("dependencies"):
                    dependencies = lineage_response.get("dependencies", {})
                    
                    # Find table dependencies (inode-* patterns with type "table")
                    for node_id, dependency_info in dependencies.items():
                        # Store mapping: inodeId -> table name
                        inode_to_table_mapping[node_id] =   {   
                                                                "table_name": dependency_info.get("name", ""),
                                                                "table_type": dependency_info.get("type", "")
                                                            }
            
            if inode_to_table_mapping:
                for table_id, table_info in list(inode_to_table_mapping.items()):
                    node_id = table_id.split("inode-")[1] if "inode-" in table_id else table_id
                    type = table_info.get("table_type","")
                    table_schema = tables_data.get(node_id, {})
                    if type != 'sheet':
                        table = {
                            "table_id": table_id ,
                            "table_name": table_info.get("table_name",""),
                            "table_type": table_info.get("table_type",""),
                            **table_schema,
                            "level": 2
                        }
                        workbook["tables"].append(table)
                    elif type == 'sheet' and sources_response[0].get("type") == "data-model":
                        table_schema = tables_data.get(node_id, {})
                        if not table_schema:
                            for id, value in tables_data.items():
                                params = dict(
                                    request_type="get",
                                    request_url=f"connections/paths/{id}",
                                    access_token = config.get("access_token", "")
                                )
                                table_response = __get_response(config, params=params)
                                if not table_response:
                                    continue
                                path_array = table_response.get("path", [])
                                table_name = path_array[2] if len(path_array) > 2 else ""
                                if table_name == table_info.get("table_name",""):
                                    table_schema = value
                                    new_table_id = 'inode-'+id
                                    inode_to_table_mapping[new_table_id] = inode_to_table_mapping.pop(table_id)
                                    table_id = new_table_id
                                    break
                        table = {
                            "table_id": table_id ,
                            "table_name": table_info.get("table_name",""),
                            "table_type": "",
                            **table_schema,
                            "level": 2
                        }
                        workbook["tables"].append(table)

            # Now extract columns from sheets in workbook schema
            sheets = workbook_schema.get("sheets", {})
            for sheet_id, sheet_data in sheets.items():
                sheet_columns = sheet_data.get("columns", {})
                
                for column_id, column_data in sheet_columns.items():
                    formula = column_data.get("formula", {})
                    formula_type = formula.get("type")
                    
                    # Only process nameRef type columns (actual table columns)
                    if formula_type == "nameRef":
                        path = formula.get("path", [])
                        if len(path) >= 2:
                            resolved_column_id, inode_id, column_name = _resolve_nameref_column_reference(
                                column_id, formula
                            )
                            # Get table name from our mapping
                            inode_data = inode_to_table_mapping.get(inode_id)
                            table_name = ""
                            if inode_data:
                                table_name = inode_data.get("table_name","")
                            
                        
                                
                            if table_name:
                                # Create column object matching tableau.py structure
                                column = {
                                    "name": column_name,
                                    "id": resolved_column_id,
                                    "data_type": "string",  # Default data type
                                    "is_null": True,  # Default value
                                    "is_semantics": False,  # Default value
                                    "table_name": table_name,
                                    "table_id": inode_id,  # Spread all table properties
                                    "level": 4
                                }
                                
                                # Check if column already exists
                                existing_column = next(
                                    (c for c in workbook["columns"] if c.get("id") == resolved_column_id), None
                                )
                                if not existing_column and column_name:
                                    workbook["columns"].append(column)
        
            # Update Table Columns (matching tableau.py structure)
            for table in workbook["tables"]:
                table.update({
                    "fields": [item for item in workbook.get("columns") if item.get("table_id", "") == table.get("table_id")]
                })
        
        return workbook
        
    except Exception as e:
        log_error("Sigma Extract Workbook Data Failed", e)
        raise e


def __get_sigma_page_image(config, dag_info: dict, properties: dict):
    img_url = ""
    try:
        workbook_id = properties.get("workbook_id", "")
        view_id = properties.get("view_id", "")

        # Step 1: Export the page as PNG
        export_params = dict(
            request_type="post",
            request_url=f"workbooks/{workbook_id}/export",
            request_params={
                "format": {"type": "png"},
                "pageId": view_id
            },
            access_token = config.get("access_token", "")
        )
        export_response = __get_response(config, params=export_params)
        query_id = export_response.get('queryId', '')
        download_params = dict(
            request_type="get",
            request_url=f"query/{query_id}/download",
            access_token = config.get("access_token", ""),
            is_file_download = True,
        )
        download_response = __get_response(config, params=download_params)
        image_data = bytes(download_response) if download_response else None
        file_name = view_id if image_data else ""
        if not file_name:
            return
        img_url = __save_image(dag_info, file_name, image_data)
    except Exception as e:
        log_error("Sigma Get Page Image Failed ", e)
        raise e
    finally:
        return img_url


def __save_image(dag_info, view_id, file):
    img_url = ""
    try:
        storage_service = get_storage_service(dag_info)
        file_path = f"reports/{view_id}.jpeg"
        image = __convert_base64(file)
        if image:
            img_url = storage_service.upload_file(image, file_path, dag_info)
    except Exception as e:
        log_error("Tableau Save Image Failed ", e)
        raise e
    finally:
        return img_url


def __convert_base64(file):
    decoded_file = ""
    try:
        decoded_file = base64.b64encode(file)
        decoded_file = base64.b64decode(decoded_file)
    except Exception as e:
        log_error("Tableau Image Base 64 Convert Failed ", e)
        raise e
    finally:
        return decoded_file

def __recursive_path_finding(all_projects, obj, path):
    try:
        parentPrject_id = obj.get("parentProjectId", None)
        if parentPrject_id:
            project_folders = [
                obj for obj in all_projects if (obj.get("id", 0) == parentPrject_id)
            ]
            if project_folders:
                project_folders = project_folders[0]
                path.append(project_folders.get("name", ""))
                __recursive_path_finding(all_projects, project_folders, path)
    except Exception as e:
        log_error("Tableau Get Projects Recursive Path Failed ", e)
        raise e
    finally:
        return path

def _find_all_paths_recursive(obj, paths_found=None):
    """
    Recursively find all 'path' keys in a nested dictionary/object structure.
    
    Args:
        obj: The object to search through
        paths_found: List to store found paths (used for recursion)
    
    Returns:
        List of all found path values
    """
    if paths_found is None:
        paths_found = []
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "path" and isinstance(value, list):
                paths_found.extend(value)
            elif isinstance(value, (dict, list)):
                _find_all_paths_recursive(value, paths_found)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _find_all_paths_recursive(item, paths_found)
    
    return paths_found

def _resolve_nameref_column_reference(column_id, formula):
    """
    Normalize Sigma nameRef references to a stable column identifier.

    Handles both:
    - Standard workbook format: path = [inode_id, column_name]
    - Data-model format: path = [some_id, "inode_id/column_name"]
    """
    resolved_column_id = column_id
    inode_id = ""
    column_name = ""
    path = formula.get("path", []) if isinstance(formula, dict) else []

    if len(path) < 2:
        return resolved_column_id, inode_id, column_name

    path_first = path[0]
    path_second = path[1]

    if isinstance(path_second, str) and path_second.startswith("inode-") and "/" in path_second:
        resolved_column_id = path_second
        inode_id, column_name = path_second.split("/", 1)
        return resolved_column_id, inode_id, column_name

    if isinstance(path_first, str) and path_first.startswith("inode-") and isinstance(path_second, str):
        resolved_column_id = f"{path_first}/{path_second}"
        inode_id = path_first
        column_name = path_second
        return resolved_column_id, inode_id, column_name

    inode_id = path_first if isinstance(path_first, str) else ""
    column_name = path_second if isinstance(path_second, str) else ""
    return resolved_column_id, inode_id, column_name

def _extract_column_from_id(column_id, column_key, workbook, page_columns, sheet_data, element_columns):
    """
    Helper function to extract column data from a column ID and add it to element and page columns.
    
    Args:
        column_id: The ID of the column to extract
        column_key: The key from SIGMA_CHART_BASED_COLUMN_KEYS (e.g., 'axis', 'series', 'color', etc.)
        workbook: The workbook data containing all columns
        element_columns: List to append element columns to
        page_columns: List to append page columns to
        sheet_data: The sheet data containing column definitions
    """
    try:
        # Find the column definition in the sheet's columns
        sheet_columns = sheet_data.get("columns", {})
        column_data_from_sheet = sheet_columns.get(column_id)
        if column_data_from_sheet:
            resolved_column_id, _, _ = _resolve_nameref_column_reference(
                column_id, column_data_from_sheet.get("formula", {})
            )
            if column_data_from_sheet.get("formula", {}).get("type", "") != "nameRef":
                column_info = next((column for column in element_columns 
                   if column.get("columnId") == column_id), {})
                column_name = column_info.get("label", "")
                column_formula = column_info.get("formula", "")
                
                all_paths = _find_all_paths_recursive(column_data_from_sheet)
                used_attributes = []
                # Process each found path
                for path_value in all_paths:
                    if isinstance(path_value, str):
                        # The path should contain the actual column reference
                        actual_column_id = path_value
                        
                        # Find the actual column in our existing columns
                        actual_column_detail = next(
                            (col for col in workbook.get("columns", []) 
                            if col.get("id") == actual_column_id), None
                        )
                        
                        if actual_column_detail:
                            # Check if column is already present in page_columns
                            column_data = {
                                    "id": actual_column_id,
                                    "name": actual_column_detail.get("name", ""),
                                    "role_name": column_key,
                                    "semantic_role": column_key,
                                    "type": column_key,
                                    "properties": {},
                                    **actual_column_detail
                                }
                            used_attributes.append(column_data)

                properties = {
                    'is_calculated_field': True,
                    'formula': column_formula,
                    'source': 'sigma',
                    'used_attributes': used_attributes
                }
                if column_name:
                    page_columns.append({
                        "id": column_id,
                        "name": column_name,
                        "properties": properties
                    })
            else:
                actual_column_detail = next(
                            (col for col in workbook.get("columns", []) 
                            if col.get("id") == resolved_column_id), None
                        )
                        
                if actual_column_detail:
                    # Check if column is already present in page_columns
                    column_data = {
                            "id": resolved_column_id,
                            "name": actual_column_detail.get("name", ""),
                            "role_name": column_key,
                            "semantic_role": column_key,
                            "type": column_key,
                            "properties": {},
                            **actual_column_detail
                        }
                    if not any(col.get("id") == resolved_column_id for col in page_columns):
                        if column_data.get("name"):
                            page_columns.append(column_data)
                

        
    except Exception as e:
        log_error(f"Error extracting column {column_id} for key {column_key}", e)

def __extract_levels_value(levels_data):
    column_values = []
    for data in levels_data:
        if data.get("keys"): 
            column_values.extend(data.get("keys"))
        if data.get("calcs"): 
            column_values.extend(data.get("calcs")) 
    return column_values

def __extract_workbook_views(config, workbook_schema, workbook) -> dict:
    try:
        workbook_id = workbook_schema.get("workbookId")
        # Initialize pages array
        workbook["pages"] = []
        
        # Get pages from workbook schema
        pages = workbook_schema.get("pages", {})
        
        # Process each page
        for page_id, page_data in pages.items():
            page_name = page_data.get("title", "")
            elements = page_data.get("layout", {}).get("elements", [])
            elements_data = workbook_schema.get("elements", {})
            # Collect all columns used in this page
            page_columns = []
            
            # Process each element in the page
            for element in elements:
                element_id = element.get("id")
                viz_data = elements_data.get(element_id, {}).get("viz",{})
                sheet_id = viz_data.get("sheetId")
                columns_params = dict(
                    request_type="get",
                    request_url=f"workbooks/{workbook_id}/elements/{element_id}/columns",
                    access_token = config.get("access_token", ""),
                    paginate = True
                )
                element_columns = __get_response(config, params = columns_params)
                if sheet_id:
                    # Get the sheet data
                    sheets = workbook_schema.get("sheets", {})
                    sheet_data = sheets.get(sheet_id, {})
                    
                    if sheet_data:
                        type = sheet_data.get("type")
                        # Extract columns from all chart-based column keys
                        column_keys = SIGMA_CHART_BASED_COLUMN_KEYS.get(type)
                        if column_keys:
                            for column_key in column_keys:
                                column_value = sheet_data.get(column_key)
                                if column_key == "base":
                                    column_value = sheet_data.get(column_key).get("columns")
                                if column_key == "levels":
                                    levels_data = sheet_data.get(column_key)
                                    column_value = __extract_levels_value(levels_data) if levels_data else ""
                                if column_value:  
                                    # Handle both string and list values
                                    if isinstance(column_value, list):
                                        # Process list of column IDs
                                        for column_id in column_value:
                                            _extract_column_from_id(
                                                column_id, column_key, workbook,
                                                page_columns, sheet_data, element_columns
                                            )
                                    else:
                                        # Process single column ID (string)
                                        _extract_column_from_id(
                                            column_value, column_key, workbook, 
                                            page_columns, sheet_data, element_columns
                                        )
            # Create page object
            page_object = {
                "id": page_id,
                "name": page_name,
                "source_id": page_id,
                "type": "Page",
                "upstreamDatasources": [],
                "upstreamDatabases": [],
                "upstreamTables": [],
                "upstreamFields": [],
                "upstreamColumns": [],
                "fields": page_columns,
                "level": 3
            }
            
            workbook["pages"].append(page_object)
            
            # Update Used Columns
            used_columns = workbook.get("used_columns", [])
            workbook.update({"used_columns": used_columns + page_columns})
        
        return workbook
        
    except Exception as e:
        log_error(f"Sigma Connector - Extract Workbook Views Info ", e)
        raise e

def __save_columns(config: dict, workbook: dict):
    try:
        if not workbook:
            log_error("Tableau Save Columns", "Workbook is empty or None")
            return
        
        # Save Columns by View ID
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        report_id = __get_report_table_id(config)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            views = workbook.get('pages',[])
            for view in views:
                report_view = __get_report_view_table(
                    config, view.get("source_id"))
                if report_view:
                    report_view_id = report_view.get("id")
                    query_string = f"""
                        delete from core.reports_columns
                        where asset_id = '{asset_id}' and report_view_id = '{report_view_id}'
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    insert_objects = []
                    columns = view.get('fields')
                    if columns:
                        for column in columns:
                            query_input = (
                                uuid4(),
                                column.get('name'),
                                column.get('table_name'),
                                column.get('database'),
                                column.get('schema'),
                                column.get('data_type'),
                                json.dumps(column.get("properties", {})).replace("'", "''"),
                                column.get('is_null') if column.get(
                                    'is_null') else True,
                                column.get('is_semantics') if column.get(
                                    'is_semantics') else False,
                                column.get('connection_type'),
                                column.get('sheet_source_id') if column.get(
                                    'sheet_source_id') else None,
                                True,
                                False,
                                report_view_id,
                                report_id,
                                asset_id,
                                connection_id,
                            )
                            input_literals = ", ".join(
                                ["%s"] * len(query_input))
                            query_param = cursor.mogrify(
                                f"({input_literals})", query_input
                            ).decode("utf-8")
                            insert_objects.append(query_param)

                        insert_objects = split_queries(insert_objects)
                        for input_values in insert_objects:
                            try:
                                query_input = ",".join(input_values)
                                query_string = f"""
                                    insert into core.reports_columns(
                                        id, name, table_name, database, schema, data_type, report_column_properties, is_null, is_semantics,
                                        connection_type, source_id, is_active, is_delete, 
                                        report_view_id, report_id, asset_id, connection_id
                                    ) values {query_input} 
                                """
                                cursor = execute_query(
                                    connection, cursor, query_string)
                            except Exception as e:
                                log_error(
                                    'Tableau Workbook Views Columns Insert Failed  ', e)
                                raise e
                    report_view = __get_report_view_table(config, view.get("source_id"))
                    # Update View Statistics
                    __update_view_statistics(config, report_view, view)
    except Exception as e:
        log_error(f"Tableau Connector - Save Views Columns By View ID Failed ", e)
        raise e


def __update_workbook_statistics(config: dict, workbook: dict):
    try:
        if workbook is None:
            log_error("Tableau Save Workbook Statistics", "Workbook is empty or None")
            return

        # Update Workbook Statistics
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id, properties from core.reports
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            properties = report.get('properties', {})
            properties.update({
                "tot_datasources": len(workbook.get('data_sources', [])) if workbook.get('data_sources') else 0,
                "tot_databases": len(workbook.get('databases', [])) if workbook.get('databases') else 0,
                "tot_tables": len(workbook.get('tables', [])) if workbook.get('tables') else 0,
                "tot_columns": len(workbook.get('used_columns', [])) if workbook.get('used_columns') else 0,
                "tot_pages": len(workbook.get('pages', [])) if workbook.get('pages') else 0,


            })
            # path = __get_workbook_and_view_url(config, "workbook", properties)
            query_string = f"""
                    update core.reports set 
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    where asset_id = '{asset_id}'
                """
            cursor = execute_query(
                connection, cursor, query_string)

    except Exception as e:
        log_error(f"Tableau Connector - Update Workbook Statistics ", e)
        raise e


def __update_view_statistics(config: dict, report_view: dict, view: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            query_string = f"""
                select id, properties from core.reports
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            workbook_properties = report.get('properties', {})

            report_view_id = report_view.get("id")
            properties = report_view.get('properties', {})
            properties.update({
                "tot_columns": report_view.get('tot_columns', 0) if report_view.get('tot_columns') else 0,
                "tot_tables": report_view.get('tot_tables', 0) if report_view.get('tot_tables') else 0
            })
            path = f"{asset_properties.get('url')}?:nodeId={view.get('source_id')}"
            query_string = f"""
                update core.reports_views set 
                    type = '{view.get('type', '')}',
                    properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                    url = '{path}'
                where id = '{report_view_id}'
            """
            cursor = execute_query(
                connection, cursor, query_string)

    except Exception as e:
        log_error(f"Tableau Connector - Update View Statistics ", e)
        raise e


def __extract_tables(detail, tables, databases):
    data = None
    table_id = detail.get("name") if detail.get(
        "name") else detail.get("table")
    table_id = table_id.replace("'", "")
    database = next(
        (item for item in databases if item["database_id"] == detail.get("connection")), {})
    existing_table = next(
        (item for item in tables if item.get("table_id", "") == table_id), None)
    if not existing_table:
        data = {
            **database,
            "table_id": table_id,
            "table_name": detail.get("name"),
            "table_type": detail.get("type"),
            "level": 2
        }
    return data



def __prepare_lineage(config, workbook):
    """
    Helper function to prepare Lineage for Sigma
    """
    try:
        lineage = {
            "tables": [],
            "relations": []
        }

        lineage = __prepare_lineage_tables(lineage, workbook)
        lineage = __prepare_lineage_relations(lineage, workbook)

        # Save Lineage
        save_reports_lineage(config, {**lineage})

        # Save Associated Asset Map
        map_asset_with_lineage(config, lineage, "report")

        # Save Propagation Values
        update_reports_propagations(config, config.get("asset", {}))

    except Exception as e:
        log_error(f"Sigma Connector - Prepare Lineage Failed ", e)
        raise e


def __prepare_lineage_tables(lineage: dict, workbook: dict) -> dict:
    """
    Helper function to prepare Lineage Tables for Sigma
    """
    try:
        workbook_tables = workbook.get("tables", [])
        workbook_pages = workbook.get("pages", [])

        for table in workbook_tables:
            table.update({
                "id": table.get("table_id"),
                "name": table.get("table_name")
            })

        lineage.update({
            "tables": workbook_tables + workbook_pages
        })
        return lineage
    except Exception as e:
        log_error(f"Sigma Connector - Prepare Lineage Table Failed ", e)
        raise e

def __prepare_lineage_relations(lineage: dict, workbook: dict) -> dict:
    """
    Helper function to prepare Lineage Relations for Sigma
    """
    try:
        workbook_pages = workbook.get("pages", [])
        
        for page in workbook_pages:
            fields = page.get("fields", [])
            page_id = page.get("id")
            
            for field in fields:
                # Create relation from table column to page
                relation = {
                    "srcTableId": field.get("table_name"),
                    "tgtTableId": page_id ,
                    "srcTableColName": field.get("name"),
                    "tgtTableColName": field.get("name")
                }
                lineage["relations"].append(relation)
        
        return lineage
    except Exception as e:
        log_error(f"Sigma Connector - Prepare Lineage Relations Failed ", e)
        raise e

def __run_postgres_query (config, query_string: str, query_type: str, output_statement: str = ''):

        """ Helper function to run postgres queries """
        connection = get_postgres_connection(config)
        records = ''
        with connection.cursor() as cursor:
            try:
                cursor = execute_query(
                        connection, cursor, query_string)
            except Exception as e:
                log_error(f"Atlan Connector - Query failed Error", e)
            
            if not query_type in ['insert','update','delete']:
                if query_type == 'fetchone':
                    records = fetchone(cursor)
                elif query_type == 'fetchall':
                    records = fetchall(cursor)
                
                return records
            
            log_info(("Query run:", query_string))
            log_info(('Query Executed',output_statement))

def __get_sigma_tags(config, workbook_id, workbook):
    params=dict(request_type= "get",
                                request_url= f"tags",
                                access_token = config.get("access_token", ""),
                                paginate = True)
    sigma_tags_list = __get_response(config, params = params)
    __delete_sigma_tags_in_dqlabs(config, sigma_tags_list)
    if sigma_tags_list:
        __insert_sigma_tags_into_dqlabs(config, sigma_tags_list)
        params=dict(request_type= "get",
                        request_url= f"workbooks/{workbook_id}/tags",
                        access_token = config.get("access_token", ""),
                        paginate = True)
        workbook_sigma_tags_list = workbook.get("tags", [])
        if workbook_sigma_tags_list:
            __map_sigma_tags(config, workbook_sigma_tags_list)

def __insert_sigma_tags_into_dqlabs(config, sigma_tags_list) -> object:
        organization_id = config.get('organization_id')
        log_info(("tags_info",sigma_tags_list))
        tag_id_name = {tag.get("versionTagId"): tag.get("name") for tag in sigma_tags_list if tag.get("versionTagId")}
        tag_ids = list(tag_id_name.keys())
    
        tag_ids_list = "', '".join(tag_ids)
        tag_check_query = f"""
            SELECT id FROM core.tags
            WHERE id IN ('{tag_ids_list}')
            AND source = '{ConnectionType.Sigma.value}' AND is_active = true AND is_delete = false
        """

        existing_tags = __run_postgres_query(config,tag_check_query, query_type='fetchall')
        existing_tag_ids = {item["id"] for item in existing_tags}

        new_tags = [tag_id for tag_id in tag_ids if tag_id not in existing_tag_ids]
        if not new_tags:
            log_info("No new tags to insert.")
            return

        insert_values = []
        for tag_id in new_tags:
            tag_name = tag_id_name[tag_id]
            insert_values.append((
                tag_id,
                tag_name,
                tag_name,
                None,
                "#21598a",
                ConnectionType.Sigma.value,
                False,
                json.dumps({"type": ConnectionType.Sigma.value}),
                1,
                False,
                True,
                False,
                str(organization_id),
                ConnectionType.Sigma.value
            ))

        try:
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(insert_values[0]))
                values_sql = b",".join([
                    cursor.mogrify(f"({placeholders}, CURRENT_TIMESTAMP)", vals)
                    for vals in insert_values
                ]).decode("utf-8")

                insert_tags_query = f"""
                    INSERT INTO core.tags (
                        id, name, technical_name, description, color, db_name, native_query_run, properties,
                        "order", is_mask_data, is_active, is_delete, organization_id, source, created_date
                    )
                    VALUES {values_sql}
                    RETURNING id;
                """

                log_info(f"Inserting {insert_tags_query} new tags into core.tags")
                execute_query(connection, cursor, insert_tags_query)
        except Exception as e:
            log_error("Bulk insert tags failed", e)
            raise e

def __delete_sigma_tags_in_dqlabs(config, sigma_tags_list) -> object:
        dqlabs_atlan_tags = []
        atlan_tags_guids = [tag.get("versionTagId") for tag in sigma_tags_list] # fetch atlan tags guids
        # fetch all atlan glossary terms in dqlabs
        dqlabs_tags_query = f"""
                            select id from core.tags
                            where source = '{ConnectionType.Sigma.value}' 
                            """
        dqlabs_atlan_tags_response = __run_postgres_query(
                                                    config,
                                                    query_string = dqlabs_tags_query,
                                                    query_type='fetchall'
                                                       )
        if dqlabs_atlan_tags_response:
            dqlabs_atlan_tags = [tag.get("id") for tag in dqlabs_atlan_tags_response]

        if dqlabs_atlan_tags:
            tags_to_remove = [tag_id for tag_id in dqlabs_atlan_tags if tag_id not in atlan_tags_guids]
            if tags_to_remove:
                tag_ids = "', '".join(tags_to_remove)
                remove_tags_mapping_query = f"""
                                                delete from core.tags_mapping
                                                where tags_id in ('{tag_ids}')
                                            """
                log_info(("remove_tags_mapping_query",remove_tags_mapping_query))
                __run_postgres_query(
                                config,
                                query_string = remove_tags_mapping_query,
                                query_type = 'delete',
                                output_statement = f"{tags_to_remove} unmapped from tags mapping")
                remove_tags_id = f"""
                                    delete from core.tags
                                    where id in ('{tag_ids}')
                                """
                # delete the term
                __run_postgres_query(
                                config,
                                query_string = remove_tags_id,
                                query_type = 'delete',
                                output_statement = f"{tag_ids} deleted from tags") 

def __map_sigma_tags(config, workbook_sigma_tags_list): 
    asset = config.get("asset", {})
    asset_id = asset.get("id")
    connection = get_postgres_connection(config)
    mapped_tag_ids = []
    if workbook_sigma_tags_list:
        for tag_mapped in workbook_sigma_tags_list:
            asset_tag_id = tag_mapped.get("versionTagId")
            mapped_tag_ids.append(asset_tag_id)
            #check if term id in tags table 
            attribute_tag_mapped_check_query =f"""
                                select exists (
                                select 1
                                from core."tags_mapping"
                                where tags_id = '{asset_tag_id}'
                                and asset_id = '{asset_id}'
                                );
                            """
            attribute_tag_mapped = __run_postgres_query(
                                                        config,
                                                        attribute_tag_mapped_check_query,
                                                        query_type='fetchone'
                                                        )
            attribute_tag_mapped = attribute_tag_mapped.get("exists",False)
            log_info(("attribute_tag_mapped for debug",attribute_tag_mapped))
            if not attribute_tag_mapped:
                #  map atlan terms to attributes
                try:
                    with connection.cursor() as cursor:
                        # Prepare attribute-tag mapping insert data
                        query_input = (
                            str(uuid4()),  # Unique ID
                            'asset',  # Level
                            asset_id,  # Asset ID
                            None,  # Attribute ID
                            asset_tag_id,  # Tag ID
                        )

                        # Generate placeholders for the query using %s
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals}, CURRENT_TIMESTAMP)", query_input
                        ).decode("utf-8")

                        # Insert attribute-tag mapping query
                        insert_asset_tag_query = f"""
                            INSERT INTO core.tags_mapping(
                                id, level, asset_id, attribute_id, tags_id, created_date
                            )
                            VALUES {query_param}
                            RETURNING id;
                        """
                        log_info(("insert_asset_tag_query", insert_asset_tag_query))

                        # Run query
                        execute_query(
                            connection, cursor, insert_asset_tag_query
                        )
                except Exception as e:
                    log_error(
                        f"Insert Asset Tag Mapping failed", e)
                    raise e
        # delete the tags mapping for unmapped tags to the asset
        mapped_tag_ids = "', '".join(mapped_tag_ids)
        delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
        and tm.asset_id = '{asset_id}' and tm.tags_id not in ('{mapped_tag_ids}') and tm.level = 'asset'  and t.source = '{ConnectionType.Sigma.value}'"""
        delete_tags_mapping_retrieve=__run_postgres_query(config,
                                                        delete_tags_mapping_retrieve_query,
                                                        query_type='delete'
                                                        )
        log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
    else:
        # delete the tags mapping if no tags has been mapped to the asset
        delete_tags_mapping_retrieve_query=f"""DELETE FROM core.tags_mapping tm using core.tags t where t.id = tm.tags_id
        and tm.asset_id = '{asset_id}' and tm.level = 'asset'  and t.source = '{ConnectionType.Sigma.value}'"""
        delete_tags_mapping_retrieve=__run_postgres_query(
        config,delete_tags_mapping_retrieve_query,
                                                        query_type='delete'
                                                        )
        log_info(("delete_tags_mapping_retrieve_query",delete_tags_mapping_retrieve_query))
                    
