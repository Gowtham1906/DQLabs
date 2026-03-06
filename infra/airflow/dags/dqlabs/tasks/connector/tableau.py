"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import json
import base64
import os
import re
import xml.etree.ElementTree as ET
import zipfile
import shutil
from uuid import uuid4
import requests

from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall, split_queries
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.utils.extract_workflow import update_asset_run_id, is_description_manually_updated
from dqlabs.app_helper.lineage_helper import save_reports_lineage, map_asset_with_lineage, update_reports_propagations, update_report_last_runs
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_constants.dq_constants import TABLEAU_CONNECTION_TYPE_MAPPING
from dqlabs.app_helper import agent_helper

TASK_CONFIG = None


def extract_tableau_data(config, **kwargs):
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
        credentials = decrypt_connection_config(credentials, connection_type)
        TASK_CONFIG = config

        token_obj = __validate_connection_establish()
        if token_obj:
            token = token_obj.get("token_id", "")
            if token:
                workbook = __get_workbook(config, asset_properties)
                if workbook:

                    # Save Views
                    __save_views(dag_info, config, asset_properties, workbook)

                    # Download Workbook for Lineage
                    __download_workbook_from_server(
                        config, asset_properties, workbook)

                    # Prepare Description
                    description = __update_description(workbook)

                    # Prepare Search Keys
                    search_keys = __prepare_search_key(asset_properties)
                    updatedAt=workbook.get('updatedAt','')
                    update_description = ""
                    if not is_description_manually_updated(config):
                        update_description = f" description='{description}'," if description else ""
                    print("update_description", update_description)
                    connection = get_postgres_connection(config)
                    with connection.cursor() as cursor:
                        query_string = f"""
                            update core.asset set {update_description} search_keys='{search_keys}',
                            properties = jsonb_set(properties, '{{updatedAt}}', to_jsonb('{updatedAt}'::text), true)
                            where id = '{asset_id}' and is_active=true and is_delete = false
                        """
                        print("query_string", query_string)
                        cursor = execute_query(
                            connection, cursor, query_string)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Tableau Pull Failed", e)
    finally:
        update_report_last_runs(config)


def __get_response(
    url,
    method_type: str = "get",
    params: dict = None,
    is_metadata: bool = False,
    api_version: str = "3.18",
):
    try:
        global TASK_CONFIG
        pg_connection = get_postgres_connection(TASK_CONFIG)
        api_response = agent_helper.execute_query(
            TASK_CONFIG,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                request_url=url,
                request_type=method_type,
                request_params=params,
                is_metadata=is_metadata,
                api_version=api_version,
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
        raise e


def __validate_connection_establish():
    try:
        api_response = None
        api_response = __get_response(
            "", "post", params=dict(method_type="get_token"))
        api_response = api_response if api_response else {}
    except Exception as e:
        log_error("Tableau Validate Connection Failed", e)
        raise e
    finally:
        return api_response


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


def __get_workbook(config: dict, properties: dict):
    try:
        # Tableau Api Request to get Workbook by Id
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        site_id = properties.get("site_id")
        workbook_id = properties.get("id")

        if workbook_id and site_id:
            url = f"sites/{site_id}/workbooks/{workbook_id}"
            response = __get_response(url)
            workbook = response.get("workbook", {})
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
                            properties = '{properties_obj}',
                            updated_at='{workbook.get('updatedAt')}'
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


def __prepare_description(properties) -> str:
    name = properties.get("name", "")
    project = properties.get("project", {})
    owner = properties.get("owner", {})
    description = f"""Workbook {name} is part of Project {project.get('name', "")} and owned by  {owner.get("name", "")}. """
    return f"{description}".strip()

def __update_description(properties) -> str:
    description = properties.get("description", "")
    if not description:
        description =  __prepare_description(properties)
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

        views = workbook.get('views', [])
        views = views.get('view', [])
        new_views = []

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            for view in views:
                view_id = view.get("id")

                # Validate Existing Reports Views (Sheet / Dashboard) Information
                query_string = f""" 
                    select id from core.reports_views 
                    where asset_id = '{asset_id}' and source_id = '{view_id}' 
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_view = fetchone(cursor)
                existing_view = existing_view.get(
                    'id') if existing_view else None

                properties_obj = {
                    "contentUrl": view.get("contentUrl", ""),
                    "viewUrlName": view.get("viewUrlName", ""),
                    "dataAccelerationConfig": view.get("dataAccelerationConfig", "")
                }
                tags = view.get("tags") if view.get("tags") else []
                image = {
                    "p_img": __get_view_preview_image(dag_info, {"site_id": properties.get("site_id"), "workbook_id": properties.get("id"), "view_id": view.get('id')}),
                    "img": __get_view_image(dag_info, {"site_id": properties.get("site_id"), "view_id": view.get('id')})
                }
                folder_path = __get_project_path(properties, workbook)

                if existing_view:
                    query_string = f"""
                        update core.reports_views set 
                            name = '{view.get('name', '')}',
                            updated_at = {f"'{view.get('updatedAt')}'" if view.get('updatedAt') else 'NULL'},
                            properties = '{json.dumps(properties_obj, default=str).replace("'", "''")}',
                            tags = '{tags}',
                            folder_path = '{folder_path}',
                            image = '{json.dumps(image, default=str).replace("'", "''")}'
                        where id = '{existing_view}'
                    """
                    cursor = execute_query(
                        connection, cursor, query_string)
                else:
                    query_input = (
                        str(uuid4()),
                        view.get("id"),
                        view.get("name"),
                        json.dumps(properties_obj, default=str).replace(
                            "'", "''"),
                        tags,
                        json.dumps(image, default=str).replace("'", "''"),
                        folder_path,
                        view.get("createdAt", ""),
                        view.get("updatedAt", ""),
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
                        folder_path, created_at, updated_at, report_id, asset_id, connection_id, is_active, is_delete)
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


def __get_view_preview_image(dag_info: dict, properties: dict):
    p_img = ""
    try:
        site_id = properties.get("site_id", "")
        workbook_id = properties.get("workbook_id", "")
        view_id = properties.get("view_id", "")

        # Tableau Api Request
        url = f"sites/{site_id}/workbooks/{workbook_id}/views/{view_id}/previewImage"
        response = __get_response(url, params=dict(is_file_download=True))
        image_data = bytes(response) if response else None
        file_name = f"{view_id}-p" if image_data else ""
        if not file_name:
            return
        p_img = __save_image(dag_info, file_name, image_data)
    except Exception as e:
        log_error("Tableau Get Preview Image Failed ", e)
        raise e
    finally:
        return p_img


def __get_view_image(dag_info: dict, properties: dict):
    img = ""
    try:
        site_id = properties.get("site_id", "")
        view_id = properties.get("view_id", "")

        # Tableau Api Request
        url = f"sites/{site_id}/views/{view_id}/image"
        response = __get_response(url, params=dict(is_file_download=True))
        image_data = bytes(response) if response else None
        file_name = view_id if image_data else ""
        if not file_name:
            return
        img = __save_image(dag_info, file_name, image_data)
    except Exception as e:
        log_error("Tableau Get Preview Image Failed ", e)
        raise e
    finally:
        return img


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


def __get_project_path(properties: dict, workbook: dict):
    path = ""
    try:
        site_id = properties.get("site_id", "")
        source_project = workbook.get("project", {})
        origin_path = source_project.get("name", "")

        if source_project and origin_path:
            # Tableau Api Request Get all Project By Site ID
            url = f"sites/{site_id}/projects?pageSize=1000"
            response = __get_response(url)
            projects_obj = response.get("projects", {})
            all_projects = projects_obj.get("project", [])

            project_by_source_id = [
                obj for obj in all_projects if obj["id"] == source_project["id"]
            ]

            if project_by_source_id:
                project_by_source_id = project_by_source_id[0]
                path = __recursive_path_finding(
                    all_projects, project_by_source_id, [])
                path.reverse()
                path.append(origin_path)
                path = "/".join(path)
    except Exception as e:
        log_error("Tableau Get Projects Failed ", e)
        raise e
    finally:
        return path


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


def __get_workbook_and_view_url(config: dict, type: str = "workbook", workbook: dict = {}, view: dict = None) -> str:
    tableau_url = ""
    try:
        connection = config.get("connection", {})
        asset = config.get("asset", {})
        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")

        ssl = credentials.get("ssl", "")
        server_type = credentials.get("server_type", "Cloud")
        site = asset_properties.get("site_name", "default")
        site_url = f"""site/{site}/"""
 


        connection_secure = "http://"
        if ssl:
            connection_secure = "https://"
        server = credentials.get("host", "")
        port = credentials.get("port", "")

        if port:
            tableau_site_url = f"{connection_secure}{server}:{port}"
        else:
            tableau_site_url = f"{connection_secure}{server}"

        if type == "workbook":
            workbook_portal_id = workbook.get('webpageUrl', '')
            workbook_portal_id = workbook_portal_id.split(
                "/")[-1] if workbook_portal_id else ""
            view_url = f"""workbooks/{workbook_portal_id}/views"""
        else:
            view_url = f"""views/{workbook.get("contentUrl")}/{view.get("viewUrlName")}"""

        # form the url
        if site.lower() == "default":
            tableau_url = f"""{tableau_site_url}/#/{view_url}"""
        else:
            tableau_url = f"""{tableau_site_url}/#/{site_url}{view_url}"""

    except Exception as e:
        log_error("Tableau Get Workbook or View Redirect URL Failed ", e)
        raise e
    finally:
        return tableau_url


def __download_workbook_from_server(config: dict, properties: dict, source_workbook: dict):
    try:
        # Download workbook from Server
        site_id = properties.get("site_id")
        workbook_id = properties.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        url = f"sites/{site_id}/workbooks/{workbook_id}/content"
        response = __get_response(url, params=dict(is_file_download=True, is_stream=True))
        #response = bytes(response) if response else None
        # make get http requests to the endpoint
        response = requests.get(url=response.get("endpoint"), headers=response.get("headers"), verify=False, stream=True)
        if not response:
            raise Exception(
                f"Failed to download workbook: , {response}")

        filename = f"{asset_id}.twbx"
        base_dir = str(os.path.abspath(os.path.join(
            os.path.dirname(__file__), "..", '..', '..', '..')))
        base_dir = os.path.join(
            base_dir, "infra/airflow/tests", f"{asset_id}/{workbook_id}")
        source_file_path = ""

        # create directory if doesn't exists
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        source_file_path = os.path.join(base_dir, filename)
        with open(source_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        if base_dir and source_file_path:
            workbook = __extract_downloaded_workboook_file(
                base_dir, source_file_path, source_workbook)

            # Save Columns
            __save_columns(config, workbook)

            # Update Workbook Statistics
            __update_workbook_statistics(config, workbook)

            # Prepare Lineage
            __prepare_lineage(config, workbook)

    except Exception as e:
        log_error("Tableau Download Workbook On Server Failed, Now Trying On S3 ", e)
        handle_processing_workbook_on_s3(config, properties, source_workbook)

def __extract_downloaded_workboook_file(folder_path, file_path, source_workbook):
    """
    Helper function to extract datasource , database , sheets, dashboard information from downloaded workbook.
    """
    try:
        extracted_folder = "extracted_twbx_content"
        workbook = {
            "data_sources": [],
            "databases": [],
            "tables": [],
            "columns": [],
            "used_columns": [],
            "sheets": [],
            "dashboards": []
        }

        # change directory
        os.chdir(folder_path)
        # Extract the .twbx content
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extracted_folder)

        # Get the path of the .twb file
        twb_file_path = os.path.join(extracted_folder, os.path.basename(
            file_path).replace(".twbx", ".twb"))

        # If .twb file not exists, found the .twb file
        if not os.path.exists(twb_file_path):
            existing_twb_files = [f for f in os.listdir(
                extracted_folder) if f.endswith(".twb")]
            if existing_twb_files:
                twb_file_path = os.path.join(
                    extracted_folder, existing_twb_files[0])
            else:
                return workbook

        # If .twb file exists, parse it
        tree = ET.parse(twb_file_path)
        root = tree.getroot()

        # Extract Workbook Information
        workbook = __extract_workbook_info(root, workbook)

        calculated_fields = __extract_calculated_fields(root, workbook)
        workbook['calculated_fields'] = calculated_fields

        # Extract Workbook Views Information
        workbook = __extract_workbook_views(root, workbook, source_workbook)

        # Cleanup extracted files
        shutil.rmtree(extracted_folder)
        os.remove(file_path)

        return workbook
    except Exception as e:
        log_error("Tableau Get Projects Recursive Path Failed ", e)
        raise e


def __extract_workbook_info(root, workbook):
    try:
        # Iterate through all worksheets and dashboards
        for datasource in root.findall('datasources'):
            # Prepare Data Source and Other Info
            for ds in datasource.findall('datasource'):
                workbook["data_sources"].append(
                    {
                        "id": ds.get("name", ""),
                        "name": ds.get("caption", "") if ds.get("caption") else ds.get("name", "")
                    }
                )

                # Prepare Database List
                for database in ds.findall("./connection/named-connections/named-connection"):
                    connection = database.find('connection')
                    workbook["databases"].append(
                        {
                            "datasource_id": ds.get("name", ""),
                            "datasource_name": ds.get("caption", "") if ds.get("caption") else ds.get("name", ""),
                            "database_id": database.get("name", ""),
                            "database": connection.get("dbname", ""),
                            "schema": connection.get('schema', ""),
                            "server": connection.get("server", ""),
                            "filename": connection.get("filename", ""),
                            "connection_type": TABLEAU_CONNECTION_TYPE_MAPPING.get(connection.get("class"), "") if connection.get("class") else "",
                            "level": 1
                        }
                    )

                # Prepare Tables List
                table_collections = ds.findall(
                    "./connection/*[@type='collection']")
                if table_collections:
                    for table in table_collections:
                        table_list = table.findall('relation')
                        for detail in table_list:
                            table = __extract_tables(detail, workbook.get(
                                "tables"), workbook.get("databases"))
                            if table:
                                workbook["tables"].append(table)
                else:
                    for table in ds.findall("./connection/*[@type='table']"):
                        table = __extract_tables(table, workbook.get(
                            "tables"), workbook.get("databases"))
                        if table:
                            workbook["tables"].append(table)

                # Prepare Columns List
                for attribute in ds.findall("./connection/metadata-records/metadata-record"):
                    column_name = attribute.find(
                        "remote-name").text if attribute.find("remote-name") != None else ""
                    table_name = attribute.find(
                        "parent-name").text if attribute.find("parent-name") != None else ""
                    table_name = table_name.replace("[", "").replace("]", "")
                    table = next((item for item in workbook.get(
                        "tables") if item.get("table_name", "") == table_name), {})
                    workbook["columns"].append(
                        {
                            "name": column_name,
                            "id": attribute.find("local-name").text if attribute.find("local-name") != None else column_name,
                            "data_type": attribute.find("local-type").text if attribute.find("local-type") != None else "string",
                            "is_null": attribute.find("contains-null").text if attribute.find("contains-null") != None else True,
                            "is_semantics": attribute.find("padded-semantics").text if attribute.find("padded-semantics") != None else False,
                            **table,
                            "level": 4
                        }
                    )

                # Update Table Columns
                for table in workbook["tables"]:
                    table.update({
                        "fields": [item for item in workbook.get("columns") if item.get("table_id", "") == table.get("table_id")]
                    })
        return workbook
    except Exception as e:
        log_error(f"Tableau Connector - Extract Workbook Info ", e)
        raise e


def __get_dashboard_attributes(root, workbook):
    dashboards = workbook.get("dashboards")
    sheets = workbook.get("sheets")
    workbook.update({
        "dashboards": []
    })
    for dashboard in dashboards:
        dashboard_name = dashboard.get("name", "")
        dashboard_view = root.find(f".//dashboard[@name='{dashboard_name}']")
        zone_names = list({zone.attrib['name'] for zone in dashboard_view.findall(
            ".//zone") if 'name' in zone.attrib}) if dashboard_view else []
        attributes_list = []
        for zone_name in zone_names:
            for sheet in sheets:
                if sheet.get("name", "") == zone_name:
                    fields_list = sheet.get("fields")
                    fields_list = [
                        {
                            **field,
                            "sheet_source_id": sheet.get("source_id"),
                            "table_id": sheet.get("source_id")
                        }
                        for field in fields_list
                    ]
                    attributes_list.extend(fields_list)
        dashboard.update({
            "fields": attributes_list
        })
        workbook["dashboards"].append(dashboard)
    return workbook


def __extract_workbook_views(root, workbook, source_workbook: dict) -> dict:
    try:

        # Get Source Workbook Views
        source_views = source_workbook.get('views', [])
        source_views = source_views.get('view', [])

        # Process both worksheets and dashboards
        for view in root.findall(".//worksheet") + root.findall(".//dashboard"):
            view_detail = view.find("repository-location", "")
            item_name = view.get('name', '')
            item_id = view_detail.get("id", "") if view_detail else ""

            if item_id:
                source_view = next(
                    (item for item in source_views if item.get("viewUrlName") == item_id), {})
            else:
                source_view = next(
                    (item for item in source_views if item.get("name") == item_name), {})

            columns = __extract_worbook_viwes_columns(
                view, workbook, source_view)
            columns = columns if columns else []

            common_object = {
                "id": item_id,
                "name": item_name,
                "source_id": source_view.get("id") if source_view else "",
                "type": "Sheet",
                "upstreamDatasources": [],
                "upstreamDatabases": [],
                "upstreamTables": [],
                "upstreamFields": [],
                "upstreamColumns": [],
                "fields": columns,
                "level": 3
            }
            if view.tag == "worksheet":
                workbook["sheets"].append(common_object)
            elif view.tag == "dashboard":
                common_object["type"] = "Dashboard"
                workbook["dashboards"].append(common_object)

            # Update Used Columns
            used_columns = workbook.get("used_columns", [])
            workbook.update({"used_columns": used_columns + columns})
            workbook = __get_dashboard_attributes(root, workbook)
        return workbook
    except Exception as e:
        log_error(f"Tableau Connector - Extract Workbook Views Info ", e)
        raise e


def __extract_worbook_viwes_columns(element, workbook, source_view):
    """
    Helper function to process columns in a worksheet or dashboard.
    """
    try:
        columns = []
        for column in element.findall(".//datasource-dependencies/column"):
            column_detail = next((item for item in workbook.get(
                "columns") if item["id"] == column.get("name")), {})
            columns.append({
                "id": column.get("name"),
                "name": column.get("caption", "") if column.get("caption") else column.get("name", ""),
                "role_name": column.get("role", ""),
                "semantic_role": column.get("semantic-role", ""),
                "type": column.get("type", ""),
                **column_detail
            })
        return columns
    except Exception as e:
        log_error(f"Tableau Connector - Extract Workbook Views Info ", e)
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
            views = workbook.get('sheets', []) + workbook.get('dashboards', [])
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
                                        id, name, table_name, database, schema, data_type, is_null, is_semantics,
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
            
            # Process and save calculated fields
            calculated_fields = workbook.get('calculated_fields', [])
            if calculated_fields:
                __save_calculated_fields_with_scores(config, calculated_fields, report_id, asset_id, connection_id, connection)
                
    except Exception as e:
        log_error(f"Tableau Connector - Save Views Columns By View ID Failed ", e)
        raise e

def __save_calculated_fields_with_scores(config: dict, calculated_fields: list, report_id: str, asset_id: str, connection_id: str, connection=None):
    """
    Save calculated fields to the database with their calculated scores.
    """
    try:
        if connection is None:
            connection = get_postgres_connection(config)
        else:
            log_info("Using existing database connection")
        with connection.cursor() as cursor:
            for calculated_field in calculated_fields:
                # Calculate the score for this calculated field
                score = calculate_calculated_field_score(config, calculated_field)
                
                # Prepare the properties JSON
                properties = {
                    'is_calculated_field': True,
                    'formula': calculated_field.get('formula', ''),
                    'used_attributes': calculated_field.get('used_attributes', []),
                    'calculated_score': score,
                    'source': 'tableau',
                    'worksheet_name': calculated_field.get('worksheet_name', ''),
                    'caption': calculated_field.get('caption', ''),
                    'attribute_scores': calculated_field.get('attribute_scores', []),
                    'total_attributes': calculated_field.get('total_attributes', 0),
                    'configured_attributes': calculated_field.get('configured_attributes', 0),
                    'unconfigured_attributes': calculated_field.get('unconfigured_attributes', 0)
                }

                existing_query = f"""
                    SELECT id FROM core.reports_columns 
                    WHERE asset_id = '{asset_id}' 
                    AND LOWER(name) = LOWER('{calculated_field.get('caption', '')}')
                """
                cursor.execute(existing_query)
                existing = fetchall(cursor)                
                
                if existing:
                    for record in existing:
                        existing_id = record.get('id')
                        
                        if existing_id:
                            data_type = calculated_field.get('datatype', 'string')
                            # Properly escape the JSON string for SQL
                            properties_json = json.dumps(properties).replace("'", "''")
                            update_query = f"""
                                UPDATE core.reports_columns 
                                SET
                                    data_type = '{data_type}',
                                    report_column_properties = '{properties_json}'::jsonb
                                WHERE id = '{existing_id}'
                            """
                            cursor.execute(update_query)
                        else:
                            log_info(f"No valid ID found in record: {record}")
                else:
                    log_info(f"No existing calculated field found for: {calculated_field.get('caption', calculated_field.get('name'))}")

    except Exception as e:
        log_error(f"Tableau Connector - Save Calculated Fields With Scores Failed ", e)
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
                "tot_sheets": len(workbook.get('sheets', [])) if workbook.get('sheets') else 0,
                "tot_dashboards": len(workbook.get('dashboards', [])) if workbook.get('dashboards') else 0
            })
            path = __get_workbook_and_view_url(config, "workbook", properties)
            query_string = f"""
                    update core.reports set 
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                        url = '{path}'
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
            path = __get_workbook_and_view_url(config, view.get(
                'type', ''), workbook_properties, properties)
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
    Helper function to prepare Lineage
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
        log_error(f"Tableau Connector - Prepare Lineage Failed ", e)
        raise e


def __prepare_lineage_tables(lineage: dict, workbook: dict) -> dict:
    """
    Helper function to prepare  Lineage Tables
    """
    try:
        workbook_tables = workbook.get("tables", [])
        workbook_sheets = workbook.get("sheets", [])
        workbook_dashboard = workbook.get("dashboards", [])

        for table in workbook_tables:
            table.update({
                "id": table.get("table_id"),
                "name": table.get("table_name")
            })

        lineage.update({
            "tables": workbook_tables + workbook_sheets + workbook_dashboard
        })
        return lineage
    except Exception as e:
        log_error(f"Tableau Connector - Prepare Lineage Table Failed ", e)
        raise e


def __prepare_lineage_relations(lineage: dict, workbook: dict) -> dict:
    """
    Helper function to prepare Lineage Relations
    """
    try:
        workbook_views = workbook.get(
            "sheets", []) + workbook.get("dashboards", [])
        total_sheet_fields = []
        for view in workbook_views:
            fields = view.get("fields", [])
            view_type = view.get("type")
            if view_type == "Sheet":
                total_sheet_fields.extend(fields)
            for field in fields:
                if view_type == "Sheet":
                    columns = [item for item in workbook.get(
                        "columns") if item.get("id") == field.get("id")]
                else:
                    columns = [item for item in total_sheet_fields if item.get(
                        "id") == field.get("id")]
                for col in columns:
                    relation = {
                        "srcTableId": col.get("table_id") if view_type == "Sheet" else field.get("table_id"),
                        "tgtTableId": view.get("source_id") if view.get("source_id") else view.get("id"),
                        "srcTableColName": col.get("name"),
                        "tgtTableColName": field.get("name")
                    }
                    lineage["relations"].append(relation)
        return lineage
    except Exception as e:
        log_error(f"Tableau Connector - Prepare Lineage Table Failed ", e)

def __download_workbook_to_s3(config: dict, properties: dict, source_workbook: dict):
    try:
        log_info("START HANDLING OF TABLEAU WORKBOOK ON S3...")
        # Download workbook from Server
        site_id = properties.get("site_id")
        workbook_id = source_workbook.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        dag_info = config.get("dag_info")
        # Construct the URL for the Tableau API request
        url = f"sites/{site_id}/workbooks/{workbook_id}/content"
        # make get http requests to the endpoint
        response = __get_response(url, params=dict(is_file_download=True, is_stream=True))
        destination_path = f"tableau/{asset_id}/{workbook_id}/"
        target_prefix = f"{destination_path}extracted"
        destination_key = f"tableau/{asset_id}/{workbook_id}/{asset_id}.twbx"
        storage_service = get_storage_service(dag_info)
        # upload the file to S3
        with requests.get(url=response.get("endpoint"), headers=response.get("headers"), verify=False, stream=True) as stream_response:
            stream_response.raise_for_status()
            storage_service.upload_streaming_file(dag_info, stream_response, destination_key)
        
        # unzip the file and upload to S3
        storage_service.stream_extract_on_s3(dag_info=dag_info, source_key=destination_key, target_prefix=target_prefix)
        
        # get files listed in the target folder
        workbook = {
            "data_sources": [],
            "databases": [],
            "tables": [],
            "columns": [],
            "used_columns": [],
            "sheets": [],
            "dashboards": []
        }
        response = storage_service.list_files(dag_info=dag_info, prefix=target_prefix)
        for file in response:
            if file.endswith(".twb"):
                root = storage_service.stream_parse_xml_from_s3(dag_info=dag_info, file_key=file)
                # Extract Workbook Information
                workbook = __extract_workbook_info(root, workbook)
                calculated_fields = __extract_calculated_fields(root, workbook)
                workbook['calculated_fields'] = calculated_fields
                # Extract Workbook Views Information
                workbook = __extract_workbook_views(root, workbook, source_workbook)
        
        # delete the source folder
        storage_service.delete_s3_folder_versioned(dag_info=dag_info, folder_prefix=destination_path)
        log_info("END HANDLING OF TABLEAU WORKBOOK ON S3...")
        return workbook
    except Exception as e:
        log_error("Tableau download -> upload -> extract -> read workbook on s3 bucket failed ", e)


def handle_processing_workbook_on_s3(config: dict, properties: dict, source_workbook: dict):
    """
    Handle the processing of workbook on S3.
    """
    try:
        # download -> upload -> extract -> read workbook on s3 bucket
        workbook = __download_workbook_to_s3(config, properties, source_workbook)
        # Save Columns
        __save_columns(config, workbook)

        # Update Workbook Statistics
        __update_workbook_statistics(config, workbook)

        # Prepare Lineage
        __prepare_lineage(config, workbook)
    except Exception as e:
        log_error("Tableau Workbook Processing On S3 Failed ", e)

def __find_asset_by_db_schema_table(config, database, schema, table_name):
    """
    Find asset by database, schema, and table name.
    """
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Build condition query
            condition_query = f"""
                and lower(asset.name) = lower('{table_name}')
                and lower(asset.properties->>'database') = lower('{database}')
            """
            if schema:
                condition_query = f"""
                    and lower(asset.name) = lower('{table_name}')
                    and lower(asset.properties->>'schema') = lower('{schema}')
                    and lower(asset.properties->>'database') = lower('{database}')
                """
            
            query_string = f"""
                select asset.id, asset.name, asset.score, asset.properties
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
        log_error("Tableau - Find Asset By Database,Schema and Table Failed ", e)
        return []

def __get_attribute_score_from_asset(config, asset_id, attribute_name):
    """
    Get the score of a specific attribute from a configured asset.
    """
    try:
        if not asset_id:
            return None
            
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Clean up attribute name (remove brackets if present)
            clean_attribute_name = attribute_name.replace('[', '').replace(']', '')
            
            query_string = f"""
                SELECT 
                    attribute.id,
                    attribute.name,
                    attribute.score,
                    attribute.alerts,
                    attribute.datatype,
                    attribute.is_active
                FROM core.attribute
                WHERE attribute.asset_id = '{asset_id}'
                  AND attribute.is_active = TRUE
                  AND attribute.is_delete = FALSE
                  AND (
                      LOWER(attribute.name) = LOWER('{clean_attribute_name}')
                      OR LOWER(attribute.name) = LOWER('{attribute_name}')
                      OR LOWER(attribute.technical_name) = LOWER('{clean_attribute_name}')
                      OR LOWER(attribute.technical_name) = LOWER('{attribute_name}')
                  )
                ORDER BY attribute.id ASC
                LIMIT 1
            """
            cursor = execute_query(connection, cursor, query_string)
            result = fetchone(cursor)
            
            if result:
                return {
                    'attribute_id': result.get('id'),
                    'attribute_name': result.get('name'),
                    'score': float(result.get('score')) if result.get('score') is not None else None,
                    'alerts': result.get('alerts', 0),
                    'datatype': result.get('datatype'),
                    'is_active': result.get('is_active', True)
                }
            
            return None
    except Exception as e:
        log_error(f"Tableau Connector - Get Attribute Score Failed ", e)
        return None

def __calculate_calculated_field_score(config, calculated_field):
    """
    Calculate the average score for a calculated field based on its used attributes.
    """
    try:
        used_attributes = calculated_field.get('used_attributes', [])
        if not used_attributes:
            return None
            
        total_score = 0
        valid_attributes = 0
        attribute_scores = []
        
        for attr in used_attributes:
            table_name = attr.get('table_name', '')
            database = attr.get('database', '')
            schema = attr.get('schema', '')
            attribute_name = attr.get('name', '')
            
            log_info(f"Processing attribute: {attribute_name} from table: {table_name}, database: {database}, schema: {schema}")
            
            if table_name and database:
                # Find the asset for this table
                assets = __find_asset_by_db_schema_table(config, database, schema, table_name)
                
                if assets and len(assets) > 0:
                    asset_id = assets[0].get('id')
                    asset_score = assets[0].get('score')
                    
                    # Get the specific attribute score from the asset
                    attribute_score_data = __get_attribute_score_from_asset(config, asset_id, attribute_name)
                    
                    if attribute_score_data and attribute_score_data.get('score') is not None:
                        attr_score = attribute_score_data.get('score')
                        total_score += attr_score
                        valid_attributes += 1
                        
                        # Store detailed score information
                        attribute_scores.append({
                            'attribute_name': attribute_name,
                            'table_name': table_name,
                            'database': database,
                            'schema': schema,
                            'asset_id': asset_id,
                            'asset_score': asset_score,
                            'attribute_score': attr_score,
                            'attribute_id': attribute_score_data.get('attribute_id'),
                            'alerts': attribute_score_data.get('alerts', 0),
                            'datatype': attribute_score_data.get('datatype')
                        })
                        
                    else:
                        attribute_scores.append({
                            'attribute_name': attribute_name,
                            'table_name': table_name,
                            'database': database,
                            'schema': schema,
                            'asset_id': asset_id,
                            'asset_score': asset_score,
                            'attribute_score': None,
                            'attribute_id': None,
                            'alerts': 0,
                            'datatype': None,
                            'reason': 'Attribute not found in configured asset'
                        })
                else:
                    attribute_scores.append({
                        'attribute_name': attribute_name,
                        'table_name': table_name,
                        'database': database,
                        'schema': schema,
                        'asset_id': None,
                        'asset_score': None,
                        'attribute_score': None,
                        'attribute_id': None,
                        'alerts': 0,
                        'datatype': None,
                        'reason': 'Asset not configured in portal'
                    })
        
        if valid_attributes > 0:
            average_score = round(total_score / valid_attributes, 2)
            
            # # Add detailed information to the calculated field
            calculated_field['attribute_scores'] = attribute_scores
            calculated_field['total_attributes'] = len(used_attributes)
            calculated_field['configured_attributes'] = valid_attributes
            calculated_field['unconfigured_attributes'] = len(used_attributes) - valid_attributes
            
            return average_score
        
        calculated_field['attribute_scores'] = attribute_scores
        calculated_field['total_attributes'] = len(used_attributes)
        calculated_field['configured_attributes'] = 0
        calculated_field['unconfigured_attributes'] = len(used_attributes)
        
        return None
    except Exception as e:
        log_error(f"Tableau Connector - Calculate Calculated Field Score Failed ", e)
        return None

def calculate_calculated_field_score(config, calculated_field):
    """
    Calculate the average score for a single calculated field.
    
    Args:
        config: Configuration dictionary
        calculated_field: Dictionary containing calculated field data with used_attributes
        
    Returns:
        float: Average score of the calculated field, or None if no valid attributes
    """
    try:
        return __calculate_calculated_field_score(config, calculated_field)
    except Exception as e:
        log_error(f"Tableau Connector - Calculate Calculated Field Score Failed ", e)
        return None

def __replace_calculated_field_ids_in_formula(formula, calculated_fields):
    """
    Replace calculated field IDs (like Calculation_123) with their actual names in the formula.
    
    Args:
        formula: The formula string to process
        calculated_fields: List of all calculated fields in the workbook
    
    Returns:
        str: Formula with calculated field IDs replaced with names
    """
    try:
        if not formula or not calculated_fields:
            return formula
            
        # Create a mapping of calculated field IDs to their names
        id_to_name_map = {}
        for calc_field in calculated_fields:
            field_name = calc_field.get('name', '')
            field_caption = calc_field.get('caption', '')
            
            # Map both the internal name and caption to the display name
            if field_name:
                id_to_name_map[field_name] = field_caption if field_caption else field_name
            if field_caption and field_caption != field_name:
                id_to_name_map[field_caption] = field_caption
        
        # Find all column references in the formula (enclosed in square brackets)
        column_pattern = r'\[([^\]]+)\]'
        
        def replace_match(match):
            content = match.group(1)
            
            # Check if this is a calculated field ID (starts with Calculation_)
            if content.startswith('Calculation_'):
                # Look for a calculated field with this exact name
                for calc_field in calculated_fields:
                    if calc_field.get('name') == f'[{content}]':
                        # Use caption if available, otherwise use the name without brackets
                        display_name = calc_field.get('caption', '')
                        if not display_name:
                            display_name = calc_field.get('name', '').replace('[', '').replace(']', '')
                        return f'[{display_name}]'
                # If not found, return original
                return match.group(0)
            else:
                # Check if this content matches any calculated field name or caption
                for calc_field in calculated_fields:
                    field_name = calc_field.get('name', '').replace('[', '').replace(']', '')
                    field_caption = calc_field.get('caption', '')
                    
                    if (content == field_name or 
                        content == field_caption or
                        content == calc_field.get('name', '')):
                        # Use caption if available, otherwise use the name
                        display_name = field_caption if field_caption else field_name
                        return f'[{display_name}]'
                
                # If not a calculated field, return original
                return match.group(0)
        
        # Replace all matches in the formula
        updated_formula = re.sub(column_pattern, replace_match, formula)
        
        return updated_formula
        
    except Exception as e:
        log_error(f"Tableau Connector - Replace Calculated Field IDs in Formula Failed ", e)
        return formula

def __extract_calculated_fields(root, workbook):
    """
    Extract calculated fields from the Tableau workbook XML.
    """
    try:
        calculated_fields = []
        # Find calculated fields in worksheets
        for worksheet in root.findall('.//worksheet'):
            worksheet_name = worksheet.get('name', '')
            for column in worksheet.findall('.//column'):
                calculation = column.find('.//calculation')
                if calculation is not None:
                    # Get formula from the 'formula' attribute
                    formula = calculation.get('formula', '')
                    field_name = column.get('name', '')
                    caption = column.get('caption', '')
                    datatype = column.get('datatype', 'string')
                    
                    calculated_fields.append({
                        'name': field_name,
                        'caption': caption,
                        'datatype': datatype,
                        'formula': formula,
                        'used_attributes': [],
                        'worksheet_name': worksheet_name
                    })
        
        # Replace calculated field IDs with names in all formulas
        for calculated_field in calculated_fields:
            original_formula = calculated_field['formula']
            updated_formula = __replace_calculated_field_ids_in_formula(original_formula, calculated_fields)
            calculated_field['formula'] = updated_formula
            
            # Log the change for debugging
            if original_formula != updated_formula:
                log_info(f"Tableau Connector - Updated formula for {calculated_field.get('caption', calculated_field.get('name', ''))}: {original_formula} -> {updated_formula}")
        
        # Now extract attributes for each calculated field with access to all calculated fields
        for calculated_field in calculated_fields:
            used_attributes = __extract_formula_attributes_recursive(
                calculated_field['formula'], 
                workbook, 
                calculated_fields,
                set()  # visited set to prevent infinite recursion
            )
            calculated_field['used_attributes'] = used_attributes
            
        return calculated_fields
    except Exception as e:
        log_error(f"Tableau Connector - Extract Calculated Fields Failed ", e)
        return []

def __extract_formula_attributes_recursive(formula, workbook, calculated_fields, visited):
    """
    Extract attributes used in a calculated field formula with recursive support for nested calculated fields.
    
    Args:
        formula: The formula string to analyze
        workbook: The workbook data containing columns
        calculated_fields: List of all calculated fields in the workbook
        visited: Set of already visited calculated field names to prevent infinite recursion
    
    Returns:
        List of attributes used in the formula
    """
    try:
        used_attributes = []
        # Find all column references in the formula (enclosed in square brackets)
        column_pattern = r'\[([^\]]+)\]'
        matches = re.findall(column_pattern, formula)
        
        for match in matches:
            # Skip only internal Tableau objects, but NOT Calculation_ fields
            if match.startswith('__tableau_internal_object_id__'):
                continue
            
            # Check if this reference is a calculated field by looking for Calculation_ prefix
            referenced_calculated_field = None
            if match.startswith('Calculation_'):
                # This is a reference to another calculated field by its internal ID
                for calc_field in calculated_fields:
                    if calc_field.get('name') == f'[{match}]':
                        referenced_calculated_field = calc_field
                        break
            else:
                # Check if this is a reference to a calculated field by name or caption
                for calc_field in calculated_fields:
                    if (calc_field.get('name') == f'[{match}]' or 
                        calc_field.get('caption') == match or
                        calc_field.get('name') == match):
                        referenced_calculated_field = calc_field
                        break
            
            if referenced_calculated_field:
                # This is a reference to another calculated field
                calc_field_name = referenced_calculated_field.get('name', '')
                calc_field_caption = referenced_calculated_field.get('caption', '')
                
                # Use the internal name (with brackets) as the key to prevent infinite recursion
                field_key = calc_field_name
                
                # Prevent infinite recursion
                if field_key not in visited:
                    visited.add(field_key)
                    try:
                        # Recursively extract attributes from the referenced calculated field
                        nested_attributes = __extract_formula_attributes_recursive(
                            referenced_calculated_field['formula'],
                            workbook,
                            calculated_fields,
                            visited.copy()  # Pass a copy to allow different branches
                        )
                        used_attributes.extend(nested_attributes)
                    except Exception as e:
                        log_error(f"Tableau Connector - Recursive extraction failed for calculated field {field_key}", e)
                    finally:
                        visited.discard(field_key)  # Remove from visited set
                else:
                    log_info(f"Tableau Connector - Skipping recursive reference to calculated field {field_key} to prevent infinite loop")
            else:
                # This is a reference to a regular column/attribute
                for column in workbook.get('columns', []):
                    column_name = column.get('name', '')
                    column_id = column.get('id', '')
                    
                    # Check if this column matches the formula reference
                    if (column_name == f'[{match}]' or 
                        column_id == match or 
                        column_name == match or
                        column.get('remote-name') == match):
                        
                        # Get table information from the column
                        table_name = column.get('table_name', '')
                        database = column.get('database', '')
                        schema = column.get('schema', '')
                       
                        
                        # If table info is not directly available, try to get from parent_name
                        if not table_name and column.get('parent-name'):
                            parent_name = column.get('parent-name', '')
                            # Extract table name from parent_name like "[EMPLOYEE_SALES_VW]"
                            table_match = re.search(r'\[([^\]]+)\]', parent_name)
                            if table_match:
                                table_name = table_match.group(1)
                        
                        used_attributes.append({
                            'name': column.get('name', ''),
                            'table_name': table_name,
                            'database': database,
                            'schema': schema
                        })
                        break
        
        # Remove duplicates while preserving order
        seen = set()
        unique_attributes = []
        for attr in used_attributes:
            # Create a unique key for each attribute
            attr_key = (attr.get('name', ''), attr.get('table_name', ''), attr.get('database', ''), attr.get('schema', ''))
            if attr_key not in seen:
                seen.add(attr_key)
                unique_attributes.append(attr)
        
        return unique_attributes
    except Exception as e:
        log_error(f"Tableau Connector - Extract Formula Attributes Recursive Failed ", e)
        return []