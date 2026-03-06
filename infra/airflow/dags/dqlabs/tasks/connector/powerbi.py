"""
    Migration Notes From V2 to V3:
    Migrations Completed 
"""

import time
import re
import os
import zipfile
import shutil
from itertools import groupby
import ast
from dqlabs.app_helper.db_helper import execute_query, fetchone, split_queries
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.storage_helper import get_storage_service
from dqlabs.app_helper.dq_helper import get_attribute_label
from dqlabs.app_helper.log_helper import log_error
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.app_helper.lineage_helper import save_reports_lineage, map_asset_with_lineage, update_reports_propagations, update_report_last_runs
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_constants.dq_constants import POWERBI_CONNECTION_TYPE_MAPPING
import base64
from dqlabs.utils.extract_workflow import update_asset_run_id, is_description_manually_updated
from dqlabs.app_helper import agent_helper
import json
from uuid import uuid4
from PIL import Image
import requests
from io import BytesIO
import concurrent.futures
import threading
thread_local = threading.local()
import random


def extract_powerbi_data(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return
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

        workspace_id = asset_properties.get("id")
        workspace_scanResult = {}
        workspace_info = {}
        try:
            scanId = __start_lineage_scan(config, asset_properties)
            if scanId:
                scanStatus = __check_scan_status(config, scanId)
                if scanStatus:
                    workspace_scanResult = __get_scan_result(config, scanId)
                    workspace_scanResult = workspace_scanResult.get("workspaces")[0] if workspace_scanResult.get("workspaces") else {}
                    workspace_info = extract_workspace_info(config, workspace_scanResult)
                    __get_dashboard_and_reports_by_workspace_id(
                        config, asset_properties, workspace_scanResult, workspace_info
                    )
        except Exception as e:
            # powerbi with limited access
            workspace_scanResult = fetch_workspace_scanResult_without_scanId(config, workspace_id)
            workspace_scanResult = workspace_scanResult.get("workspaces")[0] if workspace_scanResult.get("workspaces") else {}
            workspace_info = extract_workspace_info(config, workspace_scanResult)
            __get_dashboard_and_reports_by_workspace_id_without_scanId(
                config, asset_properties, workspace_scanResult, workspace_info
            )

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            workspace_id = workspace_scanResult.get("id")
            reports = workspace_scanResult.get("reports", [])
            workspce_detail = workspace_scanResult.get("Exploration", [])
            workspce_detail = workspce_detail[0] if workspce_detail else {}

            # if workspce_detail:
            owner = workspce_detail.get("createdBy", "")
            description = workspce_detail.get("description", "")
            created_at = workspce_detail.get("createdDate", "")
            updated_at = workspce_detail.get("lastUpdatedDate", "")
            if not updated_at:
                owner = next(( x.get("createdBy")for x in reports if x.get("createdBy")), None)
                updated_at = max((x["modifiedDateTime"] for x in reports if "modifiedDateTime" in x), default=None)
                if updated_at:
                    updated_at = f"{updated_at}Z"
            name = workspace_scanResult.get("name")
            is_valid = workspce_detail.get("state")
            url = fr"https://app.powerbi.com/groups/{workspace_id}"

            properties_obj = {
                "id": workspace_id,
                "name": name,
                "type": 'workspace',
                "is_valid": is_valid,
                "owner_id": workspce_detail.get("createdById", ""),
                "createdAt": created_at,
                "workspace": name,
                "updatedAt": updated_at,
                "owner_name": owner,
            }
            created_date_condition = f"created_date='{created_at}'," if created_at else ""
            modified_date_condition = f" modified_date='{updated_at}'," if updated_at else ""
            if not description:
                if owner:
                    description = f"""Workspace {name} is owned by  {owner}."""
                else:
                    description = f"""Workspace {name}."""
            updated_date_condition = f" updated_at='{updated_at}'," if updated_at else ""
            properties_obj = json.dumps(
                properties_obj, default=str).replace("'", "''")
            update_description = ""
            if not is_description_manually_updated(config):
                update_description = f"description = '{description}'," if description else ""
            print("update_description", update_description)
            query_string = f"""
                update core.asset set
                {update_description}
                {created_date_condition} 
                {modified_date_condition}
                properties='{properties_obj}' 
                where id = '{asset_id}' and is_active=true and is_delete = false
            """
            print("query_string", query_string)
            view_query_string = f"""
                        update core.reports set 
                            {updated_date_condition}
                            description = '{description}',  
                            owner = '{owner}',
                            url = '{url}'
                        where asset_id = '{asset_id}'
                    """

            # if created_at and updated_at:
            cursor = execute_query(connection, cursor, query_string)

            execute_query(connection, cursor, view_query_string)

        # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Power BI Pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_report_last_runs(config)


def extract_workspace_info(config, workspace_scan_result):
    workspace = {
        "data_sources": [],
        "databases": [],
        "datasets": [],
        "tables": [],
        "columns": [],
        "used_columns": [],
        "reports": [],
        "dashboards": []
    }
    datasourceInstances = workspace_scan_result.get("datasourceInstances", [])
    datasets = workspace_scan_result.get("datasets", [])
    reports = workspace_scan_result.get("reports", [])
    reports_datasets = list({report.get("datasetId") for report in reports})
    dashboards = workspace_scan_result.get("dashboards", [])
    for datasource in datasourceInstances:
        if datasource:
            connection_details = datasource.get("connectionDetails", "")
            datasourceType = datasource.get("datasourceType", "")
            server_name = ""
            if datasourceType == ["AnalysisServices", "Oracle", "SAPHana", "Sql"]:
                server_name = f"{connection_details.get('server', '')}"
            elif datasourceType == "AzureBlobs":
                server_name = f"{connection_details.get('domain', '')}"
            elif datasourceType in ["File"]:
                server_name = f"{connection_details.get('path', '')}"
            elif datasourceType in ["Extension"]:
                server_name = f"{connection_details.get('extensionDataSourcePath', '')}"
            elif datasourceType == "Exchange":
                server_name = f"{connection_details.get('emailAddress', '')}"
            elif datasourceType == ["OData", "SharePointList"]:
                server_name = f"{connection_details.get('url', '')}"
            elif datasourceType == "Salesforce":
                server_name = f"{connection_details.get('loginServer', '')}"
            elif datasourceType == "SalesforceMarketing":
                server_name = f"{connection_details.get('loginServer', '')}"
            else:
                server_name = datasourceType

            connection_details.update({"Datasource Type": datasourceType})
            workspace["data_sources"].append(
                {
                    "datasource_id": datasource.get("datasourceId"),
                    "datasource_name": server_name,
                    "database_id": datasource.get("gatewayId"),
                    "server": server_name,
                    "connectionDetails": connection_details,
                    "source_type": "Datasource",
                    "level": 1,
                }
            )
    for dataset in datasets:
        if dataset.get("id") in reports_datasets:
            dataset_id = dataset.get("id")
            dataset_name = dataset.get("name")
            tables = dataset.get("tables")
            if dataset.get("id") not in (x.get("id") for x in workspace["datasets"]):
                workspace["datasets"].append(dataset)
            workspace = __extracts_tables(tables, dataset, workspace)
    for table in workspace["tables"]:
        table.update({
            "fields": [item for item in workspace["columns"] if item["table_id"] == table.get("table_id")]
        })
    return workspace


def __extracts_tables(dataset_tables, lineage_dataset, workspace_info):
    if dataset_tables:
        res = []
        for table in dataset_tables:
            name = get_attribute_label(table.get("name", ""))
            id = f"{lineage_dataset.get('id')}_{name}"
            columns = table.get("columns", [])
            table_source = table.get("source")[0]if table.get("source") else None
            database_name = ""
            database_type = ""
            schema_name = ""
            if table_source:
                table_source = table_source.get("expression")
                if "Source" in table_source:
                    if "\n" in table_source:
                        database_name, schema_name, database_type = extract_database_schema(
                            table_source)
            obj = {
                "table_id": table.get("name", ""),
                "table_name": table.get("name", ""),
                "isHidden": table.get("isHidden", ""),
                "source": table.get("source", []),
                "measures": table.get("measures", []),
                "isCollapse": False,
                "table_type": "table",
                "database": database_name,
                "src_id": lineage_dataset.get("id"),
                "databaseType": (
                    POWERBI_CONNECTION_TYPE_MAPPING.get(database_type, "")
                    if database_type
                    else ""
                ),
                "schema": schema_name,
                "level": 2,
            }
            # check if table is already exist
            if obj.get("table_id") not in (x.get("table_id") for x in workspace_info["tables"]):
                workspace_info["tables"].append(obj)
            for column in columns:
                column_name = column.get("name")
                column_datatype = column.get("dataType")
                unique_column_id = f"{schema_name}:{obj.get('table_name')}:{column_datatype}:{column_name}"
                existing_column_ids = [col.get("id") for col in workspace_info["columns"]]
                if unique_column_id not in existing_column_ids:
                    workspace_info["columns"].append({
                        "name": column_name,
                        "id": unique_column_id,
                        "data_type": column_datatype,
                        "database": database_name,
                        "schema": schema_name,
                        "connection_type": database_type,
                        **obj,
                        "level": 4
                    })
            res.append(obj)

    return workspace_info


def __extract_workspace_dashboard_views(config, workspace, view, wrokspace_scan_result: dict, relations, dashboard_columns=None) -> dict:
    # Process  dashboards
    all_reports = workspace.get("reports")
    if view:
        # view_detail = view.find("repository-location")
        item_name = view.get('displayName', '')
        item_id = view.get("id", "")
        report_ids = [x.get("reportId") for x in view.get(
            "tiles", []) if x.get("reportId")] if view.get("tiles") else []

        columns = dashboard_columns if dashboard_columns else []
        common_object = {
            "id": item_id,
            "name": item_name,
            "source_id": item_id,
            "type": "Dashboard",
            "upstreamDatasources": [],
            "upstreamDatabases": [],
            "upstreamTables": [],
            "upstreamFields": [],
            "upstreamColumns": [],
            "fields": columns,
            "level": 3
        }

        workspace["dashboards"].append(common_object)
        new_relations = []
        if report_ids:
            for report_id in report_ids:
                report_obj = [
                    x for x in all_reports if x.get("id") == report_id]
                report_obj = report_obj[0] 
                if report_obj:
                    report_fields = report_obj.get("fields", [])
                    for report_field in report_fields:
                        report_col_name = report_field.get("name")

                        # Try to find matching column in dashboard_columns
                        matching_dashboard_col = next(
                            (col for col in columns if col.get("name") == report_col_name), None)

                        if matching_dashboard_col:
                            new_relations.append({
                                "srcTableId": report_obj.get("id"),
                                "tgtTableId": item_id,
                                "srcTableColName": report_col_name,
                                "tgtTableColName": matching_dashboard_col.get("name"),
                                "report": report_obj
                            })
        else:
            new_relations.append({
                "srcTableId": item_id,
            })

        # Update Used Columns
        used_columns = workspace.get("used_columns", [])
        workspace.update({"used_columns": used_columns + columns})
    return workspace, relations + new_relations


def __extract_workspace_report_views(config, workspace, view, wrokspace_scan_result: dict, report_columns, relations: list = []) -> dict:
    try:

        # Extract report views
        if view:
            # view_detail = view.find("repository-location")
            item_name = view.get('name', '')
            item_id = view.get("id", "")
            source_id = view.get("datasetId", "")

            columns = __extract_workspace_report_columns(
                view, workspace, report_columns)
            columns = columns if columns else []
            common_object = {
                "id": item_id,
                "name": item_name,
                "source_id": source_id,
                "type": "Report",
                "upstreamDatasources": [],
                "upstreamDatabases": [],
                "upstreamTables": [],
                "upstreamFields": [],
                "upstreamColumns": [],
                "fields": columns,
                "level": 3
            }

            workspace["reports"].append(common_object)
            for col in columns:
                relations.append({
                    "srcTableId": col.get("entity_id"),
                    "tgtTableId": item_id,
                    "srcTableColName": col.get("name"),
                    "tgtTableColName": col.get("name"),
                    "xyz": view.get("source_id")
                })
            if not columns:
                relations.append({
                    "srcTableId": item_id,
                })
            # Update Used Columns
            used_columns = workspace.get("used_columns", [])
            workspace.update({"used_columns": used_columns + columns})
        return workspace, relations
    except Exception as e:
        log_error(f"Power BI Connector - Extract Workspace Report_Views Info ", e)
        raise e


def __extract_workspace_report_columns(element, workbook, report_columns):
    """
    Helper function to process columns in a Report.
    """
    try:
        columns = []
        dataset_id = element.get("datasetId", "")
        for column in report_columns:
            # column_detail = next((item for item in workbook.get(
            #     "columns") if item["id"] == column.get("name")), {})
            columns.append({
                "id": column.get("name"),
                "name": column.get("name"),
                "role_name": column.get("role"),
                "semantic_role": column.get("semantic-role"),
                "type": column.get("data_type"),
                "entity_id": column.get("table_name"),
                "database": column.get("database"),
                "schema": column.get("schema")
            })
        return columns
    except Exception as e:
        log_error(f"Power Bi Connector - Extract Workspace Report_columns Info ", e)
        raise e


def __get_response(config: dict, url: str = "", method_type: str = "get", params=None):
    api_response = None
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                request_url=url, request_type=method_type, request_params=params
            ),
        )
        api_response = api_response if api_response else {}
        return api_response
    except Exception as e:
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
                select * from core.reports_views
                where asset_id='{asset_id}' and source_id = '{source_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            return report
    except Exception as e:
        log_error(str(e), e)
        raise e


def __get_dashboard_and_reports_by_workspace_id(config: dict, properties: dict, 
                                              workspace_scan_result: dict, workspace_info):
    def get_db_connection():
        if not hasattr(thread_local, "connection"):
            thread_local.connection = get_postgres_connection(config)
        return thread_local.connection

    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        dag_info = config.get("dag_info")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        workspace_id = properties.get("id")
        report_table_id = __get_report_table_id(config)

        scan_reports = workspace_scan_result.get("reports")
        scan_dashboards = workspace_scan_result.get("dashboards")
        if workspace_id:
            # if not workspace_scan_result:
            dashboard_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards"
            response = __get_response(config, dashboard_url)
            response = response if response else {}
            dashboards = response.get("value", [])

            report_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
            response = __get_response(config, report_url)
            response = response if response else {}
            reports = response.get("value", [])
            relations=[]
            def process_report(report, scan_reports=None):
                try:
                    connection = get_db_connection()
                    with connection.cursor() as cursor:
                        report_id = report.get("id")
                        scan_report = next((rep for rep in scan_reports if rep.get("id") == report_id), {})
                        query_string = f""" 
                            select id from core.reports_views 
                            where asset_id = '{asset_id}' and source_id = '{report_id}' 
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        existing_view = fetchone(cursor)
                        existing_view = existing_view.get('id') if existing_view else None

                        properties_obj = {
                            "webUrl": report.get("webUrl", ""),
                            "reportType": report.get("reportType", ""),
                            "embedUrl": report.get("embedUrl", ""),
                            "datasetId": report.get("datasetId", ""),
                            "datasetWorkspaceId": report.get("datasetWorkspaceId", ""),
                            "createdBy": scan_report.get("createdBy"),
                        }
                        url = f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"
                        type = 'Report'
                        report_pages = get_report_pages(config, report_id, workspace_id)
                        page_count = len(report_pages.get('value', []))
                        properties_obj["tot_pages"] = page_count if page_count else ''
                        # Export Report as Images
                        export_report_as_image = ""
                        image = {}
                        exportId = __start_report_export_to_image(config, report_id, report_pages, workspace_id)
                        if exportId:
                            exportStatus = __check_report_export_status(config, report_id, exportId, workspace_id)
                            if exportStatus:
                                export_report_as_image = __download_report_export_to_image(
                                    config, report, dag_info, exportId, workspace_id
                                )
                                #scan_reports = [
                                #    {**x, "image_url": export_report_as_image, "page_count": page_count}
                                #    if x.get("id") == report_id else x
                                #    for x in scan_reports
                                #]
                                for i, x in enumerate(scan_reports):
                                    if x.get("id") == report_id:
                                        scan_reports[i] = {**x, "image_url": export_report_as_image, "page_count": page_count}

                                if export_report_as_image:
                                    image = {"img": export_report_as_image, "p_img": export_report_as_image}

                        if existing_view:
                            query_string = f"""
                                update core.reports_views set 
                                name = '{report.get('name', '').replace("'", "''")}',
                                url = '{url}',
                                created_at = '{scan_report.get("createdDateTime")}',
                                updated_at = '{scan_report.get("modifiedDateTime")}',
                                properties = '{json.dumps(properties_obj, default=str).replace("'", "''")}',
                                image = '{json.dumps(image, default=str).replace("'", "''")}'
                                where id = '{existing_view}'
                            """
                            execute_query(connection, cursor, query_string)
                        else:
                            query_input = (
                                str(uuid4()),
                                report.get("id"),
                                report.get("name"),
                                json.dumps(properties_obj, default=str).replace("'", "''"),
                                url,
                                type,
                                None,
                                json.dumps(image, default=str).replace("'", "''"),
                                None,
                                scan_report.get("createdDateTime"),
                                scan_report.get("modifiedDateTime"),
                                str(report_table_id),
                                str(asset_id),
                                str(connection_id),
                                True,
                                False
                            )
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                            insert_report_view(config, query_param)

                        try:
                            columns = __download_report_with_retry(config, report, workspace_scan_result)
                        except Exception as download_error:
                            log_error(f"Failed to download report {report.get('id')}: {str(download_error)}", download_error)
                            # Skip reports that can't be downloaded but continue processing others
                            columns = []
                        
                        return __extract_workspace_report_views(
                            config, workspace_info.copy(), report, workspace_scan_result, columns, [])
                
                except Exception as e:
                    log_error(f"Failed to process report {report_id}", e)
                    return workspace_info.copy(), []

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                report_futures = [executor.submit(process_report, report, scan_reports) for report in reports]
                
                for future in concurrent.futures.as_completed(report_futures):
                    try:
                        ws_info, rels = future.result()
                        workspace_info.update(ws_info)
                        relations.extend(rels)
                    except Exception as e:
                        log_error("Report processing failed, continuing with others", e)
                        continue
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                for dashboard in dashboards:
                    dashboard_id = dashboard.get("id")
                    scaned_dashboard = next(
                        x for x in scan_dashboards if x.get("id") == dashboard_id)
                    dashboard.update({"tiles": scaned_dashboard.get("tiles")})

                    # Validate Existing Reports_Views  Dashboards Information
                    query_string = f""" 
                        select id from core.reports_views 
                        where asset_id = '{asset_id}' and source_id = '{dashboard_id}' 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    existing_view = fetchone(cursor)
                    existing_view = existing_view.get(
                        'id') if existing_view else None

                    properties_obj = {
                        "webUrl": dashboard.get("webUrl", ""),
                        "embedUrl": dashboard.get("embedUrl", ""),
                    }
                    url = f"https://app.powerbi.com/groups/{workspace_id}/dashboards/{dashboard_id}"

                    type = 'Dashboard'
                    report_ids = [x.get("reportId") for x in dashboard.get(
                        "tiles") if "reportId" in x] if dashboard.get("tiles") else []
                    
                    dashboard_reports = [x for x in scan_reports if x.get("id") in report_ids]
                    updated_at = max((x["modifiedDateTime"] for x in dashboard_reports if "modifiedDateTime" in x), default=None)
                    updated_at_value="NULL" if updated_at is None else f"'{updated_at}'"
                    image_url, page_count = process_dashboard_image(config, dashboard, dashboard_reports)
                    properties_obj["tot_pages"] = page_count if page_count else ''
                    image = {}
                    if image_url:
                        image = (
                            {"img": image_url,
                             "p_img": image_url}
                        )
                    if existing_view:
                        query_string = f"""
                            update core.reports_views set 
                                name = '{dashboard.get('displayName', '')}',
                                url = '{url}',
                                properties = '{json.dumps(properties_obj, default=str).replace("'", "''")}',
                                image = '{json.dumps(image, default=str).replace("'", "''")}',
                                updated_at= {updated_at_value}
                            where id = '{existing_view}'
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    else:
                        query_input = (
                            str(uuid4()),
                            dashboard.get("id"),
                            dashboard.get("displayName"),
                            json.dumps(properties_obj, default=str).replace(
                                "'", "''"),
                            url,
                            type,
                            None,
                            json.dumps(image, default=str).replace("'", "''"),
                            None,
                            None,
                            updated_at,
                            str(report_table_id),
                            str(asset_id),
                            str(connection_id),
                            True,
                            False
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals})", query_input).decode("utf-8")
                        # new_views.append(query_param)
                        insert_report_view(config, query_param)
                    all_reports = workspace_info.get("reports", [])
                    dashboard_colums = []
                    for r_id in report_ids:
                        rep_obj = next(
                            x for x in all_reports if r_id == x.get("id"))
                        dashboard_colums.extend(rep_obj.get(
                            "fields") if rep_obj.get("fields") else [])
                    __save_dashboard_columns(
                        config, dashboard_id, dashboard_colums)
                    workspace_info, relations = __extract_workspace_dashboard_views(
                        config, workspace_info, dashboard, workspace_scan_result, relations, dashboard_colums)

                __update_workspace_statistics(config, workspace_info)
                __prepare_lineage(config, workspace_info, relations)

    except Exception as e:
        log_error("powerBi Reports_views save Failed ", e)
        raise e
    finally:
        if hasattr(thread_local, "connection"):
            try:
                thread_local.connection.close()
            except Exception:
                pass
            
            
def __get_dashboard_and_reports_by_workspace_id_without_scanId(config: dict, properties: dict, 
                                              workspace_scan_result: dict, workspace_info):
    def get_db_connection():
        if not hasattr(thread_local, "connection"):
            thread_local.connection = get_postgres_connection(config)
        return thread_local.connection

    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        dag_info = config.get("dag_info")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        workspace_id = properties.get("id")
        report_table_id = __get_report_table_id(config)

        scan_reports = workspace_scan_result.get("reports")
        scan_dashboards = workspace_scan_result.get("dashboards")
        if workspace_id:
            relations=[]
            def process_report(report, workspace_scan_result, scan_reports):
                try:
                    connection = get_db_connection()
                    with connection.cursor() as cursor:
                        report_id = report.get("id")
                        query_string = f""" 
                            select id from core.reports_views 
                            where asset_id = '{asset_id}' and source_id = '{report_id}' 
                        """
                        cursor = execute_query(connection, cursor, query_string)
                        existing_view = fetchone(cursor)
                        existing_view = existing_view.get('id') if existing_view else None

                        properties_obj = {
                            "webUrl": report.get("webUrl", ""),
                            "reportType": report.get("reportType", ""),
                            "embedUrl": report.get("embedUrl", ""),
                            "datasetId": report.get("datasetId", ""),
                            "datasetWorkspaceId": report.get("datasetWorkspaceId", ""),
                            "createdBy": report.get("createdBy"),
                        }
                        url = f"https://app.powerbi.com/groups/{workspace_id}/reports/{report_id}"
                        type = 'Report'
                        report_pages = get_report_pages(config, report_id, workspace_id)
                        page_count = len(report_pages.get('value', []))
                        properties_obj["tot_pages"] = page_count if page_count else ''
                        # Export Report as Images
                        export_report_as_image = ""
                        image = {}
                        exportId = __start_report_export_to_image(config, report_id, report_pages, workspace_id)
                        if exportId:
                            exportStatus = __check_report_export_status(config, report_id, exportId, workspace_id)
                            if exportStatus:
                                export_report_as_image = __download_report_export_to_image(
                                    config, report, dag_info, exportId, workspace_id
                                )
                                #scan_reports = [
                                #    {**x, "image_url": export_report_as_image, "page_count": page_count}
                                #    if x.get("id") == report_id else x
                                #    for x in scan_reports
                                #]
                                for i, x in enumerate(scan_reports):
                                    if x.get("id") == report_id:
                                        scan_reports[i] = {**x, "image_url": export_report_as_image, "page_count": page_count}

                                if export_report_as_image:
                                    image = {"img": export_report_as_image, "p_img": export_report_as_image}

                        if existing_view:
                            created_at = report.get("createdDateTime")
                            updated_at = report.get("modifiedDateTime")
                            query_string = f"""
                                update core.reports_views set 
                                name = '{report.get('name', '').replace("'", "''")}',
                                url = '{url}',
                                created_at = {f"'{created_at}'" if created_at else "NULL"},
                                updated_at = {f"'{updated_at}'" if updated_at else "NULL"},
                                properties = '{json.dumps(properties_obj, default=str).replace("'", "''")}',
                                image = '{json.dumps(image, default=str).replace("'", "''")}'
                                where id = '{existing_view}'
                            """
                            execute_query(connection, cursor, query_string)
                        else:
                            query_input = (
                                str(uuid4()),
                                report.get("id"),
                                report.get("name"),
                                json.dumps(properties_obj, default=str).replace("'", "''"),
                                url,
                                type,
                                None,
                                json.dumps(image, default=str).replace("'", "''"),
                                None,
                                report.get("createdDateTime"),
                                report.get("modifiedDateTime"),
                                str(report_table_id),
                                str(asset_id),
                                str(connection_id),
                                True,
                                False
                            )
                            input_literals = ", ".join(["%s"] * len(query_input))
                            query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                            insert_report_view(config, query_param)

                        try:
                            if report.get("is_downloaded") == True:
                                __save_columns(config, report, workspace_scan_result = workspace_scan_result, without_scanId=True)
                                columns, _= __process_attribute_data(report, workspace_scan_result=workspace_scan_result, without_scanId=True)
                            else:
                                columns = []
                        except:
                            columns = []
                        
                        return __extract_workspace_report_views(
                            config, workspace_info.copy(), report, workspace_scan_result, columns, [])
                
                except Exception as e:
                    log_error(f"Failed to process report {report_id}", e)
                    return workspace_info.copy(), []

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                report_futures = [executor.submit(process_report, report, workspace_scan_result, scan_reports) for report in scan_reports]
                for future in concurrent.futures.as_completed(report_futures):
                    try:
                        ws_info, rels = future.result()
                        workspace_info.update(ws_info)
                        relations.extend(rels)
                    except Exception as e:
                        log_error("Report processing failed, continuing with others", e)
                        continue
            connection = get_postgres_connection(config)
            with connection.cursor() as cursor:
                for dashboard in  scan_dashboards:
                    dashboard_id = dashboard.get("id")

                    # Validate Existing Reports_Views  Dashboards Information
                    query_string = f""" 
                        select id from core.reports_views 
                        where asset_id = '{asset_id}' and source_id = '{dashboard_id}' 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    existing_view = fetchone(cursor)
                    existing_view = existing_view.get(
                        'id') if existing_view else None

                    properties_obj = {
                        "webUrl": dashboard.get("webUrl", ""),
                        "embedUrl": dashboard.get("embedUrl", ""),
                    }
                    url = f"https://app.powerbi.com/groups/{workspace_id}/dashboards/{dashboard_id}"

                    type = 'Dashboard'
                    report_ids = [x.get("reportId") for x in dashboard.get(
                        "tiles",[]) if x.get("reportId") ] if dashboard.get("tiles") else []
                    
                    dashboard_reports = [x for x in scan_reports if x.get("id") in report_ids]
                    updated_at = max((x["modifiedDateTime"] for x in dashboard_reports if "modifiedDateTime" in x), default=None)
                    updated_at_value="NULL" if updated_at is None else f"'{updated_at}'"
                    image_url, page_count = process_dashboard_image(config, dashboard, dashboard_reports)
                    properties_obj["tot_pages"] = page_count if page_count else ''
                    image = {}
                    if image_url:
                        image = (
                            {"img": image_url,
                             "p_img": image_url}
                        )
                    if existing_view:
                        query_string = f"""
                            update core.reports_views set 
                                name = '{dashboard.get('displayName', '')}',
                                url = '{url}',
                                properties = '{json.dumps(properties_obj, default=str).replace("'", "''")}',
                                image = '{json.dumps(image, default=str).replace("'", "''")}',
                                updated_at= {updated_at_value}
                            where id = '{existing_view}'
                        """
                        cursor = execute_query(
                            connection, cursor, query_string)
                    else:
                        query_input = (
                            str(uuid4()),
                            dashboard.get("id"),
                            dashboard.get("displayName"),
                            json.dumps(properties_obj, default=str).replace(
                                "'", "''"),
                            url,
                            type,
                            None,
                            json.dumps(image, default=str).replace("'", "''"),
                            None,
                            None,
                            updated_at,
                            str(report_table_id),
                            str(asset_id),
                            str(connection_id),
                            True,
                            False
                        )
                        input_literals = ", ".join(["%s"] * len(query_input))
                        query_param = cursor.mogrify(
                            f"({input_literals})", query_input).decode("utf-8")
                        # new_views.append(query_param)
                        insert_report_view(config, query_param)
                    all_reports = workspace_info.get("reports", [])
                    dashboard_colums = []
                    for r_id in report_ids:
                        rep_obj = next(
                            x for x in all_reports if r_id == x.get("id"))
                        dashboard_colums.extend(rep_obj.get(
                            "fields") if rep_obj.get("fields") else [])
                    __save_dashboard_columns(
                        config, dashboard_id, dashboard_colums)
                    workspace_info, relations = __extract_workspace_dashboard_views(
                        config, workspace_info, dashboard, workspace_scan_result, relations, dashboard_colums)

                __update_workspace_statistics(config, workspace_info)
                __prepare_lineage(config, workspace_info, relations)
    except Exception as e:
        log_error("powerBi Reports_views save Failed ", e)
        raise e
    finally:
        if hasattr(thread_local, "connection"):
            try:
                thread_local.connection.close()
            except Exception:
                pass

def process_dashboard_image(config, dashboard, dashboard_reports):
    dag_info = config.get("dag_info")
    image_links = []
    image_url=""
    totoal_pages = 0
    for dashboard_report in dashboard_reports:
        image_url = dashboard_report.get("image_url") if dashboard_report.get("image_url") else ""
        if image_url:
            image_links.append(image_url)
        totoal_pages += dashboard_report.get("page_count", 0)

    # Combine all images into one image using PIL
    try:
        images = [Image.open(BytesIO(requests.get(link).content)) for link in image_links]
        if not images:
            return image_url, totoal_pages
    except Exception as e:
        log_error("Error in Processing Dashboard Image", e)
        return None, totoal_pages
    widths, heights = zip(*(i.size for i in images))

    total_width = max(widths)
    total_height = sum(heights)

    combined_image = Image.new('RGB', (total_width, total_height))

    y_offset = 0
    for im in images:
        combined_image.paste(im, (0, y_offset))
        y_offset += im.height

    storage_service = get_storage_service(dag_info)
    dashboard_name = __prepare_file_name_for_s3(
            dashboard.get("displayName"), dashboard.get("id"),)
    file_path = f"dashboards/{dashboard_name}.png"
    image_bytes = BytesIO()
    combined_image.save(image_bytes, format='PNG')
    image = __convert_base64(image_bytes.getvalue()) if combined_image else None
    if image:
        image_url = storage_service.upload_file(
            image, file_path, dag_info)
    return image_url, totoal_pages


def insert_report_view(config, views_input):
    # create each views
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        # views_input = split_queries(new_views)
        try:
            # query_input = ",".join(input_values)
            insert_query = f"""
                insert into core.reports_views (id, source_id, name, properties, url, type, tags, image,
                folder_path, created_at, updated_at, report_id, asset_id, connection_id, is_active, is_delete)
                values {views_input}
            """
            cursor = execute_query(
                connection, cursor, insert_query)
        except Exception as e:
            log_error(
                "extract properties: inserting views level", e)


def __save_columns(config: dict, report: dict, extracted_report=None, workspace_scan_result: dict = None, without_scanId=False):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        report_id = report.get("id")
        report_table_id = __get_report_table_id(config)

        # Process Columns Data for insert in to DB
        columns, tables = __process_attribute_data(
            report, extracted_report, workspace_scan_result = workspace_scan_result, without_scanId=without_scanId)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            report_view = __get_report_view_table(config, report_id)
            if report_view:
                report_view_id = report_view.get("id")
                query_string = f"""
                    delete from core.reports_columns
                    where asset_id = '{asset_id}' and report_view_id = '{report_view_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                insert_objects = []
                if extracted_report or without_scanId:
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
                            True,
                            False,
                            report_view_id,
                            report_table_id,
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
                                    connection_type, is_active, is_delete, 
                                    report_view_id, report_id, asset_id, connection_id
                                ) values {query_input} 
                            """
                            cursor = execute_query(
                                connection, cursor, query_string)
                        except Exception as e:
                            log_error(
                                'PowerBi Report Views Columns Insert Failed  ', e)

                # Update View Statistics
                __update_view_statistics(config, report_view, columns, tables)
    except Exception as e:
        log_error(f"PowerBi Connector - Save Views Columns By View ID Failed ", e)
        raise e


def __save_dashboard_columns(config: dict, dashboard_id, dashboard_columns: dict):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get('id')
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        dashboard_id = dashboard_id
        dashboard_table_id = __get_report_table_id(config)

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            dashboard_view = __get_report_view_table(config, dashboard_id)
            if dashboard_view:
                dashboard_view_id = dashboard_view.get("id")
                query_string = f"""
                    delete from core.reports_columns
                    where asset_id = '{asset_id}' and report_view_id = '{dashboard_view_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                insert_objects = []
                if dashboard_columns:
                    for column in dashboard_columns:
                        query_input = (
                            uuid4(),
                            column.get('name'),
                            column.get('entity_id'),
                            column.get('database', ''),
                            column.get('schema', ''),
                            column.get('type', ''),
                            column.get('is_null') if column.get(
                                'is_null') else True,
                            column.get('is_semantics') if column.get(
                                'is_semantics') else False,
                            column.get('connection_type', ''),
                            True,
                            False,
                            dashboard_view_id,
                            dashboard_table_id,
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
                                    connection_type, is_active, is_delete, 
                                    report_view_id, report_id, asset_id, connection_id
                                ) values {query_input} 
                            """
                            cursor = execute_query(
                                connection, cursor, query_string)
                        except Exception as e:
                            log_error(
                                'PowerBi Report Views Columns Insert Failed  ', e)

                # Update View Statistics
                __update_view_statistics(
                    config, dashboard_view, dashboard_columns)
    except Exception as e:
        log_error(f"PowerBi Connector - Save Views Columns By View ID Failed ", e)
        raise e


def __process_attribute_data(report, extracted_reports=None, workspace_scan_result= None, without_scanId=False):
    query_names = []
    if extracted_reports:
        for ext_rep in extracted_reports:
            query_names.extend([select.get("queryName")
                            for select in ext_rep.get("selects", [])])

    # Find the matching report and extract the dataset ID
    reports_data = workspace_scan_result.get("reports", [])
    dataset_id = next(
        (r.get("datasetId")
         for r in reports_data if r.get("id") == report.get("id")),
        None
    )

    if not dataset_id:
        return [], set()

    # Find the matching dataset and extract table data
    datasets_data = workspace_scan_result.get("datasets", [])
    tables_data = next(
        (d.get("tables", [])
         for d in datasets_data if d.get("id") == dataset_id),
        []
    )

    # Extract attributes from the table columns
    attributes = []
    tables = set()
    for table in tables_data:
        table_name = table.get("name", "")
        columns_data = table.get("columns", [])
        table_source = table.get("source")[0]if table.get("source") else None
        database_name = ""
        database_type = ""
        schema_name = ""
        if table_source:
            table_source = table_source.get("expression")
            database_name, schema_name, database_type = extract_database_schema(
                table_source)
        for column in columns_data:
            column_name = column.get("name")
            column_datatype = column.get("dataType")
            if any(f"{table_name}.{column_name}" in str(query) for query in query_names) or  without_scanId:
                attributes.append({
                    "table_name": table_name,
                    "name": column_name,
                    "data_type": column_datatype,
                    "database": database_name,
                    "schema": schema_name,
                    "connection_type": database_type
                })
                tables.add(table_name)
    return attributes, tables


def extract_database_schema(table_source):
    database_name = schema_name = database_type = ''
    if "Source" in table_source:
        if "\n" in table_source:
            if "Snowflake.Databases" in table_source:
                # Use regex to find the database name
                match = re.search(
                    r'Snowflake\.Databases\("([^"]+)",\s*"([^"]+)"(?:,\s*\[[^\]]+\])?\)', table_source)
                if match:
                    database_name = match.group(2)

                # Use regex to find schema name
                schema_match = re.search(
                    r'([A-Za-z0-9_]+)_Schema\s*=\s*[^;]*\{[^}]*Name="([^"]+)",Kind="Schema"', table_source)
                if schema_match:
                    schema_name = schema_match.group(2)
                database_type = "snowflake"
            if "Sql.Database" in table_source:
                # Use regex to find the database name
                match = re.search(
                    r'Sql\.Database\("([^"]*)",\s*"([^"]*)"\)', table_source)
                if match:
                    database_name = match.group(2)

                # Use regex to find the schema name
                schema_match = re.search(r'Schema="([^"]*)"', table_source)
                if schema_match:
                    schema_name = schema_match.group(1)
                database_type = "sql"
    return database_name, schema_name, database_type


def __update_workspace_statistics(config: dict, workspace: dict):
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
            properties = report.get('properties', {})
            tablescount = 0
            columnscount = 0
            for dataset in workspace.get('datasets', []):
                tot_tables = len(dataset.get('tables', [])
                                 ) if dataset.get('tables') else 0
                tablescount += tot_tables
                for table in dataset.get('tables', []):
                    tot_columns = len(table.get('columns', [])
                                      ) if table.get('columns') else 0
                    columnscount += tot_columns
            properties.update({
                "tot_tables": len(workspace.get('tables', [])) if workspace.get('used_columns') else 0,
                "tot_columns": len(workspace.get('used_columns', [])) if workspace.get('used_columns') else 0,
                "total_dataset": len(workspace.get('datasets', [])) if workspace.get('datasets') else 0,
                "tot_reports": len(workspace.get('reports', [])) if workspace.get('reports') else 0,
                "tot_dashboards": len(workspace.get('dashboards', [])) if workspace.get('dashboards') else 0
            })
            query_string = f"""
                    update core.reports set 
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    where asset_id = '{asset_id}'
                """
            cursor = execute_query(
                connection, cursor, query_string)

    except Exception as e:
        log_error(f"Power BI Connector - Update Worspace Statistics ", e)
        raise e


def __update_view_statistics(config: dict, report_view: dict, columns: dict, tables: set = {}):
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            report_view_id = report_view.get("id")
            properties = report_view.get('properties', {})
            properties.update({
                "tot_columns": len(columns) if columns else 0,
                "tot_tables": len(tables) if tables else 0
            })
            query_string = f"""
                    update core.reports_views set 
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    where id = '{report_view_id}'
                """
            cursor = execute_query(
                connection, cursor, query_string)

    except Exception as e:
        log_error(f"Power BI Connector - Update View Statistics ", e)
        raise e

def __download_report_with_retry(config: dict, report: dict, workspace_scan_result: dict, max_retries: int = 3):
    """
    Download PowerBI Report with retry logic
    """
    for attempt in range(max_retries):
        try:
            return __download_report_from_server(config, report, workspace_scan_result)
        except Exception as e:
            if "ExportPBIX_ModelessWorkbookNotFound" in str(e):
                # Don't retry for this specific error as it's a permanent issue
                log_error(f"Report not found, skipping retry: {str(e)}", e)
                return []
            elif attempt < max_retries - 1:
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                log_error(f"Download attempt {attempt + 1} failed, retrying in {wait_time:.2f} seconds: {str(e)}", e)
                time.sleep(wait_time)
            else:
                log_error(f"All download attempts failed for report {report.get('id')}: {str(e)}", e)
                return []
    return []
def __download_report_from_server(config: dict, report: dict, workspace_scan_result: dict, return_layout=False):
    """
    Download PowerBi Report 
    """
    try:
        # Download Report from Server
        extracted_report = None
        columns = []
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        report_id = report.get("id")
        workspace_id = workspace_scan_result.get("id")
        
        # Check if report exists and is accessible before attempting download
        try:
            report_info_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}"
            report_info = __get_response(config, report_info_url)
            if not report_info or not report_info.get("id"):
                log_error(f"Report {report_id} not found or not accessible in workspace {workspace_id}", None)
                return []
        except Exception as check_error:
            log_error(f"Failed to check report {report_id} accessibility: {str(check_error)}", check_error)
            return []
        
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Export?downloadType=IncludeModel"
        response = __get_response(
            config, url, params=dict(is_file_download=True))
        response = bytes(response) if response else None
        
        if not response:
            raise Exception(
                f"Failed to download reports: , {response}")

        filename = f"{asset_id}.pbix"
        base_dir = str(os.path.abspath(os.path.join(
            os.path.dirname(__file__), "..", '..', '..', '..')))
        base_dir = os.path.join(
            base_dir, "infra/airflow/tests", f"powerbi/{asset_id}/{report_id}")
        source_file_path = ""

        # create directory if doesn't exists
        if not os.path.exists(base_dir):
            os.makedirs(base_dir, exist_ok=True)
        source_file_path = os.path.join(base_dir, filename)
        try:
            with open(source_file_path, "wb") as file:
                file.write(response)
        except Exception as exp:
            log_error("Exception occure", str(exp))

        if base_dir and source_file_path:
            extracted_report = __extract_downloaded_report_file(
                base_dir, source_file_path, report)

        #     # Save Columns
            if not return_layout:
                __save_columns(config, report, extracted_report,
                           workspace_scan_result)
        if not return_layout:
            columns, _ = __process_attribute_data(
            report, extracted_report, workspace_scan_result)
    except Exception as e:
        log_error("PowerBI Download Report On Server Failed, Now Trying On S3 ", e)
        columns, extracted_report = handle_processing_workbook_on_s3(config, report, workspace_scan_result, return_layout=return_layout)

    finally:
        if return_layout:
            return extracted_report 
        return columns if columns else []


def __extract_downloaded_report_file(folder_path, file_path, report):
    """
    Helper function to extract datasource , database , sheets, dashboard information from downloaded workbook.
    """
    try:
        extracted_folder = "extracted_pbix_content"
        report_folder = "Report"
        layout_file_name = "Layout"
        report = []

        # change directory
        os.chdir(folder_path)
        # Extract the .pbix content
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extracted_folder)

        # Get the path of the .twb file
        layout_file_path = os.path.join(
            extracted_folder, report_folder, layout_file_name)
        file_content = ""
        layout_json = {}
        if os.path.exists(layout_file_path):
            with open(layout_file_path, "rb") as f:
                file_content = f.read()
                layout_json = json.loads(file_content)

        # Extract Report Information
        report = __extract_report_info(layout_json, report)

        # Cleanup extracted files
        shutil.rmtree(extracted_folder)
        os.remove(file_path)

        return report
    except Exception as e:
        log_error("PowerBi Get Report Recursive Path Failed ", e)

        # Cleanup extracted files
        shutil.rmtree(extracted_folder)
        os.remove(file_path)
        raise e


def __extract_report_info(layout_json, report):
    try:
        if not layout_json or 'sections' not in layout_json or not layout_json['sections']:
            log_error("Invalid layout_json structure - no sections found", None)
            return []
        
        layout_json = layout_json['sections'][0]
        if 'visualContainers' not in layout_json:
            log_error("No visualContainers found in layout_json", None)
            return []
            
        layout_json = layout_json['visualContainers']
        
        # Safely extract dataTransforms with error handling
        result = []
        for x in layout_json:
            try:
                if 'dataTransforms' in x:
                    result.append(json.loads(x['dataTransforms']))
                else:
                    log_error(f"dataTransforms not found in visual container: {x.get('id', 'unknown')}", None)
            except (KeyError, json.JSONDecodeError) as e:
                log_error(f"Failed to parse dataTransforms for visual container: {str(e)}", e)
                continue
        
        return result
    except Exception as e:
        log_error(f"Error extracting report info: {str(e)}", e)
        return []


def __start_lineage_scan(config: dict, properties: dict) -> str:
    try:
        workspace_id = properties.get("id")
        if workspace_id:
            url_groups = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True"
            data = {"workspaces": [workspace_id]}

            api_response = __get_response(config, url_groups, "post", data)
            return api_response.get("id")
    except Exception as e:
        raise e


def __check_scan_status(config, scanId: str) -> dict:
    try:
        url_groups = (
            f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scanId}"
        )
        retries = 0
        while retries < 3:
            api_response = __get_response(config, url_groups)
            status = api_response.get("status")
            if status == "Succeeded":
                return True
            else:
                time.sleep(30)
                retries += 1
        return False
    except Exception as e:
        raise e


def __get_scan_result(config: dict, scanId: str) -> dict:
    try:
        url_groups = (
            f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scanId}"
        )
        api_response = __get_response(config, url_groups)
        return api_response
    except Exception as e:
        raise e
    
def format_datasource_expression(kind, path, table_name):
    """
    Returns a Power Query M expression for Snowflake or SQL that can be parsed with regex.
    """
    kind_lower = kind.lower()

    if kind_lower == "sql":
        server, database = path.split(";") if ";" in path else ("", "")
        return (
            f'let\n'
            f'    Source = Sql.Database("{server}", "{database}"),\n'
            f'    {table_name} = Source{{[Item="{table_name}"]}}[Data]\n'
            f'in\n'
            f'    {table_name}'
        )

    elif kind_lower == "snowflake":
        account, database = path.split(";") if ";" in path else ("", "")
        return (
            f'let\n'
            f'    Source = Snowflake.Databases("{account}","{database}"),\n'
            f'    {table_name}_Table = Source{{[Name="{table_name}",Kind="Table"]}}[Data]\n'
            f'in\n'
            f'    {table_name}_Table'
        )

    else:
        return f'{kind}: {path} | table: {table_name}'





def fetch_workspace_scanResult_without_scanId(config, workspace_id):
    result = {"workspaces": [], "datasourceInstances": []}

    # Workspace metadata
    workspace_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
    workspace_details = __get_response(config, workspace_url) or {}

    # Reports
    reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
    reports = (__get_response(config, reports_url) or {}).get("value", [])

    # Dashboards and tiles
    dashboards_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards"
    dashboards = (__get_response(config, dashboards_url) or {}).get("value", [])
    transformed_dashboards = []
    for dashboard in dashboards:
        tiles_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards/{dashboard['id']}/tiles"
        tiles = (__get_response(config, tiles_url) or {}).get("value", [])
        transformed_tiles = [{
            "id": tile.get("id"),
            "title": tile.get("title", ""),
            "subTitle": tile.get("subTitle", ""),
            "reportId": tile.get("reportId", ""),
            "datasetId": tile.get("datasetId", "")
        } for tile in tiles]

        transformed_dashboards.append({
            "id": dashboard["id"],
            "displayName": dashboard.get("displayName", ""),
            "isReadOnly": dashboard.get("isReadOnly", False),
            "tiles": transformed_tiles,
            "tags": []
        })

    # Datasets
    datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    datasets_response = __get_response(config, datasets_url)
    datasets = datasets_response.get('value', []) if datasets_response else []

    dataset_id_map = {}
    transformed_datasets = []
    for dataset in datasets:
        dataset_entry = {
            "id": dataset.get("id"),
            "name": dataset.get("name"),
            "tables": []
        }
        dataset_id_map[dataset["id"]] = dataset_entry
        transformed_datasets.append(dataset_entry)

    # Extract schema from reports
    for report in reports:
        report_id = report.get("id")
        dataset_id = report.get("datasetId")
        if not dataset_id:
            continue

        report["is_downloaded"] = False
        layout_json = __download_report_from_server(config, report, {"id": workspace_id}, return_layout=True)
        if not layout_json:
            continue

        report["is_downloaded"] = True
        selects = layout_json[0].get("selects", []) if isinstance(layout_json, list) else []
        all_tables = {}

        for sel in selects:
            expr = sel.get("expr", {})
            source_ref = None
            col_name = None

            if "Column" in expr:
                source_ref = expr["Column"]["Expression"]["SourceRef"]["Entity"]
                col_name = expr["Column"]["Property"]
            elif "Aggregation" in expr and "Column" in expr["Aggregation"]["Expression"]:
                col_expr = expr["Aggregation"]["Expression"]["Column"]
                source_ref = col_expr["Expression"]["SourceRef"]["Entity"]
                col_name = col_expr["Property"]

            if source_ref and col_name:
                if source_ref not in all_tables:
                    all_tables[source_ref] = []
                col_metadata = {
                    "name": col_name,
                    "dataType": "String",
                    "isHidden": False,
                    "columnType": "Data"
                }
                all_tables[source_ref].append(col_metadata)

        dataset_entry = dataset_id_map[dataset_id] if dataset_id in dataset_id_map else None
        for tbl_name, cols in all_tables.items():
            if dataset_entry is None:
                continue
            dataset_entry["tables"].append({
                "name": tbl_name,
                "isHidden": False,
                "storageMode": "Import",
                "columns": cols
            })

    # Add datasource expressions and deduplicate
    all_datasource_items = []
    for dataset in datasets:
        dataset_id = dataset.get("id")
        if not dataset_id:
            continue

        datasource_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources"
        datasource_response = __get_response(config, datasource_url)
        datasource_items = datasource_response.get("value", []) if datasource_response else []
        all_datasource_items.extend(datasource_items)

        dataset_entry = dataset_id_map.get(dataset_id)
        if not dataset_entry:
            continue

        for table in dataset_entry.get("tables", []):
            table_name = table.get("name")
            sources = []

            for datasource_item in datasource_items:
                connection_details = datasource_item.get("connectionDetails", {})
                kind = connection_details.get("kind", datasource_item.get("datasourceType", "Unknown"))
                if kind.lower() == "sql":
                    server = connection_details.get("server", "")
                    database = connection_details.get("database", "")
                    path = f"{server};{database}"
                else:
                    path = connection_details.get("path", "")

                expression = format_datasource_expression(kind, path, table_name)
                sources.append({"expression": expression})

            table["source"] = sources

    # Deduplicate datasourceInstances by datasourceId
    datasource_id_map = {}
    for datasource_item in all_datasource_items:
        datasource_id = datasource_item.get("datasourceId")
        if datasource_id and datasource_id not in datasource_id_map:
            datasource_id_map[datasource_id] = datasource_item

    unique_datasource_instances = list(datasource_id_map.values())
    result["datasourceInstances"] = unique_datasource_instances

    # Final workspace packaging
    workspace_data = {
        "id": workspace_id,
        "name": workspace_details.get("name"),
        "type": workspace_details.get("type", "Workspace"),
        "state": workspace_details.get("state"),
        "isOnDedicatedCapacity": workspace_details.get("isOnDedicatedCapacity", False),
        "capacityId": workspace_details.get("capacityId"),
        "defaultDatasetStorageFormat": workspace_details.get("defaultDatasetStorageFormat"),
        "isReadOnly": workspace_details.get("isReadOnly", False),
        "reports": reports,
        "dashboards": transformed_dashboards,
        "datasets": transformed_datasets,
        "dataflows": [],
        "datamarts": []
    }

    result["workspaces"].append(workspace_data)
    return result


def __prepare_lineage(config, workspace_info, relations):
    """
    Helper function to prepare Lineage
    """
    try:
        lineage = {
            "tables": [],
            "relations": []
        }

        lineage = __prepare_lineage_tables(lineage, workspace_info)
        lineage = __prepare_lineage_relations(
            lineage, workspace_info, relations)

        # Save Lineage
        save_reports_lineage(config, {**lineage})

        # Save Associated Asset Map
        map_asset_with_lineage(config, lineage, "report")

        # Save Propagation Values
        update_reports_propagations(config, config.get("asset", {}))

    except Exception as e:
        log_error(f"Power BI Connector - Prepare Lineage Table Failed ", e)
        raise e


def __prepare_lineage_tables(lineage: dict, workspace_info) -> dict:
    """
    Helper function to prepare  Lineage Tables
    """
    try:
        workspace_tables = workspace_info.get("tables", [])
        workspace_sheets = workspace_info.get("reports", [])
        workspace_dashboard = workspace_info.get("dashboards", [])

        for table in workspace_tables:
            table.update({
                "id": table.get("table_id"),
                "name": table.get("table_name")
            })

        lineage.update({
            "tables": workspace_tables + workspace_sheets + workspace_dashboard
        })
        return lineage
    except Exception as e:
        log_error(f"Power BI Connector - Prepare Lineage Table Failed ", e)
        raise e


def __prepare_lineage_relations(lineage: dict, workspace_info: dict, relations) -> dict:
    """
    Helper function to prepare Lineage Relations
    """
    try:
        workspace_views = workspace_info.get(
            "reports", []) + workspace_info.get("dashboards", [])
        for view in workspace_views:
            fields = view.get("fields", [])
            for field in fields:
                columns = [item for item in workspace_info.get(
                    "columns") if item["id"] == field.get("id")]
                for col in columns:
                    relation = {
                        "srcTableId": col.get("table_id"),
                        "tgtTableId": view.get("source_id") if view.get("source_id") else view.get("id"),
                        "srcTableColName": col.get("name"),
                        "tgtTableColName": field.get("name")
                    }
                    lineage["relations"].append(relation)
        lineage["relations"].extend(relations)
        return lineage
    except Exception as e:
        log_error(f"Power BI Connector - Prepare Lineage Table Failed ", e)
        raise e


def get_report_pages(config, report_id, workspace_id):
    try:
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/pages"
        api_response = __get_response(config, url, "get")
    except:
        api_response = {}
    finally:
        return api_response


def __start_report_export_to_image(config, report_id, properties: dict, workspace_id) -> str:
    try:
        pages = properties.get("value", [])
        pageName = "ReportSection"
        if pages and isinstance(pages[0], dict) and "name" in pages[0]:
            pageName = pages[0]["name"]
        if report_id and pages:
            url_groups = (
                f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/ExportTo"
            )
            data = {
                "format": "PNG",
                "powerBIReportConfiguration": {"pages": [{"pageName": pageName}]},
            }
            api_response = __get_response(config, url_groups, "post", data)
            return api_response.get("id")
    except Exception as e:
        raise e


def __check_report_export_status(config, report_id, exportId: str, workspace_id) -> dict:
    try:
        url_groups = (
            f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/exports/{exportId}"
        )
        retries = 0
        while retries < 3:
            api_response = __get_response(config, url_groups)
            status = api_response.get("status")
            if status == "Succeeded":
                return True
            else:
                time.sleep(30)
                retries += 1
        return False
    except Exception as e:
        raise e


def __download_report_export_to_image(
    config, properties: dict, dag_info, exportId: str, workspace_id
) -> dict:
    try:
        report_url = ""
        reportId = properties.get("id")
        url_groups = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{reportId}/exports/{exportId}/file"
        api_response = __get_response(
            config, url_groups, params=dict(is_file_download=True)
        )
        image_data = bytes(api_response) if api_response else None
        storage_service = get_storage_service(dag_info)
        report_name = __prepare_file_name_for_s3(
            properties.get("name"), reportId)
        file_path = f"reports/{report_name}.png"
        image = __convert_base64(image_data) if image_data else None
        if image:
            report_url = storage_service.upload_file(
                image, file_path, dag_info)
        return report_url
    except Exception as e:
        raise e


def __prepare_file_name_for_s3(name: str, report_id: str):
    file_name = re.sub("[^A-Za-z0-9]", "_", name.strip())
    file_name = f"{file_name}_{report_id}"
    return file_name


def __convert_base64(file):
    # function to encode file data to base 64 format
    decoded_file = ""
    try:
        decoded_file = base64.b64encode(file)
        decoded_file = base64.b64decode(decoded_file)
    except Exception as e:
        log_error("PowerBI Image Base 64 Convert Failed ", e)
    finally:
        return decoded_file
  
def __download_report_to_s3(config: dict, report: dict, workspace_scan_result: dict) -> dict:
    """
    Download Power BI report, upload to S3, extract, and parse
    """
    try:
        log_info("START HANDLING OF POWERBI REPORT ON S3...")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        report_id = report.get("id")
        workspace_id = workspace_scan_result.get("id")
        dag_info = config.get("dag_info")
        credentials = config.get("connection", {}).get("credentials", {})
        
        # Construct Power BI API URL
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports/{report_id}/Export?downloadType=IncludeModel"

        # Get streaming response info
        response = __get_response(config, url, params=dict(is_file_download=True, is_stream=True))

        # Construct S3 paths
        destination_path = f"powerbi/{asset_id}/{report_id}/"
        target_prefix = f"{destination_path}extracted"
        destination_key = f"{destination_path}{asset_id}.pbix"

        storage_service = get_storage_service(dag_info)
        
        # Upload the file to S3
        with requests.get(url=url, headers=response.get("headers"), verify=False, stream=True) as stream_response:
            stream_response.raise_for_status()
            storage_service.upload_streaming_file(dag_info, stream_response, destination_key)
        
        # Extract PBIX contents on S3
        storage_service.stream_extract_on_s3(dag_info=dag_info, source_key=destination_key, target_prefix=target_prefix)

        # Extract layout and report info from extracted files
        extracted_report = __extract_report_file_from_s3(config, report, target_prefix)

        # Clean up
        storage_service.delete_s3_folder_versioned(dag_info=dag_info, folder_prefix=destination_path)

        log_info("END HANDLING OF POWERBI REPORT ON S3...")
        return extracted_report

    except Exception as e:
        log_error("PowerBI download → upload → extract → read report on S3 bucket failed", e)
        return {}

def __extract_report_file_from_s3(config: dict, report: dict, target_prefix: str) -> dict:
    """
    Extracts Power BI report data (Layout.json) directly from S3 after .pbix is extracted.
    Replaces the local file extraction method with S3-based extraction.
    """
    try:
        dag_info = config.get("dag_info")
        storage_service = get_storage_service(dag_info)
        report = []

        # List files in the target S3 prefix
        files = storage_service.list_files(dag_info=dag_info, prefix=target_prefix)
        
        # Find the Layout file in the extracted content
        layout_file_key = None
        for file_key in files:
            if file_key.endswith("Report/Layout"):
                layout_file_key = file_key
                break

        if not layout_file_key:
            log_error("Layout file not found in extracted PBIX content on S3.")
            return report

        layout_json = storage_service.stream_parse_json_from_s3(dag_info=dag_info, file_key=layout_file_key)

        # Extract report info using the same method as local extraction
        extracted_report = __extract_report_info(layout_json, report)

        return extracted_report

    except Exception as e:
        log_error("Power BI extract report from S3 failed", e)
        raise e

def handle_processing_workbook_on_s3(config: dict, report: dict, workspace_scan_result: dict, return_layout=False) -> list:
    """
    Handle the processing of Power BI report from S3.
    """
    try:
        # download -> upload -> extract -> read report on s3 bucket
        columns = []
        extracted_report = __download_report_to_s3(config, report, workspace_scan_result)

        # Save Columns
        if not return_layout:
            __save_columns(config, report, extracted_report, workspace_scan_result)

        # Update Attributes
        if not return_layout:
            columns, _ = __process_attribute_data(report, extracted_report, workspace_scan_result)

    except Exception as e:
        log_error("PowerBI workbook processing on S3 failed", e)
    finally:
        return columns , extracted_report
