"""
    Migration Notes From V2 to V3:
    No Migration Changes
"""

import json
import pandas as pd
import numpy as np

from dqlabs.utils.connections import (
    get_dq_connections
)
from dqlabs.utils.user_activity import (
    get_user_session,
    get_user_activity,
    save_user_activity_log,
    get_user_activity_log,
    insert_user_activity_log
)
from dqlabs.utils.extract_workflow import get_queries
from dqlabs.utils.request_queue import update_queue_detail_status, update_queue_status
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.utils.tasks import get_task_config

from dqlabs.utils.extract_workflow import (
    get_queries
)
from dqlabs.utils.lookup_process import (
    generate_lookup_source_large_datatype
)
from dqlabs.app_helper.dag_helper import execute_native_query
from dqlabs.enums.connection_types import ConnectionType


def run_useractivity(config: dict, **kwargs):
    """
    Run user activity task
    """
    try:
        # Get Reports Settings
        general_settings = config.get("settings", {})
        if not general_settings:
            dag_info = config.get("dag_info", {})
            dag_info = dag_info if dag_info else {}
            general_settings = dag_info.get("settings", {})
        general_settings = general_settings if general_settings else {}
        general_settings = json.loads(general_settings, default=str) if isinstance(
            general_settings, str) else general_settings

        report_settings = config.get("dag_info").get("report_settings")
        if not report_settings:
            report_settings = general_settings.get("reporting")
        report_settings = json.loads(report_settings, default=str) if report_settings and isinstance(
            report_settings, str) else report_settings
        report_settings = report_settings if report_settings else {}
        schema_name = report_settings.get("schema")
        schema_name = schema_name if schema_name else ""
        destination_conn_database = report_settings.get("database", "")
        if not schema_name:
            raise Exception(
                "Please define the schema name in settings -> platform -> configuration -> remediate -> push down metrics section to export the data.")

        # Get Destination Connection
        destination_connection_id = report_settings.get('connection')
        destination_connection_id = destination_connection_id if destination_connection_id else {}
        destination_connection_id = destination_connection_id.get('id', None)

        destination_connection_object = get_dq_connections(
            config, destination_connection_id)

        if not destination_connection_object or len(destination_connection_object) == 0:
            raise Exception("Missing Connection Details.")

        destination_connection_object = destination_connection_object[0]
        destination_connection_object = destination_connection_object if destination_connection_object else {}
        destination_config = {}
        if destination_connection_object:
            destination_conn_credentials = destination_connection_object.get(
                'credentials')
            user_destination_conn_database = destination_conn_credentials.get(
                'database')
            destination_conn_database = destination_conn_database if destination_conn_database else user_destination_conn_database
            report_settings_database = report_settings.get("database")
            connection_type = config.get("connection_type")
            if connection_type == ConnectionType.DB2IBM.value and not report_settings_database:
                report_settings_database = 'sample'
            if not report_settings_database:
                report_settings.update({
                    'database': destination_conn_database if destination_conn_database else ''
                })
            destination_config = {
                "connection_type": destination_connection_object.get('type'),
                "source_connection_id": destination_connection_object.get('airflow_connection_id'),
                "connection": {**destination_connection_object}
            }
            destination_connection_object.update({
                "connection_id": destination_connection_object.get('airflow_connection_id'),
                "connection_type": destination_connection_object.get('type')
            })

        isexits, output_details = get_user_activity_log(config, {'connection_id': destination_connection_id, 'database': destination_conn_database,
                                                                 'schema': schema_name})

        last_push_date = None
        if output_details:
            last_push_date = output_details.get('start_time')

        insert_data = {
            "connection_id": destination_connection_id,
            "queue_id": config.get('queue_id'),
            "status": ScheduleStatus.Running.value,
            "database": destination_conn_database,
            "schema": schema_name
        }
        getid = insert_user_activity_log(config, insert_data)
        max_date = None

        user_session = get_user_session(config)

        # Get Queries Based On Connection Type
        default_destination_queries = get_queries(
            {**config, "connection_type": destination_connection_object.get('type')})

        # create failed rows table
        failed_rows_query = default_destination_queries.get("failed_rows", {})

        current_date_query = failed_rows_query.get("current_date")
        current_date = str(current_date_query.split('AS')[0]).strip()
        schema_query = failed_rows_query.get("schema")
        schema_query = str(schema_query) if schema_query else ""
        text_value_query = failed_rows_query.get("text_value")
        text_value_query = str(text_value_query) if text_value_query else ""
        task_config = get_task_config(config, kwargs)

        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config)
        update_queue_status(config, ScheduleStatus.Running.value, True)

        df = pd.DataFrame.from_dict(user_session)
        df = df.replace(np.nan, '')

        column_list = ""
        static_col = ""

        source_datatype = generate_lookup_source_large_datatype(
            {'connection_type': destination_connection_object.get("type").lower()})

        special_string = "`" if destination_connection_object.get("type").lower(
        ) in [ConnectionType.BigQuery.value, ConnectionType.Databricks.value, ConnectionType.MySql.value] else "\""

        for col in df.columns:
            column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
            static_col = f"""{static_col}{special_string}{col}{special_string} ,"""
        column_list = column_list[:-1]
        static_col = static_col[:-1]
        column_list = f"""{column_list}"""
        static_col = f"""{static_col}"""

        lookup_process_queries = default_destination_queries.get(
            "lookup_process", {})
        create_table_query = lookup_process_queries.get("create_table")
        insert_table_query = lookup_process_queries.get("insert_table")
        drop_table_query = lookup_process_queries.get("drop_table")

        config.update({
            "connection_type": destination_connection_object.get('type'),
            "source_connection_id": destination_connection_object.get('airflow_connection_id'),
            "connection": destination_connection_object
        })
        drop_table_query = drop_table_query.replace("<database_name>", destination_conn_database).replace(
            "<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<query_string>", column_list)
        execute_native_query(config, drop_table_query,
                             None, True, no_response=True)
        create_table_query = create_table_query.replace("<database_name>", destination_conn_database).replace(
            "<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<query_string>", column_list)
        execute_native_query(config, create_table_query,
                             None, True, no_response=True)
        insert_query = insert_table_query.replace("<database_name>", destination_conn_database).replace(
            "<schema_name>", schema_name).replace("<table_name>", f"""USER_SESSION""").replace("<columns>", static_col)
        orginal_value = f""""""
        df_length = len(df)
        for index, row in df.iterrows():
            index_value = index + 1
            insert_value = "values"
            if config.get('connection_type', '').lower() == ConnectionType.Synapse.value or config.get('connection_type', '').lower() == ConnectionType.Oracle.value:
                special_value = ""
                if config.get('connection_type', '').lower() == ConnectionType.Oracle.value:
                    special_value = f"""FROM DUAL"""
                string_list = [
                    f"""'{str(element)}'""" for element in row.values]
                insert_value = ""
                orginal_value = f"""{orginal_value} SELECT {str(string_list[0])} {special_value} UNION ALL""" if len(
                    string_list) == 1 else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} UNION ALL"""
            else:
                string_list = [str(element) for element in row.values]
                orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                    string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""

            if df_length == index_value:
                orginal_value = orginal_value[:-
                                              1] if insert_value else orginal_value[:-9]
                insert_data = insert_query.replace(
                    "<insert_query>", f"""{insert_value} {orginal_value}""")
                execute_native_query(config, insert_data,
                                     None, True, no_response=True)
                orginal_value = f""""""
            elif (index_value % 998 == 0):
                orginal_value = orginal_value[:-
                                              1] if insert_value else orginal_value[:-9]
                insert_data = insert_query.replace(
                    "<insert_query>", f"""{insert_value} {orginal_value}""")
                execute_native_query(config, insert_data,
                                     None, True, no_response=True)
                orginal_value = f""""""

        """ User Activity Insert Details """
        user_activity = get_user_activity(config, last_push_date)
        if user_activity:
            df = pd.DataFrame.from_dict(user_activity)
            df = df.replace(np.nan, '')
            max_date = max(df['CREATED_DATE'])

            column_list = ""
            static_col = ""

            for col in df.columns:
                column_list = f"""{column_list}{special_string}{col}{special_string} {source_datatype},"""
                static_col = f"""{static_col}{special_string}{col}{special_string} ,"""
            column_list = column_list[:-1]
            static_col = static_col[:-1]
            column_list = f"""{column_list}"""
            static_col = f"""{static_col}"""

            lookup_process_queries = default_destination_queries.get(
                "lookup_process", {})
            create_table_query = lookup_process_queries.get("create_table")
            insert_table_query = lookup_process_queries.get("insert_table")
            drop_table_query = lookup_process_queries.get("drop_table")
            if not isexits:
                drop_table_query = drop_table_query.replace("<database_name>", destination_conn_database).replace(
                    "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<query_string>", column_list)
                execute_native_query(
                    config, drop_table_query, None, True, no_response=True)
                create_table_query = create_table_query.replace("<database_name>", destination_conn_database).replace(
                    "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<query_string>", column_list)
                execute_native_query(
                    config, create_table_query, None, True, no_response=True)
            if user_activity:
                insert_query = insert_table_query.replace("<database_name>", destination_conn_database).replace(
                    "<schema_name>", schema_name).replace("<table_name>", f"""USER_ACTIVITY""").replace("<columns>", static_col)
                orginal_value = f""""""
                df_length = len(df)
                for index, row in df.iterrows():
                    index_value = index + 1
                    insert_value = "values"
                    if destination_connection_object.get("type").lower() == ConnectionType.Synapse.value or destination_connection_object.get("type").lower() == ConnectionType.Oracle.value:
                        special_value = ""
                        if destination_connection_object.get("type").lower() == ConnectionType.Oracle.value:
                            special_value = f"""FROM DUAL"""
                        string_list = [
                            f"""'{str(element)}'""" for element in row.values]
                        insert_value = ""
                        orginal_value = f"""{orginal_value} SELECT {str(string_list[0])} {special_value} UNION ALL""" if len(
                            string_list) == 1 else f"""{orginal_value} SELECT {",".join(string_list)} {special_value} UNION ALL"""
                    else:
                        string_list = [str(element) for element in row.values]
                        orginal_value = f"""{orginal_value}('{str(string_list[0])}'),""" if len(
                            string_list) == 1 else f"""{orginal_value}{str(tuple(string_list))},"""

                    if df_length == index_value:
                        orginal_value = orginal_value[:-
                                                      1] if insert_value else orginal_value[:-9]
                        insert_data = insert_query.replace(
                            "<insert_query>", f"""{insert_value} {orginal_value}""")
                        execute_native_query(
                            config, insert_data, None, True, no_response=True)
                        orginal_value = f""""""
                    elif (index_value % 998 == 0):
                        orginal_value = orginal_value[:-
                                                      1] if insert_value else orginal_value[:-9]
                        insert_data = insert_query.replace(
                            "<insert_query>", f"""{insert_value} {orginal_value}""")
                        execute_native_query(
                            config, insert_data, None, True, no_response=True)
                        orginal_value = f""""""
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        # update request queue status
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)
        insert_data = {
            "status": ScheduleStatus.Completed.value,
            "start_time": max_date,
            "id": getid.get('id') if getid else None
        }
        save_user_activity_log(config, insert_data)
