import airflow
import os
import pymssql
import requests
import json
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string


from decimal import *
import decimal

from dqlabs.utils import (
    load_env,
)
from dqlabs.app_helper.db_helper import fetch_count

# load the env variables
base_path = str(Path(__file__).parents[0])
load_env(base_path)


def remove_exponent(d):
    try:
        return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
    except:
        return d


def fetchall(cursor):
    query_results = cursor.fetchall()
    result = []
    if query_results:
        column_description = cursor.description
        for query_result in query_results:
            dict_data = dict(zip([col[0] for col in column_description], query_result))
            for col in column_description:
                if isinstance(dict_data[col[0]], decimal.Decimal):
                    dict_data[col[0]] = remove_exponent(dict_data[col[0]])
            result.append(dict_data)
    return result


def create_connection():
    # connection_id = 'test_sql_authentication'
    # provider = "airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook"
    # provider_args = {"mssql_conn_id": connection_id}
    # hook_object = import_string(provider)
    # hook_object = hook_object(**provider_args)

    # conn = hook_object.get_conn()
    conn = pymssql.connect(
        autocommit=True,
        host="198.37.101.135",
        user="dqlabs",
        password="Intellectyx@123",
        database="dqlabs",
        port="1433",
        timeout=0,
    )

    return conn


def execute_query():
    try:
        query_string = (
            " SELECT * FROM dqlabs.[PERFORMANCE_SCHEMA].[Customer_performance] "
        )
        print("**************** Creating Connection  *********************")
        connection = create_connection()
        print("**************** Connection Created Successfully  *********************")
        with connection.cursor() as cursor:
            print("**************** Executing The Query  *********************")
            cursor.execute(query_string)
            response = fetch_count(cursor)
            print("**************** Query Executed Successfully  *********************")
        print("response", response)
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e


def execute_agent_query():
    try:
        query_string = (
            "SELECT * FROM dqlabs.[PERFORMANCE_SCHEMA].[Customer_performance]"
        )
        endpoint = "http://98.84.153.4:8005/api/connection/execute/"
        data = {
            "connection_type": "mssql",
            "credentials": {
                "port": "1433",
                "user": "bDsJq0q1u9bOkVXwdWtFrA==",
                "schema": "",
                "server": "198.37.101.135",
                "database": "dqlabs",
                "password": "PS8RYH0kwaIO3/eUd5D54Q==",
                "vault_key": "",
                "is_read_only": False,
                "selected_vault": "",
                "is_vault_enabled": False,
                "authentication_type": "Username and Password",
                "trust_server_certificate": False,
                "db_config": {},
            },
            "vault_config": {},
            "query": query_string,
            "is_encrypted": True,
            "is_list": False,
            "is_count_only": True,
            "is_all": False,
            "commit": False,
            "columns_only": False,
            "limit": 0,
            "no_response": False,
            "convert_lower": True,
            "log_level": "info",
            "method_name": "",
            "parameters": {},
            "db_config": {},
        }
        headers = {
            "Content-Type": "application/json",
            "Client-Id": "yyR11YDhGPGgpfNV15JORO6UV6mZ5mBhw9INwMBRPtClWEIq/WRsZu2/5xPvfEEw",
            "Client-Secret": "RXezuDl1vVvWLTxL4HwdXC0sXj5IgqWhOn3YxU38r8IimTa/cdXz5CIUzoayDRqdX3jiEZZbyM54TpXeVKs8hHYl9OeqijZhtmeUACaHGCM=",
        }
        print("Request SENT")
        response = requests.post(
            endpoint,
            data=json.dumps(data),
            headers=headers,
        )
        print("Request RECIEVED")

        if not response.ok:
            response.raise_for_status()
        print("response", response.json())
    except Exception as e:
        print("Request FAILED")
        print(f"Error: {str(e)}")
        raise e


def create_sample_dag() -> DAG:
    dag = DAG(
        dag_id="windows_authentication_test",
        schedule_interval=None,
        start_date=airflow.utils.dates.days_ago(1),
        catchup=False,
        is_paused_upon_creation=False,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
    )

    with dag:
        load_env = PythonOperator(
            task_id="windows_auth",
            python_callable=execute_agent_query,
        )
        load_env
    return dag


# register dags
dag: DAG = create_sample_dag()
