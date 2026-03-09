import airflow
import os
import pymssql
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string


from decimal import *
import decimal


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
            dict_data = dict(
                zip([col[0] for col in column_description], query_result))
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
        host='192.168.0.12',
        user='bdcadds\\sqluser',
        password='Intellectyx@123',
    )

    return conn


def execute_query():
    try:
        query_string = "select count(*) as total_count from [dqlabs].[dbo].[EMPLOYEES]"
        print("**************** Creating Connection  *********************")
        connection = create_connection()
        print("**************** Connection Created Successfully  *********************")
        with connection.cursor() as cursor:
            print("**************** Executing The Query  *********************")
            cursor.execute(query_string)
            response = fetchall(cursor)
            print("**************** Query Executed Successfully  *********************")
        print("response", response)
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e


def create_sample_dag()-> DAG:
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
            python_callable=execute_query,
        )
        load_env
    return dag

# register dags
dag: DAG = create_sample_dag()