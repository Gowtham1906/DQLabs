import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.module_loading import import_string



def print_text():
    try:
        connection_id = "snowflake__577b3579-121c-45b6-bd0a-7fd93b6bcbbe"
        provider = "airflow.contrib.hooks.snowflake_hook.SnowflakeHook"
        provider_args = {"snowflake_conn_id": connection_id}
        hook_object = import_string(provider)
        hook_object = hook_object(**provider_args)
        connection = hook_object.get_conn()
    except Exception as e:
        raise e

def create_connection_id_check()-> DAG:
    dag = DAG(
        dag_id="connection_id_check",
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
            task_id="conn_id_check",
            python_callable=print_text,
        )
        load_env
    return dag

# register dags
dag: DAG = create_connection_id_check()
        