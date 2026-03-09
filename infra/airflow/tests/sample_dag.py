import airflow
import os
from airflow.configuration import conf 
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


def print_text(*args, **kwargs):
    k_conf = kwargs.get('conf')
    k_conf = k_conf if k_conf else {}
    base_url = k_conf.get('webserver', 'BASE_URL')
    print('ENV_NAME 1', k_conf.get('core', 'ENV_NAME'))
    base_url = conf.get('webserver', 'BASE_URL')
    print('base_url 1', base_url)
    print('ENV_NAME 2', conf.get('core', 'ENV_NAME'))
    print(kwargs)
    print(args)
    for key, value in os.environ.items():
        print(key, value)
    print("Hello World!")

def create_sample_dag()-> DAG:
    dag = DAG(
        dag_id="sample_dag",
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
            task_id="sample_task",
            python_callable=print_text,
        )
        load_env
    return dag

# register dags
dag: DAG = create_sample_dag()
        