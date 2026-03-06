import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from copy import deepcopy
from pathlib import Path

from dqlabs.utils import load_env
from dqlabs.app_helper.dag_helper import default_args



def load_environment_file():
    base_path = str(Path(__file__).parents[0])
    load_env(base_path)

def create_env_loader_dag()-> DAG:
    dag_args = deepcopy(default_args)
    dag = DAG(
        dag_id="env_loader",
        default_args=dag_args,
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
            task_id="load_env",
            python_callable=load_environment_file,
        )
        load_env
    return dag

# register dags
dag: DAG = create_env_loader_dag()
        