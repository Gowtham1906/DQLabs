import airflow
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from dqlabs.utils import load_env
from dqlabs.app_helper.dag_helper import default_args
from dqlabs.tasks.connections import create_airflow_connections

# load the env variables
base_path = str(Path(__file__).parents[0])
load_env(base_path)

dag = DAG(
    dag_id="manage_connections",
    default_args=default_args,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
    concurrency=2,
    max_active_tasks=1,
)
with dag:
    connection_task = PythonOperator(
        task_id="create_connection",
        python_callable=create_airflow_connections
    )
