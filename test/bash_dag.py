import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from dqlabs.app_helper.dag_helper import default_args

dag = DAG(
    dag_id="bash_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
    is_paused_upon_creation=True,
)
with dag:
    connection_task = BashOperator(
        task_id="run_airflow_command",
        bash_command="airflow dags list"
    )
