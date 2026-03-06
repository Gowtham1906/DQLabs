import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator


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
        alternatives = BashOperator(
            task_id="alternatives",
            bash_command="ls ${AIRFLOW_HOME}/plugins/driver/jdbc",
        )
        alternatives
    return dag

# register dags
dag: DAG = create_sample_dag()
        