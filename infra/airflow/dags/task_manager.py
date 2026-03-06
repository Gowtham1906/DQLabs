import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.timetables.interval import DeltaDataIntervalTimetable
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from dqlabs.utils import load_env
from dqlabs.app_helper.dag_helper import default_args
from dqlabs.utils.tasks import schedule_tasks


# load the env variables
base_path = str(Path(__file__).parents[0])
load_env(base_path)


def create_task_manager_dag()-> DAG:
    priority_weight = 10000
    weight_rule = "absolute"
    # execution_timeout = timedelta(minutes=1)
    # dagrun_timeout = timedelta(minutes=1)
    dag_args = deepcopy(default_args)
    dag_args.update({
        "priority_weight": priority_weight,
        "weight_rule": weight_rule,
        # "execution_timeout": execution_timeout,
    })
    dag = DAG(
        dag_id="task_manager",
        default_args=dag_args,
        # timetable=DeltaDataIntervalTimetable(timedelta(seconds=15)),
        schedule_interval="@continuous",
        start_date=airflow.utils.dates.days_ago(1),
        catchup=False,
        is_paused_upon_creation=False,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
        # dagrun_timeout=dagrun_timeout,
    )

    with dag:
        scheduler_task = PythonOperator(
            task_id="schedule_tasks",
            python_callable=schedule_tasks,
            priority_weight = priority_weight,
            weight_rule = weight_rule,
            # execution_timeout = execution_timeout,
        )
        scheduler_task
    return dag

# register dags
dag: DAG = create_task_manager_dag()
        