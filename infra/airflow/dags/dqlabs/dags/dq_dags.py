import airflow
from airflow.models import DAG
from dqlabs.app_helper.dag_helper import default_args
from dqlabs.app_constants.dq_constants import CUSTOM, BUSINESS_RULES
from dqlabs.tasks import create_tasks


def create_dag(dag_info: dict, category: str) -> DAG:
    """
    Create dag for each job categories
    """
    core_connection_id = dag_info.get("core_connection_id")
    if not core_connection_id:
        return None

    organization_id = dag_info.get("admin_organization")
    if not organization_id:
        organization_id = dag_info.get("id")

    dag_name = BUSINESS_RULES if category == CUSTOM else category
    dag_id = f"{str(organization_id)}_{dag_name}"
    schedule_interval = dag_info.get("schedule_interval")
    schedule_interval = schedule_interval if schedule_interval else None
    dag_params = {}
    # if category in [EXPORT_FAILED_ROWS, CATALOG_UPDATE]:
    #     dag_params = { "max_active_tasks": 2}

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=airflow.utils.dates.days_ago(1),
        catchup=False,
        is_paused_upon_creation=False,
        tags=[organization_id],
        max_active_runs=1,
        **dag_params,
    )

    with dag:
        tasks = create_tasks(dag_info, category)
        tasks
    return dag
