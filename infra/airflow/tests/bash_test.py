import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.module_loading import import_string

def print_class_path():
    import jpype
    print(jpype.getClassPath())


def connect():
    connection_id = 'denodo__dev_test'
    provider = "airflow.providers.jdbc.hooks.jdbc.JdbcHook"
    provider_args = {"jdbc_conn_id": connection_id}
    hook_object = import_string(provider)
    hook_object = hook_object(**provider_args)
    connection = hook_object.get_conn()

    query = "SELECT * FROM employees"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        print("total rows", len(results))



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
            bash_command="alternatives --list",
        )
        class_path = PythonOperator(
            task_id="class_path",
            python_callable=print_class_path,
        )
        scheduler_task = PythonOperator(
            task_id="denodo_test",
            python_callable=connect,
        )
        alternatives >> class_path >> scheduler_task
    return dag

# register dags
dag: DAG = create_sample_dag()
        