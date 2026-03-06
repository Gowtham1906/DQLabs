from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from dqlabs.dq_package.operators import DQLabsCircuitBreakerOperator
from dqlabs.dq_package.callbacks.dq_callbacks import dq_task_failure_callback
from airflow.operators.python import ShortCircuitOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}


with DAG(
        'test_circuit_breaker_example',
        default_args=default_args,
        start_date=datetime(2022, 2, 8),
        catchup=False,
        schedule_interval=None,
        tags=["dqlabs", "airflow"]
) as dag:
    task1 = BashOperator(
        task_id='example_elt_job_1',
        bash_command='echo I am transforming a very important table!'
    )
    cond_true = DQLabsCircuitBreakerOperator(
        task_id='condition_is_True',
        rule={'asset_id': '12eed221-da0a-4f14-9cf7-950185ebe712',
              'condition': '>', 'threshold': 50}
    )
    cond_false = DQLabsCircuitBreakerOperator(
        task_id='condition_is_False',
        rule={'connection': {'connection_name': 'Snowflake', 'database_name': 'DQLABS', 'schema_name': 'DQLABS', 'table_name': 'ASSET_METADATA'},
              'condition': '>', 'threshold': 30}
    )
    # cond_false_sco = ShortCircuitOperator(
    #     task_id="cond_false_sco",
    #     python_callable=lambda: False,
    # )
    # cond_true_sco = ShortCircuitOperator(
    #     task_id="cond_true_sco",
    #     python_callable=lambda: True,
    # )
    task2 = BashOperator(
        task_id='example_elt_job_2',
        bash_command='echo I am building a very important dashboard from the table created in task1!',
        trigger_rule='none_failed'
    )
    task3 = BashOperator(
        task_id='example_elt_job_3',
        bash_command='echo I am building a very important dashboard from the table created in task3!',
        trigger_rule='none_failed',
        on_failure_callback = [dq_task_failure_callback]
    )

    # task1 >> [cond_false_sco, cond_true_sco] >> task2

    task1 >> [cond_true, cond_false] >> task2 >> task3
