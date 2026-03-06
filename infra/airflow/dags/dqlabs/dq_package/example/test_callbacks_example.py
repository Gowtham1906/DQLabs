from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import time

from dqlabs.dq_package.callbacks.dq_callbacks import dq_task_success_callback

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,  # You can adjust the number of retries as needed
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Create a DAG instance and specify the default arguments
dag = DAG(
    'test_callbacks_example',
    default_args=default_args,
    description='A simple Airflow DAG with two tasks scheduled 4 times a day',
    # Runs at midnight, 6 AM, 12 PM, and 6 PM
    # schedule_interval='0 0,6,12,18 * * *',
    tags=["circuit"]
)

# Define the first task


def task1():
    print("Executing Task 1")


task1_task = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
    on_success_callback=dq_task_success_callback
)

# Define the second task


def task2():
    print("Executing Task 2")


task2_task = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

# Set the task dependencies
task1_task >> task2_task
