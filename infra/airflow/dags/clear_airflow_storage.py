from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from requests import get
import os
import shutil

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 10, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def cleanup_logs():
    try:
        log_dir = os.path.join(os.getenv("AIRFLOW_HOME", "~/airflow"), "logs")
        print(log_dir)
        # log_dir = "/airflow/logs"
        retention_days = 1
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        log_dir = os.path.expanduser(log_dir)

        for root, dirs, files in os.walk(log_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if datetime.fromtimestamp(os.path.getmtime(file_path)) < cutoff_date:
                    os.remove(file_path)
                    print("file deleted")
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                if not os.listdir(dir_path):
                    shutil.rmtree(dir_path)
        print(f"Clearing logs in {log_dir}...")
    except Exception as e:
        print("The error is: ",e)

with DAG(
        default_args=default_args,
        dag_id='clear_airflow_storage',
        catchup=False,
        is_paused_upon_creation=False,
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='clear_storage',
        python_callable=cleanup_logs
    )

    task1