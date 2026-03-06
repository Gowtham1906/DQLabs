import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


import time
import requests
import json
import time


class LivyService(object):
    def __init__(self, host, driver_file_path):
        self.headers = {"Content-Type": "application/json"}
        self.host = host
        self.driver_file_path = driver_file_path
        self.batch_url = f"{self.host}/batches"

    def kill_job(self, job_id):
        if job_id is None or job_id == -1:
            return
        end_point = f"{self.host}/batches/{job_id}"
        response = requests.delete(end_point, headers=self.headers)
        return response.json()

    def _parse_batch_output(self, batch_id: int):
        response_data = None
        end_point = f"{self.host}/batches/{batch_id}/log"
        response = requests.get(end_point, headers=self.headers)
        batch_log = response.json()
        log_output = batch_log.get("log", [])
        log_output = log_output if log_output else []
        job_output = next(
            (log for log in log_output if log.strip().startswith("<-dq_result-> ")),
            None,
        )
        if job_output:
            json_string = job_output.replace("<-dq_result-> ", "").strip()
            response_data = json.loads(json_string)
        return response_data

    def _get_batch_output(self, batch_id: int):
        time.sleep(2)
        end_point = f"{self.host}/batches/{batch_id}/state"
        response = requests.get(end_point, headers=self.headers)
        batch_info = response.json()
        job_status = batch_info.get("state", "")
        job_status = job_status.lower() if job_status else ""

        response_data = None
        if job_status != "success":
            response_data = self._get_batch_output(batch_id)
        else:
            response_data = self._parse_batch_output(batch_id)
        return response_data

    def _run_batch_job(self, file_path: str, input_config: dict):
        data = {
            "file": f"file://{file_path}",
            "args": [json.dumps(input_config)],
        }
        response = requests.post(
            self.batch_url, data=json.dumps(data), headers=self.headers
        )
        batch_job = response.json()
        job_id = batch_job.get("id", None) if batch_job else None
        response = self._get_batch_output(job_id) if job_id else None
        return response

    def start_batch(self):
        try:
            storage_account_name = "adlsdqconnector"  # container name
            storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
            client_id = os.environ.get("AZURE_CLIENT_ID", "")
            client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")
            tenant_id = os.environ.get("AZURE_TENANT_ID", "")
            storage_account_name = "adlsdqconnector"
            storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY", "")
            container_name = "dqlabs-adls001"
            file_path = "BankingProduct.csv"

            input_config = {
                "spark_conf": {
                    f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": storage_account_key,
                    f"spark.hadoop.fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "OAuth",
                    f"spark.hadoop.fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    f"spark.hadoop.fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net": client_id,
                    f"spark.hadoop.fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net": client_secret,
                    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
                },
                "job_config": {
                    "container_name": container_name,
                    "storage_account_name": storage_account_name,
                    "file_path": file_path,
                    "adls_file_path": f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}",
                },
            }
            response = self._run_batch_job(
                f"{self.driver_file_path}/spark_driver.py", input_config
            )
            print("response", response)
            return response
        except Exception as error:
            raise ValueError(f"Failed to execute the script: {str(error)}")


def execute_spark_query():
    try:
        # Init spark contextprint("Creating Spark Context")
        livy_service = LivyService(
            "http://host.docker.internal:8998",
            "/Users/gobi/Projects/POC/IcebergPOC/iceberg_local",
        )
        response = livy_service.start_batch()
        print("response", response)
    except Exception as e:
        raise e


def create_sample_dag() -> DAG:
    dag = DAG(
        dag_id="spark_local_test",
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
            task_id="spark_test",
            python_callable=execute_spark_query,
        )
        load_env
    return dag


# register dags
dag: DAG = create_sample_dag()
