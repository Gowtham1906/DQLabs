import logging
from airflow.models import DAG, TaskInstance
from airflow.utils.log.log_reader import TaskLogReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_error(message: str, error: Exception):
    logger.error(f"{message} : {str(error)}", exc_info=True)


def log_info(message):
    logger.info(message)


def get_airflow_logs(task_config: dict):
    """
    Returns detailed error log from local airflow instance
    """
    detailed_error = ""
    try:
        attempt = task_config.get("attempt")
        attempt = 1
        dag: DAG = task_config.get("dag")
        task_instance: TaskInstance = task_config.get("task_instance")
        task_instance.task = dag.get_task(task_instance.task_id)
        task_log_reader: TaskLogReader = TaskLogReader()
        logs, _ = task_log_reader.read_log_chunks(task_instance, attempt, {})
        logs = logs[0] if logs else []

        for log in logs:
            id, message = log
            if not message:
                continue
            detailed_error = detailed_error + f"{id} {message}"
    except Exception as e:
        log_error("get_airflow_logs", e)
        detailed_error = ""
        detailed_error = str(e)
    return detailed_error
