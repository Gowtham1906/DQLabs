import logging

from dqlabs.dq_package.utils import DQClientUtils

logger = logging.getLogger(__name__)


def dq_dag_success_callback(context: dict):
    """
    Callback function that sends the DAG success event to DQLabs, it must be configured as
    `on_success_callback` in a DAG object.
    """
    try:
        DQClientUtils().dq_send_dag_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send dag result to dq: {ex}')


def dq_dag_failure_callback(context: dict):
    """
    Callback function that sends the DAG failure event to DQLabs, it must be configured as
    `on_failure_callback` in a DAG object.
    """
    try:
        DQClientUtils().dq_send_dag_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send dag failure result to dq: {ex}')


def dq_task_success_callback(context: dict):
    """
    Callback function that sends the Task success event to DQLabs, it must be configured as
    `on_success_callback` in a Task object.
    """
    try:
        DQClientUtils().dq_send_task_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send task result to dq: {ex}')


def dq_task_execute_callback(context: dict):
    """
    Callback function that sends the Task execution event to DQLabs, it must be configured as
    `on_execute_callback` in a Task object.
    """
    try:
        DQClientUtils().dq_send_task_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send task result to dq: {ex}')


def dq_task_failure_callback(context: dict):
    """
    Callback function that sends the Task failure event to DQLabs, it must be configured as
    `on_failure_callback` in a Task object.
    """
    try:
        DQClientUtils().dq_send_task_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send task failure result to dq: {ex}')


def dq_task_retry_callback(context: dict):
    """
    Callback function that sends the Task retry event to DQLabs, it must be configured as
    `on_retry_callback` in a Task object.
    """
    try:
        DQClientUtils().dq_send_task_result(context)
    except Exception as ex:
        logger.exception(f'Failed to send task retry result to dq: {ex}')


def dq_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback function that sends the SLA misses event to DQLabs, it must be configured as
    `sla_miss_callback` in a DAG object.
    """
    try:
        DQClientUtils().dq_send_sla_misses(dag=dag, sla_misses=slas)
    except Exception as ex:
        logger.exception(f'Failed to send SLA misses result to dq: {ex}')


task_callbacks = {
    'on_success_callback': dq_task_success_callback,
    'on_failure_callback': dq_task_failure_callback,
    'on_retry_callback': dq_task_retry_callback,
    'on_execute_callback': dq_task_execute_callback,
}

dag_callbacks = {
    'on_failure_callback': dq_dag_failure_callback,
    'on_success_callback': dq_dag_success_callback,
    'sla_miss_callback': dq_sla_miss_callback,
}
