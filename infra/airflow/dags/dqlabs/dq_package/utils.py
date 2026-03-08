
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging


from airflow.hooks.http_hook import HttpHook
from airflow.models import SlaMiss, DagRun, DAG
from airflow.exceptions import AirflowSkipException


logger = logging.getLogger(__name__)

_DEFAULT_DQ_CONN_ID = 'dq_default_http_conn_id'
_DEFAULT_CALL_TIMEOUT = 10
_SUCCESS_STATES = ['success', 'skipped']
_EXCEPTION_MSG_LIMIT = 10 * 1024  # 10kb


class DQClientUtils():
    def __init__(self, rule=None) -> None:
        self.dq_default_http_conn_id = _DEFAULT_DQ_CONN_ID
        self.rule = rule

    def get_circuit_breaker_rule_to_dq(self):
        try:
            result = False
            hook = HttpHook(method='post',
                            http_conn_id=self.dq_default_http_conn_id)
            if hook:
                body = {
                    "response": self.rule,
                    "callback_type": 'circuit_breaker'
                }
                response = hook.run(endpoint='',
                                    headers={
                                        'content-type': 'application/json',
                                        'Accept': 'application/json'
                                    },
                                    data=json.dumps(body),
                                    extra_options={"timeout": _DEFAULT_CALL_TIMEOUT})
                if (response and response.status_code in [200]):
                    result = response.json()
                return result
            else:
                logger.exception(f'Failed to load HTTP Connection')
                raise Exception
        except Exception as ex:
            logger.exception(f'Failed to call server: {ex}')
            raise Exception

    def send_callbacks_events_to_dq(self, method: str, data: dict):
        try:
            hook = HttpHook(method=method,
                            http_conn_id=self.dq_default_http_conn_id)
            if hook:
                body = {
                    "response": data,
                    "callback_type": 'callbacks'
                }
                hook.run(endpoint='',
                         headers={
                             'content-type': 'application/json',
                             'Accept': 'application/json'
                         },
                         data=json.dumps(body),
                         extra_options={"timeout": _DEFAULT_CALL_TIMEOUT})
            else:
                logger.exception(f'Failed to load HTTP Connection')
        except Exception as ex:
            logger.exception(f'Failed to call server: {ex}')

    def dq_send_dag_result(self, context: Dict):
        if not self._validate_dag_callback_context(context=context):
            return

        dag: DAG = context['dag']
        dag_run: DagRun = context['dag_run']
        data = {
            "dag_id": dag.dag_id,
            "run_id": context['run_id'],
            "success": dag_run.state in _SUCCESS_STATES,
            "reason": context['reason'],
            "state": dag_run.state,
            "execution_date": self._get_datetime_isoformat(dag_run.execution_date),
            "start_date": self._get_datetime_isoformat(dag_run.start_date),
            "end_date": self._get_datetime_isoformat(dag_run.end_date),
            "original_dates": self._get_original_dates(dag_run.execution_date, dag_run.start_date, dag_run.end_date)
        }
        self.send_callbacks_events_to_dq('post', data)

    def dq_send_task_result(self, context: Dict):
        if 'dag' not in context or 'run_id' not in context or 'task_instance' not in context:
            logger.error(
                'Invalid context received in task callback: dag, run_id and task_instance are expected')
            return

        dag = context['dag']
        ti = context['task_instance']
        exception_message = self._truncate_string(
            str(context['exception']),
            _EXCEPTION_MSG_LIMIT,
        ) if 'exception' in context else None
        task_instance_result = self._get_task_instance_result(
            ti, exception_message)

        data = {
            "dag_id": dag.dag_id,
            "run_id": context['run_id'],
            "success": task_instance_result.get('state', '') in _SUCCESS_STATES,
            "task": task_instance_result,
        }
        self.send_callbacks_events_to_dq('post', data)

    def dq_send_sla_misses(self, dag: DAG, sla_misses: List[SlaMiss]):
        data = {
            "dag_id": dag.dag_id,
            "sla_misses": [
                {
                    "task_id": sla_miss.task_id,
                    "execution_date": self._get_datetime_isoformat(
                        sla_miss.execution_date),
                    "timestamp": self._get_datetime_isoformat(sla_miss.timestamp),
                } for sla_miss in sla_misses
            ]
        }
        self.send_callbacks_events_to_dq('post', data)

    def _validate_dag_callback_context(self, context: Dict) -> bool:
        error_message: Optional[str] = None
        if 'dag' not in context or 'run_id' not in context or 'dag_run' not in context:
            error_message = 'dag, run_id and dag_run are expected'
        else:
            dag_run: DagRun = context['dag_run']
            if not dag_run.end_date:
                error_message = 'no dag_run.end_date set, it looks like the dag is still running'
            elif 'reason' not in context:
                error_message = 'no reason set, it looks like the dag is still running'

        if error_message:
            logger.error(f'Invalid context received in dag callback: {error_message}. '
                         'Please check your callbacks are configured properly.')
            return False
        return True

    def _get_task_instance_result(
            self,
            ti: Any,
            exception_message: Optional[str] = None
    ) -> Any:
        return {
            "task_id": ti.task_id,
            "state": ti.state,
            "log_url": ti.log_url,
            "prev_attempted_tries": ti.prev_attempted_tries,
            "duration": ti.duration or 0,
            "execution_date": self._get_datetime_isoformat(ti.execution_date),
            "start_date": self._get_datetime_isoformat(ti.start_date),
            "end_date": self._get_datetime_isoformat(ti.end_date),
            "next_retry_datetime": self._get_next_retry_datetime(ti),
            "max_tries": ti.max_tries,
            "try_number": ti.try_number,
            "exception_message": exception_message,
            "original_dates": self._get_original_dates(ti.execution_date, ti.start_date, ti.end_date),
        }

    def _get_datetime_isoformat(self, d: Optional[datetime]) -> str:
        return d.isoformat() if d else datetime.now(tz=timezone.utc).isoformat()

    def _get_original_dates(self, execution_date: Optional[datetime], start_date: Optional[datetime], end_date: Optional[datetime]) -> str:
        return f"execution={str(execution_date)}, start_date={str(start_date)}, end_date={str(end_date)}"

    def _get_optional_datetime_isoformat(self, d: Optional[datetime]) -> Optional[str]:
        return d.isoformat() if d else None

    def _get_next_retry_datetime(self, ti: Any) -> Optional[str]:
        if not hasattr(ti, 'task') or not ti.end_date:
            return None
        return self._get_optional_datetime_isoformat(ti.next_retry_datetime())

    def _truncate_string(self, string, length):
        if len(string) <= length:
            return string
        else:
            return string[:length] + "..."
