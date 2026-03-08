from airflow.models import SkipMixin
from airflow.exceptions import AirflowFailException

from dqlabs.dq_package.operators.base_operator import BaseDQLabsOperator
from dqlabs.dq_package.utils import DQClientUtils


class DQLabsCircuitBreakerOperator(BaseDQLabsOperator, SkipMixin):
    def __init__(self, rule: dict, *args, **kwargs):
        """
            rule params options
            {
                asset_id : ID of asset
                measure_id: ID of measure
                connection: {
                    connection_name: Connection Name
                    database_name: Client Database Name
                    schema_name: Client Schema Name
                    table_name: Client Table Name
                }
                condition: Condition Need to Satisfied (< , < >= <= , =)
                threshold: Condition need to Validate the Value (integer)
            }
        """
        super().__init__(rule=rule, *args, **kwargs)

    def execute(self, context) -> bool:
        """
        Execute the circuit breaker operator.
        """
        try:
            status = DQClientUtils(self.rule).get_circuit_breaker_rule_to_dq()
            if not status:
                downstream_tasks = context['task'].get_flat_relatives(
                    upstream=False)
                if downstream_tasks:
                    self.skip(
                        context['dag_run'], context['ti'].execution_date, downstream_tasks)
        except Exception as err:
            message = 'Encountered an error when executing the rule, but failing open.'
            raise AirflowFailException(message)
