from abc import ABC

from airflow.models import BaseOperator


class BaseDQLabsOperator(BaseOperator, ABC):
    def __init__(self, rule: dict, *args, **kwargs):
        """
        Base Operator for DQLabs.
        """
        super().__init__(*args, **kwargs)
        self.rule = rule
