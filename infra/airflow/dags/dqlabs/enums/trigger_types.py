from enum import Enum


class TriggerType(Enum):
    Manual = "manual"
    Schedule = "schedule"
    Once = "once"
