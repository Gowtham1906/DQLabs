from enum import Enum


class ScheduleTypes(Enum):
    Asset = "asset"
    Semantic = "semantics"
    Measure = "measure"
    Report = "report"
    Metadata = "metadata"