from enum import Enum


class ScheduleStatus(Enum):
    Submitted = "Submitted"
    Queued = "Queued"
    Pending = "Pending"
    Running = "Running"
    Completed = "Completed"
    Failed = "Failed"
    Killed = "Killed"
    UpForRetry = "UpForRetry"