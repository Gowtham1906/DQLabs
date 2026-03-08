from enum import Enum


class ApprovalStatus(Enum):
    Pending = "Pending"
    ReadyForReview = "Ready For Review"
    Verified = "Verified"
    Deprecated = "Deprecated"