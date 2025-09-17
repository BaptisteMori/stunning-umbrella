from enum import Enum


class TaskStatus(Enum):
    NOT_CREATED = "not_created"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"
    CANCELLED = "cancelled"