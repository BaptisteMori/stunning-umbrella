from enum import IntEnum
from dataclasses import dataclass


class TaskPriority(IntEnum):
    """
    Priority levels for task execution.
    Higher values indicate higher priority.
    """
    CRITICAL = 100  # System-critical tasks
    HIGH = 75       # Time-sensitive user tasks
    NORMAL = 50     # Standard priority
    LOW = 25        # Background tasks
    IDLE = 0        # Best-effort tasks
