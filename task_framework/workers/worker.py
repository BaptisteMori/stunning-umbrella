from enum import Enum

from ..core.registry import TaskRegistry
from ..queue.queue import TaskQueue


class WorkerStatus(Enum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class Worker:
    def __init__(self, queue: TaskQueue, registry: TaskRegistry):
        self.queue: TaskQueue = queue
        self.registry: TaskRegistry = registry

    def run(self):
        raise NotImplemented()
