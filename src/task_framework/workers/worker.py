from enum import Enum
import logging
import signal

from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue


LOGGER: logging.Logger = logging.getLogger(__name__)


class WorkerStatus(Enum):
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class Worker:
    def __init__(self, queue: Queue, registry: TaskRegistry):
        self.queue: Queue = queue
        self.registry: TaskRegistry = registry

        self.running: bool = True
        self.status: WorkerStatus = WorkerStatus.IDLE

        # Graceful management of the stop
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)   
    
    def _shutdown(self, signum, frame):
        """Handles the shutdown signal, gracefully stopping the worker.

        Args:
            signum (int): The signal number that triggered the shutdown.
            frame (frame): The current stack frame at the time of the signal.
        """
        LOGGER.info(f"Shutdown of the worker (signal {signum})")
        self._update_status(WorkerStatus.STOPPING)
        self.running = False

    def _update_status(self, status: WorkerStatus) -> None:
        """Updates the internal status of the worker.

        Args:
            status (WorkerStatus): The new status to set for the worker.
        """
        LOGGER.debug("Status change : %s", status.value)
        self.status = status

    def run(self):
        """
        Main execution loop for the worker.

        This method should be implemented by subclasses to define the
        worker's specific task processing logic. It is expected to run
        continuously until the worker is stopped.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError()
