import signal
import logging

from workers.worker import WorkerStatus, Worker
from ..core.executor import TaskExecutor
from ..core.registry import TaskRegistry
from ..queue.queue import TaskQueue


LOGGER: logging.Logger = logging.getLogger(__name__)


class TaskWorker(Worker):
    """Worker to process tasks from a queue."""    
    def __init__(self, 
                 queue: TaskQueue,
                 registry: TaskRegistry,
                 max_retries: int = 3):
        super().__init__(queue, registry)

        self.executor = TaskExecutor(self.registry)
        self.max_retries = max_retries
        self.running = True
        self.status: WorkerStatus = WorkerStatus.PENDING

        # Graceful management of the stop
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        """Graceful shutdown handler"""
        LOGGER.info(f"Shutdown of the worker (signal {signum})")
        self._update_status(WorkerStatus.STOPPING)
        self.running = False

    def _update_status(self, status: WorkerStatus) -> None:
        self.status = status
    
    def run(self):
        """Worker main loop"""
        LOGGER.info("Worker started, Waiting for a task...")
        
        while self.running:
            self._update_status(WorkerStatus.STARTING)
            try:
                message = self.queue.dequeue(timeout=1)
                if message:
                    self._update_status(WorkerStatus.RUNNING)
                    self._process_message(message)
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                self._update_status(WorkerStatus.FAILED)
                LOGGER.error(f"Error in the main loop of the worker: {e}")
        self._update_status(WorkerStatus.STOPPED)
        LOGGER.info("Worker stopped")
    
    def _process_message(self, message):
        """Process a message from the queue."""
        try:
            LOGGER.info(f"Process of the task: {message.task_name}")
            self.executor.execute_task(message.task_name, message.params)
            LOGGER.info(f"Task {message.task_name} ended with success")
        
        except Exception as e:
            LOGGER.error(f"Error during the execution of the task {message.task_name}: {e}")
            
            # retry
            if message.retry_count < self.max_retries:
                message.retry_count += 1
                LOGGER.info(f"Retry {message.retry_count}/{self.max_retries}")
                self.queue.enqueue(message.task_name, message.params)
            else:
                LOGGER.error(f"Task {message.task_name} definitively failed after {self.max_retries} retries")
