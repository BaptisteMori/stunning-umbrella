import logging

from workers.worker import WorkerStatus, Worker
from core.executor import TaskExecutor
from core.registry import TaskRegistry
from queue.queue import TaskQueue
from queue.models import TaskMessage


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
    
    def _process_message(self, message: TaskMessage):
        """Process a message from the queue with proper retry handling."""
        try:
            LOGGER.info(f"Processing task: {message.task_name}")
            self.executor.execute_task(message.task_name, message.params)
            LOGGER.info(f"Task {message.task_name} completed successfully")
        
        except Exception as e:
            LOGGER.error(f"Error executing task {message.task_name}: {e}")
            
            # Use message.max_retries instead of worker's max_retries
            if message.retry_count < message.max_retries:
                message.retry_count += 1
                LOGGER.info(f"Retry {message.retry_count}/{message.max_retries}")
                
                # Re-enqueue the same message with updated retry count
                self.queue.enqueue(message)  # New method needed
            else:
                LOGGER.error(f"Task {message.task_name} failed after {message.max_retries} retries")