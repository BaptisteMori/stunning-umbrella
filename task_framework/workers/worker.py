import signal
import logging
from enum import Enum
from ..core.executor import TaskExecutor
from ..core.registry import TaskRegistry
from ..queue.redis_queue import RedisTaskQueue


LOGGER: logging.Logger = logging.getLogger(__name__)


class TaskWorker:
    """Worker to process tasks from a queue."""
    
    def __init__(self, 
                 queue: RedisTaskQueue,
                 registry: TaskRegistry|None = None,
                 max_retries: int = 3):
        self.queue = queue
        self.executor = TaskExecutor(registry or TaskRegistry())
        self.max_retries = max_retries
        self.running = True
        
        # Graceful management of the stop
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        """Graceful shutdown handler"""
        LOGGER.info(f"\nShutdown of the worker (signal {signum})")
        self.running = False
    
    def run(self):
        """Worker main loop"""
        LOGGER.info("Worker started, Waiting for a task...")
        
        while self.running:
            try:
                message = self.queue.dequeue(timeout=1)
                if message:
                    self._process_message(message)
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                LOGGER.error(f"Error in the main loop of the worker: {e}")
        
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
