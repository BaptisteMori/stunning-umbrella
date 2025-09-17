import os
import logging
import time
import random
from typing import Any

from task_framework.workers.worker import WorkerStatus, Worker
from task_framework.core.executor import TaskExecutor
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue
from task_framework.core.task_status import TaskStatus
from task_framework.core.message import ResultMessage, TaskMessage
from task_framework.monitoring.metrics import MetricsConnector, NoOpMetricsConnector


LOGGER: logging.Logger = logging.getLogger(__name__)


class TaskWorker(Worker):
    """Worker to process tasks from a queue with observability."""    
    
    def __init__(self, 
                 task_queue: Queue,
                 registry: TaskRegistry,
                 result_queue: Queue = None,
                 metric: MetricsConnector = NoOpMetricsConnector(),
                 max_retries: int = 3,
                 worker_id: str = None):
        super().__init__(
            task_queue=task_queue, 
            result_queue=result_queue, 
            registry=registry,
            metric=metric
        )

        self.executor = TaskExecutor(self.registry)
        self.max_retries = max_retries
        self.worker_id = worker_id or f"worker-{os.getpid()}"
   
    def run(self):
        """Worker main loop with metrics tracking."""
        LOGGER.info("Worker started", extra={"worker_id": self.worker_id, "registry_tasks":len(self.registry.list_tasks())})
        
        self.metric.gauge('worker.status', 1, {'worker_id': self.worker_id})

        while self.running:
            self._update_status(WorkerStatus.IDLE)
            if random.random() < 0.1:  # 10% of the time
                stats = self.task_queue.get_queue_size()
                self.metric.gauge('queue.size', stats.get('total', 0))
            
            try:
                message = self.task_queue.dequeue(timeout=1, message_type=TaskMessage)
                if message:                    
                    self._process_message(message)
                    
            except KeyboardInterrupt:
                LOGGER.info("Received interrupt signal, stopping worker", extra={"worker_id":self.worker_id})
                break
            except Exception as e:
                self._update_status(WorkerStatus.FAILED)
                LOGGER.error(f"Error in worker main loop, {e}", extra= {"worker_id":self.worker_id, "error":str(e)})
                self.metric.counter(
                    'worker.errors', 
                    tags={
                        'task_name': message.task_name,
                        'worker_id': self.worker_id
                    }
                )
        
        self.metric.gauge('worker.status', 0, tags={'worker': self.worker_id})
        self._update_status(WorkerStatus.STOPPED)
        LOGGER.info("Worker stopped", extra={"worker_id":self.worker_id})
    
    def _process_message(self, message: TaskMessage):
        """Process a message from the queue with proper retry handling and metrics."""
        start_time = time.time()
        self._update_status(WorkerStatus.RUNNING)

        self.metric.counter('task.started', tags={
            'task': message.task_name,
            'worker': self.worker_id
        })

        try:
            LOGGER.info("Processing task started", 
                extra={
                    "task_name":message.task_name, 
                    "task_id":message.task_id,
                    "retry_count":message.retry_count,
                    "worker_id":self.worker_id
                }
            )
            
            result = self.executor.execute_task(message.task_name, message.params)
            
            end_time = time.time()
            self._handle_success(
                message=message, 
                result=result,
                start_time = start_time,
                end_time = end_time    
            )            
        
        except Exception as e:
            end_time = time.time()
            self._handle_failure(
                message=message,
                error=e,
                start_time=start_time,
                end_time=end_time
            )
            raise
    
    def _handle_success(self, message: TaskMessage, result: Any, start_time: int, end_time: int):
        """Process the success of a task."""
        execution_time = end_time - start_time
        self.metric.counter('task.success', tags={'task': message.task_name})
        self.metric.timing('task.duration', execution_time, tags={'task': message.task_name})
        
        LOGGER.info(f"Task {message.task_id}:{message.task_name} completed successfully", extra={
                        "task_name":message.task_name,
                        "task_id":message.task_id,
                        "duration":execution_time,
                        "worker_id":self.worker_id
                    })
        
        # Save and send the result
        if self.result_queue:
            message.status = TaskStatus.COMPLETED.value
            task_result: ResultMessage = ResultMessage.from_message(message=message)
            task_result.result = result # TODO make the serialization
            
            task_result.started_at = start_time
            task_result.completed_at = end_time
            task_result.execution_time = execution_time

            task_result.worker_id = self.worker_id

            self.result_queue.enqueue(message=task_result)
        
    
    def _handle_failure(self, message: TaskMessage, error: Exception, start_time: int, end_time: int):
        """Gère l'échec d'une tâche avec retry."""
        execution_time = end_time - start_time
        
        LOGGER.error(f"Task {message.task_id}:{message.task_name} execution failed", extra={
            "task_name": message.task_name,
            "task_id": message.task_id,
            "error": str(error),
            "retry_count": message.retry_count,
            "worker_id": self.worker_id
        })
        LOGGER.exception(error)

        # check if the task can retry
        if message.retry_count < message.max_retries:
            message.retry_count += 1
            
            LOGGER.info(
                f"Retrying task {message.task_id}:{message.task_name} (attempt {message.retry_count}/{message.max_retries})",
                extra={
                    "task_name":message.task_name,
                    "task_id":message.task_id,
                    "error":str(error),
                    "retry_count":message.retry_count,
                    "worker_id":self.worker_id
                }
            )

            # Enqueue the message 
            message.status = TaskStatus.RETRY.value
            self.task_queue.enqueue(message)
            
            self.metric.counter(
                'tasks.retry', 
                tags={
                    'task': message.task_name,
                    'worker': self.worker_id
                }
            )
        else:
            # Definitive failure    
            if self.result_queue:
                message.status = TaskStatus.FAILED.value
                task_result: ResultMessage = ResultMessage.from_message(message=message)
                task_result.error = str(error)
                task_result.traceback = str(error.__traceback__)

                task_result.started_at = start_time
                task_result.completed_at = end_time
                task_result.execution_time = execution_time

                task_result.worker_id = self.worker_id

                self.result_queue.enqueue(task_result)

            self.metric.counter(
                'tasks.failed', 
                tags={
                    'task': message.task_name,
                    'worker': self.worker_id
                }
            )
