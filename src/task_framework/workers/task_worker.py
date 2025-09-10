import os

from task_framework.workers.worker import WorkerStatus, Worker
from task_framework.core.executor import TaskExecutor
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue
from task_framework.core.task import TaskMessage
from task_framework.observability.metrics import track_task_execution, metrics, StructuredLogger


class TaskWorker(Worker):
    """Worker to process tasks from a queue with observability."""    
    
    def __init__(self, 
                 queue: Queue,
                 registry: TaskRegistry,
                 max_retries: int = 3,
                 worker_id: str = None):
        super().__init__(queue, registry)

        self.executor = TaskExecutor(self.registry)
        self.max_retries = max_retries
        self.worker_id = worker_id or f"worker-{os.getpid()}"
        self.logger = StructuredLogger(f"worker.{self.worker_id}")
   
    def run(self):
        """Worker main loop with metrics tracking."""
        self.logger.info("Worker started", worker_id=self.worker_id, registry_tasks=len(self.registry.list_tasks()))
        
        metrics.set_gauge("active_workers", 1.0, {"worker_id": self.worker_id})
        
        while self.running:
            self._update_status(WorkerStatus.IDLE)
            try:
                message = self.queue.dequeue(timeout=1)
                if message:
                    self._update_status(WorkerStatus.RUNNING)
                    self._process_message(message)
                else:
                    metrics.increment_counter("queue_polls_empty", {"worker_id": self.worker_id})
            
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, stopping worker", worker_id=self.worker_id)
                break
            except Exception as e:
                self._update_status(WorkerStatus.FAILED)
                self.logger.error("Error in worker main loop", worker_id=self.worker_id, error=str(e))
                metrics.increment_counter("worker_errors", {"worker_id": self.worker_id, "error_type": "main_loop"})
        
        metrics.set_gauge("active_workers", 0.0, {"worker_id": self.worker_id})
        self._update_status(WorkerStatus.STOPPED)
        self.logger.info("Worker stopped", worker_id=self.worker_id)
    
    def _process_message(self, message: TaskMessage):
        """Process a message from the queue with proper retry handling and metrics."""
        
        with track_task_execution(message.task_name, message.id, self.worker_id) as task_metrics:
            try:
                self.logger.info("Processing task started", 
                               task_name=message.task_name, 
                               task_id=message.id,
                               retry_count=message.retry_count,
                               worker_id=self.worker_id)
                
                result = self.executor.execute_task(message.task_name, message.params)
                
                self.logger.info("Task completed successfully", 
                               task_name=message.task_name,
                               task_id=message.id,
                               duration=task_metrics.duration,
                               worker_id=self.worker_id)
                
                metrics.increment_counter("tasks_processed", {
                    "task_name": message.task_name,
                    "status": "success",
                    "worker_id": self.worker_id
                })
                
                return result
            
            except Exception as e:
                self.logger.error("Task execution failed", 
                                task_name=message.task_name,
                                task_id=message.id,
                                error=str(e),
                                retry_count=message.retry_count,
                                worker_id=self.worker_id)
                
                if message.retry_count < message.max_retries:
                    message.retry_count += 1
                    
                    self.logger.info("Retrying task", 
                                   task_name=message.task_name,
                                   task_id=message.id,
                                   retry_attempt=message.retry_count,
                                   max_retries=message.max_retries)
                    
                    try:
                        self.queue.enqueue(
                            task_name=message.task_name,
                            params=message.params,
                            priority=message.priority
                        )
                        
                        metrics.increment_counter("tasks_retried", {
                            "task_name": message.task_name,
                            "worker_id": self.worker_id
                        })
                        
                    except Exception as retry_error:
                        self.logger.error("Failed to re-queue task", 
                                        task_name=message.task_name,
                                        task_id=message.id,
                                        error=str(retry_error))
                        
                        metrics.increment_counter("requeue_failures", {
                            "task_name": message.task_name,
                            "worker_id": self.worker_id
                        })
                else:
                    self.logger.error("Task failed permanently", 
                                    task_name=message.task_name,
                                    task_id=message.id,
                                    total_retries=message.max_retries,
                                    worker_id=self.worker_id)
                    
                    metrics.increment_counter("tasks_failed_permanently", {
                        "task_name": message.task_name,
                        "worker_id": self.worker_id
                    })
                    
                    # TODO: Envoyer vers une dead letter queue
                    self._handle_permanent_failure(message)
                
                # Re-raise l'exception pour que le context manager la track
                raise
    
    def _handle_permanent_failure(self, message: TaskMessage):
        """Handle tasks that have failed permanently."""
        # TODO: Implement dead letter queue
        # TODO: Send notification/alert
        # TODO: Store failure details for analysis
        pass
    
    def get_stats(self) -> dict:
        """Get worker-specific statistics."""
        return {
            "worker_id": self.worker_id,
            "status": self.status.value,
            "registered_tasks": list(self.registry.list_tasks().keys()),
            **metrics.get_stats()
        }