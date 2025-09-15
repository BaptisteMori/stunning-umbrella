import time
import signal
import multiprocessing as mp
from dataclasses import dataclass
from enum import Enum
from typing import Type, Optional

from task_framework.queue.queue import Queue
from task_framework.core.registry import TaskRegistry
from task_framework.workers.worker import Worker
from task_framework.workers.task_worker import TaskWorker
from task_framework.config.settings import TaskFrameworkConfig
from task_framework.monitoring.metrics import MetricsConnector, MetricType

class ProcessStatus(Enum):
    """Process execution states for worker management."""
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class WorkerProcess:
    """Container for worker process metadata and management."""
    id: int
    process: Optional[mp.Process] = None
    status: ProcessStatus = ProcessStatus.STARTING
    restart_count: int = 0
    max_restarts: int = 3
    worker_id: str = ""
    
    def __post_init__(self):
        if not self.worker_id:
            self.worker_id = f"worker-{self.id}"


class TaskOrchestrator:
    """
    Multi-process task orchestrator with configuration and observability.
    
    This orchestrator manages multiple worker processes that consume tasks from
    a shared queue, providing fault tolerance through automatic restart
    and graceful shutdown capabilities.
    """
    
    def __init__(self, 
                 queue: Queue,
                 registry: TaskRegistry,
                 config: TaskFrameworkConfig = None,
                 worker_class: Type[Worker] = TaskWorker):
        """
        Initialize orchestrator with configuration.
        
        Args:
            queue: Task queue for worker communication
            registry: Task registry containing available task implementations
            config: Framework configuration (uses default if None)
            worker_class: Worker class to instantiate
        """
        self.config = config or TaskFrameworkConfig.from_env()
        
        if self.config.worker.count < 1:
            raise ValueError("Worker count must be at least 1")

        if not issubclass(worker_class, Worker):
            raise ValueError("worker_class must be a subclass of Worker")

        self.queue: Queue = queue
        self.registry: TaskRegistry = registry
        self.worker_class: Type[Worker] = worker_class
        
        self.workers: dict[int, WorkerProcess] = {}
        self.running: bool = False
        
        self.logger = StructuredLogger("orchestrator")
        
        self.config.setup_logging()
        
        # Graceful shutdown signal handling
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        
        prod_issues = self.config.validate_production()
        if prod_issues:
            self.logger.warning("Production configuration issues found", issues=prod_issues)
    
    def _create_worker_process(self, worker_id: int) -> mp.Process:
        """Create a new worker process with isolated task execution environment."""
        def worker_target():
            # Initialize and run worker with config
            worker: Worker = self.worker_class(
                self.queue, 
                self.registry,
                max_retries=self.config.worker.max_retries,
                worker_id=f"worker-{worker_id}"
            )
            worker.run()
        
        process: mp.Process = mp.Process(
            target=worker_target,
            name=f"TaskWorker-{worker_id}"
        )
        return process
    
    def _start_worker(self, worker_id: int) -> bool:
        """Start or restart a specific worker process."""
        if worker_id not in self.workers:
            self.workers[worker_id] = WorkerProcess(
                id=worker_id,
                max_restarts=self.config.worker.max_retries
            )
        
        worker: WorkerProcess = self.workers[worker_id]
        
        # Check restart limits
        if worker.restart_count >= worker.max_restarts:
            self.logger.error("Worker exceeded max restarts", 
                            worker_id=worker.worker_id,
                            max_restarts=worker.max_restarts)
            worker.status = ProcessStatus.FAILED
            
            metrics.increment_counter("worker_permanent_failures", {"worker_id": worker.worker_id})
            return False
        
        try:
            # Clean up existing process if necessary
            if worker.process and worker.process.is_alive():
                worker.process.terminate()
                worker.process.join(timeout=5)
            
            # Create and start new process
            worker.process = self._create_worker_process(worker_id)
            worker.process.start()
            worker.status = ProcessStatus.RUNNING
            worker.restart_count += 1
            
            metrics.increment_counter("worker_starts", {"worker_id": worker.worker_id})
            
            self.logger.info("Started worker", 
                           worker_id=worker.worker_id,
                           restart_count=worker.restart_count,
                           pid=worker.process.pid)
            return True
            
        except Exception as e:
            self.logger.error("Failed to start worker", 
                            worker_id=worker.worker_id,
                            error=str(e))
            worker.status = ProcessStatus.FAILED
            
            metrics.increment_counter("worker_start_failures", {"worker_id": worker.worker_id})
            return False
    
    def _monitor_workers(self) -> None:
        """Monitor worker processes and restart failed workers automatically."""
        active_workers = 0
        
        for worker_id, worker in list(self.workers.items()):
            if worker.process is None:
                continue
            
            # Check if process is still running
            if not worker.process.is_alive():
                exit_code = worker.process.exitcode
                
                if worker.status != ProcessStatus.STOPPING:
                    self.logger.warning("Worker died unexpectedly", 
                                      worker_id=worker.worker_id,
                                      exit_code=exit_code)
                    
                    metrics.increment_counter("worker_crashes", {"worker_id": worker.worker_id})
                    
                    # Attempt restart if within limits
                    if worker.restart_count < worker.max_restarts:
                        self.logger.info("Restarting worker", worker_id=worker.worker_id)
                        if self._start_worker(worker_id):
                            active_workers += 1
                    else:
                        worker.status = ProcessStatus.FAILED
                        self.logger.error("Worker permanently failed", worker_id=worker.worker_id)
                else:
                    worker.status = ProcessStatus.STOPPED
                    self.logger.info("Worker stopped gracefully", worker_id=worker.worker_id)
            else:
                active_workers += 1
        
        metrics.set_gauge("orchestrator_active_workers", float(active_workers))
        
        try:
            queue_stats = self.queue.get_queue_size()
            for stat_name, value in queue_stats.items():
                metrics.set_gauge(f"queue_size_{stat_name}", float(value))
        except Exception as e:
            self.logger.error("Failed to get queue stats", error=str(e))
    
    def start(self) -> None:
        """Start the orchestrator and all worker processes."""
        if self.running:
            raise RuntimeError("Orchestrator is already running")
        
        self.running = True
        
        self.logger.info("Starting orchestrator", 
                        worker_count=self.config.worker.count,
                        config=self.config.to_dict())
        
        metrics.increment_counter("orchestrator_starts")
        
        # Start all worker processes
        for worker_id in range(self.config.worker.count):
            self._start_worker(worker_id)
        
        # Main monitoring loop
        try:
            while self.running:
                self._monitor_workers()
                time.sleep(1)  # Check every second
                
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            self._shutdown()
    
    def _shutdown_handler(self, signum: int, frame) -> None:
        """Handle shutdown signals for graceful orchestrator termination."""
        self.logger.info("Received shutdown signal", signal=signum)
        self.running = False
    
    def _shutdown(self) -> None:
        """Perform graceful shutdown of all worker processes."""
        self.logger.info("Shutting down orchestrator")
        
        metrics.increment_counter("orchestrator_shutdowns")
        
        # Signal all workers to stop
        for worker in self.workers.values():
            if worker.process and worker.process.is_alive():
                worker.status = ProcessStatus.STOPPING
                worker.process.terminate()
        
        # Wait for graceful shutdown with timeout from config
        shutdown_timeout = getattr(self.config.worker, 'shutdown_timeout', 10)
        for worker in self.workers.values():
            if worker.process:
                worker.process.join(timeout=shutdown_timeout)
                if worker.process.is_alive():
                    self.logger.warning("Force killing worker", worker_id=worker.worker_id)
                    worker.process.kill()
                    worker.process.join()
        
        self.logger.info("All workers stopped")
    
    def get_worker_status(self) -> dict[int, str]:
        """Retrieve current status of all managed worker processes."""
        return {
            worker_id: worker.status.value 
            for worker_id, worker in self.workers.items()
        }
    
    def get_stats(self) -> dict[str, any]:
        """Generate orchestrator statistics for monitoring and debugging."""
        status_counts = {}
        for status in ProcessStatus:
            status_counts[status.value] = sum(
                1 for w in self.workers.values() 
                if w.status == status
            )
        
        base_stats = {
            "total_workers": len(self.workers),
            "registered_tasks": len(self.registry.list_tasks()),
            "config": {
                "environment": self.config.environment,
                "worker_count": self.config.worker.count,
                "max_retries": self.config.worker.max_retries,
            },
            "worker_status_counts": status_counts,
            "worker_details": {
                worker.worker_id: {
                    "status": worker.status.value,
                    "restart_count": worker.restart_count,
                    "pid": worker.process.pid if worker.process else None
                }
                for worker in self.workers.values()
            }
        }
        
        global_stats = metrics.get_stats()
        base_stats.update(global_stats)
        
        return base_stats
    
    def healthcheck(self) -> dict[str, any]:
        """Perform health check and return status."""
        stats = self.get_stats()
        
        healthy_workers = stats["worker_status_counts"].get("running", 0)
        total_workers = stats["total_workers"]
        
        # Consider healthy if at least 50% of workers are running
        health_threshold = max(1, total_workers // 2)
        is_healthy = healthy_workers >= health_threshold
        
        return {
            "healthy": is_healthy,
            "status": "healthy" if is_healthy else "degraded",
            "workers": {
                "healthy": healthy_workers,
                "total": total_workers,
                "threshold": health_threshold
            },
            "uptime": time.time() - getattr(self, '_start_time', time.time()),
            "queue_size": stats.get("gauges", {}).get("queue_size_total", 0)
        }