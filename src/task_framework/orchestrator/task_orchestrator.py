import time
import signal
import logging
import multiprocessing as mp
from dataclasses import dataclass
from enum import Enum

from queue.queue import TaskQueue
from core.registry import TaskRegistry
from workers.worker import Worker
from workers.task_worker import TaskWorker


LOGGER = logging.getLogger(__name__)


class ProcessStatus(Enum):
    """Process execution states for worker management."""
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass
class WorkerProcess:
    """
    Container for worker process metadata and management.
    
    Tracks individual worker processes with their execution state,
    process handle, and restart capabilities for fault tolerance.
    
    Attributes:
        id (int): Unique identifier for the worker process
        process (Optional[mp.Process]): Multiprocessing handle for the worker
        status (ProcessStatus): Current execution state of the worker
        restart_count (int): Number of times this worker has been restarted
        max_restarts (int): Maximum allowed restarts before permanent failure
    """
    id: int
    process: mp.Process|None = None
    status: ProcessStatus = ProcessStatus.STARTING
    restart_count: int = 0
    max_restarts: int = 3


class TaskOrchestrator:
    """
    Multi-process task orchestrator for managing distributed worker execution.
    
    This orchestrator manages multiple worker processes that consume tasks from
    a shared Redis queue, providing fault tolerance through automatic restart
    and graceful shutdown capabilities.
    
    Attributes:
        worker_count (int): Number of worker processes to maintain
        queue (RedisTaskQueue): Shared task queue for worker communication
        registry (TaskRegistry): Task registry containing available task implementations
        workers (dict[int, WorkerProcess]): Active worker process tracking
        running (bool): Orchestrator execution state flag
    """
    
    def __init__(self, 
                 worker_count: int,
                 queue: TaskQueue,
                 registry: TaskRegistry,
                 worker_class: Worker = TaskWorker,
        ):
        """
        Initialize orchestrator with worker configuration and task discovery.
        
        Args:
            worker_count (int): Number of worker processes to spawn and maintain
            redis_url (str, optional): Redis connection string. Defaults to localhost
            queue_name (str, optional): Queue identifier for task distribution
            tasks_path (Optional[str], optional): Path for automatic task discovery
        
        Raises:
            ValueError: If worker_count is less than 1
            ConnectionError: If Redis connection cannot be established
        """
        if worker_count < 1:
            raise ValueError("Worker count must be at least 1")

        if not issubclass(worker_class, Worker):
            raise ValueError("worker_class must be a subclass of Worker")

        self.worker_count: int = worker_count
        self.queue: TaskQueue = queue
        self.registry: TaskRegistry = registry
        self.worker_class: Worker = worker_class
        
        self.workers: dict[int, WorkerProcess] = {}
        self.running: bool = False
        
        # Graceful shutdown signal handling
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
    
    def _create_worker_process(self, worker_id: int) -> mp.Process:
        """
        Create a new worker process with isolated task execution environment.
        
        Args:
            worker_id (int): Unique identifier for the worker process
        
        Returns:
            mp.Process: Configured worker process ready for execution
        """
        def worker_target():
            # Initialize and run worker
            worker: Worker = self.worker_class(self.queue, self.registry)
            worker.run()
        
        process: mp.Process = mp.Process(
            target=worker_target,
            name=f"TaskWorker-{worker_id}"
        )
        return process
    
    def _start_worker(self, worker_id: int) -> bool:
        """
        Start or restart a specific worker process.
        
        Args:
            worker_id (int): Identifier of worker to start
        
        Returns:
            bool: True if worker started successfully, False otherwise
        """
        if worker_id not in self.workers:
            self.workers[worker_id] = WorkerProcess(id=worker_id)
        
        worker: WorkerProcess = self.workers[worker_id]
        
        # Check restart limits
        if worker.restart_count >= worker.max_restarts:
            LOGGER.error(f"Worker {worker_id} exceeded max restarts ({worker.max_restarts})")
            worker.status = ProcessStatus.FAILED
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
            
            LOGGER.info(f"Started worker {worker_id} (restart #{worker.restart_count})")
            return True
            
        except Exception as e:
            LOGGER.exception("Failed to start worker: %s", worker_id)
            worker.status = ProcessStatus.FAILED
            return False
    
    def _monitor_workers(self) -> None:
        """
        Monitor worker processes and restart failed workers automatically.
        
        Performs health checks on all managed worker processes and initiates
        restart procedures for any workers that have terminated unexpectedly.
        """
        for worker_id, worker in list(self.workers.items()):
            if worker.process is None:
                continue
            
            # Check if process is still running
            if not worker.process.is_alive():
                exit_code = worker.process.exitcode
                
                if worker.status != ProcessStatus.STOPPING:
                    LOGGER.warning(f"Worker {worker_id} died unexpectedly (exit code: {exit_code})")
                    
                    # Attempt restart if within limits
                    if worker.restart_count < worker.max_restarts:
                        LOGGER.info(f"Restarting worker {worker_id}")
                        self._start_worker(worker_id)
                    else:
                        worker.status = ProcessStatus.FAILED
                        LOGGER.error(f"Worker {worker_id} permanently failed")
                else:
                    worker.status = ProcessStatus.STOPPED
                    LOGGER.info(f"Worker {worker_id} stopped gracefully")
    
    def start(self) -> None:
        """
        Start the orchestrator and all worker processes.
        
        Initializes all configured worker processes and begins the monitoring
        loop for fault tolerance and automatic recovery.
        
        Raises:
            RuntimeError: If orchestrator is already running
        """
        if self.running:
            raise RuntimeError("Orchestrator is already running")
        
        self.running = True
        LOGGER.info(f"Starting orchestrator with {self.worker_count} workers")
        
        # Start all worker processes
        for worker_id in range(self.worker_count):
            self._start_worker(worker_id)
        
        # Main monitoring loop
        try:
            while self.running:
                self._monitor_workers()
                time.sleep(1)  # Check every second
                
        except KeyboardInterrupt:
            LOGGER.info("Received interrupt signal")
        finally:
            self._shutdown()
    
    def _shutdown_handler(self, signum: int, frame) -> None:
        """
        Handle shutdown signals for graceful orchestrator termination.
        
        Args:
            signum (int): Signal number received
            frame: Current stack frame
        """
        LOGGER.info(f"Received shutdown signal {signum}")
        self.running = False
    
    def _shutdown(self) -> None:
        """
        Perform graceful shutdown of all worker processes.
        
        Terminates all active worker processes with timeout-based cleanup
        to ensure proper resource deallocation.
        """
        LOGGER.info("Shutting down orchestrator")
        
        # Signal all workers to stop
        for worker in self.workers.values():
            if worker.process and worker.process.is_alive():
                worker.status = ProcessStatus.STOPPING
                worker.process.terminate()
        
        # Wait for graceful shutdown with timeout
        for worker in self.workers.values():
            if worker.process:
                worker.process.join(timeout=10)
                if worker.process.is_alive():
                    LOGGER.warning(f"Force killing worker {worker.id}")
                    worker.process.kill()
                    worker.process.join()
        
        LOGGER.info("All workers stopped")
    
    def get_worker_status(self) -> dict[int, str]:
        """
        Retrieve current status of all managed worker processes.
        
        Returns:
            dict[int, str]: Mapping of worker IDs to their current status
        """
        return {
            worker_id: worker.status.value 
            for worker_id, worker in self.workers.items()
        }
    
    def get_stats(self) -> dict[str, int]:
        """
        Generate orchestrator statistics for monitoring and debugging.
        
        Returns:
            dict[str, int]: Statistical information about worker states
        """
        status_counts = {}
        for status in ProcessStatus:
            status_counts[status.value] = sum(
                1 for w in self.workers.values() 
                if w.status == status
            )
        
        return {
            "total_workers": len(self.workers),
            "registered_tasks": len(self.registry.list_tasks()),
            **status_counts
        }