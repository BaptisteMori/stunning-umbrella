import time
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, Any
from datetime import datetime
import threading


@dataclass
class TaskMetrics:
    """Basic task execution metrics."""
    task_name: str
    start_time: float
    end_time: float = None
    status: str = "running"
    error: str = None
    retry_count: int = 0
    worker_id: str = None
    
    @property
    def duration(self) -> float:
        """Calculate task duration in seconds."""
        if self.end_time:
            return self.end_time - self.start_time
        return None
    
    def to_dict(self) -> dict:
        """Export metrics as dictionary for logging."""
        return {
            "task_name": self.task_name,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "duration": self.duration,
            "status": self.status,
            "error": self.error,
            "retry_count": self.retry_count,
            "worker_id": self.worker_id,
        }


class SimpleMetricsCollector:
    """Simple in-memory metrics collector (production should use Prometheus)."""
    
    def __init__(self):
        self._lock = threading.RLock()
        self._counters: Dict[str, int] = defaultdict(int)
        self._histograms: Dict[str, list[float]] = defaultdict(list)
        self._gauges: Dict[str, float] = {}
        self._active_tasks: Dict[str, TaskMetrics] = {}
    
    def increment_counter(self, name: str, labels: Dict[str, str] = None, value: int = 1):
        """Increment a counter metric."""
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a value in a histogram."""
        with self._lock:
            key = self._make_key(name, labels)
            self._histograms[key].append(value)
            # Keep only last 1000 values to prevent memory leaks
            if len(self._histograms[key]) > 1000:
                self._histograms[key] = self._histograms[key][-1000:]
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric value."""
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value
    
    def start_task(self, task_id: str, task_name: str, worker_id: str = None) -> TaskMetrics:
        """Start tracking a task."""
        metrics = TaskMetrics(
            task_name=task_name,
            start_time=time.time(),
            worker_id=worker_id
        )
        with self._lock:
            self._active_tasks[task_id] = metrics
        return metrics
    
    def finish_task(self, task_id: str, status: str = "completed", error: str = None):
        """Finish tracking a task."""
        with self._lock:
            if task_id in self._active_tasks:
                metrics = self._active_tasks[task_id]
                metrics.end_time = time.time()
                metrics.status = status
                metrics.error = error
                
                # Record metrics
                labels = {"task_name": metrics.task_name, "status": status}
                self.increment_counter("tasks_total", labels)
                
                if metrics.duration:
                    self.record_histogram("task_duration_seconds", metrics.duration, 
                                        {"task_name": metrics.task_name})
                
                # Remove from active tasks
                completed_metrics = self._active_tasks.pop(task_id)
                return completed_metrics
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        with self._lock:
            stats = {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "active_tasks": len(self._active_tasks),
                "histogram_stats": {}
            }
            
            # Calculate histogram statistics
            for key, values in self._histograms.items():
                if values:
                    stats["histogram_stats"][key] = {
                        "count": len(values),
                        "min": min(values),
                        "max": max(values),
                        "avg": sum(values) / len(values),
                        "p95": self._percentile(values, 95),
                        "p99": self._percentile(values, 99)
                    }
            
            return stats
    
    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Create a key from metric name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def _percentile(self, values: list[float], p: int) -> float:
        """Calculate percentile."""
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]


# Global metrics collector instance
metrics = SimpleMetricsCollector()


@contextmanager
def track_task_execution(task_name: str, task_id: str = None, worker_id: str = None):
    """Context manager to track task execution."""
    task_id = task_id or f"{task_name}_{int(time.time() * 1000)}"
    
    task_metrics = metrics.start_task(task_id, task_name, worker_id)
    logger = logging.getLogger(f"task.{task_name}")
    
    try:
        logger.info("Task started", extra={
            "task_name": task_name,
            "task_id": task_id,
            "worker_id": worker_id
        })
        
        yield task_metrics
        
        # Task completed successfully
        completed = metrics.finish_task(task_id, "completed")
        if completed:
            logger.info("Task completed", extra=completed.to_dict())
    
    except Exception as e:
        # Task failed
        error_msg = str(e)
        completed = metrics.finish_task(task_id, "failed", error_msg)
        if completed:
            logger.error("Task failed", extra=completed.to_dict())
        raise


class StructuredLogger:
    """Structured logger that works with or without structlog."""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        try:
            import structlog
            self.structured = True
            self.log = structlog.get_logger(name)
        except ImportError:
            self.structured = False
            self.log = self.logger
    
    def info(self, message: str, **kwargs):
        if self.structured:
            self.log.info(message, **kwargs)
        else:
            extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            self.logger.info(f"{message} {extra_str}".strip())
    
    def error(self, message: str, **kwargs):
        if self.structured:
            self.log.error(message, **kwargs)
        else:
            extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            self.logger.error(f"{message} {extra_str}".strip())
    
    def warning(self, message: str, **kwargs):
        if self.structured:
            self.log.warning(message, **kwargs)
        else:
            extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            self.logger.warning(f"{message} {extra_str}".strip())
    
    def debug(self, message: str, **kwargs):
        if self.structured:
            self.log.debug(message, **kwargs)
        else:
            extra_str = " ".join(f"{k}={v}" for k, v in kwargs.items()) if kwargs else ""
            self.logger.debug(f"{message} {extra_str}".strip())