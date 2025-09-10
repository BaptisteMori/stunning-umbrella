from typing import Optional, Type
from task_framework.core.task import Task, TaskStatus
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue
from task_framework.queue.queue_type.redis_queue import RedisQueue
from task_framework.results.backend import ResultBackend
from task_framework.results.redis_backend import RedisResultBackend
from task_framework.monitoring.metrics import MetricsCollector, SimpleMetricsCollector
from task_framework.worker.worker import Worker
from task_framework.worker.orchestrator import Orchestrator

class TaskFramework:
    """
    Point d'entrée principal du framework.
    """
    
    _instance: Optional['TaskFramework'] = None
    
    def __init__(self):
        self.registry = TaskRegistry()
        self.queue: Optional[Queue] = None
        self.result_backend: Optional[ResultBackend] = None
        self.metrics: Optional[MetricsCollector] = None
        
    @classmethod
    def setup(cls,
              queue: Optional[Queue] = None,
              result_backend: Optional[ResultBackend] = None,
              metrics: Optional[MetricsCollector] = None,
              redis_url: str = "redis://localhost:6379") -> 'TaskFramework':
        """
        Configure le framework globalement.
        
        Usage:
            framework = TaskFramework.setup(
                redis_url="redis://localhost:6379"
            )
        """
        if not cls._instance:
            cls._instance = cls()
        
        instance = cls._instance
        
        # Configurer la queue
        instance.queue = queue or RedisQueue(redis_url)
        
        # Configurer le backend de résultats
        instance.result_backend = result_backend or RedisResultBackend(redis_url)
        
        # Configurer les métriques
        instance.metrics = metrics or SimpleMetricsCollector()
        
        # Injecter dans Task
        Task._queue = instance.queue
        Task._registry = instance.registry
        Task._result_backend = instance.result_backend
        
        return instance
    
    def register(self, task_class: Type[Task], name: Optional[str] = None):
        """Enregistre une tâche."""
        task_name = name or task_class.get_name()
        self.registry.register(task_name, task_class)
        return task_class
    
    def task(self, name: Optional[str] = None):
        """
        Décorateur pour enregistrer une tâche.
        
        Usage:
            @framework.task()
            class MyTask(Task):
                def run(self):
                    return "result"
        """
        def decorator(task_class: Type[Task]):
            self.register(task_class, name)
            return task_class
        return decorator
    
    def create_worker(self, worker_id: Optional[str] = None) -> Worker:
        """Crée un worker configuré."""
        return Worker(
            queue=self.queue,
            registry=self.registry,
            result_backend=self.result_backend,
            metrics=self.metrics,
            worker_id=worker_id
        )
    
    def create_orchestrator(self, num_workers: int = 4) -> Orchestrator:
        """Crée un orchestrateur multi-process."""
        return Orchestrator(
            queue=self.queue,
            registry=self.registry,
            result_backend=self.result_backend,
            metrics=self.metrics,
            num_workers=num_workers
        )
    
    @classmethod
    def get_instance(cls) -> 'TaskFramework':
        """Récupère l'instance configurée."""
        if not cls._instance:
            raise RuntimeError("TaskFramework not configured. Call setup() first.")
        return cls._instance

# Exports simplifiés
__all__ = [
    'Task',
    'TaskStatus', 
    'TaskFramework',
    'Worker',
    'Orchestrator'
]