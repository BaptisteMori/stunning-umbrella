import inspect
from typing import Optional, Type, Callable, Union
from functools import wraps

from task_framework.core.message import ResultMessage
from task_framework.core.task import Task, TaskStatus
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue
from task_framework.monitoring.metrics import MetricsConnector, NoOpMetricsConnector
from task_framework.workers.task_worker import TaskWorker
# from task_framework.worker.orchestrator import Orchestrator

class TaskFramework:
    """
    Entry point, set a TaskFramework and use it everywhere

    Usages:
    task_framework: TaskFramework = TaskFramework(
        registry, task_queue, result_queue
    )

    @task_framework.task()
    def myTask(var1: str, var2: int = 13)
        print(var1)
        return var2

    @framework.task()
    class MyTask(Task):
        def run(self):
            return "result"
    """
    
    _instance: Optional['TaskFramework'] = None
    
    def __init__(
            self, 
            task_queue: Queue, 
            result_queue: Queue, 
            registry: TaskRegistry = TaskRegistry(),
            metrics: MetricsConnector = None
        ):
        
        self.task_queue: Optional[Queue] = task_queue
        self.result_queue: Optional[Queue] = result_queue
        self.registry = registry
        self.metrics: Optional[MetricsConnector] = metrics
        
    @classmethod
    def setup(cls,
              task_queue: Optional[Queue],
              result_queue: Optional[Queue] = None,
              metrics: Optional[MetricsConnector] = None) -> 'TaskFramework':
        """
        Configure le framework globalement.
        
        Usage:
            framework = TaskFramework.setup(
                redis_url="redis://localhost:6379"
            )
        """
        if not cls._instance:
            cls._instance = cls(task_queue, result_queue, metrics=metrics)
        
        instance = cls._instance
        
        instance.task_queue = task_queue
        instance.result_queue = result_queue
        
        instance.metrics = metrics or NoOpMetricsConnector()

        # Inject in Task
        Task._task_queue = instance.task_queue
        Task._registry = instance.registry
        Task._result_queue = instance.result_queue
        
        return instance
    
    def register(self, task_class: Type[Task], name: Optional[str] = None):
        """Enregistre une tâche."""
        task_name = name or task_class.get_name()
        self.registry.register(task_name, task_class)
        return task_class
    
    def task(self, 
             name: Optional[str] = None,
             priority: int = 0,
             max_retries: int = 3,
             timeout: int = 300,
             queue_name: str = "default"):
        """
        Décorateur universel pour tâches (classes ou fonctions).
        
        Usage avec classe:
            @framework.task()
            class MyTask(Task):
                def run(self):
                    return "result"
        
        Usage avec fonction:
            @framework.task()
            def my_function(param1: str, param2: int = 10):
                return param1 * param2
        """
        def decorator(obj: Union[Type[Task], Callable]):
            if inspect.isclass(obj) and issubclass(obj, Task):
                # If it's a class of type Task
                return self._register_task_class(
                    obj, name, priority, max_retries, timeout, queue_name
                )
            elif callable(obj) and not inspect.isclass(obj):
                # If it's a function
                return self._register_function_task(
                    obj, name, priority, max_retries, timeout, queue_name
                )
            else:
                # TODO: do a custom exception
                raise ValueError(
                    f"@task decorator can only be used on Task classes or functions, "
                    f"not {type(obj)}"
                )
        
        return decorator
    
    def _register_task_class(self, 
            task_class: Type[Task],
            name: Optional[str],
            priority: int,
            max_retries: int,
            timeout: int,
            queue_name: str
        ) -> Type[Task]:
        """Register a class."""

        # not sure about that
        if not hasattr(task_class, 'priority'):
            task_class._priority = priority
        if not hasattr(task_class, 'max_retries'):
            task_class._max_retries = max_retries
        if not hasattr(task_class, 'timeout'):
            task_class._timeout = timeout
        if not hasattr(task_class, 'queue_name'):
            task_class._queue_name = queue_name
            
        task_name = name or task_class.get_name()
        self.registry.register(task_name, task_class)
        return task_class
    
    def _register_function_task(self,
                               func: Callable,
                               name: Optional[str],
                               priority: int,
                               max_retries: int,
                               timeout: int,
                               queue_name: str):
        """Convertit une fonction en Task et l'enregistre."""
        task_name = name or func.__name__
        # Créer une classe Task dynamiquement pour cette fonction
        class FunctionTask(Task):
            """Wrapper Task pour une fonction."""
            
            # Configuration de la tâche
            _task_name = task_name
            _priority = priority
            _max_retries = max_retries
            _timeout = timeout
            _queue_name = queue_name
            
            def run(self):
                # Récupérer la signature de la fonction
                sig = inspect.signature(func)
                
                # Mapper les params aux arguments de la fonction
                bound_args = self._bind_params_to_function(sig)
                
                # Appeler la fonction avec les bons arguments
                return func(**bound_args)
            
            def _bind_params_to_function(self, sig: inspect.Signature) -> ict:
                """Mappe self.params aux arguments de la fonction."""
                bound_args = {}
                
                for param_name, param in sig.parameters.items():
                    if param_name in self.params:
                        # Paramètre fourni
                        bound_args[param_name] = self.params[param_name]
                    elif param.default is not param.empty:
                        # Utiliser la valeur par défaut
                        bound_args[param_name] = param.default
                    else:
                        # Paramètre requis manquant
                        raise TypeError(
                            f"Missing required parameter '{param_name}' "
                            f"for task '{task_name}'"
                        )
                
                return bound_args

        # Enregistrer la classe Task
        self.registry.register(task_name, FunctionTask)

        # Créer un wrapper pour la fonction originale
        return self._create_function_wrapper(
            func, FunctionTask, task_name, priority
        )
    
    def _create_function_wrapper(self,
                                func: Callable,
                                task_class: Type[Task],
                                task_name: str,
                                default_priority: int):
        """Crée un wrapper qui permet l'appel direct et .delay()."""
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Appel direct de la fonction (exécution synchrone locale)
            return func(*args, **kwargs)
        
        def delay(*args, **kwargs) -> str:
            """
            Enqueue la fonction comme tâche.
            
            Usage:
                task_id = my_function.delay("param1", param2=42)
            """
            # Convertir args en kwargs basé sur la signature
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            params = dict(bound.arguments)
            
            # Créer et enqueue la tâche
            task = task_class(**params)
            return task.enqueue()
        
        def apply_async(args: tuple = None, 
                       kwargs: dict = None,
                       priority: int = None,
                       delay: int = None) -> str:
            """
            Interface Celery-like pour enqueue.
            
            Usage:
                task_id = my_function.apply_async(
                    args=("param1",),
                    kwargs={"param2": 42},
                    priority=10,
                    delay=60
                )
            """
            args = args or ()
            kwargs = kwargs or {}
            
            # Convertir en params
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            params = dict(bound.arguments)
            
            # Créer la tâche
            task = task_class(**params)
            
            # Enqueue avec options
            enqueue_kwargs = {}
            if priority is not None:
                enqueue_kwargs['priority'] = priority
            if delay is not None:
                enqueue_kwargs['delay'] = delay
                
            return task.enqueue(**enqueue_kwargs)
        
        def get_result(task_id: str, timeout: Optional[int] = None):
            """
            Récupère le résultat d'une exécution précédente.
            
            Usage:
                result = my_function.get_result(task_id, timeout=30)
            """
            if not self.result_queue:
                raise RuntimeError("Result backend not configured")
            return self.result_queue.get_message(task_id, ResultMessage)
        
        # Attacher les méthodes au wrapper
        wrapper.delay = delay
        wrapper.apply_async = apply_async
        wrapper.get_result = get_result
        wrapper.task_name = task_name
        wrapper.task_class = task_class
        wrapper.original_func = func
        wrapper.is_task = True
        
        # Métadonnées utiles
        wrapper.__doc__ = (
            f"{func.__doc__ or ''}\n\n"
            f"This function is registered as task '{task_name}'.\n"
            f"Use .delay() or .apply_async() to run asynchronously."
        )
        
        return wrapper

    def create_worker(self) -> TaskWorker:
        """Create a configured worker."""
        return TaskWorker(
            task_queue=self.task_queue,
            result_queue=self.result_queue,
            registry=self.registry,
            metric=self.metrics
        )
    
    # def create_orchestrator(self, num_workers: int = 4) -> Orchestrator:
    #     """Crée un orchestrateur multi-process."""
    #     return Orchestrator(
    #         queue=self.queue,
    #         registry=self.registry,
    #         result_backend=self.result_backend,
    #         metrics=self.metrics,
    #         num_workers=num_workers
    #     )
    
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
    # 'Orchestrator'
]