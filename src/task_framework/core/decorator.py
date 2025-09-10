import functools
import inspect
from typing import Callable

from task_framework.core.task import Task
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue import Queue


def create_task_decorator(registry: TaskRegistry, queue: Queue):
    """
    Factory qui créé un décorateur @task configuré avec registry et queue.
    """
    
    def task(func: Callable = None, *, name: str = None, priority: int = 0):
        """
        Décorateur qui transforme une fonction en Task + ajoute .delay()
        """
        def decorator(f):
            task_name = name or f.__name__
            
            class FunctionTask(Task):
                def __init__(self, params: dict = None):
                    super().__init__(params)
                
                def run(self):
                    sig = inspect.signature(f)
                    if not self.params:
                        return f()
                    
                    try:
                        # Essayer de mapper params -> arguments de fonction
                        bound = sig.bind(**self.params)
                        bound.apply_defaults()
                        return f(**bound.arguments)
                    except TypeError:
                        # Fallback : passer tout comme premier argument
                        return f(self.params)
            
            registry.register(task_name, FunctionTask)
            
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                # Appel direct = fonction normale
                return f(*args, **kwargs)
            
            def delay(*args, priority_override=None, **kwargs):
                """
                Enqueue la tâche avec priorité optionnelle.
                
                Args:
                    *args, **kwargs: Arguments pour la fonction
                    priority_override: Priorité à utiliser (override la priorité par défaut)
                """
                # Utiliser priority_override si fourni, sinon la priorité par défaut
                actual_priority = priority_override if priority_override is not None else priority
                
                # Convertir args/kwargs en params dict
                sig = inspect.signature(f)
                try:
                    bound = sig.bind(*args, **kwargs)
                    bound.apply_defaults()
                    params = dict(bound.arguments)
                except TypeError:
                    params = {'args': args, 'kwargs': kwargs}
                
                return queue.enqueue(task_name, params, actual_priority)
            
            # Ajouter des méthodes de convenance pour différentes priorités
            def delay_high_priority(*args, **kwargs):
                """Enqueue avec priorité haute."""
                from task_framework.core.priorities import TaskPriority
                return delay(*args, priority_override=TaskPriority.HIGH, **kwargs)
            
            def delay_low_priority(*args, **kwargs):
                """Enqueue avec priorité basse."""
                from task_framework.core.priorities import TaskPriority
                return delay(*args, priority_override=TaskPriority.LOW, **kwargs)
            
            def delay_critical(*args, **kwargs):
                """Enqueue avec priorité critique."""
                from task_framework.core.priorities import TaskPriority
                return delay(*args, priority_override=TaskPriority.CRITICAL, **kwargs)
            
            wrapper.delay = delay
            wrapper.delay_high = delay_high_priority
            wrapper.delay_low = delay_low_priority
            wrapper.delay_critical = delay_critical
            wrapper.task_name = task_name
            wrapper.default_priority = priority
            
            # Celery Compatibility
            wrapper.apply_async = lambda args=None, kwargs=None, priority=None: delay(
                *(args or ()), 
                priority_override=priority,
                **(kwargs or {})
            )
            
            return wrapper
        
        if func is None:
            return decorator
        return decorator(func)
    
    return task