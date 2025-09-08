import functools
import inspect
from typing import Callable

from core.task import Task
from core.registry import TaskRegistry
from task_queue.task_queue import TaskQueue


def create_task_decorator(registry: TaskRegistry, queue: TaskQueue):
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
                        bound = sig.bind(**self.params)
                        bound.apply_defaults()
                        return f(**bound.arguments)
                    except TypeError:
                        return f(self.params)
            
            registry.register(task_name, FunctionTask)
            
            @functools.wraps(f)
            def wrapper(*args, **kwargs):
                return f(*args, **kwargs)
            
            def delay(*args, **kwargs):
                sig = inspect.signature(f)
                try:
                    bound = sig.bind(*args, **kwargs)
                    bound.apply_defaults()
                    params = dict(bound.arguments)
                except TypeError:
                    params = {'args': args, 'kwargs': kwargs}
                
                return queue.enqueue(task_name, params, priority)
            
            wrapper.delay = delay
            wrapper.task_name = task_name
            wrapper.apply_async = lambda args=None, kwargs=None: delay(*(args or ()), **(kwargs or {}))
            
            return wrapper
        
        if func is None:
            return decorator
        return decorator(func)
    
    return task