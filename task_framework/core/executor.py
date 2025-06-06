from core.registry import TaskRegistry


class TaskExecutor:
    """
    Executor for managing and running registered tasks.
    
    This class provides a centralized mechanism for executing tasks that are
    registered in a TaskRegistry. It handles task instantiation and execution
    with optional parameter passing.
    """
    
    def __init__(self, registry: TaskRegistry):
        self.registry = registry
    
    def execute_task(self, task_name: str, params: dict[str, any]|None = None):
        """
        Execute a registered task by name with optional parameters.
        
        Retrieves the task class from the registry, instantiates it with
        the provided parameters, and executes the task's run method.
        
        Args:
            task_name (str): Name of the task to execute as registered 
                in the task registry.
            params (dict[str, any], optional): Dictionary of parameters 
                to pass to the task constructor. Defaults to None.
        
        Returns:
            any: The result returned by the task's run() method.
        
        Raises:
            KeyError: If the task_name is not found in the registry.
            TypeError: If the task class cannot be instantiated with 
                the provided parameters.
        """
        task_class = self.registry.get_task(task_name)
        task_instance = task_class(params)
        return task_instance.run()
