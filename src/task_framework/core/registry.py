import importlib
import inspect
import logging
from pathlib import Path

from core.task import Task
from utils.string_parsing import string_matches_any_pattern


LOGGER: logging.Logger = logging.getLogger(__name__)


class TaskRegistry:
    """
    A registry for managing and storing task classes.
    
    This class provides a centralized system for registering, retrieving, and 
    listing task classes that inherit from a base Task class.
    """

    def __init__(self):
        self.tasks: dict[str, type[Task]] = {}
    
    def register(self, name: str, task_class: type[Task]):
        """
        Register a task class with a given name.
        
        Args:
            name (str): The name to associate with the task class
            task_class (type[Task]): The task class to register
        
        Raises:
            ValueError: If task_class is not a subclass of Task
        """
        if not issubclass(task_class, Task):
            raise ValueError(f"{task_class} need to be a subclass of Task")
        self.tasks[name] = task_class
    
    def get_task(self, name: str) -> type[Task]:
        """
        Retrieve a task class by its registered name.
        
        Args:
            name (str): The name of the task to retrieve
        
        Returns:
            type[Task]: The task class associated with the given name
        
        Raises:
            ValueError: If no task is found with the specified name
        """
        if name not in self.tasks:
            raise ValueError(f"Task '{name}' not found")
        return self.tasks[name]
    
    def list_tasks(self) -> dict[str, type[Task]]:
        """
        Get a copy of all registered tasks.
        
        Returns:
            dict[str, type[Task]]: A dictionary mapping task names to task classes
        """
        return self.tasks.copy()
    

def discover_tasks(tasks_path: Path, except_patterns: list[str] = [r"__.*", r"base.py"]) -> dict[str, Task]:
    """
    Automatically discover and load task classes from a specified directory.
    
    This function scans a directory for Python files, dynamically imports them,
    and identifies classes that inherit from the Task base class. It excludes
    files matching specified patterns and the Task base class itself.
    
    Args:
        tasks_path (str|Path): Path to the directory containing task modules
        except_patterns (list[str], optional): List of regex patterns for files 
            to exclude. Defaults to [r"__.*", r"base.py"]
    
    Returns:
        dict[str, Task]: Dictionary mapping task names to task classes
    
    Raises:
        FileNotFoundError: If the specified tasks_path does not exist
        ImportError: If there are issues importing task modules (logged but not raised)
    
    Note:
        - Task names are derived from class names by removing "Task" suffix and 
          converting to lowercase
        - Import errors are logged but don't stop the discovery process
    """
    available_tasks: dict[str, type[Task]] = {}

    if type(tasks_path) == str:
        tasks_path = Path(tasks_path)
    
    if not tasks_path.exists():
        raise FileNotFoundError(tasks_path)
    
    for file_path in tasks_path.glob("*.py"):
        if string_matches_any_pattern(file_path.name, except_patterns):
            continue

        module_name = file_path.stem
        try:
            # Import module dynamically
            module_path = str(tasks_path).replace('/', '.').replace('\\', '.')
            module = importlib.import_module(f"{module_path}.{module_name}")
            
            # Search each class that inherits from Task
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (issubclass(obj, Task) and 
                    obj != Task and 
                    obj.__module__ == module.__name__):
                    
                    available_tasks[name] = obj
                    LOGGER.debug(f"Task found: {name}")
        
        except ImportError as e:
            LOGGER.exception("Error during the import of %s", module_name)
            
    return available_tasks
