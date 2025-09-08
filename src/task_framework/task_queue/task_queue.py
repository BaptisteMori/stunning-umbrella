from abc import ABC, abstractmethod
from typing import Dict, Optional
from core.models import TaskMessage
from core.priorities import TaskPriority


class Queue(ABC):
    """
    Interface pour différents backends de queue.
    
    Permet d'échanger facilement l'implémentation sans changer
    le code utilisateur. Prépare pour sharding/pooling futur.
    """
    
    @abstractmethod
    def enqueue(self, task_name: str, params: dict = None, priority: int = 0) -> str:
        """Ajouter une tâche à la queue."""
        pass
    
    @abstractmethod
    def dequeue(self, timeout: int = 1) -> Optional[TaskMessage]:
        """Récupérer une tâche de la queue."""
        pass
    
    @abstractmethod
    def get_queue_size(self) -> Dict[str, int]:
        """Stats de la queue."""
        pass
    
    @abstractmethod
    def peek(self) -> Optional[TaskMessage]:
        """Voir la prochaine tâche sans la retirer."""
        pass


class TaskQueue(Queue):
    """
    Abstract base class for managing task queues with priority-based processing.
    
    This class provides a standardized interface for task queue operations including
    adding tasks with priority levels, retrieving tasks for processing, and inspecting
    queue contents without modification.
    """
    def __init__(self, queue_name: str, priorities: type[TaskPriority] = TaskPriority):
        self.queue_name: str = queue_name
        self.priorities = priorities

    def _get_sorted_priorities(self) -> list[TaskPriority]:
        """
        Returns the task priorities sorted in descending order of their values.

        This is useful when tasks should be processed starting from the highest 
        priority (e.g., CRITICAL) down to the lowest (e.g., IDLE).

        Returns:
            List[TaskPriority]: A list of TaskPriority members sorted from 
                                highest to lowest priority.
        """
        return sorted(self.priorities, key=lambda p: p.value, reverse=True)

    def enqueue(self, task_name: str, params: dict[str, any] = None, priority: int = 0) -> str:
        """
        Add a new task to the queue with specified parameters and priority.
        
        Args:
            task_name (str): Descriptive name identifying the type of task
            params (dict[str, any], optional): Dictionary containing task-specific parameters.
                                             Defaults to None.
            priority (int, optional): Task priority level for queue ordering. Defaults to 0.
        
        Returns:
            str: Unique task identifier that can be used to track or reference this task
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError

    def dequeue(self, timeout: int = 1) -> TaskMessage:
        """
        Remove and return the highest priority task from the queue.
        
        Args:
            timeout (int, optional): Maximum time to wait for a task in seconds. Defaults to 1

        Returns:
            TaskMessage: Task message object containing all task details
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError

    def peek(self) -> TaskMessage:
        """
        Examine the next task in queue without removing it.
        
        Returns:
            TaskMessage: Task message object for the next task to be processed
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError
    
    def get_queue_size(self) -> dict[str, int]:
        """
        Get the stats of the queue, the total number of messages in the queue and per priorities.

        Returns:
            dict[str, int]: Dictionary of stats
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError
    

