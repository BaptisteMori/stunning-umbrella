from queue.models import TaskMessage
from queue.priorities import TaskPriority


class TaskQueue:
    """
    Abstract base class for managing task queues with priority-based processing.
    
    This class provides a standardized interface for task queue operations including
    adding tasks with priority levels, retrieving tasks for processing, and inspecting
    queue contents without modification.
    
    Attributes:
        queue_name (str): Unique identifier for the task queue instance
    """
    def __init__(self, queue_name: str, priorities: TaskPriority = TaskPriority):
        self.queue_name: str = queue_name
        self.priorities: TaskPriority = priorities

    @classmethod
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

    def enqueue(self, task_name: str, params: dict[str, any] = {}, priority: int = 0) -> str:
        """
        Add a new task to the queue with specified parameters and priority.
        
        Tasks are organized by priority level, with higher priority values
        typically processed before lower priority tasks. The exact priority
        handling behavior depends on the concrete implementation.
        
        Args:
            task_name (str): Descriptive name identifying the type of task
            params (dict[str, any]): Dictionary containing task-specific parameters
                                   and configuration data required for execution
            priority (int, optional): Task priority level for queue ordering.
                                    Higher values indicate higher priority. Defaults to 0.
        
        Returns:
            str: Unique task identifier that can be used to track or reference
                this specific task instance
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError

    def dequeue(self, timeout: int = 1) -> TaskMessage:
        """
        Remove and return the highest priority task from the queue.
        
        This operation modifies the queue state by permanently removing the
        returned task. If multiple tasks share the same priority level,
        the selection behavior depends on the concrete implementation
        (typically FIFO within priority levels).
        
        Args:
            timeout (int, optional): Maximum time to wait for a task in seconds.
                                    Defaults to 1

        Returns:
            TaskMessage: Task message object containing all task details
                        including name, parameters, priority, and metadata
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError

    def peek(self) -> TaskMessage:
        """
        Examine the next task in queue without removing it.
        
        This operation allows inspection of the highest priority task
        without modifying the queue state. Useful for determining task
        characteristics before deciding whether to process or skip.
        
        Returns:
            TaskMessage: Task message object for the next task to be processed,
                        containing the same information as dequeue() but without
                        removing the task from the queue
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError
    
    def get_queue_size(self) -> dict[str, int]:
        """
        Get the stats of the queue, the total number of messages in the queue and per priorities.

        Returns:
            dict[str, int]: Dictionnary of stats
        
        Raises:
            NotImplementedError: This is an abstract method that must be implemented
                               by concrete subclasses
        """
        raise NotImplementedError
