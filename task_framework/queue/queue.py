from queue.models import TaskMessage


class TaskQueue:
    """
    Abstract base class for managing task queues with priority-based processing.
    
    This class provides a standardized interface for task queue operations including
    adding tasks with priority levels, retrieving tasks for processing, and inspecting
    queue contents without modification.
    
    Attributes:
        queue_name (str): Unique identifier for the task queue instance
    """
    def __init__(self, queue_name: str):
        self.queue_name = queue_name

    def enqueue(self, task_name: str, params: dict[str, any], priority: int = 0) -> str:
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
    
