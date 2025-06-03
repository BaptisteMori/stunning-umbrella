import redis
from .models import TaskMessage
from .queue import TaskQueue


class RedisTaskQueue(TaskQueue):
    """
    Redis-based task queue implementation providing priority-based task management.
    
    This class implements a task queue using Redis as the backend storage, supporting
    both regular and high-priority task queues with blocking operations for efficient
    task processing.
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", queue_name: str = "tasks"):
        self.redis_client = redis.from_url(redis_url)
        self.queue_name = queue_name
    
    def enqueue(self, task_name: str, params: dict[str, any], priority: int = 0) -> str:
        """
        Add a task to the appropriate queue based on priority.
        
        Tasks with priority > 0 are added to the high-priority queue, while others
        go to the standard queue. Uses LPUSH for LIFO ordering within each queue.
        
        Args:
            task_name (str): Name/identifier of the task to execute
            params (dict[str, any]): Parameters dictionary for task execution
            priority (int, optional): Task priority level. Values > 0 indicate high priority. Defaults to 0.
        
        Returns:
            str: Unique identifier for the enqueued task message
        """
        message = TaskMessage(task_name=task_name, params=params, priority=priority)
        
        if priority > 0:
            # Priority queue
            self.redis_client.lpush(f"{self.queue_name}:high", message.to_json())
        else:
            self.redis_client.lpush(self.queue_name, message.to_json())
        
        return message.id
    
    def dequeue(self, timeout: int = 1) -> TaskMessage|None:
        """
        Retrieve and remove a task from the queue with priority handling.
        
        This method implements priority-based dequeuing by first checking the high-priority
        queue with a short timeout, then falling back to the standard queue. Uses blocking
        operations (BRPOP) for efficient waiting.
        
        Args:
            timeout (int, optional): Maximum time to wait for a task in seconds. Defaults to 1.
        
        Returns:
            TaskMessage|None: The next available task message, or None if timeout expires
                             without finding any tasks.
        """
        # First verify the priority queue
        result = self.redis_client.brpop(f"{self.queue_name}:high", timeout=0.1)
        if not result:
            result = self.redis_client.brpop(self.queue_name, timeout=timeout)
        
        if result:
            _, message_json = result
            return TaskMessage.from_json(message_json.decode())
        return None