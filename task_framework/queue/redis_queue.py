import time
import redis

from queue.models import TaskMessage
from queue.queue import TaskQueue


class RedisTaskQueue(TaskQueue):
    """
    Redis-based task queue implementation providing priority-based task management.
    
    This class implements a task queue using Redis as the backend storage, supporting
    both regular and high-priority task queues with blocking operations for efficient
    task processing.
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 queue_name: str = "tasks", 
                 max_retries: int = 3):
        super().__init__(queue_name)
        self.redis_url = redis_url
        self.max_retries = max_retries
        self._connect()
    
    def _connect(self):
        """Establish Redis connection with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self.redis_client = redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                self.redis_client.ping()
                return
            except redis.ConnectionError as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise ConnectionError(f"Failed to connect to Redis: {e}")
    
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
            # message_json est déjà une string si decode_responses=True
            return TaskMessage.from_json(message_json)
        return None
    
    def peek(self) -> TaskMessage|None:
        """
        Examine the next task without removing it from queue.
        
        Returns:
            TaskMessage|None: Next task if available, None otherwise
        """
        # Check high priority queue first
        result = self.redis_client.lindex(f"{self.queue_name}:high", -1)
        if not result:
            result = self.redis_client.lindex(self.queue_name, -1)
        
        if result:
            return TaskMessage.from_json(result)
        return None