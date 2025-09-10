import time
import redis
import logging

from task_framework.core.priorities import TaskPriority
from task_framework.core.message import Message, TaskMessage, ResultMessage 
from task_framework.queue.queue import Queue


LOGGER = logging.getLogger(__name__)


class RedisTaskQueue(Queue):
    """
    Redis-based task queue implementation providing priority-based task management.
    
    This class implements a task queue using Redis as the backend storage, supporting
    priority-based task queues with blocking operations for efficient task processing.
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 queue_name: str = "tasks", 
                 max_retries: int = 3,
                 priorities: type[TaskPriority] = TaskPriority):
        super().__init__(queue_name, priorities=priorities)
        self.redis_url: str = redis_url
        self.max_retries: int = max_retries
        
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
                LOGGER.info(f"Connected to Redis at {self.redis_url}")
                return
            except redis.ConnectionError as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    LOGGER.warning(f"Redis connection failed (attempt {attempt + 1}/{self.max_retries}), retrying in {wait_time}s")
                    time.sleep(wait_time)  # Exponential backoff
                else:
                    raise ConnectionError(f"Failed to connect to Redis after {self.max_retries} attempts: {e}")
    
    def _get_queue_key(self, priority: int) -> str:
        """Generate Redis key for a specific priority queue."""
        return f"{self.queue_name}:priority:{priority}"

    def enqueue(self, message: Message) -> str:
        """
        Add a task to the appropriate priority queue.
        
        Args:
            message (Message): the message to enqueue
        Returns:
            str: Unique identifier for the enqueued task message
        """
        if type(message) is TaskMessage:
            queue_key = self._get_queue_key(message.priority)
        
            try:
                self.redis_client.lpush(queue_key, message.to_json())
                LOGGER.debug(f"Enqueued task {message.task_name} with priority {message.priority} (ID: {message.task_id})")
                return message.task_id
            except redis.RedisError as e:
                LOGGER.error(f"Failed to enqueue task {message.task_name}: {e}")
                raise
        elif type(message) is ResultMessage:
            pass
    
    def dequeue(self, timeout: int = 1) -> TaskMessage:
        """
        Retrieve and remove a task from the queue with priority handling.
        
        This method implements priority-based dequeuing by checking all priority queues
        in order from highest to lowest priority. Uses blocking operations (BRPOP) 
        for efficient waiting.
        
        Args:
            timeout (int, optional): Maximum time to wait for a task in seconds. Defaults to 1.
        
        Returns:
            TaskMessage|None: The next available task message, or None if timeout expires
        """
        queue_keys = [self._get_queue_key(p.value) for p in self._get_sorted_priorities()]
        
        try:
            result = self.redis_client.brpop(queue_keys, timeout=timeout)
            
            if result:
                queue_key, message_json = result
                message = TaskMessage.from_json(message_json)
                LOGGER.debug(f"Dequeued task {message.task_name} from {queue_key} (ID: {message.task_id})")
                return message
                
        except redis.RedisError as e:
            LOGGER.error(f"Failed to dequeue task: {e}")
            raise
            
        return None
    
    def peek(self) -> TaskMessage:
        """
        Examine the next task without removing it from queue.
        
        Returns:
            TaskMessage|None: Next task if available, None otherwise
        """
        # Vérifier les queues par ordre de priorité
        for priority in self._get_sorted_priorities():
            queue_key = self._get_queue_key(priority.value)
            try:
                result = self.redis_client.lindex(queue_key, -1)
                if result:
                    return TaskMessage.from_json(result)
            except redis.RedisError as e:
                LOGGER.error(f"Failed to peek queue {queue_key}: {e}")
                continue
                
        return None
    
    def get_queue_size(self) -> dict[str, int]:
        """
        Get the stats of the queue, the total number of messages in the queue and per priorities.

        Returns:
            dict[str, int]: Dictionary of stats including total and per-priority counts
        """
        stats = {}
        total = 0
        
        try:
            for priority in self.priorities:
                queue_key = self._get_queue_key(priority.value)
                count = self.redis_client.llen(queue_key)
                stats[f"priority_{priority.value}"] = count
                stats[priority.name.lower()] = count  # Ex: "critical", "high", etc.
                total += count
            
            stats["total"] = total
            return stats
            
        except redis.RedisError as e:
            LOGGER.error(f"Failed to get queue size: {e}")
            raise

    def clear_queue(self, priority: int = None):
        """
        Clear all tasks from queue(s).
        
        Args:
            priority (int, optional): Clear only this priority level. If None, clear all.
        """
        try:
            if priority is not None:
                queue_key = self._get_queue_key(priority)
                deleted = self.redis_client.delete(queue_key)
                LOGGER.info(f"Cleared {deleted} tasks from priority {priority}")
            else:
                # Clear all priority queues
                keys_to_delete = [self._get_queue_key(p.value) for p in self.priorities]
                if keys_to_delete:
                    deleted = self.redis_client.delete(*keys_to_delete)
                    LOGGER.info(f"Cleared all queues, deleted {deleted} keys")
        except redis.RedisError as e:
            LOGGER.error(f"Failed to clear queue: {e}")
            raise
    
    def ack(self, message_id):
        return
    
    def nack(self, message_id, requeue = True):
        return

    def delete(self, message_id):
        return
    
    def get_status(self, message_id):
        return
    
    def _get_sorted_priorities(self, descending: bool = True) -> list[TaskPriority]:
        """
        Return all TaskPriority values sorted by their numeric value.

        Args:
            descending (bool): If True, highest priority first. 
                            If False, lowest priority first.

        Returns:
            list[TaskPriority]: Sorted list of priorities.
        """
        return sorted(TaskPriority, key=lambda p: p.value, reverse=descending)