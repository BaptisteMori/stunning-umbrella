import time
import redis
import logging
from enum import Enum

from task_framework.core.priorities import TaskPriority
from task_framework.core.message import Message, TaskMessage, ResultMessage 
from task_framework.queue.queue import Queue


LOGGER = logging.getLogger(__name__)


# class RedisStructureType(Enum):



class RedisTaskQueue(Queue):
    """
    Redis implementation of generic queue interface.
    
    This implementation uses Redis lists for task queues (with priority)
    and Redis hashes for persistent storage (results, DLQ, etc).
    
    Attributes:
        client: Redis client instance.
        queue_prefix: Prefix for all Redis keys.
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 queue_name: str = "default", 
                 max_retries: int = 3,
                 priorities: type[TaskPriority] = TaskPriority,
                 structure: str = "fifo"
                 ):
        """
        Initialize Redis queue.
        
        Args:
            redis_url: Redis connection URL.
            queue_name: Prefix for Redis keys to avoid collisions.
            max_retries: Number of retries before abandon
        """
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
        """
        Generate Redis key for a queue.
        
        Args:
            priority: Optional priority level for task queues.
            
        Returns:
            str: Redis key for the queue.
        """
        return f"{self.queue_name}:priority:{priority}"

    def enqueue(self, message: Message) -> str:
        """
        Add a message to the specified queue.
        
        For task-like queues (with priority), uses Redis lists.
        For result-like queues, uses Redis hashes.
        
        Args:
            message: Message to enqueue.
            
        Returns:
            str: Message ID.
            
        Raises:
            redis.RedisError: If Redis operation fails.
        """
        queue_key = self._get_queue_key(message.priority)
        if type(message) is ResultMessage:
            self.redis_client.hset(queue_key, message.to_json())
            LOGGER.debug(f"Enqueued in hset result {message.task_name} with priority {message.priority} (ID: {message.task_id})")
            return message.task_id
        
        self.redis_client.lpush(queue_key, message.to_json())
        LOGGER.debug(f"Enqueued in fifo task {message.task_name} with priority {message.priority} (ID: {message.task_id})")
        return message.task_id
    
    def dequeue(self, timeout: int = 1) -> TaskMessage:
        """
        Remove and return next message from queue.
        
        Only works for list-based queues (tasks, retry).
        Checks priorities from high to low.
        
        Args:
            timeout: Seconds to wait for message.
            
        Returns:
            Optional[Message]: Message or None if timeout/empty.
        """
        # queue_keys = [self._get_queue_key(p.value) for p in self._get_sorted_priorities()]
        
        queue_keys = []
        for priority in range(100, -1, -1):
            key = self._get_queue_key(priority)
            if self.redis_client.exists(key):
                queue_keys.append(key)
        
        if not queue_keys:
            return None
        
        result = self.redis_client.brpop(queue_keys, timeout=timeout)
        
        if result:
            queue_key, message_json = result
            message: TaskMessage = TaskMessage.from_json(message_json)
            LOGGER.debug(f"Dequeued task {message.task_name} from {queue_key} (ID: {message.task_id})")
            return message
            
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