import time
import redis
import logging
from enum import Enum
from typing import Optional, Type, List, Any, Dict
from datetime import datetime, timezone

from task_framework.core.priorities import TaskPriority
from task_framework.core.message import Message 
from task_framework.queue.queue import Queue
from task_framework.core.task import TaskStatus
from task_framework.core.exception import MessageNotExist


LOGGER = logging.getLogger(__name__)

class RedisStructure(Enum):
    FIFO = "fifo"
    HSET = "hset"


class RedisQueueStructureException(Exception):
    pass

class RedisTaskQueue(Queue):
    """
    Redis implementation of generic queue interface.
    
    This implementation uses Redis lists for task queues (with priority)
    and Redis hashes for persistent storage (results, DLQ, etc).
    
    Attributes:
        client: Redis client instance.
        queue_prefix: Prefix for all Redis keys.
        structure: Structure of the queue, fifo or hset, depending on the operation 
            on the queue, by default FIFO
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 queue_name: str = "default", 
                 max_retries: int = 3,
                 priorities: type[TaskPriority] = TaskPriority,
                 queue_structure: RedisStructure = RedisStructure.FIFO,
                 default_message_type: Type[Message] = Message
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
        self.queue_structure: RedisStructure = queue_structure
        self.event_publisher: bool = True
        self.default_message_type: Type[Message] = default_message_type

        # For HSET structure, we track messages in processing
        self.processing_set_key = f"{queue_name}:processing" if queue_structure == RedisStructure.HSET else None
        
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
    
    def _get_queue_key(self, priority: int = TaskPriority.NORMAL) -> str:
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
        Add a message to the queue.
        
        FIFO: Adds to list at appropriate priority.
        HSET: Stores with task_id as key.
        
        Args:
            message: Message to enqueue.
            
        Returns:
            str: Message task_id.
            
        Raises:
            redis.RedisError: If Redis operation fails.
        """
        task_id = message.task_id
        
        if self.queue_structure == RedisStructure.HSET:
            # Use task_id as key directly in hash
            queue_key = self._get_queue_key()
            self.redis_client.hset(queue_key, task_id, message.to_json())
            
        elif self.queue_structure == RedisStructure.FIFO:
            # Add to priority list
            queue_key = self._get_queue_key(message.priority)
            self.redis_client.lpush(queue_key, message.to_json())
            
        else:
            raise ValueError(f"Unknown queue structure: {self.queue_structure}")
        
        LOGGER.debug(f"Enqueued message {task_id} in {self.queue_structure.value} queue {self.queue_name}")
        
        if self.event_publisher:
            self._publish_event("enqueued", task_id)
            
        return task_id
    
    def dequeue(self, timeout: int = 1, message_type: Type[Message] = Message) -> Optional[Message]:
        """
        Remove and return next message from queue.
        
        Only works for FIFO queues. Checks priorities from high to low.
        
        Args:
            timeout: Seconds to wait for message.
            
        Returns:
            Optional[Message]: Next message or None.
            
        Raises:
            ValueError: If called on HSET queue.
        """
        if self.queue_structure != RedisStructure.FIFO:
            raise ValueError("dequeue() only works with FIFO queues")

        # Build list of keys sorted by priority
        queue_keys = []
        for priority in range(100, -1, -1):
            key = self._get_queue_key(priority)
            if self.redis_client.exists(key):
                queue_keys.append(key)
        
        if not queue_keys:
            return None
        
        # Blocking pop from multiple lists
        result = self.redis_client.brpop(queue_keys, timeout=timeout)
        
        if result:
            queue_key, message_json = result
            message = message_type.from_json(message_json)
            
            LOGGER.debug(f"Dequeued message {message.task_id} from {queue_key}")
            
            if self.event_publisher:
                self._publish_event("dequeued", message.task_id)
                
            return message
            
        return None
    
    def get_message(self, message_id: str, 
                   message_type: Type[Message] = Message) -> Optional[Message]:
        """
        Retrieve a specific message without removing it.
        
        Args:
            message_id: ID of message to retrieve.
            message_type: Type for deserialization.
            
        Returns:
            Optional[Message]: Message if found, None otherwise.
        """
        if self.queue_structure == RedisStructure.HSET:
            # Direct O(1) lookup in hash
            queue_key = self._get_queue_key()
            message_json = self.redis_client.hget(queue_key, message_id)
            
            if message_json:
                return message_type.from_json(message_json)
                
        elif self.queue_structure == RedisStructure.FIFO:
            # Must scan through all priority lists
            for priority in range(100, -1, -1):
                key = self._get_queue_key(priority)
                messages = self.redis_client.lrange(key, 0, -1)
                
                for msg_json in messages:
                    msg = message_type.from_json(msg_json)
                    if msg.task_id == message_id:
                        return msg
        
        return None
    
    def update_message(self, message_id: str, message: Message) -> bool:
        """
        Update an existing message.
        
        HSET: Simple overwrite with O(1).
        FIFO: Complex scan and replace.
        
        Args:
            message_id: ID of message to update.
            message: New message content.
            
        Returns:
            bool: True if successful.
        """
        if self.queue_structure == RedisStructure.HSET:
            # Simple O(1) update in hash
            queue_key = self._get_queue_key()
            self.redis_client.hset(queue_key, message_id, message.to_json())
            
            if self.event_publisher:
                self._publish_event("updated", message_id)
            return True
            
        elif self.queue_structure == RedisStructure.FIFO:
            # Complex operation - need atomic update
            message_json = message.to_json()
            
            # Lua script for atomic find and replace
            lua_script = """
            local key_prefix = ARGV[1]
            local target_id = ARGV[2]
            local new_json = ARGV[3]
            
            for priority = 100, 0, -1 do
                local key = key_prefix .. ':priority:' .. priority
                local messages = redis.call('lrange', key, 0, -1)
                
                for i, msg_json in ipairs(messages) do
                    local msg = cjson.decode(msg_json)
                    if msg.task_id == target_id then
                        redis.call('lset', key, i-1, new_json)
                        return 1
                    end
                end
            end
            return 0
            """
            
            result = self.redis_client.eval(
                lua_script, 0, self.queue_name, message_id, message_json
            )
            return bool(result)
        
        return False
        
    def delete_message(self, message_id: str) -> bool:
        """
        Remove a specific message from queue.
        
        Args:
            message_id: ID of message to delete.
            
        Returns:
            bool: True if deleted, False if not found.
        """
        if self.queue_structure == RedisStructure.HSET:
            # Simple O(1) delete from hash
            queue_key = self._get_queue_key()
            return self.redis_client.hdel(queue_key, message_id) > 0

        elif self.queue_structure == RedisStructure.FIFO:
            # Must find and remove from list
            for priority in range(100, -1, -1):
                key = self._get_queue_key(priority)
                messages = self.redis_client.lrange(key, 0, -1)

                for msg_json in messages:
                    msg = Message.from_json(msg_json)
                    if msg.task_id == message_id:
                        removed = self.redis_client.lrem(key, 1, msg_json)
                        return removed > 0

        return False

    def peek(self, count: int = 1,
             message_type: Type[Message] = Message) -> List[Message]:
        """
        View messages without removing them.
        
        Args:
            count: Number of messages to peek.
            message_type: Type for deserialization.
            
        Returns:
            List[Message]: List of messages (may be less than count).
        """
        messages = []
        
        if self.queue_structure == RedisStructure.FIFO:
            # Check each priority queue
            for priority in range(100, -1, -1):
                if len(messages) >= count:
                    break
                    
                key = self._get_queue_key(priority)
                # Get from end (what would be dequeued next)
                items = self.redis_client.lrange(key, -count, -1)
                
                for item in reversed(items):
                    if len(messages) < count:
                        messages.append(message_type.from_json(item))
                        
        elif self.queue_structure == RedisStructure.HSET:
            # Get first N from hash
            queue_key = self._get_queue_key()
            cursor = 0
            
            while len(messages) < count:
                cursor, items = self.redis_client.hscan(queue_key, cursor, count=count)
                
                for msg_id, msg_json in items.items():
                    if len(messages) < count:
                        messages.append(message_type.from_json(msg_json))
                
                if cursor == 0:  # Completed full scan
                    break
        
        return messages
    
    def list_messages(self, message_type: Type[Message] = Message,
                     filter_func: Optional[callable] = None) -> List[Message]:
        """
        List all messages with optional filtering.
        
        Args:
            message_type: Type for deserialization.
            filter_func: Optional filter function(message) -> bool.
            
        Returns:
            List[Message]: Filtered list of messages.
        """
        messages = []
        
        if self.queue_structure == RedisStructure.FIFO:
            # Scan all priority queues
            for priority in range(100, -1, -1):
                key = self._get_queue_key(priority)
                items = self.redis_client.lrange(key, 0, -1)
                
                for item in items:
                    msg = message_type.from_json(item)
                    if not filter_func or filter_func(msg):
                        messages.append(msg)
                        
        elif self.queue_structure == RedisStructure.HSET:
            # Get all from hash
            queue_key = self._get_queue_key()
            all_items = self.redis_client.hgetall(queue_key)
            
            for msg_id, msg_json in all_items.items():
                msg = message_type.from_json(msg_json)
                if not filter_func or filter_func(msg):
                    messages.append(msg)
        
        return messages

    def get_queue_size(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        
        Returns:
            Dict[str, Any]: Statistics including total and breakdown.
        """
        stats = {'total': 0}
        
        if self.queue_structure == RedisStructure.FIFO:
            # Count by priority
            stats['by_priority'] = {}
            
            for priority in range(100, -1, -1):
                key = self._get_queue_key(priority)
                count = self.redis_client.llen(key)
                if count > 0:
                    stats['by_priority'][priority] = count
                    stats['total'] += count
                    
        elif self.queue_structure == RedisStructure.HSET:
            # Simple hash size
            queue_key = self._get_queue_key()
            stats['total'] = self.redis_client.hlen(queue_key)
            
            # Add processing count if available
            if self.processing_set_key:
                stats['processing'] = self.redis_client.scard(self.processing_set_key)
        
        return stats

    def clear_queue(self, priority: Optional[int] = None):
        """
        Clear all messages from queue.
        
        Args:
            priority: For FIFO queues, clear only this priority.
        """
        if self.queue_structure == RedisStructure.FIFO:
            if priority is not None:
                # Clear specific priority
                queue_key = self._get_queue_key(priority)
                deleted = self.redis_client.delete(queue_key)
                LOGGER.info(f"Cleared {deleted} messages from priority {priority}")
            else:
                # Clear all priorities
                deleted = 0
                for p in range(101):
                    key = self._get_queue_key(p)
                    if self.redis_client.exists(key):
                        deleted += self.redis_client.delete(key)
                LOGGER.info(f"Cleared {deleted} priority queues")
                
        elif self.queue_structure == RedisStructure.HSET:
            # Clear the hash
            queue_key = self._get_queue_key()
            size = self.redis_client.hlen(queue_key)
            self.redis_client.delete(queue_key)
            
            # Also clear processing set if exists
            if self.processing_set_key:
                self.redis_client.delete(self.processing_set_key)
                
            LOGGER.info(f"Cleared {size} messages from hash queue")
    
    def ack(self, message_id: str) -> bool:
        """
        Acknowledge successful message processing.
        
        For HSET queues: Moves from processing set to completed.
        For FIFO queues: No-op (message already removed by dequeue).
        
        Args:
            message_id: ID of message to acknowledge.
            
        Returns:
            bool: True if acknowledged successfully.
        """
        if self.queue_structure == RedisStructure.HSET and self.processing_set_key:
            # Move from processing to completed
            self.redis_client.srem(self.processing_set_key, message_id)
            
            # Optionally move to a completed set
            completed_key = f"{self.queue_name}:completed"
            self.redis_client.sadd(completed_key, message_id)
            
            if self.event_publisher:
                self._publish_event("acked", message_id)
            return True
            
        # For FIFO, message was already removed by dequeue
        return True
    
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """
        Negative acknowledge - processing failed.
        
        For HSET: Removes from processing, optionally requeues.
        For FIFO: Requeues the message if requested.
        
        Args:
            message_id: ID of message to nack.
            requeue: Whether to requeue the message.
            
        Returns:
            bool: True if nacked successfully.
        """
        if self.queue_structure == RedisStructure.HSET:
            if self.processing_set_key:
                # Remove from processing set
                self.redis_client.srem(self.processing_set_key, message_id)
            
            if not requeue:
                # Move to failed/DLQ
                failed_key = f"{self.queue_name}:failed"
                message = self.get_message(message_id)
                if message:
                    self.redis_client.hset(failed_key, message_id, message.to_json())
                    self.delete_message(message_id)
                    
        elif self.queue_structure == RedisStructure.FIFO and requeue:
            # Need to get the original message and re-enqueue
            # This is tricky as message might be gone - would need to store it somewhere
            LOGGER.warning(f"Cannot requeue message {message_id} in FIFO queue - message already removed")
            return False
        
        if self.event_publisher:
            self._publish_event("nacked", message_id)
            
        return True

    def get_status(self, message_id: str) -> Optional[TaskStatus]:
        """
        Get status of a message (typically for ResultMessage).
        
        Works best with HSET structure for O(1) access.
        
        Args:
            message_id: ID of message to check.
            
        Returns:
            Optional[TaskStatus]: Status if found and message has status field.
        """
        message = self.get_message(message_id, Message)
        
        if message and hasattr(message, 'status'):
            return message.status
            
        return None
    
    def update_status(self, message_id: str, 
                     status: TaskStatus,
                     message_type: Type[Message] = None) -> bool:
        """
        Update status of a message (typically ResultMessage).
        
        Args:
            message_id: ID of message to update.
            status: New status value.
            error: Optional error message.
            result: Optional result value.
            
        Returns:
            bool: True if updated successfully.
        """
        if not message_type:
            message_type = self.default_message_type

        # Get existing message
        message = self.get_message(message_id, message_type)
        
        if not message:
            # Create new ResultMessage if doesn't exist
            raise MessageNotExist(f"The message {message_id} does not exist")
        else:
            # Update existing message
            message.status = status
            
        # Update timestamps based on status
        if status == TaskStatus.RUNNING and not message.started_at:
            message.started_at = datetime.now(timezone.utc).isoformat()
        elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
            message.completed_at = datetime.now(timezone.utc).isoformat()
            if message.started_at:
                start = datetime.fromisoformat(message.started_at)
                message.execution_time = (datetime.now(timezone.utc) - start).total_seconds()
        message.updated_at = datetime.now(timezone.utc).isoformat()

        # Update the message
        success = self.update_message(message_id, message)
        
        if success and self.event_publisher:
            self._publish_event(f"status:{status.value}", message_id)
            
        return success

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
    
    def _publish_event(self, event: str, data: str) -> None:
        """
        Publish event for listeners.
        
        Args:
            event: Event name.
            data: Event data (usually message ID).
        """
        if self.event_publisher:
            channel = f"{self.queue_name}:events:{event}"
            self.redis_client.publish(channel, data)