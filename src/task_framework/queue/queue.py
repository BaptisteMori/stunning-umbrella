from abc import abstractmethod
from typing import Dict, Optional, List, Type
from enum import Enum

from task_framework.core.message import Message
from task_framework.core.priorities import TaskPriority
from task_framework.core.task import TaskStatus


class QueueType(Enum):
    """Enumeration of different queue types in the system."""
    TASK = "task"
    RESULT = "result"
    DLQ = "dlq"  # Dead Letter Queue
    RETRY = "retry"


class Queue:
    """
    Interface pour différents backends de queue.
    
    Permet d'échanger facilement l'implémentation sans changer
    le code utilisateur. Prépare pour sharding/pooling futur.
    """
    def __init__(
            self, queue_name: str, 
            priorities: type[TaskPriority] = TaskPriority
            ):
        self.queue_name: str = queue_name
        self.priorities: type[TaskPriority] = priorities
    
    @abstractmethod
    def enqueue(self, message: Message) -> str:
        """
        Add a message to the specified queue.
        
        Args:
            message: Message instance to enqueue.
            queue_name: Name of the target queue. Defaults to "default".
            priority: Message priority (higher = more important). Defaults to 0.
            
        Returns:
            str: Message ID of the enqueued message.
            
        Raises:
            QueueException: If enqueueing fails.
        """
        pass
    
    @abstractmethod
    def dequeue(self, timeout: int = 1, message_type: Type[Message] = Message) -> Optional[Message]:
        """
        Remove and return the next message from the queue.
        
        Args:
            queue_name: Name of the queue to dequeue from. Defaults to "default".
            timeout: Seconds to wait for a message. Defaults to 1.
            
        Returns:
            Optional[Message]: Next message or None if queue is empty/timeout.
            
        Raises:
            QueueException: If dequeueing fails.
        """
        pass
    
    @abstractmethod
    def get_message(self, message_id: str, message_type: Type[Message]) -> Optional[Message]:
        """
        Retrieve a specific message by ID without removing it.
        
        Args:
            message_id: Unique identifier of the message.
            message_type: Class type for deserialization.
            
        Returns:
            Optional[Message]: Message if found, None otherwise.
            
        Example:
            msg = queue.get_message("task-123", "results", ResultMessage)
        """
        pass

    @abstractmethod
    def update_message(self, message_id: str, message: Message, ) -> bool:
        """
        Update an existing message in the queue.
        
        Args:
            message_id: ID of the message to update.
            message: New message content.
            
        Returns:
            bool: True if update successful, False otherwise.
        """
        pass

    @abstractmethod
    def delete_message(self, message_id: str) -> bool:
        """
        Remove a specific message from the queue.
        
        Args:
            message_id: ID of the message to delete.
            
        Returns:
            bool: True if deletion successful, False otherwise.
        """
        pass

    @abstractmethod
    def peek(self, count: int = 1, message_type: Type[Message] = None) -> List[Message]:
        """
        View messages without removing them from the queue.
        
        Args:
            count: Number of messages to peek. Defaults to 1.
            message_type: Type for deserialization. Required for typed results.
            
        Returns:
            List[Message]: List of messages (may be empty).
        """
        pass
    
    def list_messages(self, message_type: Type[Message],
                     filter_func: Optional[callable] = None) -> List[Message]:
        """
        List all messages in a queue with optional filtering.
        
        Args:
            queue_name: Queue to list messages from.
            message_type: Type for deserialization.
            filter_func: Optional filter function(message) -> bool.
            
        Returns:
            List[Message]: Filtered list of messages.
            
        Example:
            # Get all failed results
            failed = queue.list_messages(
                "results", 
                ResultMessage,
                lambda m: m.status == TaskStatus.FAILED
            )
        """
        pass

    @abstractmethod
    def get_queue_size(self) -> Dict[str, int]:
        """
        Get statistics for queue(s).
        
        Args:
            queue_name: Specific queue name or None for all queues.
            
        Returns:
            Dict[str, any]: Statistics dictionary with queue metrics.
        """
        pass

    @abstractmethod
    def get_status(self, message_id: str) -> bool:
        """Get the status of a message"""
        pass

    @abstractmethod
    def update_status(self, message_id: str, status: TaskStatus) -> bool:
        """Update the status of a message"""
        pass