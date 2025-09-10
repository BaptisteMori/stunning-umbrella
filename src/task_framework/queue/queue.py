from abc import ABC, abstractmethod
from typing import Dict, Optional, List

from task_framework.core.message import Message
from task_framework.core.priorities import TaskPriority


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
        """Ajouter une tâche à la queue."""
        pass
    
    @abstractmethod
    def dequeue(self, timeout: int = 1) -> Optional[Message]:
        """Récupérer une tâche de la queue."""
        pass
    
    @abstractmethod
    def peek(self, count: int = 1) -> List[Message]:
        """Check the next messages without deleting them."""
        pass
    
    @abstractmethod
    def ack(self, message_id: str) -> bool:
        """Acknowledge a message ( for the queue that support it)."""
        pass
    
    @abstractmethod
    def nack(self, message_id: str, requeue: bool = True) -> bool:
        """Negative acknowledge."""
        pass

    @abstractmethod
    def get_queue_size(self) -> Dict[str, int]:
        """Stats of the queue."""
        pass

    @abstractmethod
    def delete(self, message_id: str) -> bool:
        """Delete a message in the queue"""
        pass

    @abstractmethod
    def get_status(self, message_id: str) -> bool:
        """Get the status of a message"""
        pass