from typing import Dict, Optional

from core.models import TaskMessage
from task_queue.task_queue import Queue, TaskQueue


class ShardedTaskQueue(Queue):
    """
    Queue distribuée sur plusieurs Redis.
    
    TODO: Implémentation complète quand nécessaire.
    Pour l'instant, juste l'interface pour préparer.
    """
    
    def __init__(self, queues: list[TaskQueue]):
        self.queues: list[TaskQueue] = queues
        # TODO: Initialiser les shards
        raise NotImplementedError("ShardedTaskQueue not implemented yet")
    
    def enqueue(self, task_name: str, params: dict = None, priority: int = 0) -> str:
        # TODO: Router vers le bon shard
        raise NotImplementedError()
    
    def dequeue(self, timeout: int = 1) -> Optional[TaskMessage]:
        # TODO: Dequeue round-robin sur tous les shards
        raise NotImplementedError()
    
    def get_queue_size(self) -> Dict[str, int]:
        # TODO: Agréger stats de tous les shards
        raise NotImplementedError()
    
    def peek(self) -> Optional[TaskMessage]:
        # TODO: Peek sur tous les shards
        raise NotImplementedError()