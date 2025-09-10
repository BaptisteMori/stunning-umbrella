from typing import List, Optional, Callable, Dict
import hashlib


from task_framework.queue.queue import Queue
from task_framework.core.task import TaskMessage

class ShardingPolicy:
    """Politique de sélection de shard."""
    
    @staticmethod
    def round_robin(shards: List[Queue], message: TaskMessage, state: Dict) -> Queue:
        """Distribution round-robin."""
        state['counter'] = state.get('counter', 0)
        shard = shards[state['counter'] % len(shards)]
        state['counter'] += 1
        return shard
    
    @staticmethod
    def hash_based(shards: List[Queue], message: TaskMessage, state: Dict) -> Queue:
        """Distribution basée sur le hash du task_id."""
        hash_value = int(hashlib.md5(message.task_id.encode()).hexdigest(), 16)
        return shards[hash_value % len(shards)]
    
    @staticmethod
    def priority_based(shards: List[Queue], message: TaskMessage, state: Dict) -> Queue:
        """Shards dédiés par niveau de priorité."""
        if message.priority >= 100:  # Critical
            return shards[0]
        elif message.priority >= 50:  # High
            return shards[min(1, len(shards)-1)]
        else:  # Normal/Low
            return shards[-1]
    
    @staticmethod
    def least_loaded(shards: List[Queue], message: TaskMessage, state: Dict) -> Queue:
        """Sélectionne le shard avec le moins de messages."""
        min_size = float('inf')
        selected = shards[0]
        
        for shard in shards:
            stats = shard.get_stats()
            size = stats.get('total', 0)
            if size < min_size:
                min_size = size
                selected = shard
                
        return selected

class ShardedQueue(Queue):
    """
    Distributed queue on multiple shards.
    """
    
    def __init__(self, shards: List[Queue], 
                 policy: Callable = ShardingPolicy.round_robin):
        self.shards = shards
        self.policy = policy
        self.state = {}  # État pour les politiques stateful
        
    def enqueue(self, message: TaskMessage) -> bool:
        """Enqueue vers le shard approprié."""
        shard = self.policy(self.shards, message, self.state)
        return shard.enqueue(message)
    
    def dequeue(self, timeout: int = 1) -> Optional[TaskMessage]:
        """Dequeue depuis tous les shards (round-robin)."""
        # Essayer chaque shard avec un timeout court
        timeout_per_shard = max(1, timeout // len(self.shards))
        
        for shard in self.shards:
            message = shard.dequeue(timeout=timeout_per_shard)
            if message:
                return message
                
        return None
    
    def get_stats(self) -> Dict[str, int]:
        """Agrège les stats de tous les shards."""
        total_stats = {'total': 0, 'shards': {}}
        
        for i, shard in enumerate(self.shards):
            shard_stats = shard.get_stats()
            total_stats[f'shard_{i}'] = shard_stats
            total_stats['total'] += shard_stats.get('total', 0)
            
        return total_stats