from typing import Dict
import time

from task_framework.monitoring.metrics import MetricsConnector, MetricType


class RedisMetricsConnector(MetricsConnector):
    """
    Connecteur qui stocke les métriques dans Redis.
    Utile pour dashboard custom ou agrégation.
    """
    
    def __init__(self, redis_client, ttl: int = 3600):
        self.redis = redis_client
        self.ttl = ttl
    
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict = None):
        
        
        # Créer la clé avec tags
        tag_str = ':'.join(f"{k}={v}" for k, v in sorted((tags or {}).items()))
        key = f"metrics:{metric_name}:{tag_str}" if tag_str else f"metrics:{metric_name}"
        
        # Stocker selon le type
        try:
            if metric_type == MetricType.COUNTER:
                # Incrémenter
                self.redis.incrbyfloat(key, value)
                self.redis.expire(key, self.ttl)
                
            elif metric_type == MetricType.GAUGE:
                # Remplacer
                self.redis.setex(key, self.ttl, value)
                
            elif metric_type in [MetricType.HISTOGRAM, MetricType.TIMING]:
                # Ajouter à une liste pour calculs statistiques
                timestamp = time.time()
                self.redis.zadd(f"{key}:histogram", {f"{value}:{timestamp}": timestamp})
                self.redis.expire(f"{key}:histogram", self.ttl)
                
                # Garder seulement les 1000 dernières valeurs
                self.redis.zremrangebyrank(f"{key}:histogram", 0, -1001)
        except:
            pass  # Ignorer les erreurs
