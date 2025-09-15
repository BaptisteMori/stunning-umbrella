from typing import Dict
import socket

from task_framework.monitoring.metrics import MetricsConnector, MetricType


class StatsDMetricsConnector(MetricsConnector):
    """Connecteur StatsD pour Datadog, Graphite, etc."""
    
    def __init__(self, host: str = 'localhost', port: int = 8125, prefix: str = 'task_framework'):
        self.host = host
        self.port = port
        self.prefix = prefix
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict = None):
        # Format StatsD
        full_name = f"{self.prefix}.{metric_name}"
        
        if metric_type == MetricType.COUNTER:
            metric_str = f"{full_name}:{value}|c"
        elif metric_type == MetricType.GAUGE:
            metric_str = f"{full_name}:{value}|g"
        elif metric_type == MetricType.HISTOGRAM:
            metric_str = f"{full_name}:{value}|h"
        elif metric_type == MetricType.TIMING:
            metric_str = f"{full_name}:{value}|ms"
        
        # Ajouter les tags (format Datadog)
        if tags:
            tag_str = ','.join(f"{k}:{v}" for k, v in tags.items())
            metric_str += f"|#{tag_str}"
        
        # Envoyer en UDP (fire-and-forget)
        try:
            self.socket.sendto(metric_str.encode(), (self.host, self.port))
        except:
            pass  # Ignorer les erreurs silencieusement
