from typing import Dict
import logging

from task_framework.monitoring.metrics import MetricsConnector, MetricType


class LogMetricsConnector(MetricsConnector):
    """Connecteur qui log les métriques (pour debug/dev)."""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('metrics')
    
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict = None):
        tag_str = ','.join(f"{k}={v}" for k, v in (tags or {}).items())
        self.logger.debug(f"[{metric_type.value}] {metric_name}={value} {tag_str}")