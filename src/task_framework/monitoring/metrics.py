from abc import ABC, abstractmethod
from typing import Dict
from enum import Enum

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMING = "timing"

class MetricsConnector(ABC):
    """
    Interface pour envoyer des métriques sans état local.
    Fire-and-forget pattern.
    """
    
    @abstractmethod
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict[str, str] = None):
        """Send directly a metric."""
        pass
    
    def counter(self, name: str, value: float = 1, tags: Dict = None):
        """Raccourci pour un counter."""
        self.send(name, value, MetricType.COUNTER, tags)
    
    def gauge(self, name: str, value: float, tags: Dict = None):
        """Raccourci pour une gauge."""
        self.send(name, value, MetricType.GAUGE, tags)
    
    def timing(self, name: str, milliseconds: float, tags: Dict = None):
        """Raccourci pour un timing."""
        self.send(name, milliseconds, MetricType.TIMING, tags)


class NoOpMetricsConnector(MetricsConnector):
    """Default connector that does nothing."""
    
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict = None):
        pass  # Silencieusement ignorer


