from typing import Dict

from task_framework.monitoring.metrics import MetricsConnector, MetricType


class PrometheusMetricsConnector(MetricsConnector):
    """Connecteur pour Prometheus Pushgateway."""
    
    def __init__(self, pushgateway_url: str, job_name: str = 'task_framework'):
        self.pushgateway_url = pushgateway_url.rstrip('/')
        self.job_name = job_name
        self.batch = []  # Buffer pour batch push
        self.batch_size = 100
    
    def send(self, metric_name: str, value: float, metric_type: MetricType, tags: Dict = None):
        # Formatter pour Prometheus
        labels = ','.join(f'{k}="{v}"' for k, v in (tags or {}).items())
        
        if labels:
            metric_line = f'{metric_name}{{{labels}}} {value}'
        else:
            metric_line = f'{metric_name} {value}'
        
        self.batch.append(metric_line)
        
        # Flush si batch plein
        if len(self.batch) >= self.batch_size:
            self._flush()
    
    def _flush(self):
        """Envoie le batch à Pushgateway."""
        if not self.batch:
            return
        
        import requests
        data = '\n'.join(self.batch)
        
        try:
            requests.post(
                f'{self.pushgateway_url}/metrics/job/{self.job_name}',
                data=data,
                headers={'Content-Type': 'text/plain'},
                timeout=1  # Timeout court pour ne pas bloquer
            )
        except:
            pass  # Ignorer les erreurs
        
        self.batch = []