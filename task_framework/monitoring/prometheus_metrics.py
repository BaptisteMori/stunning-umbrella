from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from prometheus_client.core import CollectorRegistry
import time
import threading
from typing import Optional, Dict, Any

class PrometheusMonitoring:
    """Monitoring Prometheus pour l'orchestrateur de tâches"""
    
    def __init__(self, port: int = 8000, registry: Optional[CollectorRegistry] = None):
        self.port = port
        self.registry = registry or CollectorRegistry()
        self.server_started = False
        
        # Métriques de l'orchestrateur
        self.orchestrator_info = Info(
            'task_orchestrator_info',
            'Informations sur l\'orchestrateur',
            registry=self.registry
        )
        
        self.workers_total = Gauge(
            'task_workers_total',
            'Nombre total de workers configurés',
            registry=self.registry
        )
        
        self.workers_running = Gauge(
            'task_workers_running',
            'Nombre de workers en cours d\'exécution',
            registry=self.registry
        )
        
        self.workers_failed = Gauge(
            'task_workers_failed',
            'Nombre de workers en échec',
            registry=self.registry
        )
        
        self.worker_restarts_total = Counter(
            'task_worker_restarts_total',
            'Nombre total de redémarrages de workers',
            ['worker_id'],
            registry=self.registry
        )
        
        self.worker_uptime_seconds = Gauge(
            'task_worker_uptime_seconds',
            'Temps de fonctionnement du worker en secondes',
            ['worker_id'],
            registry=self.registry
        )
        
        # Métriques des tâches
        self.tasks_processed_total = Counter(
            'task_tasks_processed_total',
            'Nombre total de tâches traitées',
            ['worker_id', 'task_name', 'status'],
            registry=self.registry
        )
        
        self.task_duration_seconds = Histogram(
            'task_duration_seconds',
            'Durée d\'exécution des tâches en secondes',
            ['worker_id', 'task_name'],
            buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, float('inf')),
            registry=self.registry
        )
        
        # Métriques de la queue
        self.queue_size = Gauge(
            'task_queue_size',
            'Taille actuelle de la queue',
            ['queue_name'],
            registry=self.registry
        )
        
        self.queue_processing_rate = Gauge(
            'task_queue_processing_rate',
            'Taux de traitement de la queue (tâches/seconde)',
            ['queue_name'],
            registry=self.registry
        )
        
        # Métriques Redis
        self.redis_connections = Gauge(
            'task_redis_connections',
            'Nombre de connexions Redis actives',
            registry=self.registry
        )
        
        self.redis_memory_usage = Gauge(
            'task_redis_memory_usage_bytes',
            'Utilisation mémoire Redis en bytes',
            registry=self.registry
        )
    
    def start_http_server(self):
        """Démarre le serveur HTTP Prometheus"""
        if not self.server_started:
            start_http_server(self.port, registry=self.registry)
            self.server_started = True
            print(f"Serveur Prometheus démarré sur le port {self.port}")
            print(f"Métriques disponibles sur http://localhost:{self.port}/metrics")
    
    def set_orchestrator_info(self, version: str, workers_count: int, queue_host: str):
        """Met à jour les informations de l'orchestrateur"""
        self.orchestrator_info.info({
            'version': version,
            'workers_configured': str(workers_count),
            'queue_host': queue_host,
            'start_time': str(int(time.time()))
        })
    
    def update_worker_metrics(self, workers_stats: Dict[str, Dict[str, Any]]):
        """Met à jour les métriques des workers"""
        running_count = 0
        failed_count = 0
        
        for worker_id, stats in workers_stats.items():
            status = stats.get('status', 'unknown')
            uptime = stats.get('uptime', 0)
            restart_count = stats.get('restart_count', 0)
            
            if status == 'running':
                running_count += 1
            elif status == 'failed':
                failed_count += 1
            
            # Uptime par worker
            self.worker_uptime_seconds.labels(worker_id=worker_id).set(uptime)
            
            # Redémarrages (compteur, donc on set la valeur actuelle)
            current_restarts = self.worker_restarts_total.labels(worker_id=worker_id)._value._value
            if restart_count > current_restarts:
                self.worker_restarts_total.labels(worker_id=worker_id).inc(restart_count - current_restarts)
        
        # Totaux
        self.workers_total.set(len(workers_stats))
        self.workers_running.set(running_count)
        self.workers_failed.set(failed_count)
    
    def record_task_processed(self, worker_id: str, task_name: str, status: str, duration: float):
        """Enregistre l'exécution d'une tâche"""
        self.tasks_processed_total.labels(
            worker_id=worker_id,
            task_name=task_name,
            status=status
        ).inc()
        
        self.task_duration_seconds.labels(
            worker_id=worker_id,
            task_name=task_name
        ).observe(duration)
    
    def update_queue_metrics(self, queue_name: str, size: int, processing_rate: float = 0):
        """Met à jour les métriques de la queue"""
        self.queue_size.labels(queue_name=queue_name).set(size)
        if processing_rate > 0:
            self.queue_processing_rate.labels(queue_name=queue_name).set(processing_rate)
    
    def update_redis_metrics(self, connections: int, memory_usage: int):
        """Met à jour les métriques Redis"""
        self.redis_connections.set(connections)
        self.redis_memory_usage.set(memory_usage)
