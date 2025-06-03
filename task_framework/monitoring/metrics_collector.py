import time
import threading
from ..queue.redis_queue import RedisTaskQueue
from .prometheus_metrics import PrometheusMonitoring

class MetricsCollector:
    """Collecteur de métriques en arrière-plan"""
    
    def __init__(self, 
                 monitoring: PrometheusMonitoring,
                 queue: RedisTaskQueue,
                 collection_interval: int = 10):
        self.monitoring = monitoring
        self.queue = queue
        self.collection_interval = collection_interval
        self.running = False
        self.collector_thread = None
        
        # Statistiques pour calcul du taux de traitement
        self.last_queue_size = 0
        self.last_check_time = time.time()
        self.processed_tasks_count = 0
    
    def start(self):
        """Démarre la collecte de métriques"""
        if self.running:
            return
        
        self.running = True
        self.collector_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self.collector_thread.start()
        print("Collecteur de métriques démarré")
    
    def stop(self):
        """Arrête la collecte de métriques"""
        self.running = False
        if self.collector_thread and self.collector_thread.is_alive():
            self.collector_thread.join(timeout=5)
        print("Collecteur de métriques arrêté")
    
    def _collection_loop(self):
        """Boucle principale de collecte"""
        while self.running:
            try:
                self._collect_queue_metrics()
                self._collect_redis_metrics()
                time.sleep(self.collection_interval)
            except Exception as e:
                print(f"Erreur lors de la collecte de métriques: {e}")
                time.sleep(30)  # Attendre plus longtemps en cas d'erreur
    
    def _collect_queue_metrics(self):
        """Collecte les métriques de la queue"""
        try:
            # Taille de la queue
            current_size = self.queue.redis_client.llen(self.queue.queue_name)
            
            # Calcul du taux de traitement
            current_time = time.time()
            time_elapsed = current_time - self.last_check_time
            
            if time_elapsed > 0:
                # Estimation basée sur la diminution de la taille de la queue
                size_diff = self.last_queue_size - current_size
                if size_diff > 0:  # La queue a diminué = tâches traitées
                    processing_rate = size_diff / time_elapsed
                else:
                    processing_rate = 0
                
                self.monitoring.update_queue_metrics(
                    queue_name=self.queue.queue_name,
                    size=current_size,
                    processing_rate=processing_rate
                )
            
            self.last_queue_size = current_size
            self.last_check_time = current_time
        
        except Exception as e:
            print(f"Erreur collecte queue: {e}")
    
    def _collect_redis_metrics(self):
        """Collecte les métriques Redis"""
        try:
            # Informations Redis
            redis_info = self.queue.redis_client.info()
            
            # Connexions
            connected_clients = redis_info.get('connected_clients', 0)
            
            # Utilisation mémoire
            used_memory = redis_info.get('used_memory', 0)
            
            self.monitoring.update_redis_metrics(
                connections=connected_clients,
                memory_usage=used_memory
            )
        
        except Exception as e:
            print(f"Erreur collecte Redis: {e}")