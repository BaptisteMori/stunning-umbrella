import os
import signal
import time
import threading
import subprocess
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum
import psutil
from ..queue.redis_queue import RedisTaskQueue
from ..core.registry import TaskRegistry
from ..monitoring.prometheus_metrics import PrometheusMonitoring
from ..monitoring.metrics_collector import MetricsCollector

class WorkerStatus(Enum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"

@dataclass
class WorkerInfo:
    """Informations sur un worker"""
    worker_id: str
    pid: Optional[int] = None
    status: WorkerStatus = WorkerStatus.STARTING
    start_time: Optional[float] = None
    restart_count: int = 0
    last_heartbeat: Optional[float] = None
    process: Optional[subprocess.Popen] = None

class WorkerOrchestrator:
    """Orchestrateur de workers avec monitoring Prometheus"""
    
    def __init__(self, 
                 queue: RedisTaskQueue,
                 registry: Optional[TaskRegistry] = None,
                 num_workers: int = 4,
                 max_restarts: int = 5,
                 worker_timeout: int = 30,
                 health_check_interval: int = 5,
                 worker_script: Optional[str] = None,
                 enable_monitoring: bool = True,
                 monitoring_port: int = 8000):
        
        self.queue = queue
        self.registry = registry or TaskRegistry()
        self.num_workers = num_workers
        self.max_restarts = max_restarts
        self.worker_timeout = worker_timeout
        self.health_check_interval = health_check_interval
        self.worker_script = worker_script
        
        self.workers: Dict[str, WorkerInfo] = {}
        self.running = False
        self.orchestrator_thread = None
        self.health_check_thread = None
        
        # Monitoring Prometheus
        self.monitoring = None
        self.metrics_collector = None
        if enable_monitoring:
            self.monitoring = PrometheusMonitoring(port=monitoring_port)
            self.metrics_collector = MetricsCollector(self.monitoring, self.queue)
        
        # Configuration graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
    
    def start(self):
        """Démarre l'orchestrateur et tous les workers"""
        if self.running:
            print("L'orchestrateur est déjà en cours d'exécution")
            return
        
        print(f"Démarrage de l'orchestrateur avec {self.num_workers} workers")
        self.running = True
        
        # Démarrer le monitoring Prometheus
        if self.monitoring:
            self.monitoring.start_http_server()
            self.monitoring.set_orchestrator_info(
                version="1.0.0",
                workers_count=self.num_workers,
                queue_host=f"{self.queue.redis_client.connection_pool.connection_kwargs.get('host', 'localhost')}:{self.queue.redis_client.connection_pool.connection_kwargs.get('port', 6379)}"
            )
            self.metrics_collector.start()
        
        # Démarrer les workers
        for i in range(self.num_workers):
            worker_id = f"worker_{i+1}"
            self._start_worker(worker_id)
        
        # Démarrer le thread de monitoring
        self.orchestrator_thread = threading.Thread(
            target=self._orchestrator_loop, 
            daemon=True
        )
        self.orchestrator_thread.start()
        
        # Démarrer le health check
        self.health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True
        )
        self.health_check_thread.start()
        
        print("Orchestrateur démarré")
        if self.monitoring:
            print(f"Métriques Prometheus disponibles sur http://localhost:{self.monitoring.port}/metrics")
    
    def _start_worker(self, worker_id: str):
        """Démarre un worker"""
        if worker_id in self.workers and self.workers[worker_id].status == WorkerStatus.RUNNING:
            return
        
        print(f"Démarrage du worker {worker_id}")
        
        try:
            if self.worker_script:
                # Utiliser le script personnalisé
                cmd = [
                    "python", 
                    self.worker_script,
                    "--worker-id", worker_id,
                    "--queue-host", f"{self.queue.redis_client.connection_pool.connection_kwargs.get('host', 'localhost')}:{self.queue.redis_client.connection_pool.connection_kwargs.get('port', 6379)}",
                    "--queue-name", self.queue.queue_name
                ]
            else:
                # Utiliser le worker par défaut du framework
                cmd = [
                    "python", "-m", "task_framework.workers.worker_process",
                    "--worker-id", worker_id,
                    "--queue-name", self.queue.queue_name,
                    "--redis-url", f"redis://{self.queue.redis_client.connection_pool.connection_kwargs.get('host', 'localhost')}:{self.queue.redis_client.connection_pool.connection_kwargs.get('port', 6379)}"
                ]
            
            # Variables d'environnement pour le worker
            env = os.environ.copy()
            env.update({
                "WORKER_ID": worker_id,
                "QUEUE_NAME": self.queue.queue_name,
                "REDIS_HOST": str(self.queue.redis_client.connection_pool.connection_kwargs.get('host', 'localhost')),
                "REDIS_PORT": str(self.queue.redis_client.connection_pool.connection_kwargs.get('port', 6379)),
                "PROMETHEUS_ENABLED": "true" if self.monitoring else "false"
            })
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                preexec_fn=os.setsid  # Nouveau groupe de processus
            )
            
            worker_info = WorkerInfo(
                worker_id=worker_id,
                pid=process.pid,
                status=WorkerStatus.STARTING,
                start_time=time.time(),
                process=process
            )
            
            self.workers[worker_id] = worker_info
            print(f"Worker {worker_id} démarré (PID: {process.pid})")
        
        except Exception as e:
            print(f"Erreur lors du démarrage du worker {worker_id}: {e}")
            if worker_id in self.workers:
                self.workers[worker_id].status = WorkerStatus.FAILED
    
    def _restart_worker(self, worker_id: str):
        """Redémarre un worker"""
        if worker_id not in self.workers:
            return
        
        worker = self.workers[worker_id]
        
        if worker.restart_count >= self.max_restarts:
            print(f"Worker {worker_id} a atteint le max de redémarrages ({self.max_restarts})")
            worker.status = WorkerStatus.FAILED
            return
        
        print(f"Redémarrage du worker {worker_id}")
        self._stop_worker(worker_id, graceful=False)
        
        # Attendre un peu avant de redémarrer
        time.sleep(2)
        
        worker.restart_count += 1
        self._start_worker(worker_id)
        
        # Incrémenter la métrique de redémarrage
        if self.monitoring:
            self.monitoring.worker_restarts_total.labels(worker_id=worker_id).inc()
    
    def _orchestrator_loop(self):
        """Boucle principale de l'orchestrateur"""
        while self.running:
            try:
                self._monitor_workers()
                
                # Mettre à jour les métriques Prometheus
                if self.monitoring:
                    stats = self.get_worker_stats()
                    self.monitoring.update_worker_metrics(stats)
                
                time.sleep(2)
            except Exception as e:
                print(f"Erreur dans la boucle de l'orchestrateur: {e}")
                time.sleep(5)
    
    def _monitor_workers(self):
        """Surveille l'état des workers"""
        for worker_id, worker in list(self.workers.items()):
            if not worker.process:
                continue
            
            # Vérifier si le processus est encore vivant
            if worker.process.poll() is not None:
                # Le processus s'est arrêté
                return_code = worker.process.returncode
                
                if worker.status == WorkerStatus.STOPPING:
                    worker.status = WorkerStatus.STOPPED
                    print(f"Worker {worker_id} arrêté proprement")
                else:
                    print(f"Worker {worker_id} s'est arrêté inopinément (code: {return_code})")
                    if self.running:
                        self._restart_worker(worker_id)
            
            elif worker.status == WorkerStatus.STARTING:
                # Vérifier si le worker a fini de démarrer
                if time.time() - worker.start_time > 10:  # 10s max pour démarrer
                    worker.status = WorkerStatus.RUNNING
                    print(f"Worker {worker_id} opérationnel")
    
    def _health_check_loop(self):
        """Vérifie la santé des workers via Redis heartbeat"""
        while self.running:
            try:
                self._check_worker_health()
                time.sleep(self.health_check_interval)
            except Exception as e:
                print(f"Erreur lors du health check: {e}")
                time.sleep(10)
    
    def _check_worker_health(self):
        """Vérifie la santé des workers via heartbeat Redis"""
        for worker_id, worker in self.workers.items():
            if worker.status != WorkerStatus.RUNNING:
                continue
            
            # Vérifier le heartbeat dans Redis
            heartbeat_key = f"worker:{worker_id}:heartbeat"
            last_heartbeat = self.queue.redis_client.get(heartbeat_key)
            
            if last_heartbeat:
                last_time = float(last_heartbeat)
                worker.last_heartbeat = last_time
                
                # Si pas de heartbeat depuis trop longtemps
                if time.time() - last_time > self.worker_timeout:
                    print(f"Worker {worker_id} ne répond plus (timeout)")
                    self._restart_worker(worker_id)
    
    def get_worker_stats(self) -> Dict[str, Dict]:
        """Retourne les statistiques des workers"""
        stats = {}
        for worker_id, worker in self.workers.items():
            stats[worker_id] = {
                "status": worker.status.value,
                "pid": worker.pid,
                "uptime": time.time() - worker.start_time if worker.start_time else 0,
                "restart_count": worker.restart_count,
                "last_heartbeat": worker.last_heartbeat
            }
        return stats
    
    def stop(self, graceful: bool = True, timeout: int = 30):
        """Arrête l'orchestrateur et tous les workers"""
        print("Arrêt de l'orchestrateur...")
        self.running = False
        
        # Arrêter la collecte de métriques
        if self.metrics_collector:
            self.metrics_collector.stop()
        
        if graceful:
            self._graceful_shutdown(timeout)
        else:
            self._force_shutdown()
        
        # Attendre les threads de l'orchestrateur
        if self.orchestrator_thread and self.orchestrator_thread.is_alive():
            self.orchestrator_thread.join(timeout=5)
        
        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=5)
        
        print("Orchestrateur arrêté")
    
    def _graceful_shutdown(self, timeout: int):
        """Arrêt gracieux de tous les workers"""
        print("Arrêt gracieux des workers...")
        
        # Envoyer signal TERM à tous les workers
        for worker_id in list(self.workers.keys()):
            self._stop_worker(worker_id, graceful=True)
        
        # Attendre que tous se terminent
        end_time = time.time() + timeout
        while time.time() < end_time:
            alive_workers = [
                w for w in self.workers.values() 
                if w.process and w.process.poll() is None
            ]
            
            if not alive_workers:
                break
            
            time.sleep(1)
        
        # Force kill les récalcitrants
        self._force_shutdown()
    
    def _force_shutdown(self):
        """Arrêt forcé de tous les workers"""
        for worker_id in list(self.workers.keys()):
            self._stop_worker(worker_id, graceful=False)
    
    def _stop_worker(self, worker_id: str, graceful: bool = True):
        """Arrête un worker"""
        if worker_id not in self.workers:
            return
        
        worker = self.workers[worker_id]
        if worker.status in [WorkerStatus.STOPPING, WorkerStatus.STOPPED]:
            return
        
        print(f"Arrêt du worker {worker_id}")
        worker.status = WorkerStatus.STOPPING
        
        if worker.process and worker.process.poll() is None:
            try:
                if graceful:
                    # Arrêt gracieux
                    worker.process.terminate()
                    try:
                        worker.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        worker.process.kill()
                else:
                    # Arrêt forcé
                    worker.process.kill()
                
                worker.status = WorkerStatus.STOPPED
                print(f"Worker {worker_id} arrêté")
            
            except Exception as e:
                print(f"Erreur lors de l'arrêt du worker {worker_id}: {e}")
    
    def _shutdown_handler(self, signum, frame):
        """Gestionnaire de signal pour arrêt propre"""
        print(f"\nSignal {signum} reçu, arrêt de l'orchestrateur...")
        self.stop()