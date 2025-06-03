import argparse
import os
import sys
import time
import signal
import logging
import threading

from ..workers.worker import TaskWorker
from ..queue.redis_queue import RedisTaskQueue
from ..core.registry import TaskRegistry

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DefaultWorkerProcess:
    """Processus worker par défaut du framework"""
    
    def __init__(self, worker_id: str, redis_url: str, queue_name: str):
        self.worker_id = worker_id
        self.redis_url = redis_url
        self.queue_name = queue_name
        self.logger = logging.getLogger(f"Worker-{worker_id}")
        
        # Composants
        self.queue = None
        self.registry = None
        self.worker = None
        self.heartbeat_thread = None
        self.running = True
        
        # Configuration des signaux pour arrêt gracieux
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        signal.signal(signal.SIGINT, self._shutdown_handler)
    
    def initialize(self):
        """Initialise les composants du worker"""
        try:
            # Créer la queue Redis
            self.queue = RedisTaskQueue(redis_url=self.redis_url, queue_name=self.queue_name)
            self.logger.info(f"Connexion Redis établie: {self.redis_url}")
            
            # Créer le registry et découvrir les tâches
            self.registry = TaskRegistry()
            
            # Essayer de découvrir les tâches automatiquement
            # On suppose que les tâches sont dans le package principal
            try:
                # Essayer de découvrir depuis le répertoire courant
                current_dir = os.getcwd()
                if os.path.exists(os.path.join(current_dir, "tasks")):
                    sys.path.insert(0, current_dir)
                    # Obtenir le nom du package depuis le répertoire
                    package_name = os.path.basename(current_dir)
                    self.registry.discover_tasks(package_name)
                    self.logger.info(f"Tâches découvertes depuis {package_name}.tasks")
                else:
                    self.logger.warning("Aucun répertoire 'tasks' trouvé, aucune tâche découverte")
            
            except Exception as e:
                self.logger.warning(f"Impossible de découvrir les tâches automatiquement: {e}")
            
            # Créer le worker
            self.worker = TaskWorker(
                queue=self.queue,
                registry=self.registry,
                worker_id=self.worker_id,
                max_retries=3
            )
            
            self.logger.info(f"Worker {self.worker_id} initialisé")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'initialisation: {e}")
            raise
    
    def start_heartbeat(self):
        """Démarre le thread de heartbeat"""
        def heartbeat_loop():
            while self.running:
                try:
                    # Envoyer heartbeat à Redis
                    heartbeat_key = f"worker:{self.worker_id}:heartbeat"
                    self.queue.redis_client.setex(heartbeat_key, 60, time.time())
                    
                    # Envoyer aussi des stats basiques
                    stats_key = f"worker:{self.worker_id}:stats"
                    stats = {
                        "status": "running",
                        "last_seen": time.time(),
                        "pid": os.getpid()
                    }
                    self.queue.redis_client.hset(stats_key, mapping=stats)
                    self.queue.redis_client.expire(stats_key, 120)
                    
                    time.sleep(5)  # Heartbeat toutes les 5 secondes
                    
                except Exception as e:
                    self.logger.error(f"Erreur heartbeat: {e}")
                    time.sleep(10)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        self.logger.info("Heartbeat démarré")
    
    def run(self):
        """Lance le worker"""
        try:
            # Initialiser
            self.initialize()
            
            # Démarrer le heartbeat
            self.start_heartbeat()
            
            self.logger.info(f"Worker {self.worker_id} prêt à traiter les tâches")
            
            # Lancer la boucle principale du worker
            self.worker.run()
            
        except KeyboardInterrupt:
            self.logger.info("Arrêt demandé par l'utilisateur")
        except Exception as e:
            self.logger.error(f"Erreur fatale dans le worker: {e}")
            raise
        finally:
            self._cleanup()
    
    def _shutdown_handler(self, signum, frame):
        """Gestionnaire de signal pour arrêt gracieux"""
        self.logger.info(f"Signal {signum} reçu, arrêt du worker {self.worker_id}")
        self.running = False
        if self.worker:
            self.worker.stop()
    
    def _cleanup(self):
        """Nettoyage lors de l'arrêt"""
        self.running = False
        
        # Marquer le worker comme arrêté dans Redis
        try:
            if self.queue:
                stats_key = f"worker:{self.worker_id}:stats"
                self.queue.redis_client.hset(stats_key, "status", "stopped")
                self.queue.redis_client.expire(stats_key, 60)
                
                # Supprimer le heartbeat
                heartbeat_key = f"worker:{self.worker_id}:heartbeat"
                self.queue.redis_client.delete(heartbeat_key)
        
        except Exception as e:
            self.logger.error(f"Erreur lors du nettoyage: {e}")
        
        self.logger.info(f"Worker {self.worker_id} arrêté")


def main():
    """Point d'entrée principal du worker process"""
    parser = argparse.ArgumentParser(description="Worker de traitement de tâches")
    
    parser.add_argument("--worker-id", 
                       required=True, 
                       help="ID unique du worker")
    
    parser.add_argument("--redis-url", 
                       default="redis://localhost:6379", 
                       help="URL de connexion Redis")
    
    parser.add_argument("--queue-name", 
                       default="tasks", 
                       help="Nom de la queue à traiter")
    
    parser.add_argument("--log-level",
                       default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Niveau de log")
    
    parser.add_argument("--max-retries",
                       type=int,
                       default=3,
                       help="Nombre maximum de tentatives pour une tâche")
    
    args = parser.parse_args()
    
    # Configurer le niveau de log
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Afficher les informations de démarrage
    print(f"Démarrage du worker {args.worker_id}")
    print(f"  - Redis: {args.redis_url}")
    print(f"  - Queue: {args.queue_name}")
    print(f"  - Log level: {args.log_level}")
    print(f"  - PID: {os.getpid()}")
    
    try:
        # Créer et lancer le worker
        worker_process = DefaultWorkerProcess(
            worker_id=args.worker_id,
            redis_url=args.redis_url,
            queue_name=args.queue_name
        )
        
        worker_process.run()
        
    except KeyboardInterrupt:
        print(f"\nArrêt du worker {args.worker_id} demandé")
        sys.exit(0)
    except Exception as e:
        print(f"Erreur fatale: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
