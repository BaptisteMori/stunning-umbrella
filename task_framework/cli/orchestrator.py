import argparse
import sys
import signal
import time
from pathlib import Path
from ..orchestrator.worker_orchestrator import WorkerOrchestrator
from ..queue.redis_queue import RedisTaskQueue
from ..core.registry import TaskRegistry

def main():
    """Point d'entrée principal de la CLI avec support Prometheus"""
    parser = argparse.ArgumentParser(
        description="Orchestrateur de workers pour framework de tâches",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  task-orchestrator -n 4 -q localhost:6379 myworker.py
  task-orchestrator --workers 8 --queue redis-server:6379 --monitoring-port 9090 worker.py
  task-orchestrator -n 2 -q localhost:6379 --no-monitoring worker.py
        """
    )
    
    parser.add_argument('worker_file', 
                       help='Fichier Python du worker (doit avoir une fonction run())')
    
    parser.add_argument('-n', '--workers', 
                       type=int, 
                       default=4,
                       help='Nombre de workers à démarrer (défaut: 4)')
    
    parser.add_argument('-q', '--queue', 
                       default='localhost:6379',
                       help='Host de la queue Redis (format: host:port, défaut: localhost:6379)')
    
    parser.add_argument('--queue-name',
                       default='tasks',
                       help='Nom de la queue (défaut: tasks)')
    
    parser.add_argument('-r', '--max-restarts',
                       type=int,
                       default=3,
                       help='Nombre maximum de redémarrages par worker (défaut: 3)')
    
    parser.add_argument('-t', '--timeout',
                       type=int,
                       default=30,
                       help='Timeout heartbeat des workers en secondes (défaut: 30)')
    
    # Options de monitoring
    parser.add_argument('--monitoring-port',
                       type=int,
                       default=8000,
                       help='Port pour les métriques Prometheus (défaut: 8000)')
    
    parser.add_argument('--no-monitoring',
                       action='store_true',
                       help='Désactiver le monitoring Prometheus')
    
    parser.add_argument('--version',
                       action='version',
                       version='task-framework 1.0.0')
    
    args = parser.parse_args()
    
    try:
        # Charger le module worker
        worker_path = Path(args.worker_file).resolve()
        if not worker_path.exists():
            raise FileNotFoundError(f"Fichier worker non trouvé: {args.worker_file}")
        
        # Configuration de la queue
        redis_url = f"redis://{args.queue}"
        queue = RedisTaskQueue(redis_url=redis_url, queue_name=args.queue_name)
        
        # Créer le registry
        registry = TaskRegistry()
        
        # Créer l'orchestrateur
        orchestrator = WorkerOrchestrator(
            queue=queue,
            registry=registry,
            num_workers=args.workers,
            max_restarts=args.max_restarts,
            worker_timeout=args.timeout,
            worker_script=args.worker_file,
            enable_monitoring=not args.no_monitoring,
            monitoring_port=args.monitoring_port
        )
        
        # Gestionnaire d'arrêt
        def shutdown_handler(signum, frame):
            print(f"\nSignal {signum} reçu, arrêt de l'orchestrateur...")
            orchestrator.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Information de démarrage
        print(f"Démarrage de l'orchestrateur:")
        print(f"  - Workers: {args.workers}")
        print(f"  - Queue: {redis_url}/{args.queue_name}")
        print(f"  - Script: {args.worker_file}")
        if not args.no_monitoring:
            print(f"  - Monitoring: http://localhost:{args.monitoring_port}/metrics")
        
        # Démarrer l'orchestrateur
        orchestrator.start()
        
        try:
            # Boucle principale avec stats
            while True:
                time.sleep(30)
                stats = orchestrator.get_worker_stats()
                running_workers = sum(1 for w in stats.values() if w['status'] == 'running')
                print(f"Workers actifs: {running_workers}/{args.workers}")
                
        except KeyboardInterrupt:
            pass
        finally:
            orchestrator.stop()
            
    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur")
        sys.exit(0)
    except Exception as e:
        print(f"Erreur: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()