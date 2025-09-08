"""
Exemple complet d'utilisation de la librairie améliorée.
"""
import time
from pathlib import Path

# Vos classes améliorées
from task_framework.config.settings import TaskFrameworkConfig
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue_type.redis_queue import RedisTaskQueue
from task_framework.core.priorities import TaskPriority
from task_framework.workers.task_worker import TaskWorker
from task_framework.core.task import Task, TaskParams
from task_framework.orchestrator.task_orchestrator import TaskOrchestrator
from task_framework.observability.metrics import metrics


class EmailParams(TaskParams):
    recipient: str
    subject: str
    body: str
    priority: str = "normal"


class SendEmailTask(Task):
    params_model = EmailParams
    
    def run(self):
        recipient = self.get_param('recipient')
        subject = self.get_param('subject')
        body = self.get_param('body')
        
        self.logger.info(f"Sending email to {recipient}: {subject}")
        
        # Simulation d'envoi d'email
        time.sleep(0.5)  # Simulate API call
        
        if "fail" in subject.lower():  # Pour tester les erreurs
            raise Exception("Email sending failed")
        
        self.logger.info(f"Email sent successfully to {recipient}")
        return {"status": "sent", "recipient": recipient}


class DataProcessingParams(TaskParams):
    input_file: str
    output_format: str = "json"
    batch_size: int = 100


class DataProcessingTask(Task):
    params_model = DataProcessingParams
    
    def run(self):
        input_file = self.get_param('input_file')
        output_format = self.get_param('output_format')
        batch_size = self.get_param('batch_size')
        
        self.logger.info(f"Processing {input_file} -> {output_format} (batch: {batch_size})")
        
        # Simulation de traitement
        time.sleep(1.0)
        
        return {
            "processed_file": input_file,
            "format": output_format,
            "records_processed": batch_size * 10
        }


def main():
    """Exemple principal d'utilisation."""
    
    # ✅ 1. Configuration centralisée
    config = TaskFrameworkConfig(
        redis={"url": "redis://localhost:6379"},
        worker={"count": 2, "max_retries": 3},
        queue={"name": "example_tasks"},
        logging={"level": "INFO", "structured": True},
        environment="development"
    )
    
    # ✅ 2. Setup du logging
    config.setup_logging()
    
    # ✅ 3. Registry des tâches
    registry = TaskRegistry()
    registry.register("send_email", SendEmailTask)
    registry.register("process_data", DataProcessingTask)
    
    # Alternative : découverte automatique
    # discovered_tasks = discover_tasks("tasks/")
    # for name, task_class in discovered_tasks.items():
    #     registry.register(name, task_class)
    
    # ✅ 4. Queue avec priorités
    queue = RedisTaskQueue(
        redis_url=config.redis.url,
        queue_name=config.queue.name,
        priorities=TaskPriority
    )
    
    # ✅ 5. Enqueue quelques tâches de test
    print("Enqueueing test tasks...")
    
    # Tâche normale
    queue.enqueue(
        "send_email",
        {
            "recipient": "user@example.com",
            "subject": "Welcome!",
            "body": "Welcome to our service"
        },
        priority=TaskPriority.NORMAL
    )
    
    # Tâche haute priorité
    queue.enqueue(
        "process_data",
        {
            "input_file": "/data/important.csv",
            "output_format": "parquet",
            "batch_size": 1000
        },
        priority=TaskPriority.HIGH
    )
    
    # Tâche qui va échouer (pour tester les retries)
    queue.enqueue(
        "send_email",
        {
            "recipient": "test@example.com",
            "subject": "This will fail",
            "body": "Testing error handling"
        },
        priority=TaskPriority.LOW
    )
    
    # ✅ 6. Démarrer l'orchestrateur
    orchestrator = TaskOrchestrator(
        queue=queue,
        registry=registry,
        config=config,
        worker_class=TaskWorker
    )
    
    print(f"Starting orchestrator with {config.worker.count} workers...")
    
    try:
        # Démarrer en arrière-plan pour la démo
        import threading
        orchestrator_thread = threading.Thread(target=orchestrator.start)
        orchestrator_thread.daemon = True
        orchestrator_thread.start()
        
        # ✅ 7. Monitoring et stats
        print("\nMonitoring for 30 seconds...")
        for i in range(30):
            time.sleep(1)
            
            if i % 5 == 0:  # Toutes les 5 secondes
                stats = orchestrator.get_stats()
                health = orchestrator.healthcheck()
                
                print(f"\n--- Stats at {i}s ---")
                print(f"Health: {health['status']}")
                print(f"Active workers: {health['workers']['healthy']}/{health['workers']['total']}")
                print(f"Queue size: {health['queue_size']}")
                print(f"Tasks processed: {stats['counters'].get('tasks_processed', 0)}")
                print(f"Tasks failed: {stats['counters'].get('tasks_failed_permanently', 0)}")
                
                # Metrics détaillées
                if 'histogram_stats' in stats:
                    for metric, data in stats['histogram_stats'].items():
                        if 'task_duration_seconds' in metric:
                            print(f"Task duration avg: {data['avg']:.2f}s (p95: {data['p95']:.2f}s)")
    
    except KeyboardInterrupt:
        print("\nStopping...")
    
    finally:
        # ✅ 8. Stats finales
        print("\n--- Final Stats ---")
        final_stats = metrics.get_stats()
        print("Counters:", final_stats.get('counters', {}))
        print("Gauges:", final_stats.get('gauges', {}))


def example_with_config_file():
    """Exemple avec fichier de configuration."""
    
    # ✅ Créer un fichier de config YAML
    config_yaml = """
redis:
  url: "redis://localhost:6379"
  max_retries: 5

worker:
  count: 4
  max_retries: 3
  task_timeout: 600

queue:
  name: "production_tasks"
  default_priority: 0

logging:
  level: "INFO"
  structured: true

environment: "production"
debug: false
"""
    
    # Sauvegarder le config
    config_path = Path("task_config.yaml")
    config_path.write_text(config_yaml)
    
    try:
        # ✅ Charger depuis fichier
        config = TaskFrameworkConfig.from_file("task_config.yaml")
        
        # ✅ Validation production
        issues = config.validate_production()
        if issues:
            print("Production issues:", issues)
        
        print("Config loaded:", config.to_dict())
        
    finally:
        # Cleanup
        if config_path.exists():
            config_path.unlink()


def example_worker_only():
    """Exemple d'exécution d'un seul worker (utile pour scaling horizontal)."""
    
    config = TaskFrameworkConfig.from_env()
    registry = TaskRegistry()
    registry.register("send_email", SendEmailTask)
    
    queue = RedisTaskQueue(
        redis_url=config.redis.url,
        queue_name=config.queue.name
    )
    
    # ✅ Worker unique avec ID spécifique
    worker = TaskWorker(
        queue=queue,
        registry=registry,
        worker_id="worker-001"  # Pour identifier le worker en prod
    )
    
    print("Starting single worker...")
    try:
        worker.run()
    except KeyboardInterrupt:
        print("Worker stopped")


if __name__ == "__main__":
    # Exemples d'utilisation
    print("=== Configuration File Example ===")
    example_with_config_file()
    
    print("\n=== Full Orchestrator Example ===")
    main()
    
    print("\n=== Single Worker Example ===")
    # example_worker_only()  # Décommentez pour tester