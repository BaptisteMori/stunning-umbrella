import sys
import os
import time

# Ajouter src/ au path pour pouvoir importer task_framework
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


from task_framework.queue.queue_connectors.redis_queue import RedisTaskQueue
from task_framework import TaskFramework

from typing import List, Dict
import time

queue: RedisTaskQueue = RedisTaskQueue(
    "redis://localhost:6379",
    "default"
)
# Configuration
framework = TaskFramework.setup(
    task_queue=queue, 
    result_queue=queue
)

# 1. Fonction simple
@framework.task()
def send_email(recipient: str, subject: str, body: str) -> dict:
    """Envoie un email."""
    print(f"Sending email to {recipient}: {subject}")
    time.sleep(1)  # Simuler l'envoi
    return {
        "sent": True,
        "recipient": recipient,
        "timestamp": time.time()
    }

# 2. Fonction avec valeurs par défaut
@framework.task(priority=10, max_retries=5)
def process_file(file_path: str, 
                 format: str = "json",
                 compress: bool = False) -> dict:
    """Traite un fichier avec options."""
    print(f"Processing {file_path} as {format}")
    
    if compress:
        print("Compressing output...")
    
    return {
        "file": file_path,
        "format": format,
        "compressed": compress,
        "size": 1024
    }

# 3. Fonction avec types complexes
@framework.task(name="aggregate_data")
def calculate_statistics(numbers: List[float], 
                        options: Dict[str, any] = None) -> dict:
    """Calcule des statistiques."""
    options = options or {}
    
    result = {
        "count": len(numbers),
        "sum": sum(numbers),
        "mean": sum(numbers) / len(numbers) if numbers else 0,
        "min": min(numbers) if numbers else None,
        "max": max(numbers) if numbers else None
    }
    
    if options.get("include_variance", False):
        mean = result["mean"]
        variance = sum((x - mean) ** 2 for x in numbers) / len(numbers)
        result["variance"] = variance
    
    return result

# 4. Fonction avec *args et **kwargs
@framework.task()  # Utilisation du décorateur explicite
def flexible_task(*args, **kwargs):
    """Tâche flexible qui accepte n'importe quoi."""
    print(f"Args: {args}")
    print(f"Kwargs: {kwargs}")
    return {"args_count": len(args), "kwargs_count": len(kwargs)}

# 5. Utilisation
if __name__ == "__main__":
    
    # === Exécution Directe (Synchrone) ===
    
    # Appel direct - exécute localement sans passer par la queue
    result = send_email("user@example.com", "Test", "Hello World")
    print(f"Direct result: {result}")
    
    # === Exécution Asynchrone via Queue ===
    
    # Méthode 1: .delay() avec arguments positionnels et nommés
    task_id1 = send_email.delay(
        "admin@example.com",
        "Important",
        body="Urgent message"
    )
    print(f"Task 1 ID: {task_id1}")
    
    # Méthode 2: .apply_async() style Celery
    task_id2 = process_file.apply_async(
        args=("/data/input.csv",),
        kwargs={"format": "parquet", "compress": True},
        priority=100,  # Override la priorité
        # delay=5  # Délai de 5 secondes
    )
    print(f"Task 2 ID: {task_id2}")
    
    # Méthode 3: Avec valeurs par défaut
    task_id3 = process_file.delay("/data/file.txt")  # Utilise format="json" par défaut
    print(f"Task 3 ID: {task_id3}")
    
    # Méthode 4: Types complexes
    task_id4 = calculate_statistics.delay(
        numbers=[1.5, 2.3, 3.7, 4.1, 5.9],
        options={"include_variance": True}
    )
    print(f"Task 4 ID: {task_id4}")
    
    # === Récupération des Résultats ===
    
    # Attendre et récupérer le résultat
    try:
        result = send_email.get_result(task_id1, timeout=10)
        print(f"Email result: {result}")
    except TimeoutError:
        print("Task not completed in time")
    
    # === Monitoring ===
    
    # Vérifier les tâches enregistrées
    print("\nRegistered tasks:")
    for name, task_class in framework.registry.list_tasks().items():
        print(f"  - {name}: {task_class.__name__}")
    
    # Stats de la queue
    stats = framework.queue.get_stats()
    print(f"\nQueue stats: {stats}")
    
    # === Lancer un Worker ===
    
    print("\nStarting worker...")
    worker = framework.create_worker(worker_id="test-worker")
    
    # Le worker va traiter toutes les tâches enqueued
    try:
        worker.run()  # Ctrl+C pour arrêter
    except KeyboardInterrupt:
        print("Worker stopped")