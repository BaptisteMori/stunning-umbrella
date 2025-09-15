"""
Exemple d'utilisation de base de la task framework.
À lancer depuis la racine du projet : python examples/basic_usage.py
"""

import sys
import os
import time

# Ajouter src/ au path pour pouvoir importer task_framework
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Imports de votre librairie
from task_framework.core.registry import TaskRegistry
from task_framework.queue.queue_connectors.redis_queue import RedisTaskQueue
from task_framework.core.priorities import TaskPriority
from task_framework.workers.task_worker import TaskWorker
from task_framework.core.decorator import create_task_decorator
from task_framework.core.task import Task, TaskParams, TaskStatus
from task_framework.config.settings import TaskFrameworkConfig
from task_framework.core.message import TaskMessage


def test_basic_functionality():
    """Test de base : registry + queue + worker"""
    
    print("=== TEST 1: FONCTIONNALITÉ DE BASE ===")
    
    # Setup
    registry = TaskRegistry()
    queue = RedisTaskQueue(queue_name="test_basic")
    
    # Test 1: Classe Task classique
    class EmailParams(TaskParams):
        recipient: str
        subject: str
    
    class SendEmailTask(Task):
        params_model = EmailParams
        
        def run(self):
            recipient = self.get_param('recipient')
            subject = self.get_param('subject')
            print(f"📧 Sending email to {recipient}: {subject}")
            return {"sent": True, "recipient": recipient}

    print(f"subclass : {issubclass(SendEmailTask, Task)}")
    # Enregistrer la tâche
    registry.register("SendEmailTask", SendEmailTask)
    
    # Enqueue une tâche
    send_mail_msg = TaskMessage(
        "test_send_mail",
        "SendEmailTask",
        TaskStatus.PENDING.value,
        params={
            "recipient": "test@example.com",
            "subject": "Hello World"
        }
    )
    task_id = queue.enqueue(send_mail_msg)
    
    print(f"✅ Task enqueued: {task_id}")
    
    # Vérifier stats
    stats = queue.get_queue_size()
    print(f"📊 Queue stats: {stats}")
    
    # Test dequeue manuel
    message = queue.dequeue(timeout=2)
    if message:
        print(f"📦 Dequeued: {message.task_name}")
        
        # Exécuter la tâche manuellement
        from task_framework.core.executor import TaskExecutor
        executor = TaskExecutor(registry)
        result = executor.execute_task(message.task_name, message.params)
        print(f"🎯 Task result: {result}")
    else:
        print("❌ No message found")
    
    print("✅ Test 1 completed\n")


def test_decorator_functionality():
    """Test du décorateur"""
    
    print("=== TEST 2: DÉCORATEUR ===")
    
    # Setup
    registry = TaskRegistry()
    queue = RedisTaskQueue(queue_name="test_decorator")
    task = create_task_decorator(registry, queue)
    
    # Définir des tâches avec décorateur
    @task(priority=10)
    def process_file(file_path: str, user_id: int):
        print(f"🔄 Processing {file_path} for user {user_id}")
        time.sleep(0.1)  # Simulation
        return {"processed": file_path, "user_id": user_id}
    
    @task(name="send_notification")
    def notify_user(user_id: int, message: str):
        print(f"🔔 Notifying user {user_id}: {message}")
        return {"notified": user_id}
    
    # Test exécution directe
    print("Direct execution:")
    result1 = process_file("/tmp/test.pdf", 123)
    print(f"✅ Direct result: {result1}")
    
    result2 = notify_user(456, "Your file is ready")
    print(f"✅ Direct result: {result2}")
    
    # Test exécution asynchrone
    print("\nAsync execution:")
    task_id1 = process_file.delay("/tmp/async.pdf", 789)
    print(f"🚀 Async task 1: {task_id1}")
    
    task_id2 = notify_user.delay(101, "Background notification")
    print(f"🚀 Async task 2: {task_id2}")
    
    # Vérifier registry
    registered_tasks = registry.list_tasks()
    print(f"📋 Registered tasks: {list(registered_tasks.keys())}")
    
    # Vérifier queue
    stats = queue.get_queue_size()
    print(f"📊 Queue stats: {stats}")
    
    print("✅ Test 2 completed\n")


def test_worker_processing():
    """Test du worker qui process les tâches"""
    
    print("=== TEST 3: WORKER PROCESSING ===")
    
    # Setup
    registry = TaskRegistry()
    queue = RedisTaskQueue(queue_name="test_worker")
    task = create_task_decorator(registry, queue)
    
    # Vider la queue au cas où
    queue.clear_queue()
    
    # Ajouter quelques tâches
    @task
    def quick_task(name: str):
        print(f"⚡ Quick task for {name}")
        time.sleep(0.2)
        return {"done": name}
    
    @task(priority=TaskPriority.HIGH)
    def priority_task(importance: str):
        print(f"🔥 Priority task: {importance}")
        time.sleep(0.1)
        return {"priority_done": importance}
    
    # Enqueue plusieurs tâches
    print("Enqueueing tasks...")
    quick_task.delay("Alice")
    quick_task.delay("Bob")
    priority_task.delay("URGENT")
    quick_task.delay("Charlie")
    
    stats = queue.get_queue_size()
    print(f"📦 Enqueued 4 tasks, queue size: {stats}")
    
    # Créer un worker
    worker = TaskWorker(queue, registry, worker_id="test-worker")
    
    print("🏃 Starting worker for 8 seconds...")
    
    # Simuler worker en arrière-plan
    import threading
    
    def run_worker():
        try:
            worker.run()
        except Exception as e:
            print(f"Worker error: {e}")
    
    worker_thread = threading.Thread(target=run_worker, daemon=True)
    worker_thread.start()
    
    # Attendre et monitorer
    for i in range(8):
        time.sleep(1)
        current_stats = queue.get_queue_size()
        print(f"  After {i+1}s: {current_stats}")
        
        if current_stats["total"] == 0:
            print("🎉 All tasks processed!")
            break
    
    # Arrêter le worker
    worker.running = False
    time.sleep(0.5)  # Laisser le temps de s'arrêter
    
    final_stats = queue.get_queue_size()
    print(f"📊 Final queue size: {final_stats}")
    print("✅ Test 3 completed\n")


def test_worker_processing_with_dynamic_priority():
    """Test une même tâche avec différentes priorités"""
    
    print("=== TEST 4: PRIORITÉS DYNAMIQUES ===")
    
    registry = TaskRegistry()
    queue = RedisTaskQueue(queue_name="test_priority")
    task = create_task_decorator(registry, queue)
    
    queue.clear_queue()
    
    # UNE SEULE tâche avec priorités différentes
    @task(priority=TaskPriority.NORMAL)
    def process_task(name: str, context: str):
        print(f"Processing {name} from {context}")
        time.sleep(0.2)
        return {"processed": name, "context": context}
    
    # Enqueue la MÊME tâche avec différentes priorités
    print("Enqueueing same task with different priorities...")
    
    # Service routine (priorité normale par défaut)
    process_task.delay("user_data_1", "routine_service")
    process_task.delay("user_data_2", "routine_service")
    
    # Service urgent (priorité haute)
    process_task.delay("urgent_data_1", "urgent_service", priority_override=TaskPriority.HIGH)
    
    # Service critique (priorité critique)
    process_task.delay_critical("critical_data_1", "critical_service")
    
    # Service maintenance (priorité basse)
    process_task.delay_low("background_data_1", "maintenance_service")
    
    stats = queue.get_queue_size()
    print(f"Enqueued 5 tasks with different priorities: {stats}")
    
    # Le worker va traiter dans l'ordre : CRITICAL -> HIGH -> NORMAL -> LOW
    worker = TaskWorker(queue, registry, worker_id="priority-test-worker")
    
    print("Starting worker - watch the execution order...")
    
    import threading
    def run_worker():
        worker.run()
    
    worker_thread = threading.Thread(target=run_worker, daemon=True)
    worker_thread.start()
    
    # Observer l'ordre de traitement
    for i in range(10):
        time.sleep(1)
        current_stats = queue.get_queue_size()
        print(f"  After {i+1}s: {current_stats}")
        
        if current_stats["total"] == 0:
            print("All tasks processed!")
            break
    
    worker.running = False
    time.sleep(0.5)
    
    print("Notice: Critical and High priority tasks were processed first!")
    print("✅ Test 4 completed\n")


def test_configuration():
    """Test de la configuration"""
    
    print("=== TEST 5: CONFIGURATION ===")
    
    # Test config par défaut
    config = TaskFrameworkConfig.from_env()
    print(f"🔧 Environment: {config.environment}")
    print(f"👥 Worker count: {config.worker.count}")
    print(f"🔗 Redis URL: {config.redis.url}")
    
    # Test validation
    issues = config.validate_production()
    if issues:
        print(f"⚠️  Production issues: {issues}")
    else:
        print("✅ No production issues found")
    
    print("✅ Test 5 completed\n")


def check_redis_connection():
    """Vérifier que Redis est accessible"""
    
    print("=== REDIS CONNECTION CHECK ===")
    
    
    queue = RedisTaskQueue(queue_name="connection_test")
    # Test simple
    test_message = TaskMessage(
        task_id="test",
        task_name="testTask",
        status=TaskStatus.PENDING.value,
        params={"data": "ping"}
    )

    test_id = queue.enqueue(test_message)
    message = queue.dequeue(timeout=1)

    if message and message.task_name == "testTask":
        print("✅ Redis connection OK")
        return True
    else:
        print("❌ Redis connection issue")
        return False
            
    # except Exception as e:
    #     print(f"❌ Redis connection failed: {e}")
    #     print("💡 Make sure Redis is running: docker run -p 6379:6379 redis:alpine")
    #     return False


def main():
    """Lancer tous les tests"""
    
    print("🧪 TESTING YOUR TASK FRAMEWORK")
    print("=" * 50)
    
    # Vérifier Redis d'abord
    if not check_redis_connection():
        print("\n❌ Please start Redis first and try again")
        return
    
    try:
        test_basic_functionality()
        test_decorator_functionality() 
        test_worker_processing()
        test_worker_processing_with_dynamic_priority()
        test_configuration()
        
        print("🎉 ALL TESTS PASSED!")
        print("\n✨ Votre librairie fonctionne correctement!")
        print("\n📋 Prochaines étapes possibles:")
        print("   - Créer un projet exemple avec une API")
        print("   - Tester avec plus de workers en parallèle")
        print("   - Implémenter des tâches plus complexes")
        print("   - Ajouter plus de monitoring")
        
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        
        print(f"\n💡 TIPS:")
        print("   - Vérifiez que Redis tourne")
        print("   - Vérifiez les imports dans src/task_framework/")
        print("   - Lancez depuis la racine: python examples/basic_usage.py")


if __name__ == "__main__":
    main()