import docker
from typing import Dict, List, Optional
from .worker_orchestrator import WorkerOrchestrator, WorkerInfo, WorkerStatus

class DockerWorkerOrchestrator(WorkerOrchestrator):
    """Orchestrateur utilisant des containers Docker pour les workers"""
    
    def __init__(self, 
                 queue_config: Dict,
                 docker_image: str = "task-worker:latest",
                 **kwargs):
        
        super().__init__(**kwargs)
        self.docker_client = docker.from_env()
        self.docker_image = docker_image
        self.queue_config = queue_config
        self.containers: Dict[str, docker.models.containers.Container] = {}
    
    def _start_worker(self, worker_id: str):
        """Démarre un worker dans un container Docker"""
        if worker_id in self.workers and self.workers[worker_id].status == WorkerStatus.RUNNING:
            return
        
        print(f"Démarrage du container worker {worker_id}")
        
        try:
            # Configuration du container
            environment = {
                "WORKER_ID": worker_id,
                "REDIS_URL": self.queue_config.get("redis_url", "redis://redis:6379"),
                "QUEUE_NAME": self.queue_config.get("queue_name", "tasks")
            }
            
            container = self.docker_client.containers.run(
                self.docker_image,
                command=["python", "-m", "task_framework.workers.worker_process"],
                environment=environment,
                detach=True,
                name=f"worker-{worker_id}",
                restart_policy={"Name": "unless-stopped"},
                labels={"orchestrator": "task-framework", "worker_id": worker_id}
            )
            
            worker_info = WorkerInfo(
                worker_id=worker_id,
                pid=None,  # Pas de PID direct avec Docker
                status=WorkerStatus.STARTING,
                start_time=time.time()
            )
            
            self.workers[worker_id] = worker_info
            self.containers[worker_id] = container
            
            print(f"Container worker {worker_id} démarré (ID: {container.short_id})")
        
        except Exception as e:
            print(f"Erreur lors du démarrage du container worker {worker_id}: {e}")
            if worker_id in self.workers:
                self.workers[worker_id].status = WorkerStatus.FAILED
    
    def _stop_worker(self, worker_id: str, graceful: bool = True):
        """Arrête un container worker"""
        if worker_id not in self.containers:
            return
        
        container = self.containers[worker_id]
        worker = self.workers[worker_id]
        
        print(f"Arrêt du container worker {worker_id}")
        worker.status = WorkerStatus.STOPPING
        
        try:
            if graceful:
                container.stop(timeout=10)
            else:
                container.kill()
            
            container.remove()
            del self.containers[worker_id]
            worker.status = WorkerStatus.STOPPED
            
            print(f"Container worker {worker_id} arrêté")
        
        except Exception as e:
            print(f"Erreur lors de l'arrêt du container worker {worker_id}: {e}")
    
    def _monitor_workers(self):
        """Surveille l'état des containers"""
        for worker_id, worker in list(self.workers.items()):
            if worker_id not in self.containers:
                continue
            
            container = self.containers[worker_id]
            
            try:
                container.reload()
                status = container.status
                
                if status == "running" and worker.status == WorkerStatus.STARTING:
                    worker.status = WorkerStatus.RUNNING
                    print(f"Container worker {worker_id} opérationnel")
                
                elif status in ["exited", "dead"] and worker.status == WorkerStatus.RUNNING:
                    print(f"Container worker {worker_id} s'est arrêté inopinément")
                    if self.running:
                        self._restart_worker(worker_id)
            
            except docker.errors.NotFound:
                print(f"Container worker {worker_id} non trouvé")
                if self.running:
                    self._restart_worker(worker_id)
            
            except Exception as e:
                print(f"Erreur lors de la surveillance du worker {worker_id}: {e}")
