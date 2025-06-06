```
task_framework/
├── core/
│   ├── __init__.py
│   ├── task.py
│   ├── registry.py
│   └── executor.py
├── queue/
│   ├── __init__.py
│   ├── queue.py
│   ├── models.py
│   └── redis_queue.py
├── workers/
│   ├── __init__.py
│   ├── worker.py
│   └── task_worker.py
├── utils/
│   ├── __init__.py
│   └── string_parsing.py
tests/
├── __init__.py
├── test_task_system.py
├── unit/
│   ├── __init__.py
│   ├── test_task.py
│   ├── test_registry.py
│   └── test_executor.py
└── integration/
    ├── __init__.py
    └── test_end_to_end.py
```