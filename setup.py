from setuptools import setup, find_packages

setup(
    name="task-framework",
    version="1.0.0",
    description="Framework task execution with queues and monitoring",
    author="Baptiste Mori",
    author_email="votre.email@example.com",
    packages=find_packages(),
    install_requires=[
        "redis>=4.0.0",
        "pika>=1.3.0",
        "celery>=5.2.0",
        "pydantic>=1.10.0",
        "psutil>=5.9.0",
        "prometheus-client>=0.16.0",
    ],
    entry_points={
        'console_scripts': [
            'task-orchestrator=task_framework.cli.orchestrator:main',
            'to=task_framework.cli.orchestrator:main',  # Alias
        ],
    },
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
        ],
        "docker": [
            "docker>=6.0.0",
        ],
        "monitoring": [
            "grafana-api>=1.0.3",
            "alertmanager-api>=0.1.0",
        ]
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
