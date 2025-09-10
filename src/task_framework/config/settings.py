from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
import os


class RedisConfig(BaseModel):
    """Redis connection configuration."""
    url: str = Field(default="redis://localhost:6379", description="Redis connection URL")
    max_retries: int = Field(default=3, ge=1, le=10, description="Max connection retry attempts")
    socket_timeout: int = Field(default=5, ge=1, le=60, description="Socket timeout in seconds")


class WorkerConfig(BaseModel):
    """Worker process configuration."""
    count: int = Field(default=4, ge=1, le=100, description="Number of worker processes")
    max_retries: int = Field(default=3, ge=0, le=10, description="Max task retry attempts")
    task_timeout: int = Field(default=300, ge=1, description="Task timeout in seconds")
    queue_timeout: int = Field(default=1, ge=1, le=30, description="Queue polling timeout")


class QueueConfig(BaseModel):
    """Queue configuration."""
    name: str = Field(default="tasks", min_length=1, description="Queue name")
    default_priority: int = Field(default=0, description="Default task priority")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$", description="Log level")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s", description="Log format")
    structured: bool = Field(default=False, description="Use structured logging (JSON)")


class TaskFrameworkConfig(BaseModel):
    """Complete task framework configuration."""
    model_config = ConfigDict(
        env_prefix="TASK_",  # Variables d'environnement TASK_REDIS_URL, etc.
        env_nested_delimiter="__",  # TASK_REDIS__URL
        case_sensitive=False
    )
    
    redis: RedisConfig = Field(default_factory=RedisConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    queue: QueueConfig = Field(default_factory=QueueConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
    # Environnement
    environment: str = Field(default="development", description="Environment name")
    debug: bool = Field(default=False, description="Enable debug mode")
    
    @classmethod
    def from_env(cls) -> "TaskFrameworkConfig":
        """Load configuration from environment variables."""
        return cls()
    
    @classmethod
    def from_file(cls, config_path: str) -> "TaskFrameworkConfig":
        """Load configuration from YAML/JSON file."""
        import json
        import yaml
        from pathlib import Path
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        if config_file.suffix in ['.yaml', '.yml']:
            with open(config_file) as f:
                data = yaml.safe_load(f)
        elif config_file.suffix == '.json':
            with open(config_file) as f:
                data = json.load(f)
        else:
            raise ValueError("Config file must be .yaml, .yml, or .json")
        
        return cls(**data)
    
    def setup_logging(self):
        """Configure logging based on settings."""
        import logging
        import structlog
        
        logging.basicConfig(
            level=getattr(logging, self.logging.level),
            format=self.logging.format
        )
        
        if self.logging.structured:
            # Configure structured logging
            structlog.configure(
                processors=[
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.add_log_level,
                    structlog.processors.JSONRenderer()
                ],
                wrapper_class=structlog.stdlib.BoundLogger,
                logger_factory=structlog.stdlib.LoggerFactory(),
            )
    
    def to_dict(self) -> dict:
        """Export configuration as dictionary."""
        return self.model_dump()
    
    def validate_production(self) -> list[str]:
        """Validate configuration for production use."""
        issues = []
        
        if self.environment == "production":
            if "localhost" in self.redis.url:
                issues.append("Redis URL should not use localhost in production")
            
            if self.debug:
                issues.append("Debug mode should be disabled in production")
            
            if self.logging.level == "DEBUG":
                issues.append("Log level should not be DEBUG in production")
            
            if self.worker.count < 2:
                issues.append("Production should have at least 2 workers")
        
        return issues


# Configuration globale par défaut
config = TaskFrameworkConfig.from_env()

# Usage example:
# config.setup_logging()
# issues = config.validate_production()
# if issues:
#     print("Production issues found:", issues)