from abc import ABC, abstractmethod
import logging
from pydantic import BaseModel, ValidationError, ConfigDict
from typing import TYPE_CHECKING, Optional
from enum import Enum
import uuid
from datetime import datetime, timezone

from task_framework.core.exception import TaskQueueNotSet
from task_framework.core.message import TaskMessage, ResultMessage
if TYPE_CHECKING:
    from task_framework.queue.queue import Queue
    from task_framework.core.registry import TaskRegistry


class TaskStatus(Enum):
    NOT_CREATED = "not_created"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"
    CANCELLED = "cancelled"


class TaskParams(BaseModel):
    """
    Base model for task parameter validation using Pydantic.
    
    This class serves as a foundation for parameter validation in task implementations.
    It allows additional fields beyond those explicitly defined, providing flexibility
    for different task types while maintaining validation capabilities.
    """
    model_config = ConfigDict(extra="allow")  # Allow extra fields


class Task(ABC):
    """
    Abstract base class for defining executable tasks with parameter validation.
    
    This class provides a standardized framework for creating tasks with automatic
    parameter validation, logging capabilities, and a consistent interface. Each
    task can define its own parameter validation model and implement custom execution logic.
    
    Attributes:
        params_model (type|None): Optional Pydantic model class for parameter validation
        params (dict): Dictionary containing task parameters
        logger (logging.Logger): Logger instance for the task class
        status (TaskStatus): Current status of the task
    
    Example:
        >>> class MyTask(Task):
        ...     def run(self):
        ...         return f"Processing with params: {self.params}"
        >>> task = MyTask({"key": "value"})
        >>> result = task.run()
    """ 
    # Validation model, to be overridden
    params_model: type[TaskParams] = None
    
    _task_name: str = None
    _queue_name: str = "default"
    _priority: int = 50
    _max_retries: int = 3
    _retry_delay: int = 60
    _timeout: int = 300
    _task_queue: type["Queue"] = None
    _registry: type["TaskRegistry"] = None
    _result_queue: type["Queue"] = None

    _exclude_params: list[str] = [
        "priority", "task_name", "max_retries", "timeout"
    ]

    def __init__(self, **kwargs):
        self.params = self._filter_params(kwargs)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._status = TaskStatus.NOT_CREATED
        self._validate_params()
        
        self._task_name: str = self.get_name()
        self._task_id: str = f"{self._task_name}_{str(uuid.uuid4())}"
        self._retry_count: int = 0

        self._created_at: datetime = datetime.now(timezone.utc)
        self._sended_at: Optional[datetime] = None 
        self._started_at: Optional[datetime] = None
        self._completed_at: Optional[datetime] = None

    def _filter_params(self, params: dict[str: any]):
        """Filter params and set them to the right variable

        Args:
            params (dict[str: any]): dictionary of params
        """        
        for param in params:
            if param in self._exclude_params:
                self.__dict__[f"_{param}"] = params[param]
                params.pop(param)
        return params

    @abstractmethod
    def run(self):
        """
        Execute the task's main logic.
        
        This method must be implemented by all concrete task subclasses to define
        the specific behavior and processing logic for the task.
        
        Returns:
            The result of task execution (type varies by implementation)
        
        Note:
            This is an abstract method that must be overridden in subclasses
        """
        pass

    def before_run(self):
        pass

    def on_failure(self):
        pass

    def on_success(self):
        pass
    
    def execute(self):
        """
        Execute the task with status tracking.
        
        This is a wrapper around the run() method that handles status updates
        and error handling in a consistent way.
        
        Returns:
            The result of task execution
        """
        self._logger.info(f"Starting execution of {self.__class__.__name__}")
        self.status = TaskStatus.RUNNING
        
        try:
            result = self.run()
            self.status = TaskStatus.COMPLETED
            self._logger.info(f"Successfully completed {self.__class__.__name__}")
            return result
        except Exception as e:
            self.status = TaskStatus.FAILED
            self._logger.error(f"Failed to execute {self.__class__.__name__}: {e}")
            raise
    
    def enqueue(self, priority: Optional[int] = None) -> str:
        """
        Enqueue the task.
        
        Usage:
            task = MyTask(param1="value1")
            task_id = task.enqueue(priority=10)
        """
        if not self._task_queue:
            raise TaskQueueNotSet("Queue not configured. Use TaskFramework.setup()")
        
        message = TaskMessage(
            task_id=self._task_id,
            task_name=self._task_name,
            params=self.params,
            priority=priority if priority else self._priority,
            retry_count=self._retry_count,
            max_retries=self._max_retries,
            timeout=self._timeout,
            status=TaskStatus.PENDING.value
        )
        
        self._sended_at = datetime.now(timezone.utc)
        self._task_queue.enqueue(message)
            
        # Save the initial status
        if self._result_queue:
            result_message: ResultMessage = ResultMessage(
                task_id=self._task_id,
                task_name=self._task_name,
                priority=self._priority,
                status=TaskStatus.PENDING.value,
                created_at=self._created_at.isoformat(),
                updated_at=self._created_at.isoformat()
            )

            self._result_queue.enqueue(result_message)
            
        return self._task_id

    def cancel(self) -> bool:
        """Cancel the task if it's not already executed"""
        if self.status in [TaskStatus.PENDING, TaskStatus.RETRY]:
            self.status = TaskStatus.CANCELLED
            if self._result_queue:
                self._result_queue.set_status(
                    self._task_id, TaskStatus.CANCELLED
                )
            return True
        return False

    @classmethod
    def get_name(cls) -> str:
        if not cls._task_name:
            return cls.__name__
        return cls._task_name

    def get_required_params(self) -> list[str]:
        """
        Retrieve the list of required parameter names for this task.
        
        This method can be overridden in subclasses to specify which parameters
        are mandatory for successful task execution. The base implementation
        returns an empty list, indicating no required parameters.
        
        Returns:
            list[str]: List of required parameter names
        """
        return []
    
    def get_param(self, key: str, default=None):
        """
        Safely retrieve a parameter value with optional default fallback.
        
        Args:
            key (str): The parameter name to retrieve
            default (any, optional): Default value to return if key not found.
                Defaults to None.
        
        Returns:
            any: The parameter value if found, otherwise the default value
        """
        return self.params.get(key, default)
    
    def __str__(self):
        """
        Return a string representation of the task instance.
        
        Returns:
            str: String containing the class name and current parameters
        """
        return f"{self.__class__.__name__}({self.params})"
    
    def _validate_params(self):
        """
        Perform automatic parameter validation using Pydantic model if defined.
        
        This method validates the task parameters against the params_model class
        attribute if it exists. The validation ensures type safety and parameter
        correctness before task execution.
        
        Raises:
            ValueError: When parameters fail validation against the defined model,
                with detailed error information from Pydantic
        """
        if self.params_model:
            try:
                validated = self.params_model(**self.params)
                self.params = validated.model_dump()
            except ValidationError as e:
                raise ValueError(f"Invalid parameters for {self.__class__.__name__}: {e}")

    @classmethod
    def delay(cls, **kwargs) -> str:
        """
        Class method to enqueue directly.
        
        Usage:
            task_id = MyTask.delay(param1="value1")
        """
        task = cls(**kwargs)
        return task.enqueue()
