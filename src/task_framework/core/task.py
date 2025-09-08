from abc import ABC, abstractmethod
import logging
from pydantic import BaseModel, ValidationError, ConfigDict
from enum import Enum


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


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
        >>> task.run()
    """
    # Validation model, to be overridden
    params_model: type[TaskParams] = None
    
    def __init__(self, params: dict[str, any] = None):
        self.params = params or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.status = TaskStatus.PENDING
        self._validate_params()
    
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
    
    def execute(self):
        """
        Execute the task with status tracking.
        
        This is a wrapper around the run() method that handles status updates
        and error handling in a consistent way.
        
        Returns:
            The result of task execution
        """
        self.logger.info(f"Starting execution of {self.__class__.__name__}")
        self.status = TaskStatus.RUNNING
        
        try:
            result = self.run()
            self.status = TaskStatus.COMPLETED
            self.logger.info(f"Successfully completed {self.__class__.__name__}")
            return result
        except Exception as e:
            self.status = TaskStatus.FAILED
            self.logger.error(f"Failed to execute {self.__class__.__name__}: {e}")
            raise
    
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