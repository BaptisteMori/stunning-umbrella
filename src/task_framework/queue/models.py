import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class TaskMessage:
    """
    A standardized message container for task execution with built-in metadata and serialization.
    
    This dataclass encapsulates all necessary information for task processing, including
    task identification, parameters, retry logic, and automatic ID/timestamp generation.
    Provides JSON serialization capabilities for message queue integration.
    
    Attributes:
        task_name (str): Identifier for the specific task to be executed
        params (dict[str, any]): Dictionary containing task-specific parameters and configuration
        id (str, optional): Unique identifier for the task message. Auto-generated if not provided
        priority (int): Task execution priority level. Defaults to 0 (standard priority)
        retry_count (int): Current number of retry attempts. Defaults to 0
        max_retries (int): Maximum allowed retry attempts before task failure. Defaults to 3
        created_at (str, optional): ISO format timestamp of message creation. Auto-generated if not provided
    
    Example:
        >>> task = TaskMessage(
        ...     task_name="process_data",
        ...     params={"input_file": "data.csv", "output_format": "json"}
        ... )
        >>> task.id  # Auto-generated UUID
        '550e8400-e29b-41d4-a716-446655440000'
   """
    task_name: str
    params: dict[str, any]
    id: str = None
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = None
    
    def __post_init__(self):
        """
        Initialize auto-generated fields after dataclass instantiation.
        
        Generates unique ID and creation timestamp if not explicitly provided,
        ensuring each task message has proper identification and tracking metadata.
        """
        if self.id is None:
            self.id = str(uuid.uuid4())
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
    
    def to_json(self) -> str:
        """
        Serialize the task message to JSON string format.
        
        Converts the dataclass instance to a JSON representation suitable for
        message queue transmission or persistent storage.
        
        Returns:
            str: JSON string representation of the task message
            
        Example:
            >>> task = TaskMessage("process_data", {"file": "test.csv"})
            >>> json_str = task.to_json()
            >>> print(json_str)
            '{"task_name": "process_data", "params": {"file": "test.csv"}, ...}'
        """
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TaskMessage':
        """
        Deserialize a JSON string to create a TaskMessage instance.
        
        Reconstructs a TaskMessage object from its JSON representation,
        enabling message queue consumption and data persistence retrieval.
        
        Args:
            json_str (str): Valid JSON string containing task message data
            
        Returns:
            TaskMessage: Reconstructed task message instance
            
        Raises:
            json.JSONDecodeError: If the input string is not valid JSON
            TypeError: If required fields are missing from the JSON data
            
        Example:
            >>> json_data = '{"task_name": "process_data", "params": {"file": "test.csv"}}'
            >>> task = TaskMessage.from_json(json_data)
            >>> task.task_name
            'process_data'
        """
        data = json.loads(json_str)
        return cls(**data)
