"""YAML configuration file parser for Worker registration."""

from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class WorkerConfig(BaseModel):
    """Worker configuration model for validation."""
    
    worker_name: str = Field(..., min_length=1, max_length=200)
    host: str = Field(..., min_length=1, max_length=255)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(..., min_length=1, max_length=100)
    auth_method: str = Field(..., pattern="^key$")
    auth_credential: str = Field(..., min_length=1)  # SSH key path
    remote_workdir: str = Field(..., min_length=1, max_length=500)
    queue_name: str = Field(..., min_length=1, max_length=100)
    worker_type: str = Field(..., pattern="^(cpu|gpu)$")
    ntasks_per_node: int = Field(..., gt=0)
    gres: Optional[str] = Field(default=None, max_length=100)
    gpus_per_task: Optional[int] = Field(default=None, gt=0)
    
    @field_validator('auth_credential')
    @classmethod
    def validate_auth_credential(cls, v: str, info) -> str:
        """Validate auth_credential.
        
        For key method: path to SSH key file
        """
        if len(v) < 1:
            raise ValueError("auth_credential cannot be empty")
        return v
    
    @field_validator('gres', 'gpus_per_task')
    @classmethod
    def validate_gpu_fields(cls, v, info) -> Optional[Any]:
        """Validate GPU-related fields for GPU workers."""
        # Will be validated after all fields are parsed
        return v
    
    def model_post_init(self, __context: Any) -> None:
        """Post-initialization validation."""
        # GPU worker must have gres and gpus_per_task
        if self.worker_type == 'gpu':
            if not self.gres:
                raise ValueError("GPU worker must specify 'gres'")
            if not self.gpus_per_task:
                raise ValueError("GPU worker must specify 'gpus_per_task'")
        
        # CPU worker should not have gres or gpus_per_task
        if self.worker_type == 'cpu':
            if self.gres:
                raise ValueError("CPU worker should not specify 'gres'")
            if self.gpus_per_task:
                raise ValueError("CPU worker should not specify 'gpus_per_task'")
    
    class Config:
        """Pydantic configuration."""
        str_strip_whitespace = True


def parse_worker_config(yaml_path: str | Path) -> WorkerConfig:
    """Parse and validate worker configuration from YAML file.
    
    Args:
        yaml_path: Path to YAML configuration file
        
    Returns:
        Validated WorkerConfig object
        
    Raises:
        FileNotFoundError: If YAML file doesn't exist
        yaml.YAMLError: If YAML parsing fails
        pydantic.ValidationError: If configuration validation fails
    """
    yaml_path = Path(yaml_path)
    
    if not yaml_path.exists():
        raise FileNotFoundError(f"Worker configuration file not found: {yaml_path}")
    
    # Read and parse YAML
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    if not isinstance(data, dict):
        raise ValueError("YAML file must contain a dictionary at root level")
    
    # Validate using Pydantic
    config = WorkerConfig(**data)
    
    return config


def worker_config_to_dict(config: WorkerConfig) -> Dict[str, Any]:
    """Convert WorkerConfig to dictionary.
    
    Args:
        config: WorkerConfig object
        
    Returns:
        Dictionary representation
    """
    return config.model_dump(exclude_none=False)
