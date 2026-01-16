"""Job configuration YAML parser with Pydantic validation."""

import fnmatch
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class SyncPattern:
    """
    File synchronization pattern matcher.
    
    Supports glob-style patterns:
    - '*'       : matches all files (sync_all)
    - '*.log'   : suffix pattern (matches files ending with .log)
    - 'test_*'  : prefix pattern (matches files starting with test_)
    - 'logs/*'  : directory pattern
    - 'exact.txt': exact match
    """
    
    def __init__(self, pattern: str):
        self.pattern = pattern.strip()
        self.is_sync_all = (self.pattern == '*')
    
    def matches(self, filename: str) -> bool:
        """Check if filename matches this pattern."""
        if self.is_sync_all:
            return True
        return fnmatch.fnmatch(filename, self.pattern)
    
    def matches_path(self, path: str) -> bool:
        """Check if a path matches this pattern."""
        if self.is_sync_all:
            return True
        # Match against filename
        if fnmatch.fnmatch(Path(path).name, self.pattern):
            return True
        # Match against full relative path
        if fnmatch.fnmatch(path, self.pattern):
            return True
        return False
    
    def __repr__(self):
        return f"SyncPattern('{self.pattern}')"


class SyncRules(BaseModel):
    """Synchronization rules configuration.
    
    Patterns support glob syntax:
    - '*'       : sync all files
    - '*.log'   : suffix pattern
    - 'test_*'  : prefix pattern
    - 'logs/*'  : directory pattern
    """
    
    # Stream sync patterns (real-time tail -f)
    stream: List[str] = Field(default_factory=list, description="Stream sync patterns (e.g., '*.log')")
    
    # Download/batch sync patterns
    download: List[str] = Field(default_factory=list, description="Download patterns (e.g., '*.csv', 'result.txt')")
    
    @field_validator('stream', 'download')
    @classmethod
    def validate_patterns(cls, v: List[str]) -> List[str]:
        """Validate patterns are not empty strings."""
        result = []
        for pattern in v:
            if not pattern or not pattern.strip():
                raise ValueError("Pattern cannot be empty")
            result.append(pattern.strip())
        return result
    
    @model_validator(mode='after')
    def check_conflicts(self):
        """Check for conflicts between stream and download patterns.
        
        Raises error if:
        - Both stream and download contain '*' (both sync_all)
        - Both stream and download contain identical patterns
        """
        stream_has_all = '*' in self.stream
        download_has_all = '*' in self.download
        
        # Error if both have sync_all
        if stream_has_all and download_has_all:
            raise ValueError(
                "Conflict: both 'stream' and 'download' contain '*' (sync all). "
                "A file cannot be both streamed and batch-downloaded."
            )
        
        # Error if sync_all appears with other patterns in same category
        if stream_has_all and len(self.stream) > 1:
            raise ValueError(
                "Invalid: 'stream' contains '*' with other patterns. "
                "Use '*' alone for sync_all, or specify individual patterns."
            )
        if download_has_all and len(self.download) > 1:
            raise ValueError(
                "Invalid: 'download' contains '*' with other patterns. "
                "Use '*' alone for sync_all, or specify individual patterns."
            )
        
        # Check for identical patterns in both lists
        common = set(self.stream) & set(self.download)
        if common:
            raise ValueError(
                f"Conflict: patterns {common} appear in both 'stream' and 'download'. "
                "Each file can only be synced one way."
            )
        
        # Check for potential overlaps (e.g., *.log in stream and *.log in download)
        # This is a basic check - runtime will do more thorough checking
        for s_pattern in self.stream:
            for d_pattern in self.download:
                if s_pattern == d_pattern:
                    raise ValueError(
                        f"Conflict: pattern '{s_pattern}' appears in both stream and download"
                    )
        
        return self


class JobConfig(BaseModel):
    """Job configuration model."""
    
    # Required fields
    command: str = Field(..., description="Command to execute on remote worker")
    worker: str = Field(..., description="Worker ID or name")
    
    # Resource configuration (optional, overrides Worker defaults)
    nodes: Optional[int] = Field(None, ge=1, description="Number of nodes to use")
    ntasks_per_node: Optional[int] = Field(None, ge=1, description="Tasks per node")
    gres: Optional[str] = Field(None, description="GRES resources (e.g., gpu:v100:2)")
    gpus_per_task: Optional[int] = Field(None, ge=0, description="GPUs per task")
    
    # File upload (optional)
    upload: List[str] = Field(default_factory=list, description="Files/directories to upload")
    
    # Synchronization rules (optional)
    sync_rules: Optional[SyncRules] = Field(None, description="File sync rules")
    
    # Rsync options (optional)
    rsync_options: Optional[str] = Field(None, description="Additional rsync options")
    rsync_mode: Optional[str] = Field(None, description="Rsync mode: periodic or final_only. If not set, uses database default.")
    rsync_interval: Optional[int] = Field(None, ge=1, description="Rsync interval in seconds")
    
    # Local workdir (optional)
    local_workdir: Optional[str] = Field(None, description="Local working directory")
    
    @field_validator('command')
    @classmethod
    def validate_command(cls, v: str) -> str:
        """Validate command is not empty."""
        if not v or not v.strip():
            raise ValueError("Command cannot be empty")
        return v.strip()
    
    @field_validator('rsync_mode')
    @classmethod
    def validate_rsync_mode(cls, v: Optional[str]) -> Optional[str]:
        """Validate rsync mode."""
        if v is not None and v not in ['periodic', 'final_only']:
            raise ValueError(f"Invalid rsync_mode: {v}. Must be 'periodic' or 'final_only'")
        return v
    
    @model_validator(mode='after')
    def validate_rsync_interval(self):
        """Validate rsync_interval is only set when rsync_mode is periodic or unset."""
        # Only reject if rsync_mode is explicitly set to 'final_only'
        if self.rsync_mode == 'final_only' and self.rsync_interval is not None:
            raise ValueError("rsync_interval is only valid when rsync_mode='periodic'")
        return self


def parse_job_config(yaml_path: str) -> JobConfig:
    """Parse job configuration from YAML file.
    
    Args:
        yaml_path: Path to YAML configuration file
        
    Returns:
        Validated JobConfig object
        
    Raises:
        ValueError: If YAML is invalid or validation fails
        FileNotFoundError: If YAML file not found
    """
    config_path = Path(yaml_path).expanduser()
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")
    
    if not config_path.is_file():
        raise ValueError(f"Path is not a file: {yaml_path}")
    
    # Load YAML
    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML file: {e}")
    
    if not isinstance(data, dict):
        raise ValueError("YAML file must contain a dictionary")
    
    # Validate and return
    try:
        return JobConfig(**data)
    except Exception as e:
        raise ValueError(f"Configuration validation failed: {e}")


def merge_job_config(yaml_config: JobConfig, **cli_overrides) -> dict:
    """Merge YAML configuration with CLI overrides.
    
    CLI parameters take precedence over YAML values.
    
    Args:
        yaml_config: Parsed YAML configuration
        **cli_overrides: CLI parameter overrides
        
    Returns:
        Merged configuration dictionary
    """
    # Start with YAML config
    merged = yaml_config.model_dump(exclude_none=True)
    
    # Apply CLI overrides
    for key, value in cli_overrides.items():
        if value is not None:
            merged[key] = value
    
    return merged
