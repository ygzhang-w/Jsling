"""Parameter validation utilities."""

import json
import re
from pathlib import Path
from typing import Any, List, Dict


def validate_worker_id(worker_id: str) -> bool:
    """Validate worker ID format.
    
    Args:
        worker_id: Worker ID to validate
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If invalid
    """
    if not worker_id or len(worker_id) > 100:
        raise ValueError("Worker ID must be 1-100 characters")
    
    # Allow alphanumeric, underscore, hyphen
    if not re.match(r'^[a-zA-Z0-9_-]+$', worker_id):
        raise ValueError("Worker ID can only contain letters, numbers, underscore and hyphen")
    
    return True


def validate_job_id(job_id: str) -> bool:
    """Validate job ID format.
    
    Args:
        job_id: Job ID to validate
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If invalid
    """
    if not job_id or len(job_id) > 100:
        raise ValueError("Job ID must be 1-100 characters")
    
    if not re.match(r'^[a-zA-Z0-9_-]+$', job_id):
        raise ValueError("Job ID can only contain letters, numbers, underscore and hyphen")
    
    return True


def validate_config_value(value: str, value_type: str) -> Any:
    """Validate and convert configuration value based on type.
    
    Args:
        value: String value to validate
        value_type: Expected type (int/bool/string/path/json)
        
    Returns:
        Converted value
        
    Raises:
        ValueError: If validation fails
    """
    if value_type == "int":
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Invalid integer value: {value}")
    
    elif value_type == "bool":
        if value.lower() in ('true', '1', 'yes', 'on'):
            return True
        elif value.lower() in ('false', '0', 'no', 'off'):
            return False
        else:
            raise ValueError(f"Invalid boolean value: {value}")
    
    elif value_type == "path":
        # Just validate format, don't check existence
        try:
            Path(value)
            return value
        except Exception as e:
            raise ValueError(f"Invalid path format: {e}")
    
    elif value_type == "json":
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
    
    elif value_type in ("string", "str"):
        return value
    
    else:
        raise ValueError(f"Unknown value type: {value_type}")


def validate_file_sync_patterns(patterns_json: str) -> List[Dict[str, str]]:
    """Validate file synchronization patterns JSON.
    
    Args:
        patterns_json: JSON string of patterns
        
    Returns:
        Validated list of pattern dictionaries
        
    Raises:
        ValueError: If validation fails
    """
    try:
        patterns = json.loads(patterns_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    
    if not isinstance(patterns, list):
        raise ValueError("Patterns must be a JSON array")
    
    for i, pattern in enumerate(patterns):
        if not isinstance(pattern, dict):
            raise ValueError(f"Pattern {i} must be a dictionary")
        
        if 'type' not in pattern or 'pattern' not in pattern:
            raise ValueError(f"Pattern {i} must have 'type' and 'pattern' fields")
        
        if pattern['type'] not in ('suffix', 'prefix'):
            raise ValueError(f"Pattern {i} type must be 'suffix' or 'prefix'")
        
        if not isinstance(pattern['pattern'], str) or not pattern['pattern']:
            raise ValueError(f"Pattern {i} pattern must be a non-empty string")
    
    return patterns


def validate_sync_interval(value: int) -> bool:
    """Validate sync_interval configuration value.
    
    Args:
        value: Sync interval value
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If invalid
    """
    if value < -1:
        raise ValueError("sync_interval must be -1 (disabled) or positive integer")
    
    if value == 0:
        raise ValueError("sync_interval cannot be 0, use -1 to disable or positive value")
    
    return True


def validate_job_status(status: str) -> bool:
    """Validate job status value.
    
    Args:
        status: Job status
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If invalid
    """
    valid_statuses = ('pending', 'running', 'completed', 'failed', 'cancelled')
    
    if status not in valid_statuses:
        raise ValueError(f"Invalid job status: {status}. Must be one of {valid_statuses}")
    
    return True


def validate_sync_mode(mode: str) -> bool:
    """Validate synchronization mode.
    
    Args:
        mode: Sync mode
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If invalid
    """
    valid_modes = ('dynamic', 'static', 'both')
    
    if mode not in valid_modes:
        raise ValueError(f"Invalid sync mode: {mode}. Must be one of {valid_modes}")
    
    return True
