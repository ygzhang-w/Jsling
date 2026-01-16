"""Configuration manager for system settings."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from jsling.database.models import Config
from jsling.database.schema import reset_config_to_default
from jsling.utils.validators import (
    validate_config_value,
    validate_file_sync_patterns,
    validate_sync_interval
)


class ConfigManager:
    """Manager for system configurations."""
    
    def __init__(self, session: Session):
        """Initialize config manager.
        
        Args:
            session: Database session
        """
        self.session = session
    
    def get(self, key: str) -> Optional[str]:
        """Get configuration value.
        
        Args:
            key: Configuration key
            
        Returns:
            Configuration value as string, or None if not found
        """
        config = self.session.query(Config).filter(Config.config_key == key).first()
        return config.config_value if config else None
    
    def get_typed(self, key: str) -> Any:
        """Get configuration value with type conversion.
        
        Args:
            key: Configuration key
            
        Returns:
            Typed configuration value
            
        Raises:
            KeyError: If key not found
            ValueError: If type conversion fails
        """
        config = self.session.query(Config).filter(Config.config_key == key).first()
        
        if not config:
            raise KeyError(f"Configuration key not found: {key}")
        
        # Convert based on type
        return validate_config_value(config.config_value, config.config_type)
    
    def set(self, key: str, value: str, value_type: Optional[str] = None, 
            description: Optional[str] = None) -> None:
        """Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value (as string)
            value_type: Value type (int/bool/string/path/json), auto-detect if None
            description: Configuration description
            
        Raises:
            ValueError: If validation fails
        """
        # Get existing config to determine type
        existing = self.session.query(Config).filter(Config.config_key == key).first()
        
        if value_type is None and existing:
            value_type = existing.config_type
        elif value_type is None:
            value_type = "string"  # Default type
        
        # Validate value
        validate_config_value(value, value_type)
        
        # Special validation for specific keys
        if key == "sync_interval":
            validate_sync_interval(int(value))
        elif key in ("stream_sync_patterns", "rsync_sync_patterns"):
            validate_file_sync_patterns(value)
        
        # Update or create
        if existing:
            existing.config_value = value
            if description:
                existing.description = description
            existing.updated_at = datetime.now()
        else:
            config = Config(
                config_key=key,
                config_value=value,
                config_type=value_type,
                description=description or "",
                updated_at=datetime.now()
            )
            self.session.add(config)
        
        self.session.commit()
    
    def list_all(self) -> List[Dict[str, Any]]:
        """List all configurations.
        
        Returns:
            List of configuration dictionaries
        """
        configs = self.session.query(Config).all()
        
        result = []
        for config in configs:
            result.append({
                "key": config.config_key,
                "value": config.config_value,
                "type": config.config_type,
                "description": config.description,
                "updated_at": config.updated_at.isoformat()
            })
        
        return result
    
    def reset(self, key: str) -> bool:
        """Reset configuration to default value.
        
        Args:
            key: Configuration key
            
        Returns:
            True if reset successful, False if key not in defaults
        """
        return reset_config_to_default(self.session, key)
    
    def delete(self, key: str) -> bool:
        """Delete a configuration.
        
        Args:
            key: Configuration key
            
        Returns:
            True if deleted, False if not found
        """
        config = self.session.query(Config).filter(Config.config_key == key).first()
        
        if config:
            self.session.delete(config)
            self.session.commit()
            return True
        
        return False
    
    def get_sync_interval(self) -> int:
        """Get sync_interval configuration value.
        
        Returns:
            Sync interval in seconds (-1 for disabled)
            
        Raises:
            KeyError: If sync_interval not found in database
        """
        return self.get_typed("sync_interval")
    
    def get_stream_patterns(self) -> List[Dict[str, str]]:
        """Get stream synchronization patterns.
        
        Returns:
            List of pattern dictionaries
            
        Raises:
            KeyError: If stream_sync_patterns not found in database
        """
        patterns_json = self.get("stream_sync_patterns")
        if not patterns_json:
            raise KeyError(
                "Configuration key 'stream_sync_patterns' not found in database. "
                "Please run 'jsling init' to initialize the database."
            )
        return json.loads(patterns_json)
    
    def get_rsync_patterns(self) -> List[Dict[str, str]]:
        """Get rsync synchronization patterns.
        
        Returns:
            List of pattern dictionaries
            
        Raises:
            KeyError: If rsync_sync_patterns not found in database
        """
        patterns_json = self.get("rsync_sync_patterns")
        if not patterns_json:
            raise KeyError(
                "Configuration key 'rsync_sync_patterns' not found in database. "
                "Please run 'jsling init' to initialize the database."
            )
        return json.loads(patterns_json)
