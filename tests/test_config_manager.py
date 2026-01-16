"""Tests for ConfigManager."""

import json
import pytest

from jsling.core.config_manager import ConfigManager
from jsling.database.schema import init_default_configs


class TestConfigManager:
    """Test ConfigManager functionality."""
    
    def test_init_default_configs(self, temp_db):
        """Test initializing default configurations."""
        # Note: temp_db fixture already calls init_default_configs
        
        manager = ConfigManager(temp_db)
        
        # Check sync_interval exists (default is 10)
        value = manager.get("sync_interval")
        assert value is not None
        assert value == "10"
    
    def test_get_config(self, temp_db):
        """Test getting configuration value."""
        manager = ConfigManager(temp_db)
        
        value = manager.get("sync_interval")
        assert value == "10"
    
    def test_get_typed_config(self, temp_db):
        """Test getting typed configuration value."""
        manager = ConfigManager(temp_db)
        
        # Integer type
        value = manager.get_typed("sync_interval")
        assert isinstance(value, int)
        assert value == 10
    
    def test_set_config(self, temp_db):
        """Test setting configuration value."""
        manager = ConfigManager(temp_db)
        
        manager.set("sync_interval", "120")
        value = manager.get("sync_interval")
        assert value == "120"
    
    def test_set_sync_interval_negative(self, temp_db):
        """Test setting sync_interval to -1 (disabled)."""
        manager = ConfigManager(temp_db)
        
        manager.set("sync_interval", "-1")
        value = manager.get_typed("sync_interval")
        assert value == -1
    
    def test_set_sync_interval_invalid(self, temp_db):
        """Test setting invalid sync_interval value."""
        manager = ConfigManager(temp_db)
        
        # sync_interval cannot be 0
        with pytest.raises(ValueError, match="cannot be 0"):
            manager.set("sync_interval", "0")
        
        # sync_interval cannot be less than -1
        with pytest.raises(ValueError, match="must be -1"):
            manager.set("sync_interval", "-2")
    
    def test_list_all_configs(self, temp_db):
        """Test listing all configurations."""
        manager = ConfigManager(temp_db)
        
        configs = manager.list_all()
        assert len(configs) > 0
        
        # Check structure
        for config in configs:
            assert "key" in config
            assert "value" in config
            assert "type" in config
    
    def test_reset_config(self, temp_db):
        """Test resetting configuration to default."""
        manager = ConfigManager(temp_db)
        
        # Modify config
        manager.set("sync_interval", "180")
        assert manager.get("sync_interval") == "180"
        
        # Reset (default is 10)
        success = manager.reset("sync_interval")
        assert success
        assert manager.get("sync_interval") == "10"
    
    def test_get_sync_patterns(self, temp_db):
        """Test getting synchronization patterns."""
        manager = ConfigManager(temp_db)
        
        # Get stream patterns
        patterns = manager.get_stream_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) > 0
        
        # Check pattern structure
        for pattern in patterns:
            assert "type" in pattern
            assert "pattern" in pattern
            assert pattern["type"] in ("suffix", "prefix")
        
        # Get rsync patterns
        rsync_patterns = manager.get_rsync_patterns()
        assert isinstance(rsync_patterns, list)
        assert len(rsync_patterns) > 0
    
    def test_validate_json_patterns(self, temp_db):
        """Test validating JSON pattern format."""
        manager = ConfigManager(temp_db)
        
        # Valid pattern
        valid_pattern = json.dumps([
            {"type": "suffix", "pattern": ".test"}
        ])
        manager.set("stream_sync_patterns", valid_pattern)
        
        # Invalid pattern - not a list
        invalid_pattern = json.dumps({"type": "suffix", "pattern": ".test"})
        with pytest.raises(ValueError, match="must be a JSON array"):
            manager.set("stream_sync_patterns", invalid_pattern)
        
        # Invalid pattern - missing fields
        invalid_pattern2 = json.dumps([{"type": "suffix"}])
        with pytest.raises(ValueError, match="must have 'type' and 'pattern'"):
            manager.set("stream_sync_patterns", invalid_pattern2)
