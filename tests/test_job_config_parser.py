"""Unit tests for job_config_parser module."""

import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from jsling.utils.job_config_parser import (
    JobConfig,
    SyncPattern,
    SyncRules,
    parse_job_config,
    merge_job_config,
)


class TestSyncPattern:
    """Test SyncPattern class (glob-style patterns)"""
    
    def test_suffix_pattern(self):
        """Test creating a suffix pattern (*.log)"""
        pattern = SyncPattern('*.log')
        assert pattern.pattern == '*.log'
        assert pattern.is_sync_all is False
        assert pattern.matches('app.log') is True
        assert pattern.matches('app.txt') is False
    
    def test_prefix_pattern(self):
        """Test creating a prefix pattern (output_*)"""
        pattern = SyncPattern('output_*')
        assert pattern.pattern == 'output_*'
        assert pattern.matches('output_file.txt') is True
        assert pattern.matches('file_output.txt') is False
    
    def test_sync_all_pattern(self):
        """Test sync_all pattern (*)"""
        pattern = SyncPattern('*')
        assert pattern.is_sync_all is True
        assert pattern.matches('anything') is True
    
    def test_exact_match(self):
        """Test exact filename matching"""
        pattern = SyncPattern('result.txt')
        assert pattern.matches('result.txt') is True
        assert pattern.matches('other.txt') is False


class TestSyncRules:
    """Test SyncRules model (new glob-style format)"""
    
    def test_valid_sync_rules(self):
        """Test creating valid sync rules with new format"""
        rules = SyncRules(
            stream=['*.log', '*.out'],
            download=['*.dat', 'result.txt']
        )
        assert len(rules.stream) == 2
        assert len(rules.download) == 2
        assert '*.log' in rules.stream
    
    def test_optional_fields(self):
        """Test that stream and download use default empty lists"""
        rules = SyncRules()
        assert rules.stream == []
        assert rules.download == []
    
    def test_conflict_both_sync_all(self):
        """Test that both stream and download with '*' raises error"""
        with pytest.raises(ValidationError, match="Conflict"):
            SyncRules(stream=['*'], download=['*'])
    
    def test_conflict_identical_patterns(self):
        """Test that identical patterns in both lists raises error"""
        with pytest.raises(ValidationError, match="Conflict"):
            SyncRules(stream=['*.log'], download=['*.log'])
    
    def test_sync_all_with_other_patterns(self):
        """Test that '*' with other patterns is rejected"""
        with pytest.raises(ValidationError, match="Invalid"):
            SyncRules(stream=['*', '*.log'])


class TestJobConfig:
    """Test JobConfig model"""
    
    def test_minimal_config(self):
        """Test minimal valid configuration"""
        config = JobConfig(
            command="python train.py",
            worker="gpu_worker"
        )
        assert config.command == "python train.py"
        assert config.worker == "gpu_worker"
        assert config.rsync_mode is None  # No default, uses database config
    
    def test_nodes_configuration(self):
        """Test nodes configuration for multi-node jobs"""
        config = JobConfig(
            command="python train.py",
            worker="gpu_worker",
            nodes=4
        )
        assert config.nodes == 4
    
    def test_nodes_validation(self):
        """Test that nodes must be >= 1"""
        with pytest.raises(ValidationError):
            JobConfig(
                command="python train.py",
                worker="gpu_worker",
                nodes=0
            )
    
    def test_full_config(self):
        """Test full configuration with all fields"""
        # Note: Can't set rsync_interval with final_only mode
        with pytest.raises(ValidationError, match="only valid when rsync_mode='periodic'"):
            config = JobConfig(
                command="python train.py --epochs 100",
                worker="gpu_worker",
                ntasks_per_node=16,
                gres="gpu:v100:2",
                gpus_per_task=1,
                upload=["data.txt", "config/"],
                sync_rules=SyncRules(
                    stream=["*.log"],
                    download=["*.dat"]
                ),
                rsync_mode="final_only",
                rsync_interval=60,
                rsync_options="-avz --progress"
            )
    
    def test_multiline_command(self):
        """Test multiline command configuration"""
        multiline_cmd = """cd /path/to/project
source activate myenv
python train.py"""
        config = JobConfig(
            command=multiline_cmd,
            worker="gpu_worker"
        )
        assert "cd /path/to/project" in config.command
        assert "python train.py" in config.command
    
    def test_invalid_rsync_mode(self):
        """Test that invalid rsync_mode raises validation error"""
        with pytest.raises(ValidationError, match="Invalid rsync_mode"):
            JobConfig(
                command="python train.py",
                worker="worker1",
                rsync_mode="invalid_mode"
            )
    
    def test_rsync_interval_without_periodic(self):
        """Test that rsync_interval is rejected when rsync_mode != 'periodic'"""
        with pytest.raises(ValidationError, match="only valid when rsync_mode='periodic'"):
            JobConfig(
                command="python train.py",
                worker="worker1",
                rsync_mode="final_only",
                rsync_interval=60
            )
    
    def test_rsync_interval_with_periodic(self):
        """Test that rsync_interval is allowed when rsync_mode = 'periodic'"""
        config = JobConfig(
            command="python train.py",
            worker="worker1",
            rsync_mode="periodic",
            rsync_interval=120
        )
        assert config.rsync_interval == 120
    
    def test_empty_command(self):
        """Test that empty command raises validation error"""
        with pytest.raises(ValidationError):
            JobConfig(
                command="",
                worker="worker1"
            )
    
    def test_missing_required_fields(self):
        """Test that missing required fields raise validation error"""
        with pytest.raises(ValidationError):
            JobConfig()  # Missing command and worker


class TestParseJobConfig:
    """Test parse_job_config function"""
    
    def test_parse_valid_yaml(self, tmp_path):
        """Test parsing valid YAML file with new glob-style patterns"""
        yaml_content = """
command: python train.py --epochs 100
worker: gpu_cluster
ntasks_per_node: 16
gres: gpu:v100:2
rsync_mode: periodic
rsync_interval: 60
upload:
  - ./data/input.txt
  - ./config/
sync_rules:
  stream:
    - "*.log"
    - "*.out"
  download:
    - "*.dat"
    - "result.txt"
"""
        # Create temporary YAML file
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml_content)
        
        config = parse_job_config(str(yaml_path))
        assert config.command == "python train.py --epochs 100"
        assert config.worker == "gpu_cluster"
        assert config.ntasks_per_node == 16
        assert config.gres == "gpu:v100:2"
        assert config.rsync_mode == "periodic"
        assert config.rsync_interval == 60
        assert len(config.upload) == 2
        assert len(config.sync_rules.stream) == 2
        assert len(config.sync_rules.download) == 2
    
    def test_parse_multiline_command_yaml(self, tmp_path):
        """Test parsing YAML with multiline command"""
        yaml_content = """
command: |
  cd /project
  source activate env
  python train.py
worker: gpu_cluster
"""
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml_content)
        
        config = parse_job_config(str(yaml_path))
        assert "cd /project" in config.command
        assert "python train.py" in config.command
    
    def test_parse_minimal_yaml(self, tmp_path):
        """Test parsing minimal YAML file"""
        yaml_content = """
command: echo hello
worker: cpu_worker
"""
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml_content)
        
        config = parse_job_config(str(yaml_path))
        assert config.command == "echo hello"
        assert config.worker == "cpu_worker"
        assert config.rsync_mode is None  # No default, uses database config
    
    def test_parse_invalid_yaml(self, tmp_path):
        """Test parsing invalid YAML raises error"""
        yaml_content = """
command: python train.py
# Missing worker field
"""
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml_content)
        
        with pytest.raises(ValueError, match="validation failed"):
            parse_job_config(str(yaml_path))
    
    def test_parse_nonexistent_file(self):
        """Test parsing nonexistent file raises error"""
        with pytest.raises(FileNotFoundError):
            parse_job_config("/nonexistent/file.yaml")


class TestMergeJobConfig:
    """Test merge_job_config function"""
    
    def test_merge_with_cli_params(self):
        """Test merging YAML config with CLI parameters"""
        yaml_config = JobConfig(
            command="python train.py",
            worker="yaml_worker",
            ntasks_per_node=8,
            gres="gpu:v100:1"
        )
        
        merged = merge_job_config(
            yaml_config,
            worker="cli_worker",  # Override worker
            ntasks_per_node=16,   # Override ntasks
            # gres not specified, should keep YAML value
        )
        
        assert merged["worker"] == "cli_worker"
        assert merged["ntasks_per_node"] == 16
        assert merged["gres"] == "gpu:v100:1"  # From YAML
    
    def test_merge_preserves_yaml_values(self):
        """Test that YAML values are preserved when CLI params not provided"""
        yaml_config = JobConfig(
            command="python train.py",
            worker="yaml_worker",
            ntasks_per_node=8,
            rsync_mode="final_only"
        )
        
        merged = merge_job_config(yaml_config, worker="yaml_worker")
        
        assert merged["ntasks_per_node"] == 8
        assert merged["rsync_mode"] == "final_only"
    
    def test_merge_cli_only(self):
        """Test merging when only CLI params provided (no YAML)"""
        # Create a minimal config
        yaml_config = JobConfig(
            command="python train.py",
            worker="cli_worker"
        )
        
        merged = merge_job_config(
            yaml_config,
            ntasks_per_node=16,
            gres="gpu:v100:2"
        )
        
        assert merged["worker"] == "cli_worker"
        assert merged["ntasks_per_node"] == 16
        assert merged["gres"] == "gpu:v100:2"
    
    def test_merge_upload_lists(self):
        """Test merging upload lists from YAML and CLI"""
        yaml_config = JobConfig(
            command="python train.py",
            worker="worker1",
            upload=["yaml_file1.txt", "yaml_file2.txt"]
        )
        
        # Current implementation overwrites, not extends
        merged = merge_job_config(
            yaml_config,
            worker="worker1",
            upload=["cli_file1.txt", "cli_file2.txt"]
        )
        
        # CLI upload overwrites YAML upload
        assert "cli_file1.txt" in merged["upload"]
        assert "cli_file2.txt" in merged["upload"]
