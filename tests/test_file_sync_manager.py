"""Unit tests for file_sync_manager module."""

import json
import os
import tempfile
import time
import threading
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base, Worker, Job
from jsling.core.file_sync_manager import FileSyncManager
from jsling.utils.job_config_parser import SyncPattern


@pytest.fixture
def temp_db_with_job():
    """Create a temporary database with test worker and job"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    
    # Create test worker
    worker = Worker(
        worker_id="test_worker_001",
        worker_name="test_worker",
        worker_type="gpu",
        host="test.example.com",
        username="testuser",
        auth_method="key",
        auth_credential="/path/to/key",
        remote_workdir="/remote/work",
        queue_name="gpu_queue",
        ntasks_per_node=4
    )
    session.add(worker)
    session.commit()
    
    # Create test job
    job = Job(
        job_id="20240115_103045",
        worker_id=worker.worker_id,
        slurm_job_id=12345,
        script_path="/local/script.sh",
        local_workdir="/local/jobs/20240115_103045",
        remote_workdir="/remote/jobs/20240115_103045",
        job_status="running",
        sync_mode="both",
        rsync_mode="periodic",
        rsync_interval=60
    )
    session.add(job)
    session.commit()
    
    yield session, worker, job
    
    session.close()
    engine.dispose()
    Path(db_path).unlink(missing_ok=True)


class TestSyncPattern:
    """Test SyncPattern class (glob-style patterns)"""
    
    def test_suffix_pattern_matches(self):
        """Test suffix pattern (*.log) matching"""
        pattern = SyncPattern('*.log')
        
        assert pattern.matches('output.log') is True
        assert pattern.matches('error.log') is True
        assert pattern.matches('output.txt') is False
        assert pattern.matches('log.txt') is False
    
    def test_prefix_pattern_matches(self):
        """Test prefix pattern (output_*) matching"""
        pattern = SyncPattern('output_*')
        
        assert pattern.matches('output_file.txt') is True
        assert pattern.matches('output_data.csv') is True
        assert pattern.matches('result_output.txt') is False
        assert pattern.matches('file_output.txt') is False
    
    def test_sync_all_pattern(self):
        """Test sync_all pattern (*) matching"""
        pattern = SyncPattern('*')
        
        assert pattern.is_sync_all is True
        assert pattern.matches('anything.txt') is True
        assert pattern.matches('file.log') is True
        assert pattern.matches('data.csv') is True
    
    def test_directory_pattern_matches(self):
        """Test directory pattern (logs/*) matching"""
        pattern = SyncPattern('logs/*')
        
        assert pattern.matches_path('logs/output.log') is True
        assert pattern.matches_path('logs/error.txt') is True
        assert pattern.matches_path('data/output.log') is False
    
    def test_exact_match(self):
        """Test exact filename matching"""
        pattern = SyncPattern('result.txt')
        
        assert pattern.matches('result.txt') is True
        assert pattern.matches('result.csv') is False
        assert pattern.matches('my_result.txt') is False


class TestFileSyncManager:
    """Test FileSyncManager class"""
    
    def test_init(self, temp_db_with_job):
        """Test FileSyncManager initialization"""
        session, worker, job = temp_db_with_job
        
        sync_manager = FileSyncManager(session, job)
        
        assert sync_manager.job == job
        assert sync_manager.worker == worker
        assert sync_manager.is_running is False
        assert len(sync_manager.stream_threads) == 0
    
    def test_load_default_sync_rules(self, temp_db_with_job):
        """Test loading default sync rules when job has none"""
        session, worker, job = temp_db_with_job
        
        sync_manager = FileSyncManager(session, job)
        
        # Should always have mandatory output/error files as stream patterns
        assert len(sync_manager.stream_patterns) >= 2
        assert any(p.pattern == f'{job.job_id}.out' for p in sync_manager.stream_patterns)
        assert any(p.pattern == f'{job.job_id}.err' for p in sync_manager.stream_patterns)
    
    def test_load_custom_sync_rules(self, temp_db_with_job):
        """Test loading custom sync rules from job (new glob-style format)"""
        session, worker, job = temp_db_with_job
        
        # Set custom sync rules using new format
        sync_rules = {
            "stream": ["*.log", "monitor_*"],
            "download": ["*.dat", "result_*.txt"]
        }
        job.sync_rules = json.dumps(sync_rules)
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        
        # Check stream patterns (2 mandatory + 2 custom = 4 total)
        assert len(sync_manager.stream_patterns) == 4
        # Mandatory patterns first
        assert sync_manager.stream_patterns[0].pattern == f'{job.job_id}.out'
        assert sync_manager.stream_patterns[1].pattern == f'{job.job_id}.err'
        # Custom patterns after
        assert sync_manager.stream_patterns[2].pattern == '*.log'
        assert sync_manager.stream_patterns[3].pattern == 'monitor_*'
        
        # Check download patterns
        assert len(sync_manager.download_patterns) == 2
        assert sync_manager.download_patterns[0].pattern == '*.dat'
        assert sync_manager.download_patterns[1].pattern == 'result_*.txt'
    
    def test_sync_status_update(self, temp_db_with_job):
        """Test that sync status is updated in database"""
        session, worker, job = temp_db_with_job
        
        sync_manager = FileSyncManager(session, job)
        
        # Initially not running
        assert job.sync_status != 'running'
        
        # Note: We can't fully test start_sync without SSH connection
        # This is a limitation for unit tests - would need integration tests
    
    def test_rsync_mode_periodic(self, temp_db_with_job):
        """Test periodic rsync mode"""
        session, worker, job = temp_db_with_job
        
        job.rsync_mode = 'periodic'
        job.rsync_interval = 30
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        
        assert job.rsync_mode == 'periodic'
        assert job.rsync_interval == 30
    
    def test_rsync_mode_final_only(self, temp_db_with_job):
        """Test final_only rsync mode"""
        session, worker, job = temp_db_with_job
        
        job.rsync_mode = 'final_only'
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        
        assert job.rsync_mode == 'final_only'
    
    def test_file_matching(self, temp_db_with_job):
        """Test file matching against sync patterns (new glob-style)"""
        session, worker, job = temp_db_with_job
        
        sync_rules = {
            "stream": ["*.log", "*.out"],
            "download": ["*.dat", "result_*"]
        }
        job.sync_rules = json.dumps(sync_rules)
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        
        # Test stream file matching
        test_files = [
            ("output.log", True),
            ("error.out", True),
            ("data.dat", False),
            ("result_001.txt", False),
            ("script.sh", False)
        ]
        
        for filename, should_match in test_files:
            matches = any(p.matches(filename) for p in sync_manager.stream_patterns)
            assert matches == should_match, f"Failed for {filename}"
    
    def test_multiple_patterns_same_type(self, temp_db_with_job):
        """Test handling multiple patterns"""
        session, worker, job = temp_db_with_job
        
        sync_rules = {
            "stream": ["*.log", "*.out", "monitor_*", "debug_*"]
        }
        job.sync_rules = json.dumps(sync_rules)
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        
        # 2 mandatory + 4 custom = 6 total
        assert len(sync_manager.stream_patterns) == 6
        
        # Test various filenames
        test_cases = [
            ("output.log", True),
            ("error.out", True),
            ("monitor_process.dat", True),
            ("debug_001.txt", True),
            ("normal_file.csv", False)
        ]
        
        for filename, should_match in test_cases:
            matches = any(p.matches(filename) for p in sync_manager.stream_patterns)
            assert matches == should_match, f"Failed for {filename}"


class TestSyncPatternEdgeCases:
    """Test edge cases for sync patterns"""
    
    def test_special_characters_in_pattern(self):
        """Test patterns with special characters"""
        pattern = SyncPattern('*.log.gz')
        assert pattern.matches('output.log.gz') is True
        assert pattern.matches('output.log') is False
    
    def test_case_sensitivity(self):
        """Test that matching is case-sensitive"""
        pattern = SyncPattern('*.LOG')
        assert pattern.matches('output.LOG') is True
        assert pattern.matches('output.log') is False
    
    def test_nested_directory_pattern(self):
        """Test patterns with nested directories"""
        pattern = SyncPattern('logs/*.log')
        assert pattern.matches_path('logs/app.log') is True
        assert pattern.matches_path('logs/error.log') is True
        assert pattern.matches_path('data/app.log') is False


class TestStreamFile:
    """Test _stream_file method with Paramiko channel"""

    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for testing"""
        with tempfile.TemporaryDirectory() as local_dir:
            yield local_dir, "/remote/jobs/test_job"

    @pytest.fixture
    def mock_sync_manager(self, temp_db_with_job, temp_dirs):
        """Create a FileSyncManager with mocked SSH client"""
        session, worker, job = temp_db_with_job
        local_dir, remote_dir = temp_dirs
        
        # Update job paths
        job.local_workdir = local_dir
        job.remote_workdir = remote_dir
        session.commit()
        
        sync_manager = FileSyncManager(session, job)
        sync_manager.is_running = True
        
        # Mock SSH client
        mock_ssh_client = Mock()
        sync_manager.ssh_client = mock_ssh_client
        
        yield sync_manager, mock_ssh_client, local_dir, remote_dir
        
        sync_manager.is_running = False

    def test_stream_file_preserves_relative_path(self, mock_sync_manager):
        """Test that _stream_file preserves relative directory structure"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        # Setup mock channel
        mock_channel = Mock()
        mock_channel.recv_ready.side_effect = [True, False, False]
        mock_channel.recv.return_value = b"test log content\n"
        mock_channel.exit_status_ready.side_effect = [False, True]
        
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_transport.open_session.return_value = mock_channel
        
        mock_paramiko_client = Mock()
        mock_paramiko_client.get_transport.return_value = mock_transport
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value = mock_paramiko_client
            mock_decrypt.return_value = 'test_password'
            
            # Test with nested path
            remote_file = f"{remote_dir}/logs/app.log"
            sync_manager._stream_file(remote_file)
        
        # Verify local file was created with correct path
        expected_local_file = os.path.join(local_dir, "logs", "app.log")
        assert os.path.exists(expected_local_file)
        
        with open(expected_local_file, 'r') as f:
            content = f.read()
        assert "test log content" in content

    def test_stream_file_uses_paramiko_channel(self, mock_sync_manager):
        """Test that _stream_file uses Paramiko channel correctly"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        mock_channel = Mock()
        mock_channel.recv_ready.return_value = False
        mock_channel.exit_status_ready.return_value = True
        
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_transport.open_session.return_value = mock_channel
        
        mock_paramiko_client = Mock()
        mock_paramiko_client.get_transport.return_value = mock_transport
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value = mock_paramiko_client
            mock_decrypt.return_value = 'test_password'
            
            remote_file = f"{remote_dir}/output.log"
            sync_manager._stream_file(remote_file)
        
        # Verify Paramiko was used correctly
        mock_paramiko_client.connect.assert_called_once()
        mock_transport.open_session.assert_called_once()
        mock_channel.exec_command.assert_called_once_with(f"tail -f {remote_file}")
        mock_channel.close.assert_called_once()
        mock_paramiko_client.close.assert_called_once()

    def test_stream_file_handles_inactive_transport(self, mock_sync_manager):
        """Test that _stream_file handles connection error gracefully"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value.connect.side_effect = Exception("Connection failed")
            mock_decrypt.return_value = 'test_password'
            
            remote_file = f"{remote_dir}/output.log"
            
            # Should not raise exception, just log error and return
            sync_manager._stream_file(remote_file)
        
        # No file should be created
        expected_local_file = os.path.join(local_dir, "output.log")
        assert not os.path.exists(expected_local_file)

    def test_stream_file_stops_when_is_running_false(self, mock_sync_manager):
        """Test that _stream_file stops when is_running becomes False"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        call_count = [0]
        
        def recv_ready_side_effect():
            call_count[0] += 1
            if call_count[0] >= 3:
                sync_manager.is_running = False
            return False
        
        mock_channel = Mock()
        mock_channel.recv_ready.side_effect = recv_ready_side_effect
        mock_channel.exit_status_ready.return_value = False
        
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_transport.open_session.return_value = mock_channel
        
        mock_paramiko_client = Mock()
        mock_paramiko_client.get_transport.return_value = mock_transport
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value = mock_paramiko_client
            mock_decrypt.return_value = 'test_password'
            
            remote_file = f"{remote_dir}/output.log"
            sync_manager._stream_file(remote_file)
        
        # Should have stopped after is_running became False
        assert call_count[0] >= 3
        mock_channel.close.assert_called_once()

    def test_stream_file_handles_exception(self, mock_sync_manager):
        """Test that _stream_file handles exceptions gracefully"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value.connect.side_effect = Exception("Connection failed")
            mock_decrypt.return_value = 'test_password'
            
            remote_file = f"{remote_dir}/output.log"
            
            # Should not raise exception, just log error
            sync_manager._stream_file(remote_file)
        
        # No file should be created
        expected_local_file = os.path.join(local_dir, "output.log")
        assert not os.path.exists(expected_local_file)

    def test_stream_file_creates_nested_directories(self, mock_sync_manager):
        """Test that _stream_file creates nested directories as needed"""
        sync_manager, mock_ssh_client, local_dir, remote_dir = mock_sync_manager
        
        mock_channel = Mock()
        mock_channel.recv_ready.side_effect = [True, False]
        mock_channel.recv.return_value = b"data\n"
        mock_channel.exit_status_ready.side_effect = [False, True]
        
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_transport.open_session.return_value = mock_channel
        
        mock_paramiko_client = Mock()
        mock_paramiko_client.get_transport.return_value = mock_transport
        
        with patch('paramiko.SSHClient') as mock_ssh_class, \
             patch('jsling.utils.encryption.decrypt_credential') as mock_decrypt:
            mock_ssh_class.return_value = mock_paramiko_client
            mock_decrypt.return_value = 'test_password'
            
            # Test with deeply nested path
            remote_file = f"{remote_dir}/a/b/c/deep.log"
            sync_manager._stream_file(remote_file)
        
        # Verify nested directories were created
        expected_local_file = os.path.join(local_dir, "a", "b", "c", "deep.log")
        assert os.path.exists(expected_local_file)
