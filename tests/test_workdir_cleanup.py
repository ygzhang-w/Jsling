"""Tests for workdir_cleanup utility module."""

import os
import tempfile
import shutil
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from jsling.utils.workdir_cleanup import delete_workdirs, print_workdir_summary


class MockJob:
    """Mock Job object for testing."""
    
    def __init__(self, job_id, worker_id=None, local_workdir=None, remote_workdir=None, worker=None):
        self.job_id = job_id
        self.worker_id = worker_id
        self.local_workdir = local_workdir
        self.remote_workdir = remote_workdir
        self.worker = worker


class TestDeleteWorkdirs:
    """Test cases for delete_workdirs function."""
    
    def test_delete_local_workdir_exists(self):
        """Test deleting local workdir when it exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_dir = os.path.join(tmpdir, "test_job")
            os.makedirs(local_dir)
            
            job = MockJob("test_job_001", local_workdir=local_dir)
            console = Console(file=MagicMock())
            
            remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
                [job], cleanup_remote=False, cleanup_local=True, console=console
            )
            
            assert local_cleaned == 1
            assert local_failed == 0
            assert not os.path.exists(local_dir)
    
    def test_delete_local_workdir_not_exists(self):
        """Test deleting local workdir when it does not exist."""
        job = MockJob("test_job_001", local_workdir="/nonexistent/path")
        console = Console(file=MagicMock())
        
        remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
            [job], cleanup_remote=False, cleanup_local=True, console=console
        )
        
        # Should count as cleaned since goal is achieved
        assert local_cleaned == 1
        assert local_failed == 0
    
    def test_delete_no_cleanup_flags(self):
        """Test with no cleanup flags set."""
        job = MockJob("test_job_001", local_workdir="/tmp/test", remote_workdir="/scratch/test")
        console = Console(file=MagicMock())
        
        remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
            [job], cleanup_remote=False, cleanup_local=False, console=console
        )
        
        assert remote_cleaned == 0
        assert remote_failed == 0
        assert local_cleaned == 0
        assert local_failed == 0
    
    @patch('jsling.connections.worker.Worker')
    def test_delete_remote_workdir_success(self, mock_worker_conn_class):
        """Test deleting remote workdir successfully."""
        mock_worker = MagicMock()
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True)
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        mock_worker_conn.test_connection.return_value = True
        mock_worker_conn_class.return_value = mock_worker_conn
        
        job = MockJob("test_job_001", worker_id="worker1", 
                      remote_workdir="/scratch/test", worker=mock_worker)
        console = Console(file=MagicMock())
        
        remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
            [job], cleanup_remote=True, cleanup_local=False, console=console
        )
        
        assert remote_cleaned == 1
        assert remote_failed == 0
        mock_ssh.run.assert_called_once()
    
    @patch('jsling.connections.worker.Worker')
    def test_delete_remote_workdir_failure(self, mock_worker_conn_class):
        """Test deleting remote workdir when it fails."""
        mock_worker = MagicMock()
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=False)
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        mock_worker_conn.test_connection.return_value = True
        mock_worker_conn_class.return_value = mock_worker_conn
        
        job = MockJob("test_job_001", worker_id="worker1",
                      remote_workdir="/scratch/test", worker=mock_worker)
        console = Console(file=MagicMock())
        
        remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
            [job], cleanup_remote=True, cleanup_local=False, console=console
        )
        
        assert remote_cleaned == 0
        assert remote_failed == 1


class TestPrintWorkdirSummary:
    """Test cases for print_workdir_summary function."""
    
    def test_print_summary_remote_only(self, capsys):
        """Test printing summary with remote cleanup only."""
        console = Console()
        
        print_workdir_summary(
            cleanup_remote=True, cleanup_local=False,
            remote_cleaned=5, remote_failed=1,
            local_cleaned=0, local_failed=0,
            console=console
        )
        
        captured = capsys.readouterr()
        assert "Remote dirs deleted: 5" in captured.out
        assert "Local" not in captured.out
    
    def test_print_summary_local_only(self, capsys):
        """Test printing summary with local cleanup only."""
        console = Console()
        
        print_workdir_summary(
            cleanup_remote=False, cleanup_local=True,
            remote_cleaned=0, remote_failed=0,
            local_cleaned=3, local_failed=2,
            console=console
        )
        
        captured = capsys.readouterr()
        assert "Local dirs deleted: 3" in captured.out
        assert "Remote" not in captured.out
    
    def test_print_summary_both(self, capsys):
        """Test printing summary with both remote and local cleanup."""
        console = Console()
        
        print_workdir_summary(
            cleanup_remote=True, cleanup_local=True,
            remote_cleaned=2, remote_failed=0,
            local_cleaned=3, local_failed=1,
            console=console
        )
        
        captured = capsys.readouterr()
        assert "Remote dirs deleted: 2" in captured.out
        assert "Local dirs deleted: 3" in captured.out
