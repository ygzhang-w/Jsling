"""Test StatusPoller - sentinel file based status synchronization."""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock

from jsling.core.status_poller import StatusPoller, get_status_poller, JobStatusInfo
from jsling.database.models import Worker
from jsling.utils.encryption import encrypt_credential


@pytest.fixture
def mock_worker(temp_db):
    """Create a mock worker in database."""
    encrypted_key = encrypt_credential("test_key")
    
    worker = Worker(
        worker_id="test_worker_001",
        worker_name="test_worker",
        host="localhost",
        port=22,
        username="testuser",
        auth_method="key",
        auth_credential=encrypted_key,
        remote_workdir="/tmp/test",
        queue_name="test_queue",
        worker_type="cpu",
        ntasks_per_node=4,
        is_active=True
    )
    temp_db.add(worker)
    temp_db.commit()
    return worker


class TestStatusPoller:
    """Test StatusPoller class."""
    
    def test_init(self, temp_db):
        """Test StatusPoller initialization."""
        poller = StatusPoller(session=temp_db, poll_interval=10)
        
        assert poller.poll_interval == 10
        assert poller.is_running() is False
        assert len(poller._worker_connections) == 0
        assert len(poller._file_sync_managers) == 0
    
    def test_start_stop(self, temp_db):
        """Test starting and stopping the poller."""
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Start poller
        result = poller.start()
        assert result is True
        assert poller.is_running() is True
        
        # Wait a moment for thread to start
        time.sleep(0.2)
        
        # Stop poller
        poller.stop()
        assert poller.is_running() is False
    
    def test_start_twice_does_not_create_duplicate(self, temp_db):
        """Test that starting twice doesn't create duplicate threads."""
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Start twice
        result1 = poller.start()
        result2 = poller.start()
        
        assert result1 is True
        assert result2 is True  # Returns True but doesn't create new thread
        assert poller.is_running() is True
        
        # Cleanup
        poller.stop()
    
    def test_context_manager(self, temp_db):
        """Test context manager usage."""
        with StatusPoller(session=temp_db, poll_interval=5) as poller:
            assert poller.is_running() is True
        
        # After exiting context, should be stopped
        assert poller.is_running() is False
    
    def test_add_status_callback(self, temp_db):
        """Test adding status callbacks."""
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        callback_called = []
        
        def test_callback(job_id, status, info):
            callback_called.append((job_id, status))
        
        poller.add_status_callback(test_callback)
        assert len(poller._status_callbacks) == 1


class TestGetStatusPoller:
    """Test get_status_poller global singleton function."""
    
    def test_returns_same_instance(self):
        """Test that get_status_poller returns singleton instance."""
        # Reset global poller
        import jsling.core.status_poller as poller_module
        poller_module._global_poller = None
        
        # Get poller instances
        poller1 = get_status_poller(poll_interval=10)
        poller2 = get_status_poller(poll_interval=15)  # Different interval should be ignored
        
        # Should be the same instance
        assert poller1 is poller2
        assert poller1.poll_interval == 10  # First interval is used
        
        # Cleanup
        if poller1.is_running():
            poller1.stop()
        poller_module._global_poller = None


class TestJobStatusInfo:
    """Test JobStatusInfo dataclass."""
    
    def test_create_basic_info(self):
        """Test creating a basic status info."""
        info = JobStatusInfo(status="running")
        
        assert info.status == "running"
        assert info.start_time is None
        assert info.end_time is None
        assert info.exit_code is None
        assert info.error_message is None
    
    def test_create_full_info(self):
        """Test creating a full status info with all fields."""
        from datetime import datetime
        
        now = datetime.now()
        info = JobStatusInfo(
            status="failed",
            start_time=now,
            end_time=now,
            exit_code=1,
            error_message="Out of memory"
        )
        
        assert info.status == "failed"
        assert info.start_time == now
        assert info.exit_code == 1
        assert info.error_message == "Out of memory"


class TestJobDisappearedFromQueue:
    """Test behavior when job disappears from queue without sentinel file."""
    
    def test_running_job_marked_failed_when_disappeared(self, temp_db, mock_worker):
        """Test that a running job is marked as failed when it disappears from queue."""
        from jsling.database.models import Job
        
        # Create a running job
        job = Job(
            job_id="test_job_001",
            worker_id=mock_worker.worker_id,
            job_status="running",
            local_workdir="/tmp/local",
            remote_workdir="/tmp/remote",
            script_path="/tmp/script.sh",
            sync_mode="sentinel",
            slurm_job_id="12345"
        )
        temp_db.add(job)
        temp_db.commit()
        
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Mock SSH client that returns no sentinel files and empty squeue
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = False
        mock_result.stdout = ""
        mock_ssh.run.return_value = mock_result
        
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        
        with patch.object(poller, '_get_worker_connection', return_value=mock_worker_conn):
            poller._poll_job_status(temp_db, job)
        
        # Job should be marked as failed
        temp_db.refresh(job)
        assert job.job_status == "failed"
        assert job.error_message is not None
        assert "terminated unexpectedly" in job.error_message
    
    def test_running_job_with_running_sentinel_marked_failed_when_not_in_squeue(self, temp_db, mock_worker):
        """Test that a job with running sentinel is marked failed if not in squeue (scancel case)."""
        from jsling.database.models import Job
        
        # Create a running job
        job = Job(
            job_id="test_job_scancel",
            worker_id=mock_worker.worker_id,
            job_status="running",
            local_workdir="/tmp/local",
            remote_workdir="/tmp/remote",
            script_path="/tmp/script.sh",
            sync_mode="sentinel",
            slurm_job_id="99999"
        )
        temp_db.add(job)
        temp_db.commit()
        
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Mock SSH client: running sentinel exists but squeue returns empty (job was scancel'd)
        mock_ssh = MagicMock()
        
        def mock_run(cmd, **kwargs):
            result = MagicMock()
            if ".jsling_status_done" in cmd or ".jsling_status_failed" in cmd:
                result.ok = False
                result.stdout = ""
            elif ".jsling_status_running" in cmd and "test -f" in cmd:
                result.ok = True
                result.stdout = "exists"
            elif ".jsling_status_running" in cmd and "cat" in cmd:
                result.ok = True
                result.stdout = "START_TIME=2024-01-15T10:30:45"
            elif "squeue" in cmd:
                # Job not in squeue - was cancelled
                result.ok = True
                result.stdout = ""
            else:
                result.ok = False
                result.stdout = ""
            return result
        
        mock_ssh.run.side_effect = mock_run
        
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        
        with patch.object(poller, '_get_worker_connection', return_value=mock_worker_conn):
            poller._poll_job_status(temp_db, job)
        
        # Job should be marked as failed because it's not in squeue
        temp_db.refresh(job)
        assert job.job_status == "failed"
        assert job.error_message is not None
        assert "terminated unexpectedly" in job.error_message
    
    def test_pending_job_marked_failed_when_disappeared(self, temp_db, mock_worker):
        """Test that a pending job is marked as failed when it disappears from queue."""
        from jsling.database.models import Job
        
        # Create a pending job
        job = Job(
            job_id="test_job_002",
            worker_id=mock_worker.worker_id,
            job_status="pending",
            local_workdir="/tmp/local",
            remote_workdir="/tmp/remote",
            script_path="/tmp/script.sh",
            sync_mode="sentinel",
            slurm_job_id="12346"
        )
        temp_db.add(job)
        temp_db.commit()
        
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Mock SSH client that returns no sentinel files and empty squeue
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = False
        mock_result.stdout = ""
        mock_ssh.run.return_value = mock_result
        
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        
        with patch.object(poller, '_get_worker_connection', return_value=mock_worker_conn):
            poller._poll_job_status(temp_db, job)
        
        # Job should be marked as failed
        temp_db.refresh(job)
        assert job.job_status == "failed"
        assert job.error_message is not None
    
    def test_completed_job_not_changed_when_no_squeue(self, temp_db, mock_worker):
        """Test that a completed job is not changed when squeue is empty."""
        from jsling.database.models import Job
        
        # Create a completed job
        job = Job(
            job_id="test_job_003",
            worker_id=mock_worker.worker_id,
            job_status="completed",
            local_workdir="/tmp/local",
            remote_workdir="/tmp/remote",
            script_path="/tmp/script.sh",
            sync_mode="sentinel",
            slurm_job_id="12347"
        )
        temp_db.add(job)
        temp_db.commit()
        
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Mock SSH client that returns no sentinel files and empty squeue
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = False
        mock_result.stdout = ""
        mock_ssh.run.return_value = mock_result
        
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        
        with patch.object(poller, '_get_worker_connection', return_value=mock_worker_conn):
            poller._poll_job_status(temp_db, job)
        
        # Job status should remain completed
        temp_db.refresh(job)
        assert job.job_status == "completed"
    
    def test_job_with_done_sentinel_file_uses_sentinel_status(self, temp_db, mock_worker):
        """Test that done sentinel file status is trusted (terminal state)."""
        from jsling.database.models import Job
        
        # Create a running job
        job = Job(
            job_id="test_job_004",
            worker_id=mock_worker.worker_id,
            job_status="running",
            local_workdir="/tmp/local",
            remote_workdir="/tmp/remote",
            script_path="/tmp/script.sh",
            sync_mode="sentinel",
            slurm_job_id="12348"
        )
        temp_db.add(job)
        temp_db.commit()
        
        poller = StatusPoller(session=temp_db, poll_interval=5)
        
        # Mock SSH client that returns sentinel file with completed status
        mock_ssh = MagicMock()
        
        def mock_run(cmd, **kwargs):
            result = MagicMock()
            if ".jsling_status_done" in cmd:
                result.ok = True
                result.stdout = "EXIT_CODE=0\nEND_TIME=2024-01-15T10:35:22"
            else:
                result.ok = False
                result.stdout = ""
            return result
        
        mock_ssh.run.side_effect = mock_run
        
        mock_worker_conn = MagicMock()
        mock_worker_conn.ssh_client = mock_ssh
        
        with patch.object(poller, '_get_worker_connection', return_value=mock_worker_conn):
            poller._poll_job_status(temp_db, job)
        
        # Job should be marked as completed from sentinel file
        temp_db.refresh(job)
        assert job.job_status == "completed"
        assert job.exit_code == 0
