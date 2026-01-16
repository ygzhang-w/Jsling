"""Tests for jcancel command."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from jsling.commands.jcancel import main
from jsling.database.models import Job, Worker


@pytest.fixture
def sample_worker(temp_db):
    """Create a sample worker for testing."""
    worker = Worker(
        worker_id="test_worker_001",
        worker_name="test_worker",
        host="test.example.com",
        port=22,
        username="testuser",
        auth_method="key",
        auth_credential="/path/to/key",
        worker_type="cpu",
        queue_name="normal",
        remote_workdir="/scratch/jobs",
        is_active=True,
        ntasks_per_node=4
    )
    temp_db.add(worker)
    temp_db.commit()
    return worker


@pytest.fixture
def sample_job(temp_db, sample_worker):
    """Create a sample pending job for testing."""
    job = Job(
        job_id="job_test_001",
        slurm_job_id="12345",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/job_test_001",
        remote_workdir="/scratch/jobs/job_test_001",
        script_path="",
        command="echo hello",
        job_status="pending",
        sync_mode="both",
        submit_time=datetime.now()
    )
    temp_db.add(job)
    temp_db.commit()
    return job


@pytest.fixture
def running_job(temp_db, sample_worker):
    """Create a sample running job for testing."""
    job = Job(
        job_id="test_002",
        slurm_job_id="12346",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/test_002",
        remote_workdir="/scratch/jobs/test_002",
        script_path="",
        command="sleep 100",
        job_status="running",
        sync_mode="both",
        submit_time=datetime.now(),
        start_time=datetime.now()
    )
    temp_db.add(job)
    temp_db.commit()
    return job


@pytest.fixture
def completed_job(temp_db, sample_worker):
    """Create a sample completed job for testing."""
    job = Job(
        job_id="test_003",
        slurm_job_id="12347",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/test_003",
        remote_workdir="/scratch/jobs/test_003",
        script_path="",
        command="echo done",
        job_status="completed",
        sync_mode="both",
        submit_time=datetime.now(),
        end_time=datetime.now()
    )
    temp_db.add(job)
    temp_db.commit()
    return job


class TestJCancelCommand:
    """Test cases for jcancel command."""
    
    def test_cancel_no_job_ids(self):
        """Test jcancel with no job IDs provided."""
        runner = CliRunner()
        result = runner.invoke(main, [])
        assert result.exit_code != 0  # Should fail due to missing required argument
    
    @patch('jsling.commands.jcancel.get_db')
    def test_cancel_job_not_found(self, mock_get_db, temp_db):
        """Test jcancel with non-existent job ID."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ["nonexistent_job"])
        
        assert "Job not found" in result.output
        assert "No jobs to cancel" in result.output
    
    @patch('jsling.commands.jcancel.get_db')
    def test_cancel_already_completed_job(self, mock_get_db, temp_db, completed_job):
        """Test jcancel on already completed job."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [completed_job.job_id])
        
        assert "already in terminal state" in result.output
        assert "No jobs to cancel" in result.output
    
    @patch('jsling.commands.jcancel.get_db')
    @patch('jsling.core.job_manager.WorkerConnection')
    def test_cancel_pending_job_success(self, mock_worker_conn, mock_get_db, temp_db, sample_job):
        """Test successful cancellation of pending job."""
        mock_get_db.return_value = temp_db
        
        # Mock SSH client for scancel
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True, stdout="")
        mock_worker_conn.return_value.ssh_client = mock_ssh
        
        job_id = sample_job.job_id
        runner = CliRunner()
        result = runner.invoke(main, [job_id])
        
        assert "Cancelled" in result.output
        assert "Successfully cancelled 1 job" in result.output
        
        # Verify job status updated by querying DB
        updated_job = temp_db.query(Job).filter(Job.job_id == job_id).first()
        assert updated_job.job_status == "cancelled"
    
    @patch('jsling.commands.jcancel.get_db')
    @patch('jsling.core.job_manager.WorkerConnection')
    def test_cancel_running_job_success(self, mock_worker_conn, mock_get_db, temp_db, running_job):
        """Test successful cancellation of running job."""
        mock_get_db.return_value = temp_db
        
        # Mock SSH client for scancel
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True, stdout="")
        mock_worker_conn.return_value.ssh_client = mock_ssh
        
        job_id = running_job.job_id
        runner = CliRunner()
        result = runner.invoke(main, [job_id])
        
        assert "Cancelled" in result.output
        assert "Successfully cancelled 1 job" in result.output
        
        # Verify job status updated by querying DB
        updated_job = temp_db.query(Job).filter(Job.job_id == job_id).first()
        assert updated_job.job_status == "cancelled"
    
    @patch('jsling.commands.jcancel.get_db')
    @patch('jsling.core.job_manager.WorkerConnection')
    def test_cancel_by_slurm_job_id(self, mock_worker_conn, mock_get_db, temp_db, sample_job):
        """Test cancellation using slurm job ID."""
        mock_get_db.return_value = temp_db
        
        # Mock SSH client for scancel
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True, stdout="")
        mock_worker_conn.return_value.ssh_client = mock_ssh
        
        job_id = sample_job.job_id
        slurm_id = sample_job.slurm_job_id
        runner = CliRunner()
        # Use slurm_job_id instead of job_id
        result = runner.invoke(main, [slurm_id])
        
        assert "Cancelled" in result.output
        
        # Verify job status updated by querying DB
        updated_job = temp_db.query(Job).filter(Job.job_id == job_id).first()
        assert updated_job.job_status == "cancelled"
    
    @patch('jsling.commands.jcancel.get_db')
    @patch('jsling.core.job_manager.WorkerConnection')
    def test_cancel_multiple_jobs(self, mock_worker_conn, mock_get_db, temp_db, sample_job, running_job):
        """Test cancellation of multiple jobs at once."""
        mock_get_db.return_value = temp_db
        
        # Mock SSH client for scancel
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True, stdout="")
        mock_worker_conn.return_value.ssh_client = mock_ssh
        
        job_id1 = sample_job.job_id
        job_id2 = running_job.job_id
        runner = CliRunner()
        result = runner.invoke(main, [job_id1, job_id2])
        
        assert "Successfully cancelled 2 job" in result.output
        
        # Verify both jobs are cancelled by querying DB
        updated_job1 = temp_db.query(Job).filter(Job.job_id == job_id1).first()
        updated_job2 = temp_db.query(Job).filter(Job.job_id == job_id2).first()
        assert updated_job1.job_status == "cancelled"
        assert updated_job2.job_status == "cancelled"
    
    @patch('jsling.commands.jcancel.get_db')
    @patch('jsling.core.job_manager.WorkerConnection')
    def test_cancel_job_scancel_fails(self, mock_worker_conn, mock_get_db, temp_db, sample_job):
        """Test cancellation when scancel fails."""
        mock_get_db.return_value = temp_db
        
        # Mock SSH client for scancel failure
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=False, stderr="scancel: error")
        mock_worker_conn.return_value.ssh_client = mock_ssh
        
        job_id = sample_job.job_id
        runner = CliRunner()
        result = runner.invoke(main, [job_id])
        
        # Should show failure message (no sync triggered - relies on runner daemon)
        assert "Failed to cancel" in result.output


class TestJCancelHelp:
    """Test help output for jcancel command."""
    
    def test_help_output(self):
        """Test that help output is displayed correctly."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        
        assert result.exit_code == 0
        assert "Cancel one or more jobs" in result.output
        # --yes and --sync are removed, should not appear in help
        assert "--yes" not in result.output
        assert "--sync" not in result.output
