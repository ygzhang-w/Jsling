"""Tests for jqueue command."""

from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from jsling.commands.jqueue import main, parse_filter
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
def pending_job(temp_db, sample_worker):
    """Create a sample pending job for testing."""
    job = Job(
        job_id="job_pending_001",
        slurm_job_id="12345",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/job_pending_001",
        remote_workdir="/scratch/jobs/job_pending_001",
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
def cancelled_job(temp_db, sample_worker):
    """Create a sample cancelled job for testing."""
    job = Job(
        job_id="cancelled_001",
        slurm_job_id="12346",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/cancelled_001",
        remote_workdir="/scratch/jobs/cancelled_001",
        script_path="",
        command="echo cancelled",
        job_status="cancelled",
        sync_mode="both",
        submit_time=datetime.now(),
        marked_for_deletion=False
    )
    temp_db.add(job)
    temp_db.commit()
    return job


@pytest.fixture
def displayed_cancelled_job(temp_db, sample_worker):
    """Create a cancelled job that has been displayed (marked_for_deletion=True)."""
    job = Job(
        job_id="displayed_cancelled_001",
        slurm_job_id="12347",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/displayed_cancelled_001",
        remote_workdir="/scratch/jobs/displayed_cancelled_001",
        script_path="",
        command="echo displayed",
        job_status="cancelled",
        sync_mode="both",
        submit_time=datetime.now(),
        marked_for_deletion=True
    )
    temp_db.add(job)
    temp_db.commit()
    return job


@pytest.fixture
def failed_job(temp_db, sample_worker):
    """Create a sample failed job for testing."""
    job = Job(
        job_id="failed_001",
        slurm_job_id="12348",
        worker_id=sample_worker.worker_id,
        local_workdir="/tmp/jobs/failed_001",
        remote_workdir="/scratch/jobs/failed_001",
        script_path="",
        command="echo failed",
        job_status="failed",
        sync_mode="both",
        submit_time=datetime.now(),
        marked_for_deletion=False
    )
    temp_db.add(job)
    temp_db.commit()
    return job


class TestParseFilter:
    """Test cases for parse_filter function."""
    
    def test_parse_filter_worker(self):
        """Test parsing worker filter."""
        result = parse_filter(('worker=node1',))
        assert result['worker_id'] == 'node1'
        assert result['status'] is None
    
    def test_parse_filter_worker_short_key(self):
        """Test parsing worker filter with short key 'w'."""
        result = parse_filter(('w=node1',))
        assert result['worker_id'] == 'node1'
    
    def test_parse_filter_status(self):
        """Test parsing status filter."""
        result = parse_filter(('status=running',))
        assert result['status'] == 'running'
        assert result['worker_id'] is None
    
    def test_parse_filter_status_short_key(self):
        """Test parsing status filter with short key 's'."""
        result = parse_filter(('s=pending',))
        assert result['status'] == 'pending'
    
    def test_parse_filter_combined(self):
        """Test parsing multiple filters."""
        result = parse_filter(('worker=node1', 'status=running'))
        assert result['worker_id'] == 'node1'
        assert result['status'] == 'running'
    
    def test_parse_filter_empty(self):
        """Test parsing empty filter."""
        result = parse_filter(())
        assert result['worker_id'] is None
        assert result['status'] is None


class TestJQueueCommand:
    """Test cases for jqueue command."""
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_no_jobs(self, mock_get_db, temp_db, sample_worker):
        """Test jqueue with no jobs."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [])
        
        assert "No jobs found" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_list_jobs(self, mock_get_db, temp_db, pending_job):
        """Test jqueue lists jobs correctly."""
        job_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [])
        
        # Check for partial job_id match (Rich table may truncate)
        assert "job_pending" in result.output
        assert "Job Queue" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_marks_cancelled_as_displayed(self, mock_get_db, temp_db, cancelled_job):
        """Test jqueue marks cancelled jobs as displayed after first display."""
        job_id = cancelled_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [])
        
        # Check job is displayed
        assert "cancelled_0" in result.output
        
        # Verify job is now marked as displayed
        updated_job = temp_db.query(Job).filter(Job.job_id == job_id).first()
        assert updated_job.marked_for_deletion is True
        # Job should still exist (not deleted)
        assert updated_job is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_hides_displayed_cancelled_jobs(self, mock_get_db, temp_db, displayed_cancelled_job, pending_job):
        """Test jqueue hides already-displayed cancelled jobs by default."""
        displayed_job_id = displayed_cancelled_job.job_id
        pending_job_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [])
        
        # Displayed cancelled job should NOT be shown
        assert "displayed_cancelled" not in result.output
        # Pending job should still be shown
        assert "job_pending" in result.output
        # Job should still exist in database (not deleted)
        assert temp_db.query(Job).filter(Job.job_id == displayed_job_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_cancelled_job_hidden_on_second_call(self, mock_get_db, temp_db, cancelled_job, pending_job):
        """Test cancelled job is shown first, then hidden on second jqueue call."""
        cancelled_job_id = cancelled_job.job_id
        pending_job_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        
        # First call - should show the cancelled job and mark it as displayed
        result1 = runner.invoke(main, [])
        assert "cancelled_0" in result1.output
        
        # Verify job is marked as displayed but still exists
        updated_job = temp_db.query(Job).filter(Job.job_id == cancelled_job_id).first()
        assert updated_job.marked_for_deletion is True
        assert updated_job is not None
        
        # Second call - should NOT show the cancelled job
        result2 = runner.invoke(main, [])
        assert "cancelled_0" not in result2.output
        # But job still exists in database
        assert temp_db.query(Job).filter(Job.job_id == cancelled_job_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_all_flag_shows_hidden_jobs(self, mock_get_db, temp_db, displayed_cancelled_job, pending_job):
        """Test --all flag shows all jobs including hidden ones."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['--all'])
        
        # Both jobs should be shown with --all
        assert "displayed_cancelled" in result.output
        assert "job_pending" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_filter_by_worker(self, mock_get_db, temp_db, pending_job, sample_worker):
        """Test filtering jobs by worker ID."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['-f', f'worker={sample_worker.worker_id}'])
        
        assert "job_pending" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_filter_by_status(self, mock_get_db, temp_db, pending_job, cancelled_job):
        """Test filtering jobs by status."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['-f', 'status=pending'])
        
        assert "job_pending" in result.output
        assert "cancelled" not in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_jqueue_filter_shows_all_matching_status(self, mock_get_db, temp_db, cancelled_job, displayed_cancelled_job):
        """Test filtering by status shows all matching jobs including hidden ones."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['-f', 'status=cancelled'])
        
        # Both cancelled jobs should be shown when filtering by status
        assert "cancelled_0" in result.output
        assert "displayed_cancelled" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    @patch('jsling.core.job_manager.JobManager.sync_all_jobs')
    def test_jqueue_manual_sync_option(self, mock_sync_all, mock_get_db, temp_db, pending_job):
        """Test that --sync flag triggers manual synchronization."""
        mock_get_db.return_value = temp_db
        mock_sync_all.return_value = 1
        
        runner = CliRunner()
        result = runner.invoke(main, ['--sync'])
        
        assert "Manually syncing" in result.output
        mock_sync_all.assert_called_once()
    
    @patch('jsling.commands.jqueue.get_db')
    @patch('jsling.core.job_manager.JobManager.sync_all_jobs')
    def test_jqueue_default_no_sync(self, mock_sync_all, mock_get_db, temp_db, pending_job):
        """Test that jqueue by default does not sync (relies on runner daemon)."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, [])
        
        assert "syncing" not in result.output.lower() or "Manually" not in result.output
        mock_sync_all.assert_not_called()


class TestJQueueHelp:
    """Test help output for jqueue command."""
    
    def test_help_output(self):
        """Test that help output is displayed correctly."""
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        
        assert result.exit_code == 0
        assert "Query job queue status" in result.output
        assert "--all" in result.output or "-a" in result.output
        assert "--filter" in result.output or "-f" in result.output


class TestJQueueCleanup:
    """Test cases for jqueue cleanup command."""
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_no_jobs_to_clean(self, mock_get_db, temp_db, sample_worker):
        """Test cleanup with no jobs to clean."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-f'])
        
        assert "No jobs to clean up" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_by_status_cancelled(self, mock_get_db, temp_db, cancelled_job, pending_job):
        """Test cleanup only cancelled jobs."""
        cancelled_id = cancelled_job.job_id
        pending_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'cancelled', '-f'])
        
        assert "Cleaned up 1 job(s)" in result.output
        # Cancelled job should be deleted
        assert temp_db.query(Job).filter(Job.job_id == cancelled_id).first() is None
        # Pending job should still exist
        assert temp_db.query(Job).filter(Job.job_id == pending_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_by_status_failed(self, mock_get_db, temp_db, failed_job, pending_job):
        """Test cleanup only failed jobs."""
        failed_id = failed_job.job_id
        pending_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'failed', '-f'])
        
        assert "Cleaned up 1 job(s)" in result.output
        # Failed job should be deleted
        assert temp_db.query(Job).filter(Job.job_id == failed_id).first() is None
        # Pending job should still exist
        assert temp_db.query(Job).filter(Job.job_id == pending_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_by_multiple_statuses(self, mock_get_db, temp_db, cancelled_job, failed_job, pending_job):
        """Test cleanup multiple statuses using comma-separated format."""
        cancelled_id = cancelled_job.job_id
        failed_id = failed_job.job_id
        pending_id = pending_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'failed,cancelled', '-f'])
        
        assert "Cleaned up 2 job(s)" in result.output
        # Both jobs should be deleted
        assert temp_db.query(Job).filter(Job.job_id == cancelled_id).first() is None
        assert temp_db.query(Job).filter(Job.job_id == failed_id).first() is None
        # Pending job should still exist
        assert temp_db.query(Job).filter(Job.job_id == pending_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_dry_run(self, mock_get_db, temp_db, cancelled_job):
        """Test dry-run shows jobs but doesn't delete them."""
        cancelled_id = cancelled_job.job_id
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'cancelled', '-n'])
        
        assert "Dry-run mode" in result.output
        assert "Would delete" in result.output
        assert cancelled_id in result.output
        # Job should NOT be deleted
        assert temp_db.query(Job).filter(Job.job_id == cancelled_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    @patch('jsling.connections.worker.Worker')
    def test_cleanup_with_remote_flag(self, mock_worker_conn, mock_get_db, temp_db, cancelled_job):
        """Test cleanup with --remote flag calls SSH to delete remote dir."""
        cancelled_id = cancelled_job.job_id
        mock_get_db.return_value = temp_db
        
        # Mock the SSH client
        mock_ssh = MagicMock()
        mock_ssh.run.return_value = MagicMock(ok=True)
        mock_worker_instance = MagicMock()
        mock_worker_instance.test_connection.return_value = True
        mock_worker_instance.ssh_client = mock_ssh
        mock_worker_conn.return_value = mock_worker_instance
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'cancelled', '-r', '-f'])
        
        assert "Cleaned up 1 job(s)" in result.output
        assert "Remote dirs deleted: 1" in result.output
        # Verify SSH rm -rf was called
        mock_ssh.run.assert_called()
        call_args = mock_ssh.run.call_args[0][0]
        assert "rm -rf" in call_args
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_no_force_requires_confirmation(self, mock_get_db, temp_db, cancelled_job):
        """Test cleanup without --force requires confirmation."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        # Simulate user typing 'n' (no) at the prompt
        result = runner.invoke(main, ['cleanup', '-s', 'cancelled'], input='n\n')
        
        assert "Cleanup cancelled" in result.output
        # Job should still exist since we cancelled
        assert temp_db.query(Job).filter(Job.job_id == cancelled_job.job_id).first() is not None
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_invalid_status_warning(self, mock_get_db, temp_db, sample_worker):
        """Test cleanup with invalid status shows warning."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'invalid_status', '-f'])
        
        assert "Invalid status 'invalid_status'" in result.output
    
    @patch('jsling.commands.jqueue.get_db')
    def test_cleanup_no_jobs_with_status(self, mock_get_db, temp_db, pending_job):
        """Test cleanup when no jobs match the specified status."""
        mock_get_db.return_value = temp_db
        
        runner = CliRunner()
        result = runner.invoke(main, ['cleanup', '-s', 'cancelled', '-f'])
        
        assert "No jobs found with status: cancelled" in result.output

