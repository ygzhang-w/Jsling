"""Unit tests for submission_worker module."""

import json
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base, Worker, Job, Config
from jsling.core.submission_worker import SubmissionWorker


@pytest.fixture
def temp_db_session():
    """Create a temporary database session with required configs."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    
    # Add required configurations
    configs = [
        Config(config_key="submission_poll_interval", config_value="2", config_type="int", description=""),
        Config(config_key="submission_max_concurrent", config_value="5", config_type="int", description=""),
        Config(config_key="submission_retry_count", config_value="3", config_type="int", description=""),
        Config(config_key="submission_retry_delay", config_value="1", config_type="int", description=""),
        Config(config_key="default_rsync_mode", config_value="periodic", config_type="str", description=""),
    ]
    for config in configs:
        session.add(config)
    session.commit()
    
    yield session
    
    session.close()
    engine.dispose()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_worker(temp_db_session):
    """Create a test worker in database."""
    worker = Worker(
        worker_id="test_worker_001",
        worker_name="test_cluster",
        worker_type="gpu",
        host="cluster.example.com",
        port=22,
        username="testuser",
        auth_method="key",
        auth_credential="/home/user/.ssh/id_rsa",
        remote_workdir="/scratch/user/jobs",
        queue_name="gpu_queue",
        ntasks_per_node=16,
        gres="gpu:v100:2",
        gpus_per_task=1
    )
    temp_db_session.add(worker)
    temp_db_session.commit()
    return worker


@pytest.fixture
def queued_job(temp_db_session, test_worker):
    """Create a queued job for testing."""
    job_id = "20250116_120000_abc12345"
    submission_params = {
        "command": "echo 'Hello World'",
        "ntasks_per_node": 8,
        "gres": "gpu:1",
        "gpus_per_task": 1,
        "upload_files": [],
    }
    
    job = Job(
        job_id=job_id,
        slurm_job_id=None,
        worker_id=test_worker.worker_id,
        local_workdir="/tmp/test_job",
        remote_workdir=f"{test_worker.remote_workdir}/{job_id}",
        script_path="",
        command="echo 'Hello World'",
        job_status="queued",
        sync_mode="sentinel",
        rsync_mode="periodic",
        submission_params=json.dumps(submission_params),
        submit_time=datetime.now()
    )
    temp_db_session.add(job)
    temp_db_session.commit()
    return job


class TestSubmissionWorkerInit:
    """Test SubmissionWorker initialization."""
    
    def test_init_loads_config_from_database(self, temp_db_session):
        """Test that initialization loads config from database."""
        worker = SubmissionWorker(session=temp_db_session)
        
        assert worker.poll_interval == 2
        assert worker.max_concurrent == 5
        assert worker.retry_count == 3
        assert worker.retry_delay == 1
    
    def test_init_with_overrides(self, temp_db_session):
        """Test that initialization accepts config overrides."""
        worker = SubmissionWorker(
            session=temp_db_session,
            poll_interval=10,
            max_concurrent=3
        )
        
        assert worker.poll_interval == 10
        assert worker.max_concurrent == 3
        # Non-overridden values from DB
        assert worker.retry_count == 3
    
    def test_init_raises_on_missing_config(self):
        """Test that missing config raises KeyError."""
        # Create empty database without configs
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        engine = create_engine(f"sqlite:///{db_path}", echo=False)
        Base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        
        try:
            with pytest.raises(KeyError):
                SubmissionWorker(session=session)
        finally:
            session.close()
            engine.dispose()
            Path(db_path).unlink(missing_ok=True)


class TestSubmissionWorkerStartStop:
    """Test SubmissionWorker start/stop functionality."""
    
    def test_start_creates_thread(self, temp_db_session):
        """Test that start() creates a background thread."""
        worker = SubmissionWorker(session=temp_db_session)
        
        assert not worker.is_running()
        
        worker.start()
        assert worker.is_running()
        assert worker._thread is not None
        assert worker._thread.is_alive()
        
        worker.stop()
        assert not worker.is_running()
    
    def test_stop_waits_for_thread(self, temp_db_session):
        """Test that stop() waits for thread to terminate."""
        worker = SubmissionWorker(session=temp_db_session)
        
        worker.start()
        time.sleep(0.5)  # Let thread start
        
        worker.stop()
        
        assert worker._thread is None or not worker._thread.is_alive()
    
    def test_double_start_is_safe(self, temp_db_session):
        """Test that calling start() twice is safe."""
        worker = SubmissionWorker(session=temp_db_session)
        
        assert worker.start() is True
        assert worker.start() is True  # Should return True without error
        
        worker.stop()


class TestSubmissionWorkerQueue:
    """Test job queue processing."""
    
    def test_get_queue_status(self, temp_db_session, queued_job):
        """Test getting queue status."""
        worker = SubmissionWorker(session=temp_db_session)
        
        status = worker.get_queue_status()
        
        assert status["queued"] == 1
        assert status["active_submissions"] == 0
        assert status["max_concurrent"] == 5
        assert status["running"] is False
    
    def test_process_queued_jobs_respects_max_concurrent(self, temp_db_session, test_worker):
        """Test that max concurrent limit is respected."""
        # Create multiple queued jobs
        for i in range(10):
            job_id = f"20250116_12000{i}_abc1234{i}"
            submission_params = {
                "command": f"echo 'Job {i}'",
                "ntasks_per_node": None,
                "gres": None,
                "gpus_per_task": None,
                "upload_files": [],
            }
            job = Job(
                job_id=job_id,
                worker_id=test_worker.worker_id,
                local_workdir=f"/tmp/test_job_{i}",
                remote_workdir=f"{test_worker.remote_workdir}/{job_id}",
                script_path="",
                command=f"echo 'Job {i}'",
                job_status="queued",
                sync_mode="sentinel",
                rsync_mode="periodic",
                submission_params=json.dumps(submission_params),
                submit_time=datetime.now()
            )
            temp_db_session.add(job)
        temp_db_session.commit()
        
        worker = SubmissionWorker(session=temp_db_session, max_concurrent=3)
        
        # Simulate active submissions at capacity
        worker._active_submissions = {
            "job1": MagicMock(is_alive=MagicMock(return_value=True)),
            "job2": MagicMock(is_alive=MagicMock(return_value=True)),
            "job3": MagicMock(is_alive=MagicMock(return_value=True)),
        }
        
        # Should not process more jobs when at capacity
        initial_count = len(worker._active_submissions)
        worker._process_queued_jobs()
        assert len(worker._active_submissions) == initial_count


class TestSubmissionExecution:
    """Test actual submission execution."""
    
    @patch("jsling.core.submission_worker.SubmissionWorker._rsync_upload")
    @patch("jsling.core.submission_worker.WorkerConnection")
    def test_execute_submission_success(
        self,
        mock_worker_conn,
        mock_rsync_upload,
        temp_db_session,
        queued_job,
        test_worker
    ):
        """Test successful job submission."""
        worker = SubmissionWorker(session=temp_db_session)
        
        # Mock SSH client
        mock_ssh_client = MagicMock()
        mock_ssh_client.run.return_value = MagicMock(
            ok=True, 
            stdout="Submitted batch job 123456",
            stderr=""
        )
        mock_ssh_client.connect.return_value = None
        mock_worker_conn.return_value.ssh_client = mock_ssh_client
        
        # Mock rsync
        mock_rsync_upload.return_value = True
        
        # Execute submission
        params = json.loads(queued_job.submission_params)
        result = worker._execute_submission(
            temp_db_session, queued_job, test_worker, params
        )
        
        assert result is True
        assert queued_job.slurm_job_id == "123456"
        assert queued_job.job_status == "pending"
        assert queued_job.submission_params is None
    
    @patch("jsling.core.submission_worker.SubmissionWorker._rsync_upload")
    @patch("jsling.core.submission_worker.WorkerConnection")
    def test_execute_submission_sbatch_failure(
        self,
        mock_worker_conn,
        mock_rsync_upload,
        temp_db_session,
        queued_job,
        test_worker
    ):
        """Test submission failure when sbatch fails."""
        worker = SubmissionWorker(session=temp_db_session)
        
        # Mock SSH client with sbatch failure
        mock_ssh_client = MagicMock()
        mock_ssh_client.run.side_effect = [
            MagicMock(ok=True, stdout=""),  # mkdir
            MagicMock(ok=False, stdout="", stderr="sbatch: error: Batch job submission failed")
        ]
        mock_ssh_client.connect.return_value = None
        mock_worker_conn.return_value.ssh_client = mock_ssh_client
        
        # Mock rsync
        mock_rsync_upload.return_value = True
        
        # Execute submission - should raise exception
        params = json.loads(queued_job.submission_params)
        with pytest.raises(RuntimeError, match="sbatch failed"):
            worker._execute_submission(
                temp_db_session, queued_job, test_worker, params
            )
    
    def test_submit_single_job_missing_params(self, temp_db_session, test_worker):
        """Test submission fails when submission_params is missing."""
        # Create job without submission_params
        job = Job(
            job_id="test_no_params",
            worker_id=test_worker.worker_id,
            local_workdir="/tmp/test",
            remote_workdir="/scratch/test",
            script_path="",
            command="echo test",
            job_status="queued",
            sync_mode="sentinel",
            rsync_mode="periodic",
            submission_params=None,  # Missing params
            submit_time=datetime.now()
        )
        temp_db_session.add(job)
        temp_db_session.commit()
        
        worker = SubmissionWorker(session=temp_db_session)
        result = worker._submit_single_job(temp_db_session, job)
        
        assert result is False
        assert "Missing submission parameters" in job.error_message


class TestParseSlurmJobId:
    """Test Slurm job ID parsing."""
    
    def test_parse_standard_format(self, temp_db_session):
        """Test parsing standard sbatch output."""
        worker = SubmissionWorker(session=temp_db_session)
        
        result = worker._parse_slurm_job_id("Submitted batch job 123456")
        assert result == "123456"
    
    def test_parse_with_extra_output(self, temp_db_session):
        """Test parsing with extra output text."""
        worker = SubmissionWorker(session=temp_db_session)
        
        result = worker._parse_slurm_job_id("Submitted batch job 789012\nSome extra info")
        assert result == "789012"
    
    def test_parse_fallback_format(self, temp_db_session):
        """Test fallback parsing when standard format not found."""
        worker = SubmissionWorker(session=temp_db_session)
        
        result = worker._parse_slurm_job_id("Job ID: 456789")
        assert result == "456789"
    
    def test_parse_no_id_found(self, temp_db_session):
        """Test when no job ID can be found."""
        worker = SubmissionWorker(session=temp_db_session)
        
        result = worker._parse_slurm_job_id("Error occurred")
        assert result is None


class TestWorkerConnection:
    """Test worker connection management."""
    
    def test_set_worker_connections(self, temp_db_session):
        """Test setting shared worker connections."""
        worker = SubmissionWorker(session=temp_db_session)
        
        mock_connections = {"worker1": MagicMock(), "worker2": MagicMock()}
        worker.set_worker_connections(mock_connections)
        
        assert worker._worker_connections == mock_connections
    
    @patch("jsling.core.submission_worker.WorkerConnection")
    def test_get_worker_connection_creates_new(
        self, mock_worker_conn, temp_db_session, test_worker
    ):
        """Test creating new connection when not cached."""
        worker = SubmissionWorker(session=temp_db_session)
        
        mock_conn = MagicMock()
        mock_conn.ssh_client.connect.return_value = None
        mock_worker_conn.return_value = mock_conn
        
        result = worker._get_worker_connection(test_worker)
        
        assert result is not None
        assert test_worker.worker_id in worker._worker_connections
    
    def test_get_worker_connection_uses_cache(self, temp_db_session, test_worker):
        """Test that cached connection is reused."""
        worker = SubmissionWorker(session=temp_db_session)
        
        # Pre-populate cache
        cached_conn = MagicMock()
        worker._worker_connections[test_worker.worker_id] = cached_conn
        
        result = worker._get_worker_connection(test_worker)
        
        assert result is cached_conn
