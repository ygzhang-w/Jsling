"""Unit tests for job_manager module."""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base, Worker, Job, Config
from jsling.core.job_manager import JobManager


@pytest.fixture
def temp_db_session():
    """Create a temporary database session"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()
    
    # Add required default_rsync_mode config for queue_job tests
    config = Config(
        config_key="default_rsync_mode",
        config_value="periodic",
        config_type="str",
        description="Default rsync mode"
    )
    session.add(config)
    session.commit()
    
    yield session
    
    session.close()
    engine.dispose()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def test_worker(temp_db_session):
    """Create a test worker in database"""
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
def job_manager(temp_db_session):
    """Create JobManager instance"""
    return JobManager(temp_db_session)


class TestJobManagerHelpers:
    """Test other JobManager helper methods"""
    
    def test_parse_slurm_job_id_standard_format(self, job_manager):
        """Test parsing standard sbatch output"""
        output = "Submitted batch job 123456"
        
        result = job_manager._parse_slurm_job_id(output)
        
        assert result == "123456"
    
    def test_parse_slurm_job_id_with_extra_text(self, job_manager):
        """Test parsing sbatch output with extra text"""
        output = "Submitted batch job 789012\nSome extra output"
        
        result = job_manager._parse_slurm_job_id(output)
        
        assert result == "789012"
    
    def test_parse_slurm_job_id_fallback(self, job_manager):
        """Test parsing when standard format not found"""
        output = "Job ID: 456789"
        
        result = job_manager._parse_slurm_job_id(output)
        
        assert result == "456789"
    
    def test_parse_slurm_job_id_no_match(self, job_manager):
        """Test parsing when no job ID found"""
        output = "Error: submission failed"
        
        result = job_manager._parse_slurm_job_id(output)
        
        assert result is None
    
    def test_generate_job_id_format(self, job_manager):
        """Test job ID generation format"""
        job_id = job_manager._generate_job_id()
        
        # Format: YYYYMMDD_HHMMSS_xxxxxxxx
        parts = job_id.split("_")
        assert len(parts) == 3
        assert len(parts[0]) == 8  # Date part
        assert len(parts[1]) == 6  # Time part
        assert len(parts[2]) == 8  # UUID part
    
    def test_generate_job_id_unique(self, job_manager):
        """Test that generated job IDs are unique"""
        job_ids = [job_manager._generate_job_id() for _ in range(100)]
        
        assert len(set(job_ids)) == 100


class TestJobManagerQueueJob:
    """Tests for JobManager.queue_job async submission."""
    
    def test_queue_job_creates_queued_status(self, temp_db_session, test_worker):
        """Test that queue_job creates a job with 'queued' status."""
        job_mgr = JobManager(temp_db_session)
        
        job_id = job_mgr.queue_job(
            worker_id=test_worker.worker_id,
            command="echo 'Hello World'",
            local_workdir="/tmp/test",
            rsync_mode="periodic"
        )
        
        assert job_id is not None
        job = temp_db_session.query(Job).filter(Job.job_id == job_id).first()
        assert job is not None
        assert job.job_status == "queued"
        assert job.slurm_job_id is None  # Not submitted yet
    
    def test_queue_job_stores_submission_params(self, temp_db_session, test_worker):
        """Test that queue_job stores submission parameters as JSON with absolute paths."""
        job_mgr = JobManager(temp_db_session)
        
        job_id = job_mgr.queue_job(
            worker_id=test_worker.worker_id,
            command="python train.py",
            local_workdir="/tmp/test",
            ntasks_per_node=8,
            gres="gpu:2",
            gpus_per_task=1,
            upload_files=["data.csv", "config.yaml"],
            rsync_mode="final_only"
        )
        
        assert job_id is not None
        job = temp_db_session.query(Job).filter(Job.job_id == job_id).first()
        assert job.submission_params is not None
        
        params = json.loads(job.submission_params)
        assert params["command"] == "python train.py"
        assert params["ntasks_per_node"] == 8
        assert params["gres"] == "gpu:2"
        assert params["gpus_per_task"] == 1
        # Upload files should be converted to absolute paths
        assert len(params["upload_files"]) == 2
        for path in params["upload_files"]:
            assert os.path.isabs(path)  # Verify paths are absolute
    
    def test_queue_job_validates_worker_exists(self, temp_db_session):
        """Test that queue_job fails for non-existent worker."""
        job_mgr = JobManager(temp_db_session)
        
        job_id = job_mgr.queue_job(
            worker_id="nonexistent_worker",
            command="echo test",
            rsync_mode="periodic"
        )
        
        assert job_id is None
    
    def test_queue_job_validates_worker_active(self, temp_db_session, test_worker):
        """Test that queue_job fails for inactive worker."""
        # Deactivate worker
        test_worker.is_active = False
        temp_db_session.commit()
        
        job_mgr = JobManager(temp_db_session)
        
        job_id = job_mgr.queue_job(
            worker_id=test_worker.worker_id,
            command="echo test",
            rsync_mode="periodic"
        )
        
        assert job_id is None
    
    def test_queue_job_returns_immediately(self, temp_db_session, test_worker):
        """Test that queue_job returns quickly without blocking."""
        job_mgr = JobManager(temp_db_session)
        
        import time
        start = time.time()
        job_id = job_mgr.queue_job(
            worker_id=test_worker.worker_id,
            command="sleep 60",  # Long running command
            rsync_mode="periodic"
        )
        elapsed = time.time() - start
        
        # Should complete in under 1 second (no SSH/rsync/sbatch)
        assert elapsed < 1.0
        assert job_id is not None
    
    def test_queue_job_uses_default_rsync_mode(self, temp_db_session, test_worker):
        """Test that queue_job uses default rsync_mode from config."""
        job_mgr = JobManager(temp_db_session)
        
        # Don't specify rsync_mode, should use database default
        job_id = job_mgr.queue_job(
            worker_id=test_worker.worker_id,
            command="echo test"
        )
        
        assert job_id is not None
        job = temp_db_session.query(Job).filter(Job.job_id == job_id).first()
        assert job.rsync_mode == "periodic"  # Default from fixture
