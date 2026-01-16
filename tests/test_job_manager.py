"""Unit tests for job_manager module."""

import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base, Worker
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
