"""Unit tests for job_wrapper module."""

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jsling.database.models import Base, Worker
from jsling.core.job_wrapper import JobWrapper


@pytest.fixture
def temp_db_with_worker():
    """Create a temporary database with a test worker"""
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
        ntasks_per_node=16,
        gres="gpu:v100:2",
        gpus_per_task=1
    )
    session.add(worker)
    session.commit()
    
    yield session, worker
    
    session.close()
    engine.dispose()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def sample_command():
    """Create a sample job command"""
    return """echo "Starting job"
python train.py --epochs 100
echo "Job completed"
"""


class TestJobWrapper:
    """Test JobWrapper class"""
    
    def test_init(self, temp_db_with_worker, sample_command):
        """Test JobWrapper initialization"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="20240115_103045",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir",
            ntasks_per_node=16,
            gres="gpu:v100:2",
            gpus_per_task=1
        )
        
        assert wrapper.job_id == "20240115_103045"
        assert wrapper.worker == worker
        assert wrapper.command == sample_command
        assert wrapper.remote_workdir == "/remote/workdir"
        assert wrapper.ntasks_per_node == 16
        assert wrapper.gres == "gpu:v100:2"
        assert wrapper.gpus_per_task == 1
    
    def test_generate_sbatch_directives(self, temp_db_with_worker, sample_command):
        """Test SBATCH directive generation"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir",
            ntasks_per_node=16,
            gres="gpu:v100:2",
            gpus_per_task=1
        )
        
        directives = wrapper.generate_sbatch_directives()
        
        # Check essential SBATCH directives
        assert "#SBATCH --job-name=job_test" in directives
        assert "#SBATCH --partition=gpu_queue" in directives
        assert "#SBATCH --ntasks-per-node=16" in directives
        assert "#SBATCH --gres=gpu:v100:2" in directives
        # When gpus-per-task is used, --ntasks must also be specified
        assert "#SBATCH --ntasks=16" in directives  # 16 ntasks_per_node * 1 node
        assert "#SBATCH --gpus-per-task=1" in directives
        assert "#SBATCH --output=" in directives
        assert "#SBATCH --error=" in directives
    
    def test_generate_sentinel_code(self, temp_db_with_worker, sample_command):
        """Test sentinel file tracking code generation"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir"
        )
        
        pre_script, post_script, error_handler = wrapper.generate_sentinel_code()
        
        # Check sentinel files are referenced
        assert ".jsling_status_running" in pre_script
        assert ".jsling_status_done" in post_script
        assert ".jsling_status_failed" in error_handler
        
        # Check key operations
        assert "START_TIME" in pre_script
        assert "EXIT_CODE" in post_script
        assert "trap" in error_handler
    
    def test_wrap_script(self, temp_db_with_worker, sample_command):
        """Test complete script wrapping"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir",
            ntasks_per_node=8,
            gres="gpu:v100:1"
        )
        
        wrapped_content = wrapper.wrap_script()
        
        # Check wrapped content includes all components
        assert "#!/bin/bash" in wrapped_content
        assert "#SBATCH" in wrapped_content
        assert ".jsling_status_running" in wrapped_content
        assert ".jsling_status_done" in wrapped_content
        assert "trap" in wrapped_content
        
        # Check original command content is included
        assert 'echo "Starting job"' in wrapped_content
        assert "python train.py --epochs 100" in wrapped_content
    
    def test_save_wrapped_script(self, temp_db_with_worker, sample_command):
        """Test saving wrapped script to file"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir"
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "job_wrapped.sh"
            wrapper.save_wrapped_script(str(output_path))
            
            # Check file exists
            assert output_path.exists()
            
            # Check file is executable
            assert output_path.stat().st_mode & 0o111
            
            # Check content
            content = output_path.read_text()
            assert "#!/bin/bash" in content
            assert "#SBATCH" in content
    
    def test_custom_resource_params(self, temp_db_with_worker, sample_command):
        """Test custom resource parameters override worker defaults"""
        session, worker = temp_db_with_worker
        
        # Worker has ntasks_per_node=16, but we override to 32
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir",
            ntasks_per_node=32,  # Override
            gres="gpu:a100:4"    # Override
        )
        
        directives = wrapper.generate_sbatch_directives()
        
        assert "#SBATCH --ntasks-per-node=32" in directives
        assert "#SBATCH --gres=gpu:a100:4" in directives
    
    def test_use_worker_defaults(self, temp_db_with_worker, sample_command):
        """Test using worker default values when not specified"""
        session, worker = temp_db_with_worker
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir="/remote/workdir"
            # No custom params, should use worker defaults
        )
        
        directives = wrapper.generate_sbatch_directives()
        
        assert "#SBATCH --ntasks-per-node=16" in directives  # Worker default
        assert "#SBATCH --gres=gpu:v100:2" in directives      # Worker default
    
    def test_sentinel_file_paths(self, temp_db_with_worker, sample_command):
        """Test sentinel file paths are correct"""
        session, worker = temp_db_with_worker
        
        remote_workdir = "/remote/jobs/20240115"
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=sample_command,
            remote_workdir=remote_workdir
        )
        
        pre_script, post_script, error_handler = wrapper.generate_sentinel_code()
        
        # Check paths include remote_workdir
        assert f"{remote_workdir}/.jsling_status_running" in pre_script
        assert f"{remote_workdir}/.jsling_status_done" in post_script
        assert f"{remote_workdir}/.jsling_status_failed" in error_handler
    
    def test_multiline_command(self, temp_db_with_worker):
        """Test multiline command is properly wrapped"""
        session, worker = temp_db_with_worker
        
        multiline_cmd = """cd /project
source activate env
python train.py --lr 0.001"""
        
        wrapper = JobWrapper(
            job_id="job_test",
            worker=worker,
            command=multiline_cmd,
            remote_workdir="/remote/workdir"
        )
        
        wrapped_content = wrapper.wrap_script()
        
        assert "cd /project" in wrapped_content
        assert "source activate env" in wrapped_content
        assert "python train.py --lr 0.001" in wrapped_content
