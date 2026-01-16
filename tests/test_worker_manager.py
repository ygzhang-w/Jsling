"""Tests for WorkerManager."""

import pytest
from datetime import datetime

from jsling.core.worker_manager import WorkerManager
from jsling.database.models import Worker as WorkerModel


@pytest.fixture
def worker_manager(temp_db):
    """Create WorkerManager with test database."""
    return WorkerManager(temp_db)


@pytest.fixture
def sample_workers(temp_db):
    """Create sample workers in database."""
    workers = [
        WorkerModel(
            worker_id="cpu_worker_1",
            worker_name="cpu_cluster_1",
            host="cpu1.example.com",
            port=22,
            username="user",
            auth_method="key",
            auth_credential="encrypted_key",
            remote_workdir="/home/user/work",
            queue_name="cpu_queue",
            worker_type="cpu",
            ntasks_per_node=16,
            is_active=True,
            created_at=datetime.utcnow()
        ),
        WorkerModel(
            worker_id="gpu_worker_1",
            worker_name="gpu_cluster_1",
            host="gpu1.example.com",
            port=22,
            username="user",
            auth_method="key",
            auth_credential="encrypted_key",
            remote_workdir="/home/user/work",
            queue_name="gpu_queue",
            worker_type="gpu",
            ntasks_per_node=8,
            gres="gpu:4",
            gpus_per_task=1,
            is_active=True,
            created_at=datetime.utcnow()
        ),
        WorkerModel(
            worker_id="cpu_worker_inactive",
            worker_name="cpu_cluster_inactive",
            host="cpu2.example.com",
            port=22,
            username="user",
            auth_method="key",
            auth_credential="encrypted_key",
            remote_workdir="/home/user/work",
            queue_name="cpu_queue",
            worker_type="cpu",
            ntasks_per_node=16,
            is_active=False,
            created_at=datetime.utcnow()
        ),
    ]
    
    for w in workers:
        temp_db.add(w)
    temp_db.commit()
    
    return workers


class TestListWorkers:
    """Tests for list_workers method."""
    
    def test_list_workers_all_active(self, worker_manager, sample_workers):
        """Test listing all active workers."""
        workers = worker_manager.list_workers()
        
        assert len(workers) == 2
        worker_ids = [w.worker_id for w in workers]
        assert "cpu_worker_1" in worker_ids
        assert "gpu_worker_1" in worker_ids
        assert "cpu_worker_inactive" not in worker_ids
    
    def test_list_workers_include_inactive(self, worker_manager, sample_workers):
        """Test listing all workers including inactive."""
        workers = worker_manager.list_workers(include_inactive=True)
        
        assert len(workers) == 3
        worker_ids = [w.worker_id for w in workers]
        assert "cpu_worker_inactive" in worker_ids
    
    def test_list_workers_filter_by_cpu_type(self, worker_manager, sample_workers):
        """Test filtering workers by CPU type."""
        workers = worker_manager.list_workers(worker_type="cpu")
        
        assert len(workers) == 1
        assert workers[0].worker_id == "cpu_worker_1"
        assert workers[0].worker_type == "cpu"
    
    def test_list_workers_filter_by_gpu_type(self, worker_manager, sample_workers):
        """Test filtering workers by GPU type."""
        workers = worker_manager.list_workers(worker_type="gpu")
        
        assert len(workers) == 1
        assert workers[0].worker_id == "gpu_worker_1"
        assert workers[0].worker_type == "gpu"
    
    def test_list_workers_filter_by_type_include_inactive(self, worker_manager, sample_workers):
        """Test filtering by type with inactive included."""
        workers = worker_manager.list_workers(worker_type="cpu", include_inactive=True)
        
        assert len(workers) == 2
        for w in workers:
            assert w.worker_type == "cpu"
    
    def test_list_workers_empty_result(self, worker_manager):
        """Test empty result when no workers exist."""
        workers = worker_manager.list_workers()
        
        assert len(workers) == 0
