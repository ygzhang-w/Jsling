"""Tests for WorkerStatusPoller (integrated into StatusPoller)."""

import time
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch

import pytest

from jsling.database.models import Worker, WorkerLiveStatus
from jsling.core.status_poller import StatusPoller


@pytest.fixture
def sample_worker(temp_db):
    """Create a sample worker for testing."""
    worker = Worker(
        worker_id="test_worker_1",
        worker_name="TestWorker",
        host="test.example.com",
        port=22,
        username="testuser",
        auth_method="key",
        auth_credential="fake_key",
        remote_workdir="/home/testuser/work",
        queue_name="compute",
        worker_type="cpu",
        ntasks_per_node=32,
        is_active=True
    )
    temp_db.add(worker)
    temp_db.commit()
    return worker


class TestWorkerStatusPoller:
    """Test cases for worker status polling in unified StatusPoller."""
    
    def test_init(self, temp_db):
        """Test StatusPoller initialization with worker polling."""
        poller = StatusPoller(session=temp_db, poll_interval=10, worker_poll_interval=60)
        
        assert poller.session == temp_db
        assert poller.poll_interval == 10
        assert poller.worker_poll_interval == 60
        assert not poller.is_running()
    
    def test_start_stop(self, temp_db):
        """Test starting and stopping the poller."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # Start
        result = poller.start()
        assert result is True
        assert poller.is_running()
        
        # Stop
        poller.stop()
        assert not poller.is_running()
    
    def test_start_already_running(self, temp_db):
        """Test starting an already running poller."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        poller.start()
        
        # Try to start again
        result = poller.start()
        assert result is True  # Should return True but not start another thread
        
        poller.stop()
    
    def test_context_manager(self, temp_db):
        """Test using poller as context manager."""
        with StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60) as poller:
            assert poller.is_running()
        
        assert not poller.is_running()
    
    def test_update_worker_status(self, temp_db, sample_worker):
        """Test updating worker status in database."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # Update status
        poller._update_worker_status(
            temp_db,
            sample_worker.worker_id,
            queue_state="up",
            cpu_alloc=10,
            cpu_idle=22,
            cpu_other=0,
            cpu_total=32,
            nodes_available=2,
            nodes_total=4
        )
        
        # Verify status was created
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status is not None
        assert status.queue_state == "up"
        assert status.cpu_alloc == 10
        assert status.cpu_idle == 22
        assert status.cpu_other == 0
        assert status.cpu_total == 32
        assert status.nodes_available == 2
        assert status.nodes_total == 4
        assert status.error_message is None
    
    def test_update_worker_status_updates_existing(self, temp_db, sample_worker):
        """Test that updating status updates existing record."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # Create initial status
        poller._update_worker_status(
            temp_db, sample_worker.worker_id,
            queue_state="up", nodes_available=2, nodes_total=4
        )
        
        # Update status
        poller._update_worker_status(
            temp_db, sample_worker.worker_id,
            queue_state="up", nodes_available=1, nodes_total=4
        )
        
        # Should only have one record
        count = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).count()
        
        assert count == 1
        
        # Verify updated values
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status.nodes_available == 1
    
    def test_update_worker_error(self, temp_db, sample_worker):
        """Test updating worker status with error message."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # Update with error
        poller._update_worker_error(temp_db, sample_worker.worker_id, "Connection failed")
        
        # Verify error was recorded
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status is not None
        assert status.error_message == "Connection failed"
    
    def test_update_clears_previous_error(self, temp_db, sample_worker):
        """Test that successful update clears previous error."""
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # First set an error
        poller._update_worker_error(temp_db, sample_worker.worker_id, "Connection failed")
        
        # Then update successfully
        poller._update_worker_status(
            temp_db, sample_worker.worker_id,
            queue_state="up", nodes_available=2, nodes_total=4
        )
        
        # Error should be cleared
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status.error_message is None
        assert status.queue_state == "up"
    
    @patch('jsling.core.status_poller.WorkerConnection')
    def test_poll_worker_status_success(self, mock_worker_conn, temp_db, sample_worker):
        """Test successful polling of worker status."""
        # Setup mock SSH result
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "up|10/22/0/32|4|1/2/1/4"
        mock_ssh.run.return_value = mock_result
        
        mock_conn_instance = MagicMock()
        mock_conn_instance.ssh_client = mock_ssh
        mock_conn_instance.ssh_client._connection = MagicMock()
        mock_conn_instance.ssh_client._connection.is_connected = True
        mock_worker_conn.return_value = mock_conn_instance
        
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        poller._worker_connections[sample_worker.worker_id] = mock_conn_instance
        
        # Poll worker status
        poller._poll_worker_status(temp_db, sample_worker)
        
        # Verify status was updated
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status is not None
        assert status.queue_state == "up"
        assert status.cpu_alloc == 10
        assert status.cpu_idle == 22
        assert status.nodes_total == 4
    
    @patch('jsling.core.status_poller.WorkerConnection')
    def test_poll_worker_status_connection_failure(self, mock_worker_conn, temp_db, sample_worker):
        """Test polling handles connection failure."""
        # Make connection fail
        mock_worker_conn.side_effect = Exception("Connection refused")
        
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        
        # This should not raise, but should log error
        poller._poll_worker_status(temp_db, sample_worker)
        
        # Check error was recorded
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status is not None
        assert status.error_message == "Failed to connect"


class TestSinfoOutputParsing:
    """Test parsing of sinfo command output in StatusPoller."""
    
    @patch('jsling.core.status_poller.WorkerConnection')
    def test_parse_standard_output(self, mock_worker_conn, temp_db, sample_worker):
        """Test parsing standard sinfo output."""
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = True
        # Standard format: availability|cpu_states|total_nodes|node_states
        mock_result.stdout = "up|64/128/0/192|6|2/3/1/6"
        mock_ssh.run.return_value = mock_result
        
        mock_conn = MagicMock()
        mock_conn.ssh_client = mock_ssh
        mock_conn.ssh_client._connection = MagicMock()
        mock_conn.ssh_client._connection.is_connected = True
        mock_worker_conn.return_value = mock_conn
        
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        poller._worker_connections[sample_worker.worker_id] = mock_conn
        
        poller._poll_worker_status(temp_db, sample_worker)
        
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status.queue_state == "up"
        assert status.cpu_alloc == 64
        assert status.cpu_idle == 128
        assert status.cpu_other == 0
        assert status.cpu_total == 192
        assert status.nodes_total == 6
        assert status.nodes_available == 3  # idle nodes
    
    @patch('jsling.core.status_poller.WorkerConnection')
    def test_parse_down_queue(self, mock_worker_conn, temp_db, sample_worker):
        """Test parsing output when queue is down."""
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "down|0/0/64/64|2|0/0/2/2"
        mock_ssh.run.return_value = mock_result
        
        mock_conn = MagicMock()
        mock_conn.ssh_client = mock_ssh
        mock_conn.ssh_client._connection = MagicMock()
        mock_conn.ssh_client._connection.is_connected = True
        mock_worker_conn.return_value = mock_conn
        
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        poller._worker_connections[sample_worker.worker_id] = mock_conn
        
        poller._poll_worker_status(temp_db, sample_worker)
        
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status.queue_state == "down"
        assert status.cpu_other == 64
        assert status.nodes_available == 0
    
    @patch('jsling.core.status_poller.WorkerConnection')
    def test_handle_malformed_output(self, mock_worker_conn, temp_db, sample_worker):
        """Test handling malformed sinfo output."""
        mock_ssh = MagicMock()
        mock_result = MagicMock()
        mock_result.ok = True
        mock_result.stdout = "up|invalid"  # Malformed
        mock_result.stderr = ""
        mock_ssh.run.return_value = mock_result
        
        mock_conn = MagicMock()
        mock_conn.ssh_client = mock_ssh
        mock_conn.ssh_client._connection = MagicMock()
        mock_conn.ssh_client._connection.is_connected = True
        mock_worker_conn.return_value = mock_conn
        
        poller = StatusPoller(session=temp_db, poll_interval=60, worker_poll_interval=60)
        poller._worker_connections[sample_worker.worker_id] = mock_conn
        
        poller._poll_worker_status(temp_db, sample_worker)
        
        # Should record error
        status = temp_db.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == sample_worker.worker_id
        ).first()
        
        assert status is not None
        assert "Unexpected sinfo output" in status.error_message
