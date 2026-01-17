"""Tests for SSHClient.get_ssh_command_args() and Worker.get_login_command()."""

import pytest
from unittest.mock import patch, MagicMock

from jsling.connections.ssh_client import SSHClient
from jsling.connections.worker import Worker
from jsling.database.models import Worker as WorkerModel


class TestSSHClientGetSSHCommandArgs:
    """Test cases for SSHClient.get_ssh_command_args()."""
    
    def test_basic_ssh_command(self):
        """Test basic SSH command with default port."""
        client = SSHClient(
            host="example.com",
            username="testuser",
            port=22,
            auth_method="key",
            auth_credential=None
        )
        
        args = client.get_ssh_command_args()
        
        assert args[0] == "ssh"
        assert "testuser@example.com" in args
        # Port 22 should not be explicitly added
        assert "-p" not in args
    
    def test_ssh_command_with_custom_port(self):
        """Test SSH command with non-default port."""
        client = SSHClient(
            host="example.com",
            username="testuser",
            port=2222,
            auth_method="key",
            auth_credential=None
        )
        
        args = client.get_ssh_command_args()
        
        assert "-p" in args
        port_index = args.index("-p")
        assert args[port_index + 1] == "2222"
    
    @patch('jsling.connections.ssh_client.decrypt_credential')
    def test_ssh_command_with_key_file(self, mock_decrypt):
        """Test SSH command includes key file when auth_method is key."""
        mock_decrypt.return_value = "/path/to/key"
        
        client = SSHClient(
            host="example.com",
            username="testuser",
            port=22,
            auth_method="key",
            auth_credential="encrypted_key"
        )
        
        args = client.get_ssh_command_args()
        
        assert "-i" in args
        key_index = args.index("-i")
        assert args[key_index + 1] == "/path/to/key"
    
    def test_ssh_command_with_password_auth(self):
        """Test SSH command with password auth doesn't include -i."""
        client = SSHClient(
            host="example.com",
            username="testuser",
            port=22,
            auth_method="password",
            auth_credential="encrypted_password"
        )
        
        args = client.get_ssh_command_args()
        
        assert "-i" not in args
    
    def test_ssh_command_with_extra_args(self):
        """Test SSH command with extra arguments."""
        client = SSHClient(
            host="example.com",
            username="testuser",
            port=22,
            auth_method="key",
            auth_credential=None
        )
        
        args = client.get_ssh_command_args(extra_args=["-t", "cd /tmp && bash"])
        
        assert "-t" in args
        assert "cd /tmp && bash" in args


class TestWorkerGetLoginCommand:
    """Test cases for Worker.get_login_command()."""
    
    @pytest.fixture
    def mock_worker_model(self):
        """Create a mock WorkerModel."""
        model = MagicMock(spec=WorkerModel)
        model.worker_id = "test_worker"
        model.worker_name = "Test Worker"
        model.host = "cluster.example.com"
        model.port = 22
        model.username = "admin"
        model.auth_method = "key"
        model.auth_credential = None
        model.remote_workdir = "/home/admin/jobs"
        model.queue_name = "default"
        model.worker_type = "cpu"
        model.ntasks_per_node = 4
        model.gres = None
        model.gpus_per_task = None
        model.is_active = True
        return model
    
    def test_get_login_command_without_directory(self, mock_worker_model):
        """Test login command without directory."""
        worker = Worker(mock_worker_model)
        
        args = worker.get_login_command()
        
        assert args[0] == "ssh"
        assert "admin@cluster.example.com" in args
        # No -t flag without directory
        assert "-t" not in args
    
    def test_get_login_command_with_directory(self, mock_worker_model):
        """Test login command with directory changes to that directory."""
        worker = Worker(mock_worker_model)
        
        args = worker.get_login_command(directory="/scratch/job123")
        
        assert "-t" in args
        # Find the command argument after -t
        t_index = args.index("-t")
        cmd = args[t_index + 1]
        assert "cd /scratch/job123" in cmd
        assert "$SHELL -l" in cmd
    
    def test_get_login_command_with_custom_port(self, mock_worker_model):
        """Test login command with custom SSH port."""
        mock_worker_model.port = 2222
        worker = Worker(mock_worker_model)
        
        args = worker.get_login_command()
        
        assert "-p" in args
        port_index = args.index("-p")
        assert args[port_index + 1] == "2222"
