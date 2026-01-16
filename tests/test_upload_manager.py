"""Unit tests for upload_manager module."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from jsling.core.upload_manager import UploadManager


@pytest.fixture
def mock_ssh_client():
    """Create a mock SSH client"""
    client = MagicMock()
    client.run.return_value = MagicMock(ok=True, stdout="", stderr="")
    client.upload_file = MagicMock()
    return client


@pytest.fixture
def upload_manager(mock_ssh_client):
    """Create UploadManager with mock SSH client"""
    return UploadManager(mock_ssh_client, "/remote/workdir/job_123")


class TestUploadManager:
    """Test UploadManager class"""
    
    def test_init(self, mock_ssh_client):
        """Test UploadManager initialization"""
        manager = UploadManager(mock_ssh_client, "/remote/workdir")
        
        assert manager.ssh_client == mock_ssh_client
        assert manager.remote_workdir == "/remote/workdir"
    
    def test_create_remote_workdir_success(self, upload_manager, mock_ssh_client):
        """Test successful remote workdir creation"""
        mock_ssh_client.run.return_value = MagicMock(ok=True)
        
        result = upload_manager.create_remote_workdir()
        
        assert result is True
        mock_ssh_client.run.assert_called_once_with(
            "mkdir -p /remote/workdir/job_123",
            hide=True,
            warn=True
        )
    
    def test_create_remote_workdir_failure(self, upload_manager, mock_ssh_client):
        """Test failed remote workdir creation"""
        mock_ssh_client.run.return_value = MagicMock(ok=False)
        
        result = upload_manager.create_remote_workdir()
        
        assert result is False
    
    def test_upload_script_content_success(self, upload_manager, mock_ssh_client):
        """Test successful direct script content upload"""
        script_content = """#!/bin/bash
#SBATCH --job-name=test_job
echo "Hello from Slurm"
"""
        mock_ssh_client.run.return_value = MagicMock(ok=True, stderr="")
        
        result = upload_manager.upload_script_content(script_content)
        
        assert result is True
        
        # Verify run was called with heredoc command
        calls = mock_ssh_client.run.call_args_list
        assert len(calls) >= 1
        
        # Check heredoc command was used
        first_call = calls[0]
        cmd_arg = first_call[0][0]
        assert "cat >" in cmd_arg
        assert "JSLING_EOF" in cmd_arg
        assert script_content in cmd_arg
    
    def test_upload_script_content_failure(self, upload_manager, mock_ssh_client):
        """Test failed script content upload"""
        mock_ssh_client.run.return_value = MagicMock(ok=False, stderr="Permission denied")
        
        result = upload_manager.upload_script_content("#!/bin/bash\necho test")
        
        assert result is False
    
    def test_upload_script_content_custom_filename(self, upload_manager, mock_ssh_client):
        """Test script content upload with custom filename"""
        mock_ssh_client.run.return_value = MagicMock(ok=True, stderr="")
        
        result = upload_manager.upload_script_content(
            "#!/bin/bash\necho test",
            remote_filename="custom_script.sh"
        )
        
        assert result is True
        
        # Check filename in command
        calls = mock_ssh_client.run.call_args_list
        first_call = calls[0]
        cmd_arg = first_call[0][0]
        assert "custom_script.sh" in cmd_arg
    
    def test_upload_file_success(self, upload_manager, mock_ssh_client):
        """Test successful single file upload"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            file_path = f.name
        
        try:
            result = upload_manager.upload_file(file_path)
            
            assert result is True
            mock_ssh_client.upload_file.assert_called_once()
        finally:
            Path(file_path).unlink(missing_ok=True)
    
    def test_upload_file_not_found(self, upload_manager):
        """Test upload_file with non-existent file"""
        result = upload_manager.upload_file("/nonexistent/file.txt")
        
        assert result is False
    
    def test_upload_files_mixed_results(self, upload_manager, mock_ssh_client):
        """Test uploading multiple files with some failures"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            existing_file = f.name
        
        try:
            file_list = [existing_file, "/nonexistent/file.txt"]
            successful, failed = upload_manager.upload_files(file_list)
            
            assert existing_file in successful
            assert "/nonexistent/file.txt" in failed
        finally:
            Path(existing_file).unlink(missing_ok=True)


class TestUploadScriptContentEdgeCases:
    """Test edge cases for upload_script_content method"""
    
    def test_script_with_special_characters(self, mock_ssh_client):
        """Test uploading script with special shell characters"""
        manager = UploadManager(mock_ssh_client, "/remote/work")
        mock_ssh_client.run.return_value = MagicMock(ok=True, stderr="")
        
        script_content = '''#!/bin/bash
VAR="value with $dollar and 'quotes'"
echo "$VAR"
'''
        
        result = manager.upload_script_content(script_content)
        
        assert result is True
    
    def test_empty_script_content(self, mock_ssh_client):
        """Test uploading empty script content"""
        manager = UploadManager(mock_ssh_client, "/remote/work")
        mock_ssh_client.run.return_value = MagicMock(ok=True, stderr="")
        
        result = manager.upload_script_content("")
        
        assert result is True
    
    def test_multiline_script(self, mock_ssh_client):
        """Test uploading multiline script"""
        manager = UploadManager(mock_ssh_client, "/remote/work")
        mock_ssh_client.run.return_value = MagicMock(ok=True, stderr="")
        
        script_content = """#!/bin/bash
#SBATCH --job-name=test
#SBATCH --partition=gpu

cd /project
source activate env
python train.py \\
    --epochs 100 \\
    --lr 0.001
    
echo "Done"
"""
        
        result = manager.upload_script_content(script_content)
        
        assert result is True
        
        # Verify content is preserved in command
        call_args = mock_ssh_client.run.call_args_list[0][0][0]
        assert "python train.py" in call_args
