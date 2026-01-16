"""Upload manager - handles file uploads to remote workers."""

import os
from pathlib import Path
from typing import List, Optional

from jsling.connections.ssh_client import SSHClient


class UploadManager:
    """Manages file uploads to remote worker for job execution."""
    
    def __init__(self, ssh_client: SSHClient, remote_workdir: str):
        """Initialize upload manager.
        
        Args:
            ssh_client: SSH client for remote connection
            remote_workdir: Remote working directory path
        """
        self.ssh_client = ssh_client
        self.remote_workdir = remote_workdir
    
    def create_remote_workdir(self) -> bool:
        """Create remote working directory.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory with parents
            cmd = f"mkdir -p {self.remote_workdir}"
            result = self.ssh_client.run(cmd, hide=True, warn=True)
            return result.ok
        except Exception as e:
            print(f"Failed to create remote workdir: {e}")
            return False
    
    def upload_script_content(self, content: str, remote_filename: str = "job.sh") -> bool:
        """Upload script content directly to remote workdir without creating local file.
        
        Uses SSH heredoc (cat > file << 'EOF') to write content directly on remote server.
        The quoted heredoc delimiter ('JSLING_EOF') ensures content is written literally
        without shell variable expansion or command substitution.
        
        Args:
            content: Script content to upload
            remote_filename: Filename on remote server
            
        Returns:
            True if successful, False otherwise
        """
        try:
            remote_path = os.path.join(self.remote_workdir, remote_filename)
            
            # Use heredoc to write content directly on remote file system
            # Note: 'JSLING_EOF' (quoted) means content is treated literally - no escaping needed
            cmd = f"cat > {remote_path} << 'JSLING_EOF'\n{content}\nJSLING_EOF"
            result = self.ssh_client.run(cmd, hide=True, warn=True)
            
            if not result.ok:
                raise RuntimeError(f"Failed to write script: {result.stderr}")
            
            # Make script executable
            self.ssh_client.run(f"chmod +x {remote_path}", hide=True, warn=True)
            
            return True
        except Exception as e:
            print(f"Failed to upload script content: {e}")
            return False
    
    def upload_file(self, local_path: str, remote_subdir: str = "") -> bool:
        """Upload a single file to remote workdir.
        
        Args:
            local_path: Path to local file
            remote_subdir: Subdirectory in remote workdir (empty string means directly in workdir)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            local_file = Path(local_path).expanduser()
            if not local_file.exists():
                raise FileNotFoundError(f"File not found: {local_path}")
            
            # Determine remote directory
            if remote_subdir:
                remote_dir = os.path.join(self.remote_workdir, remote_subdir)
                self.ssh_client.run(f"mkdir -p {remote_dir}", hide=True, warn=True)
            else:
                remote_dir = self.remote_workdir
            
            # Upload file
            remote_path = os.path.join(remote_dir, local_file.name)
            self.ssh_client.upload_file(str(local_file), remote_path)
            
            return True
        except Exception as e:
            print(f"Failed to upload file {local_path}: {e}")
            return False
    
    def upload_directory(self, local_dir: str, remote_subdir: str = "") -> bool:
        """Upload entire directory to remote workdir.
        
        Args:
            local_dir: Path to local directory
            remote_subdir: Subdirectory in remote workdir (empty string means directly in workdir)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            local_path = Path(local_dir).expanduser()
            if not local_path.exists():
                raise FileNotFoundError(f"Directory not found: {local_dir}")
            
            if not local_path.is_dir():
                raise ValueError(f"Path is not a directory: {local_dir}")
            
            # Determine remote target directory
            if remote_subdir:
                remote_base = os.path.join(self.remote_workdir, remote_subdir)
            else:
                remote_base = self.remote_workdir
            remote_target = os.path.join(remote_base, local_path.name)
            self.ssh_client.run(f"mkdir -p {remote_target}", hide=True, warn=True)
            
            # Upload all files in directory recursively
            for item in local_path.rglob('*'):
                if item.is_file():
                    # Calculate relative path
                    rel_path = item.relative_to(local_path)
                    remote_file = os.path.join(remote_target, str(rel_path))
                    
                    # Ensure parent directory exists
                    remote_parent = os.path.dirname(remote_file)
                    self.ssh_client.run(f"mkdir -p {remote_parent}", hide=True, warn=True)
                    
                    # Upload file
                    self.ssh_client.upload_file(str(item), remote_file)
            
            return True
        except Exception as e:
            print(f"Failed to upload directory {local_dir}: {e}")
            return False
    
    def upload_files(self, file_list: List[str]) -> tuple[List[str], List[str]]:
        """Upload multiple files/directories.
        
        Args:
            file_list: List of file/directory paths to upload
            
        Returns:
            Tuple of (successful_uploads, failed_uploads)
        """
        successful = []
        failed = []
        
        for path in file_list:
            local_path = Path(path).expanduser()
            
            if not local_path.exists():
                failed.append(path)
                continue
            
            if local_path.is_file():
                success = self.upload_file(str(local_path))
            elif local_path.is_dir():
                success = self.upload_directory(str(local_path))
            else:
                failed.append(path)
                continue
            
            if success:
                successful.append(path)
            else:
                failed.append(path)
        
        return successful, failed
    
    def set_file_permissions(self, remote_path: str, mode: str = "644") -> bool:
        """Set file permissions on remote file.
        
        Args:
            remote_path: Remote file path
            mode: Permission mode (e.g., "755", "644")
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cmd = f"chmod {mode} {remote_path}"
            result = self.ssh_client.run(cmd, hide=True, warn=True)
            return result.ok
        except Exception as e:
            print(f"Failed to set permissions: {e}")
            return False
