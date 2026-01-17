"""Worker abstraction for remote cluster management."""

from typing import Optional

from jsling.connections.ssh_client import SSHClient
from jsling.database.models import Worker as WorkerModel


class Worker:
    """Worker abstraction representing a remote cluster queue."""
    
    def __init__(self, worker_model: WorkerModel):
        """Initialize Worker from database model.
        
        Args:
            worker_model: SQLAlchemy Worker model
        """
        self.worker_id = worker_model.worker_id
        self.worker_name = worker_model.worker_name
        self.host = worker_model.host
        self.port = worker_model.port
        self.username = worker_model.username
        self.auth_method = worker_model.auth_method
        self.auth_credential = worker_model.auth_credential
        self.remote_workdir = worker_model.remote_workdir
        self.queue_name = worker_model.queue_name
        self.worker_type = worker_model.worker_type
        self.ntasks_per_node = worker_model.ntasks_per_node
        self.gres = worker_model.gres
        self.gpus_per_task = worker_model.gpus_per_task
        self.is_active = worker_model.is_active
        
        self._ssh_client: Optional[SSHClient] = None
    
    @property
    def ssh_client(self) -> SSHClient:
        """Get or create SSH client.
        
        Returns:
            SSHClient instance
        """
        if self._ssh_client is None:
            self._ssh_client = SSHClient(
                host=self.host,
                username=self.username,
                port=self.port,
                auth_method=self.auth_method,
                auth_credential=self.auth_credential
            )
        return self._ssh_client
    
    def test_connection(self) -> bool:
        """Test SSH connection to worker.
        
        Returns:
            True if connection successful
        """
        return self.ssh_client.test_connection()
    
    def validate(self) -> tuple[bool, str]:
        """Validate worker configuration.
        
        Returns:
            Tuple of (success, message)
        """
        # Test SSH connection
        if not self.ssh_client.test_connection():
            return False, f"SSH connection failed to {self.host}"
        
        # Check queue exists
        if not self.ssh_client.check_slurm_queue(self.queue_name):
            return False, f"Slurm queue '{self.queue_name}' not found"
        
        # Check remote workdir
        if not self.ssh_client.check_directory(self.remote_workdir, create=True):
            return False, f"Cannot access remote workdir: {self.remote_workdir}"
        
        return True, "Validation successful"
    
    def get_login_command(self, directory: str = None) -> list:
        """Get SSH command for interactive login to worker.
        
        Args:
            directory: Optional directory to cd into after login
            
        Returns:
            List of command arguments for os.execvp
        """
        extra_args = None
        if directory:
            # Use -t for pseudo-terminal, cd to directory and start login shell
            extra_args = ["-t", f"cd {directory} && exec $SHELL -l"]
        
        return self.ssh_client.get_ssh_command_args(extra_args=extra_args)
    
    def close(self):
        """Close SSH connection."""
        if self._ssh_client:
            self._ssh_client.close()
    
    def __repr__(self) -> str:
        return f"<Worker(id={self.worker_id}, name={self.worker_name}, type={self.worker_type})>"
