"""SSH client wrapper using Fabric with reverse tunnel support."""

import logging
import socket
import select
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, Callable

from fabric import Connection
from invoke import UnexpectedExit
import paramiko

from jsling.utils.encryption import decrypt_credential


logger = logging.getLogger(__name__)


class ReverseTunnel:
    """Manages a single SSH reverse tunnel.
    
    A reverse tunnel forwards a remote port to a local address,
    allowing remote processes to connect back to local services.
    
    For HPC environments, the tunnel can bind to the login node's
    internal IP address so compute nodes can access it.
    """
    
    def __init__(self, transport: paramiko.Transport,
                 remote_port: int, local_host: str, local_port: int,
                 remote_bind_address: str = ''):
        """Initialize reverse tunnel.
        
        Args:
            transport: Paramiko SSH transport
            remote_port: Remote port to listen on
            local_host: Local host to forward to
            local_port: Local port to forward to
            remote_bind_address: Address to bind on remote host
                                 '' = all interfaces (0.0.0.0)
                                 '127.0.0.1' = localhost only
                                 '10.0.0.1' = specific IP
        """
        self.transport = transport
        self.remote_port = remote_port
        self.local_host = local_host
        self.local_port = local_port
        self.remote_bind_address = remote_bind_address
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._channel: Optional[paramiko.Channel] = None
    
    def start(self) -> bool:
        """Start the reverse tunnel.
        
        Returns:
            True if tunnel started successfully
        """
        if self._running:
            return True
        
        try:
            # Request remote port forwarding
            # Bind to specified address ('' = all interfaces)
            self.transport.request_port_forward(self.remote_bind_address, self.remote_port)
            self._running = True
            
            # Start handler thread
            self._thread = threading.Thread(
                target=self._handle_tunnel,
                daemon=True,
                name=f"ReverseTunnel-{self.remote_port}"
            )
            self._thread.start()
            
            bind_info = self.remote_bind_address or "0.0.0.0"
            logger.info(f"Reverse tunnel started: {bind_info}:{self.remote_port} -> {self.local_host}:{self.local_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start reverse tunnel: {e}")
            self._running = False
            return False
    
    def _handle_tunnel(self) -> None:
        """Handle incoming tunnel connections."""
        while self._running:
            try:
                # Accept forwarded connection with timeout
                channel = self.transport.accept(timeout=1)
                if channel is None:
                    continue
                
                # Forward data between remote and local
                self._forward_channel(channel)
                
            except Exception as e:
                if self._running:
                    logger.debug(f"Tunnel handler error (may be normal): {e}")
    
    def _forward_channel(self, channel: paramiko.Channel) -> None:
        """Forward data between SSH channel and local socket.
        
        Args:
            channel: Paramiko channel from remote connection
        """
        try:
            # Connect to local service
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.local_host, self.local_port))
            sock.setblocking(False)
            channel.setblocking(False)
            
            # Forward data in a separate thread
            def forward():
                try:
                    while True:
                        r, w, x = select.select([sock, channel], [], [], 1)
                        
                        if sock in r:
                            data = sock.recv(4096)
                            if not data:
                                break
                            channel.send(data)
                        
                        if channel in r:
                            data = channel.recv(4096)
                            if not data:
                                break
                            sock.send(data)
                except Exception as e:
                    logger.debug(f"Forward connection closed: {e}")
                finally:
                    sock.close()
                    channel.close()
            
            thread = threading.Thread(target=forward, daemon=True)
            thread.start()
            
        except Exception as e:
            logger.error(f"Failed to forward channel to local: {e}")
            channel.close()
    
    def stop(self) -> None:
        """Stop the reverse tunnel."""
        if not self._running:
            return
        
        self._running = False
        
        try:
            self.transport.cancel_port_forward(self.remote_bind_address, self.remote_port)
        except Exception as e:
            logger.debug(f"Error canceling port forward: {e}")
        
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        
        bind_info = self.remote_bind_address or "0.0.0.0"
        logger.info(f"Reverse tunnel stopped: {bind_info}:{self.remote_port}")
    
    def is_running(self) -> bool:
        """Check if tunnel is running."""
        return self._running


class SSHClient:
    """SSH client wrapper for remote command execution with reverse tunnel support."""
    
    def __init__(self, host: str, username: str, port: int = 22,
                 auth_method: str = "key", auth_credential: str = None):
        """Initialize SSH client.
        
        Args:
            host: Remote host address
            username: SSH username
            port: SSH port
            auth_method: Authentication method (only 'key' is supported)
            auth_credential: Encrypted path to SSH key file
        """
        self.host = host
        self.username = username
        self.port = port
        self.auth_method = auth_method
        self._encrypted_credential = auth_credential
        self._connection: Optional[Connection] = None
        self._paramiko_client: Optional[paramiko.SSHClient] = None
        self._reverse_tunnels: Dict[int, ReverseTunnel] = {}
    
    def _get_credential(self) -> str:
        """Decrypt and get credential.
        
        Returns:
            Decrypted credential
        """
        if self._encrypted_credential:
            return decrypt_credential(self._encrypted_credential)
        return ""
    
    def get_ssh_command_args(self, extra_args: list = None) -> list:
        """Get SSH command arguments for interactive session.
        
        This is used when you need to spawn an actual SSH process
        (e.g., for interactive login) rather than using Fabric/Paramiko.
        
        Args:
            extra_args: Additional SSH arguments to include (e.g., ["-t", "cd /dir && bash"])
            
        Returns:
            List of SSH command arguments (e.g., ["ssh", "-p", "22", "-i", "key", "user@host"])
        """
        args = ["ssh"]
        
        # Add port if not default
        if self.port and self.port != 22:
            args.extend(["-p", str(self.port)])
        
        # Add key file if using key auth
        if self.auth_method == "key" and self._encrypted_credential:
            credential = self._get_credential()
            if credential:
                args.extend(["-i", credential])
        
        # Add user@host
        args.append(f"{self.username}@{self.host}")
        
        # Add extra arguments
        if extra_args:
            args.extend(extra_args)
        
        return args
    
    def connect(self) -> Connection:
        """Establish SSH connection.
        
        Returns:
            Fabric Connection object
            
        Raises:
            Exception: If connection fails
        """
        if self._connection and self._connection.is_connected:
            return self._connection
        
        try:
            credential = self._get_credential()
            
            connect_kwargs: Dict[str, Any] = {}
            
            # Use key file authentication
            key_path = Path(credential).expanduser()
            if not key_path.exists():
                raise FileNotFoundError(f"SSH key file not found: {credential}")
            connect_kwargs["key_filename"] = str(key_path)
            
            self._connection = Connection(
                host=self.host,
                user=self.username,
                port=self.port,
                connect_kwargs=connect_kwargs
            )
            
            # Test connection
            self._connection.open()
            
            logger.info(f"SSH connection established to {self.username}@{self.host}:{self.port}")
            return self._connection
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.host}: {str(e)}")
            raise
    
    def run(self, command: str, hide: bool = False, warn: bool = False) -> Any:
        """Run command on remote host.
        
        Args:
            command: Command to execute
            hide: Hide command output
            warn: Don't raise exception on failure
            
        Returns:
            Command result
            
        Raises:
            UnexpectedExit: If command fails and warn=False
        """
        conn = self.connect()
        
        try:
            result = conn.run(command, hide=hide, warn=warn)
            return result
        except UnexpectedExit as e:
            logger.error(f"Command failed: {command}")
            logger.error(f"Error: {str(e)}")
            raise
    
    def test_connection(self) -> bool:
        """Test SSH connection.
        
        Returns:
            True if connection successful
        """
        try:
            conn = self.connect()
            result = conn.run("echo 'test'", hide=True)
            return result.ok
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def check_slurm_queue(self, queue_name: str) -> bool:
        """Check if Slurm queue exists.
        
        Args:
            queue_name: Queue/partition name
            
        Returns:
            True if queue exists
        """
        try:
            result = self.run(f"sinfo -p {queue_name}", hide=True, warn=True)
            
            if result.ok and queue_name in result.stdout:
                logger.info(f"Queue '{queue_name}' found")
                return True
            else:
                logger.warning(f"Queue '{queue_name}' not found")
                return False
                
        except Exception as e:
            logger.error(f"Failed to check queue: {str(e)}")
            return False
    
    def check_directory(self, path: str, create: bool = True) -> bool:
        """Check if directory exists, optionally create it.
        
        Args:
            path: Directory path
            create: Create if doesn't exist
            
        Returns:
            True if directory exists or was created
        """
        try:
            # Check if exists
            result = self.run(f"test -d {path}", hide=True, warn=True)
            
            if result.ok:
                logger.info(f"Directory exists: {path}")
                return True
            
            if create:
                # Try to create
                result = self.run(f"mkdir -p {path}", hide=True, warn=True)
                if result.ok:
                    logger.info(f"Created directory: {path}")
                    return True
                else:
                    logger.error(f"Failed to create directory: {path}")
                    return False
            else:
                logger.warning(f"Directory does not exist: {path}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to check directory: {str(e)}")
            return False
    
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload file to remote host.
        
        Args:
            local_path: Local file path
            remote_path: Remote file path
            
        Returns:
            True if upload successful
        """
        try:
            conn = self.connect()
            conn.put(local_path, remote_path)
            logger.info(f"Uploaded {local_path} to {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload file: {str(e)}")
            return False
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download file from remote host.
        
        Args:
            remote_path: Remote file path
            local_path: Local file path
            
        Returns:
            True if download successful
        """
        try:
            conn = self.connect()
            conn.get(remote_path, local_path)
            logger.info(f"Downloaded {remote_path} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download file: {str(e)}")
            return False
    
    def _get_paramiko_client(self) -> paramiko.SSHClient:
        """Get or create paramiko SSH client for tunneling.
        
        Returns:
            Paramiko SSHClient instance
        """
        if self._paramiko_client is not None:
            # Check if still connected
            transport = self._paramiko_client.get_transport()
            if transport and transport.is_active():
                return self._paramiko_client
        
        # Create new paramiko client
        self._paramiko_client = paramiko.SSHClient()
        self._paramiko_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        credential = self._get_credential()
        
        connect_kwargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.username,
        }
        
        # Use key file authentication
        key_path = Path(credential).expanduser()
        if key_path.exists():
            connect_kwargs['key_filename'] = str(key_path)
        
        self._paramiko_client.connect(**connect_kwargs)
        logger.info(f"Paramiko connection established to {self.host}")
        
        return self._paramiko_client
    
    def create_reverse_tunnel(self, remote_port: int,
                               local_host: str = "127.0.0.1",
                               local_port: Optional[int] = None,
                               remote_bind_address: str = '') -> bool:
        """Create a reverse tunnel (remote port forwarding).
        
        This allows remote processes to connect to a local service by
        connecting to the specified address:port on the remote machine.
        
        For HPC environments where compute nodes need to access the tunnel,
        set remote_bind_address to the login node's internal IP or ''.
        
        Args:
            remote_port: Port to listen on remote machine
            local_host: Local host to forward to (default 127.0.0.1)
            local_port: Local port to forward to (default same as remote_port)
            remote_bind_address: Address to bind on remote host:
                                 '' = all interfaces (0.0.0.0) - for compute node access
                                 '127.0.0.1' = localhost only
                                 '10.x.x.x' = specific internal IP
            
        Returns:
            True if tunnel created successfully
        """
        if local_port is None:
            local_port = remote_port
        
        if remote_port in self._reverse_tunnels:
            tunnel = self._reverse_tunnels[remote_port]
            if tunnel.is_running():
                logger.info(f"Reverse tunnel already exists on port {remote_port}")
                return True
        
        try:
            client = self._get_paramiko_client()
            transport = client.get_transport()
            
            if not transport or not transport.is_active():
                logger.error("SSH transport is not active")
                return False
            
            tunnel = ReverseTunnel(transport, remote_port, local_host, local_port,
                                   remote_bind_address=remote_bind_address)
            if tunnel.start():
                self._reverse_tunnels[remote_port] = tunnel
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to create reverse tunnel: {e}")
            return False
    
    def get_remote_internal_ip(self) -> Optional[str]:
        """Get the remote host's internal IP address.
        
        This is useful for HPC environments where compute nodes need to
        connect to the login node via its internal IP.
        
        Returns:
            Internal IP address or None if cannot be determined
        """
        try:
            # Try to get the primary internal IP
            # This command gets the IP used for the default route
            result = self.run(
                "hostname -I 2>/dev/null | awk '{print $1}' || "
                "ip route get 1.1.1.1 2>/dev/null | grep -oP 'src \\K[^ ]+' || "
                "hostname -i 2>/dev/null",
                hide=True, warn=True
            )
            
            if result.ok and result.stdout.strip():
                ip = result.stdout.strip().split()[0]
                # Validate it's an IP address
                import re
                if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ip):
                    logger.info(f"Remote internal IP: {ip}")
                    return ip
        except Exception as e:
            logger.debug(f"Failed to get remote internal IP: {e}")
        
        return None
    
    def test_tunnel_connectivity(self, host: str, port: int, timeout: int = 3) -> bool:
        """Test if a host:port is reachable from the remote machine.
        
        Args:
            host: Target host
            port: Target port
            timeout: Connection timeout in seconds
            
        Returns:
            True if connection successful
        """
        try:
            # Use bash to test connectivity
            cmd = f"timeout {timeout} bash -c '</dev/tcp/{host}/{port}' 2>/dev/null && echo OK || echo FAIL"
            result = self.run(cmd, hide=True, warn=True)
            return result.ok and "OK" in result.stdout
        except Exception as e:
            logger.debug(f"Tunnel connectivity test failed: {e}")
            return False
    
    def close_reverse_tunnel(self, remote_port: int) -> None:
        """Close a specific reverse tunnel.
        
        Args:
            remote_port: Remote port of the tunnel to close
        """
        if remote_port in self._reverse_tunnels:
            tunnel = self._reverse_tunnels.pop(remote_port)
            tunnel.stop()
    
    def close_all_reverse_tunnels(self) -> None:
        """Close all reverse tunnels."""
        for port, tunnel in list(self._reverse_tunnels.items()):
            tunnel.stop()
        self._reverse_tunnels.clear()
    
    def get_active_tunnels(self) -> Dict[int, Tuple[str, int]]:
        """Get all active reverse tunnels.
        
        Returns:
            Dict mapping remote_port to (local_host, local_port)
        """
        return {
            port: (tunnel.local_host, tunnel.local_port)
            for port, tunnel in self._reverse_tunnels.items()
            if tunnel.is_running()
        }
    
    def close(self):
        """Close SSH connection and all tunnels."""
        # Close all reverse tunnels first
        self.close_all_reverse_tunnels()
        
        # Close paramiko client
        if self._paramiko_client:
            try:
                self._paramiko_client.close()
            except:
                pass
            self._paramiko_client = None
        
        # Close fabric connection
        if self._connection and self._connection.is_connected:
            self._connection.close()
            logger.info(f"SSH connection closed to {self.host}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
