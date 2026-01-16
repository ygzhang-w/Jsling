"""SSH connection pool management."""

import time
from typing import Dict, Optional
from threading import Lock

from jsling.connections.ssh_client import SSHClient


class ConnectionPool:
    """Connection pool for reusing SSH connections."""
    
    def __init__(self, max_idle_time: int = 300):
        """Initialize connection pool.
        
        Args:
            max_idle_time: Maximum idle time in seconds before closing connection
        """
        self.max_idle_time = max_idle_time
        self._pool: Dict[str, tuple[SSHClient, float]] = {}
        self._lock = Lock()
    
    def _get_key(self, host: str, username: str, port: int) -> str:
        """Generate unique key for connection.
        
        Args:
            host: Remote host
            username: SSH username
            port: SSH port
            
        Returns:
            Unique connection key
        """
        return f"{username}@{host}:{port}"
    
    def get_connection(self, host: str, username: str, port: int = 22,
                      auth_method: str = "key", auth_credential: str = None) -> SSHClient:
        """Get or create SSH connection.
        
        Args:
            host: Remote host
            username: SSH username
            port: SSH port
            auth_method: Authentication method
            auth_credential: Encrypted credential
            
        Returns:
            SSHClient instance
        """
        key = self._get_key(host, username, port)
        
        with self._lock:
            # Check if connection exists and is still valid
            if key in self._pool:
                client, last_used = self._pool[key]
                
                # Check if connection is still alive
                if client.test_connection():
                    # Update last used time
                    self._pool[key] = (client, time.time())
                    return client
                else:
                    # Connection dead, remove from pool
                    del self._pool[key]
            
            # Create new connection
            client = SSHClient(
                host=host,
                username=username,
                port=port,
                auth_method=auth_method,
                auth_credential=auth_credential
            )
            
            # Add to pool
            self._pool[key] = (client, time.time())
            
            return client
    
    def release_connection(self, host: str, username: str, port: int = 22):
        """Release a connection (update last used time).
        
        Args:
            host: Remote host
            username: SSH username
            port: SSH port
        """
        key = self._get_key(host, username, port)
        
        with self._lock:
            if key in self._pool:
                client, _ = self._pool[key]
                self._pool[key] = (client, time.time())
    
    def cleanup_idle(self):
        """Close and remove idle connections."""
        current_time = time.time()
        
        with self._lock:
            keys_to_remove = []
            
            for key, (client, last_used) in self._pool.items():
                if current_time - last_used > self.max_idle_time:
                    client.close()
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self._pool[key]
    
    def close_all(self):
        """Close all connections in pool."""
        with self._lock:
            for client, _ in self._pool.values():
                client.close()
            self._pool.clear()
    
    def __del__(self):
        """Cleanup on deletion."""
        self.close_all()


# Global connection pool instance
_global_pool: Optional[ConnectionPool] = None


def get_connection_pool() -> ConnectionPool:
    """Get global connection pool instance.
    
    Returns:
        ConnectionPool instance
    """
    global _global_pool
    
    if _global_pool is None:
        _global_pool = ConnectionPool()
    
    return _global_pool
