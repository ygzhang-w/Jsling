"""
Runner Daemon - Background daemon process for Jsling.

This module provides a daemon process that runs independently of the CLI,
enabling continuous background tasks like:
- Job status polling via sentinel files
- Streaming file synchronization
- Periodic rsync synchronization
"""

import os
import sys
import signal
import logging
import atexit
import time
from pathlib import Path
from typing import Optional
from datetime import datetime

from jsling.core.status_poller import StatusPoller
from jsling.database.session import init_database
from jsling.database.config import JSLING_HOME

logger = logging.getLogger(__name__)


# Default paths
def get_jsling_dir() -> Path:
    """Get Jsling configuration directory."""
    return JSLING_HOME


def get_pid_file() -> Path:
    """Get PID file path."""
    return get_jsling_dir() / "runner.pid"


def get_log_file() -> Path:
    """Get log file path."""
    return get_jsling_dir() / "runner.log"


class RunnerDaemon:
    """
    Daemon process for running background Jsling services.
    
    Features:
    - Runs as a background daemon process
    - Manages PID file for tracking
    - Handles signals for graceful shutdown
    - Runs StatusPoller for job monitoring
    """
    
    def __init__(self, pid_file: Optional[Path] = None, log_file: Optional[Path] = None):
        """
        Initialize runner daemon.
        
        Args:
            pid_file: Path to PID file
            log_file: Path to log file
        """
        self.pid_file = pid_file or get_pid_file()
        self.log_file = log_file or get_log_file()
        self.poller: Optional[StatusPoller] = None
        self._running = False
    
    def _setup_logging(self) -> None:
        """Setup logging to file."""
        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure file handler
        file_handler = logging.FileHandler(str(self.log_file))
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        
        # Set up root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        
        # Also log jsling modules at debug level
        jsling_logger = logging.getLogger('jsling')
        jsling_logger.setLevel(logging.DEBUG)
    
    def _daemonize(self) -> None:
        """
        Daemonize the process using double-fork technique.
        
        This ensures the daemon is completely detached from the terminal.
        """
        # First fork
        try:
            pid = os.fork()
            if pid > 0:
                # Parent process exits
                sys.exit(0)
        except OSError as e:
            logger.error(f"First fork failed: {e}")
            sys.exit(1)
        
        # Decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
        
        # Second fork
        try:
            pid = os.fork()
            if pid > 0:
                # First child exits
                sys.exit(0)
        except OSError as e:
            logger.error(f"Second fork failed: {e}")
            sys.exit(1)
        
        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Redirect stdin, stdout, stderr to /dev/null
        with open('/dev/null', 'r') as devnull:
            os.dup2(devnull.fileno(), sys.stdin.fileno())
        with open('/dev/null', 'a+') as devnull:
            os.dup2(devnull.fileno(), sys.stdout.fileno())
            os.dup2(devnull.fileno(), sys.stderr.fileno())
    
    def _write_pid_file(self) -> None:
        """Write current PID to file."""
        self.pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid = os.getpid()
        with open(self.pid_file, 'w') as f:
            f.write(str(pid))
        logger.info(f"PID file written: {self.pid_file} (PID: {pid})")
    
    def _remove_pid_file(self) -> None:
        """Remove PID file."""
        try:
            if self.pid_file.exists():
                self.pid_file.unlink()
                logger.info(f"PID file removed: {self.pid_file}")
        except Exception as e:
            logger.error(f"Failed to remove PID file: {e}")
    
    def _signal_handler(self, signum: int, frame) -> None:
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGHUP, self._signal_handler)
    
    def get_pid(self) -> Optional[int]:
        """Get PID from PID file.
        
        Returns:
            PID if daemon is running, None otherwise
        """
        try:
            if self.pid_file.exists():
                with open(self.pid_file, 'r') as f:
                    pid = int(f.read().strip())
                
                # Check if process is actually running
                try:
                    os.kill(pid, 0)  # Signal 0 just checks if process exists
                    return pid
                except OSError:
                    # Process not running, clean up stale PID file
                    self._remove_pid_file()
                    return None
        except Exception:
            pass
        return None
    
    def is_running(self) -> bool:
        """Check if daemon is running."""
        return self.get_pid() is not None
    
    def start(self, foreground: bool = False) -> bool:
        """
        Start the daemon.
        
        Args:
            foreground: If True, run in foreground (for debugging)
            
        Returns:
            True if started successfully
        """
        # Check if already running
        pid = self.get_pid()
        if pid:
            logger.warning(f"Daemon already running with PID {pid}")
            return False
        
        if not foreground:
            # Daemonize the process
            self._daemonize()
        
        # Setup after fork
        self._setup_logging()
        self._setup_signal_handlers()
        self._write_pid_file()
        
        # Register cleanup on exit
        atexit.register(self._remove_pid_file)
        
        logger.info("=" * 60)
        logger.info("Jsling Runner Daemon starting...")
        logger.info(f"PID: {os.getpid()}")
        logger.info(f"Log file: {self.log_file}")
        logger.info("=" * 60)
        
        # Initialize database
        try:
            init_database()
            logger.info("Database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            return False
        
        # Start unified status poller (handles both jobs and workers)
        self.poller = StatusPoller()
        self.poller.start()
        logger.info("Unified status poller started (job + worker)")
        
        # Main loop
        self._running = True
        try:
            while self._running:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        finally:
            self._shutdown()
        
        return True
    
    def _shutdown(self) -> None:
        """Shutdown daemon gracefully."""
        logger.info("Shutting down daemon...")
        
        # Stop unified status poller
        if self.poller:
            try:
                self.poller.stop()
                logger.info("Unified status poller stopped")
            except Exception as e:
                logger.error(f"Error stopping poller: {e}")
        
        # Remove PID file
        self._remove_pid_file()
        
        logger.info("Daemon shutdown complete")
    
    def stop(self) -> bool:
        """
        Stop the daemon.
        
        Returns:
            True if stopped successfully
        """
        pid = self.get_pid()
        if not pid:
            logger.info("Daemon is not running")
            return True
        
        logger.info(f"Stopping daemon with PID {pid}...")
        
        try:
            # Send SIGTERM first
            os.kill(pid, signal.SIGTERM)
            
            # Wait for process to terminate
            for _ in range(30):  # Wait up to 30 seconds
                try:
                    os.kill(pid, 0)
                    time.sleep(1)
                except OSError:
                    # Process has terminated
                    logger.info("Daemon stopped successfully")
                    self._remove_pid_file()
                    return True
            
            # Force kill if still running
            logger.warning("Daemon did not stop gracefully, sending SIGKILL...")
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)
            self._remove_pid_file()
            return True
            
        except OSError as e:
            logger.error(f"Error stopping daemon: {e}")
            self._remove_pid_file()
            return False
    
    def restart(self, foreground: bool = False) -> bool:
        """
        Restart the daemon.
        
        Args:
            foreground: If True, run in foreground after restart
            
        Returns:
            True if restarted successfully
        """
        self.stop()
        time.sleep(2)  # Wait a bit before starting
        return self.start(foreground=foreground)
    
    def status(self) -> dict:
        """
        Get daemon status.
        
        Returns:
            Status dictionary with running state and details
        """
        pid = self.get_pid()
        
        status = {
            "running": pid is not None,
            "pid": pid,
            "pid_file": str(self.pid_file),
            "log_file": str(self.log_file),
        }
        
        # Get log file info
        if self.log_file.exists():
            stat = self.log_file.stat()
            status["log_size"] = stat.st_size
            status["log_modified"] = datetime.fromtimestamp(stat.st_mtime).isoformat()
        
        return status


# Convenience functions for CLI
def start_daemon(foreground: bool = False) -> bool:
    """Start the runner daemon."""
    daemon = RunnerDaemon()
    return daemon.start(foreground=foreground)


def stop_daemon() -> bool:
    """Stop the runner daemon."""
    daemon = RunnerDaemon()
    return daemon.stop()


def restart_daemon(foreground: bool = False) -> bool:
    """Restart the runner daemon."""
    daemon = RunnerDaemon()
    return daemon.restart(foreground=foreground)


def get_daemon_status() -> dict:
    """Get daemon status."""
    daemon = RunnerDaemon()
    return daemon.status()


def is_daemon_running() -> bool:
    """Check if daemon is running."""
    daemon = RunnerDaemon()
    return daemon.is_running()
