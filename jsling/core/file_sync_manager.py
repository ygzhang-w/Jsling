"""File Sync Manager: Hybrid streaming and batch synchronization

This module manages file synchronization for remote jobs:
- Streaming sync: tail -f for real-time logs
- Batch sync: SFTP/rsync for periodic or final-only bulk transfers

Pattern syntax (glob-style):
- '*'       : sync all files
- '*.log'   : suffix pattern
- 'test_*'  : prefix pattern
- 'logs/*'  : directory pattern
"""

import fnmatch
import os
import json
import threading
import time
import subprocess
import logging
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime

from sqlalchemy.orm import Session

from jsling.database.models import Job, Worker
from jsling.connections.ssh_client import SSHClient
from jsling.utils.job_config_parser import SyncPattern

logger = logging.getLogger(__name__)


class FileSyncManager:
    """
    File synchronization manager with hybrid streaming and batch sync
    
    Features:
    - Streaming sync: tail -f for real-time logs
      * Mandatory: Job output/error files ({job_id}.out, {job_id}.err) are always streamed
      * Optional: Additional patterns can be configured via sync_rules
    - Batch sync: SFTP/rsync for bulk file transfers (patterns like '*.csv', '*')
    - Conflict handling: Stream patterns take priority over download patterns
    
    Pattern syntax (glob-style):
    - '*'       : sync all files
    - '*.log'   : suffix pattern
    - 'test_*'  : prefix pattern
    - 'logs/*'  : directory pattern
    """
    
    def __init__(self, session: Session, job: Job):
        """
        Args:
            session: Database session
            job: Job object to synchronize
        """
        self.session = session
        self.job = job
        self.worker = job.worker
        self.ssh_client = None
        
        # Sync state
        self.is_running = False
        self.stream_threads: List[threading.Thread] = []
        self.rsync_thread: Optional[threading.Thread] = None
        
        # Sync patterns (using new glob-style syntax)
        self.stream_patterns: List[SyncPattern] = []  # e.g., ['*.log', '*.out']
        self.download_patterns: List[SyncPattern] = []  # e.g., ['*.csv', 'result.txt']
        
        # Synced files tracking
        self.streaming_files: Set[str] = set()  # Files currently being streamed
        self.downloaded_files: Set[str] = set()  # Files already downloaded
        
        # Uploaded files exclusion (to avoid overwriting local files)
        self.uploaded_files: Set[str] = set()  # Files that were uploaded, should not be downloaded
        
        # Runtime conflict tracking
        self._conflict_warnings: Set[str] = set()  # Files with conflicts (logged once)
        
        # Load sync rules
        self._load_sync_rules()
        
        # Load uploaded files list
        self._load_uploaded_files()
        
        logger.info(f"FileSyncManager initialized for job {job.job_id}")
        logger.info(f"  Mandatory stream files: {job.job_id}.out, {job.job_id}.err")
        logger.info(f"  Additional stream patterns: {[p.pattern for p in self.stream_patterns if p.pattern not in [f'{job.job_id}.out', f'{job.job_id}.err']]}")
        logger.info(f"  Download patterns: {[p.pattern for p in self.download_patterns]}")
        logger.info(f"  Excluded uploaded files: {len(self.uploaded_files)} files")
    
    def _load_sync_rules(self):
        """Load sync rules from job configuration (new glob-style format)"""
        # Always add job output/error files to stream patterns (mandatory)
        # These files are named with job_id: {job_id}.out and {job_id}.err
        mandatory_patterns = [
            SyncPattern(f'{self.job.job_id}.out'),
            SyncPattern(f'{self.job.job_id}.err'),
        ]
        self.stream_patterns.extend(mandatory_patterns)
        
        # Load from job.sync_rules (JSON field)
        if self.job.sync_rules:
            try:
                sync_rules = json.loads(self.job.sync_rules)
                
                # Load stream patterns (new format: list of pattern strings)
                if 'stream' in sync_rules:
                    stream_list = sync_rules['stream']
                    if isinstance(stream_list, list):
                        for pattern in stream_list:
                            if isinstance(pattern, str):
                                self.stream_patterns.append(SyncPattern(pattern))
                            elif isinstance(pattern, dict) and 'pattern' in pattern:
                                # Backward compatibility with old format
                                self.stream_patterns.append(SyncPattern(f"*{pattern['pattern']}" if pattern.get('type') == 'suffix' else pattern['pattern']))
                
                # Load download patterns
                if 'download' in sync_rules:
                    download_list = sync_rules['download']
                    if isinstance(download_list, list):
                        for pattern in download_list:
                            if isinstance(pattern, str):
                                self.download_patterns.append(SyncPattern(pattern))
                
                logger.info(f"Loaded sync rules from job config")
            except Exception as e:
                logger.error(f"Failed to load sync rules: {e}")
    
    def _load_uploaded_files(self):
        """Load list of uploaded files to exclude from sync.
        
        Uploaded files should not be downloaded back to avoid overwriting local files.
        """
        if self.job.uploaded_files:
            try:
                uploaded_list = json.loads(self.job.uploaded_files)
                if isinstance(uploaded_list, list):
                    for filepath in uploaded_list:
                        # Store relative paths (without leading ./)
                        rel_path = filepath.lstrip('./')
                        self.uploaded_files.add(rel_path)
                        # Also add the basename for simple matching
                        self.uploaded_files.add(os.path.basename(rel_path))
                    logger.info(f"Loaded {len(uploaded_list)} uploaded files to exclude from sync")
            except Exception as e:
                logger.warning(f"Failed to load uploaded files list: {e}")
    
    def _is_uploaded_file(self, filename: str, rel_path: str) -> bool:
        """Check if a file was uploaded and should be excluded from sync.
        
        Args:
            filename: The filename (basename)
            rel_path: Relative path from remote workdir
            
        Returns:
            True if file was uploaded and should be excluded
        """
        # Check exact match with relative path
        if rel_path in self.uploaded_files:
            return True
        # Check basename match
        if filename in self.uploaded_files:
            return True
        # Check with normalized path (no leading ./)
        normalized = rel_path.lstrip('./')
        if normalized in self.uploaded_files:
            return True
        return False
    
    def start_sync(self):
        """Start file synchronization (streaming + batch)"""
        if self.is_running:
            logger.warning(f"Sync already running for job {self.job.job_id}")
            return
        
        self.is_running = True
        
        # Update job sync status
        self.job.sync_status = 'running'
        self.job.sync_pid = os.getpid()
        self.session.commit()
        
        logger.info(f"Starting sync for job {self.job.job_id}")
        
        # Start streaming sync threads
        self._start_streaming_sync()
        
        # Start batch sync thread (if periodic mode)
        if self.job.rsync_mode == 'periodic':
            self._start_batch_sync()
        
        logger.info(f"Sync started for job {self.job.job_id}")
    
    def stop_sync(self):
        """Stop all synchronization processes"""
        if not self.is_running:
            return
        
        logger.info(f"Stopping sync for job {self.job.job_id}")
        self.is_running = False
        
        # Wait for all threads to finish
        for thread in self.stream_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        if self.rsync_thread and self.rsync_thread.is_alive():
            self.rsync_thread.join(timeout=5)
        
        # Update job sync status
        self.job.sync_status = 'stopped'
        self.session.commit()
        
        # Note: DO NOT close SSH connection here as it may be shared
        # The connection will be closed by the StatusPoller when needed
        
        logger.info(f"Sync stopped for job {self.job.job_id}")
    
    def _start_streaming_sync(self):
        """Start streaming sync threads for matching files"""
        try:
            # Get SSH client
            if not self.ssh_client:
                self.ssh_client = SSHClient(
                    host=self.worker.host,
                    username=self.worker.username,
                    auth_method=self.worker.auth_method,
                    auth_credential=self.worker.auth_credential,
                    port=self.worker.port
                )
                self.ssh_client.connect()
            
            # List files in remote workdir
            result = self.ssh_client.run(f"find {self.job.remote_workdir} -type f 2>/dev/null", hide=True, warn=True)
            if not result.ok:
                logger.error(f"Failed to list remote files: {result.stderr}")
                return
            
            # Handle empty result
            if not result.stdout.strip():
                logger.info(f"No files found in remote workdir yet")
                return
            
            remote_files = result.stdout.strip().split('\n')
            
            # Filter files matching stream patterns
            for remote_file in remote_files:
                filename = os.path.basename(remote_file)
                rel_path = os.path.relpath(remote_file, self.job.remote_workdir)
                
                # Skip sentinel files
                if filename.startswith('.jsling_status_'):
                    continue
                
                # Check if matches any stream pattern
                if self._should_stream_file(filename, rel_path):
                    # Start streaming thread for this file
                    thread = threading.Thread(
                        target=self._stream_file,
                        args=(remote_file,),
                        daemon=True
                    )
                    thread.start()
                    self.stream_threads.append(thread)
                    self.streaming_files.add(rel_path)  # Use rel_path for conflict checking
                    logger.info(f"Started streaming sync for {rel_path}")
        
        except Exception as e:
            logger.error(f"Failed to start streaming sync: {e}")
    
    def _stream_file(self, remote_file: str):
        """Stream a single file using tail -f via Paramiko channel.
        
        This method creates its own SSH connection to stream
        file content in real-time, avoiding connection sharing issues.
        
        Args:
            remote_file: Full path to the remote file to stream
        """
        # Preserve relative path structure from remote workdir
        rel_path = os.path.relpath(remote_file, self.job.remote_workdir)
        local_file = os.path.join(self.job.local_workdir, rel_path)
        
        # Ensure local directory exists (handle nested directories)
        local_dir = os.path.dirname(local_file)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        
        try:
            # Create a dedicated Paramiko client for this stream
            import paramiko
            from jsling.utils.encryption import decrypt_credential
            
            stream_client = paramiko.SSHClient()
            stream_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            credential = decrypt_credential(self.worker.auth_credential)
            
            connect_kwargs = {
                'hostname': self.worker.host,
                'port': self.worker.port,
                'username': self.worker.username,
            }
            
            if self.worker.auth_method == 'key':
                key_path = os.path.expanduser(credential)
                if os.path.exists(key_path):
                    connect_kwargs['key_filename'] = key_path
            else:
                connect_kwargs['password'] = credential
            
            stream_client.connect(**connect_kwargs)
            
            # Open a channel for tail -f command
            transport = stream_client.get_transport()
            channel = transport.open_session()
            channel.exec_command(f"tail -f {remote_file}")
            
            # Stream data to local file
            with open(local_file, 'a') as f:
                while self.is_running:
                    if channel.recv_ready():
                        data = channel.recv(4096)
                        if not data:
                            break
                        f.write(data.decode('utf-8', errors='replace'))
                        f.flush()  # Ensure real-time writing
                    elif channel.exit_status_ready():
                        # Remote process exited
                        break
                    else:
                        time.sleep(0.1)  # Small sleep to avoid busy loop
            
            channel.close()
            stream_client.close()
            logger.info(f"Streaming sync completed for {rel_path}")
        
        except Exception as e:
            logger.error(f"Streaming sync failed for {remote_file}: {e}")
    
    def _start_batch_sync(self):
        """Start periodic batch sync thread"""
        self.rsync_thread = threading.Thread(
            target=self._batch_sync_loop,
            daemon=True
        )
        self.rsync_thread.start()
    
    def _batch_sync_loop(self):
        """Periodic batch sync loop"""
        # Get rsync interval from job, or fallback to database config
        interval = self.job.rsync_interval
        if interval is None:
            from jsling.core.config_manager import ConfigManager
            config_mgr = ConfigManager(self.session)
            interval = config_mgr.get_typed("rsync_interval")
        
        logger.info(f"Starting batch sync loop with interval {interval}s")
        
        while self.is_running:
            try:
                # Execute rsync
                self._execute_rsync()
                
                # Wait for next interval
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Batch sync error: {e}")
                time.sleep(interval)
    
    def _execute_rsync(self):
        """Execute rsync command to download all files"""
        # For password authentication, use SFTP instead of rsync
        if self.worker.auth_method == 'password':
            self._execute_sftp_sync()
            return
        
        try:
            # Build rsync command with proper SSH parameters
            # Handle IPv6 addresses - they need to be wrapped in brackets for rsync
            host = self.worker.host
            if ':' in host:  # IPv6 address
                host = f"[{host}]"
            remote_path = f"{self.worker.username}@{host}:{self.job.remote_workdir}/"
            local_path = self.job.local_workdir
            
            # Build SSH command with port and key/password
            ssh_cmd = self._build_rsync_ssh_command()
            
            # Exclude sentinel files, streaming files, uploaded files, and job.sh
            exclude_args = ['--exclude', '.jsling_status_*', '--exclude', 'job.sh']
            for stream_file in self.streaming_files:
                exclude_args.extend(['--exclude', stream_file])
            # Exclude uploaded files to avoid overwriting local files
            for uploaded_file in self.uploaded_files:
                exclude_args.extend(['--exclude', uploaded_file])
            
            rsync_cmd = [
                'rsync',
                '-avz',
                '-e', ssh_cmd,
                *exclude_args,
                remote_path,
                local_path
            ]
            
            # Execute rsync with environment that disables proxy
            env = os.environ.copy()
            # Disable common proxy environment variables
            for proxy_var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']:
                env.pop(proxy_var, None)
            
            # Execute rsync
            result = subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minutes timeout
                env=env
            )
            
            if result.returncode == 0:
                # Update last rsync time
                self.job.last_rsync_time = datetime.now()
                self.session.commit()
                logger.info(f"Batch sync completed for job {self.job.job_id}")
            else:
                logger.error(f"Rsync failed: {result.stderr}")
        
        except subprocess.TimeoutExpired:
            logger.error("Rsync timeout (5 minutes)")
        except Exception as e:
            logger.error(f"Rsync error: {e}")
    
    def _execute_sftp_sync(self):
        """Execute file sync via SFTP for password authentication."""
        try:
            # Get or create Paramiko SFTP client
            if not self.ssh_client:
                self.ssh_client = SSHClient(
                    host=self.worker.host,
                    username=self.worker.username,
                    auth_method=self.worker.auth_method,
                    auth_credential=self.worker.auth_credential,
                    port=self.worker.port
                )
                self.ssh_client.connect()
            
            paramiko_client = self.ssh_client._get_paramiko_client()
            sftp = paramiko_client.open_sftp()
            
            try:
                # List remote files
                self._sftp_download_dir(sftp, self.job.remote_workdir, self.job.local_workdir)
                
                # Update last rsync time
                self.job.last_rsync_time = datetime.now()
                self.session.commit()
                logger.info(f"SFTP sync completed for job {self.job.job_id}")
            finally:
                sftp.close()
                
        except Exception as e:
            logger.error(f"SFTP sync error: {e}")
    
    def _sftp_download_dir(self, sftp, remote_dir: str, local_dir: str, is_root: bool = True):
        """Recursively download directory via SFTP.
        
        Args:
            sftp: SFTP client
            remote_dir: Remote directory path
            local_dir: Local directory path
            is_root: Whether this is the root directory (for relative path calculation)
        """
        import stat
        
        os.makedirs(local_dir, exist_ok=True)
        
        try:
            for entry in sftp.listdir_attr(remote_dir):
                remote_path = f"{remote_dir}/{entry.filename}"
                local_path = os.path.join(local_dir, entry.filename)
                
                # Calculate relative path from remote workdir
                rel_path = os.path.relpath(remote_path, self.job.remote_workdir)
                
                # Skip sentinel files
                if entry.filename.startswith('.jsling_status_'):
                    continue
                
                # Skip job.sh (system-generated submission script)
                if entry.filename == 'job.sh':
                    continue
                
                # Skip streaming files (use relative path for accurate matching)
                if rel_path in self.streaming_files:
                    continue
                
                if stat.S_ISDIR(entry.st_mode):
                    # Recursive download subdirectory
                    self._sftp_download_dir(sftp, remote_path, local_path, is_root=False)
                else:
                    # Check if this file should be downloaded
                    if self._should_download_file(entry.filename, remote_path):
                        try:
                            sftp.get(remote_path, local_path)
                            self.downloaded_files.add(rel_path)
                            logger.debug(f"Downloaded {rel_path}")
                        except Exception as e:
                            logger.warning(f"Failed to download {remote_path}: {e}")
        except Exception as e:
            logger.error(f"Error listing directory {remote_dir}: {e}")
    
    def _should_stream_file(self, filename: str, rel_path: str) -> bool:
        """Check if a file should be streamed based on stream patterns.
        
        Args:
            filename: The filename (basename)
            rel_path: Relative path from remote workdir
            
        Returns:
            True if file should be streamed
        """
        # Skip uploaded files - they should not be synced back
        if self._is_uploaded_file(filename, rel_path):
            return False
        
        for pattern in self.stream_patterns:
            if pattern.matches(filename) or pattern.matches_path(rel_path):
                return True
        return False
    
    def _should_download_file(self, filename: str, remote_path: str) -> bool:
        """Check if a file should be downloaded based on sync configuration.
        
        Conflict resolution: If a file matches both stream and download patterns,
        stream takes priority and the file will NOT be batch-downloaded.
        
        Args:
            filename: The filename (basename)
            remote_path: Full remote path
            
        Returns:
            True if file should be downloaded
        """
        # Calculate relative path from remote workdir
        rel_path = os.path.relpath(remote_path, self.job.remote_workdir)
        
        # Skip job.sh (system-generated submission script)
        if filename == 'job.sh':
            return False
        
        # Skip uploaded files - they should not be synced back
        if self._is_uploaded_file(filename, rel_path):
            return False
        
        # If no download patterns, don't download anything
        if not self.download_patterns:
            return False
        
        # Check if file matches any download pattern
        matches_download = False
        for pattern in self.download_patterns:
            if pattern.matches(filename) or pattern.matches_path(rel_path):
                matches_download = True
                break
        
        if not matches_download:
            return False
        
        # CONFLICT CHECK: If file also matches stream patterns, stream takes priority
        matches_stream = self._should_stream_file(filename, rel_path)
        
        if matches_stream:
            # Log conflict warning once per file
            if rel_path not in self._conflict_warnings:
                self._conflict_warnings.add(rel_path)
                logger.warning(
                    f"Runtime conflict: '{rel_path}' matches both stream and download patterns. "
                    f"Using streaming sync (download skipped)."
                )
            return False
        
        # Also check if already being streamed
        if rel_path in self.streaming_files:
            return False
        
        return True
    
    def _build_rsync_ssh_command(self) -> str:
        """Build SSH command string for rsync with proper authentication."""
        from jsling.utils.encryption import decrypt_credential
        
        ssh_parts = ['ssh']
        
        # Add port
        if self.worker.port and self.worker.port != 22:
            ssh_parts.extend(['-p', str(self.worker.port)])
        
        # Add key file or use sshpass for password
        if self.worker.auth_method == 'key':
            credential = decrypt_credential(self.worker.auth_credential)
            key_path = os.path.expanduser(credential)
            if os.path.exists(key_path):
                ssh_parts.extend(['-i', key_path])
        
        # Disable strict host key checking for automation
        ssh_parts.extend(['-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null'])
        
        return ' '.join(ssh_parts)
    
    def force_sync(self, direction: str = 'download'):
        """
        Force immediate rsync synchronization
        
        Args:
            direction: 'download', 'upload', or 'both'
        """
        if direction in ['download', 'both']:
            self._execute_rsync()
        
        if direction in ['upload', 'both']:
            self._execute_rsync_upload()
    
    def _execute_rsync_upload(self):
        """Execute rsync to upload files to remote"""
        try:
            local_path = f"{self.job.local_workdir}/"
            
            # Handle IPv6 addresses - they need to be wrapped in brackets for rsync
            host = self.worker.host
            if ':' in host:  # IPv6 address
                host = f"[{host}]"
            remote_path = f"{self.worker.username}@{host}:{self.job.remote_workdir}/"
            
            # Build SSH command with port and key/password
            ssh_cmd = self._build_rsync_ssh_command()
            
            rsync_cmd = [
                'rsync',
                '-avz',
                '-e', ssh_cmd,
                '--progress',
                local_path,
                remote_path
            ]
            
            # Execute rsync with environment that disables proxy
            env = os.environ.copy()
            for proxy_var in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']:
                env.pop(proxy_var, None)
            
            result = subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                timeout=300,
                env=env
            )
            
            if result.returncode == 0:
                logger.info(f"Upload sync completed for job {self.job.job_id}")
            else:
                logger.error(f"Upload rsync failed: {result.stderr}")
        
        except Exception as e:
            logger.error(f"Upload rsync error: {e}")
    
    def final_sync(self):
        """Execute final synchronization when job completes"""
        logger.info(f"Executing final sync for job {self.job.job_id}")
        
        # Stop streaming threads
        for thread in self.stream_threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        # Execute final rsync (download all files)
        self.force_sync(direction='download')
        
        # Update sync status
        self.job.sync_status = 'completed'
        self.session.commit()
        
        logger.info(f"Final sync completed for job {self.job.job_id}")
