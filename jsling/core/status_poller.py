"""
Status Poller - Sentinel file based job status synchronization.

This module provides reliable job status synchronization via sentinel file polling:
- Primary sync mechanism for HPC environments
- Works through shared file systems (NFS, Lustre, GPFS, etc.)
- Managed by RunnerDaemon for persistent background operation
"""

import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Callable, List, Set
from dataclasses import dataclass

from sqlalchemy.orm import Session

from jsling.database.models import Job, Worker, WorkerLiveStatus
from jsling.database.session import get_db
from jsling.connections.ssh_client import SSHClient
from jsling.connections.worker import Worker as WorkerConnection
from jsling.core.file_sync_manager import FileSyncManager
from jsling.core.config_manager import ConfigManager

logger = logging.getLogger(__name__)


@dataclass
class JobStatusInfo:
    """Parsed status from sentinel files."""
    status: str  # pending, running, completed, failed
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None


class StatusPoller:
    """
    Unified poller for job and worker status synchronization.
    
    Features:
    - Polls sentinel files on remote workers via SSH for job status
    - Falls back to squeue for jobs without sentinel files
    - Polls sinfo for worker queue status (nodes, CPU usage)
    - Thread-safe job and worker tracking
    - Configurable poll intervals (separate for jobs and workers)
    - Managed by RunnerDaemon for persistent operation
    """
    
    def __init__(self, session: Optional[Session] = None,
                 poll_interval: Optional[int] = None,
                 worker_poll_interval: Optional[int] = None):
        """
        Initialize status poller.
        
        Args:
            session: Database session (optional, will create new if not provided)
            poll_interval: Job status polling interval override
            worker_poll_interval: Worker status polling interval override
            
        Raises:
            KeyError: If required configuration keys are missing from database
        """
        self.session = session
        
        # Load configuration from database (no fallback to hardcoded defaults)
        temp_session = session if session else get_db()
        try:
            config_mgr = ConfigManager(temp_session)
            db_poll_interval = config_mgr.get_sync_interval()
            db_worker_poll_interval = config_mgr.get_typed("worker_status_poll_interval")
            self.initial_delay = config_mgr.get_typed("status_poll_initial_delay")
        except KeyError as e:
            raise KeyError(
                f"Missing configuration in database: {e}. "
                "Please run 'jsling init' to initialize the database."
            )
        finally:
            if not session:
                temp_session.close()
        
        # Use overrides if provided, otherwise use values from DB
        self.poll_interval = poll_interval if poll_interval is not None else db_poll_interval
        self.worker_poll_interval = worker_poll_interval if worker_poll_interval is not None else db_worker_poll_interval
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._worker_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Cache worker connections
        self._worker_connections: Dict[str, WorkerConnection] = {}
        
        # Track active file sync managers
        self._file_sync_managers: Dict[str, FileSyncManager] = {}
        
        # External callbacks
        self._status_callbacks: List[Callable[[str, str, Optional[JobStatusInfo]], None]] = []
        
        # Track last worker poll time
        self._last_worker_poll: float = 0
        
        logger.info(f"StatusPoller initialized with job interval {poll_interval}s, worker interval {worker_poll_interval}s")
    
    def add_status_callback(self, callback: Callable[[str, str, Optional[JobStatusInfo]], None]) -> None:
        """Add callback for status changes.
        
        Args:
            callback: Function(job_id, new_status, status_info) called on status change
        """
        self._status_callbacks.append(callback)
    
    def start(self) -> bool:
        """Start background polling.
        
        Returns:
            True if started successfully
        """
        with self._lock:
            if self._running:
                logger.warning("Status poller already running")
                return True
            
            self._running = True
            
            # Start job status polling thread
            self._thread = threading.Thread(
                target=self._poll_loop,
                daemon=True,
                name="JobStatusPoller"
            )
            self._thread.start()
            
            # Start worker status polling thread
            self._worker_thread = threading.Thread(
                target=self._worker_poll_loop,
                daemon=True,
                name="WorkerStatusPoller"
            )
            self._worker_thread.start()
            
            logger.info("Status poller started (job + worker)")
        
        # Initialize sync for already running jobs
        self._init_running_jobs_sync()
        
        return True
    
    def _init_running_jobs_sync(self) -> None:
        """Initialize file sync for jobs that are already running.
        
        This handles the case where the daemon is restarted while jobs
        are still running.
        """
        try:
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                # Find all running jobs that don't have active sync managers
                running_jobs = session.query(Job).filter(
                    Job.job_status == "running"
                ).all()
                
                for job in running_jobs:
                    if job.job_id not in self._file_sync_managers:
                        try:
                            logger.info(f"Initializing file sync for already running job {job.job_id}")
                            sync_mgr = FileSyncManager(session, job)
                            sync_mgr.start_sync()
                            self._file_sync_managers[job.job_id] = sync_mgr
                        except Exception as e:
                            logger.error(f"Failed to init file sync for job {job.job_id}: {e}")
                
                if running_jobs:
                    logger.info(f"Initialized sync for {len(self._file_sync_managers)} running jobs")
                    
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error initializing running jobs sync: {e}")
    
    def stop(self) -> None:
        """Stop background polling."""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
        
        # Stop job status thread
        if self._thread:
            self._thread.join(timeout=self.poll_interval + 5)
            self._thread = None
        
        # Stop worker status thread
        if self._worker_thread:
            self._worker_thread.join(timeout=self.worker_poll_interval + 5)
            self._worker_thread = None
        
        # Close cached connections
        for conn in self._worker_connections.values():
            try:
                conn.close()
            except:
                pass
        self._worker_connections.clear()
        
        # Stop all file sync managers
        for sync_mgr in self._file_sync_managers.values():
            try:
                sync_mgr.stop_sync()
            except:
                pass
        self._file_sync_managers.clear()
        
        logger.info("Status poller stopped")
    
    def is_running(self) -> bool:
        """Check if poller is running."""
        return self._running
    
    def _poll_loop(self) -> None:
        """Main polling loop."""
        # Initial delay
        for _ in range(self.initial_delay):
            if not self._running:
                return
            time.sleep(1)
        
        while self._running:
            try:
                self._poll_all_jobs()
            except Exception as e:
                logger.error(f"Error in poll loop: {e}")
            
            # Wait for next poll interval
            for _ in range(self.poll_interval):
                if not self._running:
                    break
                time.sleep(1)
    
    def _poll_all_jobs(self) -> None:
        """Poll status for all active jobs."""
        try:
            # Get database session
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                # Find all non-terminal jobs
                jobs = session.query(Job).filter(
                    ~Job.job_status.in_(["completed", "failed", "cancelled"])
                ).all()
                
                # Clean up sync managers for jobs terminated externally (e.g., via CLI cancel)
                # This handles the case where job status is changed outside of StatusPoller
                self._cleanup_terminated_job_sync_managers(session, jobs)
                
                if not jobs:
                    return
                
                logger.debug(f"Polling {len(jobs)} active jobs")
                
                for job in jobs:
                    try:
                        self._poll_job_status(session, job)
                    except Exception as e:
                        logger.error(f"Error polling job {job.job_id}: {e}")
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error in _poll_all_jobs: {e}")
    
    def _cleanup_terminated_job_sync_managers(self, session: Session, active_jobs: List[Job]) -> None:
        """Clean up sync managers for jobs that were terminated externally.
        
        This handles cases where a job's status is changed outside of StatusPoller
        (e.g., via jcancel CLI command), ensuring the sync manager is properly stopped.
        
        Args:
            session: Database session
            active_jobs: List of currently active (non-terminal) jobs
        """
        if not self._file_sync_managers:
            return
        
        # Get IDs of active jobs
        active_job_ids = {job.job_id for job in active_jobs}
        
        # Find sync managers for jobs that are no longer active
        orphaned_job_ids = [
            job_id for job_id in self._file_sync_managers.keys()
            if job_id not in active_job_ids
        ]
        
        for job_id in orphaned_job_ids:
            # Verify job is actually in terminal state
            job = session.query(Job).filter(Job.job_id == job_id).first()
            if job and job.job_status in ["completed", "failed", "cancelled"]:
                logger.info(
                    f"Cleaning up sync manager for externally terminated job {job_id} "
                    f"(status: {job.job_status})"
                )
                try:
                    sync_mgr = self._file_sync_managers[job_id]
                    # Execute final sync to retrieve any remaining files
                    sync_mgr.final_sync()
                    sync_mgr.stop_sync()
                    del self._file_sync_managers[job_id]
                except Exception as e:
                    logger.error(f"Failed to cleanup sync manager for job {job_id}: {e}")
    
    def _poll_job_status(self, session: Session, job: Job) -> None:
        """Poll status for a single job.
        
        Args:
            session: Database session
            job: Job to poll
        """
        # Get worker connection
        worker = job.worker
        if not worker:
            worker = session.query(Worker).filter(
                Worker.worker_id == job.worker_id
            ).first()
            if not worker:
                logger.warning(f"Worker not found for job {job.job_id}")
                return
        
        # Get or create SSH connection
        worker_conn = self._get_worker_connection(worker)
        if not worker_conn:
            return
        
        ssh_client = worker_conn.ssh_client
        
        # Check sentinel files for terminal states (done/failed) first
        status_info = self._check_sentinel_files(ssh_client, job.remote_workdir)
        
        if status_info and status_info.status in ["completed", "failed"]:
            # Terminal state from sentinel file - trust it
            new_status = status_info.status
        else:
            # For non-terminal states, verify with squeue
            squeue_status = self._check_squeue(ssh_client, job.slurm_job_id)
            
            if squeue_status:
                # Job is in squeue - use squeue status
                new_status = squeue_status
                # If sentinel says running and squeue confirms, use sentinel info
                if status_info and status_info.status == "running" and squeue_status == "running":
                    pass  # Keep status_info for start_time etc.
                else:
                    status_info = None  # Don't use sentinel info
            else:
                # Job not in squeue - it has terminated
                old_status = job.job_status
                if old_status in ["running", "pending"]:
                    # Job disappeared from queue without terminal sentinel - mark as failed
                    logger.warning(
                        f"Job {job.job_id} (slurm_id={job.slurm_job_id}) disappeared from queue "
                        f"without terminal sentinel file, marking as failed"
                    )
                    new_status = "failed"
                    status_info = JobStatusInfo(
                        status="failed",
                        end_time=datetime.now(),
                        error_message=f"Job terminated unexpectedly (not in squeue, no terminal sentinel). "
                                      f"The job may have been killed, timed out, or crashed."
                    )
                else:
                    # For other statuses, don't change anything
                    return
        
        if not new_status:
            return
        
        # Update if status changed
        old_status = job.job_status
        if new_status != old_status:
            job.job_status = new_status
            job.last_sync_time = datetime.now()
            
            if status_info:
                # For start_time and end_time, always use local time to ensure consistent timeline
                # Remote cluster time may not be synchronized with local machine
                if new_status == "running" and not job.start_time:
                    job.start_time = datetime.now()
                if new_status in ["completed", "failed", "cancelled"] and not job.end_time:
                    job.end_time = datetime.now()
                # Use exit_code and error_message from sentinel file
                if status_info.exit_code is not None:
                    job.exit_code = status_info.exit_code
                if status_info.error_message:
                    job.error_message = status_info.error_message
            elif new_status == "running" and not job.start_time:
                job.start_time = datetime.now()
            elif new_status in ["completed", "failed", "cancelled"] and not job.end_time:
                job.end_time = datetime.now()
            
            session.commit()
            logger.info(f"Job {job.job_id} status: {old_status} -> {new_status}")
            
            # Handle file sync based on status changes
            self._handle_file_sync_on_status_change(session, job, old_status, new_status)
            
            # Call callbacks
            for callback in self._status_callbacks:
                try:
                    callback(job.job_id, new_status, status_info)
                except Exception as e:
                    logger.error(f"Error in status callback: {e}")
    
    def _get_worker_connection(self, worker: Worker) -> Optional[WorkerConnection]:
        """Get or create cached worker connection."""
        if worker.worker_id in self._worker_connections:
            return self._worker_connections[worker.worker_id]
        
        try:
            conn = WorkerConnection(worker)
            # Test connection
            conn.ssh_client.connect()
            self._worker_connections[worker.worker_id] = conn
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to worker {worker.worker_id}: {e}")
            return None
    
    def _check_sentinel_files(self, ssh_client: SSHClient,
                               remote_workdir: str) -> Optional[JobStatusInfo]:
        """Check sentinel files for job status.
        
        Args:
            ssh_client: SSH client connected to worker
            remote_workdir: Remote working directory
            
        Returns:
            JobStatusInfo or None if no sentinel files found
        """
        import os
        
        done_file = os.path.join(remote_workdir, ".jsling_status_done")
        failed_file = os.path.join(remote_workdir, ".jsling_status_failed")
        running_file = os.path.join(remote_workdir, ".jsling_status_running")
        
        try:
            # Check done file
            result = ssh_client.run(f"cat {done_file} 2>/dev/null", hide=True, warn=True)
            if result.ok and result.stdout.strip():
                info = self._parse_sentinel_file(result.stdout, "completed")
                return info
            
            # Check failed file
            result = ssh_client.run(f"cat {failed_file} 2>/dev/null", hide=True, warn=True)
            if result.ok and result.stdout.strip():
                info = self._parse_sentinel_file(result.stdout, "failed")
                return info
            
            # Check running file
            result = ssh_client.run(f"test -f {running_file} && echo exists", hide=True, warn=True)
            if result.ok and "exists" in result.stdout:
                # Read running file for start time
                result = ssh_client.run(f"cat {running_file} 2>/dev/null", hide=True, warn=True)
                if result.ok:
                    info = self._parse_sentinel_file(result.stdout, "running")
                    return info
                return JobStatusInfo(status="running")
            
        except Exception as e:
            logger.debug(f"Error checking sentinel files: {e}")
        
        return None
    
    def _parse_sentinel_file(self, content: str, status: str) -> JobStatusInfo:
        """Parse sentinel file content.
        
        Expected format:
            START_TIME=2024-01-15T10:30:45+00:00
            END_TIME=2024-01-15T10:35:22+00:00
            EXIT_CODE=0
            ERROR_MESSAGE=Some error
        """
        info = JobStatusInfo(status=status)
        
        for line in content.strip().split('\n'):
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                if key == "START_TIME":
                    try:
                        info.start_time = datetime.fromisoformat(value.replace('+00:00', ''))
                    except:
                        pass
                elif key == "END_TIME":
                    try:
                        info.end_time = datetime.fromisoformat(value.replace('+00:00', ''))
                    except:
                        pass
                elif key == "EXIT_CODE":
                    try:
                        info.exit_code = int(value)
                    except:
                        pass
                elif key == "ERROR_MESSAGE":
                    info.error_message = value
        
        return info
    
    def _check_squeue(self, ssh_client: SSHClient, slurm_job_id: str) -> Optional[str]:
        """Check job status via squeue.
        
        Args:
            ssh_client: SSH client
            slurm_job_id: Slurm job ID
            
        Returns:
            Status string or None
        """
        try:
            cmd = f"squeue -j {slurm_job_id} -h -o %T 2>/dev/null"
            result = ssh_client.run(cmd, hide=True, warn=True)
            
            if not result.ok or not result.stdout.strip():
                # Job not in queue - might be completed
                return None
            
            slurm_status = result.stdout.strip().upper()
            
            if "RUNNING" in slurm_status:
                return "running"
            elif "PENDING" in slurm_status:
                return "pending"
            elif "COMPLETING" in slurm_status:
                return "running"
            elif "COMPLETED" in slurm_status:
                return "completed"
            elif "FAILED" in slurm_status:
                return "failed"
            elif "CANCELLED" in slurm_status:
                return "cancelled"
            
        except Exception as e:
            logger.debug(f"Error checking squeue: {e}")
        
        return None
    
    def _handle_file_sync_on_status_change(self, session: Session, job: Job, 
                                            old_status: str, new_status: str) -> None:
        """Handle file synchronization based on job status changes.
        
        Args:
            session: Database session
            job: Job object
            old_status: Previous job status
            new_status: New job status
        """
        job_id = job.job_id
        
        # Start file sync when job starts running
        if new_status == "running" and old_status != "running":
            if job_id not in self._file_sync_managers:
                try:
                    logger.info(f"Starting file sync for job {job_id}")
                    sync_mgr = FileSyncManager(session, job)
                    sync_mgr.start_sync()
                    self._file_sync_managers[job_id] = sync_mgr
                except Exception as e:
                    logger.error(f"Failed to start file sync for job {job_id}: {e}")
        
        # Execute final sync and stop when job completes
        elif new_status in ["completed", "failed", "cancelled"] and job_id in self._file_sync_managers:
            try:
                logger.info(f"Executing final sync for job {job_id}")
                sync_mgr = self._file_sync_managers[job_id]
                sync_mgr.final_sync()
                sync_mgr.stop_sync()
                del self._file_sync_managers[job_id]
            except Exception as e:
                logger.error(f"Failed to execute final sync for job {job_id}: {e}")
    
    def poll_job_now(self, job_id: str) -> Optional[str]:
        """Immediately poll status for a specific job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Current status or None
        """
        try:
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                job = session.query(Job).filter(Job.job_id == job_id).first()
                if not job:
                    return None
                
                self._poll_job_status(session, job)
                return job.job_status
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error polling job {job_id}: {e}")
            return None
    
    def __enter__(self) -> 'StatusPoller':
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()
    
    # ========== Worker Status Polling Methods ==========
    
    def _worker_poll_loop(self) -> None:
        """Main polling loop for worker status."""
        # Initial delay
        for _ in range(self.initial_delay):
            if not self._running:
                return
            time.sleep(1)
        
        while self._running:
            try:
                self._poll_all_workers()
            except Exception as e:
                logger.error(f"Error in worker poll loop: {e}")
            
            # Wait for next poll interval
            for _ in range(self.worker_poll_interval):
                if not self._running:
                    break
                time.sleep(1)
    
    def _poll_all_workers(self) -> None:
        """Poll status for all active workers."""
        try:
            # Get database session
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                # Find all active workers
                workers = session.query(Worker).filter(
                    Worker.is_active == True
                ).all()
                
                if not workers:
                    return
                
                logger.debug(f"Polling status for {len(workers)} active workers")
                
                for worker in workers:
                    try:
                        self._poll_worker_status(session, worker)
                    except Exception as e:
                        logger.error(f"Error polling worker {worker.worker_id}: {e}")
                        # Update error status in database
                        self._update_worker_error(session, worker.worker_id, str(e))
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error in _poll_all_workers: {e}")
    
    def _poll_worker_status(self, session: Session, worker: Worker) -> None:
        """Poll status for a single worker.
        
        Args:
            session: Database session
            worker: Worker to poll
        """
        # Get or create SSH connection (reuse from job polling)
        worker_conn = self._get_worker_connection(worker)
        if not worker_conn:
            self._update_worker_error(session, worker.worker_id, "Failed to connect")
            return
        
        ssh_client = worker_conn.ssh_client
        queue_name = worker.queue_name
        
        # Query sinfo for comprehensive queue status
        # Format: %a=availability, %C=cpu_states, %D=nodes, %F=node_states, %G=gres, %b=gres_used
        # %F format: alloc/idle/other/total (nodes by state)
        # %G format: gres_name:count (e.g., gpu:8)
        # %b format: gres_name:count (e.g., gpu:2 for 2 GPUs in use)
        cmd = f"sinfo -p {queue_name} -h -o '%a|%C|%D|%F|%G|%b'"
        
        try:
            result = ssh_client.run(cmd, hide=True, warn=True)
            
            if not result.ok or not result.stdout.strip():
                self._update_worker_error(
                    session, worker.worker_id, 
                    f"sinfo failed: {result.stderr[:100] if result.stderr else 'no output'}"
                )
                return
            
            # Parse the output
            output = result.stdout.strip()
            parts = output.split('|')
            
            if len(parts) < 4:
                self._update_worker_error(
                    session, worker.worker_id,
                    f"Unexpected sinfo output format: {output[:50]}"
                )
                return
            
            queue_state = parts[0].strip()  # up/down
            cpu_usage = parts[1].strip()    # alloc/idle/other/total
            nodes_total_str = parts[2].strip()  # total node count
            node_states = parts[3].strip()  # alloc/idle/other/total (node counts)
            gres_config = parts[4].strip() if len(parts) > 4 else None  # gpu:8 (total)
            gres_used = parts[5].strip() if len(parts) > 5 else None  # gpu:2 (used)
            
            # Parse CPU usage: alloc/idle/other/total
            cpu_alloc, cpu_idle, cpu_other, cpu_total = None, None, None, None
            try:
                cpu_parts = cpu_usage.split('/')
                if len(cpu_parts) == 4:
                    cpu_alloc = int(cpu_parts[0])
                    cpu_idle = int(cpu_parts[1])
                    cpu_other = int(cpu_parts[2])
                    cpu_total = int(cpu_parts[3])
            except ValueError:
                logger.warning(f"Failed to parse CPU usage: {cpu_usage}")
            
            # Parse node counts
            nodes_total = None
            try:
                nodes_total = int(nodes_total_str)
            except ValueError:
                logger.warning(f"Failed to parse total nodes: {nodes_total_str}")
            
            # Parse node states: alloc/idle/other/total
            nodes_available = None
            try:
                node_parts = node_states.split('/')
                if len(node_parts) == 4:
                    # idle nodes are "available"
                    nodes_idle = int(node_parts[1])
                    nodes_available = nodes_idle
            except ValueError:
                logger.warning(f"Failed to parse node states: {node_states}")
            
            # Parse GPU GRES info
            # %G (gres_config) format: gpu:8 or gpu:tesla:2 or (null)
            # %b (gres_used) format: gpu:2 or gpu:tesla:1 or (null)
            gpu_alloc, gpu_total = None, None
            
            # Parse total GPU from %G (GRES config)
            if gres_config and gres_config not in ('(null)', 'N/A', ''):
                try:
                    # Format: gpu:8 or gpu:tesla:2
                    if 'gpu' in gres_config.lower():
                        # Split by comma for multiple GRES types
                        for gres_item in gres_config.split(','):
                            if 'gpu' in gres_item.lower():
                                # Extract count from gpu:8 or gpu:tesla:2
                                parts_g = gres_item.strip().split(':')
                                # Last part should be the count
                                gpu_total = int(parts_g[-1])
                                break
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse GPU GRES config: {gres_config}, error: {e}")
            
            # Parse used GPU from %b (GRES used)
            if gres_used and gres_used not in ('(null)', 'N/A', ''):
                try:
                    if 'gpu' in gres_used.lower():
                        for gres_item in gres_used.split(','):
                            if 'gpu' in gres_item.lower():
                                parts_u = gres_item.strip().split(':')
                                gpu_alloc = int(parts_u[-1])
                                break
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse GPU GRES used: {gres_used}, error: {e}")
            
            # If we have total but no used info, assume 0 used
            if gpu_total is not None and gpu_alloc is None:
                gpu_alloc = 0
            
            # Update or create WorkerLiveStatus
            self._update_worker_status(
                session, worker.worker_id,
                queue_state=queue_state,
                cpu_alloc=cpu_alloc,
                cpu_idle=cpu_idle,
                cpu_other=cpu_other,
                cpu_total=cpu_total,
                gpu_alloc=gpu_alloc,
                gpu_total=gpu_total,
                nodes_available=nodes_available,
                nodes_total=nodes_total
            )
            
            logger.debug(
                f"Worker {worker.worker_id} status: queue={queue_state}, "
                f"nodes={nodes_available}/{nodes_total}, "
                f"cpu={cpu_alloc}/{cpu_total}, gpu={gpu_alloc}/{gpu_total}"
            )
            
        except Exception as e:
            logger.error(f"Error querying sinfo for worker {worker.worker_id}: {e}")
            self._update_worker_error(session, worker.worker_id, str(e)[:200])
    
    def _update_worker_status(
        self, session: Session, worker_id: str,
        queue_state: Optional[str] = None,
        cpu_alloc: Optional[int] = None,
        cpu_idle: Optional[int] = None,
        cpu_other: Optional[int] = None,
        cpu_total: Optional[int] = None,
        gpu_alloc: Optional[int] = None,
        gpu_total: Optional[int] = None,
        nodes_available: Optional[int] = None,
        nodes_total: Optional[int] = None
    ) -> None:
        """Update worker live status in database.
        
        Args:
            session: Database session
            worker_id: Worker identifier
            queue_state: Queue availability state
            cpu_*: CPU slot statistics
            gpu_*: GPU slot statistics
            nodes_*: Node statistics
        """
        # Get or create status record
        status = session.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == worker_id
        ).first()
        
        if not status:
            status = WorkerLiveStatus(worker_id=worker_id)
            session.add(status)
        
        # Update fields
        status.queue_state = queue_state
        status.cpu_alloc = cpu_alloc
        status.cpu_idle = cpu_idle
        status.cpu_other = cpu_other
        status.cpu_total = cpu_total
        status.gpu_alloc = gpu_alloc
        status.gpu_total = gpu_total
        status.nodes_available = nodes_available
        status.nodes_total = nodes_total
        status.error_message = None  # Clear any previous error
        status.last_updated = datetime.now()
        
        session.commit()
    
    def _update_worker_error(self, session: Session, worker_id: str, error: str) -> None:
        """Update worker status with error message.
        
        Args:
            session: Database session
            worker_id: Worker identifier
            error: Error message
        """
        status = session.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == worker_id
        ).first()
        
        if not status:
            status = WorkerLiveStatus(worker_id=worker_id)
            session.add(status)
        
        status.error_message = error[:500]  # Truncate long errors
        status.last_updated = datetime.now()
        
        session.commit()
        logger.warning(f"Worker {worker_id} status error: {error[:100]}")
    
    def poll_worker_now(self, worker_id: str) -> Optional[WorkerLiveStatus]:
        """Immediately poll status for a specific worker.
        
        Args:
            worker_id: Worker identifier
            
        Returns:
            Updated WorkerLiveStatus or None
        """
        try:
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                worker = session.query(Worker).filter(
                    Worker.worker_id == worker_id
                ).first()
                
                if not worker:
                    return None
                
                self._poll_worker_status(session, worker)
                
                return session.query(WorkerLiveStatus).filter(
                    WorkerLiveStatus.worker_id == worker_id
                ).first()
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error polling worker {worker_id}: {e}")
            return None


# Global singleton instance
_global_poller: Optional[StatusPoller] = None


def get_status_poller(session: Optional[Session] = None, 
                      poll_interval: Optional[int] = None) -> StatusPoller:
    """Get or create global status poller instance.
    
    Args:
        session: Database session
        poll_interval: Optional poll interval override
        
    Returns:
        StatusPoller instance
    """
    global _global_poller
    
    if _global_poller is None:
        _global_poller = StatusPoller(session=session, poll_interval=poll_interval)
    
    return _global_poller
