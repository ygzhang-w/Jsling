"""
Status Worker - Background job submission and cancellation processor.

This module provides asynchronous job submission and cancellation capability,
enabling high-throughput operations without blocking the CLI.

Jobs are queued locally and processed by the daemon:
- Submission: queued -> pending (via sbatch)
- Cancellation: cancelling -> cancelled (via scancel)
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from sqlalchemy.orm import Session

from jsling.connections.worker import Worker as WorkerConnection
from jsling.core.config_manager import ConfigManager
from jsling.core.job_wrapper import JobWrapper
from jsling.database.models import Job, Worker
from jsling.database.session import get_db

logger = logging.getLogger(__name__)


class StatusWorker:
    """
    Background worker for processing job submission and cancellation queues.
    
    Features:
    - Polls database for 'queued' jobs (submission) and 'cancelling' jobs (cancel)
    - Executes remote operations (SSH + rsync + sbatch/scancel)
    - Reuses SSH connection pool from StatusPoller
    - Handles failures with retry logic
    - Configurable concurrency and retry settings
    """
    
    def __init__(
        self,
        session: Optional[Session] = None,
        worker_connections: Optional[Dict[str, WorkerConnection]] = None,
        poll_interval: Optional[int] = None,
        max_concurrent: Optional[int] = None
    ):
        """
        Initialize status worker.
        
        Args:
            session: Database session (optional, creates new if not provided)
            worker_connections: Shared worker connection pool from StatusPoller
            poll_interval: Override polling interval (seconds)
            max_concurrent: Override max concurrent submissions
            
        Raises:
            KeyError: If required configuration keys are missing from database
        """
        self.session = session
        self._worker_connections = worker_connections or {}
        
        # Load configuration from database
        temp_session = session if session else get_db()
        try:
            config_mgr = ConfigManager(temp_session)
            db_poll_interval = config_mgr.get_typed("submission_poll_interval")
            db_max_concurrent = config_mgr.get_typed("submission_max_concurrent")
            self.retry_count = config_mgr.get_typed("submission_retry_count")
            self.retry_delay = config_mgr.get_typed("submission_retry_delay")
        except KeyError as e:
            raise KeyError(
                f"Missing configuration in database: {e}. "
                "Please run 'jsling init' to initialize the database."
            )
        finally:
            if not session:
                temp_session.close()
        
        # Use overrides if provided
        self.poll_interval = poll_interval if poll_interval is not None else db_poll_interval
        self.max_concurrent = max_concurrent if max_concurrent is not None else db_max_concurrent
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
        # Track active submissions
        self._active_submissions: Dict[str, threading.Thread] = {}
        
        logger.info(
            f"StatusWorker initialized with poll_interval={self.poll_interval}s, "
            f"max_concurrent={self.max_concurrent}"
        )
    
    def set_worker_connections(self, connections: Dict[str, WorkerConnection]) -> None:
        """Set shared worker connection pool.
        
        Args:
            connections: Worker connection pool from StatusPoller
        """
        self._worker_connections = connections
    
    def start(self) -> bool:
        """Start background submission processing.
        
        Returns:
            True if started successfully
        """
        with self._lock:
            if self._running:
                logger.warning("Status worker already running")
                return True
            
            self._running = True
            
            self._thread = threading.Thread(
                target=self._worker_loop,
                daemon=True,
                name="StatusWorker"
            )
            self._thread.start()
            
            logger.info("Status worker started")
        
        return True
    
    def stop(self) -> None:
        """Stop status worker."""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
        
        # Wait for main thread to finish
        if self._thread:
            self._thread.join(timeout=self.poll_interval + 5)
            self._thread = None
        
        # Wait for active submissions to complete
        for job_id, thread in list(self._active_submissions.items()):
            thread.join(timeout=30)
        self._active_submissions.clear()
        
        logger.info("Status worker stopped")
    
    def is_running(self) -> bool:
        """Check if status worker is running."""
        return self._running
    
    def _worker_loop(self) -> None:
        """Main loop for processing queued and cancellation-requested jobs."""
        # Initial delay to let system stabilize
        time.sleep(2)
        
        while self._running:
            try:
                self._process_queued_jobs()
                self._process_cancellation_requests()
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
            
            # Wait for next poll interval
            for _ in range(self.poll_interval):
                if not self._running:
                    break
                time.sleep(1)
    
    def _process_queued_jobs(self) -> None:
        """Process all jobs in queued state."""
        try:
            # Get database session
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                # Clean up completed submission threads
                self._cleanup_completed_threads()
                
                # Check concurrent limit
                active_count = len(self._active_submissions)
                if active_count >= self.max_concurrent:
                    logger.debug(f"Max concurrent submissions reached ({active_count})")
                    return
                
                # Find queued jobs (oldest first)
                available_slots = self.max_concurrent - active_count
                queued_jobs = session.query(Job).filter(
                    Job.job_status == "queued"
                ).order_by(Job.submit_time).limit(available_slots).all()
                
                if not queued_jobs:
                    return
                
                logger.debug(f"Found {len(queued_jobs)} queued jobs to process")
                
                for job in queued_jobs:
                    if job.job_id not in self._active_submissions:
                        # Start submission in separate thread
                        thread = threading.Thread(
                            target=self._submit_job_thread,
                            args=(job.job_id,),
                            daemon=True,
                            name=f"Submit-{job.job_id}"
                        )
                        self._active_submissions[job.job_id] = thread
                        thread.start()
                        logger.info(f"Started submission for job {job.job_id}")
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error in _process_queued_jobs: {e}")
    
    def _cleanup_completed_threads(self) -> None:
        """Remove completed submission threads from tracking."""
        completed = [
            job_id for job_id, thread in self._active_submissions.items()
            if not thread.is_alive()
        ]
        for job_id in completed:
            del self._active_submissions[job_id]
    
    def _submit_job_thread(self, job_id: str) -> None:
        """Thread function for submitting a single job.
        
        Args:
            job_id: Job identifier to submit
        """
        session = get_db()
        try:
            job = session.query(Job).filter(Job.job_id == job_id).first()
            if not job or job.job_status != "queued":
                logger.warning(f"Job {job_id} not found or not in queued state")
                return
            
            # Execute submission with retry logic
            success = self._submit_single_job(session, job)
            
            if not success:
                # Mark as submission_failed after all retries exhausted
                job.job_status = "submission_failed"
                job.end_time = datetime.now()
                session.commit()
                logger.error(f"Job {job_id} submission failed after retries")
            
        except Exception as e:
            logger.error(f"Error submitting job {job_id}: {e}")
            try:
                job = session.query(Job).filter(Job.job_id == job_id).first()
                if job:
                    job.job_status = "submission_failed"
                    job.error_message = str(e)[:500]
                    job.end_time = datetime.now()
                    session.commit()
            except Exception:
                pass
        finally:
            session.close()
    
    def _submit_single_job(self, session: Session, job: Job) -> bool:
        """Execute actual remote submission for one job.
        
        Args:
            session: Database session
            job: Job to submit
            
        Returns:
            True if submission successful
        """
        # Parse submission params
        if not job.submission_params:
            logger.error(f"Job {job.job_id} has no submission_params")
            job.error_message = "Missing submission parameters"
            return False
        
        try:
            params = json.loads(job.submission_params)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid submission_params JSON for job {job.job_id}: {e}")
            job.error_message = f"Invalid submission parameters: {e}"
            return False
        
        # Get worker
        worker = session.query(Worker).filter(
            Worker.worker_id == job.worker_id
        ).first()
        
        if not worker:
            job.error_message = f"Worker not found: {job.worker_id}"
            return False
        
        if not worker.is_active:
            job.error_message = f"Worker is not active: {worker.worker_name}"
            return False
        
        # Retry logic
        last_error = None
        for attempt in range(self.retry_count):
            try:
                result = self._execute_submission(session, job, worker, params)
                if result:
                    return True
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Submission attempt {attempt + 1}/{self.retry_count} failed "
                    f"for job {job.job_id}: {e}"
                )
                if attempt < self.retry_count - 1:
                    time.sleep(self.retry_delay)
        
        job.error_message = f"Submission failed after {self.retry_count} attempts: {last_error}"
        return False
    
    def _execute_submission(
        self,
        session: Session,
        job: Job,
        worker: Worker,
        params: Dict[str, Any]
    ) -> bool:
        """Execute the actual submission to remote cluster.
        
        Args:
            session: Database session
            job: Job record
            worker: Worker configuration
            params: Submission parameters
            
        Returns:
            True if submission successful
        """
        # Get or create worker connection
        worker_conn = self._get_worker_connection(worker)
        if not worker_conn:
            raise ConnectionError(f"Failed to connect to worker {worker.worker_id}")
        
        ssh_client = worker_conn.ssh_client
        
        # Extract parameters
        command = params.get("command", "")
        remote_workdir = job.remote_workdir
        ntasks_per_node = params.get("ntasks_per_node")
        gres = params.get("gres")
        gpus_per_task = params.get("gpus_per_task")
        upload_files = params.get("upload_files", [])
        
        # Create JobWrapper for script generation
        wrapper = JobWrapper(
            job_id=job.job_id,
            worker=worker,
            command=command,
            remote_workdir=remote_workdir,
            ntasks_per_node=ntasks_per_node,
            gres=gres,
            gpus_per_task=gpus_per_task
        )
        wrapped_content = wrapper.wrap_script()
        
        # Ensure remote workdir exists
        ssh_client.run(f"mkdir -p {remote_workdir}", hide=True, warn=True)
        
        # Use a temporary staging directory for initial upload
        staging_dir = Path(tempfile.mkdtemp(prefix=f"jsling_upload_{job.job_id}_"))
        try:
            # Write wrapped script to staging directory
            script_path = staging_dir / "job.sh"
            script_path.write_text(wrapped_content)
            script_path.chmod(0o755)
            
            # Stage additional upload files
            if upload_files:
                self._prepare_upload_files(staging_dir, upload_files)
            
            # Upload staging directory to remote using rsync
            if not self._rsync_upload(worker, staging_dir, remote_workdir):
                raise RuntimeError("Failed to upload files via rsync")
            
        finally:
            try:
                shutil.rmtree(staging_dir)
            except Exception:
                pass
        
        # Submit job via sbatch
        remote_script = os.path.join(remote_workdir, "job.sh")
        sbatch_cmd = f"cd {remote_workdir} && sbatch {remote_script}"
        result = ssh_client.run(sbatch_cmd, hide=True, warn=True)
        
        if not result.ok:
            raise RuntimeError(f"sbatch failed: {result.stderr}")
        
        # Parse Slurm job ID
        slurm_job_id = self._parse_slurm_job_id(result.stdout)
        if not slurm_job_id:
            raise RuntimeError(f"Failed to parse Slurm job ID from: {result.stdout}")
        
        # Update job record
        job.slurm_job_id = slurm_job_id
        job.job_status = "pending"
        job.submission_params = None  # Clear params after successful submission
        session.commit()
        
        logger.info(f"Job {job.job_id} submitted successfully, slurm_id={slurm_job_id}")
        return True
    
    def _get_worker_connection(self, worker: Worker) -> Optional[WorkerConnection]:
        """Get or create worker connection.
        
        Args:
            worker: Worker configuration
            
        Returns:
            WorkerConnection or None if connection fails
        """
        # Try to use shared connection pool first
        if worker.worker_id in self._worker_connections:
            return self._worker_connections[worker.worker_id]
        
        # Create new connection
        try:
            conn = WorkerConnection(worker)
            conn.ssh_client.connect()
            # Cache for future use
            self._worker_connections[worker.worker_id] = conn
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to worker {worker.worker_id}: {e}")
            return None
    
    def _prepare_upload_files(
        self,
        staging_dir: Path,
        upload_files: List[str]
    ) -> None:
        """Prepare upload files in staging directory.
        
        Args:
            staging_dir: Temporary staging directory
            upload_files: List of file/directory paths to upload
        """
        for path in upload_files:
            src = Path(path).expanduser()
            if not src.exists():
                logger.warning(f"Upload file not found: {path}")
                continue
            
            dest = staging_dir / src.name
            try:
                if src.is_file():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dest)
                elif src.is_dir():
                    if dest.exists():
                        shutil.copytree(src, dest, dirs_exist_ok=True)
                    else:
                        shutil.copytree(src, dest)
            except Exception as e:
                logger.warning(f"Failed to stage upload file {path}: {e}")
    
    def _rsync_upload(
        self,
        worker: Worker,
        staging_dir: Path,
        remote_workdir: str
    ) -> bool:
        """Upload staged files to remote using rsync.
        
        Args:
            worker: Worker configuration
            staging_dir: Local staging directory
            remote_workdir: Remote working directory
            
        Returns:
            True if rsync succeeded
        """
        from jsling.utils.encryption import decrypt_credential
        
        local_path = f"{str(staging_dir)}/"
        
        # Handle IPv6 addresses - they need to be wrapped in brackets for rsync
        host = worker.host
        if ':' in host:  # IPv6 address
            host = f"[{host}]"
        remote_path = f"{worker.username}@{host}:{remote_workdir}/"
        
        # Build SSH command
        ssh_parts = ["ssh"]
        if worker.port and worker.port != 22:
            ssh_parts.extend(["-p", str(worker.port)])
        
        if worker.auth_method == "key":
            credential = decrypt_credential(worker.auth_credential)
            key_path = os.path.expanduser(credential)
            if os.path.exists(key_path):
                ssh_parts.extend(["-i", key_path])
        
        ssh_parts.extend(["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"])
        ssh_cmd = " ".join(ssh_parts)
        
        rsync_cmd = [
            "rsync",
            "-avz",
            "-e", ssh_cmd,
            "--progress",
            local_path,
            remote_path,
        ]
        
        env = os.environ.copy()
        for proxy_var in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"]:
            env.pop(proxy_var, None)
        
        try:
            result = subprocess.run(
                rsync_cmd,
                capture_output=True,
                text=True,
                timeout=300,
                env=env,
            )
        except subprocess.TimeoutExpired:
            logger.error("Rsync timeout (5 minutes)")
            return False
        except Exception as e:
            logger.error(f"Rsync error: {e}")
            return False
        
        if result.returncode != 0:
            logger.error(f"Rsync failed: {result.stderr}")
            return False
        
        return True
    
    def _parse_slurm_job_id(self, sbatch_output: str) -> Optional[str]:
        """Parse Slurm job ID from sbatch output.
        
        Args:
            sbatch_output: Output from sbatch command
            
        Returns:
            Slurm job ID or None
        """
        import re
        match = re.search(r'Submitted batch job (\d+)', sbatch_output)
        if match:
            return match.group(1)
        match = re.search(r'(\d+)', sbatch_output)
        if match:
            return match.group(1)
        return None
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status for both submissions and cancellations.
        
        Returns:
            Dictionary with queue statistics
        """
        session = get_db() if self.session is None else self.session
        try:
            queued_count = session.query(Job).filter(
                Job.job_status == "queued"
            ).count()
            
            cancelling_count = session.query(Job).filter(
                Job.job_status == "cancelling"
            ).count()
            
            failed_count = session.query(Job).filter(
                Job.job_status == "submission_failed"
            ).count()
            
            return {
                "queued": queued_count,
                "cancelling": cancelling_count,
                "active_submissions": len(self._active_submissions),
                "submission_failed": failed_count,
                "max_concurrent": self.max_concurrent,
                "running": self._running
            }
        finally:
            if self.session is None:
                session.close()
    
    def _process_cancellation_requests(self) -> None:
        """Process all jobs in cancelling state."""
        try:
            # Get database session
            if self.session is not None:
                session = self.session
                should_close = False
            else:
                session = get_db()
                should_close = True
            
            try:
                # Find jobs requesting cancellation
                jobs_to_cancel = session.query(Job).filter(
                    Job.job_status == "cancelling"
                ).order_by(Job.submit_time).all()
                
                if not jobs_to_cancel:
                    return
                
                logger.debug(f"Found {len(jobs_to_cancel)} jobs to cancel")
                
                for job in jobs_to_cancel:
                    try:
                        success = self._cancel_single_job(session, job)
                        if success:
                            logger.info(f"Job {job.job_id} cancelled successfully")
                        else:
                            logger.warning(f"Failed to cancel job {job.job_id}")
                    except Exception as e:
                        logger.error(f"Error cancelling job {job.job_id}: {e}")
                        # Mark as cancelled anyway since we can't reach the cluster
                        job.job_status = "cancelled"
                        job.end_time = datetime.now()
                        job.error_message = f"Cancellation error: {str(e)[:400]}"
                        session.commit()
                
            finally:
                if should_close:
                    session.close()
                    
        except Exception as e:
            logger.error(f"Error in _process_cancellation_requests: {e}")
    
    def _cancel_single_job(self, session: Session, job: Job) -> bool:
        """Execute scancel for a single job.
        
        Args:
            session: Database session
            job: Job to cancel
            
        Returns:
            True if cancellation successful
        """
        # Get worker
        worker = session.query(Worker).filter(
            Worker.worker_id == job.worker_id
        ).first()
        
        if not worker:
            job.job_status = "cancelled"
            job.end_time = datetime.now()
            job.error_message = f"Worker not found: {job.worker_id}"
            session.commit()
            return False
        
        if not worker.is_active:
            job.job_status = "cancelled"
            job.end_time = datetime.now()
            job.error_message = f"Worker is not active: {worker.worker_name}"
            session.commit()
            return False
        
        # Get or create worker connection
        worker_conn = self._get_worker_connection(worker)
        if not worker_conn:
            job.job_status = "cancelled"
            job.end_time = datetime.now()
            job.error_message = f"Failed to connect to worker {worker.worker_id}"
            session.commit()
            return False
        
        # Execute scancel
        try:
            if job.slurm_job_id:
                result = worker_conn.ssh_client.run(
                    f"scancel {job.slurm_job_id}", 
                    hide=True, 
                    warn=True
                )
                
                if result.ok:
                    job.job_status = "cancelled"
                    job.end_time = datetime.now()
                    session.commit()
                    return True
                else:
                    # scancel failed - job might have already finished
                    logger.warning(f"scancel failed for job {job.job_id}: {result.stderr}")
                    job.job_status = "cancelled"
                    job.end_time = datetime.now()
                    job.error_message = f"scancel failed: {result.stderr[:200] if result.stderr else 'unknown error'}"
                    session.commit()
                    return False
            else:
                # No slurm_job_id - job was never actually submitted
                job.job_status = "cancelled"
                job.end_time = datetime.now()
                session.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error executing scancel for job {job.job_id}: {e}")
            job.job_status = "cancelled"
            job.end_time = datetime.now()
            job.error_message = f"scancel error: {str(e)[:400]}"
            session.commit()
            return False

