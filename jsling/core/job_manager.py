"""Job manager - handles job lifecycle management.

Job status synchronization is managed by RunnerDaemon's StatusPoller,
which runs as a persistent background process.
"""

import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session

from jsling.connections.ssh_client import SSHClient
from jsling.connections.worker import Worker as WorkerConnection
from jsling.database.models import Job, Worker, Config
from jsling.database.config import JSLING_HOME


class JobManager:
    """Manages job submission, tracking, and status queries.
    
    Note: Job status synchronization is handled by RunnerDaemon's StatusPoller.
    This class focuses on job submission, querying, and cancellation.
    Make sure RunnerDaemon is running for automatic status updates.
    """
    
    DEFAULT_POLL_INTERVAL = 10
    
    def __init__(self, session: Session):
        """Initialize job manager.
        
        Args:
            session: Database session
        """
        self.session = session
        self._poll_interval = self._get_config_int("status_poll_interval", self.DEFAULT_POLL_INTERVAL)
    
    def _get_config(self, key: str, default: str) -> str:
        """Get configuration value from database."""
        config = self.session.query(Config).filter(Config.config_key == key).first()
        return config.config_value if config else default
    
    def _get_config_int(self, key: str, default: int) -> int:
        """Get integer configuration value from database."""
        value = self._get_config(key, str(default))
        try:
            return int(value)
        except ValueError:
            return default
    
    def _generate_job_id(self) -> str:
        """Generate unique job ID.
        
        Returns:
            Unique job identifier
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"
    
    def _create_local_workdir(self, job_id: str, base_dir: Optional[str] = None) -> Path:
        """Create local working directory for job.
        
        Args:
            job_id: Job identifier
            base_dir: Base directory (uses current directory directly if specified,
                     otherwise creates ~/.jsling/jobs/job_id)
            
        Returns:
            Path to local working directory
        """
        if base_dir:
            # Use specified directory directly (current working directory)
            workdir = Path(base_dir).expanduser().resolve()
        else:
            # Fallback to ~/.jsling/jobs/job_id if no base_dir specified
            workdir = JSLING_HOME / "jobs" / job_id
        
        workdir.mkdir(parents=True, exist_ok=True)
        return workdir
    
    def queue_job(
        self,
        worker_id: str,
        command: str,
        local_workdir: Optional[str] = None,
        ntasks_per_node: Optional[int] = None,
        gres: Optional[str] = None,
        gpus_per_task: Optional[int] = None,
        upload_files: Optional[List[str]] = None,
        rsync_mode: Optional[str] = None,
        rsync_interval: Optional[int] = None,
        sync_rules: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Queue a job for asynchronous submission by the daemon.
        
        This method creates a job record in 'queued' state and returns immediately.
        The actual submission (SSH, rsync, sbatch) is handled by SubmissionWorker
        running in the daemon process.
        
        Args:
            worker_id: Worker ID or name
            command: Command to execute on remote worker
            local_workdir: Local working directory (optional)
            ntasks_per_node: Tasks per node override (optional)
            gres: GRES resources override (optional)
            gpus_per_task: GPUs per task override (optional)
            upload_files: Additional files to upload (optional)
            rsync_mode: Rsync mode (periodic or final_only)
            rsync_interval: Rsync interval in seconds
            sync_rules: File synchronization rules
            
        Returns:
            Job ID if queued successfully, None otherwise
        """
        # Get worker to validate and get remote_workdir
        worker = self.session.query(Worker).filter(
            (Worker.worker_id == worker_id) | (Worker.worker_name == worker_id)
        ).first()
        
        if not worker:
            print(f"Worker not found: {worker_id}")
            return None
        
        if not worker.is_active:
            print(f"Worker is not active: {worker.worker_name}")
            return None
        
        # Get rsync_mode from database if not provided
        if rsync_mode is None:
            from jsling.core.config_manager import ConfigManager
            config_mgr = ConfigManager(self.session)
            rsync_mode = config_mgr.get_typed("default_rsync_mode")
        
        # Generate job ID
        job_id = self._generate_job_id()
        
        # Create local working directory
        local_dir = self._create_local_workdir(job_id, local_workdir)
        
        # Remote working directory
        remote_workdir = os.path.join(worker.remote_workdir, job_id)
        
        try:
            # Convert upload_files to absolute paths (daemon runs from /)
            absolute_upload_files = []
            if upload_files:
                for f in upload_files:
                    abs_path = str(Path(f).expanduser().resolve())
                    absolute_upload_files.append(abs_path)
            
            # Prepare submission parameters (to be used by SubmissionWorker)
            submission_params = {
                "command": command,
                "ntasks_per_node": ntasks_per_node,
                "gres": gres,
                "gpus_per_task": gpus_per_task,
                "upload_files": absolute_upload_files,
            }
            
            # Create job record in queued state
            job = Job(
                job_id=job_id,
                slurm_job_id=None,  # Will be set after actual submission
                worker_id=worker.worker_id,
                local_workdir=str(local_dir),
                remote_workdir=remote_workdir,
                script_path="",  # DEPRECATED
                command=command,
                job_status="queued",  # Queued for submission
                sync_mode="sentinel",
                rsync_mode=rsync_mode,
                rsync_interval=rsync_interval,
                sync_rules=json.dumps(sync_rules) if sync_rules else None,
                uploaded_files=json.dumps(absolute_upload_files) if absolute_upload_files else None,
                submission_params=json.dumps(submission_params),
                submit_time=datetime.now()
            )
            
            self.session.add(job)
            self.session.commit()
            
            return job_id
            
        except Exception as e:
            print(f"Error queuing job: {e}")
            self.session.rollback()
            return None
    
    
    def _parse_slurm_job_id(self, sbatch_output: str) -> Optional[str]:
        """Parse Slurm job ID from sbatch command output."""
        match = re.search(r'Submitted batch job (\d+)', sbatch_output)
        if match:
            return match.group(1)
        match = re.search(r'(\d+)', sbatch_output)
        if match:
            return match.group(1)
        return None
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return self.session.query(Job).filter(Job.job_id == job_id).first()
    
    def list_jobs(
        self,
        worker_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by: str = "submit_time"
    ) -> List[Job]:
        """List jobs with optional filtering."""
        query = self.session.query(Job)
        
        if worker_id:
            query = query.filter(Job.worker_id == worker_id)
        if status:
            query = query.filter(Job.job_status == status)
        
        if sort_by == "submit_time":
            query = query.order_by(Job.submit_time.desc())
        elif sort_by == "job_id":
            query = query.order_by(Job.job_id)
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def sync_job_status(self, job_id: str) -> Optional[Job]:
        """Manually sync single job status from remote.
        
        Note: For automatic status sync, use RunnerDaemon (jrunner start).
        This method is for manual/one-time status checks.
        """
        job = self.get_job(job_id)
        if not job:
            return None
        
        try:
            worker = self.session.query(Worker).filter(
                Worker.worker_id == job.worker_id
            ).first()
            
            if not worker:
                return None
            
            worker_conn = WorkerConnection(worker)
            ssh_client = worker_conn.ssh_client
            
            # Check sentinel files first
            status = self._check_sentinel_files(ssh_client, job.remote_workdir)
            
            if status:
                job.job_status = status
                if status in ["completed", "failed", "cancelled"]:
                    job.end_time = datetime.now()
                elif status == "running" and not job.start_time:
                    job.start_time = datetime.now()
            else:
                # Fall back to squeue
                status = self._check_squeue(ssh_client, job.slurm_job_id)
                if status:
                    job.job_status = status
                    if status == "running" and not job.start_time:
                        job.start_time = datetime.now()
            
            job.last_sync_time = datetime.now()
            self.session.commit()
            
            return job
            
        except Exception as e:
            print(f"Error syncing job status: {e}")
            return None
    
    def _check_sentinel_files(self, ssh_client: SSHClient, remote_workdir: str) -> Optional[str]:
        """Check sentinel files for job status."""
        done_file = os.path.join(remote_workdir, ".jsling_status_done")
        failed_file = os.path.join(remote_workdir, ".jsling_status_failed")
        running_file = os.path.join(remote_workdir, ".jsling_status_running")
        
        # Check done
        result = ssh_client.run(f"test -f {done_file} && echo exists", hide=True, warn=True)
        if result.ok and "exists" in result.stdout:
            return "completed"
        
        # Check failed
        result = ssh_client.run(f"test -f {failed_file} && echo exists", hide=True, warn=True)
        if result.ok and "exists" in result.stdout:
            return "failed"
        
        # Check running
        result = ssh_client.run(f"test -f {running_file} && echo exists", hide=True, warn=True)
        if result.ok and "exists" in result.stdout:
            return "running"
        
        return None
    
    def _check_squeue(self, ssh_client: SSHClient, slurm_job_id: str) -> Optional[str]:
        """Check job status via squeue."""
        cmd = f"squeue -j {slurm_job_id} -h -o %T"
        result = ssh_client.run(cmd, hide=True, warn=True)
        
        if not result.ok or not result.stdout.strip():
            return None
        
        slurm_status = result.stdout.strip().upper()
        
        if "RUNNING" in slurm_status:
            return "running"
        elif "PENDING" in slurm_status:
            return "pending"
        elif "COMPLETING" in slurm_status:
            return "running"
        
        return None
    
    def sync_all_jobs(self) -> int:
        """Manually sync all non-terminal jobs.
        
        Note: For automatic status sync, use RunnerDaemon (jrunner start).
        """
        jobs = self.session.query(Job).filter(
            ~Job.job_status.in_(["completed", "failed", "cancelled"])
        ).all()
        
        synced = 0
        for job in jobs:
            if self.sync_job_status(job.job_id):
                synced += 1
        
        return synced
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job on the remote worker via SLURM.

        The method:
        1. Database search:
            1) Search the job by job_id, checks job status;
            2) Search the corresponding worker.
        2. Workload management operations:
            1) Open an SSH connection;
            2) Execute `scancel <slurm_job_id>` on the remote cluster;
            3) On success, updates the local job status to "cancelled", sets end_time.

        Args:
            job_id: Local job identifier.

        Returns:
            True if the job was successfully cancelled on the remote cluster and the local
            record updated, otherwise False.
        """
        job = self.get_job(job_id)
        if not job or job.job_status in ["completed", "failed", "cancelled"]:
            return False
        
        try:
            worker = self.session.query(Worker).filter(
                Worker.worker_id == job.worker_id
            ).first()
            
            if not worker:
                return False
            
            worker_conn = WorkerConnection(worker)
            result = worker_conn.ssh_client.run(f"scancel {job.slurm_job_id}", hide=True, warn=True)
            
            if result.ok:
                job.job_status = "cancelled"
                job.end_time = datetime.now()
                self.session.commit()
                return True
            return False
        except:
            return False
    
    def cleanup_completed_job(self, job_id: str) -> None:
        """Clean up resources for a completed job.
        
        Args:
            job_id: Job identifier
        """
        # Currently no cleanup needed since tunnel sync is removed
        pass
