"""Job manager - handles job lifecycle management.

Job status synchronization is managed by RunnerDaemon's StatusPoller,
which runs as a persistent background process.
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

from sqlalchemy.orm import Session

from jsling.connections.ssh_client import SSHClient
from jsling.connections.worker import Worker as WorkerConnection
from jsling.core.job_wrapper import JobWrapper
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
    
    def submit_job(
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
        """Submit job to remote worker.
        
        Note: Status synchronization is handled by RunnerDaemon. Make sure 
        the daemon is running with `jrunner start` for automatic status updates.
        
        Args:
            worker_id: Worker ID or name
            command: Command to execute on remote worker
            local_workdir: Local working directory (optional)
            ntasks_per_node: Tasks per node override (optional)
            gres: GRES resources override (optional)
            gpus_per_task: GPUs per task override (optional)
            upload_files: Additional files to upload (optional)
            rsync_mode: Rsync mode (periodic or final_only). If not provided, reads from database config.
            rsync_interval: Rsync interval in seconds (for periodic mode)
            sync_rules: File synchronization rules (stream and rsync patterns)
            
        Returns:
            Job ID if successful, None otherwise
            
        Raises:
            KeyError: If rsync_mode not provided and database config is missing
        """
        # Get worker
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
            # Create worker connection
            worker_conn = WorkerConnection(worker)
            ssh_client = worker_conn.ssh_client
            
            # Wrap command with JobWrapper
            wrapper = JobWrapper(
                job_id=job_id,
                worker=worker,
                command=command,
                remote_workdir=remote_workdir,
                ntasks_per_node=ntasks_per_node,
                gres=gres,
                gpus_per_task=gpus_per_task
            )
            wrapped_content = wrapper.wrap_script()
            
            uploaded_files: List[str] = []
            
            # Ensure remote workdir exists (rsync will create contents)
            ssh_client.run(f"mkdir -p {remote_workdir}", hide=True, warn=True)
            
            # Use a temporary staging directory for initial upload
            staging_dir = Path(tempfile.mkdtemp(prefix=f"jsling_upload_{job_id}_"))
            try:
                # Write wrapped script to staging directory
                script_path = staging_dir / "job.sh"
                script_path.write_text(wrapped_content)
                script_path.chmod(0o755)
                
                # Stage additional upload files into staging directory
                staged_successful, staged_failed = self._prepare_local_upload_files(staging_dir, upload_files)
                uploaded_files = staged_successful
                if staged_failed:
                    print(f"Warning: Failed to stage some files for upload: {staged_failed}")
                
                # Upload staging directory to remote using rsync
                if not self._rsync_initial_upload(worker, staging_dir, remote_workdir):
                    print("Failed to upload files via rsync")
                    return None
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
                print(f"Failed to submit job: {result.stderr}")
                return None
            
            # Parse Slurm job ID from output
            slurm_job_id = self._parse_slurm_job_id(result.stdout)
            if not slurm_job_id:
                print(f"Failed to parse Slurm job ID from: {result.stdout}")
                return None
            
            # Create job record in database
            job = Job(
                job_id=job_id,
                slurm_job_id=slurm_job_id,
                worker_id=worker.worker_id,
                local_workdir=str(local_dir),
                remote_workdir=remote_workdir,
                script_path="",  # DEPRECATED: kept for compatibility
                command=command,
                job_status="pending",
                sync_mode="sentinel",  # Only sentinel file sync is supported
                rsync_mode=rsync_mode,
                rsync_interval=rsync_interval,
                sync_rules=json.dumps(sync_rules) if sync_rules else None,
                uploaded_files=json.dumps(uploaded_files) if uploaded_files else None,
                submit_time=datetime.now()
            )
            
            self.session.add(job)
            self.session.commit()
            
            return job_id
            
        except Exception as e:
            print(f"Error submitting job: {e}")
            self.session.rollback()
            return None
    
    def _prepare_local_upload_files(self, staging_dir: Path, upload_files: Optional[List[str]]) -> tuple[List[str], List[str]]:
        """Prepare upload files in a local staging directory.
        
        Args:
            staging_dir: Temporary staging directory path
            upload_files: List of user-specified file or directory paths
        
        Returns:
            Tuple of (successful_paths, failed_paths) based on original input strings.
        """
        successful: List[str] = []
        failed: List[str] = []
        if not upload_files:
            return successful, failed
        
        for path in upload_files:
            src = Path(path).expanduser()
            if not src.exists():
                failed.append(path)
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
                else:
                    failed.append(path)
                    continue
                successful.append(path)
            except Exception:
                failed.append(path)
        
        return successful, failed
    
    def _build_rsync_ssh_command(self, worker: Worker) -> str:
        """Build SSH command string for rsync based on worker configuration."""
        from jsling.utils.encryption import decrypt_credential
        
        ssh_parts = ["ssh"]
        
        if worker.port and worker.port != 22:
            ssh_parts.extend(["-p", str(worker.port)])
        
        if worker.auth_method == "key":
            credential = decrypt_credential(worker.auth_credential)
            key_path = os.path.expanduser(credential)
            if os.path.exists(key_path):
                ssh_parts.extend(["-i", key_path])
        
        ssh_parts.extend(["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"])
        
        return " ".join(ssh_parts)
    
    def _rsync_initial_upload(self, worker: Worker, staging_dir: Path, remote_workdir: str) -> bool:
        """Upload staged files to remote workdir using rsync.
        
        Args:
            worker: Worker configuration
            staging_dir: Local staging directory containing job.sh and upload files
            remote_workdir: Remote working directory for this job
        
        Returns:
            True if rsync command succeeded, False otherwise
        """
        local_path = f"{str(staging_dir)}/"
        remote_path = f"{worker.username}@{worker.host}:{remote_workdir}/"
        
        ssh_cmd = self._build_rsync_ssh_command(worker)
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
            print("Rsync timeout (5 minutes)")
            return False
        except Exception as e:
            print(f"Rsync error: {e}")
            return False
        
        if result.returncode != 0:
            print(f"Rsync failed: {result.stderr}")
            return False
        
        return True
    
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
