"""Job wrapper - generates enhanced Slurm scripts with sentinel files for status tracking."""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from jsling.database.models import Worker


class JobWrapper:
    """Wraps user commands with Slurm SBATCH directives and sentinel file tracking."""
    
    def __init__(
        self,
        job_id: str,
        worker: Worker,
        command: str,
        remote_workdir: str,
        ntasks_per_node: Optional[int] = None,
        nodes: Optional[int] = None,
        gres: Optional[str] = None,
        gpus_per_task: Optional[int] = None
    ):
        """Initialize job wrapper.
        
        Args:
            job_id: Unique job identifier
            worker: Worker configuration
            command: Command to execute on remote worker
            remote_workdir: Remote working directory path
            ntasks_per_node: Override tasks per node (uses worker default if None)
            nodes: Number of nodes to request
            gres: Override GRES resources (uses worker default if None)
            gpus_per_task: Override GPUs per task (uses worker default if None)
        """
        self.job_id = job_id
        self.worker = worker
        self.command = command
        self.remote_workdir = remote_workdir
        
        # Resource configuration (CLI override or Worker default)
        self.ntasks_per_node = ntasks_per_node or worker.ntasks_per_node
        self.nodes = nodes
        self.gres = gres or worker.gres
        self.gpus_per_task = gpus_per_task or worker.gpus_per_task
    
    def generate_sbatch_directives(self) -> str:
        """Generate SBATCH directives for Slurm script.
        
        Returns:
            SBATCH directives as multi-line string
        """
        directives = []
        directives.append("#!/bin/bash")
        directives.append(f"#SBATCH --job-name={self.job_id}")
        directives.append(f"#SBATCH --partition={self.worker.queue_name}")
        directives.append(f"#SBATCH --ntasks-per-node={self.ntasks_per_node}")
        
        # Node count if specified
        if self.nodes:
            directives.append(f"#SBATCH --nodes={self.nodes}")
        
        # GPU-specific resources
        if self.worker.worker_type == 'gpu':
            if self.gres:
                directives.append(f"#SBATCH --gres={self.gres}")
            if self.gpus_per_task:
                directives.append(f"#SBATCH --gpus-per-task={self.gpus_per_task}")
        
        # Output and error logs (named with job_id for synchronization)
        log_path = os.path.join(self.remote_workdir, f"{self.job_id}.out")
        err_path = os.path.join(self.remote_workdir, f"{self.job_id}.err")
        directives.append(f"#SBATCH --output={log_path}")
        directives.append(f"#SBATCH --error={err_path}")
        
        return "\n".join(directives)
    
    def generate_sentinel_code(self) -> tuple[str, str, str]:
        """Generate sentinel file tracking code.
        
        Returns:
            Tuple of (pre_script, post_script, error_handler) code blocks
        """
        # Sentinel file paths
        running_file = os.path.join(self.remote_workdir, ".jsling_status_running")
        done_file = os.path.join(self.remote_workdir, ".jsling_status_done")
        failed_file = os.path.join(self.remote_workdir, ".jsling_status_failed")
        
        # Pre-script: Mark as running
        pre_script = f"""
# Jsling: Mark job as running
echo "START_TIME=$(date -Iseconds)" > {running_file}
"""
        
        # Post-script: Mark as done with exit code
        post_script = f"""
# Jsling: Mark job as completed
EXIT_CODE=$?
echo "EXIT_CODE=$EXIT_CODE" > {done_file}
echo "END_TIME=$(date -Iseconds)" >> {done_file}
rm -f {running_file}
exit $EXIT_CODE
"""
        
        # Error handler: Mark as failed
        error_handler = f"""
# Jsling: Error handler - mark job as failed
trap 'EXIT_CODE=$?; ERROR_MSG="Script failed with exit code $EXIT_CODE"; echo "EXIT_CODE=$EXIT_CODE" > {failed_file}; echo "END_TIME=$(date -Iseconds)" >> {failed_file}; echo "ERROR_MESSAGE=$ERROR_MSG" >> {failed_file}; rm -f {running_file}; exit $EXIT_CODE' ERR
"""
        
        return pre_script, post_script, error_handler
    
    def wrap_script(self) -> str:
        """Generate complete wrapped Slurm script.
        
        Returns:
            Complete enhanced script content
        """
        # Generate components
        sbatch_directives = self.generate_sbatch_directives()
        pre_script, post_script, error_handler = self.generate_sentinel_code()
        
        # Assemble complete script
        wrapped = []
        wrapped.append(sbatch_directives)
        wrapped.append("")
        wrapped.append(error_handler)
        wrapped.append(pre_script)
        wrapped.append("# ===== User Command Starts Here =====")
        wrapped.append("")
        wrapped.append(self.command)
        wrapped.append("")
        wrapped.append("# ===== User Command Ends Here =====")
        wrapped.append(post_script)
        
        return '\n'.join(wrapped)
    
    def save_wrapped_script(self, output_path: str) -> None:
        """Save wrapped script to file.
        
        Args:
            output_path: Path to save the wrapped script
        """
        wrapped_content = self.wrap_script()
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write(wrapped_content)
        
        # Make script executable
        output_file.chmod(0o755)
