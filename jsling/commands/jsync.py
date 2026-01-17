"""
jsync command: Force rsync synchronization for jobs

This command triggers immediate rsync synchronization without waiting for periodic intervals.
"""

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from sqlalchemy.orm import Session

from jsling.database.session import get_db
from jsling.database.models import Job

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("job_id", type=str)
@click.option("--force", "-f", is_flag=True, help="Force sync even if job is not completed")
@click.option(
    "--direction",
    "-d",
    type=click.Choice(["download", "upload", "both"]),
    default="download",
    help="Sync direction (default: download)"
)
def main(job_id: str, force: bool, direction: str):
    """
    Force rsync synchronization for a job.
    
    JOB_ID: The job ID to synchronize
    
    Examples:
        jsync 20240115_103045
        jsync 20240115_103045 --force
        jsync 20240115_103045 -d both
    """
    session: Session = get_db()
    
    try:
        # Query job information
        job = session.query(Job).filter(Job.job_id == job_id).first()
        
        if not job:
            console.print(f"[red]Error: Job '{job_id}' not found[/red]")
            return
        
        # Check if job is running or completed
        if not force and job.job_status not in ["running", "completed", "failed"]:
            console.print(f"[yellow]Warning: Job is in '{job.job_status}' status[/yellow]")
            console.print("[yellow]Use --force to sync anyway[/yellow]")
            return
        
        # Display job information
        console.print(Panel(
            f"[cyan]Job ID:[/cyan] {job.job_id}\n"
            f"[cyan]Status:[/cyan] {job.job_status}\n"
            f"[cyan]Worker:[/cyan] {job.worker.worker_name}\n"
            f"[cyan]Remote Dir:[/cyan] {job.remote_workdir}\n"
            f"[cyan]Local Dir:[/cyan] {job.local_workdir}",
            title="Job Information",
            border_style="cyan"
        ))
        
        # Execute rsync synchronization
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            if direction in ["download", "both"]:
                task = progress.add_task("Downloading files from remote...", total=None)
                try:
                    synced_count = _sync_download(session, job)
                    progress.update(task, completed=True)
                    console.print(f"[green]✓ Downloaded {synced_count} files[/green]")
                except Exception as e:
                    progress.update(task, completed=True)
                    console.print(f"[red]✗ Download failed: {e}[/red]")
            
            if direction in ["upload", "both"]:
                task = progress.add_task("Uploading files to remote...", total=None)
                try:
                    synced_count = _sync_upload(session, job)
                    progress.update(task, completed=True)
                    console.print(f"[green]✓ Uploaded {synced_count} files[/green]")
                except Exception as e:
                    progress.update(task, completed=True)
                    console.print(f"[red]✗ Upload failed: {e}[/red]")
        
        # Update last_rsync_time
        from datetime import datetime
        job.last_rsync_time = datetime.now()
        session.commit()
        
        console.print("\n[green]Synchronization completed[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise
    finally:
        session.close()


def _sync_download(session: Session, job: Job) -> int:
    """
    Download files from remote to local using rsync.
    
    Args:
        session: Database session
        job: Job object
        
    Returns:
        Number of files synchronized
        
    Raises:
        RuntimeError: If rsync command fails
    """
    try:
        # Construct rsync command
        # Use rsync to download all files from remote_workdir to local_workdir
        # Handle IPv6 addresses - they need to be wrapped in brackets for rsync
        host = job.worker.host
        if ':' in host:  # IPv6 address
            host = f"[{host}]"
        remote_path = f"{job.worker.username}@{host}:{job.remote_workdir}/"
        local_path = job.local_workdir
        
        # Build rsync command with options:
        # -a: archive mode (preserves permissions, timestamps, etc.)
        # -v: verbose
        # -z: compress during transfer
        # --progress: show progress
        rsync_cmd = f"rsync -avz --progress {remote_path} {local_path}"
        
        # Execute rsync command locally
        import subprocess
        result = subprocess.run(
            rsync_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Rsync failed: {result.stderr}")
        
        # Count synchronized files from rsync output
        synced_count = result.stdout.count("receiving incremental file list")
        # Parse number of files from rsync output
        lines = result.stdout.split('\n')
        file_count = len([line for line in lines if line and not line.startswith('receiving') and not line.startswith('sent')])
        
        return max(file_count - 2, 0)  # Subtract header lines
        
    except subprocess.TimeoutExpired:
        raise RuntimeError("Rsync timeout (5 minutes)")
    except Exception as e:
        raise RuntimeError(f"Rsync error: {e}")


def _sync_upload(session: Session, job: Job) -> int:
    """
    Upload files from local to remote using rsync.
    
    Args:
        session: Database session
        job: Job object
        
    Returns:
        Number of files synchronized
        
    Raises:
        RuntimeError: If rsync command fails
    """
    try:
        # Construct rsync command for upload
        local_path = f"{job.local_workdir}/"
        
        # Handle IPv6 addresses - they need to be wrapped in brackets for rsync
        host = job.worker.host
        if ':' in host:  # IPv6 address
            host = f"[{host}]"
        remote_path = f"{job.worker.username}@{host}:{job.remote_workdir}/"
        
        rsync_cmd = f"rsync -avz --progress {local_path} {remote_path}"
        
        # Execute rsync command locally
        import subprocess
        result = subprocess.run(
            rsync_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Rsync failed: {result.stderr}")
        
        # Count synchronized files
        lines = result.stdout.split('\n')
        file_count = len([line for line in lines if line and not line.startswith('sending') and not line.startswith('sent')])
        
        return max(file_count - 2, 0)
        
    except subprocess.TimeoutExpired:
        raise RuntimeError("Rsync timeout (5 minutes)")
    except Exception as e:
        raise RuntimeError(f"Rsync error: {e}")


if __name__ == "__main__":
    main()
