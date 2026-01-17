"""Shared utility for work directory cleanup operations."""

import os
import shutil
from typing import List, Tuple

from rich.console import Console


def delete_workdirs(
    jobs,
    cleanup_remote: bool,
    cleanup_local: bool,
    console: Console,
    verbose: bool = True
) -> Tuple[int, int, int, int]:
    """Delete work directories for the given jobs.
    
    This function only handles directory deletion, not job record deletion.
    Use this when you want to delete directories without removing job records.
    
    Args:
        jobs: List of Job objects
        cleanup_remote: Whether to delete remote directories
        cleanup_local: Whether to delete local directories
        console: Rich console for output
        verbose: Whether to print detailed progress (default True)
        
    Returns:
        Tuple of (remote_cleaned, remote_failed, local_cleaned, local_failed)
    """
    from jsling.connections.worker import Worker as WorkerConnection
    
    remote_cleaned = 0
    remote_failed = 0
    local_cleaned = 0
    local_failed = 0
    
    # Group jobs by worker for efficient SSH connections
    jobs_by_worker = {}
    for job in jobs:
        worker_id = job.worker_id or "none"
        if worker_id not in jobs_by_worker:
            jobs_by_worker[worker_id] = []
        jobs_by_worker[worker_id].append(job)
    
    for worker_id, worker_jobs in jobs_by_worker.items():
        worker_conn = None
        
        try:
            if cleanup_remote and worker_id != "none":
                # Get worker and establish connection
                worker = worker_jobs[0].worker
                if worker:
                    try:
                        worker_conn = WorkerConnection(worker)
                        if not worker_conn.test_connection():
                            console.print(f"[yellow]Warning: Cannot connect to worker {worker_id}, skipping remote cleanup[/yellow]")
                            worker_conn = None
                    except Exception as e:
                        console.print(f"[yellow]Warning: Failed to connect to worker {worker_id}: {e}[/yellow]")
                        worker_conn = None
            
            for job in worker_jobs:
                # Delete remote directory if requested
                if cleanup_remote and worker_conn and job.remote_workdir:
                    try:
                        result = worker_conn.ssh_client.run(
                            f"rm -rf {job.remote_workdir}",
                            hide=True,
                            warn=True
                        )
                        if result.ok:
                            if verbose:
                                console.print(f"[green]✓ Deleted remote: {job.remote_workdir}[/green]")
                            remote_cleaned += 1
                        else:
                            if verbose:
                                console.print(f"[red]✗ Failed to delete remote: {job.remote_workdir}[/red]")
                            else:
                                console.print(f"[yellow]Warning: Failed to delete remote dir for {job.job_id}[/yellow]")
                            remote_failed += 1
                    except Exception as e:
                        if verbose:
                            console.print(f"[red]✗ Error deleting remote {job.remote_workdir}: {e}[/red]")
                        remote_failed += 1
                
                # Delete local directory if requested
                if cleanup_local and job.local_workdir:
                    try:
                        if os.path.exists(job.local_workdir):
                            shutil.rmtree(job.local_workdir)
                            if verbose:
                                console.print(f"[green]✓ Deleted local: {job.local_workdir}[/green]")
                            local_cleaned += 1
                        else:
                            if verbose:
                                console.print(f"[dim]Local dir does not exist: {job.local_workdir}[/dim]")
                            # Count as cleaned since the goal is achieved
                            local_cleaned += 1
                    except Exception as e:
                        if verbose:
                            console.print(f"[red]✗ Error deleting local {job.local_workdir}: {e}[/red]")
                        else:
                            console.print(f"[yellow]Warning: Failed to delete local dir for {job.job_id}: {e}[/yellow]")
                        local_failed += 1
        
        finally:
            if worker_conn:
                worker_conn.close()
    
    return remote_cleaned, remote_failed, local_cleaned, local_failed


def print_workdir_summary(
    cleanup_remote: bool,
    cleanup_local: bool, 
    remote_cleaned: int,
    remote_failed: int,
    local_cleaned: int,
    local_failed: int,
    console: Console
) -> None:
    """Print a summary of workdir cleanup results.
    
    Args:
        cleanup_remote: Whether remote cleanup was requested
        cleanup_local: Whether local cleanup was requested
        remote_cleaned: Number of remote dirs cleaned
        remote_failed: Number of remote dir failures
        local_cleaned: Number of local dirs cleaned
        local_failed: Number of local dir failures
        console: Rich console for output
    """
    console.print()
    if cleanup_remote:
        console.print(f"Remote dirs deleted: {remote_cleaned}, failed: {remote_failed}")
    if cleanup_local:
        console.print(f"Local dirs deleted: {local_cleaned}, failed: {local_failed}")
