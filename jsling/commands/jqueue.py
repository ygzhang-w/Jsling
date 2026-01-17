"""jqueue - Job queue query command."""

import click
from datetime import datetime, timedelta
from typing import Optional, Tuple
from rich.console import Console

from jsling.database.session import get_db
from jsling.core.job_manager import JobManager
from jsling.utils.cli_render import render_borderless_table, colorize_status

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def parse_filter(filter_values: Tuple[str, ...]) -> dict:
    """Parse filter option values into worker_id and status filters.
    
    Args:
        filter_values: Tuple of filter strings in format 'key=value'
        
    Returns:
        Dictionary with 'worker_id' and 'status' keys
    """
    result = {'worker_id': None, 'status': None}
    valid_statuses = ['queued', 'pending', 'running', 'completed', 'failed', 'cancelled', 'submission_failed']
    
    for fv in filter_values:
        if '=' not in fv:
            console.print(f"[yellow]Warning: Invalid filter format '{fv}', expected 'key=value'[/yellow]")
            continue
        key, value = fv.split('=', 1)
        key = key.strip().lower()
        value = value.strip()
        
        if key in ('worker', 'w'):
            result['worker_id'] = value
        elif key in ('status', 's'):
            if value.lower() in valid_statuses:
                result['status'] = value.lower()
            else:
                console.print(f"[yellow]Warning: Invalid status '{value}', valid: {', '.join(valid_statuses)}[/yellow]")
        else:
            console.print(f"[yellow]Warning: Unknown filter key '{key}', valid: worker, status[/yellow]")
    
    return result


def _ssh_login_to_job(job_id: str) -> None:
    """Open an SSH session to the job's remote work directory.
    
    Args:
        job_id: The job ID to login to
    """
    import os
    from jsling.database.models import Job
    from jsling.connections.worker import Worker as WorkerConnection
    
    session = get_db()
    try:
        # Find job (support partial ID match)
        job = session.query(Job).filter(Job.job_id.like(f"{job_id}%")).first()
        if not job:
            console.print(f"[red]Error: Job '{job_id}' not found[/red]")
            return
        
        if not job.worker:
            console.print(f"[red]Error: Worker not found for job '{job.job_id}'[/red]")
            return
        
        remote_dir = job.remote_workdir
        if not remote_dir:
            console.print(f"[red]Error: No remote work directory for job '{job.job_id}'[/red]")
            return
        
        # Use Worker abstraction to get login command
        worker_conn = WorkerConnection(job.worker)
        ssh_args = worker_conn.get_login_command(directory=remote_dir)
        
        console.print(f"[cyan]Connecting to {job.worker.host}:{remote_dir}...[/cyan]")
        
        # Replace current process with SSH
        os.execvp("ssh", ssh_args)
        
    finally:
        session.close()

@click.group(invoke_without_command=True, context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.option("--all", "-a", "show_all", is_flag=True, help="Show all jobs including previously displayed failed/cancelled jobs")
@click.option("--filter", "-f", "filters", multiple=True, 
              help="Filter jobs by key=value (e.g. -f worker=node1 -f status=running)")
@click.option("--sync", "-S", is_flag=True, help="Manually sync job status (normally handled by runner daemon)")
@click.option("--limit", "-n", type=int, help="Limit number of results")
@click.option("--sort", type=click.Choice(['submit_time', 'job_id']), default='submit_time', 
              help="Sort by field")
@click.option("--login", "-l", "login_job_id", type=str, help="Open SSH session to job's remote work directory")
def main(ctx, show_all, filters, sync, limit, sort, login_job_id):
    """Query job queue status.
    
    By default, failed/cancelled jobs are shown only once. Use --all to show all jobs.
    
    Job status is automatically updated by the runner daemon (jrunner).
    Use --sync to manually trigger a status sync if needed.
    
    \b
    Job statuses:
      queued            - Waiting for daemon to submit to cluster
      pending           - Submitted to Slurm, waiting for resources
      running           - Currently executing on cluster
      completed         - Finished successfully
      failed            - Execution failed
      cancelled         - Cancelled by user
      submission_failed - Failed to submit to cluster
    
    \b
    Filter examples:
      jqueue -f worker=node1                      # Filter by worker ID
      jqueue -f status=running                    # Filter by status
      jqueue -f worker=node1 -f status=pending    # Combined filters
      jqueue -f status=queued                     # Show submission queue
    """
    if ctx.invoked_subcommand is None:
        # Handle --login option: SSH into job's remote work directory
        if login_job_id:
            _ssh_login_to_job(login_job_id)
            return
        
        # Main command - list jobs
        session = get_db()
        job_manager = JobManager(session)
        
        try:
            # Parse filter options
            filter_opts = parse_filter(filters)
            worker_id = filter_opts['worker_id']
            status_filter = filter_opts['status']
            
            # Manual sync if requested
            # Note: Status is normally updated by runner daemon (jrunner start)
            if sync:
                console.print("[cyan]Manually syncing job status...[/cyan]")
                synced = job_manager.sync_all_jobs()
                console.print(f"[green]Synced {synced} jobs[/green]\n")
            
            # Get jobs from database
            from jsling.database.models import Job
            query = session.query(Job)
            
            # Apply filters
            if worker_id:
                query = query.filter(Job.worker_id == worker_id)
            if status_filter:
                query = query.filter(Job.job_status == status_filter)
            
            # If not showing all and no specific filter, apply default filtering
            if not show_all and not status_filter:
                # Show: queued, pending, running, completed, 
                # AND failed/cancelled/submission_failed that haven't been displayed yet
                query = query.filter(
                    (Job.job_status.in_(['queued', 'pending', 'running', 'completed'])) |
                    ((Job.job_status.in_(['failed', 'cancelled', 'submission_failed'])) & (Job.marked_for_deletion == False))
                )
            
            # Apply sorting
            if sort == 'submit_time':
                query = query.order_by(Job.submit_time.desc())
            else:
                query = query.order_by(Job.job_id.desc())
            
            # Apply limit
            if limit:
                query = query.limit(limit)
            
            jobs = query.all()
            
            if not jobs:
                console.print("[yellow]No jobs found[/yellow]")
                return
            
            # Prepare table data
            columns = [
                {'header': 'Job ID', 'style': 'cyan'},
                {'header': 'Slurm ID', 'style': 'magenta'},
                {'header': 'Status'},
                {'header': 'Worker', 'style': 'blue'},
                {'header': 'Submit Time'},
                {'header': 'Duration'}
            ]
            
            rows = []
            newly_displayed_count = 0
            for job in jobs:
                # Calculate duration
                duration = ""
                if job.start_time:
                    end = job.end_time or datetime.now()
                    delta = end - job.start_time
                    total_seconds = int(delta.total_seconds())
                    days = total_seconds // 86400
                    hours = (total_seconds % 86400) // 3600
                    minutes = (total_seconds % 3600) // 60
                    if days > 0:
                        duration = f"{days}d {hours}h {minutes}m"
                    else:
                        duration = f"{hours}h {minutes}m"
                
                # Colorize status
                status_text = colorize_status(job.job_status)
                
                rows.append([
                    job.job_id,
                    job.slurm_job_id or "N/A",
                    status_text,
                    job.worker_id,
                    job.submit_time.strftime("%Y-%m-%d %H:%M"),
                    duration
                ])
                
                # Mark failed/cancelled/submission_failed as displayed
                if job.job_status in ['failed', 'cancelled', 'submission_failed'] and not job.marked_for_deletion:
                    job.marked_for_deletion = True
                    newly_displayed_count += 1
            
            session.commit()
            render_borderless_table(console, "Job Queue", columns, rows)
            
            # Show hint if there are hidden jobs
            if not show_all and newly_displayed_count > 0:
                console.print(f"\n[dim]Note: {newly_displayed_count} failed/cancelled job(s) will be hidden in future calls. Use --all to show all jobs.[/dim]")
            
        finally:
            session.close()


@main.command(context_settings=CONTEXT_SETTINGS)
@click.argument("job_ids", nargs=-1, required=False)
@click.option("--status", "-s", "statuses", multiple=True, 
              help="Filter by status (comma-separated or multiple -s flags). Valid: completed, failed, cancelled, submission_failed")
@click.option("--remote", "-r", "cleanup_remote", is_flag=True, 
              help="Also delete remote work directories via SSH")
@click.option("--local", "-l", "cleanup_local", is_flag=True, 
              help="Also delete local work directories")
@click.option("--dry-run", "-n", is_flag=True, 
              help="Show what would be deleted without deleting")
@click.option("--force", "-f", is_flag=True, 
              help="Skip confirmation prompt")
def cleanup(job_ids, statuses, cleanup_remote, cleanup_local, dry_run, force):
    """Clean up terminated jobs or cancel specific jobs.
    
    When JOB_IDS are provided, cancel those specific jobs (similar to jcancel).
    Otherwise, cleans up completed jobs older than configured threshold
    and jobs marked for deletion (previously displayed failed/cancelled).
    
    \b
    Examples:
      jqueue cleanup                    # Default cleanup
      jqueue cleanup job123             # Cancel specific job
      jqueue cleanup -s failed          # Clean only failed jobs
      jqueue cleanup -s failed,cancelled  # Clean failed and cancelled
      jqueue cleanup -s cancelled --remote  # Also delete remote dirs
      jqueue cleanup --local            # Also delete local dirs
      jqueue cleanup -n                 # Dry-run (show what would be deleted)
    """
    from jsling.database.models import Job
    from jsling.core.config_manager import ConfigManager
    from jsling.connections.worker import Worker as WorkerConnection
    from jsling.core.job_manager import JobManager
    import shutil
    import os
    
    session = get_db()
    
    try:
        # Handle specific job IDs (cancel specific jobs)
        if job_ids:
            job_manager = JobManager(session)
            jobs_to_cancel = []
            
            for job_id in job_ids:
                job = job_manager.get_job(job_id)
                
                # Try finding by slurm_job_id if not found by job_id
                if not job:
                    job = session.query(Job).filter(Job.slurm_job_id == job_id).first()
                
                if not job:
                    console.print(f"[yellow]Warning: Job not found: {job_id}[/yellow]")
                    continue
                
                if job.job_status in ["completed", "failed", "cancelled", "submission_failed"]:
                    # Already in terminal state - can proceed with cleanup
                    jobs_to_cancel.append(job)
                elif job.job_status == "cancelling":
                    console.print(f"[yellow]Warning: Job {job.job_id} is being cancelled, will clean up...[/yellow]")
                    jobs_to_cancel.append(job)
                else:
                    # Queue for cancellation first
                    result = job_manager.queue_cancel_job(job.job_id)
                    if result:
                        console.print(f"[cyan]⏳ Queued for cancellation: {job.job_id}[/cyan]")
                        jobs_to_cancel.append(job)
                    else:
                        console.print(f"[yellow]Warning: Failed to cancel {job.job_id}[/yellow]")
            
            if not jobs_to_cancel:
                console.print("[yellow]No jobs to clean up[/yellow]")
                return
            
            # Show jobs to be cleaned
            console.print(f"\n[cyan]Found {len(jobs_to_cancel)} job(s) to clean:[/cyan]")
            for job in jobs_to_cancel:
                console.print(f"  - {job.job_id} ({job.job_status})")
                if cleanup_local:
                    console.print(f"    Local dir: {job.local_workdir}")
                if cleanup_remote:
                    console.print(f"    Remote dir: {job.remote_workdir}")
            
            if dry_run:
                console.print("\n[yellow]Dry-run mode - no changes made[/yellow]")
                return
            
            if not force and not click.confirm("Proceed with cleanup?"):
                console.print("[yellow]Cleanup cancelled[/yellow]")
                return
            
            _perform_cleanup(session, jobs_to_cancel, cleanup_remote, cleanup_local, console)
            return
        
        config_manager = ConfigManager(session)
        
        # Parse statuses (support comma-separated and multiple flags)
        valid_statuses = {'completed', 'failed', 'cancelled', 'submission_failed'}
        target_statuses = set()
        
        for status_arg in statuses:
            for s in status_arg.split(','):
                s = s.strip().lower()
                if s:
                    if s in valid_statuses:
                        target_statuses.add(s)
                    else:
                        console.print(f"[yellow]Warning: Invalid status '{s}', valid: {', '.join(sorted(valid_statuses))}[/yellow]")
        
        # Build query based on options
        if target_statuses:
            # User specified statuses - clean all matching jobs
            jobs_query = session.query(Job).filter(Job.job_status.in_(target_statuses))
        else:
            # Default behavior: completed older than threshold + marked_for_deletion
            completed_days = int(config_manager.get('cleanup_completed_days') or '1')
            cutoff_time = datetime.now() - timedelta(days=completed_days)
            
            # Completed jobs older than cutoff
            completed_jobs = session.query(Job).filter(
                Job.job_status == 'completed',
                Job.end_time < cutoff_time
            ).all()
            
            # Failed/cancelled jobs marked for deletion
            marked_jobs = session.query(Job).filter(
                Job.marked_for_deletion == True
            ).all()
            
            jobs_to_clean = completed_jobs + marked_jobs
            
            if not jobs_to_clean:
                console.print("[yellow]No jobs to clean up[/yellow]")
                return
            
            # Show what will be cleaned
            console.print(f"[cyan]Found {len(jobs_to_clean)} job(s) to clean:[/cyan]")
            console.print(f"  - Completed (>{completed_days}d): {len(completed_jobs)}")
            console.print(f"  - Marked for deletion: {len(marked_jobs)}")
            
            if dry_run:
                console.print("\n[yellow]Dry-run mode - no changes made[/yellow]")
                for job in jobs_to_clean:
                    console.print(f"  Would delete: {job.job_id} ({job.job_status})")
                    if cleanup_local:
                        console.print(f"    Local dir: {job.local_workdir}")
                    if cleanup_remote:
                        console.print(f"    Remote dir: {job.remote_workdir}")
                return
            
            if not force and not click.confirm("Proceed with cleanup?"):
                console.print("[yellow]Cleanup cancelled[/yellow]")
                return
            
            # Perform cleanup
            _perform_cleanup(session, jobs_to_clean, cleanup_remote, cleanup_local, console)
            return
        
        # For status-filtered cleanup
        jobs_to_clean = jobs_query.all()
        
        if not jobs_to_clean:
            console.print(f"[yellow]No jobs found with status: {', '.join(sorted(target_statuses))}[/yellow]")
            return
        
        console.print(f"[cyan]Found {len(jobs_to_clean)} job(s) to clean with status: {', '.join(sorted(target_statuses))}[/cyan]")
        
        if dry_run:
            console.print("\n[yellow]Dry-run mode - no changes made[/yellow]")
            for job in jobs_to_clean:
                console.print(f"  Would delete: {job.job_id} ({job.job_status})")
                if cleanup_local:
                    console.print(f"    Local dir: {job.local_workdir}")
                if cleanup_remote:
                    console.print(f"    Remote dir: {job.remote_workdir}")
            return
        
        if not force and not click.confirm("Proceed with cleanup?"):
            console.print("[yellow]Cleanup cancelled[/yellow]")
            return
        
        _perform_cleanup(session, jobs_to_clean, cleanup_remote, cleanup_local, console)
        
    finally:
        session.close()


def _perform_cleanup(session, jobs_to_clean, cleanup_remote, cleanup_local, console):
    """Perform the actual cleanup of jobs.
    
    Args:
        session: Database session
        jobs_to_clean: List of Job objects to clean
        cleanup_remote: Whether to delete remote directories
        cleanup_local: Whether to delete local directories
        console: Rich console for output
    """
    from jsling.utils.workdir_cleanup import delete_workdirs
    
    # Delete work directories using shared utility
    if cleanup_remote or cleanup_local:
        remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
            jobs_to_clean, cleanup_remote, cleanup_local, console, verbose=False
        )
    else:
        remote_cleaned = remote_failed = local_cleaned = local_failed = 0
    
    # Delete job records
    total_cleaned = 0
    for job in jobs_to_clean:
        try:
            session.delete(job)
            total_cleaned += 1
        except Exception as e:
            console.print(f"[red]Failed to delete job record {job.job_id}: {e}[/red]")
    
    session.commit()
    
    console.print(f"\n[green]Cleaned up {total_cleaned} job(s)[/green]")
    if cleanup_local:
        console.print(f"  - Local dirs deleted: {local_cleaned}")
        if local_failed > 0:
            console.print(f"  - Local dir failures: {local_failed}")
    if cleanup_remote:
        console.print(f"  - Remote dirs deleted: {remote_cleaned}")
        if remote_failed > 0:
            console.print(f"  - Remote dir failures: {remote_failed}")


if __name__ == "__main__":
    main()

