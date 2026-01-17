"""jcancel - Job cancellation command."""

import click
from rich.console import Console

from jsling.database.session import get_db
from jsling.core.job_manager import JobManager
from jsling.core.runner_daemon import is_daemon_running

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("job_ids", nargs=-1, required=True)
@click.option("--remote", "-r", "cleanup_remote", is_flag=True,
              help="Also delete remote work directories via SSH")
@click.option("--local", "-l", "cleanup_local", is_flag=True,
              help="Also delete local work directories")
@click.option("--force", "-f", is_flag=True,
              help="Skip confirmation prompt for directory deletion")
def main(job_ids, cleanup_remote, cleanup_local, force):
    """Cancel one or more jobs.
    
    JOB_IDS: One or more job IDs to cancel (supports both jsling job_id and slurm_job_id)
    
    Use --remote/-r to also delete the remote work directory.
    Use --local/-l to also delete the local work directory.
    Use --force/-f to skip the confirmation prompt when deleting directories.
    
    Examples:
        jcancel 20231222_123456_abc12345
        jcancel job1 job2 job3
        jcancel job1 --remote              # Also delete remote workdir
        jcancel job1 -r -l                 # Delete both remote and local workdirs
        jcancel job1 -r -l -f              # Delete workdirs without confirmation
    """
    if not job_ids:
        console.print("[red]Error: No job IDs provided[/red]")
        return
    
    session = get_db()
    job_manager = JobManager(session)
    
    try:
        # Collect jobs to cancel
        jobs_to_cancel = []
        for job_id in job_ids:
            job = job_manager.get_job(job_id)
            
            # Try finding by slurm_job_id if not found by job_id
            if not job:
                from jsling.database.models import Job
                job = session.query(Job).filter(Job.slurm_job_id == job_id).first()
            
            if not job:
                console.print(f"[yellow]Warning: Job not found: {job_id}[/yellow]")
                continue
            
            if job.job_status in ["completed", "failed", "cancelled", "submission_failed"]:
                console.print(f"[yellow]Warning: Job {job.job_id} already in terminal state: {job.job_status}[/yellow]")
                continue
            
            if job.job_status == "cancelling":
                console.print(f"[yellow]Warning: Job {job.job_id} is already being cancelled[/yellow]")
                continue
            
            jobs_to_cancel.append(job)
        
        if not jobs_to_cancel:
            console.print("[yellow]No jobs to cancel[/yellow]")
            return
        
        # Show jobs to be cancelled
        console.print(f"\n[bold]Jobs to cancel ({len(jobs_to_cancel)}):[/bold]")
        for job in jobs_to_cancel:
            slurm_id = job.slurm_job_id or "N/A"
            console.print(f"  - {job.job_id} (Slurm: {slurm_id}, Status: {job.job_status})")
        
        # Queue cancellations
        console.print()
        success_count = 0
        fail_count = 0
        queued_count = 0
        
        for job in jobs_to_cancel:
            try:
                was_queued = job.job_status == "queued"
                result = job_manager.queue_cancel_job(job.job_id)
                if result:
                    if was_queued:
                        console.print(f"[green]✓ Cancelled: {job.job_id}[/green]")
                        success_count += 1
                    else:
                        console.print(f"[cyan]⏳ Queued for cancellation: {job.job_id}[/cyan]")
                        queued_count += 1
                else:
                    console.print(f"[yellow]⚠ Failed to queue cancellation for {job.job_id}[/yellow]")
                    fail_count += 1
            except Exception as e:
                console.print(f"[red]✗ Error cancelling {job.job_id}: {str(e)}[/red]")
                fail_count += 1
        
        # Summary
        console.print()
        if success_count > 0:
            console.print(f"[green]Successfully cancelled {success_count} job(s)[/green]")
        if queued_count > 0:
            console.print(f"[cyan]Queued {queued_count} job(s) for cancellation[/cyan]")
            # Check daemon status
            daemon_running = is_daemon_running()
            if daemon_running:
                console.print("[green]Runner daemon will process cancellations shortly[/green]")
            else:
                console.print("[yellow]Warning: Runner daemon is not running![/yellow]")
                console.print("[yellow]Start it with: jrunner start[/yellow]")
        if fail_count > 0:
            console.print(f"[yellow]Failed to cancel {fail_count} job(s)[/yellow]")
        
        # Handle workdir deletion if requested
        if cleanup_remote or cleanup_local:
            _delete_workdirs(session, jobs_to_cancel, cleanup_remote, cleanup_local, force)
    
    finally:
        session.close()


def _delete_workdirs(session, jobs, cleanup_remote, cleanup_local, force):
    """Delete work directories and job records for the given jobs.
    
    When directories are being deleted, the job records are also removed
    from the database since there's no value in keeping them.
    
    Args:
        session: Database session for deleting job records
        jobs: List of Job objects
        cleanup_remote: Whether to delete remote directories
        cleanup_local: Whether to delete local directories
        force: Whether to skip confirmation prompt
    """
    from jsling.utils.workdir_cleanup import delete_workdirs, print_workdir_summary
    
    # Show what will be deleted
    console.print(f"\n[bold]Work directories to delete:[/bold]")
    for job in jobs:
        if cleanup_remote and job.remote_workdir:
            console.print(f"  [cyan]Remote:[/cyan] {job.remote_workdir}")
        if cleanup_local and job.local_workdir:
            console.print(f"  [cyan]Local:[/cyan] {job.local_workdir}")
    console.print(f"\n[dim]Note: Job records will also be removed from database[/dim]")
    
    # Confirm unless --force is used
    if not force:
        console.print()
        if not click.confirm("Proceed with directory deletion and job removal?"):
            console.print("[yellow]Cleanup cancelled[/yellow]")
            return
    
    # Use shared utility for actual deletion
    remote_cleaned, remote_failed, local_cleaned, local_failed = delete_workdirs(
        jobs, cleanup_remote, cleanup_local, console, verbose=True
    )
    
    # Delete job records from database
    deleted_count = 0
    for job in jobs:
        try:
            session.delete(job)
            deleted_count += 1
        except Exception as e:
            console.print(f"[red]Failed to delete job record {job.job_id}: {e}[/red]")
    session.commit()
    
    # Print summary
    print_workdir_summary(
        cleanup_remote, cleanup_local,
        remote_cleaned, remote_failed,
        local_cleaned, local_failed,
        console
    )
    console.print(f"[green]Removed {deleted_count} job record(s) from database[/green]")


if __name__ == "__main__":
    main()

