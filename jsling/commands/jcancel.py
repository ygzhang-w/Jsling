"""jcancel - Job cancellation command."""

import click
from rich.console import Console

from jsling.database.session import get_db
from jsling.core.job_manager import JobManager

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("job_ids", nargs=-1, required=True)
def main(job_ids):
    """Cancel one or more jobs.
    
    JOB_IDS: One or more job IDs to cancel (supports both jsling job_id and slurm_job_id)
    
    Examples:
        jcancel 20231222_123456_abc12345
        jcancel job1 job2 job3
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
            
            if job.job_status in ["completed", "failed", "cancelled"]:
                console.print(f"[yellow]Warning: Job {job.job_id} already in terminal state: {job.job_status}[/yellow]")
                continue
            
            jobs_to_cancel.append(job)
        
        if not jobs_to_cancel:
            console.print("[yellow]No jobs to cancel[/yellow]")
            return
        
        # Show jobs to be cancelled
        console.print(f"\n[bold]Jobs to cancel ({len(jobs_to_cancel)}):[/bold]")
        for job in jobs_to_cancel:
            console.print(f"  - {job.job_id} (Slurm: {job.slurm_job_id}, Status: {job.job_status})")
        
        # Cancel jobs
        console.print()
        success_count = 0
        fail_count = 0
        
        for job in jobs_to_cancel:
            try:
                result = job_manager.cancel_job(job.job_id)
                if result:
                    console.print(f"[green]✓ Cancelled: {job.job_id}[/green]")
                    success_count += 1
                else:
                    # Cancel failed - status will be updated by runner daemon
                    console.print(f"[yellow]⚠ Failed to cancel {job.job_id} (job may have already finished)[/yellow]")
                    fail_count += 1
            except Exception as e:
                console.print(f"[red]✗ Error cancelling {job.job_id}: {str(e)}[/red]")
                fail_count += 1
        
        # Summary
        console.print()
        if success_count > 0:
            console.print(f"[green]Successfully cancelled {success_count} job(s)[/green]")
        if fail_count > 0:
            console.print(f"[yellow]Failed to cancel {fail_count} job(s)[/yellow]")
    
    finally:
        session.close()


if __name__ == "__main__":
    main()
