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
    valid_statuses = ['pending', 'running', 'completed', 'failed', 'cancelled']
    
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


@click.group(invoke_without_command=True, context_settings=CONTEXT_SETTINGS)
@click.pass_context
@click.option("--all", "-a", "show_all", is_flag=True, help="Show all jobs including previously displayed failed/cancelled jobs")
@click.option("--filter", "-f", "filters", multiple=True, 
              help="Filter jobs by key=value (e.g. -f worker=node1 -f status=running)")
@click.option("--sync", "-S", is_flag=True, help="Manually sync job status (normally handled by runner daemon)")
@click.option("--limit", "-n", type=int, help="Limit number of results")
@click.option("--sort", type=click.Choice(['submit_time', 'job_id']), default='submit_time', 
              help="Sort by field")
def main(ctx, show_all, filters, sync, limit, sort):
    """Query job queue status.
    
    By default, failed/cancelled jobs are shown only once. Use --all to show all jobs.
    
    Job status is automatically updated by the runner daemon (jrunner).
    Use --sync to manually trigger a status sync if needed.
    
    \b
    Filter examples:
      jqueue -f worker=node1                      # Filter by worker ID
      jqueue -f status=running                    # Filter by status
      jqueue -f worker=node1 -f status=pending    # Combined filters
    """
    if ctx.invoked_subcommand is None:
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
            
            # If not showing all, exclude already-displayed failed/cancelled jobs
            if not show_all and not status_filter:
                # Show: pending, running, completed, 
                # AND failed/cancelled that haven't been displayed yet (marked_for_deletion=False)
                query = query.filter(
                    (Job.job_status.in_(['pending', 'running', 'completed'])) |
                    ((Job.job_status.in_(['failed', 'cancelled'])) & (Job.marked_for_deletion == False))
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
                
                # Mark failed/cancelled as displayed (will be hidden in future default calls)
                if job.job_status in ['failed', 'cancelled'] and not job.marked_for_deletion:
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
def cleanup():
    """Clean up terminated jobs."""
    session = get_db()
    job_manager = JobManager(session)
    
    try:
        from jsling.database.models import Job
        from jsling.core.config_manager import ConfigManager
        
        config_manager = ConfigManager(session)
        
        # Get cleanup config
        completed_days = int(config_manager.get('cleanup_completed_days') or '1')
        keep_local = (config_manager.get('cleanup_keep_local') or 'false').lower() == 'true'
        
        # Find jobs to clean
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
        
        total_cleaned = 0
        
        for job in completed_jobs + marked_jobs:
            try:
                # TODO: Stop sync process if running
                # TODO: Delete remote workdir via SSH
                # TODO: Delete local workdir if not keep_local
                
                session.delete(job)
                total_cleaned += 1
            except Exception as e:
                console.print(f"[red]Failed to clean job {job.job_id}: {e}[/red]")
        
        session.commit()
        console.print(f"[green]Cleaned up {total_cleaned} jobs[/green]")
        console.print(f"  - Completed (>{completed_days}d): {len(completed_jobs)}")
        console.print(f"  - Marked for deletion: {len(marked_jobs)}")
        
    finally:
        session.close()


if __name__ == "__main__":
    main()
