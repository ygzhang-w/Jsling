"""jsub - Job submission command."""

import click
from pathlib import Path
from rich.console import Console

from jsling.database.session import get_db
from jsling.core.job_manager import JobManager
from jsling.core.runner_daemon import is_daemon_running
from jsling.utils.job_config_parser import parse_job_config, merge_job_config

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--worker", "-w", help="Override target worker ID or name")
@click.option("--ntasks", "-n", type=int, help="Override tasks per node")
@click.option("--gres", "-g", help="Override GRES resources")
@click.option("--gpus-per-task", "-G", type=int, help="Override GPUs per task")
@click.option("--local-workdir", "-d", type=click.Path(), help="Local working directory")
@click.option("--upload", "-u", multiple=True, help="Extra files/directories to upload")
@click.option("--rsync-mode", "-r", type=click.Choice(['periodic', 'final_only']), help="Rsync mode")
def main(config_file, worker, ntasks, gres, gpus_per_task, local_workdir, upload, rsync_mode):
    """Submit a job to remote Slurm cluster.
    
    CONFIG_FILE: Path to job configuration YAML file
    """
    session = get_db()
    job_manager = JobManager(session)
    
    try:
        # Parse YAML configuration
        config_path = Path(config_file)
        if config_path.suffix.lower() not in ['.yaml', '.yml']:
            console.print("[red]Config file must be a YAML file (.yaml or .yml)[/red]")
            return
        
        try:
            job_config = parse_job_config(str(config_path))
            
            # Merge with CLI overrides
            config = merge_job_config(
                job_config,
                worker=worker,
                ntasks_per_node=ntasks,
                gres=gres,
                gpus_per_task=gpus_per_task,
                local_workdir=local_workdir,
                rsync_mode=rsync_mode
            )
            
            # Extract parameters
            actual_command = config['command']
            actual_worker = config['worker']
            actual_ntasks = config.get('ntasks_per_node')
            actual_gres = config.get('gres')
            actual_gpus = config.get('gpus_per_task')
            # Default local_workdir to current working directory
            actual_local_dir = config.get('local_workdir') or str(Path.cwd())
            actual_upload = list(upload) + config.get('upload', [])
            actual_rsync_mode = config.get('rsync_mode')  # None will use database default
            actual_rsync_interval = config.get('rsync_interval')
            
            # Convert sync_rules from Pydantic object to dict if present
            sync_rules_obj = config.get('sync_rules')
            if sync_rules_obj and hasattr(sync_rules_obj, 'model_dump'):
                actual_sync_rules = sync_rules_obj.model_dump()
            else:
                actual_sync_rules = sync_rules_obj
            
        except Exception as e:
            console.print(f"[red]Failed to parse YAML configuration: {e}[/red]")
            return
        
        # Submit job (queue for async submission by daemon)
        console.print("[cyan]Queuing job for submission...[/cyan]")
        job_id = job_manager.queue_job(
            worker_id=actual_worker,
            command=actual_command,
            local_workdir=actual_local_dir,
            ntasks_per_node=actual_ntasks,
            gres=actual_gres,
            gpus_per_task=actual_gpus,
            upload_files=actual_upload,
            rsync_mode=actual_rsync_mode,
            rsync_interval=actual_rsync_interval,
            sync_rules=actual_sync_rules
        )
                
        if not job_id:
            console.print("[red]Job queuing failed[/red]")
            return
                
        # Get job details
        job = job_manager.get_job(job_id)
        if not job:
            console.print("[red]Failed to retrieve job details[/red]")
            return
                
        # Check if daemon is running
        daemon_running = is_daemon_running()
                
        # Display success information
        console.print("[green]\u2713 Job queued successfully![/green]\n")
        console.print(f"[bold]Job ID:[/bold] {job.job_id}")
        console.print(f"[bold]Status:[/bold] {job.job_status}")
        console.print(f"[bold]Worker:[/bold] {job.worker_id}")
        console.print(f"[bold]Local Workdir:[/bold] {job.local_workdir}")
        console.print(f"[bold]Remote Workdir:[/bold] {job.remote_workdir}")
        console.print(f"[bold]Rsync Mode:[/bold] {job.rsync_mode}")
        if job.sync_rules:
            console.print(f"[bold]Sync Rules:[/bold] Configured (streaming enabled)")
                
        if daemon_running:
            console.print("\n[green]Job will be submitted automatically by the runner daemon[/green]")
        else:
            console.print("\n[yellow]Warning: Runner daemon is not running![/yellow]")
            console.print("[yellow]Start it with: jrunner start[/yellow]")
                
        # Next steps
        console.print("\n[bold]Next steps:[/bold]")
        console.print(f"  \u2022 Check status: [cyan]jqueue[/cyan]")
        console.print(f"  \u2022 View submission queue: [cyan]jqueue --pending[/cyan]")
        if not daemon_running:
            console.print(f"  \u2022 Start daemon: [cyan]jrunner start[/cyan]")
        
    finally:
        session.close()


if __name__ == "__main__":
    main()
