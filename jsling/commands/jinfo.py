"""jinfo - Worker resource information command."""

import json
import click
import yaml
from rich.console import Console

from jsling.database.session import get_db
from jsling.database.models import WorkerLiveStatus
from jsling.core.worker_manager import WorkerManager
from jsling.utils.cli_render import render_borderless_table, colorize_status

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--worker", "-w", help="Show detailed info for specific worker")
@click.option("--type", "-t", type=click.Choice(['cpu', 'gpu']), help="Filter by worker type")
@click.option("--format", "-f", type=click.Choice(['table', 'json', 'yaml']), default='table', 
              help="Output format")
def main(worker, type, format):
    """Display worker resource information.
    
    By default shows a summary view with available/total nodes for all workers.
    Use --worker/-w to see detailed information for a specific worker.
    
    Node availability data is polled by the runner daemon. Make sure
    'jsling runner start' is running for up-to-date information.
    
    Examples:
        jinfo                # Show all workers summary
        jinfo -w myworker    # Show detailed info for myworker
        jinfo -t gpu         # Show only GPU workers
        jinfo -f json        # Output as JSON
    """
    session = get_db()
    worker_manager = WorkerManager(session)
    
    try:
        # Get workers
        workers = worker_manager.list_workers(
            worker_type=type,
            include_inactive=False
        )
        
        if not workers:
            console.print("[yellow]No workers found[/yellow]")
            return
        
        # If specific worker requested, show detailed view
        if worker:
            _show_worker_detail(session, workers, worker, format)
        else:
            _show_workers_summary(session, workers, format)
    
    finally:
        session.close()


def _get_combined_status(is_active: bool, queue_state: str, has_error: bool) -> str:
    """Get combined status from worker config and queue state.
    
    Args:
        is_active: Whether worker is enabled in Jsling config
        queue_state: Slurm queue state ('up', 'down', etc.)
        has_error: Whether there was an error fetching live status
    
    Returns:
        Combined status: 'active', 'inactive', or 'down'
        - active: enabled in Jsling AND queue is up
        - inactive: disabled in Jsling (regardless of queue state)
        - down: enabled in Jsling BUT queue is down or has error
    """
    if not is_active:
        return 'inactive'
    
    # Worker is enabled, check queue state
    if has_error or queue_state in ('down', 'drain', 'draining', None, 'N/A', '-'):
        return 'down'
    elif queue_state == 'up':
        return 'active'
    else:
        # Unknown state, treat as down for safety
        return 'down'


def _show_workers_summary(session, workers, format):
    """Show summary view of all workers with node availability."""
    worker_data = []
    
    for w in workers:
        # Get live status from database
        live_status = session.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == w.worker_id
        ).first()
        
        # Determine combined status
        if live_status:
            queue_state = live_status.queue_state
            has_error = bool(live_status.error_message)
        else:
            queue_state = None
            has_error = False  # No data yet, not an error per se
        
        combined_status = _get_combined_status(w.is_active, queue_state, has_error)
        
        data = {
            'worker_id': w.worker_id,
            'name': w.worker_name,
            'type': w.worker_type,
            'queue': w.queue_name,
            'status': combined_status,
        }
        
        # Add live status info
        if live_status and not live_status.error_message:
            nodes_avail = live_status.nodes_available
            nodes_total = live_status.nodes_total
            if nodes_avail is not None and nodes_total is not None:
                data['nodes'] = f"{nodes_avail}/{nodes_total}"
            else:
                data['nodes'] = "N/A"
        elif live_status and live_status.error_message:
            data['nodes'] = "Error"
        else:
            data['nodes'] = "-"
        
        worker_data.append(data)
    
    # Output based on format
    if format == 'json':
        console.print_json(data=worker_data)
    elif format == 'yaml':
        console.print(yaml.dump(worker_data, default_flow_style=False))
    else:  # table
        columns = [
            {'header': 'Worker', 'style': 'cyan'},
            {'header': 'Name', 'style': 'green'},
            {'header': 'Type', 'style': 'magenta'},
            {'header': 'Queue'},
            {'header': 'Nodes', 'style': 'yellow'},
            {'header': 'Status'},
        ]
        
        rows = []
        for data in worker_data:
            # Colorize status
            status_text = colorize_status(data['status'])
            
            row = [
                data['worker_id'],
                data['name'],
                data['type'],
                data['queue'],
                data['nodes'],
                status_text,
            ]
            rows.append(row)
        
        render_borderless_table(console, "Worker Summary", columns, rows)
        console.print("[dim]Tip: Use 'jinfo -w <worker>' for detailed info[/dim]")


def _show_worker_detail(session, workers, worker_filter, format):
    """Show detailed view for a specific worker."""
    # Filter by worker ID or name
    filtered = [w for w in workers if w.worker_id == worker_filter or w.worker_name == worker_filter]
    
    if not filtered:
        console.print(f"[red]Worker not found: {worker_filter}[/red]")
        return
    
    worker_data = []
    
    for w in filtered:
        # Get live status from database
        live_status = session.query(WorkerLiveStatus).filter(
            WorkerLiveStatus.worker_id == w.worker_id
        ).first()
        
        # Determine combined status
        if live_status:
            queue_state = live_status.queue_state
            has_error = bool(live_status.error_message)
        else:
            queue_state = None
            has_error = False
        
        combined_status = _get_combined_status(w.is_active, queue_state, has_error)
        
        data = {
            'worker_id': w.worker_id,
            'name': w.worker_name,
            'type': w.worker_type,
            'host': w.host,
            'port': w.port,
            'queue': w.queue_name,
            'remote_workdir': w.remote_workdir,
            'ntasks': w.ntasks_per_node,
            'status': combined_status,
        }
        
        # Add GPU-specific fields
        if w.worker_type == 'gpu':
            data['gres'] = w.gres or 'N/A'
            data['gpus_per_task'] = w.gpus_per_task or 'N/A'
        
        # Add live status info
        if live_status:
            if live_status.nodes_available is not None and live_status.nodes_total is not None:
                data['nodes_available'] = live_status.nodes_available
                data['nodes_total'] = live_status.nodes_total
            if live_status.cpu_alloc is not None:
                data['cpu_alloc'] = live_status.cpu_alloc
                data['cpu_total'] = live_status.cpu_total
                data['cpu_other'] = live_status.cpu_other
            if live_status.gpu_alloc is not None:
                data['gpu_alloc'] = live_status.gpu_alloc
                data['gpu_total'] = live_status.gpu_total
            if live_status.error_message:
                data['error'] = live_status.error_message
        
        worker_data.append(data)
    
    # Output based on format
    if format == 'json':
        console.print_json(data=worker_data)
    elif format == 'yaml':
        console.print(yaml.dump(worker_data, default_flow_style=False))
    else:  # table - show as key-value pairs for detail view
        for data in worker_data:
            console.print(f"\n[bold cyan]Worker: {data['worker_id']}[/bold cyan]")
            console.print(f"  Name:          {data['name']}")
            console.print(f"  Type:          {data['type']}")
            console.print(f"  Host:          {data['host']}:{data['port']}")
            console.print(f"  Queue:         {data['queue']}")
            console.print(f"  Remote Dir:    {data['remote_workdir']}")
            console.print(f"  Tasks/Node:    {data['ntasks']}")
            if data['type'] == 'gpu':
                console.print(f"  GRES:          {data.get('gres', 'N/A')}")
                console.print(f"  GPUs/Task:     {data.get('gpus_per_task', 'N/A')}")
            console.print(f"  Status:        {colorize_status(data['status'])}")
            
            # Live status section
            if 'nodes_available' in data or 'cpu_alloc' in data or 'gpu_alloc' in data:
                console.print("\n  [bold]Resources:[/bold]")
                if 'nodes_available' in data:
                    console.print(f"    Nodes:       {data['nodes_available']}/{data['nodes_total']} available")
                
                if 'cpu_alloc' in data:
                    console.print(f"    CPU Usage:   {data['cpu_alloc']}/{data['cpu_total']} allocated")
                    if data['cpu_other'] > 0:
                        console.print(f"    CPU Other:   {data['cpu_other']} (down/drain)")
                
                if 'gpu_alloc' in data:
                    console.print(f"    GPU Usage:   {data['gpu_alloc']}/{data['gpu_total']} allocated")
            
            if 'error' in data:
                console.print(f"\n  [red]Error: {data['error']}[/red]")


if __name__ == "__main__":
    main()
