"""Main CLI entry point for Jsling."""

import click
from rich.console import Console

from jsling import __version__
from jsling.database.session import init_database, get_db
from jsling.database.schema import init_default_configs
from jsling.database.config import DB_PATH
from jsling.core.config_manager import ConfigManager
from jsling.core.worker_manager import WorkerManager
from jsling.commands.jrunner import runner
from jsling.utils.cli_render import render_borderless_table, render_key_value_list, colorize_status, print_logo


console = Console()

# Shared context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class JslingGroup(click.Group):
    """Custom Click Group that displays logo in help."""
    
    def format_help(self, ctx, formatter):
        """Write the help into the formatter if it exists."""
        # Print logo before help text
        print_logo(console, version=__version__)
        super().format_help(ctx, formatter)


@click.group(cls=JslingGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__, prog_name="jsling")
def main():
    """Jsling - Remote Job Submission System for Slurm Clusters."""
    pass


@main.command(context_settings=CONTEXT_SETTINGS)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation and always clear database")
def init(force):
    """Initialize Jsling database and configurations.
    
    This command will clear the entire database if it already exists.
    """
    try:
        if not force and DB_PATH.exists():
            if not click.confirm(f"Database at {DB_PATH} already exists. Clear and re-initialize?"):
                console.print("[yellow]Aborted[/yellow]")
                return
        
        console.print("[bold blue]Initializing Jsling...[/bold blue]")
        
        # Initialize database (force clear existing if any)
        init_database(force=True)
        console.print("✓ Database initialized", style="green")
        
        # Initialize default configurations
        db = get_db()
        try:
            init_default_configs(db)
            console.print("✓ Default configurations initialized", style="green")
        finally:
            db.close()
        
        console.print("\n[bold green]Jsling initialization complete![/bold green]")
        console.print(f"Database location: {DB_PATH}")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@main.group(cls=JslingGroup, context_settings=CONTEXT_SETTINGS)
def worker():
    """Manage remote workers."""
    pass


@worker.command("add", context_settings=CONTEXT_SETTINGS)
@click.option("--config", "-c", "config_file", required=True, type=click.Path(exists=True),
              help="Path to worker YAML configuration file")
def worker_add(config_file):
    """Register a new worker from YAML configuration."""
    try:
        console.print(f"[bold blue]Registering worker from {config_file}...[/bold blue]")
        
        db = get_db()
        try:
            manager = WorkerManager(db)
            worker_model = manager.add_from_yaml(config_file)
            
            console.print(f"[green]✓ Worker registered successfully![/green]")
            console.print(f"  Worker ID: [cyan]{worker_model.worker_id}[/cyan]")
            console.print(f"  Name: [cyan]{worker_model.worker_name}[/cyan]")
            console.print(f"  Type: [cyan]{worker_model.worker_type}[/cyan]")
            console.print(f"  Queue: [cyan]{worker_model.queue_name}[/cyan]")
            console.print(f"  Host: [cyan]{worker_model.username}@{worker_model.host}:{worker_model.port}[/cyan]")
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("list", context_settings=CONTEXT_SETTINGS)
@click.option("--type", "-t", "worker_type", type=click.Choice(["cpu", "gpu"]),
              help="Filter by worker type")
@click.option("--all", "show_all", is_flag=True, help="Show inactive workers too")
def worker_list(worker_type, show_all):
    """List all registered workers."""
    try:
        db = get_db()
        try:
            manager = WorkerManager(db)
            
            if worker_type:
                workers = manager.list_by_type(worker_type, active_only=not show_all)
            else:
                workers = manager.list_all(active_only=not show_all)
            
            if not workers:
                console.print("[yellow]No workers found[/yellow]")
                return
            
            columns = [
                {'header': 'ID', 'style': 'cyan'},
                {'header': 'Name', 'style': 'green'},
                {'header': 'Type', 'style': 'magenta'},
                {'header': 'Queue', 'style': 'yellow'},
                {'header': 'Host', 'style': 'blue'},
                {'header': 'Status'}
            ]
            
            rows = []
            for w in workers:
                status_text = colorize_status('active' if w.is_active else 'inactive')
                rows.append([
                    w.worker_id,
                    w.worker_name,
                    w.worker_type,
                    w.queue_name,
                    f"{w.username}@{w.host}",
                    status_text
                ])
            
            render_borderless_table(console, "Registered Workers", columns, rows)
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("show", context_settings=CONTEXT_SETTINGS)
@click.argument("worker_id")
def worker_show(worker_id):
    """Show detailed worker information."""
    try:
        db = get_db()
        try:
            manager = WorkerManager(db)
            worker_model = manager.get(worker_id)
            
            if not worker_model:
                # Try by name
                worker_model = manager.get_by_name(worker_id)
            
            if not worker_model:
                console.print(f"[red]Worker not found: {worker_id}[/red]")
                return
            
            status_text = colorize_status('active' if worker_model.is_active else 'inactive')
            
            items = [
                {'key': 'ID', 'value': worker_model.worker_id},
                {'key': 'Name', 'value': worker_model.worker_name},
                {'key': 'Type', 'value': worker_model.worker_type},
                {'key': 'Queue', 'value': worker_model.queue_name},
                {'key': 'Host', 'value': f"{worker_model.username}@{worker_model.host}:{worker_model.port}"},
                {'key': 'Remote workdir', 'value': worker_model.remote_workdir},
                {'key': 'Tasks per node', 'value': str(worker_model.ntasks_per_node)}
            ]
            
            if worker_model.gres:
                items.append({'key': 'GRES', 'value': worker_model.gres})
            if worker_model.gpus_per_task:
                items.append({'key': 'GPUs per task', 'value': str(worker_model.gpus_per_task)})
            
            items.extend([
                {'key': 'Status', 'value': status_text},
                {'key': 'Created', 'value': worker_model.created_at.strftime('%Y-%m-%d %H:%M:%S')}
            ])
            
            render_key_value_list(console, "Worker Information", items)
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("test", context_settings=CONTEXT_SETTINGS)
@click.argument("worker_id")
def worker_test(worker_id):
    """Test worker connection and configuration."""
    try:
        console.print(f"[bold blue]Testing worker: {worker_id}...[/bold blue]")
        
        db = get_db()
        try:
            manager = WorkerManager(db)
            success, message = manager.test_worker(worker_id)
            
            if success:
                console.print(f"[green]✓ {message}[/green]")
            else:
                console.print(f"[red]✗ {message}[/red]")
                raise click.Abort()
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("remove", context_settings=CONTEXT_SETTINGS)
@click.argument("worker_id")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation")
def worker_remove(worker_id, force):
    """Remove a worker."""
    try:
        if not force:
            if not click.confirm(f"Are you sure you want to remove worker '{worker_id}'?"):
                console.print("[yellow]Aborted[/yellow]")
                return
        
        db = get_db()
        try:
            manager = WorkerManager(db)
            success = manager.remove(worker_id)
            
            if success:
                console.print(f"[green]✓ Worker removed: {worker_id}[/green]")
            else:
                console.print(f"[red]Worker not found: {worker_id}[/red]")
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("enable", context_settings=CONTEXT_SETTINGS)
@click.argument("worker_id")
def worker_enable(worker_id):
    """Enable a worker."""
    try:
        db = get_db()
        try:
            manager = WorkerManager(db)
            success = manager.enable(worker_id)
            
            if success:
                console.print(f"[green]✓ Worker enabled: {worker_id}[/green]")
            else:
                console.print(f"[red]Worker not found: {worker_id}[/red]")
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@worker.command("disable", context_settings=CONTEXT_SETTINGS)
@click.argument("worker_id")
def worker_disable(worker_id):
    """Disable a worker."""
    try:
        db = get_db()
        try:
            manager = WorkerManager(db)
            success = manager.disable(worker_id)
            
            if success:
                console.print(f"[green]✓ Worker disabled: {worker_id}[/green]")
            else:
                console.print(f"[red]Worker not found: {worker_id}[/red]")
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@main.group(cls=JslingGroup, context_settings=CONTEXT_SETTINGS)
def config():
    """Manage system configurations."""
    pass


@config.command("list", context_settings=CONTEXT_SETTINGS)
def config_list():
    """List all configurations."""
    try:
        db = get_db()
        try:
            manager = ConfigManager(db)
            configs = manager.list_all()
            
            if not configs:
                console.print("[yellow]No configurations found[/yellow]")
                return
            
            columns = [
                {'header': 'Key', 'style': 'cyan'},
                {'header': 'Value', 'style': 'green'},
                {'header': 'Type', 'style': 'magenta'},
                {'header': 'Description'}
            ]
            
            rows = []
            for cfg in configs:
                value_display = cfg["value"][:50] + "..." if len(cfg["value"]) > 50 else cfg["value"]
                rows.append([
                    cfg["key"],
                    value_display,
                    cfg["type"],
                    cfg["description"]
                ])
            
            render_borderless_table(console, "Jsling Configurations", columns, rows)
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@config.command("get", context_settings=CONTEXT_SETTINGS)
@click.argument("key")
def config_get(key):
    """Get configuration value."""
    try:
        db = get_db()
        try:
            manager = ConfigManager(db)
            value = manager.get(key)
            
            if value is None:
                console.print(f"[yellow]Configuration key '{key}' not found[/yellow]")
            else:
                console.print(f"[bold]{key}:[/bold] {value}")
                
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@config.command("set", context_settings=CONTEXT_SETTINGS)
@click.argument("key")
@click.argument("value")
@click.option("--", "value_override", hidden=True)  # Handle negative numbers
def config_set(key, value, value_override):
    """Set configuration value."""
    try:
        # Use override if provided (for negative numbers)
        actual_value = value_override if value_override else value
        
        db = get_db()
        try:
            manager = ConfigManager(db)
            manager.set(key, actual_value)
            
            console.print(f"[green]✓ Set {key} = {actual_value}[/green]")
            
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


@config.command("reset", context_settings=CONTEXT_SETTINGS)
@click.argument("key")
def config_reset(key):
    """Reset configuration to default value."""
    try:
        db = get_db()
        try:
            manager = ConfigManager(db)
            success = manager.reset(key)
            
            if success:
                console.print(f"[green]✓ Reset {key} to default value[/green]")
            else:
                console.print(f"[yellow]No default value found for '{key}'[/yellow]")
                
        finally:
            db.close()
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        raise click.Abort()


# Register runner command group
main.add_command(runner)


if __name__ == "__main__":
    main()
