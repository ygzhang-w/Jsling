"""Runner commands for Jsling - manage background daemon process."""

import click
from rich.console import Console
from rich.table import Table

from jsling.core.runner_daemon import (
    start_daemon, stop_daemon, restart_daemon,
    get_daemon_status, RunnerDaemon, get_log_file
)
from jsling.utils.cli_render import print_logo
from jsling import __version__

console = Console()

# Context settings to enable -h as help shortcut
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


class JslingGroup(click.Group):
    """Custom Click Group that displays logo in help."""
    
    def format_help(self, ctx, formatter):
        """Write the help into the formatter if it exists."""
        # Print logo before help text
        print_logo(console, version=__version__)
        super().format_help(ctx, formatter)


@click.group(cls=JslingGroup, context_settings=CONTEXT_SETTINGS)
def runner():
    """Manage background runner daemon.
    
    The runner daemon is a background process that handles:
    - Job status polling via sentinel files
    - file synchronization
    
    Use 'jsling runner start' to start the daemon before submitting jobs.
    """
    pass


@runner.command("start", context_settings=CONTEXT_SETTINGS)
@click.option("--foreground", "-f", is_flag=True, 
              help="Run in foreground (for debugging)")
def runner_start(foreground):
    """Start the background runner daemon.
    
    The daemon will run in the background and monitor all active jobs,
    handling file synchronization automatically.
    
    Examples:
        jsling runner start          # Start as daemon
        jsling runner start -f       # Run in foreground (for debugging)
    """
    status = get_daemon_status()
    
    if status["running"]:
        console.print(f"[yellow]Runner is already running (PID: {status['pid']})[/yellow]")
        return
    
    if foreground:
        console.print("[bold blue]Starting runner in foreground mode...[/bold blue]")
        console.print("[dim]Press Ctrl+C to stop[/dim]")
        try:
            start_daemon(foreground=True)
        except KeyboardInterrupt:
            console.print("\n[yellow]Runner stopped by user[/yellow]")
    else:
        console.print("[bold blue]Starting runner daemon...[/bold blue]")
        
        import os
        import sys
        
        # Fork and start daemon in child process
        pid = os.fork()
        if pid == 0:
            # Child process
            try:
                start_daemon(foreground=False)
            except Exception as e:
                sys.exit(1)
            sys.exit(0)
        else:
            # Parent process - wait a moment and check status
            import time
            time.sleep(2)
            
            new_status = get_daemon_status()
            if new_status["running"]:
                console.print(f"[green]✓ Runner started successfully (PID: {new_status['pid']})[/green]")
                console.print(f"  Log file: {new_status['log_file']}")
            else:
                console.print("[red]✗ Failed to start runner[/red]")
                console.print(f"  Check log file: {status['log_file']}")


@runner.command("stop", context_settings=CONTEXT_SETTINGS)
def runner_stop():
    """Stop the background runner daemon.
    
    This will gracefully stop the daemon and all file synchronization tasks.
    
    Examples:
        jsling runner stop
    """
    status = get_daemon_status()
    
    if not status["running"]:
        console.print("[yellow]Runner is not running[/yellow]")
        return
    
    console.print(f"[bold blue]Stopping runner (PID: {status['pid']})...[/bold blue]")
    
    if stop_daemon():
        console.print("[green]✓ Runner stopped successfully[/green]")
    else:
        console.print("[red]✗ Failed to stop runner[/red]")


@runner.command("restart", context_settings=CONTEXT_SETTINGS)
@click.option("--foreground", "-f", is_flag=True, 
              help="Run in foreground after restart (for debugging)")
def runner_restart(foreground):
    """Restart the background runner daemon.
    
    This will stop the running daemon and start a new one.
    
    Examples:
        jsling runner restart
    """
    console.print("[bold blue]Restarting runner daemon...[/bold blue]")
    
    # First stop if running
    status = get_daemon_status()
    if status["running"]:
        console.print(f"Stopping current runner (PID: {status['pid']})...")
        stop_daemon()
        import time
        time.sleep(2)
    
    # Then start
    if foreground:
        console.print("[dim]Starting in foreground mode. Press Ctrl+C to stop[/dim]")
        try:
            start_daemon(foreground=True)
        except KeyboardInterrupt:
            console.print("\n[yellow]Runner stopped by user[/yellow]")
    else:
        import os
        import sys
        
        pid = os.fork()
        if pid == 0:
            try:
                start_daemon(foreground=False)
            except Exception:
                sys.exit(1)
            sys.exit(0)
        else:
            import time
            time.sleep(2)
            
            new_status = get_daemon_status()
            if new_status["running"]:
                console.print(f"[green]✓ Runner restarted successfully (PID: {new_status['pid']})[/green]")
            else:
                console.print("[red]✗ Failed to restart runner[/red]")


@runner.command("status", context_settings=CONTEXT_SETTINGS)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
def runner_status(as_json):
    """Show runner daemon status.
    
    Displays information about the running daemon including:
    - Running state and PID
    - Log file location and size
    - Last activity time
    
    Examples:
        jsling runner status
        jsling runner status --json
    """
    status = get_daemon_status()
    
    if as_json:
        import json
        console.print(json.dumps(status, indent=2))
        return
    
    # Create status table
    table = Table(title="Runner Status")
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="green")
    
    if status["running"]:
        table.add_row("Status", "[bold green]Running[/bold green]")
        table.add_row("PID", str(status["pid"]))
    else:
        table.add_row("Status", "[bold red]Stopped[/bold red]")
        table.add_row("PID", "-")
    
    table.add_row("PID File", status["pid_file"])
    table.add_row("Log File", status["log_file"])
    
    if "log_size" in status:
        size_kb = status["log_size"] / 1024
        if size_kb > 1024:
            size_str = f"{size_kb/1024:.1f} MB"
        else:
            size_str = f"{size_kb:.1f} KB"
        table.add_row("Log Size", size_str)
    
    if "log_modified" in status:
        table.add_row("Last Activity", status["log_modified"])
    
    console.print(table)
    
    if not status["running"]:
        console.print("\n[dim]Use 'jsling runner start' to start the runner[/dim]")


@runner.command("logs", context_settings=CONTEXT_SETTINGS)
@click.option("--lines", "-n", default=50, help="Number of lines to show")
@click.option("--follow", "-f", is_flag=True, help="Follow log output")
def runner_logs(lines, follow):
    """View runner daemon logs.
    
    Shows recent log entries from the runner daemon.
    
    Examples:
        jsling runner logs              # Show last 50 lines
        jsling runner logs -n 100       # Show last 100 lines
        jsling runner logs -f           # Follow log output (like tail -f)
    """
    log_file = get_log_file()
    
    if not log_file.exists():
        console.print("[yellow]No log file found. Runner may not have been started yet.[/yellow]")
        return
    
    if follow:
        console.print(f"[dim]Following {log_file}... (Ctrl+C to stop)[/dim]\n")
        import subprocess
        try:
            subprocess.run(["tail", "-f", str(log_file)])
        except KeyboardInterrupt:
            console.print("\n[dim]Stopped following logs[/dim]")
    else:
        import subprocess
        result = subprocess.run(
            ["tail", "-n", str(lines), str(log_file)],
            capture_output=True,
            text=True
        )
        if result.stdout:
            console.print(f"[dim]Last {lines} lines from {log_file}:[/dim]\n")
            console.print(result.stdout)
        else:
            console.print("[yellow]Log file is empty[/yellow]")
