"""CLI rendering utilities for borderless list display."""

from typing import List, Dict, Any, Optional
from rich.console import Console


# Jsling ASCII Art Logo
JSLING_LOGO = r"""
         __
        / /    __
       / /___ / /_____  ____ 
      / /  __/ /_/ __ \/ __ `
  ___/ /__  / / / / / / /_/ /
 /____/____/_/_/_/ /_/\__, /
                     /____/
"""


def print_logo(console: Console, style: str = "bold cyan", version: str = None) -> None:
    """Print the Jsling ASCII art logo.
    
    Args:
        console: Rich console instance
        style: Rich style for the logo
        version: Optional version string to display below logo
    """
    console.print(f"[{style}]{JSLING_LOGO}[/{style}]")
    if version:
        console.print(f"[dim]Version: {version}[/dim]\n")


def render_borderless_table(
    console: Console,
    title: str,
    columns: List[Dict[str, Any]],
    rows: List[List[str]],
    show_count: bool = True
) -> None:
    """Render a borderless table with aligned columns.
    
    Args:
        console: Rich console instance
        title: Table title
        columns: List of column definitions with 'header', 'style' (optional)
        rows: List of row data (each row is a list of strings)
        show_count: Whether to show total count at the bottom
    """
    import re
    
    if not rows:
        console.print(f"[yellow]No data found[/yellow]")
        return
    
    # Print title
    console.print(f"\n[bold]{title}[/bold]")
    console.print("-" * len(title))
    console.print()
    
    # Calculate column widths
    headers = [col['header'] for col in columns]
    col_widths = [len(h) for h in headers]
    
    for row in rows:
        for i, cell in enumerate(row):
            # Strip Rich markup for width calculation
            plain_text = re.sub(r'\[.*?\]', '', str(cell))
            col_widths[i] = max(col_widths[i], len(plain_text))
    
    # Print header row
    header_parts = []
    for i, col in enumerate(columns):
        header_parts.append(headers[i].ljust(col_widths[i]))
    console.print("  ".join(header_parts))
    
    # Print separator
    separator_parts = ["-" * w for w in col_widths]
    console.print("  ".join(separator_parts))
    
    # Print data rows
    for row in rows:
        row_parts = []
        for i, cell in enumerate(row):
            cell_str = str(cell)
            
            # Apply color style if defined and cell doesn't have markup
            styled_cell = cell_str
            if i < len(columns) and 'style' in columns[i]:
                if not re.search(r'\[.*?\]', cell_str):
                    styled_cell = f"[{columns[i]['style']}]{cell_str}[/{columns[i]['style']}]"
            
            # Pad for alignment (considering Rich markup)
            plain_text = re.sub(r'\[.*?\]', '', styled_cell)
            padding = col_widths[i] - len(plain_text)
            row_parts.append(styled_cell + " " * padding)
        
        console.print("  ".join(row_parts))
    
    # Print count
    if show_count:
        console.print(f"\n[dim](Total: {len(rows)})[/dim]")


def render_key_value_list(
    console: Console,
    title: str,
    items: List[Dict[str, str]],
    indent: str = "  "
) -> None:
    """Render a list of key-value pairs.
    
    Args:
        console: Rich console instance
        title: Section title
        items: List of dicts with 'key' and 'value' (value can contain Rich markup)
        indent: Indentation string for values
    """
    console.print(f"\n[bold]{title}[/bold]")
    console.print("-" * len(title))
    console.print()
    
    for item in items:
        console.print(f"{indent}{item['key']}: {item['value']}")


def render_block_list(
    console: Console,
    title: str,
    blocks: List[Dict[str, Any]],
    show_count: bool = True
) -> None:
    """Render a list of data blocks (for detailed item display).
    
    Args:
        console: Rich console instance
        title: List title
        blocks: List of dicts, each representing one item block
        show_count: Whether to show total count at the bottom
    """
    if not blocks:
        console.print(f"[yellow]No data found[/yellow]")
        return
    
    console.print(f"\n[bold]{title}[/bold]")
    console.print("-" * len(title))
    console.print()
    
    for i, block in enumerate(blocks):
        if i > 0:
            console.print()  # Empty line between blocks
        
        # Print block header if exists
        if 'header' in block:
            console.print(f"[bold]{block['header']}[/bold]")
        
        # Print fields
        for field in block.get('fields', []):
            console.print(f"  {field['key']}: {field['value']}")
    
    if show_count:
        console.print(f"\n[dim](Total: {len(blocks)})[/dim]")


def get_status_color(status: str) -> str:
    """Get color for job/worker status.
    
    Args:
        status: Status string
        
    Returns:
        Rich color name
    """
    status_lower = status.lower()
    
    # Job status colors
    job_colors = {
        'queued': 'blue',           # Waiting for daemon to submit
        'pending': 'yellow',        # Submitted to Slurm, waiting for resources
        'running': 'cyan',          # Currently executing
        'completed': 'green',       # Finished successfully
        'failed': 'red',            # Execution failed
        'cancelled': 'dim',         # Cancelled by user
        'submission_failed': 'red bold',  # Failed to submit to cluster
    }
    
    # Worker status colors
    worker_colors = {
        'active': 'green',
        'inactive': 'dim',
        'down': 'red'
    }
    
    return job_colors.get(status_lower) or worker_colors.get(status_lower) or 'white'


def colorize_status(status: str) -> str:
    """Colorize a status string.
    
    Args:
        status: Status string
        
    Returns:
        Status string with Rich color markup
    """
    color = get_status_color(status)
    return f"[{color}]{status}[/{color}]"
