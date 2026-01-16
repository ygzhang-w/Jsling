"""Tests for CLI rendering utilities including logo display."""

import pytest
from io import StringIO
from rich.console import Console

from jsling.utils.cli_render import (
    JSLING_LOGO,
    print_logo,
    colorize_status,
    get_status_color,
)


class TestJslingLogo:
    """Tests for Jsling ASCII art logo display."""
    
    def test_jsling_logo_constant_exists(self):
        """Test that the JSLING_LOGO constant is defined."""
        assert JSLING_LOGO is not None
        assert isinstance(JSLING_LOGO, str)
        assert len(JSLING_LOGO) > 0
    
    def test_print_logo(self):
        """Test that print_logo outputs the logo to console."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=80)
        
        print_logo(console)
        
        result = output.getvalue()
        # Check that output contains parts of the logo
        assert "____" in result
        assert "/" in result
    
    def test_print_logo_with_version(self):
        """Test that print_logo displays version when provided."""
        console = Console(record=True, force_terminal=False)
        
        print_logo(console, version="1.2.3")
        
        result = console.export_text()
        assert "Version: 1.2.3" in result
    
    def test_print_logo_with_custom_style(self):
        """Test that print_logo accepts custom style."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=80)
        
        # Should not raise any exceptions
        print_logo(console, style="bold green")
        
        result = output.getvalue()
        assert len(result) > 0


class TestStatusColors:
    """Tests for status colorization functions."""
    
    @pytest.mark.parametrize("status,expected_color", [
        ("pending", "yellow"),
        ("running", "cyan"),
        ("completed", "green"),
        ("failed", "red"),
        ("cancelled", "dim"),
        ("unknown", "magenta"),
        ("active", "green"),
        ("inactive", "dim"),
        ("down", "red"),
    ])
    def test_get_status_color(self, status, expected_color):
        """Test that get_status_color returns correct color for each status."""
        assert get_status_color(status) == expected_color
    
    def test_get_status_color_case_insensitive(self):
        """Test that status color lookup is case-insensitive."""
        assert get_status_color("PENDING") == "yellow"
        assert get_status_color("Running") == "cyan"
        assert get_status_color("COMPLETED") == "green"
    
    def test_get_status_color_unknown_status(self):
        """Test that unknown status returns default color."""
        assert get_status_color("nonexistent") == "white"
    
    def test_colorize_status(self):
        """Test that colorize_status wraps status with Rich markup."""
        result = colorize_status("running")
        assert "[cyan]running[/cyan]" == result
    
    def test_colorize_status_failed(self):
        """Test colorize_status with failed status."""
        result = colorize_status("failed")
        assert "[red]failed[/red]" == result
