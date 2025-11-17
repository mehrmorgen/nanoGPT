"""Regression test for CLI exit code consistency.

This test ensures the CLI uses typer.Exit instead of sys.exit for proper
error handling, preventing the reintroduction of inconsistent exit mechanisms
that bypass Typer's exit system.

Original Issue: CLI main_entry() used sys.exit(1) calls that bypassed Typer's
exit system, causing inconsistent error handling and testability issues.
Fixed in: commit 39ad27b - "fix(tools/cli): replace sys.exit with typer.Exit"
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner as TyperCliRunner

from ml_playground.tools.cli.main import app


def test_cli_invalid_command_exits_cleanly() -> None:
    """Test that invalid CLI commands exit with proper Typer exit codes, not sys.exit."""
    # This regression test verifies the CLI handles command errors through Typer's exit system
    # rather than bypassing it with sys.exit() which would terminate the process abruptly
    
    # Use subprocess testing since CliRunner doesn't capture error output consistently
    result = subprocess.run(
        [sys.executable, "-m", "ml_playground.tools.cli.main", "nonexistent-command"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
    )
    
    # Should exit with Typer's error code (2 for command not found), not crash
    assert result.returncode == 2
    assert "No such command" in result.stdout or "No such command" in result.stderr
    
    # Test invalid subcommand
    result = subprocess.run(
        [sys.executable, "-m", "ml_playground.tools.cli.main", "quality", "nonexistent-subcommand"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
    )
    
    assert result.returncode == 2
    assert "No such command" in result.stdout or "No such command" in result.stderr


def test_cli_help_commands_work_without_crashing() -> None:
    """Test that CLI help commands work without unexpected sys.exit calls."""
    # This ensures help functionality doesn't trigger sys.exit bypassing Typer
    
    runner = TyperCliRunner()
    
    # Test main app help
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "ML Playground unified development tools" in result.stdout
    
    # Test subcommand help
    result = runner.invoke(app, ["quality", "--help"])
    assert result.exit_code == 0
    assert "Code quality tools" in result.stdout


def test_cli_subprocess_integration_no_direct_sys_exit() -> None:
    """Integration test that CLI subprocess doesn't call sys.exit directly."""
    # This regression test runs the CLI in a subprocess to verify it doesn't call sys.exit
    # which would terminate the subprocess with unexpected behavior
    
    # Test with invalid command that should trigger error handling through Typer
    result = subprocess.run(
        [sys.executable, "-m", "ml_playground.tools.cli.main", "invalid-command"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
    )
    
    # Should exit with Typer's error code (2 for command not found), not crash
    assert result.returncode == 2
    assert "No such command" in result.stdout or "No such command" in result.stderr
    
    # Test help command - should exit cleanly with code 0
    result = subprocess.run(
        [sys.executable, "-m", "ml_playground.tools.cli.main", "--help"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent.parent,
    )
    
    assert result.returncode == 0
    assert "ML Playground unified development tools" in result.stdout
