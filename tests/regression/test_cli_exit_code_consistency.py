"""Regression test for CLI exit code consistency.

This test ensures sys.exit() is never used in the codebase, making any usage
a regression that violates the CLI exit code consistency policy.

Original Issue: CLI main_entry() used sys.exit(1) calls that bypassed Typer's
exit system, causing inconsistent error handling and testability issues.
Fixed in: commit 39ad27b - "fix(tools/cli): replace sys.exit with typer.Exit"
Policy: All CLI error handling must use typer.Exit() instead of sys.exit()
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, cast

import pytest


def test_no_sys_exit_usage_in_codebase() -> None:
    """Static analysis test that sys.exit() is never used in the codebase."""
    # This regression test ensures sys.exit() is never reintroduced anywhere
    # in the source code, as it would bypass Typer's exit system

    # Search for sys.exit patterns in all Python source files
    src_dir = Path(__file__).parent.parent.parent / "src"

    # Use grep to find any sys.exit usage
    result = subprocess.run(
        ["grep", "-r", "--include=*.py", "sys.exit", str(src_dir)],
        capture_output=True,
        text=True,
        cwd=src_dir.parent,
    )

    # Should find no sys.exit usage in the codebase
    if result.returncode == 0:
        # Found sys.exit usage - this is a regression
        found_lines = result.stdout.strip().split("\n")
        cast(Any, pytest).fail(
            f"Found sys.exit() usage in codebase (regression detected):\n"
            f"{chr(10).join(found_lines)}\n\n"
            "Use typer.Exit() instead of sys.exit() for CLI error handling."
        )

    # If grep returns 1, no matches found - this is the expected outcome
