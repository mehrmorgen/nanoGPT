"""Compatibility wrapper for the legacy tools CLI import path.

This module preserves imports like `from ml_playground.tools import cli as tools_cli`
by re-exporting the canonical Typer app and console entry from
`ml_playground.tools.cli.main`.
"""

from __future__ import annotations

from ml_playground.tools.cli.main import app, main_entry

__all__ = ["app", "main_entry"]
