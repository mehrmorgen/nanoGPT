"""Workflow status utilities for the tools CLI."""

from __future__ import annotations

from pathlib import Path

from ..core.config import ToolsConfig
from ..core.interfaces import ToolResult

from .batch_review import run_batch_review
from .workflow_status import run_workflow_status


def run_dev_batch_review(
    config: ToolsConfig,
    root_path: Path,
    output_format: str,
) -> ToolResult:
    """Run batch review from dev tools."""
    return run_batch_review(
        config=config,
        project_root_path=root_path,
        output_format=output_format,
        subprocess_runner=None,  # Will use default
    )


def run_dev_workflow_status(
    config: ToolsConfig,
    root_path: Path,
    output_format: str,
) -> ToolResult:
    """Run workflow status from dev tools."""
    return run_workflow_status(
        config=config,
        project_root_path=root_path,
        output_format=output_format,
        subprocess_runner=None,  # Will use default
    )
