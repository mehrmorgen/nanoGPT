"""Workflow status utilities for the tools CLI."""

from __future__ import annotations

from pathlib import Path

from ..core.config import ToolsConfig
from ..core.interfaces import ToolResult
from ..utils.subprocess_utils import SubprocessRunner

from .batch_review import run_batch_review
from .workflow_status import run_workflow_status


def run_dev_batch_review(
    config: ToolsConfig,
    root_path: Path,
    output_format: str,
    subprocess_runner: SubprocessRunner | None = None,
) -> ToolResult:
    """Run batch review from dev tools."""
    return run_batch_review(
        config=config,
        project_root_path=root_path,
        output_format=output_format,
        subprocess_runner=subprocess_runner,  # Allow DI for tests; defaults to real runner
    )


def run_dev_workflow_status(
    config: ToolsConfig,
    root_path: Path,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner | None = None,
) -> ToolResult:
    """Run workflow status from dev tools."""
    return run_workflow_status(
        config=config,
        project_root_path=root_path,
        output_format=output_format,
        subprocess_runner=subprocess_runner,  # Allow DI for tests; defaults to real runner
    )
