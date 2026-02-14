"""Linting functionality for quality tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_lint(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run Ruff lint checks.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional ruff arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(namespace="tools", category="quality", command="lint")

    # Default to check mode, allow args to override
    ruff_args = ["ruff", "check", "."]
    if args:
        # Replace default args if user provides custom ones
        ruff_args = ["ruff", *args]

    result = subprocess_runner.run_uv_command(
        ruff_args,
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="lint",
            context="Analyzing code for style violations and potential bugs",
            category="quality",
            executed_commands=[
                f"{prefix} {' '.join(ruff_args)}" if prefix else " ".join(ruff_args)
            ],
        )

    return result


def run_lint_check(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run Ruff in check-only mode (alias for lint).

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional ruff arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details
    """
    # This is an alias for lint command
    return run_lint(
        config=config,
        root_path=root_path,
        args=args,
        subprocess_runner=subprocess_runner,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
    )
