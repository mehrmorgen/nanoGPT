"""Formatting functionality for quality tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_format(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Format code with Ruff.

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
    operation_id = OperationId(namespace="tools", category="quality", command="format")

    # Run both check --fix and format
    # First, run check with --fix
    check_result = subprocess_runner.run_uv_command(
        ["ruff", "check", "--fix", ".", *args],
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if not check_result.success:
        if learning_mode:
            learning_engine = LearningModeEngine()
            learning_engine.verbosity = VerbosityLevel(verbosity_level)
            check_result.learning_info = learning_engine.explain_command(
                command="format",
                context="Automatically formatting code to match style standards",
                category="quality",
                executed_commands=[f"ruff check --fix . {' '.join(args)}".strip()],
            )
        return check_result

    # Then run format
    format_result = subprocess_runner.run_uv_command(
        ["ruff", "format", ".", *args],
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    # Combine outputs
    combined_stdout = ""
    if check_result.stdout:
        combined_stdout += f"Ruff check --fix:\n{check_result.stdout}\n"
    if format_result.stdout:
        combined_stdout += f"Ruff format:\n{format_result.stdout}"

    combined_stderr = ""
    if check_result.stderr:
        combined_stderr += f"Ruff check --fix errors:\n{check_result.stderr}\n"
    if format_result.stderr:
        combined_stderr += f"Ruff format errors:\n{format_result.stderr}"

    result = ToolResult(
        success=format_result.success,
        exit_code=format_result.exit_code,
        stdout=combined_stdout,
        stderr=combined_stderr,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="format",
            context="Automatically formatting code to match style standards",
            category="quality",
            executed_commands=[
                (
                    f"{prefix} ruff check --fix . {' '.join(args)}".strip()
                    if prefix
                    else f"ruff check --fix . {' '.join(args)}".strip()
                ),
                (
                    f"{prefix} ruff format . {' '.join(args)}".strip()
                    if prefix
                    else f"ruff format . {' '.join(args)}".strip()
                ),
            ],
        )

    return result
