"""Unit testing functionality for testing tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_unit(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run unit tests.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional pytest arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(namespace="tools", category="test", command="unit")

    result = subprocess_runner.run_pytest_command(
        ["tests/unit", *args],
        cwd=root_path,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    # Clean pytest output
    if result.stdout:
        result.stdout = _clean_pytest_output(result.stdout)

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="unit",
            context="Running unit tests to verify individual components work correctly",
            category="test",
            executed_commands=[f"pytest tests/unit {' '.join(args)}".strip()],
        )

    return result


def run_regression(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run regression policy suites.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional pytest arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(namespace="tools", category="test", command="regression")

    result = subprocess_runner.run_pytest_command(
        ["tests/regression", *args],
        cwd=root_path,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    # Clean pytest output
    if result.stdout:
        result.stdout = _clean_pytest_output(result.stdout)

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="regression",
            context="Running regression guards for policy compliance",
            category="test",
            executed_commands=[f"pytest tests/regression {' '.join(args)}".strip()],
        )

    return result


def _clean_pytest_output(output: str) -> str:
    """Remove pytest progress lines and xdist status messages."""
    lines = output.splitlines()
    cleaned_lines: list[str] = []

    for line in lines:
        # Skip progress indicators and xdist status
        if any(
            skip in line
            for skip in [
                "test session starts",
                "[gw",
                "workers [",
                "scheduling",
                ".",
                "=",
                "PASSED",
                "FAILED",
                "ERROR",
                "warnings summary",
                "short test summary",
            ]
        ):
            continue
        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)
