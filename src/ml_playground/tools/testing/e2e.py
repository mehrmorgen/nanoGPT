"""End-to-end testing functionality for testing tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_e2e(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run end-to-end tests.

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
    operation_id = OperationId(namespace="tools", category="test", command="e2e")

    result = subprocess_runner.run_pytest_command(
        ["tests/e2e", *args],
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
            command="e2e",
            context="Running end-to-end tests to verify complete user workflows",
            category="test",
            executed_commands=[f"pytest tests/e2e {' '.join(args)}".strip()],
        )

    return result


def run_acceptance(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run acceptance tests.

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
    operation_id = OperationId(namespace="tools", category="test", command="acceptance")

    result = subprocess_runner.run_pytest_command(
        ["tests/acceptance", *args],
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
            command="acceptance",
            context="Running acceptance tests to validate business requirements",
            category="test",
            executed_commands=[f"pytest tests/acceptance {' '.join(args)}".strip()],
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
