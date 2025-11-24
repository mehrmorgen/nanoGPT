"""Integration testing functionality for testing tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_integration(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run integration tests.

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
    operation_id = OperationId(
        namespace="tools", category="test", command="integration"
    )

    result = subprocess_runner.run_pytest_command(
        ["-m", "integration", "--no-cov", *args],
        cwd=root_path,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="integration",
            context="Running integration tests to verify components work together correctly",
            category="test",
            executed_commands=[
                f"pytest -m integration --no-cov {' '.join(args)}".strip()
            ],
        )

    return result
