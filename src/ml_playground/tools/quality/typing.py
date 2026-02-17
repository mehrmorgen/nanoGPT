"""Type checking functionality for quality tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner

_STRICT_WARNING_KINDS = [
    "redundant-cast",
    "redundant-condition",
    "unnecessary-comparison",
    "unreachable",
    "unused-ignore",
    "deprecated",
]


def run_typecheck(
    config: ToolsConfig,
    root_path: Path,
    pkg_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run strict static type checks.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to check
        args: Additional typecheck arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="quality", command="typecheck"
    )

    pyrefly_args = ["pyrefly", "check", str(pkg_path)]
    for kind in _STRICT_WARNING_KINDS:
        pyrefly_args.extend(["--error", kind])
    if args:
        pyrefly_args.extend(args)

    result = subprocess_runner.run_uv_command(
        pyrefly_args,
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="typecheck",
            context="Performing strict static type checking",
            category="quality",
            executed_commands=[
                f"{prefix} {' '.join(pyrefly_args)}"
                if prefix
                else " ".join(pyrefly_args)
            ],
        )

    return result


def run_typecheck_summary(
    config: ToolsConfig,
    root_path: Path,
    pkg_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run strict type checks and normalize output operation metadata.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to check
        args: Additional arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="quality", command="typecheck"
    )

    pyrefly_result = run_typecheck(
        config=config,
        root_path=root_path,
        pkg_path=pkg_path,
        args=args,
        subprocess_runner=subprocess_runner,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
    )

    result = ToolResult(
        success=pyrefly_result.success,
        exit_code=pyrefly_result.exit_code,
        stdout=pyrefly_result.stdout,
        stderr=pyrefly_result.stderr,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="typecheck",
            context="Running Pyrefly for strict static type checking",
            category="quality",
            executed_commands=[
                f"{prefix} pyrefly check {pkg_path} {' '.join(args)}".strip()
                if prefix
                else f"pyrefly check {pkg_path} {' '.join(args)}".strip(),
            ],
        )

    return result
