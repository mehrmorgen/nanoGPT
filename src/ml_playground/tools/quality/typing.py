"""Type checking functionality for quality tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner
from .mypy_guard import ensure_mypy_runtime_ready


def run_basedpyright(
    config: ToolsConfig,
    root_path: Path,
    pkg_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run BasedPyright type checks.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to check
        args: Additional basedpyright arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="quality", command="basedpyright"
    )

    basedpyright_args = ["basedpyright", str(pkg_path)]
    if args:
        basedpyright_args.extend(args)

    result = subprocess_runner.run_uv_command(
        basedpyright_args,
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="basedpyright",
            context="Performing static type checking using BasedPyright",
            category="quality",
            executed_commands=[
                f"{prefix} {' '.join(basedpyright_args)}"
                if prefix
                else " ".join(basedpyright_args)
            ],
        )

    return result


def run_mypy(
    config: ToolsConfig,
    root_path: Path,
    pkg_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run Mypy type checks.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to check
        args: Additional mypy arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(namespace="tools", category="quality", command="mypy")

    ensure_mypy_runtime_ready(root_path)

    mypy_args = ["mypy", "--incremental"]
    if args:
        mypy_args.extend(args)
    else:
        mypy_args.append(str(pkg_path))

    result = subprocess_runner.run_uv_command(
        mypy_args,
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="mypy",
            context="Performing static type checking using MyPy",
            category="quality",
            executed_commands=[
                f"{prefix} {' '.join(mypy_args)}" if prefix else " ".join(mypy_args)
            ],
        )

    return result


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
    """Run both BasedPyright and Mypy type checks.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to check
        args: Additional arguments (applied to both tools)
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="quality", command="typecheck"
    )

    # Run BasedPyright first
    basedpyright_result = run_basedpyright(
        config=config,
        root_path=root_path,
        pkg_path=pkg_path,
        args=args,
        subprocess_runner=subprocess_runner,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
    )

    # Run Mypy regardless of BasedPyright result
    mypy_result = run_mypy(
        config=config,
        root_path=root_path,
        pkg_path=pkg_path,
        args=args,
        subprocess_runner=subprocess_runner,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
    )

    # Combine results
    combined_stdout = ""
    if basedpyright_result.stdout:
        combined_stdout += f"BasedPyright:\n{basedpyright_result.stdout}\n"
    if mypy_result.stdout:
        combined_stdout += f"Mypy:\n{mypy_result.stdout}"

    combined_stderr = ""
    if basedpyright_result.stderr:
        combined_stderr += f"BasedPyright errors:\n{basedpyright_result.stderr}\n"
    if mypy_result.stderr:
        combined_stderr += f"Mypy errors:\n{mypy_result.stderr}"

    # Success only if both succeed
    success = basedpyright_result.success and mypy_result.success
    exit_code = (
        0 if success else (basedpyright_result.exit_code or mypy_result.exit_code)
    )

    result = ToolResult(
        success=success,
        exit_code=exit_code,
        stdout=combined_stdout,
        stderr=combined_stderr,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="typecheck",
            context="Running multiple type checkers for comprehensive analysis",
            category="quality",
            executed_commands=[
                (
                    f"{prefix} basedpyright {pkg_path} {' '.join(args)}".strip()
                    if prefix
                    else f"basedpyright {pkg_path} {' '.join(args)}".strip()
                ),
                (
                    f"{prefix} mypy --incremental {pkg_path} {' '.join(args)}".strip()
                    if prefix
                    else f"mypy --incremental {pkg_path} {' '.join(args)}".strip()
                ),
            ],
        )

    return result
