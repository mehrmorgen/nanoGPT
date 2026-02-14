"""Dead code analysis functionality for quality tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def run_deadcode(
    config: ToolsConfig,
    root_path: Path,
    pkg_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Scan for dead code using vulture.

    Args:
        config: Tool configuration
        root_path: Project root path
        pkg_path: Package path to scan
        args: Additional vulture arguments
        subprocess_runner: Subprocess runner
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="quality", command="deadcode"
    )

    vulture_args = ["vulture", str(pkg_path), "--min-confidence", "90"]
    if args:
        vulture_args.extend(args)

    result = subprocess_runner.run_uv_command(
        vulture_args,
        cwd=root_path,
        timeout=config.quality.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        prefix = config.display_command_prefix
        result.learning_info = learning_engine.explain_command(
            command="deadcode",
            context="Scanning for unused code that can be safely removed",
            category="quality",
            executed_commands=[
                f"{prefix} {' '.join(vulture_args)}"
                if prefix
                else " ".join(vulture_args)
            ],
        )

    return result
