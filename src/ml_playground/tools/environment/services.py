"""Environment services functionality for environment tools."""

from __future__ import annotations

from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.errors import ToolExecutionError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


def run_ai_guidelines(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    tool: str,
    dry_run: bool,
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Set up AI guideline symlinks for the requested tool.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional arguments (ignored)
        tool: Target tool name for AI guidelines
        dry_run: Whether to preview actions without executing
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(
        namespace="tools", category="env", command="ai-guidelines"
    )

    if not tool.strip():
        raise ToolExecutionError(
            "Missing tool name for AI guidelines setup",
            reason="Tool name argument is required but was empty",
            rationale="AI guidelines setup requires a specific tool name to configure",
        )

    # Delegate to integrated implementation in DevTools (lazy import to avoid circular dependency)
    try:
        # Import here to avoid circular import
        from ml_playground.tools.dev.dev import DevTools  # type: ignore[import]

        dev = DevTools(
            config=config,
            root_path=root_path,
            subprocess_runner=subprocess_runner,
        )
        result = dev.setup_ai_guidelines(tool=tool, dry_run=dry_run)
        # Attach our operation_id for consistent CLI reporting
        result.operation_id = operation_id
        return result
    except Exception as exc:
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=f"Failed to setup AI guidelines: {exc}",
            operation_id=operation_id,
        )


def run_tensorboard(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    logdir: Path,
    port: int,
    host: str,
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Launch TensorBoard for the given log directory.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional tensorboard arguments
        logdir: TensorBoard log directory
        port: Port to bind TensorBoard to
        host: Host interface to bind to
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="tensorboard")

    if not logdir.exists():
        raise ToolExecutionError(
            f"TensorBoard log directory does not exist: {logdir}",
            reason="Specified logdir path is not accessible",
            rationale="TensorBoard requires an existing directory with log files",
        )

    if not logdir.is_dir():
        raise ToolExecutionError(
            f"TensorBoard logdir is not a directory: {logdir}",
            reason="Logdir path points to a file, not a directory",
            rationale="TensorBoard requires a directory containing log files",
        )

    # Build tensorboard command
    cmd = [
        "tensorboard",
        "--logdir",
        str(logdir),
        "--port",
        str(port),
        "--host",
        host,
    ]
    cmd.extend(args)

    return subprocess_runner.run_uv_command(
        cmd,
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )


def run_gguf_help(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    """Show llama.cpp GGUF conversion help.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional arguments (ignored)
        subprocess_runner: Subprocess runner

    Returns:
        ToolResult with execution details
    """
    operation_id = OperationId(namespace="tools", category="env", command="gguf-help")

    # Run the GGUF converter help
    result = subprocess_runner.run_uv_command(
        ["python", "tools/llama_cpp/convert-hf-to-gguf.py", "--help"],
        cwd=root_path,
        timeout=config.environment.timeout,
        operation_id=operation_id,
    )

    # GGUF converter may exit with non-zero status even for help
    # This is expected behavior, so we adjust the result
    if not result.success and "usage:" in result.stdout.lower():
        # Help was displayed successfully despite non-zero exit
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=result.stdout,
            stderr="GGUF converter help displayed (exit code adjusted)",
            operation_id=operation_id,
        )

    return result
