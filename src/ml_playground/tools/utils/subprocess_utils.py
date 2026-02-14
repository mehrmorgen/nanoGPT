"""Subprocess execution utilities for tool integration."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol, Union, cast

from ml_playground.tools.core.errors import ToolExecutionError, ToolTimeoutError
from ml_playground.tools.core.interfaces import ToolResult, OperationId


class SubprocessRunner(Protocol):
    """Protocol for subprocess execution."""

    def run_subprocess(
        self,
        command: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        """Execute a subprocess with proper error handling."""
        ...

    def run_uv_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        """Execute a uv command with proper configuration."""
        ...

    def run_pytest_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
    ) -> ToolResult:
        """Execute pytest with standard configuration."""
        ...


class RealSubprocessRunner:
    """Real subprocess runner implementation."""

    def run_subprocess(
        self,
        command: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        """Execute a subprocess with proper error handling and timeout support.

        Args:
            command: Command and arguments to execute
            cwd: Working directory for the process
            env: Environment variables to set/override
            timeout: Timeout in seconds (None for no timeout)
            operation_id: Operation identifier for the result
            capture_output: Whether to capture stdout/stderr

        Returns:
            ToolResult with execution details

        Raises:
            ToolTimeoutError: If the process times out
            ToolExecutionError: If the process fails
        """
        # Prepare environment
        run_env = os.environ.copy()
        if env:
            run_env.update(env)

        # Convert cwd to Path if string
        if isinstance(cwd, str):
            cwd = Path(cwd)

        dry_run_flag = os.environ.get("ML_PLAYGROUND_TOOLS_DRY_RUN")

        try:
            if dry_run_flag == "1":
                formatted = format_command(command)
                stdout = f"[dry-run] Command execution skipped.\n  $ {formatted}"
                result = ToolResult(
                    success=True,
                    exit_code=0,
                    stdout=stdout,
                    stderr="",
                    operation_id=operation_id,
                )
                result.learning_info.commands_executed.append(formatted)
                return result

            # Execute the process
            completed = subprocess.run(
                command,
                cwd=cwd,
                env=run_env,
                timeout=timeout,
                capture_output=capture_output,
                text=True,
            )

            # Create ToolResult
            return ToolResult(
                success=completed.returncode == 0,
                exit_code=completed.returncode,
                stdout=completed.stdout if capture_output else "",
                stderr=completed.stderr if capture_output else "",
                operation_id=operation_id,
            )

        except subprocess.TimeoutExpired as exc:
            raise ToolTimeoutError(
                f"Command '{format_command(command)}' timed out after {timeout} seconds",
                reason=f"Process exceeded timeout of {timeout} seconds",
                rationale="Timeouts prevent runaway processes and indicate environmental issues",
            ) from exc

        except (OSError, subprocess.SubprocessError) as exc:
            raise ToolExecutionError(
                f"Failed to execute command '{format_command(command)}'",
                reason=f"Subprocess execution failed: {exc}",
                rationale="External tool execution must succeed for development workflow to proceed",
            ) from exc

    def run_uv_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
        python: Optional[str] = None,
        no_project: bool = False,
    ) -> ToolResult:
        """Execute a uv command with proper configuration.

        Args:
            args: Arguments to pass to uv run
            cwd: Working directory for the process
            env: Environment variables to set/override
            timeout: Timeout in seconds
            operation_id: Operation identifier for the result
            python: Python version to use
            no_project: Whether to use --no-project flag

        Returns:
            ToolResult with execution details
        """
        # Build uv command
        command = ["uv", "run"]
        project_root = Path(cwd) if cwd is not None else Path.cwd()
        uv_env = dict(env) if env is not None else {}

        if no_project:
            command.append("--no-project")
        else:
            command.extend(["--project", str(project_root)])
            src_path = project_root / "src"
            if src_path.is_dir():
                # Ensure src-layout imports resolve deterministically for spawned tools.
                existing_pythonpath = uv_env.get("PYTHONPATH") or os.environ.get(
                    "PYTHONPATH", ""
                )
                src_entry = str(src_path)
                uv_env["PYTHONPATH"] = (
                    src_entry
                    if not existing_pythonpath
                    else f"{src_entry}{os.pathsep}{existing_pythonpath}"
                )

        if python:
            command.extend(["--python", python])

        command.extend(args)

        return self.run_subprocess(
            command,
            cwd=cwd,
            env=uv_env or env,
            timeout=timeout,
            operation_id=operation_id,
        )

    def run_pytest_command(
        self,
        args: List[str],
        *,
        cwd: Optional[Union[str, Path]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        operation_id: OperationId,
    ) -> ToolResult:
        """Execute pytest with standard configuration.

        Args:
            args: Arguments to pass to pytest
            cwd: Working directory for the process
            env: Environment variables to set/override
            timeout: Timeout in seconds
            operation_id: Operation identifier for the result

        Returns:
            ToolResult with execution details
        """
        # Standard pytest base arguments
        pytest_base = [
            "-n",
            "auto",
            "-W",
            "error",
            "--strict-markers",
            "--strict-config",
        ]

        return self.run_uv_command(
            ["pytest", *pytest_base, *args],
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
        )


def validate_command_available(command: str) -> bool:
    """Check if a command is available in the system PATH.

    Args:
        command: Command name to check

    Returns:
        True if command is available, False otherwise
    """
    try:
        subprocess.run(
            [command, "--version"],
            capture_output=True,
            timeout=5,
            check=False,
        )
        return True
    except (OSError, subprocess.TimeoutExpired):
        return False


def format_command(command: List[str]) -> str:
    """Format a command for display purposes."""
    return " ".join(shlex.quote(arg) for arg in command)


class _RunnerProxy:
    """Proxy that forwards to the current default runner.

    Tests patch `_default_runner` dynamically; this proxy keeps call sites in sync.
    """

    def __init__(self, getter: Callable[[], "SubprocessRunner"]) -> None:
        self._getter: Callable[[], SubprocessRunner] = getter

    def __getattr__(self, name: str) -> object:
        runner = self._getter()
        return cast(object, getattr(runner, name))


# Default global instance for tools that don't inject their own runner
_default_runner_instance = RealSubprocessRunner()
# Legacy/test shim: exposed for deterministic runner installation in tests
_default_runner: SubprocessRunner = _default_runner_instance

# Public alias for dependency injection; proxy keeps it in sync with _default_runner
DEFAULT_RUNNER: SubprocessRunner = cast(
    SubprocessRunner, _RunnerProxy(lambda: _default_runner)
)


def run_subprocess(
    command: List[str],
    *,
    cwd: Optional[Union[str, Path]] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    operation_id: OperationId,
    capture_output: bool = True,
) -> ToolResult:
    """Execute a subprocess with proper error handling and timeout support."""
    return DEFAULT_RUNNER.run_subprocess(
        command,
        cwd=cwd,
        env=env,
        timeout=timeout,
        operation_id=operation_id,
        capture_output=capture_output,
    )


def run_uv_command(
    args: List[str],
    *,
    cwd: Optional[Union[str, Path]] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    operation_id: OperationId,
    python: Optional[str] = None,
    no_project: bool = False,
) -> ToolResult:
    """Execute a uv command with proper configuration."""
    return DEFAULT_RUNNER.run_uv_command(
        args,
        cwd=cwd,
        env=env,
        timeout=timeout,
        operation_id=operation_id,
        python=python,
        no_project=no_project,
    )


def run_pytest_command(
    args: List[str],
    *,
    cwd: Optional[Union[str, Path]] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    operation_id: OperationId,
) -> ToolResult:
    """Execute pytest with standard configuration."""
    return DEFAULT_RUNNER.run_pytest_command(
        args,
        cwd=cwd,
        env=env,
        timeout=timeout,
        operation_id=operation_id,
    )
