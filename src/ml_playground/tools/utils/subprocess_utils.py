"""Subprocess execution utilities for tool integration."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Protocol, Union

from ml_playground.tools.core.errors import ToolExecutionError, TimeoutError
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
            TimeoutError: If the process times out
            ToolExecutionError: If the process fails
        """
        # Prepare environment
        run_env = os.environ.copy()
        if env:
            run_env.update(env)
        
        # Convert cwd to Path if string
        if isinstance(cwd, str):
            cwd = Path(cwd)
        
        try:
            # Execute the process
            result = subprocess.run(
                command,
                cwd=cwd,
                env=run_env,
                timeout=timeout,
                capture_output=capture_output,
                text=True,
            )
            
            # Create ToolResult
            return ToolResult(
                success=result.returncode == 0,
                exit_code=result.returncode,
                stdout=result.stdout if capture_output else "",
                stderr=result.stderr if capture_output else "",
                operation_id=operation_id,
            )
            
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(
                f"Command '{format_command(command)}' timed out after {timeout} seconds",
                reason=f"Process exceeded timeout of {timeout} seconds",
                rationale="Timeouts prevent runaway processes and indicate environmental issues"
            ) from exc
            
        except (OSError, subprocess.SubprocessError) as exc:
            raise ToolExecutionError(
                f"Failed to execute command '{format_command(command)}'",
                reason=f"Subprocess execution failed: {exc}",
                rationale="External tool execution must succeed for development workflow to proceed"
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
        
        if no_project:
            command.append("--no-project")
        else:
            # Default to project root if cwd not specified
            project_root = cwd or Path.cwd()
            command.extend(["--project", str(project_root)])
        
        if python:
            command.extend(["--python", python])
        
        command.extend(args)
        
        return self.run_subprocess(
            command,
            cwd=cwd,
            env=env,
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
        pytest_base = ["-q", "-n", "auto", "-W", "error", "--strict-markers", "--strict-config"]
        
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


# Global instance for backward compatibility
_default_runner = RealSubprocessRunner()


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
    return _default_runner.run_subprocess(
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
    return _default_runner.run_uv_command(
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
    return _default_runner.run_pytest_command(
        args,
        cwd=cwd,
        env=env,
        timeout=timeout,
        operation_id=operation_id,
    )