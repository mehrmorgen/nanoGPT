"""Unit tests for subprocess utilities."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from ml_playground.tools.core.errors import TimeoutError, ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.utils.subprocess_utils import (
    format_command,
    run_pytest_command,
    run_subprocess,
    run_uv_command,
    validate_command_available,
)


class TestFormatCommand:
    """Test command formatting."""
    
    def test_format_simple_command(self) -> None:
        """Test formatting a simple command."""
        result = format_command(["echo", "hello"])
        assert result == "echo hello"
    
    def test_format_command_with_spaces(self) -> None:
        """Test formatting command with spaces in arguments."""
        result = format_command(["echo", "hello world", "test"])
        assert result == "echo 'hello world' test"
    
    def test_format_command_with_special_chars(self) -> None:
        """Test formatting command with special characters."""
        result = format_command(["echo", "$HOME", "test;rm -rf /"])
        assert result == "echo '$HOME' 'test;rm -rf /'"


class TestRunSubprocess:
    """Test subprocess execution."""
    
    def test_successful_execution(self) -> None:
        """Test successful command execution."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout="success output",
                stderr="",
            )
            
            result = run_subprocess(
                ["echo", "test"],
                operation_id=operation_id,
            )
            
            assert result.success is True
            assert result.exit_code == 0
            assert result.stdout == "success output"
            assert result.stderr == ""
            assert str(result.operation_id) == "tools.test.example"
    
    def test_failed_execution(self) -> None:
        """Test failed command execution."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=1,
                stdout="",
                stderr="error output",
            )
            
            result = run_subprocess(
                ["false"],
                operation_id=operation_id,
            )
            
            assert result.success is False
            assert result.exit_code == 1
            assert result.stdout == ""
            assert result.stderr == "error output"
    
    def test_timeout_handling(self) -> None:
        """Test timeout handling."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(["sleep", "10"], 5)
            
            with pytest.raises(TimeoutError) as exc_info:
                run_subprocess(
                    ["sleep", "10"],
                    timeout=5,
                    operation_id=operation_id,
                )
            
            assert "timed out after 5 seconds" in str(exc_info.value)
    
    def test_subprocess_error_handling(self) -> None:
        """Test subprocess error handling."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = OSError("Command not found")
            
            with pytest.raises(ToolExecutionError) as exc_info:
                run_subprocess(
                    ["nonexistent-command"],
                    operation_id=operation_id,
                )
            
            assert "Failed to execute command" in str(exc_info.value)
    
    def test_environment_variables(self) -> None:
        """Test environment variable handling."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            run_subprocess(
                ["env"],
                env={"TEST_VAR": "test_value"},
                operation_id=operation_id,
            )
            
            # Check that environment was passed correctly
            call_args = mock_run.call_args
            assert call_args[1]["env"]["TEST_VAR"] == "test_value"
    
    def test_working_directory(self) -> None:
        """Test working directory handling."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            test_cwd = Path("/tmp/test")
            run_subprocess(
                ["pwd"],
                cwd=test_cwd,
                operation_id=operation_id,
            )
            
            # Check that cwd was passed correctly
            call_args = mock_run.call_args
            assert call_args[1]["cwd"] == test_cwd


class TestRunUvCommand:
    """Test uv command execution."""
    
    def test_default_uv_command(self) -> None:
        """Test default uv command construction."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            run_uv_command(
                ["pytest", "--version"],
                operation_id=operation_id,
            )
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert call_args[:2] == ["uv", "run"]
            assert "--project" in call_args
            assert "pytest" in call_args
            assert "--version" in call_args
    
    def test_no_project_flag(self) -> None:
        """Test no-project flag."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            run_uv_command(
                ["python", "--version"],
                no_project=True,
                operation_id=operation_id,
            )
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "--no-project" in call_args
            assert "--project" not in call_args
    
    def test_python_version_flag(self) -> None:
        """Test python version flag."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            run_uv_command(
                ["python", "--version"],
                python="3.11",
                operation_id=operation_id,
            )
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "--python" in call_args
            assert "3.11" in call_args


class TestRunPytestCommand:
    """Test pytest command execution."""
    
    def test_pytest_base_args(self) -> None:
        """Test pytest base arguments are included."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            run_pytest_command(
                ["tests/unit"],
                operation_id=operation_id,
            )
            
            # Check command construction
            call_args = mock_run.call_args[0][0]
            assert "pytest" in call_args
            assert "-q" in call_args
            assert "-n" in call_args
            assert "auto" in call_args
            assert "--strict-markers" in call_args
            assert "--strict-config" in call_args
            assert "tests/unit" in call_args


class TestValidateCommandAvailable:
    """Test command availability validation."""
    
    def test_available_command(self) -> None:
        """Test detection of available command."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(returncode=0)
            
            result = validate_command_available("python")
            assert result is True
    
    def test_unavailable_command(self) -> None:
        """Test detection of unavailable command."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = OSError("Command not found")
            
            result = validate_command_available("nonexistent-command")
            assert result is False
    
    def test_timeout_command(self) -> None:
        """Test timeout handling in command validation."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(["slow-command"], 5)
            
            result = validate_command_available("slow-command")
            assert result is False