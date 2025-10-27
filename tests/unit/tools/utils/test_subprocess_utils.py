"""Unit tests for subprocess utilities."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    format_command,
    validate_command_available,
)
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result, create_failure_result


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


class TestFakeSubprocessRunner:
    """Test fake subprocess runner."""
    
    def test_successful_execution(self) -> None:
        """Test successful command execution with fake."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        expected_result = create_success_result(operation_id, "success output")
        runner.set_results([expected_result])
        
        result = runner.run_subprocess(
            ["echo", "test"],
            operation_id=operation_id,
        )
        
        assert result.success is True
        assert result.exit_code == 0
        assert result.stdout == "success output"
        assert result.stderr == ""
        assert str(result.operation_id) == "tools.test.example"
        
        # Verify call was recorded
        assert len(runner.calls) == 1
        assert runner.calls[0]["command"] == ["echo", "test"]
    
    def test_failed_execution(self) -> None:
        """Test failed command execution with fake."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        expected_result = create_failure_result(operation_id, 1, "", "error output")
        runner.set_results([expected_result])
        
        result = runner.run_subprocess(
            ["false"],
            operation_id=operation_id,
        )
        
        assert result.success is False
        assert result.exit_code == 1
        assert result.stdout == ""
        assert result.stderr == "error output"
    
    def test_environment_variables(self) -> None:
        """Test environment variable handling with fake."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        runner.run_subprocess(
            ["env"],
            env={"TEST_VAR": "test_value"},
            operation_id=operation_id,
        )
        
        # Check that environment was recorded correctly
        assert len(runner.calls) == 1
        assert runner.calls[0]["env"]["TEST_VAR"] == "test_value"
    
    def test_working_directory(self) -> None:
        """Test working directory handling with fake."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        test_cwd = Path("/tmp/test")
        runner.run_subprocess(
            ["pwd"],
            cwd=test_cwd,
            operation_id=operation_id,
        )
        
        # Check that cwd was recorded correctly
        assert len(runner.calls) == 1
        assert runner.calls[0]["cwd"] == test_cwd


class TestUvCommand:
    """Test uv command execution with fake."""
    
    def test_default_uv_command(self) -> None:
        """Test default uv command construction."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        runner.run_uv_command(
            ["pytest", "--version"],
            operation_id=operation_id,
        )
        
        # Check command construction
        assert len(runner.calls) == 1
        call_args = runner.calls[0]["command"]
        assert call_args[:2] == ["uv", "run"]
        assert "--project" in call_args
        assert "pytest" in call_args
        assert "--version" in call_args
    
    def test_no_project_flag(self) -> None:
        """Test no-project flag."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        runner.run_uv_command(
            ["python", "--version"],
            no_project=True,
            operation_id=operation_id,
        )
        
        # Check command construction
        assert len(runner.calls) == 1
        call_args = runner.calls[0]["command"]
        assert "--no-project" in call_args
        assert "--project" not in call_args
    
    def test_python_version_flag(self) -> None:
        """Test python version flag."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        runner.run_uv_command(
            ["python", "--version"],
            python="3.11",
            operation_id=operation_id,
        )
        
        # Check command construction
        assert len(runner.calls) == 1
        call_args = runner.calls[0]["command"]
        assert "--python" in call_args
        assert "3.11" in call_args


class TestPytestCommand:
    """Test pytest command execution with fake."""
    
    def test_pytest_base_args(self) -> None:
        """Test pytest base arguments are included."""
        operation_id = OperationId(namespace="tools", category="test", command="example")
        runner = FakeSubprocessRunner()
        
        runner.run_pytest_command(
            ["tests/unit"],
            operation_id=operation_id,
        )
        
        # Check command construction
        assert len(runner.calls) == 1
        call_args = runner.calls[0]["command"]
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
        # This function uses real subprocess, so we test with a known command
        result = validate_command_available("python")
        assert result is True
    
    def test_unavailable_command(self) -> None:
        """Test detection of unavailable command."""
        result = validate_command_available("nonexistent-command-12345")
        assert result is False