"""Unit tests for subprocess utilities."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Iterator, cast

import pytest

from ml_playground.tools.core.errors import ToolTimeoutError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
import ml_playground.tools.utils.subprocess_utils as subprocess_utils
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
    format_command,
    validate_command_available,
)
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_success_result,
    create_failure_result,
)


@pytest.fixture(autouse=True)
def reset_dry_run_env() -> Iterator[None]:
    original = os.environ.get("ML_PLAYGROUND_TOOLS_DRY_RUN")
    os.environ.pop("ML_PLAYGROUND_TOOLS_DRY_RUN", None)
    try:
        yield
    finally:
        if original is not None:
            os.environ["ML_PLAYGROUND_TOOLS_DRY_RUN"] = original


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
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
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
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
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
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
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
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
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

    def test_no_project_flag(self) -> None:
        """Test no-project flag."""
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
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
        operation_id = OperationId(
            namespace="tools", category="test", command="example"
        )
        runner = FakeSubprocessRunner()

        runner.run_uv_command(
            ["python", "--version"],
            python="3.11",
            operation_id=operation_id,
        )

        # Check command construction
        call_args = runner.calls[0]["command"]
        assert "--python" in call_args
        assert "3.11" in call_args


class TestRealSubprocessRunner:
    """Tests covering RealSubprocessRunner execution paths."""

    def test_run_subprocess_success(self) -> None:
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="real-success"
        )

        result = runner.run_subprocess(
            [sys.executable, "-c", "print('ok')"],
            operation_id=operation_id,
        )

        assert result.success is True
        assert result.exit_code == 0
        assert result.stdout.strip() == "ok"

    def test_run_subprocess_string_cwd_and_env_merge(self, tmp_path: Path) -> None:
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="real-env"
        )

        result = runner.run_subprocess(
            [sys.executable, "-c", "import os; print(os.environ.get('TEST_VAR', ''))"],
            cwd=str(tmp_path),
            env={"TEST_VAR": "abc"},
            operation_id=operation_id,
        )

        assert result.success is True
        assert result.stdout.strip() == "abc"

    def test_run_subprocess_without_capture(self) -> None:
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="real-no-capture"
        )

        result = runner.run_subprocess(
            [sys.executable, "-c", "print('ok')"],
            operation_id=operation_id,
            capture_output=False,
        )

        assert result.success is True
        assert result.stdout == ""
        assert result.stderr == ""

    def test_run_subprocess_timeout_raises(self) -> None:
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="real-timeout"
        )

        with pytest.raises(ToolTimeoutError):
            runner.run_subprocess(
                [sys.executable, "-c", "import time; time.sleep(1)"],
                operation_id=operation_id,
                timeout=0,
            )

    def test_run_subprocess_os_error_raises(self) -> None:
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="real-os-error"
        )

        with pytest.raises(subprocess_utils.ToolExecutionError):
            runner.run_subprocess(
                ["command-that-does-not-exist-xyz"],
                operation_id=operation_id,
            )

    def test_run_subprocess_dry_run(self) -> None:
        """Test dry run mode skips execution."""
        runner = RealSubprocessRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="dry-run"
        )

        os.environ["ML_PLAYGROUND_TOOLS_DRY_RUN"] = "1"
        try:
            result = runner.run_subprocess(
                ["echo", "should not run"],
                operation_id=operation_id,
            )
        finally:
            os.environ.pop("ML_PLAYGROUND_TOOLS_DRY_RUN", None)

        assert result.success is True
        assert "[dry-run]" in result.stdout
        assert "should not run" in result.stdout
        # Ideally verify it didn't run, but that's hard with RealSubprocessRunner unless we mock subprocess.run
        # The dry-run logic returns early, so subprocess.run shouldn't be called.

    def test_run_uv_command_includes_project(self, tmp_path: Path) -> None:
        class RecordingRunner(RealSubprocessRunner):
            def __init__(self) -> None:
                self.commands: list[list[str]] = []
                self.envs: list[dict[str, str] | None] = []

            def run_subprocess(
                self,
                command: list[str],
                *,
                cwd: str | Path | None = None,
                env: dict[str, str] | None = None,
                timeout: int | None = None,
                operation_id: OperationId,
                capture_output: bool = True,
            ) -> ToolResult:
                self.commands.append(command)
                self.envs.append(env)
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=operation_id,
                )

        runner = RecordingRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="uv-project"
        )
        (tmp_path / "src").mkdir()

        runner.run_uv_command(["echo", "hi"], cwd=tmp_path, operation_id=operation_id)

        recorded = runner.commands[0]
        assert recorded[:2] == ["uv", "run"]
        assert "--project" in recorded
        assert str(tmp_path) in recorded
        assert runner.envs[0] is not None
        assert runner.envs[0]["PYTHONPATH"].split(os.pathsep)[0] == str(
            tmp_path / "src"
        )

    def test_run_uv_command_no_project(self) -> None:
        class RecordingRunner(RealSubprocessRunner):
            def __init__(self) -> None:
                self.commands: list[list[str]] = []
                self.envs: list[dict[str, str] | None] = []

            def run_subprocess(
                self,
                command: list[str],
                *,
                cwd: str | Path | None = None,
                env: dict[str, str] | None = None,
                timeout: int | None = None,
                operation_id: OperationId,
                capture_output: bool = True,
            ) -> ToolResult:
                self.commands.append(command)
                self.envs.append(env)
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=operation_id,
                )

        runner = RecordingRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="uv-noproject"
        )

        runner.run_uv_command(
            ["echo", "hi"], no_project=True, operation_id=operation_id
        )

        recorded = runner.commands[0]
        assert "--no-project" in recorded
        assert "--project" not in recorded
        assert runner.envs[0] in ({}, None)

    def test_run_uv_command_python_flag(self) -> None:
        class RecordingRunner(RealSubprocessRunner):
            def __init__(self) -> None:
                self.commands: list[list[str]] = []

            def run_subprocess(
                self,
                command: list[str],
                *,
                cwd: str | Path | None = None,
                env: dict[str, str] | None = None,
                timeout: int | None = None,
                operation_id: OperationId,
                capture_output: bool = True,
            ) -> ToolResult:
                self.commands.append(command)
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=operation_id,
                )

        runner = RecordingRunner()
        operation_id = OperationId(
            namespace="tools", category="test", command="uv-python"
        )

        runner.run_uv_command(["echo"], python="3.12", operation_id=operation_id)

        recorded = runner.commands[0]
        assert "--python" in recorded
        assert "3.12" in recorded

    def test_run_pytest_command_builds_uv_invocation(self) -> None:
        class RecordingRunner(RealSubprocessRunner):
            def __init__(self) -> None:
                self.commands: list[list[str]] = []

            def run_subprocess(
                self,
                command: list[str],
                *,
                cwd: str | Path | None = None,
                env: dict[str, str] | None = None,
                timeout: int | None = None,
                operation_id: OperationId,
                capture_output: bool = True,
            ) -> ToolResult:
                self.commands.append(command)
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="",
                    stderr="",
                    operation_id=operation_id,
                )

        runner = RecordingRunner()
        operation_id = OperationId(namespace="tools", category="test", command="pytest")

        runner.run_pytest_command(["tests/unit"], operation_id=operation_id)

        recorded = runner.commands[0]
        assert recorded[:2] == ["uv", "run"]
        assert "pytest" in recorded
        assert "tests/unit" in recorded
        assert "-n" in recorded and "auto" in recorded


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


def test_subprocess_runner_protocol_stubs_execute() -> None:
    operation_id = OperationId(namespace="tools", category="test", command="proto")

    # We use cast(Any, ...) to bypass abstract usage checks for protocol stubs
    runner_cls = cast(Any, SubprocessRunner)
    obj = cast(Any, object())

    assert runner_cls.run_subprocess(obj, ["echo"], operation_id=operation_id) is None
    assert runner_cls.run_uv_command(obj, ["python"], operation_id=operation_id) is None
    assert (
        runner_cls.run_pytest_command(obj, ["tests/unit"], operation_id=operation_id)
        is None
    )
