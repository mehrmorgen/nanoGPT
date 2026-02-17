"""Comprehensive tests for `QualityTools`."""

from __future__ import annotations

from pathlib import Path

import pytest

import ml_playground.tools.quality.quality as quality_module
import ml_playground.tools.core.config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_failure_result,
    create_success_result,
)


@pytest.fixture()
def config() -> ToolsConfig:
    return ToolsConfig(
        quality=config_module.QualityToolsConfig(
            timeout=120,
            ruff_config_path=Path("pyproject.toml"),
        )
    )


@pytest.fixture()
def root_path(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture()
def subprocess_runner() -> FakeSubprocessRunner:
    return FakeSubprocessRunner()


@pytest.fixture()
def quality_tools(
    config: ToolsConfig, root_path: Path, subprocess_runner: FakeSubprocessRunner
) -> quality_module.QualityTools:
    return quality_module.QualityTools(config, root_path, subprocess_runner)


class TestLint:
    def test_success(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        subprocess_runner.set_results([create_success_result(operation_id, "lint ok")])

        result = quality_tools.lint([])

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        assert command[0:2] == ["uv", "run"]
        assert "ruff" in command

    def test_custom_args(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        subprocess_runner.set_results([create_success_result(operation_id)])

        quality_tools.lint(["check", "--fix", "src/"])

        command = subprocess_runner.calls[0]["command"]
        assert "--fix" in command
        assert "src/" in command

    def test_failure(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        subprocess_runner.set_results(
            [create_failure_result(operation_id, 1, stderr="lint fail")]
        )

        result = quality_tools.lint([])

        assert result.success is False
        assert "lint fail" in result.stderr

    def test_learning_mode(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        subprocess_runner.set_results([create_success_result(operation_id)])

        result = quality_tools.lint([], learning_mode=True, verbosity_level=2)

        assert result.learning_info.commands_executed
        assert "ruff" in result.learning_info.commands_executed[0]

    def test_lint_check_alias(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        subprocess_runner.set_results([create_success_result(operation_id, "lint ok")])

        result = quality_tools.lint_check([])

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        assert command[0:2] == ["uv", "run"]
        assert "ruff" in command


class TestFormat:
    def test_success(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [
                create_success_result(operation_id, "check"),
                create_success_result(operation_id, "fmt"),
            ]
        )

        result = quality_tools.format([])

        assert result.success is True
        assert "Ruff check --fix:" in result.stdout
        assert "Ruff format:" in result.stdout

    def test_check_failure(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [create_failure_result(operation_id, 1, stderr="check err")]
        )

        result = quality_tools.format([])

        assert result.success is False
        assert "check err" in result.stderr

    def test_check_failure_learning_mode(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [create_failure_result(operation_id, 1, stderr="check err")]
        )

        result = quality_tools.format([], learning_mode=True, verbosity_level=2)

        assert result.success is False
        assert result.learning_info is not None
        assert result.learning_info.commands_executed

    def test_success_includes_check_stderr(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [
                create_success_result(operation_id, stderr="check warn"),
                create_success_result(operation_id, "fmt ok"),
            ]
        )

        result = quality_tools.format([])

        assert result.success is True
        assert "Ruff check --fix errors:" in result.stderr
        assert "check warn" in result.stderr

    def test_format_failure(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [
                create_success_result(operation_id, "ok"),
                create_failure_result(operation_id, 1, stderr="fmt err"),
            ]
        )

        result = quality_tools.format([])

        assert result.success is False
        assert "fmt err" in result.stderr

    def test_learning_mode(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        subprocess_runner.set_results(
            [create_success_result(operation_id), create_success_result(operation_id)]
        )

        result = quality_tools.format([], learning_mode=True, verbosity_level=1)

        assert result.learning_info.commands_executed


class TestTypecheck:
    def test_success(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        subprocess_runner.set_results(
            [create_success_result(operation_id, "pyrefly ok")]
        )

        result = quality_tools.typecheck([])

        assert result.success is True
        assert "pyrefly ok" in result.stdout

    def test_failure(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        subprocess_runner.set_results(
            [create_failure_result(operation_id, 1, stderr="pyrefly err")]
        )

        result = quality_tools.typecheck([])

        assert result.success is False
        assert "pyrefly err" in result.stderr

    def test_failure_exit_code(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        subprocess_runner.set_results(
            [create_failure_result(operation_id, 2, stderr="pyrefly err")]
        )

        result = quality_tools.typecheck([])

        assert result.success is False
        assert result.exit_code == 2
        assert "pyrefly err" in result.stderr

    def test_learning_mode(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        subprocess_runner.set_results([create_success_result(operation_id)])

        result = quality_tools.typecheck([], learning_mode=True, verbosity_level=2)

        assert result.learning_info.commands_executed
        assert "pyrefly" in result.learning_info.commands_executed[0]


class TestDeadcode:
    def test_success(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        subprocess_runner.set_results(
            [create_success_result(operation_id, "deadcode ok")]
        )

        result = quality_tools.deadcode([])

        assert result.success is True
        command = subprocess_runner.calls[0]["command"]
        assert "vulture" in command

    def test_learning_mode(self, quality_tools, subprocess_runner):
        operation_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        subprocess_runner.set_results([create_success_result(operation_id)])

        result = quality_tools.deadcode([], learning_mode=True, verbosity_level=1)

        assert result.learning_info.commands_executed


class TestAllChecks:
    def test_success(self, quality_tools, subprocess_runner):
        lint_id = OperationId(namespace="tools", category="quality", command="lint")
        typecheck_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        deadcode_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        subprocess_runner.set_results(
            [
                create_success_result(lint_id, "lint ok"),
                create_success_result(typecheck_id, "typecheck ok"),
                create_success_result(deadcode_id, "deadcode ok"),
            ]
        )

        result = quality_tools.all_checks([])

        assert result.success is True
        assert "Lint:\nlint ok" in result.stdout
        assert "Deadcode:\ndeadcode ok" in result.stdout

    def test_collects_errors(self, quality_tools, subprocess_runner):
        lint_id = OperationId(namespace="tools", category="quality", command="lint")
        typecheck_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        deadcode_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        subprocess_runner.set_results(
            [
                create_failure_result(lint_id, 1, stderr="lint fail"),
                create_failure_result(typecheck_id, 2, stderr="typecheck fail"),
                create_success_result(deadcode_id, "deadcode ok"),
            ]
        )

        result = quality_tools.all_checks([])

        assert result.success is False
        assert "lint fail" in result.stderr
        assert "typecheck fail" in result.stderr

    def test_learning_mode(self, quality_tools, subprocess_runner):
        lint_id = OperationId(namespace="tools", category="quality", command="lint")
        typecheck_id = OperationId(
            namespace="tools", category="quality", command="typecheck"
        )
        deadcode_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        subprocess_runner.set_results(
            [
                create_success_result(lint_id),
                create_success_result(typecheck_id),
                create_success_result(deadcode_id),
            ]
        )

        result = quality_tools.all_checks([], learning_mode=True, verbosity_level=2)

        assert result.learning_info.commands_executed
        assert len(result.learning_info.commands_executed) == 3
