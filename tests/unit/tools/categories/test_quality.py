"""Unit tests for quality tools category."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.categories import quality as quality_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from tests.unit.tools.fakes import (
    FakeSubprocessRunner,
    create_success_result,
    create_failure_result,
)


@pytest.fixture
def config() -> ToolsConfig:
    """Create test configuration."""
    return ToolsConfig(
        quality=config_module.QualityToolsConfig(
            timeout=120,
            ruff_config_path=Path("pyproject.toml"),
        )
    )


@pytest.fixture
def root_path(tmp_path: Path) -> Path:
    """Create temporary root path."""
    return tmp_path


@pytest.fixture
def subprocess_runner() -> FakeSubprocessRunner:
    """Create fake subprocess runner."""
    return FakeSubprocessRunner()


@pytest.fixture
def quality_tools(
    config: ToolsConfig, root_path: Path, subprocess_runner: FakeSubprocessRunner
) -> quality_module.QualityTools:
    """Create quality tools instance with fake dependencies."""
    return quality_module.QualityTools(config, root_path, subprocess_runner)


class TestQualityToolsInit:
    """Test QualityTools initialization."""

    def test_init(
        self,
        quality_tools: quality_module.QualityTools,
        config: ToolsConfig,
        root_path: Path,
    ) -> None:
        """Test initialization."""
        assert quality_tools.config == config
        assert quality_tools.root_path == root_path
        assert quality_tools.pkg_path == root_path / "src" / "ml_playground"
        assert quality_tools.category == "quality"


class TestLint:
    """Test lint functionality."""

    def test_lint_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful lint execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        expected_result = create_success_result(operation_id, "All checks passed")
        subprocess_runner.set_results([expected_result])

        result = quality_tools.lint([])

        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.quality.lint"

        # Check default command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        # The command should contain the uv run wrapper and the ruff command
        assert "uv" in command
        assert "run" in command
        assert "ruff" in command
        assert "check" in command
        assert "." in command

    def test_lint_with_custom_args(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test lint with custom arguments."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        expected_result = create_success_result(operation_id)
        subprocess_runner.set_results([expected_result])

        result = quality_tools.lint(["check", "--fix", "src/"])

        assert result.success is True

        # Check custom command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        # The command should contain the uv run wrapper and the custom ruff command
        assert "uv" in command
        assert "run" in command
        assert "ruff" in command
        assert "check" in command
        assert "--fix" in command
        assert "src/" in command

    def test_lint_failure(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test failed lint execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        expected_result = create_failure_result(
            operation_id, 1, "", "Lint errors found"
        )
        subprocess_runner.set_results([expected_result])

        result = quality_tools.lint([])

        assert result.success is False
        assert result.exit_code == 1


class TestFormat:
    """Test format functionality."""

    def test_format_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful format execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        # Mock both check --fix and format commands
        check_result = create_success_result(operation_id, "Fixed 5 issues")
        format_result = create_success_result(operation_id, "Formatted 10 files")
        subprocess_runner.set_results([check_result, format_result])

        result = quality_tools.format([])

        assert result.success is True
        assert result.exit_code == 0
        assert "Ruff check --fix:" in result.stdout
        assert "Ruff format:" in result.stdout
        assert "Fixed 5 issues" in result.stdout
        assert "Formatted 10 files" in result.stdout

    def test_format_check_failure(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test format when check --fix fails."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        check_result = create_failure_result(operation_id, 1, "", "Check failed")
        subprocess_runner.set_results([check_result])

        result = quality_tools.format([])

        assert result.success is False
        assert result.exit_code == 1

    def test_format_with_args(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test format with custom arguments."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="format"
        )
        check_result = create_success_result(operation_id)
        format_result = create_success_result(operation_id)
        subprocess_runner.set_results([check_result, format_result])

        result = quality_tools.format(["--verbose"])

        assert result.success is True

        # Check that args are passed to both commands
        assert len(subprocess_runner.calls) == 2
        check_call = subprocess_runner.calls[0]["command"]
        format_call = subprocess_runner.calls[1]["command"]
        # Both calls should contain the verbose flag
        assert "--verbose" in check_call
        assert "--verbose" in format_call
        # First call should be check --fix
        assert "check" in check_call and "--fix" in check_call
        # Second call should be format
        assert "format" in format_call


class TestLintCheck:
    """Test lint-check functionality (alias)."""

    def test_lint_check_is_alias(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test that lint-check is an alias for lint."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="lint"
        )
        expected_result = create_success_result(operation_id)
        subprocess_runner.set_results([expected_result])

        result = quality_tools.lint_check(["--verbose"])

        assert result.success is True
        # Should have called the same subprocess as lint()
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "ruff" in command
        assert "--verbose" in command


class TestDeadcode:
    """Test deadcode functionality."""

    def test_deadcode_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful deadcode execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )
        expected_result = create_success_result(operation_id, "No dead code found")
        subprocess_runner.set_results([expected_result])

        result = quality_tools.deadcode([])

        assert result.success is True
        assert str(result.operation_id) == "tools.quality.deadcode"

        # Check command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "vulture" in command
        assert "--min-confidence" in command
        assert "90" in command


class TestBasedPyright:
    """Test BasedPyright functionality."""

    def test_basedpyright_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful BasedPyright execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="basedpyright"
        )
        expected_result = create_success_result(operation_id, "No type errors found")
        subprocess_runner.set_results([expected_result])

        result = quality_tools.basedpyright([])

        assert result.success is True
        assert str(result.operation_id) == "tools.quality.basedpyright"

        # Check command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "basedpyright" in command


class TestPyright:
    """Test Pyright functionality (alias)."""

    def test_pyright_is_alias(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test that pyright is an alias for basedpyright."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="basedpyright"
        )
        expected_result = create_success_result(operation_id)
        subprocess_runner.set_results([expected_result])

        result = quality_tools.pyright(["--verbose"])

        assert result.success is True
        # Should have called the same subprocess as basedpyright()
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "basedpyright" in command
        assert "--verbose" in command


class TestMypy:
    """Test Mypy functionality."""

    def test_mypy_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful Mypy execution."""
        operation_id = OperationId(
            namespace="tools", category="quality", command="mypy"
        )
        expected_result = create_success_result(
            operation_id, "Success: no issues found"
        )
        subprocess_runner.set_results([expected_result])

        result = quality_tools.mypy([])

        assert result.success is True
        assert str(result.operation_id) == "tools.quality.mypy"

        # Check command construction
        assert len(subprocess_runner.calls) == 1
        command = subprocess_runner.calls[0]["command"]
        assert "mypy" in command
        assert "--incremental" in command


class TestTypecheck:
    """Test typecheck functionality."""

    def test_typecheck_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful typecheck execution."""
        # Mock both basedpyright and mypy calls
        basedpyright_id = OperationId(
            namespace="tools", category="quality", command="basedpyright"
        )
        mypy_id = OperationId(namespace="tools", category="quality", command="mypy")

        basedpyright_result = create_success_result(
            basedpyright_id, "BasedPyright: No errors"
        )
        mypy_result = create_success_result(mypy_id, "Mypy: Success")
        subprocess_runner.set_results([basedpyright_result, mypy_result])

        result = quality_tools.typecheck([])

        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.quality.typecheck"
        assert "BasedPyright:" in result.stdout
        assert "Mypy:" in result.stdout

        # Should have called both tools
        assert len(subprocess_runner.calls) == 2

    def test_typecheck_basedpyright_failure(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test typecheck when BasedPyright fails."""
        basedpyright_id = OperationId(
            namespace="tools", category="quality", command="basedpyright"
        )
        mypy_id = OperationId(namespace="tools", category="quality", command="mypy")

        basedpyright_result = create_failure_result(
            basedpyright_id, 1, "", "BasedPyright errors"
        )
        mypy_result = create_success_result(mypy_id, "Mypy: Success")
        subprocess_runner.set_results([basedpyright_result, mypy_result])

        result = quality_tools.typecheck([])

        assert result.success is False
        assert result.exit_code == 1


class TestAllChecks:
    """Test all quality checks functionality."""

    def test_all_checks_success(
        self,
        quality_tools: quality_module.QualityTools,
        subprocess_runner: FakeSubprocessRunner,
    ) -> None:
        """Test successful execution of all quality checks."""
        # Mock results for lint, typecheck (basedpyright + mypy), and deadcode
        lint_id = OperationId(namespace="tools", category="quality", command="lint")
        basedpyright_id = OperationId(
            namespace="tools", category="quality", command="basedpyright"
        )
        mypy_id = OperationId(namespace="tools", category="quality", command="mypy")
        deadcode_id = OperationId(
            namespace="tools", category="quality", command="deadcode"
        )

        lint_result = create_success_result(lint_id, "Lint: OK")
        basedpyright_result = create_success_result(basedpyright_id, "BasedPyright: OK")
        mypy_result = create_success_result(mypy_id, "Mypy: OK")
        deadcode_result = create_success_result(deadcode_id, "Deadcode: OK")

        subprocess_runner.set_results(
            [lint_result, basedpyright_result, mypy_result, deadcode_result]
        )

        result = quality_tools.all_checks([])

        assert result.success is True
        assert result.exit_code == 0
        assert str(result.operation_id) == "tools.quality.all"
        assert "Lint:" in result.stdout
        assert "Typecheck:" in result.stdout
        assert "Deadcode:" in result.stdout
