"""Unit tests for tools CLI functionality.
Tests the CLI entry points, command routing, and learning mode integration
without using mocks, following the project's testing guidelines.
"""

from __future__ import annotations

from pathlib import Path
from contextlib import contextmanager
import importlib
from types import SimpleNamespace
from typing import Any, Iterator

import pytest
import typer

from typer.testing import CliRunner

import ml_playground.tools.cli as tools_cli
from ml_playground.tools.cli import helpers as cli_helpers
from ml_playground.tools.cli.commands import test as test_commands
from ml_playground.tools.cli.state import state as cli_state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def swap_attr(target: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(target, name)
    setattr(target, name, value)
    try:
        yield
    finally:
        setattr(target, name, original)


@contextmanager
def swap_attr_by_path(path: str, value: Any) -> Iterator[None]:
    module_path, attr = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    original = getattr(module, attr)
    setattr(module, attr, value)
    try:
        yield
    finally:
        setattr(module, attr, original)


@pytest.fixture(autouse=True)
def reset_cli_state() -> None:
    """Ensure CLI global state is reset between tests."""
    cli_state.learning_mode = False
    cli_state.verbosity = 1
    cli_state.dry_run = False
    cli_state.project_root = None
    cli_state.config = None
    cli_state._learning_mode_set = False  # pyright: ignore[reportPrivateUsage]


class TestGlobalState:
    """Test GlobalState functionality."""

    def test_global_state_initialization(self):
        """Test that global state initializes with correct defaults."""
        test_state = tools_cli.GlobalState()

        assert test_state.learning_mode is False
        assert test_state.verbosity == 1
        assert test_state.dry_run is False
        assert test_state.project_root is None
        assert test_state.config is None

    def test_global_state_modification(self):
        """Test that global state can be modified."""
        test_state = tools_cli.GlobalState()

        test_state.learning_mode = True
        test_state.verbosity = 2
        test_state.dry_run = True
        test_state.project_root = Path("/test/path")

        assert test_state.learning_mode is True
        assert test_state.verbosity == 2
        assert test_state.dry_run is True
        assert test_state.project_root == Path("/test/path")


class TestCLIBasics:
    """Test basic CLI functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        # Reset global state before each test
        tools_cli.state.learning_mode = False  # pyright: ignore[reportAttributeAccessIssue]
        tools_cli.state.verbosity = 1  # pyright: ignore[reportAttributeAccessIssue]
        tools_cli.state.dry_run = False  # pyright: ignore[reportAttributeAccessIssue]
        tools_cli.state.project_root = None  # pyright: ignore[reportAttributeAccessIssue]
        tools_cli.state.config = None  # pyright: ignore[reportAttributeAccessIssue]

    def test_cli_help(self):
        """Test that CLI shows help when no arguments provided."""
        result = self.runner.invoke(tools_cli.app, ["--help"])

        assert result.exit_code == 0
        for subcommand in ("quality", "test", "env", "ci", "agentic", "dev", "learn"):
            assert subcommand in result.stdout


class TestCLIErrorBranches:
    """Additional tests covering CLI error paths."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_main_handles_configuration_error(self, monkeypatch) -> None:
        """`load_tools_config` raising `ToolConfigurationError` should exit with code 1."""

        def fake_load_tools_config(_project_root: Path | None = None) -> ToolsConfig:
            raise ToolConfigurationError(
                "invalid configuration",
                reason="test coverage",
                rationale="simulate configuration failure",
            )

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            result = self.runner.invoke(tools_cli.app, ["version"])

        assert result.exit_code == 1
        assert "Configuration error" in (result.stderr or result.stdout)

    def test_test_unit_handles_tool_execution_error(self, monkeypatch) -> None:
        """Errors from `TestingTools.unit` should be surfaced to the CLI user."""

        class FailingTestingTools:
            def unit(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "unit tests failed",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        with swap_attr(cli_helpers, "get_testing_tools", lambda: FailingTestingTools()):
            result = self.runner.invoke(tools_cli.app, ["test", "unit"])

        assert result.exit_code == 1
        assert "unit tests failed" in (result.stderr or result.stdout)

    def test_env_tensorboard_handles_tool_execution_error(self, monkeypatch) -> None:
        """Errors from `EnvironmentTools.tensorboard` propagate via CLI handler."""

        class FailingEnvironmentTools:
            def tensorboard(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "tensorboard launch failed",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        with swap_attr(
            cli_helpers, "get_environment_tools", lambda: FailingEnvironmentTools()
        ):
            logdir = Path(".").resolve()
            result = self.runner.invoke(
                tools_cli.app,
                ["env", "tensorboard", "--logdir", str(logdir)],
            )

        assert result.exit_code == 1
        assert "tensorboard launch failed" in (result.stderr or result.stdout)

    def test_ci_quality_gate_handles_tool_execution_error(self, monkeypatch) -> None:
        """Errors from `CITools.quality_gate` should result in non-zero exit codes."""

        class FailingCITools:
            def quality_gate(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "quality gate failure",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        with swap_attr(cli_helpers, "get_ci_tools", lambda: FailingCITools()):
            result = self.runner.invoke(tools_cli.app, ["ci", "quality-gate"])

        assert result.exit_code == 1
        assert "quality gate failure" in (result.stderr or result.stdout)

    def test_cli_version(self):
        """Test version command."""
        result = self.runner.invoke(tools_cli.app, ["version"])
        assert result.exit_code == 0
        assert "ML Playground Tools" in result.stdout
        assert "v0.1.0" in result.stdout

    def test_cli_config_without_config_file(self):
        """Test config command when no config is loaded."""
        result = self.runner.invoke(tools_cli.app, ["config"])

        # Should load default config and show it
        assert result.exit_code == 0
        assert "Current tools configuration:" in result.stdout
        assert "Learning mode default:" in result.stdout
        assert "Tool categories:" in result.stdout

    def test_cli_global_options(self):
        """Test global CLI options parsing."""
        # Test learning mode option
        result = self.runner.invoke(tools_cli.app, ["--learning-mode", "version"])
        assert result.exit_code == 0

        # Test verbosity option
        result = self.runner.invoke(tools_cli.app, ["--verbosity", "2", "version"])
        assert result.exit_code == 0

        # Test dry run option
        result = self.runner.invoke(tools_cli.app, ["--dry-run", "version"])
        assert result.exit_code == 0

    def test_cli_project_root_option_loads_config(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        received: dict[str, Path | None] = {}

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            received["project_root"] = project_root
            return ToolsConfig()

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert received["project_root"] == tmp_path

    def test_cli_handles_unexpected_config_error(self, monkeypatch) -> None:
        def boom(_project_root: Path | None = None) -> ToolsConfig:
            raise RuntimeError("boom")

        with swap_attr(tools_cli, "load_tools_config", boom):
            result = self.runner.invoke(tools_cli.app, ["version"])

        assert result.exit_code == 1
        assert "Unexpected error loading configuration" in result.stderr

    def test_main_uses_config_defaults_when_not_overridden(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            assert project_root == tmp_path
            return config_obj

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert tools_cli.state.config is config_obj
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2

    def test_main_cli_arguments_override_config(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            return config_obj

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                [
                    "--project-root",
                    str(tmp_path),
                    "--no-learning-mode",
                    "--verbosity",
                    "0",
                    "--dry-run",
                    "version",
                ],
            )

        assert result.exit_code == 0
        assert tools_cli.state.learning_mode is False
        assert tools_cli.state.verbosity == 0
        assert tools_cli.state.dry_run is True
        assert tools_cli.state._learning_mode_set is True

    def test_cli_test_coverage_forwards_options(self, monkeypatch) -> None:
        captured: dict[str, object] = {}

        class FakeTestingTools:
            def coverage(
                self,
                args: list[str],
                *,
                line_threshold: float | None = None,
                branch_threshold: float | None = None,
                verbose: bool = False,
                learning_mode: bool = False,
                verbosity_level: int = 1,
                force_regen: bool = False,
            ) -> ToolResult:
                captured.update(
                    {
                        "args": args,
                        "line": line_threshold,
                        "branch": branch_threshold,
                        "verbose": verbose,
                        "learning_mode": learning_mode,
                        "verbosity_level": verbosity_level,
                        "force_regen": force_regen,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="test",
                    command="coverage",
                    stdout="coverage ok",
                )

        with swap_attr(cli_helpers, "get_testing_tools", lambda: FakeTestingTools()):
            result = self.runner.invoke(
                tools_cli.app,
                [
                    "--learning-mode",
                    "--verbosity",
                    "2",
                    "test",
                    "coverage",
                    "--line-threshold",
                    "90",
                    "--branch-threshold",
                    "80",
                    "--force-regen",
                    "--verbose",
                    "extra",
                    "args",
                ],
            )

        assert result.exit_code == 0
        assert captured["args"] == ["extra", "args"]
        assert captured["line"] == 90.0
        assert captured["branch"] == 80.0
        assert captured["verbose"] is True
        assert captured["learning_mode"] is True
        assert captured["verbosity_level"] == 2
        assert captured["force_regen"] is True

    def test_cli_coverage_failure_exits_nonzero(self, monkeypatch) -> None:
        class FakeTestingTools:
            def coverage(
                self,
                _args: list[str],
                *,
                line_threshold: float | None = None,
                branch_threshold: float | None = None,
                verbose: bool = False,
                learning_mode: bool = False,
                verbosity_level: int = 1,
                force_regen: bool = False,
            ) -> ToolResult:
                return ToolResult.create(
                    success=False,
                    exit_code=7,
                    namespace="tools",
                    category="test",
                    command="coverage",
                    stderr="coverage failed",
                )

        with swap_attr(cli_helpers, "get_testing_tools", lambda: FakeTestingTools()):
            result = self.runner.invoke(tools_cli.app, ["test", "coverage"])

        assert result.exit_code == 7
        assert "coverage failed" in (result.stderr or result.stdout)

    def test_cli_coverage_runs_successfully(self, monkeypatch) -> None:
        class FakeTestingTools:
            def coverage(
                self,
                _args: list[str],
                *,
                line_threshold: float | None = None,
                branch_threshold: float | None = None,
                verbose: bool = False,
                learning_mode: bool = False,
                verbosity_level: int = 1,
                force_regen: bool = False,
            ) -> ToolResult:
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="test",
                    command="coverage",
                    stdout="coverage ok",
                )

        with swap_attr(cli_helpers, "get_testing_tools", lambda: FakeTestingTools()):
            result = self.runner.invoke(tools_cli.app, ["test", "coverage"])

        assert result.exit_code == 0
        assert "coverage ok" in result.stdout


class TestLearnCommands:
    """Test learn subcommands."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_learn_commands_overview(self):
        """Test learn commands shows overview."""
        result = self.runner.invoke(tools_cli.app, ["learn", "commands"])

        assert result.exit_code == 0
        assert "Command Discovery" in result.stdout
        assert "quality" in result.stdout
        assert "test" in result.stdout
        assert "env" in result.stdout
        assert "ci" in result.stdout
        assert "agentic" in result.stdout
        assert "Quick Start:" in result.stdout

    def test_learn_commands_category_specific(self):
        """Test learn commands for specific category."""
        result = self.runner.invoke(
            tools_cli.app, ["learn", "commands", "--category", "quality"]
        )

        assert result.exit_code == 0
        assert "Quality Tools" in result.stdout
        assert "lint" in result.stdout
        assert "format" in result.stdout
        assert "typecheck" in result.stdout
        assert "Usage Examples:" in result.stdout

    def test_learn_commands_invalid_category(self):
        """Test learn commands with invalid category."""
        result = self.runner.invoke(
            tools_cli.app, ["learn", "commands", "--category", "invalid"]
        )

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_commands_detailed(self):
        """Test learn commands with detailed flag."""
        result = self.runner.invoke(tools_cli.app, ["learn", "commands", "--detailed"])

        assert result.exit_code == 0
        assert "Commands:" in result.stdout

    def test_learn_explain_valid_command(self):
        """Test learn explain for valid command."""
        result = self.runner.invoke(tools_cli.app, ["learn", "explain", "quality.lint"])

        assert result.exit_code == 0
        assert "Command: quality.lint" in result.stdout
        assert "Explanation:" in result.stdout
        assert "Best Practices:" in result.stdout
        assert "Related Concepts:" in result.stdout

    def test_learn_explain_invalid_format(self):
        """Test learn explain with invalid command format."""
        result = self.runner.invoke(tools_cli.app, ["learn", "explain", "invalid"])

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Command must be in format 'category.command'" in result.stderr
            or "Command must be in format 'category.command'" in result.stdout
        )

    def test_learn_explain_invalid_category(self):
        """Test learn explain with invalid category."""
        result = self.runner.invoke(
            tools_cli.app, ["learn", "explain", "invalid.command"]
        )

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_explain_verbosity_levels(self):
        """Test learn explain with different verbosity levels."""
        # Test minimal verbosity
        result = self.runner.invoke(
            tools_cli.app, ["learn", "explain", "test.unit", "--verbosity", "0"]
        )
        assert result.exit_code == 0

        # Test standard verbosity
        result = self.runner.invoke(
            tools_cli.app, ["learn", "explain", "test.unit", "--verbosity", "1"]
        )
        assert result.exit_code == 0

        # Test comprehensive verbosity
        result = self.runner.invoke(
            tools_cli.app, ["learn", "explain", "test.unit", "--verbosity", "2"]
        )
        assert result.exit_code == 0

    def test_learn_best_practices_overview(self):
        """Test learn best-practices shows overview."""
        result = self.runner.invoke(tools_cli.app, ["learn", "best-practices"])

        assert result.exit_code == 0
        assert "ML Playground Development Best Practices" in result.stdout
        assert "Development Workflow" in result.stdout
        assert "Learning Paths" in result.stdout

    def test_learn_best_practices_category_specific(self):
        """Test learn best-practices for specific category."""
        result = self.runner.invoke(
            tools_cli.app, ["learn", "best-practices", "--category", "quality"]
        )

        assert result.exit_code == 0
        assert "Best Practices for Quality Tools" in result.stdout
        assert "Beginner Path:" in result.stdout
        assert "Intermediate Path:" in result.stdout

    def test_learn_best_practices_invalid_category(self):
        """Test learn best-practices with invalid category."""
        result = self.runner.invoke(
            tools_cli.app, ["learn", "best-practices", "--category", "invalid"]
        )

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_best_practices_verbosity_levels(self):
        """Test learn best-practices with different verbosity levels."""
        # Test minimal verbosity
        result = self.runner.invoke(
            tools_cli.app, ["learn", "best-practices", "--verbosity", "0"]
        )
        assert result.exit_code == 0

        # Test comprehensive verbosity
        result = self.runner.invoke(
            tools_cli.app, ["learn", "best-practices", "--verbosity", "2"]
        )
        assert result.exit_code == 0
        assert "Advanced Path:" in result.stdout or "Advanced Users:" in result.stdout


class TestCLISubcommands:
    """Test CLI subcommand structure."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_quality_subcommand_help(self):
        """Test quality subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["quality", "--help"])

        assert result.exit_code == 0
        assert "Code quality tools" in result.stdout
        assert "lint" in result.stdout
        assert "format" in result.stdout
        assert "typecheck" in result.stdout

    def test_test_subcommand_help(self):
        """Test test subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["test", "--help"])

        assert result.exit_code == 0
        assert "Testing tools" in result.stdout
        assert "unit" in result.stdout
        assert "integration" in result.stdout
        assert "coverage" in result.stdout

    def test_env_subcommand_help(self):
        """Test env subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["env", "--help"])

        assert result.exit_code == 0
        assert "Environment management" in result.stdout
        assert "setup" in result.stdout
        assert "sync" in result.stdout
        assert "clean" in result.stdout

    def test_ci_subcommand_help(self):
        """Test ci subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["ci", "--help"])

        assert result.exit_code == 0
        assert "CI/CD operations" in result.stdout
        assert "quality-gate" in result.stdout
        assert "mutation" in result.stdout

    def test_agentic_subcommand_help(self):
        """Test agentic subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["agentic", "--help"])

        assert result.exit_code == 0
        assert "AI-assisted development" in result.stdout
        assert "guidelines-setup" in result.stdout
        assert "batch-review" in result.stdout

    def test_learn_subcommand_help(self):
        """Test learn subcommand shows help."""
        result = self.runner.invoke(tools_cli.app, ["learn", "--help"])

        assert result.exit_code == 0
        assert "Learning mode utilities" in result.stdout
        assert "commands" in result.stdout
        assert "explain" in result.stdout
        assert "best-practices" in result.stdout


class TestCLIErrorHandling:
    """Test CLI error handling."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_invalid_subcommand(self):
        """Test CLI handles invalid subcommands gracefully."""
        result = self.runner.invoke(tools_cli.app, ["invalid-command"])

        # Typer should show help for invalid commands
        assert result.exit_code != 0

    def test_invalid_global_option_values(self):
        """Test CLI handles invalid global option values."""
        # Test invalid verbosity
        result = self.runner.invoke(tools_cli.app, ["--verbosity", "5", "version"])
        assert result.exit_code != 0

        # Test invalid verbosity (negative)
        result = self.runner.invoke(tools_cli.app, ["--verbosity", "-1", "version"])
        assert result.exit_code != 0


class TestQualityCommands:
    """Exercise quality subcommands via stubbed tools."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_quality_lint_success(self, monkeypatch) -> None:
        """`quality lint` should forward arguments to the quality tools."""

        class StubQualityTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def lint(
                self,
                args: list[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "args": args,
                        "learning_mode": learning_mode,
                        "verbosity": verbosity_level,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="quality",
                    command="lint",
                    stdout="lint ok",
                )

        stub = StubQualityTools()
        with swap_attr(cli_helpers, "get_quality_tools", lambda: stub):
            result = self.runner.invoke(
                tools_cli.app, ["quality", "lint", "--", "--fix"]
            )

        assert result.exit_code == 0
        assert "lint ok" in result.stdout
        assert stub.calls == [
            {"args": ["--fix"], "learning_mode": False, "verbosity": 1}
        ]

    def test_quality_format_failure(self, monkeypatch) -> None:
        """`quality format` should propagate failure exit codes."""

        class StubQualityTools:
            def format(
                self,
                _args: list[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                assert learning_mode is False
                assert verbosity_level == 1
                return ToolResult.create(
                    success=False,
                    exit_code=5,
                    namespace="tools",
                    category="quality",
                    command="format",
                    stderr="format failed",
                )

        with swap_attr(cli_helpers, "get_quality_tools", lambda: StubQualityTools()):
            result = self.runner.invoke(tools_cli.app, ["quality", "format"])

        assert result.exit_code == 5
        assert "format failed" in (result.stderr or result.stdout)


class TestCLIIntegration:
    """Test CLI integration with tool categories."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_cli_loads_configuration(self):
        """Test that CLI loads configuration properly."""
        # This test verifies that the CLI can load configuration without errors
        result = self.runner.invoke(tools_cli.app, ["config"])

        assert result.exit_code == 0
        assert "Current tools configuration:" in result.stdout

    def test_cli_help_integration(self):
        """Test that CLI help integrates properly with all subcommands."""
        # Test main help
        result = self.runner.invoke(tools_cli.app, ["--help"])
        assert result.exit_code == 0

        # Test that all expected subcommands are present
        subcommands = ["quality", "test", "env", "ci", "agentic", "learn"]
        for subcommand in subcommands:
            assert subcommand in result.stdout

    def test_cli_learning_mode_integration(self):
        """Test that learning mode flag is properly integrated."""
        # Test that learning mode flag is accepted
        result = self.runner.invoke(tools_cli.app, ["--learning-mode", "--help"])
        assert result.exit_code == 0

        # Test that no-learning mode flag is accepted
        result = self.runner.invoke(tools_cli.app, ["--no-learning-mode", "--help"])
        assert result.exit_code == 0

    def test_test_unit_command_uses_stubbed_tools(self, monkeypatch) -> None:
        """Ensure `tools test unit` delegates to the TestingTools stub."""

        class StubTestingTools:
            def __init__(self) -> None:
                self.calls: list[tuple[list[str], bool, int]] = []

            def unit(
                self,
                args: list[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.calls.append((args, learning_mode, verbosity_level))
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="test",
                    command="unit",
                    stdout="unit ok",
                )

        stub = StubTestingTools()
        with swap_attr(cli_helpers, "get_testing_tools", lambda: stub):
            result = self.runner.invoke(
                tools_cli.app, ["test", "unit", "pattern", "--", "-k", "fast"]
            )

        assert result.exit_code == 0
        assert "unit ok" in result.stdout
        assert stub.calls == [(["-k", "fast", "-k", "pattern"], False, 1)]

    def test_ci_quality_gate_failure_propagates(self, monkeypatch) -> None:
        """Ensure failing CI tool result surfaces via Typer exit code."""

        class StubCITools:
            def __init__(self) -> None:
                self.calls: list[list[str]] = []

            def quality_gate(self, args: list[str]) -> ToolResult:
                self.calls.append(args)
                return ToolResult.create(
                    success=False,
                    exit_code=3,
                    namespace="tools",
                    category="ci",
                    command="quality-gate",
                    stderr="quality gate failed",
                )

        stub = StubCITools()
        with swap_attr(cli_helpers, "get_ci_tools", lambda: stub):
            result = self.runner.invoke(
                tools_cli.app, ["ci", "quality-gate", "--", "--verbose"]
            )

        assert result.exit_code == 3
        assert "quality gate failed" in (result.stderr or result.stdout)
        assert stub.calls == [["--verbose"]]


class TestCoverageCommands:
    """Cover testing coverage subcommands."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_test_coverage_invokes_tools(self, monkeypatch) -> None:
        """`test coverage` should pass CLI options to `TestingTools.coverage`."""

        class StubTestingTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def coverage(
                self,
                args: list[str],
                *,
                line_threshold: float | None,
                branch_threshold: float | None,
                verbose: bool,
                learning_mode: bool,
                verbosity_level: int,
                force_regen: bool = False,
            ) -> ToolResult:
                captured.append(
                    {
                        "args": args,
                        "line": line_threshold,
                        "branch": branch_threshold,
                        "verbose": verbose,
                        "learning_mode": learning_mode,
                        "verbosity": verbosity_level,
                        "force_regen": force_regen,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="test",
                    command="coverage",
                    stdout="coverage ok",
                )

        captured: list[dict[str, object]] = []
        with swap_attr(cli_helpers, "get_testing_tools", lambda: StubTestingTools()):
            result = self.runner.invoke(
                tools_cli.app,
                [
                    "test",
                    "coverage",
                    "--line-threshold",
                    "91.5",
                    "--branch-threshold",
                    "73.0",
                ],
            )

        assert result.exit_code == 0
        assert "coverage ok" in result.stdout
        assert captured == [
            {
                "args": [],
                "line": 91.5,
                "branch": 73.0,
                "verbose": False,
                "learning_mode": False,
                "verbosity": 1,
                "force_regen": False,
            }
        ]

    def test_test_coverage_failure_with_thresholds(self, monkeypatch) -> None:
        """Failure from unified `coverage` should propagate exit codes and respect thresholds options."""

        class StubTestingTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def coverage(
                self,
                args: list[str],
                *,
                line_threshold: float | None,
                branch_threshold: float | None,
                verbose: bool,
                learning_mode: bool,
                verbosity_level: int,
                force_regen: bool = False,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "args": args,
                        "line": line_threshold,
                        "branch": branch_threshold,
                        "verbose": verbose,
                        "learning_mode": learning_mode,
                        "verbosity": verbosity_level,
                        "force_regen": force_regen,
                    }
                )
                assert learning_mode is False
                assert verbosity_level == 1
                assert line_threshold == 95.0
                assert branch_threshold == 80.0
                return ToolResult.create(
                    success=False,
                    exit_code=6,
                    namespace="tools",
                    category="test",
                    command="coverage",
                    stderr="threshold failed",
                )

        with swap_attr(cli_helpers, "get_testing_tools", lambda: StubTestingTools()):
            result = self.runner.invoke(
                tools_cli.app,
                [
                    "test",
                    "coverage",
                    "--line-threshold",
                    "95",
                    "--branch-threshold",
                    "80",
                ],
            )

        assert result.exit_code == 6
        assert "threshold failed" in (result.stderr or result.stdout)


class TestEnvironmentCommands:
    """Exercise environment subcommands with stubbed tools."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_env_ai_guidelines_passes_options(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """`env ai-guidelines` should forward tool and dry-run flags."""

        class StubEnvTools:
            def __init__(self) -> None:
                self.calls: list[tuple[list[str], str, bool]] = []

            def ai_guidelines(
                self, args: list[str], *, tool: str, dry_run: bool
            ) -> ToolResult:
                self.calls.append((args, tool, dry_run))
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="env",
                    command="ai-guidelines",
                )

        stub = StubEnvTools()
        with swap_attr(cli_helpers, "get_environment_tools", lambda: stub):
            result = self.runner.invoke(
                tools_cli.app,
                [
                    "env",
                    "ai-guidelines",
                    "llama_cpp",
                    "--dry-run",
                ],
            )

        assert result.exit_code == 0
        assert stub.calls == [([], "llama_cpp", True)]

    def test_env_tensorboard_passes_arguments(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """`env tensorboard` should pass CLI options to the tool."""

        class StubEnvTools:
            def __init__(self) -> None:
                self.calls: list[tuple[list[str], Path, int, str]] = []

            def tensorboard(
                self,
                args: list[str],
                *,
                logdir: Path,
                port: int,
                host: str,
            ) -> ToolResult:
                self.calls.append((args, logdir, port, host))
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="env",
                    command="tensorboard",
                )

        stub = StubEnvTools()
        with swap_attr(cli_helpers, "get_environment_tools", lambda: stub):
            logdir = tmp_path / "logs"
            logdir.mkdir()

            result = self.runner.invoke(
                tools_cli.app,
                [
                    "env",
                    "tensorboard",
                    "--logdir",
                    str(logdir),
                    "--port",
                    "6100",
                    "--host",
                    "0.0.0.0",
                    "--",
                    "--reload_interval=1",
                ],
            )

        assert result.exit_code == 0
        assert stub.calls == [(["--reload_interval=1"], logdir, 6100, "0.0.0.0")]


class TestAgenticCommands:
    """Exercise agentic subcommands with stubbed tools."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_agentic_guidelines_uses_learning_state(self, monkeypatch) -> None:
        """Learning mode state should be forwarded to agentic tools."""

        class StubAgenticTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def guidelines_setup(
                self,
                args: list[str],
                *,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "args": args,
                        "learning_mode": learning_mode,
                        "verbosity": verbosity_level,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="agentic",
                    command="guidelines-setup",
                )

        stub = StubAgenticTools()
        with swap_attr(cli_helpers, "get_agentic_tools", lambda: stub):
            result = self.runner.invoke(
                tools_cli.app,
                ["--learning-mode", "--verbosity", "2", "agentic", "guidelines-setup"],
            )

        assert result.exit_code == 0
        assert stub.calls == [{"args": [], "learning_mode": True, "verbosity": 2}]

    def test_agentic_batch_review_handles_tool_error(self, monkeypatch) -> None:
        """`agentic batch-review` should surface ToolExecutionError as exit 1."""

        class FailingAgenticTools:
            def batch_review(
                self,
                args: list[str],
                *,
                output_format: str,
                learning_mode: bool,
                verbosity_level: int,
            ) -> ToolResult:
                raise ToolExecutionError(
                    "batch review failed", reason="oops", rationale="boom"
                )

        with swap_attr(cli_helpers, "get_agentic_tools", lambda: FailingAgenticTools()):
            result = self.runner.invoke(
                tools_cli.app, ["agentic", "batch-review", "--format", "yaml"]
            )

        assert result.exit_code == 1
        assert "Error: batch review failed" in (result.stderr or result.stdout)


class TestConfigLoadingErrors:
    """Cover additional branches in `load_config_with_error_handling`."""

    def test_load_config_handles_unexpected_exception(self, monkeypatch) -> None:
        """Unexpected exceptions should result in Typer exit."""

        def boom(_: Path | None = None) -> ToolsConfig:  # pragma: no cover - stub
            raise RuntimeError("unexpected failure")

        with swap_attr(tools_cli, "load_tools_config", boom):
            with pytest.raises(typer.Exit) as exc_info:
                tools_cli.load_config_with_error_handling()

        assert exc_info.value.exit_code == 1


class TestConfigurationLoading:
    """Tests for configuration loading helpers."""

    def test_load_config_applies_defaults(self, monkeypatch) -> None:
        config = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            assert project_root is None
            return config

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            tools_cli.load_config_with_error_handling()

        assert tools_cli.state.config is config
        assert tools_cli.state.project_root is None
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2

    def test_load_config_handles_configuration_error(self, monkeypatch) -> None:
        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            raise ToolConfigurationError("broken config", reason="bad", rationale="")

        with swap_attr(tools_cli, "load_tools_config", fake_load_tools_config):
            with pytest.raises(typer.Exit) as exc:
                tools_cli.load_config_with_error_handling()
        assert exc.value.exit_code == 1


class TestToolFactories:
    """Tests for helper functions that construct tool instances."""

    def test_get_quality_tools_uses_cached_config(self, monkeypatch) -> None:
        tools_cli.state.config = ToolsConfig()
        tools_cli.state.project_root = Path("/tmp/project")

        captured: dict[str, object] = {}

        class FakeQualityTools:
            def __init__(self, config: ToolsConfig, root_path: Path) -> None:
                captured["config"] = config
                captured["root_path"] = root_path

        with swap_attr_by_path(
            "ml_playground.tools.quality.QualityTools", FakeQualityTools
        ):
            instance = cli_helpers.get_quality_tools()

        assert isinstance(instance, FakeQualityTools)
        assert captured["config"] is tools_cli.state.config
        assert captured["root_path"] == tools_cli.state.project_root

    @pytest.mark.parametrize(
        "factory_name, target_path, expects_root",
        [
            ("get_testing_tools", "ml_playground.tools.testing.TestingTools", True),
            (
                "get_environment_tools",
                "ml_playground.tools.environment.EnvironmentTools",
                True,
            ),
            ("get_ci_tools", "ml_playground.tools.ci.CITools", True),
            (
                "get_agentic_tools",
                "ml_playground.tools.agentic.AgenticTools",
                True,
            ),
            ("get_dev_tools", "ml_playground.tools.dev.DevTools", False),
        ],
    )
    def test_tool_factories_create_expected_classes(
        self,
        monkeypatch,
        factory_name: str,
        target_path: str,
        expects_root: bool,
    ) -> None:
        tools_cli.state.config = ToolsConfig()
        tools_cli.state.project_root = Path("/tmp/project")

        captured: dict[str, object] = {}

        class FactoryStub:
            def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
                config = kwargs.get("config") or (args[0] if args else None)
                root_path = kwargs.get("root_path")
                if root_path is None and len(args) > 1:
                    root_path = args[1]
                captured["config"] = config
                captured["root_path"] = root_path

        with swap_attr_by_path(target_path, FactoryStub):
            factory = getattr(cli_helpers, factory_name)
            instance = factory()  # type: ignore[operator]

        assert isinstance(instance, FactoryStub)
        assert captured["config"] is tools_cli.state.config
        if expects_root:
            assert captured["root_path"] == tools_cli.state.project_root
        else:
            assert captured["root_path"] in {None, tools_cli.state.project_root}


class TestHandleToolResult:
    """Tests for result handling helper."""

    def test_handle_tool_result_success_prints_output(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="test",
            command="example",
            stdout="hello",
        )

        cli_helpers.handle_tool_result(result)

        captured = capsys.readouterr()
        assert "hello" in captured.out

    def test_handle_tool_result_failure_exits(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        result = ToolResult.create(
            success=False,
            exit_code=3,
            namespace="tools",
            category="test",
            command="example",
            stderr="boom",
        )

        with pytest.raises(typer.Exit) as exc:
            cli_helpers.handle_tool_result(result)

        captured = capsys.readouterr()
        assert "boom" in captured.err
        assert exc.value.exit_code == 3


class TestInvokeTests:
    """Tests for `_invoke_tests` routing logic."""

    class StubTestingTools:
        def __init__(self) -> None:
            self.received = None

        def unit(
            self, args: list[str], *, learning_mode: bool, verbosity_level: int
        ) -> ToolResult:
            self.received = {
                "args": args,
                "learning_mode": learning_mode,
                "verbosity": verbosity_level,
            }
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="test",
                command="unit",
                stdout="ran unit",
            )

    def test_invoke_tests_passes_context(
        self, monkeypatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        stub = self.StubTestingTools()
        with swap_attr(cli_helpers, "get_testing_tools", lambda: stub):
            original_learning = tools_cli.state.learning_mode
            original_verbosity = tools_cli.state.verbosity
            try:
                tools_cli.state.learning_mode = True
                tools_cli.state.verbosity = 2
                ctx = SimpleNamespace(obj={})
                test_commands._invoke_tests(
                    ctx, "tests/unit", pattern="selected", extra_args=["-q"]
                )
            finally:
                tools_cli.state.learning_mode = original_learning
                tools_cli.state.verbosity = original_verbosity

        assert stub.received is not None
        assert stub.received["args"] == ["-q", "-k", "selected"]
        assert stub.received["learning_mode"] is True
        assert stub.received["verbosity"] == 2
        captured = capsys.readouterr()
        assert "ran unit" in captured.out

    def test_invoke_tests_handles_unknown_suite(self, monkeypatch) -> None:
        with swap_attr(
            cli_helpers, "get_testing_tools", lambda: self.StubTestingTools()
        ):
            ctx = SimpleNamespace(obj={})
            with pytest.raises(typer.Exit) as exc:
                test_commands._invoke_tests(
                    ctx, "tests/unknown", pattern=None, extra_args=[]
                )

        assert exc.value.exit_code == 1
