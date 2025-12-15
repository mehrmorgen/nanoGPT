"""Unit tests for tools CLI functionality.
Tests the CLI entry points, command routing, and learning mode integration
without using mocks, following the project's testing guidelines.
"""

import os
from pathlib import Path
from typing import Callable
from types import SimpleNamespace
from collections.abc import Generator

import pytest
import typer

from typer.testing import CliRunner

import ml_playground.tools.cli.main as tools_cli
import ml_playground.tools.analysis.lit_integration as lit_integration
import ml_playground.tools.analysis.sample_quality as sample_quality
import ml_playground.tools.cli.dependencies as tools_cli_dependencies
from ml_playground.tools.cli.dependencies import ToolsDependencies
from ml_playground.tools.cli.helpers import (
    get_quality_tools,
    get_testing_tools,
    get_environment_tools,
    get_ci_tools,
    get_dev_tools,
    handle_tool_result,
)
from ml_playground.tools.cli.dependencies import (
    get_tools_dependencies,
    reset_tools_dependencies,
    configure_tools_dependencies,
    default_tools_dependencies,
    override_tools_dependencies,
)
from ml_playground.tools.cli.state import GlobalState
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult

from tests.unit.tools._cli_test_helpers import (
    deps as _deps,
    override_attr,
    override_env,
    reset_tools_cli_state as _reset_global_state,
)


@pytest.fixture(autouse=True)
def reset_cli_state() -> Generator[None, None, None]:
    """Ensure CLI global state and dependencies reset between tests."""

    _reset_global_state()
    reset_tools_dependencies()
    yield
    _reset_global_state()
    reset_tools_dependencies()


class TestGlobalState:
    """Test GlobalState functionality."""

    def test_global_state_initialization(self):
        """Test that global state initializes with correct defaults."""
        test_state = GlobalState()

        assert test_state.learning_mode is False
        assert test_state.verbosity == 1
        assert test_state.dry_run is False
        assert test_state.project_root is None
        assert test_state.config is None

    def test_global_state_modification(self):
        """Test that global state can be modified."""
        tools_cli.state.learning_mode = True
        tools_cli.state.mark_learning_mode_explicit(True)
        tools_cli.state.verbosity = 2
        tools_cli.state.dry_run = True
        tools_cli.state.project_root = Path("/test/path")

        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2
        assert tools_cli.state.dry_run is True
        assert tools_cli.state.project_root == Path("/test/path")


class TestCLIBasics:
    """Test basic CLI functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        # Reset global state before each test
        tools_cli.state.learning_mode = False
        tools_cli.state.verbosity = 1
        tools_cli.state.dry_run = False
        tools_cli.state.project_root = None
        tools_cli.state.config = None

    def test_cli_help(self):
        """Test that CLI shows help when no arguments provided."""
        result = self.runner.invoke(tools_cli.app, ["--help"])

        assert result.exit_code == 0
        assert "quality" in result.stdout
        assert "env" in result.stdout
        assert "ci" in result.stdout

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

    def test_cli_project_root_option_loads_config(self, tmp_path: Path) -> None:
        received: dict[str, Path | None] = {}

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            received["project_root"] = project_root
            return ToolsConfig()

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert received["project_root"] == tmp_path

    def test_learn_commands_unknown_category(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            tools_cli.app, ["learn", "commands", "--category", "unknown"]
        )

        assert result.exit_code == 1
        assert "Unknown category" in (result.stderr or result.stdout)

    def test_learn_commands_success_detailed(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            tools_cli.app,
            ["learn", "commands", "--category", "quality", "--detailed"],
        )

        assert result.exit_code == 0
        assert "Quality Tools" in result.stdout
        assert "lint" in result.stdout

    def test_learn_best_practices_unknown_category(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            tools_cli.app,
            ["learn", "best-practices", "--category", "missing"],
        )

        assert result.exit_code == 1
        assert "Unknown category" in (result.stderr or result.stdout)

    def test_learn_best_practices_success(self) -> None:
        runner = CliRunner()
        result = runner.invoke(
            tools_cli.app,
            ["learn", "best-practices", "--category", "ci"],
        )

        assert result.exit_code == 0
        assert "CI/CD Best Practices" in result.stdout

    def test_cli_handles_unexpected_config_error(self) -> None:
        def boom(_project_root: Path | None = None) -> ToolsConfig:
            raise AttributeError("boom")

        deps = _deps(load_config=boom)
        with override_tools_dependencies(deps):
            result = self.runner.invoke(tools_cli.app, ["version"])

        assert result.exit_code == 1
        assert "Unexpected error loading configuration" in (
            result.stderr or result.stdout
        )

    def test_analysis_lit_executes_command_body(self, tmp_path: Path) -> None:
        received: dict[str, object] = {}
        results: list[ToolResult] = []

        def fake_run_server_bundestag_char(
            *, host: str, port: int, open_browser: bool, logger: object
        ) -> None:
            received["host"] = host
            received["port"] = port
            received["open_browser"] = open_browser
            received["logger"] = logger

        def fake_load_tools_config(_project_root: Path | None = None) -> ToolsConfig:
            return ToolsConfig()

        def capture_result(result: ToolResult) -> None:
            results.append(result)

        deps = _deps(load_config=fake_load_tools_config, result_handler=capture_result)
        with (
            override_tools_dependencies(deps),
            override_attr(
                lit_integration,
                "run_server_bundestag_char",
                fake_run_server_bundestag_char,
            ),
        ):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                [
                    "--project-root",
                    str(tmp_path),
                    "analysis",
                    "lit",
                    "--port",
                    "0",
                ],
            )

        assert result.exit_code == 0
        assert received["host"] == "127.0.0.1"
        assert received["port"] == 0
        assert received["open_browser"] is False
        assert results, "Expected at least one ToolResult"
        assert results[-1].success is True
        assert results[-1].exit_code == 0
        assert results[-1].stdout == "LIT server stopped"

    def test_analysis_sample_quality_executes_command_body(
        self, tmp_path: Path
    ) -> None:
        results: list[ToolResult] = []

        def fake_analyze_sample_file(_file_path: Path) -> object:
            return object()

        def fake_format_analysis(_analysis: object) -> str:
            return "ok"

        def fake_load_tools_config(_project_root: Path | None = None) -> ToolsConfig:
            return ToolsConfig()

        def capture_result(result: ToolResult) -> None:
            results.append(result)

        sample_file = tmp_path / "sample.txt"
        sample_file.write_text("hello", encoding="utf-8")

        deps = _deps(load_config=fake_load_tools_config, result_handler=capture_result)
        with (
            override_tools_dependencies(deps),
            override_attr(
                sample_quality,
                "analyze_sample_file",
                fake_analyze_sample_file,
            ),
            override_attr(sample_quality, "format_analysis", fake_format_analysis),
        ):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                [
                    "--project-root",
                    str(tmp_path),
                    "analysis",
                    "sample-quality",
                    str(sample_file),
                ],
            )

        assert result.exit_code == 0
        assert results, "Expected at least one ToolResult"
        assert results[-1].success is True
        assert results[-1].exit_code == 0
        assert results[-1].stdout == "ok"

    def test_main_uses_config_defaults_when_not_overridden(
        self, tmp_path: Path
    ) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        called: list[bool] = []

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            assert project_root == tmp_path
            called.append(True)
            return config_obj

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert called, "load_config override was not invoked"
        assert tools_cli.state.config == config_obj
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2

    def test_main_cli_arguments_override_config(self, tmp_path: Path) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        called: list[bool] = []

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            called.append(True)
            return config_obj

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
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
        assert called, "load_config override was not invoked"


class TestCLIErrorBranches:
    """Additional tests covering CLI error paths."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_main_handles_configuration_error(self) -> None:
        """`load_tools_config` raising `ToolConfigurationError` should exit with code 1."""

        def fake_load_tools_config(_project_root: Path | None = None) -> ToolsConfig:
            raise ToolConfigurationError(
                "invalid configuration",
                reason="test coverage",
                rationale="simulate configuration failure",
            )

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            result = self.runner.invoke(tools_cli.app, ["version"])

        assert result.exit_code == 1
        assert "Configuration error" in (result.stderr or result.stdout)

    def test_test_unit_handles_tool_execution_error(self) -> None:
        """Errors from `TestingTools.unit` should be surfaced to the CLI user."""

        class FailingTestingTools:
            def unit(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "unit tests failed",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        deps = _deps(testing_factory=lambda _cfg, _root: FailingTestingTools())
        with override_tools_dependencies(deps):
            result = self.runner.invoke(tools_cli.app, ["test", "unit"])

        assert result.exit_code == 1
        assert "unit tests failed" in (result.stderr or result.stdout)

    def test_env_tensorboard_handles_tool_execution_error(self) -> None:
        """Errors from `EnvironmentTools.tensorboard` propagate via CLI handler."""

        class FailingEnvironmentTools:
            def tensorboard(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "tensorboard launch failed",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        deps = _deps(environment_factory=lambda _cfg, _root: FailingEnvironmentTools())
        with override_tools_dependencies(deps):
            logdir = Path(".").resolve()
            result = self.runner.invoke(
                tools_cli.app,
                ["env", "tensorboard", "--logdir", str(logdir)],
            )

        assert result.exit_code == 1
        assert "tensorboard launch failed" in (result.stderr or result.stdout)

    def test_ci_quality_gate_handles_tool_execution_error(self) -> None:
        """Errors from `CITools.quality_gate` should result in non-zero exit codes."""

        class FailingCITools:
            def quality_gate(self, *_args: object, **_kwargs: object) -> ToolResult:
                raise ToolExecutionError(
                    "quality gate failure",
                    reason="test coverage",
                    rationale="simulate failure",
                )

        deps = _deps(ci_factory=lambda _cfg, _root: FailingCITools())
        with override_tools_dependencies(deps):
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

    def test_cli_project_root_option_loads_config(self, tmp_path: Path) -> None:
        received: dict[str, Path | None] = {}

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            received["project_root"] = project_root
            return ToolsConfig()

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert received["project_root"] == tmp_path

    def test_cli_handles_unexpected_config_error(self) -> None:
        def boom(_project_root: Path | None = None) -> ToolsConfig:
            raise AttributeError("boom")

        deps = _deps(load_config=boom)
        with override_tools_dependencies(deps):
            result = self.runner.invoke(tools_cli.app, ["version"])

        assert result.exit_code == 1
        assert "Unexpected error loading configuration" in (
            result.stderr or result.stdout
        )

    def test_main_uses_config_defaults_when_not_overridden(
        self, tmp_path: Path
    ) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        called: list[bool] = []

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            assert project_root == tmp_path
            called.append(True)
            return config_obj

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            runner = CliRunner()
            result = runner.invoke(
                tools_cli.app,
                ["--project-root", str(tmp_path), "version"],
            )

        assert result.exit_code == 0
        assert called, "load_config override was not invoked"
        assert tools_cli.state.config == config_obj
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2

    def test_main_cli_arguments_override_config(self, tmp_path: Path) -> None:
        config_obj = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        called: list[bool] = []

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            called.append(True)
            return config_obj

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
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
        assert called, "load_config override was not invoked"
        assert tools_cli.state.learning_mode is False
        assert tools_cli.state.verbosity == 0
        assert tools_cli.state.dry_run is True
        assert tools_cli.state.learning_mode_set is True

    def test_cli_test_coverage_forwards_options(self) -> None:
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

        deps = _deps(testing_factory=lambda _cfg, _root: FakeTestingTools())
        with override_tools_dependencies(deps):
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

    def test_cli_coverage_failure_exits_nonzero(self) -> None:
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

        deps = _deps(testing_factory=lambda _cfg, _root: FakeTestingTools())
        with override_tools_dependencies(deps):
            result = self.runner.invoke(tools_cli.app, ["test", "coverage"])

        assert result.exit_code != 0
        assert "coverage failed" in (result.stderr or result.stdout)

    def test_cli_coverage_runs_successfully(self) -> None:
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

        deps = _deps(testing_factory=lambda _cfg, _root: FakeTestingTools())
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(tools_cli.app, ["test", "coverage"])

        assert result.exit_code == 0
        assert "coverage ok" in result.stdout


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

    def test_quality_lint_success(self) -> None:
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
        deps = _deps(quality_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app, ["quality", "lint", "--", "--fix"]
            )

        assert result.exit_code == 0
        assert "lint ok" in result.stdout
        assert stub.calls == [
            {"args": ["--fix"], "learning_mode": False, "verbosity": 1}
        ]

    def test_quality_format_failure(self) -> None:
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

        deps = _deps(quality_factory=lambda _cfg, _root: StubQualityTools())
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
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
        subcommands = ["quality", "test", "env", "ci", "learn"]
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

    def test_test_unit_command_uses_stubbed_tools(self) -> None:
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
        deps = _deps(testing_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app, ["test", "unit", "pattern", "--", "-k", "fast"]
            )

        assert result.exit_code == 0
        assert "unit ok" in result.stdout
        assert stub.calls == [(["-k", "fast", "-k", "pattern"], False, 1)]

    def test_ci_quality_gate_failure_propagates(self) -> None:
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
        deps = _deps(ci_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app, ["ci", "quality-gate", "--", "--verbose"]
            )

        assert result.exit_code == 3
        assert "quality gate failed" in (result.stderr or result.stdout)
        assert stub.calls == [["--verbose"]]

    def test_ci_quality_fast_success(self) -> None:
        """Test ci quality-fast command success."""

        class StubCITools:
            def __init__(self) -> None:
                self.calls: list[list[str]] = []

            def quality_fast(self, args: list[str]) -> ToolResult:
                self.calls.append(args)
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="ci",
                    command="quality-fast",
                    stdout="quality fast passed",
                )

        stub = StubCITools()
        deps = _deps(ci_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app, ["ci", "quality-fast", "--", "--verbose"]
            )

        assert result.exit_code == 0
        assert "quality fast passed" in result.stdout
        assert stub.calls == [["--verbose"]]

    def test_ci_quality_ext_success(self) -> None:
        """Test ci quality-ext command success."""

        class StubCITools:
            def __init__(self) -> None:
                self.calls: list[list[str]] = []

            def quality_ext(self, args: list[str]) -> ToolResult:
                self.calls.append(args)
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="ci",
                    command="quality-ext",
                    stdout="quality ext passed",
                )

        stub = StubCITools()
        deps = _deps(ci_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app, ["ci", "quality-ext", "--", "--verbose"]
            )

        assert result.exit_code == 0
        assert "quality ext passed" in result.stdout
        assert stub.calls == [["--verbose"]]

    def test_ci_quality_ci_local_success(self) -> None:
        """Test ci quality-ci-local command success."""

        class StubCITools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def quality_ci_local(
                self, bind_caches: bool, args: list[str]
            ) -> ToolResult:
                self.calls.append({"bind_caches": bind_caches, "args": args})
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="ci",
                    command="quality-ci-local",
                    stdout="quality ci local passed",
                )

        stub = StubCITools()
        deps = _deps(ci_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app,
                ["ci", "quality-ci-local", "--no-bind-caches", "--", "--verbose"],
            )

        assert result.exit_code == 0
        assert "quality ci local passed" in result.stdout
        assert stub.calls == [{"bind_caches": False, "args": ["--verbose"]}]

    def test_dev_review_list_success(self) -> None:
        """Test dev review-list command success."""

        class StubDevTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def review_list(
                self, pr_number: int, unreplied: bool, unresolved: bool, remote: str
            ) -> ToolResult:
                self.calls.append(
                    {
                        "pr_number": pr_number,
                        "unreplied": unreplied,
                        "unresolved": unresolved,
                        "remote": remote,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="dev",
                    command="review-list",
                    stdout="review list passed",
                )

        stub = StubDevTools()
        deps = _deps(dev_factory=lambda _cfg: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app,
                ["dev", "review-list", "123", "--unreplied", "--remote", "upstream"],
            )

        assert result.exit_code == 0
        assert "review list passed" in result.stdout
        assert stub.calls == [
            {
                "pr_number": 123,
                "unreplied": True,
                "unresolved": False,
                "remote": "upstream",
            }
        ]

    def test_dev_review_bulk_reply_success(self) -> None:
        """Test dev review-bulk-reply command success."""

        class StubDevTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def review_bulk_reply(
                self, pr_number: int, replies_file: Path, remote: str
            ) -> ToolResult:
                self.calls.append(
                    {
                        "pr_number": pr_number,
                        "replies_file": replies_file,
                        "remote": remote,
                    }
                )
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="dev",
                    command="review-bulk-reply",
                    stdout="review bulk reply passed",
                )

        stub = StubDevTools()
        deps = _deps(dev_factory=lambda _cfg: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
            result = self.runner.invoke(
                tools_cli.app,
                [
                    "dev",
                    "review-bulk-reply",
                    "123",
                    "--replies",
                    "replies.json",
                    "--remote",
                    "upstream",
                ],
            )

        assert result.exit_code == 0
        assert "review bulk reply passed" in result.stdout
        assert stub.calls == [
            {
                "pr_number": 123,
                "replies_file": Path("replies.json"),
                "remote": "upstream",
            }
        ]


class TestDevCommandFailures:
    """Validate error handling branches for dev CLI commands."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def _configure_state(self) -> None:
        tools_cli.state.config = ToolsConfig()
        tools_cli.state.project_root = Path.cwd()

    def test_dev_review_list_handles_tool_error(self) -> None:
        """ToolExecutionError should be converted to a failure ToolResult."""

        captured: list[ToolResult] = []

        class FailingDevTools:
            def review_list(self, *_: object, **__: object) -> ToolResult:
                raise ToolExecutionError("boom", reason="unit", rationale="coverage")

        deps = _deps(
            dev_factory=lambda _cfg: FailingDevTools(),
            result_handler=lambda result: captured.append(result),
        )

        with override_tools_dependencies(deps):
            self._configure_state()
            result = self.runner.invoke(tools_cli.app, ["dev", "review-list", "456"])

        assert result.exit_code == 0
        failure = captured[-1]
        assert failure.success is False
        # Errors are funneled through a generic tools error operation id; assert
        # on the category/command pair instead of the original method name.
        assert failure.operation_id.category == "utils"
        assert failure.operation_id.command == "generic-error"
        # run_tool_command now forwards the underlying error message directly.
        assert "boom" in (failure.stderr or "")

    def test_dev_batch_review_handles_configuration_error(self) -> None:
        """Configuration errors should surface via result handler."""

        captured: list[ToolResult] = []

        class FailingDevTools:
            def batch_review(self, *_: object, **__: object) -> ToolResult:
                raise ToolConfigurationError("bad config", reason="x", rationale="y")

        deps = _deps(
            dev_factory=lambda _cfg: FailingDevTools(),
            result_handler=lambda result: captured.append(result),
        )

        with override_tools_dependencies(deps):
            self._configure_state()
            cli_result = self.runner.invoke(
                tools_cli.app, ["dev", "batch-review", "--format", "yaml"]
            )

        assert cli_result.exit_code == 0
        failure = captured[-1]
        assert failure.success is False
        assert failure.operation_id.category == "utils"
        assert failure.operation_id.command == "generic-error"
        assert "bad config" in (failure.stderr or "")

    def test_dev_batch_review_success_propagates_result(self) -> None:
        """Successful batch review should reach the injected result handler."""

        captured: list[ToolResult] = []

        class StubDevTools:
            def __init__(self) -> None:
                self.seen_formats: list[str] = []

            def batch_review(self, *, output_format: str) -> ToolResult:
                self.seen_formats.append(output_format)
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="tools",
                    category="dev",
                    command="batch-review",
                    stdout=f"format={output_format}",
                )

        stub = StubDevTools()
        deps = _deps(
            dev_factory=lambda _cfg: stub,
            result_handler=lambda result: captured.append(result),
        )

        with override_tools_dependencies(deps):
            self._configure_state()
            cli_result = self.runner.invoke(
                tools_cli.app, ["dev", "batch-review", "--format", "text"]
            )

        assert cli_result.exit_code == 0
        assert stub.seen_formats == ["text"]
        assert captured[-1].success is True
        assert "format=text" in (captured[-1].stdout or "")

    def test_dev_cleanup_handles_tool_error(self) -> None:
        """Cleanup command should exit with Typer error when tools fail."""

        class FailingDevTools:
            def cleanup_ignored_tracked(self) -> ToolResult:
                raise ToolExecutionError(
                    "cleanup failed", reason="err", rationale="cov"
                )

        deps = _deps(dev_factory=lambda _cfg: FailingDevTools())

        with override_tools_dependencies(deps):
            self._configure_state()
            result = self.runner.invoke(
                tools_cli.app, ["dev", "cleanup-ignored-tracked"]
            )

        assert result.exit_code == 1
        assert "cleanup failed" in (result.stderr or result.stdout)

    def test_dev_kill_port_handles_tool_error(self) -> None:
        """kill-port should raise typer.Exit on tool failures."""

        class FailingDevTools:
            def kill_port(self, *_: object, **__: object) -> ToolResult:
                raise ToolExecutionError("kill failed", reason="err", rationale="cov")

        deps = _deps(dev_factory=lambda _cfg: FailingDevTools())

        with override_tools_dependencies(deps):
            self._configure_state()
            result = self.runner.invoke(tools_cli.app, ["dev", "kill-port", "8080"])

        assert result.exit_code == 1
        assert "kill failed" in (result.stderr or result.stdout)


class TestMainCallbackBehavior:
    def test_main_sets_state_and_env_from_cli(self, tmp_path: Path) -> None:
        captured: dict[str, object] = {}
        fake_deps = object()

        def fake_get_deps() -> object:
            return fake_deps

        def fake_load(
            project_root: Path | None = None, deps: object | None = None
        ) -> None:
            captured["project_root"] = project_root
            captured["deps"] = deps
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = project_root

        with override_attr(tools_cli, "get_tools_dependencies", fake_get_deps):
            with override_attr(tools_cli, "load_config_with_error_handling", fake_load):
                with override_env("ML_PLAYGROUND_TOOLS_DRY_RUN", None):
                    ctx = SimpleNamespace(invoked_subcommand="version")  # type: ignore[assignment]
                    tools_cli.main(
                        ctx,
                        learning_mode=True,
                        verbosity=2,
                        dry_run=True,
                        project_root=tmp_path,
                    )
                    assert os.environ.get("ML_PLAYGROUND_TOOLS_DRY_RUN") == "1"

        assert captured["project_root"] == tmp_path
        assert captured["deps"] is fake_deps
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.learning_mode_set is True
        assert tools_cli.state.verbosity == 2
        assert tools_cli.state.dry_run is True

    def test_main_clears_dry_run_env(self, tmp_path: Path) -> None:
        tools_cli.state.config = ToolsConfig()

        def fake_get_deps() -> object:
            return object()

        def fake_load(
            project_root: Path | None = None, deps: object | None = None
        ) -> None:
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = project_root

        with override_attr(tools_cli, "get_tools_dependencies", fake_get_deps):
            with override_attr(tools_cli, "load_config_with_error_handling", fake_load):
                with override_env("ML_PLAYGROUND_TOOLS_DRY_RUN", "1"):
                    ctx = SimpleNamespace(invoked_subcommand="version")  # type: ignore[assignment]
                    tools_cli.main(
                        ctx,
                        learning_mode=False,
                        verbosity=None,
                        dry_run=False,
                        project_root=tmp_path,
                    )
                    assert "ML_PLAYGROUND_TOOLS_DRY_RUN" not in os.environ


class TestShowConfig:
    def test_show_config_prints_current_state(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        tools_cli.state.config = ToolsConfig(learning_mode_default=False)
        tools_cli.state.project_root = Path("/tmp/project")

        with override_attr(tools_cli, "ensure_config_loaded", lambda: None):
            tools_cli.show_config()

        out = capsys.readouterr().out
        assert "Current tools configuration" in out
        assert "Project root" in out


class TestLearnCommands:
    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_learn_commands_overview(self) -> None:
        result = self.runner.invoke(tools_cli.app, ["learn", "commands", "--detailed"])
        assert result.exit_code == 0
        assert "ML Playground Tools Overview" in result.stdout

    def test_learn_commands_invalid_category(self) -> None:
        result = self.runner.invoke(
            tools_cli.app, ["learn", "commands", "--category", "unknown"]
        )
        assert result.exit_code == 1
        assert "Unknown category" in (result.stderr or result.stdout)

    def test_learn_explain_happy_path(self) -> None:
        result = self.runner.invoke(
            tools_cli.app,
            ["learn", "explain", "quality.lint"],
        )
        assert result.exit_code == 0
        assert "Command: quality.lint" in result.stdout

    def test_learn_explain_invalid_format(self) -> None:
        result = self.runner.invoke(tools_cli.app, ["learn", "explain", "invalid"])
        assert result.exit_code == 1
        assert "Command must be in format" in (result.stderr or result.stdout)

    def test_learn_best_practices_category(self) -> None:
        result = self.runner.invoke(
            tools_cli.app,
            ["learn", "best-practices", "--category", "ci"],
        )
        assert result.exit_code == 0
        assert "CI/CD Best Practices" in result.stdout


class TestMainEntryErrorHandling:
    def test_main_entry_handles_keyboard_interrupt(self) -> None:
        class RaisingApp:
            def __call__(self) -> None:  # noqa: D401
                raise KeyboardInterrupt

        with override_attr(tools_cli, "app", RaisingApp()):
            with pytest.raises(typer.Exit) as exc:
                tools_cli.main_entry()

        assert exc.value.exit_code == 1

    def test_main_entry_handles_tool_errors(self) -> None:
        class RaisingApp:
            def __call__(self) -> None:
                raise ToolExecutionError("failure", reason="x", rationale="y")

        with override_attr(tools_cli, "app", RaisingApp()):
            with pytest.raises(typer.Exit) as exc:
                tools_cli.main_entry()

        assert exc.value.exit_code == 1

    def test_main_entry_handles_unexpected_errors(self) -> None:
        class RaisingApp:
            def __call__(self) -> None:
                raise ValueError("boom")

        with override_attr(tools_cli, "app", RaisingApp()):
            with pytest.raises(typer.Exit) as exc:
                tools_cli.main_entry()

        assert exc.value.exit_code == 1


def test_default_tool_result_handler_uses_verbosity_fallback_when_invalid() -> None:
    captured: list[str] = []

    class StubEngine:
        def __init__(self, verbosity: object) -> None:
            assert verbosity == tools_cli_dependencies.VerbosityLevel.STANDARD

        def format_output(self, _result: object, *, learning_enabled: bool) -> str:
            assert learning_enabled is True
            return "formatted"

    with (
        override_attr(tools_cli_dependencies, "LearningModeEngine", StubEngine),
        override_attr(
            tools_cli_dependencies.typer,
            "echo",
            lambda msg, *, err=False: captured.append(str(msg)),
        ),
        override_attr(tools_cli.state, "learning_mode", True),
        override_attr(tools_cli.state, "verbosity", 999),
    ):
        tools_cli_dependencies.default_tool_result_handler(
            ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="quality",
                command="lint",
                stdout="ok",
            )
        )

    assert captured == ["formatted"]


# Additional CLI command routing tests live in TestEnvironmentCommands/TestQuality... classes


class TestCoverageCommands:
    """Cover testing coverage subcommands."""

    def setup_method(self) -> None:
        self.runner = CliRunner()

    def test_test_coverage_invokes_tools(self) -> None:
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
        deps = _deps(testing_factory=lambda _cfg, _root: StubTestingTools())
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
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

    def test_test_coverage_failure_with_thresholds(self) -> None:
        """Failure from unified `coverage` should propagate exit codes and respect thresholds options."""

        class StubTestingTools:
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

        deps = _deps(testing_factory=lambda _cfg, _root: StubTestingTools())
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
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

    def test_env_ai_guidelines_passes_options(self, tmp_path: Path) -> None:
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
        deps = _deps(environment_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
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

    def test_env_tensorboard_passes_arguments(self, tmp_path: Path) -> None:
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
        deps = _deps(environment_factory=lambda _cfg, _root: stub)
        with override_tools_dependencies(deps):
            tools_cli.state.config = ToolsConfig()
            tools_cli.state.project_root = Path.cwd()
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


class TestConfigLoadingErrors:
    """Cover additional branches in `load_config_with_error_handling`."""

    def test_load_config_handles_unexpected_exception(self) -> None:
        """Unexpected exceptions should result in Typer exit."""

        def boom(_: Path | None = None) -> ToolsConfig:  # pragma: no cover - stub
            raise AttributeError("unexpected failure")

        deps = _deps(load_config=boom)
        with override_tools_dependencies(deps):
            with pytest.raises(typer.Exit) as exc_info:
                tools_cli.load_config_with_error_handling()

        assert exc_info.value.exit_code == 1


class TestConfigurationLoading:
    """Tests for configuration loading helpers."""

    def test_load_config_applies_defaults(self) -> None:
        config = ToolsConfig(learning_mode_default=True, default_verbosity=2)

        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            assert project_root is None
            return config

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            tools_cli.state.config = None
            tools_cli.state.project_root = None
            tools_cli.load_config_with_error_handling()

        assert tools_cli.state.config is config
        assert tools_cli.state.project_root is None
        assert tools_cli.state.learning_mode is True
        assert tools_cli.state.verbosity == 2

    def test_load_config_handles_configuration_error(self) -> None:
        def fake_load_tools_config(project_root: Path | None = None) -> ToolsConfig:
            raise ToolConfigurationError("broken config", reason="bad", rationale="")

        deps = _deps(load_config=fake_load_tools_config)
        with override_tools_dependencies(deps):
            with pytest.raises(typer.Exit) as exc:
                tools_cli.load_config_with_error_handling()
        assert exc.value.exit_code == 1


class TestToolFactories:
    """Tests for helper functions that construct tool instances."""

    def test_get_quality_tools_uses_cached_config(self) -> None:
        tools_cli.state.project_root = Path("/tmp/project")
        cached = ToolsConfig(learning_mode_default=True)
        tools_cli.state.config = cached

        captured: dict[str, object] = {}

        class FakeQualityTools:
            def __init__(self, config: ToolsConfig, root_path: Path) -> None:
                captured["config"] = config
                captured["root_path"] = root_path

        def _forbidden_load_config(_root: Path | None = None) -> ToolsConfig:
            raise AssertionError(
                "load_config should not be called when config is cached"
            )

        deps = _deps(
            load_config=_forbidden_load_config,
            quality_factory=lambda cfg, root: FakeQualityTools(cfg, root),
        )

        with override_tools_dependencies(deps):
            tools_cli.state.config = cached
            instance = get_quality_tools()

        assert isinstance(instance, FakeQualityTools)
        assert tools_cli.state.config is cached
        assert captured["config"] is cached
        assert captured["root_path"] == tools_cli.state.project_root

    @pytest.mark.parametrize(
        "factory, expects_root",
        [
            (get_testing_tools, True),
            (get_environment_tools, True),
            (get_ci_tools, True),
            (get_dev_tools, False),
        ],
    )
    def test_tool_factories_create_expected_classes(
        self,
        factory: Callable[[], object],
        expects_root: bool,
    ) -> None:
        tools_cli.state.config = ToolsConfig(learning_mode_default=False)
        tools_cli.state.project_root = Path("/tmp/project")

        captured: dict[str, object] = {}

        class FactoryStub:
            def __init__(self, root_path: Path | None = None):
                captured["config"] = tools_cli.state.config
                captured["root_path"] = root_path

        def testing_factory(_cfg: ToolsConfig, root: Path) -> FactoryStub:
            return FactoryStub(root_path=root)

        def environment_factory(_cfg: ToolsConfig, root: Path) -> FactoryStub:
            return FactoryStub(root_path=root)

        def ci_factory(_cfg: ToolsConfig, root: Path) -> FactoryStub:
            return FactoryStub(root_path=root)

        def dev_factory(_cfg: ToolsConfig) -> FactoryStub:
            return FactoryStub(root_path=None)

        # Override dependencies with stub factories
        original_deps = get_tools_dependencies()
        configure_tools_dependencies(
            lambda: ToolsDependencies(
                load_config=original_deps.load_config,
                quality_factory=original_deps.quality_factory,
                testing_factory=testing_factory,
                environment_factory=environment_factory,
                ci_factory=ci_factory,
                dev_factory=dev_factory,
                result_handler=original_deps.result_handler,
            )
        )

        try:
            instance = factory()
            assert isinstance(instance, FactoryStub)
            if expects_root:
                assert captured["root_path"] == Path("/tmp/project")
            else:
                assert captured["root_path"] is None
        finally:
            reset_tools_dependencies()

    def test_nested_factory_methods_via_default_dependencies(self) -> None:
        """Test nested factory methods indirectly via default_dependencies to cover uncovered lines."""
        config = ToolsConfig(learning_mode_default=False)
        project_root = Path("/tmp/project")

        # Get default dependencies which contain the nested factory functions
        deps = default_tools_dependencies()

        # Test _environment_factory (line 147)
        env_tools = deps.environment_factory(config, project_root)
        assert hasattr(env_tools, "config")
        # EnvironmentTools may not expose project_root directly, just verify it was created

        # Test _ci_factory (line 150)
        ci_tools = deps.ci_factory(config, project_root)
        assert hasattr(ci_tools, "config")
        # CITools may not expose project_root directly, just verify it was created

        # Test _dev_factory (line 153)
        dev_tools = deps.dev_factory(config)
        assert hasattr(dev_tools, "config")
        # DevTools may not expose additional attributes, just verify it was created


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

        handle_tool_result(result)

        captured = capsys.readouterr()
        assert "hello" in captured.out


class TestInvokeTests:
    """Tests for the public `tools test` commands."""

    def setup_method(self) -> None:
        self.runner = CliRunner()
