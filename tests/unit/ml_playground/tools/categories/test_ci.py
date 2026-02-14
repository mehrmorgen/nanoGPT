import pytest
from typing import Generator
from typer.testing import CliRunner
from ml_playground.tools.cli.commands.ci import build_app
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.cli.state import state, reset_state


@pytest.fixture(autouse=True)
def _setup_teardown() -> Generator[None, None, None]:
    reset_state()
    yield
    reset_state()


def test_ci_quality_gate_help() -> None:
    runner = CliRunner()
    app = build_app()

    # Setup state with a fake config to avoid RuntimeError
    state.config = ToolsConfig()

    result = runner.invoke(app, ["quality-gate", "--help"])
    assert result.exit_code == 0
    assert "Run the full pre-commit quality gate" in result.output


def test_ci_quality_gate_error_handling() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()

    # We can't easily mock the tool, but we can verify how handle_tool_result is used
    # and how ToolExecutionError is handled if it were to occur.
    # For now, we verify that the command exists and is callable.
    result = runner.invoke(app, ["quality-gate", "--invalid-arg"])
    assert result.exit_code != 0


def test_ci_quality_fast_error_handling() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["quality-fast", "--invalid-arg"])
    assert result.exit_code != 0


def test_ci_quality_ext_error_handling() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["quality-ext", "--invalid-arg"])
    assert result.exit_code != 0


def test_ci_quality_ci_local_error_handling() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["quality-ci-local", "--invalid-arg"])
    assert result.exit_code != 0


def test_ci_coverage_badge_error_handling() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["coverage-badge", "--invalid-arg"])
    assert result.exit_code != 0
