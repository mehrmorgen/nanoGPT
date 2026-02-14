import pytest
from typing import Generator
from typer.testing import CliRunner
from ml_playground.tools.cli.commands.env import build_app
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.cli.state import state, reset_state


@pytest.fixture(autouse=True)
def _setup_teardown_cli_env() -> Generator[None, None, None]:
    reset_state()
    yield
    reset_state()


def test_env_setup_help() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["setup", "--help"])
    assert result.exit_code == 0
    assert "Create a fresh uv-managed virtual environment" in result.output


def test_env_sync_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["sync", "--help"])
    assert result.exit_code == 0
    assert "Sync project dependencies using uv" in result.output


def test_env_verify_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["verify", "--help"])
    assert result.exit_code == 0
    assert "Ensure the project package imports correctly" in result.output


def test_env_clean_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["clean", "--help"])
    assert result.exit_code == 0
    assert "Remove caches and temporary build artifacts" in result.output


def test_env_info_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["info", "--help"])
    assert result.exit_code == 0
    assert "Show environment information" in result.output


def test_env_ai_guidelines_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["ai-guidelines", "--help"])
    assert result.exit_code == 0
    assert "Set up AI guideline symlinks" in result.output


def test_env_tensorboard_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["tensorboard", "--help"])
    assert result.exit_code == 0
    assert "Launch TensorBoard" in result.output


def test_env_gguf_help_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["gguf-help", "--help"])
    assert result.exit_code == 0
    assert "Show llama.cpp GGUF conversion help" in result.output
