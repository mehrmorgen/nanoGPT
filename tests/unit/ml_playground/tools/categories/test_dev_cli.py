import pytest
from typing import Generator
from typer.testing import CliRunner
from ml_playground.tools.cli.commands.dev import build_app
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.cli.state import state, reset_state


@pytest.fixture(autouse=True)
def _setup_teardown_cli_dev() -> Generator[None, None, None]:
    reset_state()
    yield
    reset_state()


def test_dev_review_list_help() -> None:
    runner = CliRunner()
    app = build_app()
    state.config = ToolsConfig()
    result = runner.invoke(app, ["review-list", "--help"])
    assert result.exit_code == 0
    assert "List GitHub PR review comments" in result.output


def test_dev_review_bulk_reply_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["review-bulk-reply", "--help"])
    assert result.exit_code == 0
    assert "Bulk reply to GitHub PR review comments" in result.output


def test_dev_review_delete_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["review-delete", "--help"])
    assert result.exit_code == 0
    assert "Delete GitHub PR review comments" in result.output


def test_dev_cleanup_ignored_tracked_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["cleanup-ignored-tracked", "--help"])
    assert result.exit_code == 0
    assert "Clean up Git-ignored files" in result.output


def test_dev_kill_port_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["kill-port", "--help"])
    assert result.exit_code == 0
    assert "Kill processes running on a specific port" in result.output


def test_dev_setup_ai_guidelines_help() -> None:
    runner = CliRunner()
    app = build_app()
    result = runner.invoke(app, ["setup-ai-guidelines", "--help"])
    assert result.exit_code == 0
    assert "Set up AI development guidelines" in result.output
