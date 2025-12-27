from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from ml_playground.tools.cli.commands.analysis import app

runner = CliRunner()


def test_analysis_sample_quality_success(tmp_path: Path) -> None:
    """Test sample-quality command with a dummy file."""
    sample_file = tmp_path / "sample.txt"
    sample_file.write_text("Sprecher: Alice\nThema: Test\nJahr: 2022\n\nContent line.")

    result = runner.invoke(app, ["sample-quality", str(sample_file)])
    assert result.exit_code == 0
    assert "== Header ==" in result.stdout
    assert "Alice" in result.stdout


def test_analysis_sample_quality_failure() -> None:
    """Test sample-quality command with non-existent file."""
    result = runner.invoke(app, ["sample-quality", "non_existent.txt"])
    assert result.exit_code == 1
    assert "Error:" in result.stderr


@patch("ml_playground.tools.cli.commands.analysis.run_lit_server")
def test_analysis_lit_command(mock_run_server: MagicMock) -> None:
    """Test lit command entrypoint."""
    result = runner.invoke(
        app, ["lit", "--port", "1234", "--host", "0.0.0.0", "--open-browser"]
    )
    assert result.exit_code == 0
    mock_run_server.assert_called_once()
    _, kwargs = mock_run_server.call_args
    assert kwargs["port"] == 1234
    assert kwargs["host"] == "0.0.0.0"
    assert kwargs["open_browser"] is True


@patch("ml_playground.tools.cli.commands.analysis.run_lit_server")
def test_analysis_lit_command_failure(mock_run_server: MagicMock) -> None:
    """Test lit command failure handling."""
    mock_run_server.side_effect = RuntimeError("Server crash")
    result = runner.invoke(app, ["lit"])
    assert result.exit_code == 1
    assert "Error: Server crash" in result.stderr


def test_analysis_build_app() -> None:
    """Test build_app() entrypoint."""
    from ml_playground.tools.cli.commands.analysis import build_app
    import typer

    cmd_app = build_app()
    assert isinstance(cmd_app, typer.Typer)
