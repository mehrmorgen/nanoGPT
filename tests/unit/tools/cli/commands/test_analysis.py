from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ml_playground.tools.cli.commands.analysis import app
from contextlib import contextmanager


@contextmanager
def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    object.__setattr__(target, name, value)
    try:
        yield
    finally:
        if original is not missing:
            object.__setattr__(target, name, original)
        else:
            delattr(target, name)


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
    assert result.exit_code == 2
    assert "Error:" in result.stderr


def test_analysis_lit_command() -> None:
    """Test lit command entrypoint."""
    calls: list[dict[str, object]] = []

    def fake_run_lit_server(*, port: int, host: str, open_browser: bool) -> None:
        calls.append({"port": port, "host": host, "open_browser": open_browser})

    with override_attr(
        __import__("ml_playground.tools.cli.commands.analysis"),
        "run_lit_server",
        fake_run_lit_server,
    ):
        result = runner.invoke(
            app, ["lit", "--port", "1234", "--host", "0.0.0.0", "--open-browser"]
        )
    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["port"] == 1234
    assert calls[0]["host"] == "0.0.0.0"
    assert calls[0]["open_browser"] is True


def test_analysis_lit_command_failure() -> None:
    """Test lit command failure handling."""

    def fake_run_lit_server(**_: object) -> None:
        raise RuntimeError("Server crash")

    with override_attr(
        __import__("ml_playground.tools.cli.commands.analysis"),
        "run_lit_server",
        fake_run_lit_server,
    ):
        result = runner.invoke(app, ["lit"])
    assert result.exit_code == 1
    assert "Error: Server crash" in result.stderr


def test_analysis_build_app() -> None:
    """Test build_app() entrypoint."""
    from ml_playground.tools.cli.commands.analysis import build_app
    import typer

    cmd_app = build_app()
    assert isinstance(cmd_app, typer.Typer)
