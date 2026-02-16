from __future__ import annotations

import click
import pytest
import typer

import ml_playground.runtime_cli.main as cli_main


def test_app_exports_and_get_command() -> None:
    assert isinstance(cli_main.app, typer.Typer)
    cmd = cli_main.get_command(cli_main.app)
    assert getattr(cmd, "main", None) is not None


def test_main_callable_noargs_raises_help() -> None:
    with pytest.raises(click.exceptions.NoArgsIsHelpError):
        cli_main.main([])


def test_main_returns_none_when_command_main_is_not_callable() -> None:
    class _Cmd:
        main = "not-callable"

    original_get_command = cli_main.get_command
    original_loader = cli_main.registry.load_preparers
    try:
        cli_main.get_command = lambda _app: _Cmd()  # type: ignore[assignment]
        cli_main.registry.load_preparers = lambda: None  # type: ignore[assignment]
        assert cli_main.main(["prepare", "demo"]) is None
    finally:
        cli_main.get_command = original_get_command  # type: ignore[assignment]
        cli_main.registry.load_preparers = original_loader  # type: ignore[assignment]


def test_global_options_help() -> None:
    from typer.testing import CliRunner

    env = {"COLUMNS": "100", "TERM": "dumb", "NO_COLOR": "1"}
    runner = CliRunner(env=env)
    result = runner.invoke(cli_main.app, ["--help"])
    assert result.exit_code == 0
    assert "ML Playground CLI" in result.output
    assert "--exp-config" in result.output
    assert "--learning-mode" in result.output


@pytest.mark.parametrize("cmd", ["prepare", "train", "sample", "analyze"])
def test_subcommands_help(cmd: str) -> None:
    from typer.testing import CliRunner

    env = {"COLUMNS": "100", "TERM": "dumb", "NO_COLOR": "1"}
    runner = CliRunner(env=env)
    result = runner.invoke(cli_main.app, [cmd, "--help"])
    assert result.exit_code == 0
    assert cmd in result.output.lower()
