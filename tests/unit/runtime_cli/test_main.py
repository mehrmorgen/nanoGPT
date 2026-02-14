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
