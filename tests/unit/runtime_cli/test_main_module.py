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
