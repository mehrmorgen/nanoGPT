from __future__ import annotations

import click
import pytest
import typer

import ml_playground.runtime.cli as cli


def test_app_exports_and_get_command() -> None:
    assert isinstance(cli.app, typer.Typer)
    cmd = cli.get_command(cli.app)
    assert hasattr(cmd, "main")


def test_main_callable_noargs_raises_help() -> None:
    with pytest.raises(click.exceptions.NoArgsIsHelpError):
        cli.main([])
