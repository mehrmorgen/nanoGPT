from __future__ import annotations

import typer

import ml_playground.runtime.cli as cli


def test_app_is_typer_instance() -> None:
    assert isinstance(cli.app, typer.Typer)


def test_get_command_returns_click_command() -> None:
    command = cli.get_command(cli.app)
    assert hasattr(command, "name")
