from __future__ import annotations

import typer

import ml_playground.runtime_cli.main as cli_main


def test_app_is_typer_instance() -> None:
    assert isinstance(cli_main.app, typer.Typer)


def test_get_command_returns_click_command() -> None:
    command = cli_main.get_command(cli_main.app)
    assert getattr(command, "name", None) is not None
