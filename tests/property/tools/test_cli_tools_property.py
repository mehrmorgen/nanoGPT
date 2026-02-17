"""Compatibility coverage for legacy tools CLI property module path.

The comprehensive tools CLI property suite lives in
`tests/property/tools/cli/test_tools_cli_property.py`.
This module keeps lightweight smoke coverage for policy/path stability.
"""

from __future__ import annotations

from typing import cast

import click
from click.testing import CliRunner
import typer

import ml_playground.tools.cli.main as tools_cli_main


CLI_RUNNER = CliRunner()


def _get_command(app: typer.Typer) -> click.Command:
    command_getter = getattr(typer.main, "get_command", None)
    if command_getter is None:
        raise RuntimeError("Typer get_command unavailable")
    return cast(click.Command, command_getter(app))


def _collect_command_paths(app: typer.Typer) -> set[tuple[str, ...]]:
    paths: set[tuple[str, ...]] = set()

    def _walk(prefix: tuple[str, ...], typer_app: typer.Typer) -> None:
        for command_info in getattr(typer_app, "registered_commands", ()):  # type: ignore[attr-defined]
            name = command_info.name
            if name:
                paths.add((*prefix, name))

        for group_info in getattr(typer_app, "registered_groups", ()):  # type: ignore[attr-defined]
            name = group_info.name
            if not name:
                continue
            _walk((*prefix, name), group_info.typer_instance)

    _walk((), app)
    return paths


def test_tools_cli_command_graph_present() -> None:
    command_paths = _collect_command_paths(tools_cli_main.app)
    assert command_paths
    assert ("version",) in command_paths
    assert ("config",) in command_paths


def test_tools_cli_version_smoke() -> None:
    result = CLI_RUNNER.invoke(_get_command(tools_cli_main.app), ["version"])
    assert result.exit_code == 0
    assert "ML Playground Tools v" in result.stdout


def test_tools_cli_unknown_command_smoke() -> None:
    result = CLI_RUNNER.invoke(_get_command(tools_cli_main.app), ["totally-unknown"])
    assert result.exit_code != 0
    output = (result.stdout or "") + (result.stderr or "")
    lowered = output.lower()
    assert "no such command" in lowered or "unknown command" in lowered
