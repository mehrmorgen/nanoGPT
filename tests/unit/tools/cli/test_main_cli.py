from __future__ import annotations

from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import ml_playground.tools.cli.main as cli
from ml_playground.tools.cli.main import set_cli_hooks
from ml_playground.tools.cli.state import reset_state
from ml_playground.tools.core.config import ToolsConfig


def test_show_config_no_config_exits() -> None:
    reset_state()

    def stub_loader(
        project_root: Path | None = None, deps: object | None = None
    ) -> None:
        assert project_root is None
        return None

    try:
        set_cli_hooks(config_loader=stub_loader)
        runner = CliRunner()
        result = runner.invoke(cli.app, ["config"])
        assert result.exit_code == 1
        assert "Configuration not loaded" in result.stderr
    finally:
        set_cli_hooks()


def test_show_config_with_config(tmp_path: Path) -> None:
    reset_state()

    def stub_loader(
        project_root: Path | None = None, deps: object | None = None
    ) -> None:
        cfg = ToolsConfig()
        cli.state.config = cfg  # type: ignore[attr-defined]
        cli.state.project_root = project_root or tmp_path  # type: ignore[attr-defined]

    try:
        set_cli_hooks(config_loader=stub_loader)
        runner = CliRunner()
        result = runner.invoke(cli.app, ["config"])
        assert result.exit_code == 0
        assert "Current tools configuration:" in result.stdout
        assert str(tmp_path) in result.stdout
    finally:
        set_cli_hooks()


def test_main_entry_keyboard_interrupt() -> None:
    class StubApp:
        def __call__(self, *_: object, **__: object) -> None:
            raise KeyboardInterrupt()

    try:
        set_cli_hooks(app_runner=StubApp())
        with pytest.raises(typer.Exit):
            cli.main_entry()
    finally:
        set_cli_hooks()


def test_main_entry_generic_exception() -> None:
    class StubApp:
        def __call__(self, *_: object, **__: object) -> None:
            raise RuntimeError("boom")

    try:
        set_cli_hooks(app_runner=StubApp())
        with pytest.raises(typer.Exit):
            cli.main_entry()
    finally:
        set_cli_hooks()
