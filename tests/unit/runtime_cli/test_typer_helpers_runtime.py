from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
import typer

from ml_playground.runtime_cli import typer_helpers


def test_extract_exp_config_rejects_non_dict(caplog: pytest.LogCaptureFixture) -> None:
    ctx = SimpleNamespace(obj=None)
    with caplog.at_level(logging.DEBUG):
        result = typer_helpers.extract_exp_config(cast(typer.Context, ctx))
    assert result is None
    assert "no exp_config" in caplog.text


def test_extract_exp_config_accepts_path(caplog: pytest.LogCaptureFixture) -> None:
    ctx = SimpleNamespace(obj={"exp_config": Path("/tmp/test.toml")})
    with caplog.at_level(logging.DEBUG):
        result = typer_helpers.extract_exp_config(cast(typer.Context, ctx))
    assert result == Path("/tmp/test.toml")
    assert "resolved to" in caplog.text


def test_extract_exp_config_logs_unexpected_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ctx = SimpleNamespace(obj={"exp_config": 123})
    with caplog.at_level(logging.DEBUG):
        result = typer_helpers.extract_exp_config(cast(typer.Context, ctx))
    assert result is None
    assert "Unexpected exp_config value type" in caplog.text


def test_run_or_exit_passthrough_typer_exit() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(lambda: (_ for _ in ()).throw(typer.Exit(7)))
    assert excinfo.value.exit_code == 7


def test_run_or_exit_import_error() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(lambda: (_ for _ in ()).throw(ImportError("missing")))
    assert excinfo.value.exit_code == 1


def test_typer_helpers_run_or_exit_keyboard_interrupt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt),
            keyboard_interrupt_msg="cancel",
        )
    assert "cancel" in caplog.text


def test_typer_helpers_run_or_exit_keyboard_interrupt_no_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        typer_helpers.run_or_exit(lambda: (_ for _ in ()).throw(KeyboardInterrupt))
    # Should not log anything when message is None
    assert caplog.text == ""
