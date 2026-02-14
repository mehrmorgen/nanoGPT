from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import typer
import pytest
from pytest import LogCaptureFixture

from ml_playground.runtime_cli import typer_helpers


def test_extract_exp_config_with_path() -> None:
    ctx = SimpleNamespace(obj={"exp_config": Path("/tmp/config.toml")})
    assert typer_helpers.extract_exp_config(cast(typer.Context, ctx)) == Path(
        "/tmp/config.toml"
    )


def test_extract_exp_config_unexpected_type_logs_and_returns_none(
    caplog: LogCaptureFixture,
) -> None:
    ctx = SimpleNamespace(obj={"exp_config": "oops"})

    with caplog.at_level("DEBUG"):
        assert typer_helpers.extract_exp_config(cast(typer.Context, ctx)) is None
        assert any(
            "Unexpected exp_config value type" in line
            for line in caplog.text.splitlines()
        )


def test_run_or_exit_handles_value_error() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(lambda: (_ for _ in ()).throw(ValueError("bad")))

    assert excinfo.value.exit_code == 1


def test_run_or_exit_handles_keyboard_interrupt_message(
    caplog: LogCaptureFixture,
) -> None:
    def raise_keyboard():
        raise KeyboardInterrupt

    with caplog.at_level("INFO"):
        typer_helpers.run_or_exit(
            raise_keyboard, keyboard_interrupt_msg="cancelled", exception_exit_code=5
        )

    assert "cancelled" in caplog.text


def test_extract_exp_config_rejects_non_dict(caplog: LogCaptureFixture) -> None:
    ctx = SimpleNamespace(obj=None)
    with caplog.at_level("DEBUG"):
        result = typer_helpers.extract_exp_config(cast(typer.Context, ctx))
    assert result is None
    assert "no exp_config" in caplog.text


def test_run_or_exit_passthrough_typer_exit() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(typer.Exit(7))  # type: ignore
        )
    assert excinfo.value.exit_code == 7


def test_run_or_exit_import_error() -> None:
    with pytest.raises(typer.Exit) as excinfo:
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(ImportError("missing"))  # type: ignore
        )
    assert excinfo.value.exit_code == 1


def test_run_or_exit_keyboard_interrupt_no_message(
    caplog: LogCaptureFixture,
) -> None:
    with caplog.at_level("INFO"):
        typer_helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt)  # type: ignore
        )
    # Should not log anything when message is None
    assert caplog.text == ""
