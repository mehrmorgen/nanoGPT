from pathlib import Path
from typing import cast

import pytest
import typer

from ml_playground.runtime_cli.typer_helpers import extract_exp_config, run_or_exit


def test_extract_exp_config_path() -> None:
    class MockCtx:
        def __init__(self, obj: object) -> None:
            self.obj = obj

    p = Path("test.toml")
    ctx = cast(typer.Context, MockCtx({"exp_config": p}))
    assert extract_exp_config(ctx) == p


def test_extract_exp_config_none() -> None:
    class MockCtx:
        def __init__(self, obj: object) -> None:
            self.obj = obj

    ctx = cast(typer.Context, MockCtx({"exp_config": None}))
    assert extract_exp_config(ctx) is None


def test_extract_exp_config_invalid_obj() -> None:
    class MockCtx:
        def __init__(self, obj: object) -> None:
            self.obj = obj

    ctx = cast(typer.Context, MockCtx("not-a-dict"))
    assert extract_exp_config(ctx) is None


def test_run_or_exit_success() -> None:
    called = False

    def success_fn() -> None:
        nonlocal called
        called = True

    run_or_exit(success_fn)
    assert called is True


def test_run_or_exit_filenotfound() -> None:
    def fail_fn() -> None:
        raise FileNotFoundError("missing")

    with pytest.raises(typer.Exit) as exc:
        run_or_exit(fail_fn, exception_exit_code=7)
    # Cast to object to satisfy basedpyright's access to exc.value
    assert getattr(cast(object, exc.value), "exit_code", None) == 7


def test_run_or_exit_keyboard_interrupt() -> None:
    def interrupt_fn() -> None:
        raise KeyboardInterrupt()

    # Should not raise, just return None
    run_or_exit(interrupt_fn, keyboard_interrupt_msg="interrupted")
