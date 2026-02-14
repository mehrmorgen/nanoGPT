from __future__ import annotations

from pathlib import Path
from typing import Callable
import importlib

import pytest
import typer
from ml_playground.runtime_cli.main import main_entry, global_options


def _swap_attr(target: object, name: str, value: object) -> Callable[[], None]:
    original = getattr(target, name)
    target_dict = getattr(target, "__dict__", None)
    if isinstance(target_dict, dict):
        target_dict[name] = value
    else:
        object.__setattr__(target, name, value)

    def _restore() -> None:
        if isinstance(target_dict, dict):
            target_dict[name] = original
        else:
            object.__setattr__(target, name, original)

    return _restore


# --- main.py PBT ---


def test_main_entry_handles_keyboard_interrupt() -> None:
    main_module = importlib.import_module("ml_playground.runtime_cli.main")

    def _raise_interrupt() -> None:
        raise KeyboardInterrupt

    calls: list[str] = []

    def _echo(message: str, err: bool = False) -> None:
        _ = err
        calls.append(message)

    restore_app = _swap_attr(main_module, "app", _raise_interrupt)
    deps = main_module.create_default_cli_dependencies()
    restore_echo = _swap_attr(deps, "echo", _echo)
    restore_factory = _swap_attr(
        main_module, "create_default_cli_dependencies", lambda: deps
    )
    try:
        with pytest.raises(typer.Exit) as exc:
            main_entry()
        assert exc.value.exit_code == 1
        assert any("Operation cancelled" in msg for msg in calls)
    finally:
        restore_factory()
        restore_echo()
        restore_app()


def test_main_entry_handles_generic_exception() -> None:
    main_module = importlib.import_module("ml_playground.runtime_cli.main")

    def _raise_error() -> None:
        raise ValueError("Boom")

    calls: list[str] = []

    def _echo(message: str, err: bool = False) -> None:
        _ = err
        calls.append(message)

    restore_app = _swap_attr(main_module, "app", _raise_error)
    deps = main_module.create_default_cli_dependencies()
    restore_echo = _swap_attr(deps, "echo", _echo)
    restore_factory = _swap_attr(
        main_module, "create_default_cli_dependencies", lambda: deps
    )
    try:
        with pytest.raises(typer.Exit) as exc:
            main_entry()
        assert exc.value.exit_code == 1
        assert any("Runtime CLI execution failed" in msg for msg in calls)
    finally:
        restore_factory()
        restore_echo()
        restore_app()


def test_global_options_missing_config(tmp_path: Path) -> None:
    import ml_playground.runtime_cli.main as main_module

    missing_path = tmp_path / "missing.toml"
    ctx = typer.Context(typer.main.get_command(main_module.app))
    with pytest.raises(typer.Exit) as exc:
        global_options(ctx, exp_config=missing_path)
    assert exc.value.exit_code == 2
