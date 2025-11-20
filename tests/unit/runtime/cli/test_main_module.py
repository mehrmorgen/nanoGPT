from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pytest
import typer

import ml_playground.runtime.cli.main as runtime_cli_main


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_main_invokes_registry_and_command() -> None:
    calls: dict[str, Any] = {}

    def fake_load_preparers() -> None:
        calls["registry"] = calls.get("registry", 0) + 1

    class DummyCommand:
        def main(self, args: list[str] | None, standalone_mode: bool) -> int:
            calls["args"] = args
            calls["standalone"] = standalone_mode
            return 123

    def fake_get_command(app: typer.Typer) -> DummyCommand:
        calls["app"] = app
        return DummyCommand()

    with override_attr(
        runtime_cli_main.registry, "load_preparers", fake_load_preparers
    ):
        with override_attr(runtime_cli_main, "get_command", fake_get_command):
            result = runtime_cli_main.main(["--foo"])

    assert result == 123
    assert calls["registry"] == 1
    assert calls["app"] is runtime_cli_main.app
    assert calls["args"] == ["--foo"]
    assert calls["standalone"] is False


def test_main_entry_uses_custom_echo_on_exception() -> None:
    messages: list[str] = []

    def failing_runner() -> None:
        raise RuntimeError("boom")

    def custom_echo(message: str, *, err: bool = False) -> object:
        messages.append(f"{message}|{err}")
        return None

    with pytest.raises(typer.Exit) as exc:
        runtime_cli_main.main_entry(app_runner=failing_runner, echo=custom_echo)

    assert exc.value.exit_code == 1
    assert messages and "Runtime CLI execution failed: boom|True" in messages[-1]


def test_main_entry_handles_keyboard_interrupt(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def interrupting_runner() -> None:
        raise KeyboardInterrupt

    with pytest.raises(typer.Exit) as exc:
        runtime_cli_main.main_entry(app_runner=interrupting_runner)

    assert exc.value.exit_code == 1
    captured = capsys.readouterr()
    assert "Operation cancelled by user" in captured.err


def test_main_entry_runs_successfully_with_custom_echo() -> None:
    calls: dict[str, Any] = {"runner": 0, "echo": []}

    def successful_runner() -> None:
        calls["runner"] += 1

    def custom_echo(message: str, *, err: bool = False) -> object:
        calls["echo"].append((message, err))
        return None

    # Happy path: runner completes without raising, so main_entry should
    # not raise and the custom echo should not be invoked.
    runtime_cli_main.main_entry(app_runner=successful_runner, echo=custom_echo)

    assert calls["runner"] == 1
    assert calls["echo"] == []
