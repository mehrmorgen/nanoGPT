from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator


import ml_playground.runtime.cli.typer_helpers as typer_helpers
from ml_playground.runtime.core.results import VerbosityLevel


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_prepare_learning_context_reads_mapping_overrides() -> None:
    ctx = SimpleNamespace(obj={"learning_mode": True, "verbosity": 2, "extra": "x"})

    learning_mode, verbosity, overrides = typer_helpers.prepare_learning_context(ctx)  # type: ignore[arg-type]

    assert learning_mode is True
    assert verbosity == VerbosityLevel.COMPREHENSIVE
    assert overrides == {"learning_mode": True, "verbosity": 2, "extra": "x"}


def test_prepare_learning_context_defaults_when_obj_not_mapping() -> None:
    ctx = SimpleNamespace(obj=None)

    learning_mode, verbosity, overrides = typer_helpers.prepare_learning_context(ctx)  # type: ignore[arg-type]

    assert learning_mode is False
    assert verbosity == VerbosityLevel.STANDARD
    assert overrides == {}


def test_complete_experiments_delegates_to_runtime_helpers() -> None:
    calls: list[tuple[object, str]] = []

    def fake_complete_experiments(ctx: object, incomplete: str) -> list[str]:
        calls.append((ctx, incomplete))
        return ["a", "b"]

    ctx = SimpleNamespace()
    with override_attr(
        typer_helpers.rt_helpers,
        "complete_experiments",
        fake_complete_experiments,
    ):
        result = typer_helpers.complete_experiments(ctx, "in")  # type: ignore[arg-type]

    assert result == ["a", "b"]
    assert calls == [(ctx, "in")]
