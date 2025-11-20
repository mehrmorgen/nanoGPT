from __future__ import annotations

from types import SimpleNamespace


import ml_playground.runtime.cli.typer_helpers as typer_helpers
from ml_playground.runtime.core.results import VerbosityLevel


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
