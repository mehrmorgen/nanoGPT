# pyright: reportPrivateUsage=false
"""Tests for `ml_playground.tools.analysis.lit.integration` utilities."""

from __future__ import annotations

from contextlib import contextmanager


from ml_playground.tools.analysis.lit import integration


@contextmanager
def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    object.__setattr__(target, name, value)
    try:
        yield
    finally:
        if original is not missing:
            object.__setattr__(target, name, original)
        else:
            delattr(target, name)


def test_run_server_experiment_delegates_to_framework() -> None:
    """`run_server_experiment` should delegate to framework and restore overrides."""
    calls: list[str] = []

    def mock_run_server_experiment(**kwargs):
        calls.append(kwargs["experiment"])

    # We need to mock _integration.run_server_experiment because it's what's called inside the wrapper
    from ml_playground.framework.analysis.lit import (
        integration as framework_integration,
    )

    with override_attr(
        framework_integration, "run_server_experiment", mock_run_server_experiment
    ):
        # We also need to mock _resolve_experiment_lit_runner in the wrapper if it's used,
        # but the wrapper calls _integration.run_server_experiment directly.
        integration.run_server_experiment(experiment="test_exp")

    assert calls == ["test_exp"]


def test_run_server_bundestag_char_delegates_to_run_server_experiment() -> None:
    """`run_server_bundestag_char` should call `run_server_experiment` with fixed name."""
    calls: list[str] = []

    def mock_run_server_experiment(**kwargs):
        calls.append(kwargs["experiment"])

    with override_attr(
        integration, "run_server_experiment", mock_run_server_experiment
    ):
        integration.run_server_bundestag_char()

    assert calls == ["bundestag_char"]
