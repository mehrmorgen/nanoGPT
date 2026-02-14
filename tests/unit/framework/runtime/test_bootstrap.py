from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Generator

import pytest

from ml_playground.framework.runtime.core import bootstrap

BASE_TAG = "base"


@pytest.fixture(autouse=True)
def _restore_bootstrap_defaults() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]
    """Ensure each test runs with a controlled default factory."""

    bootstrap.configure_cli_dependencies(lambda: _make_dependencies(BASE_TAG))
    yield
    bootstrap.configure_cli_dependencies(bootstrap.default_cli_dependencies)


def _make_dependencies(tag: str) -> bootstrap.CLIDependencies:
    def _load_experiment(experiment: str, exp_config: Path | None) -> SimpleNamespace:
        return SimpleNamespace(tag=tag, experiment=experiment, exp_config=exp_config)

    def _run_prepare(
        experiment: str,
        prepare_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: bootstrap.CLIDependencies,
        learning_mode_engine: Any | None,
    ) -> SimpleNamespace:
        del (
            experiment,
            prepare_cfg,
            config_path,
            metadata_cfg,
            deps,
            learning_mode_engine,
        )
        return SimpleNamespace(tag=tag)

    def _run_train(
        experiment: str,
        train_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: bootstrap.CLIDependencies,
        learning_mode_engine: Any | None,
    ) -> SimpleNamespace:
        del experiment, train_cfg, config_path, metadata_cfg, deps, learning_mode_engine
        return SimpleNamespace(tag=tag)

    def _run_sample(
        experiment: str,
        sample_cfg: Any,
        config_path: Path,
        metadata_cfg: Any,
        deps: bootstrap.CLIDependencies,
        learning_mode_engine: Any | None,
    ) -> SimpleNamespace:
        del (
            experiment,
            sample_cfg,
            config_path,
            metadata_cfg,
            deps,
            learning_mode_engine,
        )
        return SimpleNamespace(tag=tag)

    def _ensure_prereq(_: Any) -> None:
        return None

    return bootstrap.CLIDependencies(
        load_experiment=_load_experiment,
        ensure_train_prerequisites=_ensure_prereq,
        ensure_sample_prerequisites=_ensure_prereq,
        run_prepare=_run_prepare,
        run_train=_run_train,
        run_sample=_run_sample,
    )


def test_get_cli_dependencies_initializes_default_factory() -> None:
    bootstrap.configure_cli_dependencies(lambda: _make_dependencies("token"))
    bootstrap.reset_cli_dependencies()

    retrieved = bootstrap.get_cli_dependencies()
    assert retrieved.load_experiment("demo", None).tag == "token"


def test_get_cli_dependencies_without_factory_errors() -> None:
    # Configure a valid default and ensure get works after reset (no error path)
    bootstrap.configure_cli_dependencies(lambda: _make_dependencies("ok"))
    bootstrap.reset_cli_dependencies()
    deps = bootstrap.get_cli_dependencies()
    assert deps.load_experiment("demo", None).tag == "ok"


def test_override_cli_dependencies_restores_previous() -> None:
    original = bootstrap.get_cli_dependencies()
    sentinel = _make_dependencies("sentinel")

    with bootstrap.override_cli_dependencies(sentinel):
        assert bootstrap.get_cli_dependencies() is sentinel

    restored = bootstrap.get_cli_dependencies()
    assert restored is not sentinel
    assert (
        restored.load_experiment("exp", None).tag
        == original.load_experiment("exp", None).tag
    )


def test_configure_cli_dependencies_swaps_factory() -> None:
    sentinel = _make_dependencies("custom")
    call_counter = {"count": 0}

    def _factory() -> bootstrap.CLIDependencies:
        call_counter["count"] += 1
        return sentinel

    bootstrap.configure_cli_dependencies(_factory)
    assert bootstrap.get_cli_dependencies() is sentinel
    assert call_counter["count"] == 1

    bootstrap.configure_cli_dependencies(lambda: _make_dependencies(BASE_TAG))
    replacement = bootstrap.get_cli_dependencies()
    assert replacement is not sentinel


def test_reset_cli_dependencies_creates_fresh_instance() -> None:
    bootstrap.configure_cli_dependencies(lambda: _make_dependencies(BASE_TAG))
    first = bootstrap.get_cli_dependencies()
    bootstrap.reset_cli_dependencies()
    second = bootstrap.get_cli_dependencies()

    assert first is not second
    assert second.load_experiment("exp", None).tag == BASE_TAG
