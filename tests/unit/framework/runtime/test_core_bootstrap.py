from __future__ import annotations

import importlib
import pytest

from ml_playground.framework.runtime.core import bootstrap
from ml_playground.framework.runtime.core.bootstrap import CLIDependencies


def _dummy_deps(marker: str = "deps") -> CLIDependencies:
    return CLIDependencies(
        load_experiment=lambda experiment, exp_config_path: marker,  # type: ignore[return-value]
        ensure_train_prerequisites=lambda exp: marker,
        ensure_sample_prerequisites=lambda exp: marker,
        run_prepare=lambda experiment, cfg, path, metadata, deps, engine=None: marker,
        run_train=lambda experiment, cfg, path, metadata, deps, engine=None: marker,
        run_sample=lambda experiment, cfg, path, metadata, deps, engine=None: marker,
    )


def test_reset_cli_dependencies_without_default_resets_to_none() -> None:
    """Reset with no default factory should leave dependencies unset."""
    fresh = importlib.reload(bootstrap)
    fresh.reset_cli_dependencies()
    with pytest.raises(RuntimeError):
        fresh.get_cli_dependencies()


def test_get_cli_dependencies_raises_when_unconfigured() -> None:
    """Ensure missing factory raises to prevent silent None returns."""
    fresh = importlib.reload(bootstrap)
    with pytest.raises(RuntimeError, match="not been configured"):
        fresh.get_cli_dependencies()


def test_reset_cli_dependencies_clears_cached_instance() -> None:
    """Reset should drop cached deps so a new instance is built next time."""
    fresh = importlib.reload(bootstrap)
    calls: list[str] = []

    def factory_one() -> CLIDependencies:
        calls.append("one")
        return _dummy_deps("one")

    fresh.configure_cli_dependencies(factory_one)
    deps_first = fresh.get_cli_dependencies()

    fresh.reset_cli_dependencies()

    def factory_two() -> CLIDependencies:
        calls.append("two")
        return _dummy_deps("two")

    fresh.configure_cli_dependencies(factory_two)
    deps_second = fresh.get_cli_dependencies()

    # factory_one called once when fetching deps_first
    # factory_two called once when fetching deps_second
    assert calls == ["one", "two"]
    assert deps_first is not deps_second


def test_get_cli_dependencies_uses_default_factory_once() -> None:
    """Configured factory should be invoked once and cached."""
    fresh = importlib.reload(bootstrap)
    calls: list[str] = []

    def factory() -> CLIDependencies:
        calls.append("called")
        return _dummy_deps()

    fresh.configure_cli_dependencies(factory)

    deps = fresh.get_cli_dependencies()
    assert isinstance(deps, CLIDependencies)
    assert calls == ["called"]

    # cached path should not call factory again
    deps_again = fresh.get_cli_dependencies()
    assert deps_again is deps
    assert calls == ["called"]
