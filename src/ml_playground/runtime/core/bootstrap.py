from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generator


@dataclass(frozen=True)
class CLIDependencies:
    """Container holding injectable runtime CLI dependencies."""

    load_experiment: Callable[[str, Path | None], Any]
    ensure_train_prerequisites: Callable[[Any], Any]
    ensure_sample_prerequisites: Callable[[Any], Any]
    run_prepare: Callable[[str, Any, Path, Any, Any | None], Any]
    run_train: Callable[[str, Any, Path, Any, Any | None], Any]
    run_sample: Callable[[str, Any, Path, Any, Any | None], Any]


Factory = Callable[[], CLIDependencies]


_default_factory: Factory | None = None
_current: CLIDependencies | None = None


def configure_runtime_cli_dependencies(factory: Factory) -> None:
    """Register the factory used to build default runtime CLI dependencies."""

    global _default_factory, _current
    _default_factory = factory
    _current = factory()


def reset_runtime_cli_dependencies() -> None:
    """Reset the current runtime CLI dependencies to the default factory."""

    global _current
    if _default_factory is None:
        _current = None
    else:
        _current = _default_factory()


def get_runtime_cli_dependencies() -> CLIDependencies:
    """Return the currently configured runtime CLI dependencies."""

    global _current
    if _current is None:
        if _default_factory is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Runtime CLI dependencies have not been configured.")
        _current = _default_factory()
    return _current


@contextmanager
def override_runtime_cli_dependencies(
    deps: CLIDependencies,
) -> Generator[None, None, None]:
    """Temporarily override runtime CLI dependencies within a context."""

    global _current
    previous = _current
    try:
        _current = deps
        yield
    finally:
        _current = previous


__all__ = [
    "CLIDependencies",
    "configure_runtime_cli_dependencies",
    "reset_runtime_cli_dependencies",
    "get_runtime_cli_dependencies",
    "override_runtime_cli_dependencies",
]
