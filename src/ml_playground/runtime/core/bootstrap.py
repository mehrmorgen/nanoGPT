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


_DEFAULT_FACTORY: Factory | None = None
_CURRENT: CLIDependencies | None = None


def configure_runtime_cli_dependencies(factory: Factory) -> None:
    """Register the factory used to build default runtime CLI dependencies."""

    global _DEFAULT_FACTORY, _CURRENT
    _DEFAULT_FACTORY = factory
    _CURRENT = factory()


def reset_runtime_cli_dependencies() -> None:
    """Reset the current runtime CLI dependencies to the default factory."""

    global _CURRENT
    if _DEFAULT_FACTORY is None:
        _CURRENT = None
    else:
        _CURRENT = _DEFAULT_FACTORY()


def get_runtime_cli_dependencies() -> CLIDependencies:
    """Return the currently configured runtime CLI dependencies."""

    global _CURRENT
    if _CURRENT is None:
        if _DEFAULT_FACTORY is None:  # pragma: no cover - defensive guard
            raise RuntimeError("Runtime CLI dependencies have not been configured.")
        _CURRENT = _DEFAULT_FACTORY()
    return _CURRENT


@contextmanager
def override_runtime_cli_dependencies(
    deps: CLIDependencies,
) -> Generator[None, None, None]:
    """Temporarily override runtime CLI dependencies within a context."""

    global _CURRENT
    previous = get_runtime_cli_dependencies()
    _CURRENT = deps
    try:
        yield
    finally:
        _CURRENT = previous


__all__ = [
    "CLIDependencies",
    "configure_runtime_cli_dependencies",
    "reset_runtime_cli_dependencies",
    "get_runtime_cli_dependencies",
    "override_runtime_cli_dependencies",
]
