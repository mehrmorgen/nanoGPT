"""Dependency wiring for tools CLI factories.

This minimal implementation exists to satisfy property tests and provide
override hooks for injecting deterministic runners in test scenarios.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterator

from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.utils.subprocess_utils import RealSubprocessRunner


Factory = Callable[..., Any]


def _noop_factory(*_: Any, **__: Any) -> Any:
    """Return a simple namespace carrying a subprocess runner."""

    return SimpleNamespace(subprocess_runner=RealSubprocessRunner())


@dataclass(frozen=True)
class ToolsDependencies:
    """Container for tool factories and config loaders."""

    load_config: Callable[[Path | None], ToolsConfig]
    quality_factory: Factory
    testing_factory: Factory
    environment_factory: Factory
    ci_factory: Factory
    dev_factory: Factory
    result_handler: Callable[[ToolResult], None] | None = None


def default_tools_dependencies() -> ToolsDependencies:
    """Provide default dependency wiring used by tests."""

    return ToolsDependencies(
        load_config=load_tools_config,
        quality_factory=_noop_factory,
        testing_factory=_noop_factory,
        environment_factory=_noop_factory,
        ci_factory=_noop_factory,
        dev_factory=_noop_factory,
        result_handler=None,
    )


_dependency_stack: list[ToolsDependencies] = []


@contextmanager
def override_tools_dependencies(
    deps: ToolsDependencies,
) -> Iterator[None]:
    """Temporarily override tool dependencies for tests."""

    _dependency_stack.append(deps)
    try:
        yield
    finally:
        _dependency_stack.pop()


def get_tools_dependencies() -> ToolsDependencies:
    """Return the active dependencies, falling back to defaults."""

    if _dependency_stack:
        return _dependency_stack[-1]
    return default_tools_dependencies()


__all__ = [
    "ToolsDependencies",
    "default_tools_dependencies",
    "override_tools_dependencies",
    "get_tools_dependencies",
]
