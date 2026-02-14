from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from ml_playground.tools.cli.dependencies import (
    ToolsDependencies,
    default_tools_dependencies,
)
from ml_playground.tools.cli.state import state as cli_state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult

_MISSING = object()

# Match ml_playground.tools.cli.dependencies.ToolsDependencies signatures
LoadConfig = Callable[[Path | None], ToolsConfig]
Factory = Callable[[ToolsConfig, Path | None], object]
ResultHandler = Callable[[ToolResult], None]


@contextmanager
def override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    original = getattr(obj, name, _MISSING)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            object.__setattr__(obj, name, original)


@contextmanager
def override_env(name: str, value: str | None) -> Iterator[None]:
    original = os.environ.get(name)
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = original


def deps(
    *,
    load_config: LoadConfig | None = None,
    quality_factory: Factory | None = None,
    testing_factory: Factory | None = None,
    environment_factory: Factory | None = None,
    ci_factory: Factory | None = None,
    agentic_factory: Factory | None = None,
    dev_factory: Factory | None = None,
) -> ToolsDependencies:
    base = default_tools_dependencies()
    load_config_fn: LoadConfig = load_config or base.load_config
    quality_factory_fn: Factory = quality_factory or base.quality_factory
    testing_factory_fn: Factory = testing_factory or base.testing_factory
    environment_factory_fn: Factory = environment_factory or base.environment_factory
    ci_factory_fn: Factory = ci_factory or base.ci_factory
    agentic_factory_fn: Factory = agentic_factory or base.agentic_factory
    dev_factory_fn: Factory = dev_factory or base.dev_factory
    return ToolsDependencies(
        load_config=load_config_fn,
        quality_factory=quality_factory_fn,
        testing_factory=testing_factory_fn,
        environment_factory=environment_factory_fn,
        ci_factory=ci_factory_fn,
        agentic_factory=agentic_factory_fn,
        dev_factory=dev_factory_fn,
    )


def reset_tools_cli_state() -> None:
    cli_state.reset()


__all__ = [
    "deps",
    "override_attr",
    "override_env",
    "reset_tools_cli_state",
]
