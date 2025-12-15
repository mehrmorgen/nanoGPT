from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Optional

import ml_playground.tools.cli.main as tools_cli
from ml_playground.tools.cli.dependencies import (
    ToolsDependencies,
    default_tools_dependencies,
)
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult


def reset_tools_cli_state() -> None:
    tools_cli.state.learning_mode = False
    tools_cli.state.learning_mode_set = False
    tools_cli.state.verbosity = 1
    tools_cli.state.dry_run = False
    tools_cli.state.project_root = None
    tools_cli.state.config = None


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Generator[None, None, None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


@contextmanager
def override_env(var: str, value: Optional[str]) -> Generator[None, None, None]:
    original = os.environ.get(var)
    if value is None:
        os.environ.pop(var, None)
    else:
        os.environ[var] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = original


def deps(
    *,
    load_config: Optional[Callable[[Path | None], ToolsConfig]] = None,
    quality_factory: Optional[Callable[[ToolsConfig, Path], Any]] = None,
    testing_factory: Optional[Callable[[ToolsConfig, Path], Any]] = None,
    environment_factory: Optional[Callable[[ToolsConfig, Path], Any]] = None,
    ci_factory: Optional[Callable[[ToolsConfig, Path], Any]] = None,
    dev_factory: Optional[Callable[[ToolsConfig], Any]] = None,
    result_handler: Optional[Callable[[ToolResult], None]] = None,
) -> ToolsDependencies:
    defaults = default_tools_dependencies()
    return ToolsDependencies(
        load_config=load_config or defaults.load_config,
        quality_factory=quality_factory or defaults.quality_factory,
        testing_factory=testing_factory or defaults.testing_factory,
        environment_factory=environment_factory or defaults.environment_factory,
        ci_factory=ci_factory or defaults.ci_factory,
        dev_factory=dev_factory or defaults.dev_factory,
        result_handler=result_handler or defaults.result_handler,
    )
