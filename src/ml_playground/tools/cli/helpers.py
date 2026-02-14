"""Helpers for tools CLI command implementations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

import typer

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult

from .dependencies import get_tools_dependencies
from .state import state

if TYPE_CHECKING:
    from ml_playground.tools.agentic.agentic import AgenticTools
    from ml_playground.tools.ci.ci import CITools
    from ml_playground.tools.dev.dev import DevTools
    from ml_playground.tools.environment.environment import EnvironmentTools
    from ml_playground.tools.quality.quality import QualityTools
    from ml_playground.tools.testing.testing import TestingTools

__all__ = [
    "get_quality_tools",
    "get_testing_tools",
    "get_environment_tools",
    "get_ci_tools",
    "get_agentic_tools",
    "get_dev_tools",
    "handle_tool_result",
    "run_tool_command",
    "get_configured_root",
]


def get_configured_root() -> Path:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    return state.project_root or Path.cwd()


def get_quality_tools() -> QualityTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.quality_factory(state.config, get_configured_root())  # type: ignore[return-value]


def get_testing_tools() -> TestingTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.testing_factory(state.config, get_configured_root())  # type: ignore[return-value]


def get_environment_tools() -> EnvironmentTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.environment_factory(state.config, get_configured_root())  # type: ignore[return-value]


def get_ci_tools() -> CITools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.ci_factory(state.config, get_configured_root())  # type: ignore[return-value]


def get_agentic_tools() -> AgenticTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.agentic_factory(state.config, get_configured_root())  # type: ignore[return-value]


def get_dev_tools() -> DevTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    deps = get_tools_dependencies()
    return deps.dev_factory(state.config, get_configured_root())  # type: ignore[return-value]


def handle_tool_result(result: ToolResult) -> None:
    """Default handler for ToolResult output.

    Surfaces stdout/stderr and exits with tool's exit code if not successful.
    """
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)
    if not result.success:
        raise typer.Exit(result.exit_code)


def run_tool_command(
    command: Callable[..., ToolResult], *args: object, **kwargs: object
) -> None:
    try:
        result = command(*args, **kwargs)
        handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)
