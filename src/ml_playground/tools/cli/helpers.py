"""Helpers for tools CLI command implementations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import typer

from ml_playground.tools.core.interfaces import ToolResult

from .state import state

if TYPE_CHECKING:  # pragma: no cover - import-time type narrowing only
    from ml_playground.tools.agentic import AgenticTools
    from ml_playground.tools.ci import CITools
    from ml_playground.tools.dev import DevTools
    from ml_playground.tools.environment import EnvironmentTools
    from ml_playground.tools.quality import QualityTools
    from ml_playground.tools.testing import TestingTools

__all__ = [
    "get_quality_tools",
    "get_testing_tools",
    "get_environment_tools",
    "get_ci_tools",
    "get_agentic_tools",
    "get_dev_tools",
    "handle_tool_result",
]


def _get_configured_root() -> Path:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    return state.project_root or Path.cwd()


def get_quality_tools() -> QualityTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import quality

    return quality.QualityTools(state.config, _get_configured_root())


def get_testing_tools() -> TestingTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import testing

    return testing.TestingTools(state.config, _get_configured_root())


def get_environment_tools() -> EnvironmentTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import environment

    return environment.EnvironmentTools(state.config, _get_configured_root())


def get_ci_tools() -> CITools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import ci

    return ci.CITools(state.config, _get_configured_root())


def get_agentic_tools() -> AgenticTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import agentic

    return agentic.AgenticTools(state.config, _get_configured_root())


def get_dev_tools() -> DevTools:
    if state.config is None:
        raise RuntimeError("Tools config must be loaded before use")
    from ml_playground.tools import dev

    return dev.DevTools(config=state.config)


def handle_tool_result(result: ToolResult) -> None:
    if result.stdout:
        typer.echo(result.stdout)
    if result.stderr:
        typer.echo(result.stderr, err=True)
    if not result.success:
        raise typer.Exit(result.exit_code)
