"""Shared helper functions for the tools CLI system."""

from pathlib import Path
from typing import Any, Callable, cast

import click
import typer
import typer.core

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.cli.config_loader import ensure_config_loaded
from ml_playground.tools.cli.dependencies import get_tools_dependencies
from ml_playground.tools.cli.state import state
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.quality.quality import QualityTools
from ml_playground.tools.testing.testing import TestingTools
from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
)


class OrderedGroup(typer.core.TyperGroup):
    """Click Group that lists commands alphabetically."""

    def list_commands(self, ctx: click.Context) -> list[str]:
        return sorted(self.commands)


def get_quality_tools() -> QualityTools:
    """Get quality tools instance."""
    deps = get_tools_dependencies()
    config = _ensure_active_config()
    return deps.quality_factory(config, state.project_root or Path.cwd())


def get_testing_tools() -> TestingTools:
    """Get testing tools instance."""
    deps = get_tools_dependencies()
    config = _ensure_active_config()
    return deps.testing_factory(config, state.project_root or Path.cwd())


def get_environment_tools() -> EnvironmentTools:
    """Get environment tools instance."""
    deps = get_tools_dependencies()
    config = _ensure_active_config()
    return deps.environment_factory(config, state.project_root or Path.cwd())


def get_ci_tools() -> CITools:
    """Get CI tools instance."""
    deps = get_tools_dependencies()
    config = _ensure_active_config()
    return deps.ci_factory(config, state.project_root or Path.cwd())


def get_dev_tools() -> DevTools:
    """Get dev tools instance."""
    deps = get_tools_dependencies()
    config = _ensure_active_config()
    return deps.dev_factory(config)


def handle_tool_result(result: ToolResult) -> None:
    """Handle tool result using current dependencies."""
    handler = get_tools_dependencies().result_handler
    handler(result)


def run_tool_command(
    command_func: Callable[..., ToolResult], *args: Any, **kwargs: Any
) -> None:
    """Execute a tool command with standardized error handling.

    This helper eliminates repetitive try/except patterns in CLI commands
    by wrapping command execution and handling common error types.

    Args:
        command_func: Function that returns a ToolResult when called
        *args: Positional arguments to pass to command_func
        **kwargs: Keyword arguments to pass to command_func
    """
    try:
        result: ToolResult = command_func(*args, **kwargs)
        handle_tool_result(result)
    except typer.Exit:
        raise
    except (ToolExecutionError, ToolConfigurationError) as exc:
        error_result = ToolResult.create(
            success=False,
            exit_code=1,
            namespace="tools",
            category="utils",
            command="generic-error",
            stderr=str(exc),
        )
        handle_tool_result(error_result)


def _ensure_active_config() -> ToolsConfig:
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    return cast(ToolsConfig, state.config)
