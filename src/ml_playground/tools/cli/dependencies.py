"""Dependency management and factory functions for the tools CLI system."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import typer

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.cli.state import state
from ml_playground.tools.core.config import load_tools_config, ToolsConfig
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.quality.quality import QualityTools
from ml_playground.tools.testing.testing import TestingTools


def default_tool_result_handler(result: ToolResult) -> None:
    """Default handler for tool results that outputs to CLI and exits on failure."""
    # When learning mode is enabled, format output using the shared
    # LearningModeEngine so that educational content attached to the
    # ToolResult is surfaced consistently.
    if state.learning_mode:
        try:
            verbosity = VerbosityLevel(state.verbosity)
        except ValueError:
            # Fall back to STANDARD if an unexpected verbosity value is present.
            verbosity = VerbosityLevel.STANDARD

        engine = LearningModeEngine(verbosity)
        formatted_output = engine.format_output(result, learning_enabled=True)
        typer.echo(formatted_output)
    else:
        if result.stdout:
            typer.echo(result.stdout)
        if result.stderr:
            typer.echo(result.stderr, err=True)
    if not result.success:
        raise typer.Exit(result.exit_code)


@dataclass(slots=True)
class ToolsDependencies:
    """Container for all tool dependencies with factory functions."""

    load_config: Callable[[Path | None], ToolsConfig]
    quality_factory: Callable[[ToolsConfig, Path], QualityTools]
    testing_factory: Callable[[ToolsConfig, Path], TestingTools]
    environment_factory: Callable[[ToolsConfig, Path], EnvironmentTools]
    ci_factory: Callable[[ToolsConfig, Path], CITools]
    dev_factory: Callable[[ToolsConfig], DevTools]
    result_handler: Callable[[ToolResult], None]


def default_tools_dependencies() -> ToolsDependencies:
    """Create default dependencies with standard factory functions."""

    def _load_config(project_root: Path | None = None) -> ToolsConfig:
        return load_tools_config(project_root)

    def _quality_factory(config: ToolsConfig, project_root: Path) -> QualityTools:
        return QualityTools(config, project_root)

    def _testing_factory(config: ToolsConfig, project_root: Path) -> TestingTools:
        return TestingTools(config, project_root)

    def _environment_factory(
        config: ToolsConfig, project_root: Path
    ) -> EnvironmentTools:
        return EnvironmentTools(config, project_root)

    def _ci_factory(config: ToolsConfig, project_root: Path) -> CITools:
        return CITools(config, project_root)

    def _dev_factory(config: ToolsConfig) -> DevTools:
        return DevTools(config=config)

    return ToolsDependencies(
        load_config=_load_config,
        quality_factory=_quality_factory,
        testing_factory=_testing_factory,
        environment_factory=_environment_factory,
        ci_factory=_ci_factory,
        dev_factory=_dev_factory,
        result_handler=default_tool_result_handler,
    )


# Global dependency management
_dependency_factory: Callable[[], ToolsDependencies] = default_tools_dependencies
_cached_dependencies: Optional[ToolsDependencies] = None


def configure_tools_dependencies(factory: Callable[[], ToolsDependencies]) -> None:
    """Configure the dependency factory for tools."""
    global _dependency_factory, _cached_dependencies
    _dependency_factory = factory
    _cached_dependencies = None


def reset_tools_dependencies() -> None:
    """Reset tools dependencies to default factory."""
    configure_tools_dependencies(default_tools_dependencies)


def get_tools_dependencies() -> ToolsDependencies:
    """Get the current tools dependencies, creating them if needed."""
    global _cached_dependencies
    if _cached_dependencies is None:
        _cached_dependencies = _dependency_factory()
    return _cached_dependencies


@contextmanager
def override_tools_dependencies(deps: ToolsDependencies):
    """Context manager to temporarily override tools dependencies."""
    global _cached_dependencies
    previous_factory = _dependency_factory
    previous_cached = _cached_dependencies
    configure_tools_dependencies(lambda: deps)
    try:
        yield
    finally:
        configure_tools_dependencies(previous_factory)
        _cached_dependencies = previous_cached
