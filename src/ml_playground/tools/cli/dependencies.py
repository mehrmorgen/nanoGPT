"""Dependency wiring for tools CLI factories.

This minimal implementation exists to satisfy property tests and provide
override hooks for injecting deterministic runners in test scenarios.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator

import typer

from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.cli.state import state

if TYPE_CHECKING:
    pass


Factory = Callable[..., object]


def _quality_factory(config: ToolsConfig, root_path: Path | None) -> object:
    from ml_playground.tools.quality.quality import QualityTools

    return QualityTools(config, root_path or Path.cwd())


def _testing_factory(config: ToolsConfig, root_path: Path | None) -> object:
    from ml_playground.tools.testing.testing import TestingTools

    return TestingTools(config, root_path or Path.cwd())


def _environment_factory(config: ToolsConfig, root_path: Path | None) -> object:
    from ml_playground.tools.environment.environment import EnvironmentTools

    return EnvironmentTools(config, root_path or Path.cwd())


def _ci_factory(config: ToolsConfig, root_path: Path | None) -> object:
    from ml_playground.tools.ci.ci import CITools

    return CITools(config, root_path or Path.cwd())


def _agentic_factory(config: ToolsConfig, root_path: Path | None) -> object:
    from ml_playground.tools.agentic.agentic import AgenticTools

    return AgenticTools(config, root_path or Path.cwd())


def _dev_factory(config: ToolsConfig, root_path: Path | None = None) -> object:
    from ml_playground.tools.dev.dev import DevTools

    return DevTools(config=config, root_path=root_path or Path.cwd())


@dataclass(frozen=True)
class ToolsDependencies:
    """Container for tool factories and config loaders."""

    load_config: Callable[[Path | None], ToolsConfig]
    quality_factory: Factory
    testing_factory: Factory
    environment_factory: Factory
    ci_factory: Factory
    agentic_factory: Factory
    dev_factory: Factory


def default_tools_dependencies() -> ToolsDependencies:
    """Provide default dependency wiring used by tests."""

    return ToolsDependencies(
        load_config=load_tools_config,
        quality_factory=_quality_factory,
        testing_factory=_testing_factory,
        environment_factory=_environment_factory,
        ci_factory=_ci_factory,
        agentic_factory=_agentic_factory,
        dev_factory=_dev_factory,
    )


def default_tool_result_handler(result: ToolResult) -> None:
    """Default handler for ToolResult output with learning mode support."""
    try:
        verbosity_level = VerbosityLevel(state.verbosity)
    except ValueError:
        verbosity_level = VerbosityLevel.STANDARD

    engine = LearningModeEngine(verbosity_level)
    output = engine.format_output(result, learning_enabled=state.learning_mode)
    typer.echo(output)
    if not result.success:
        raise typer.Exit(result.exit_code)


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


def reset_tools_dependencies() -> None:
    """Reset dependency overrides."""
    _dependency_stack.clear()


def configure_tools_dependencies(
    deps: ToolsDependencies | Callable[[], ToolsDependencies],
) -> None:
    """Set the active dependencies, replacing any overrides."""
    concrete_deps = deps() if callable(deps) else deps
    _dependency_stack.clear()
    _dependency_stack.append(concrete_deps)


__all__ = [
    "ToolsDependencies",
    "default_tools_dependencies",
    "default_tool_result_handler",
    "override_tools_dependencies",
    "get_tools_dependencies",
    "reset_tools_dependencies",
    "configure_tools_dependencies",
]
