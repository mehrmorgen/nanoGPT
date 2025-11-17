"""Shared helper functions for the tools CLI system."""

from pathlib import Path

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.quality.quality import QualityTools
from ml_playground.tools.testing.testing import TestingTools

# Import state and dependencies
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli.dependencies import get_tools_dependencies


def get_quality_tools() -> QualityTools:
    """Get quality tools instance."""
    from ml_playground.tools.cli.config_loader import ensure_config_loaded
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    deps = get_tools_dependencies()
    return deps.quality_factory(state.config, state.project_root or Path.cwd())


def get_testing_tools() -> TestingTools:
    """Get testing tools instance."""
    from ml_playground.tools.cli.config_loader import ensure_config_loaded
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    deps = get_tools_dependencies()
    return deps.testing_factory(state.config, state.project_root or Path.cwd())


def get_environment_tools() -> EnvironmentTools:
    """Get environment tools instance."""
    from ml_playground.tools.cli.config_loader import ensure_config_loaded
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    deps = get_tools_dependencies()
    return deps.environment_factory(state.config, state.project_root or Path.cwd())


def get_ci_tools() -> CITools:
    """Get CI tools instance."""
    from ml_playground.tools.cli.config_loader import ensure_config_loaded
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    deps = get_tools_dependencies()
    return deps.ci_factory(state.config, state.project_root or Path.cwd())


def get_dev_tools() -> DevTools:
    """Get dev tools instance."""
    from ml_playground.tools.cli.config_loader import ensure_config_loaded
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after ensure_config_loaded"
    )
    deps = get_tools_dependencies()
    return deps.dev_factory(state.config)


def handle_tool_result(result: ToolResult) -> None:
    """Handle tool result using current dependencies."""
    handler = get_tools_dependencies().result_handler
    handler(result)
