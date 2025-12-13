"""Configuration loading utilities for the tools CLI system."""

from pathlib import Path
from typing import Optional

import typer

from ml_playground.tools.core.errors import ToolConfigurationError

# Import state and dependencies
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli.dependencies import (
    get_tools_dependencies,
    ToolsDependencies,
)


def load_config_with_error_handling(
    project_root: Path | None = None,
    *,
    deps: Optional["ToolsDependencies"] = None,
) -> None:
    """Load configuration with proper error handling."""
    try:
        dependencies = deps or get_tools_dependencies()
        # Reuse the cached configuration when no project root override is provided.
        if project_root is None and state.config is not None:
            return

        if project_root is not None:
            if not project_root.exists():
                typer.echo(f"Project root not found: {project_root}", err=True)
                raise typer.Exit(2)
            if not project_root.is_dir():
                typer.echo(f"Project root is not a directory: {project_root}", err=True)
                raise typer.Exit(2)

        target_root = project_root if project_root is not None else state.project_root

        loaded_config = dependencies.load_config(target_root)

        # Apply configuration defaults to global state if not already set
        # We read from the local variable to avoid depending on state.config being set yet
        if not state.learning_mode_set:
            state.learning_mode = loaded_config.learning_mode_default
            state.mark_learning_mode_default(True)

        state.verbosity = loaded_config.default_verbosity

        # Finally store the config
        state.config = loaded_config

        if project_root is not None:
            state.project_root = project_root

    except ToolConfigurationError as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(1)
    except (AttributeError, TypeError, ValueError, OSError) as e:
        typer.echo(f"Unexpected error loading configuration: {e}", err=True)
        raise typer.Exit(1)


def ensure_config_loaded() -> None:
    """Common helper to ensure config is loaded, eliminating repeated None-checks."""
    if state.config is None:
        load_config_with_error_handling(state.project_root)
