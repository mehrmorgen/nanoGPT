from __future__ import annotations
from pathlib import Path
from typing import Optional

import typer
from ml_playground.tools.core.config import load_tools_config
from ml_playground.tools.core.errors import ToolConfigurationError
from ml_playground.tools.cli.state import state
from ml_playground.tools.protocols import ToolsConfigLike


def reset_state() -> None:
    """Reset the shared tools CLI state."""
    state.reset()


def set_config(config: ToolsConfigLike, project_root: Optional[Path] = None) -> None:
    """Inject a preloaded configuration for the tools CLI state."""
    state.config = config
    state.project_root = project_root


def load_config_with_error_handling(project_root: Path | None = None) -> None:
    """Load configuration with rich error handling for CLI usage."""
    try:
        state.config = load_tools_config(project_root)
        state.project_root = project_root

        config = state.config
        assert config is not None

        if not state.learning_mode:
            state.learning_mode = config.learning_mode_default
            state.mark_learning_mode_default(True)
        state.verbosity = config.default_verbosity

    except ToolConfigurationError as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        typer.echo(f"Unexpected error loading configuration: {exc}", err=True)
        raise typer.Exit(1) from exc


__all__ = [
    "state",
    "reset_state",
    "set_config",
    "load_config_with_error_handling",
]
