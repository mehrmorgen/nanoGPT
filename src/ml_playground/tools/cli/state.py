"""Shared tools CLI state and config helpers."""

from __future__ import annotations

from pathlib import Path
import os

import typer

from importlib import import_module

from ml_playground.tools.core import runtime as tools_runtime
from ml_playground.tools.core.runtime import (
    ToolsCLIState,
    reset_state,
    set_config,
    state,
)

GlobalState = ToolsCLIState
load_tools_config = tools_runtime.load_tools_config


def load_config_with_error_handling(project_root: Path | None = None) -> None:
    """Load configuration with rich error handling for CLI usage.

    Uses the module-level ``load_tools_config`` alias so test doubles can
    override configuration loading without reaching into the core runtime.
    """
    try:
        # Resolve through the public CLI module so test doubles swapped at
        # ``ml_playground.tools.cli.load_tools_config`` are honored.
        loader = import_module("ml_playground.tools.cli").load_tools_config
        state.config = loader(project_root)
        state.project_root = project_root

        if not state._learning_mode_set:  # pyright: ignore[reportPrivateUsage]
            state.learning_mode = state.config.learning_mode_default
            state.verbosity = state.config.default_verbosity

    except tools_runtime.ToolConfigurationError as exc:  # pragma: no cover
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:  # pragma: no cover - defensive catch for CLI surface
        typer.echo(f"Unexpected error loading configuration: {exc}", err=True)
        raise typer.Exit(1) from exc


def apply_cli_options(
    learning_mode: bool | None, verbosity: int | None, dry_run: bool
) -> None:
    """Apply global CLI options and manage dry-run env flag."""
    if learning_mode is not None:
        state.learning_mode = learning_mode
        state._learning_mode_set = True  # pyright: ignore[reportPrivateUsage]

    if verbosity is not None:
        state.verbosity = verbosity

    state.dry_run = dry_run
    if dry_run:
        os.environ["ML_PLAYGROUND_TOOLS_DRY_RUN"] = "1"
    else:
        os.environ.pop("ML_PLAYGROUND_TOOLS_DRY_RUN", None)


__all__ = [
    "GlobalState",
    "state",
    "load_tools_config",
    "load_config_with_error_handling",
    "reset_state",
    "set_config",
    "apply_cli_options",
]
