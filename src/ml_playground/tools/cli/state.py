"""Shared tools CLI state and config helpers."""

from __future__ import annotations

from pathlib import Path
import os
from typing import cast, Callable, Optional

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


def load_config_with_error_handling(
    project_root: Path | None = None, _loader_override: object = None
) -> None:
    """Load configuration with rich error handling for CLI usage.

    Uses ``_loader_override`` for testing without patching, or the module-level
    ``load_tools_config`` alias to avoid circular imports.
    """
    try:
        if _loader_override is not None:
            loader_raw = _loader_override
        else:
            # Resolve through the public CLI module so test doubles swapped at
            # ``ml_playground.tools.cli.load_tools_config`` are honored.
            cli_mod: object = import_module("ml_playground.tools.cli.state")
            # Use a typed variable for the loader to satisfy strict Pyright
            loader_raw = cast(object, getattr(cli_mod, "load_tools_config"))

        if not callable(loader_raw):
            from ml_playground.tools.core.errors import ToolConfigurationError

            raise ToolConfigurationError(
                "load_tools_config not found in ml_playground.tools.cli",
                reason="Configuration loader missing",
                rationale="Configuration loader must be available in the public CLI module",
            )
        loader = cast(Callable[[Optional[Path]], object], loader_raw)

        # Now we can safely call it knowing it is object typed
        from ml_playground.tools.core.config import ToolsConfig

        raw_config: object = loader(project_root)
        state.config = cast(ToolsConfig, raw_config)
        state.project_root = project_root

        if not state._learning_mode_set:  # pyright: ignore[reportPrivateUsage]
            state.learning_mode = state.config.learning_mode_default
            state.verbosity = state.config.default_verbosity

    except tools_runtime.ToolConfigurationError as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(1) from exc
    except Exception as exc:
        typer.echo(f"Unexpected error loading configuration: {exc}", err=True)
        raise typer.Exit(1) from exc


def apply_cli_options(
    learning_mode: bool | None, verbosity: int | None, dry_run: bool
) -> None:
    """Apply global CLI options and manage dry-run env flag."""
    if learning_mode is not None:
        state.learning_mode = learning_mode
        state.mark_learning_mode_explicit(True)

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
