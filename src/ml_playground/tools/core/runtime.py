from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from ml_playground.tools.core.config import ToolsConfig, load_tools_config
from ml_playground.tools.core.errors import ToolConfigurationError


class ToolsCLIState:
    """Mutable state container shared across the tools CLI surface."""

    def __init__(self) -> None:
        self.learning_mode: bool = False
        self.verbosity: int = 1
        self.dry_run: bool = False
        self.project_root: Path | None = None
        self.config: ToolsConfig | None = None
        self._learning_mode_set: bool = False

    def reset(self) -> None:
        """Reset state to defaults without loading configuration."""
        self.learning_mode = False
        self.verbosity = 1
        self.dry_run = False
        self.project_root = None
        self.config = None
        self._learning_mode_set = False

    def apply_learning_defaults(self) -> None:
        """Apply learning defaults from loaded config unless explicitly set."""
        if self._learning_mode_set or self.config is None:
            return
        self.learning_mode = self.config.learning_mode_default
        self.verbosity = self.config.default_verbosity

    @property
    def learning_mode_set(self) -> bool:
        """Whether learning mode was explicitly set via CLI options."""
        return self._learning_mode_set

    def mark_learning_mode_default(self, value: bool) -> None:
        """Record that learning mode was set from defaults."""
        self._learning_mode_set = value

    def mark_learning_mode_explicit(self, value: bool = True) -> None:
        """Record that learning mode was explicitly set via CLI options."""
        self._learning_mode_set = value


state = ToolsCLIState()


def reset_state() -> None:
    """Reset the shared tools CLI state."""
    state.reset()


def set_config(config: ToolsConfig, project_root: Optional[Path] = None) -> None:
    """Inject a preloaded configuration for the tools CLI state."""
    state.config = config
    state.project_root = project_root
    state.apply_learning_defaults()


def load_config_with_error_handling(project_root: Path | None = None) -> None:
    """Load configuration with rich error handling for CLI usage."""
    try:
        state.config = load_tools_config(project_root)
        state.project_root = project_root

        state.apply_learning_defaults()

    except ToolConfigurationError as exc:
        typer.echo(f"Configuration error: {exc}", err=True)
        raise typer.Exit(1) from exc


__all__ = [
    "state",
    "reset_state",
    "set_config",
    "load_config_with_error_handling",
    "ToolsCLIState",
]
