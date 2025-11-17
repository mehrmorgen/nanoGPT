"""Global state management for the tools CLI system."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.learning_mode import VerbosityLevel


@dataclass
class GlobalState:
    """Global state for the tools CLI system."""

    config: Optional[ToolsConfig] = None
    project_root: Optional[Path] = None
    verbosity: int = 1  # Keep as int to match original usage
    verbosity_level: VerbosityLevel = VerbosityLevel.STANDARD
    learning_mode: bool = False
    dry_run: bool = False
    learning_mode_set: bool = field(default=False, init=False)

    def mark_learning_mode_explicit(self, value: bool = True) -> None:
        """Record that learning mode was explicitly configured."""
        self.learning_mode_set = value

    def mark_learning_mode_default(self, value: bool = True) -> None:
        """Record that learning mode default was applied from configuration."""
        self.learning_mode_set = value


# Global state instance
state = GlobalState()
