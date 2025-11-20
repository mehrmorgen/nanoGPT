"""Global state management for the tools CLI system."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ml_playground.tools.core.learning_mode import VerbosityLevel
from ml_playground.tools.protocols import ToolsConfigLike


@dataclass
class GlobalState:
    """Global state for the tools CLI system."""
    config: Optional[ToolsConfigLike] = None
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

    def reset(self) -> None:
        """Reset state to defaults without loading configuration."""
        self.config = None
        self.project_root = None
        self.verbosity = 1
        self.verbosity_level = VerbosityLevel.STANDARD
        self.learning_mode = False
        self.dry_run = False
        self.learning_mode_set = False


# Global state instance
state = GlobalState()
