"""Environment tools package (new layout).

Temporary shim that re-exports EnvironmentTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from ml_playground.tools.categories.environment import (
    EnvironmentTools as EnvironmentTools,
)

__all__ = ["EnvironmentTools"]
