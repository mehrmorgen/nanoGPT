"""Quality tools package (new layout).

Temporary shim that re-exports QualityTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from ml_playground.tools.categories.quality import QualityTools as QualityTools
from ml_playground.tools.utils.subprocess_utils import (
    _default_runner as _default_runner,
)

__all__ = ["QualityTools", "_default_runner"]
