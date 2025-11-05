"""Testing tools package (new layout).

Temporary shim that re-exports TestingTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from ml_playground.tools.categories.testing import TestingTools as TestingTools
from ml_playground.tools.utils.subprocess_utils import (
    _default_runner as _default_runner,
)

__all__ = ["TestingTools", "_default_runner"]
