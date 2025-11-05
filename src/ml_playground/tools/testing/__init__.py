"""Testing tools package (new layout).

Temporary shim that re-exports TestingTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from ml_playground.tools.categories.testing import TestingTools as TestingTools

__all__ = ["TestingTools"]
