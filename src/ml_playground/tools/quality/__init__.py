"""Quality tools package (new layout).

Temporary shim that re-exports QualityTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from ml_playground.tools.categories.quality import QualityTools as QualityTools

__all__ = ["QualityTools"]
