"""CI tools package (new layout).

Temporary shim that re-exports CITools from the legacy categories module.
This keeps public API stable while we migrate code into this package.
"""

from __future__ import annotations

from ml_playground.tools.categories.ci import CITools as CITools

__all__ = ["CITools"]
