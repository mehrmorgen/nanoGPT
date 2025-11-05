"""Dev tools package (new layout).

Temporary shim that re-exports DevTools from the legacy categories module.
This keeps public API stable while we migrate code into this package.
"""

from __future__ import annotations

from ml_playground.tools.categories.dev import DevTools as DevTools

__all__ = ["DevTools"]
