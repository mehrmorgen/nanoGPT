"""Dev tools package (new layout).

Temporary shim that re-exports DevTools from the legacy categories module.
This keeps public API stable while we migrate code into this package.
"""

from __future__ import annotations

from .dev import DevTools as DevTools
from ml_playground.tools.utils.subprocess_utils import (
    _default_runner as _default_runner,
)

__all__ = ["DevTools", "_default_runner"]
