"""Runtime CLI dependencies - DEPRECATED

This module previously contained wrapper functions that forwarded to runtime.core.bootstrap.
All functionality has been moved to runtime.core.bootstrap directly to eliminate indirection.

Use ml_playground.runtime.core.bootstrap instead of this module.
"""

from __future__ import annotations

# Re-export CLIDependencies for backward compatibility during migration
from ml_playground.runtime.core.bootstrap import CLIDependencies

__all__ = ["CLIDependencies"]
