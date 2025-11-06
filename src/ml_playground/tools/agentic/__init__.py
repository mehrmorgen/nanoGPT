"""Agentic tools package (new layout).

Temporary shim that re-exports AgenticTools from the legacy categories module.
Keeps public API stable during migration.
"""

from __future__ import annotations

from .agentic import AgenticTools as AgenticTools

__all__ = ["AgenticTools"]
