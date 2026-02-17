"""Compatibility coverage for legacy dev property module path.

The substantive property invariants live in `test_dev.py`.
This module remains to preserve path-level policy coverage without duplicating
heavy Hypothesis workloads.
"""

from __future__ import annotations

from ml_playground.tools.dev.dev import apply_filters, comment_lookup


def test_dev_property_exports_available() -> None:
    """Legacy property module should continue exposing core helper callables."""
    assert callable(comment_lookup)
    assert callable(apply_filters)
