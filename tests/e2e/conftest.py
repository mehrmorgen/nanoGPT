"""Auto-mark all tests in tests/e2e/ as end-to-end tests."""

from __future__ import annotations

from _pytest.mark.structures import MarkDecorator
import pytest

pytestmark: MarkDecorator = pytest.mark.e2e  # type: ignore[attr-defined]
