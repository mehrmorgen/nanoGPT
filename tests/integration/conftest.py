"""Auto-mark all tests in tests/integration/ as integration tests."""

from __future__ import annotations

from _pytest.mark.structures import MarkDecorator
import pytest

pytestmark: MarkDecorator = pytest.mark.integration  # type: ignore[attr-defined]
