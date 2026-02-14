"""Auto-mark all tests in tests/acceptance/tools/cli/ as acceptance tests.

This keeps suite selection easy via `-m acceptance` and allows
separate reporting/CI handling for acceptance tests.
"""

from __future__ import annotations

from _pytest.mark.structures import MarkDecorator

from tests.acceptance.conftest import acceptance_marker

pytestmark: MarkDecorator = acceptance_marker
