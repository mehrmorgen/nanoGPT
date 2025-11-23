from __future__ import annotations

import shlex

import hypothesis.strategies as st
from hypothesis import given, settings

from ml_playground.tools.utils.subprocess_utils import (
    format_command,
)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(
    parts=st.lists(st.text(min_size=0, max_size=8), min_size=1, max_size=5),
)
def test_format_command_roundtrips(parts: list[str]) -> None:
    """format_command should preserve argument ordering and quoting semantics."""
    formatted = format_command(parts)
    rebuilt = shlex.split(formatted)

    assert rebuilt == parts
