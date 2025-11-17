"""Property-based tests for `TestingTools` undercovered tree formatting."""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings, strategies as st

from ml_playground.tools.testing.coverage_helpers import format_undercovered_tree


def _coverage_entries() -> st.SearchStrategy[list[tuple[str, float, float | None]]]:
    segment = st.text(min_size=1, max_size=3, alphabet="abcdefghijklmnopqrstuvwxyz")
    path_strategy = st.lists(segment, min_size=2, max_size=4).map(
        lambda parts: "/".join(parts)
    )
    percentage = st.floats(
        min_value=0.01,
        max_value=99.99,
        allow_nan=False,
        allow_infinity=False,
    )
    branch_percentage = st.one_of(st.none(), percentage)
    entry_strategy = st.tuples(path_strategy, percentage, branch_percentage)
    return st.lists(
        entry_strategy, min_size=1, max_size=5, unique_by=lambda item: item[0]
    )


@settings(max_examples=20, deadline=20, derandomize=True)
@given(entries=_coverage_entries())
def test_format_undercovered_tree_represents_each_file(
    entries: list[tuple[str, float, float | None]],
) -> None:
    """Ensure `_format_undercovered_tree` produces a line for every undercovered file."""
    lines = format_undercovered_tree(entries)

    for path, line_pct, branch_pct in entries:
        suffix = f"{Path(path).name}: line = {line_pct:.2f}%"
        if branch_pct is not None:
            suffix += f" branch = {branch_pct:.2f}%"
        assert any(line.strip().endswith(suffix) for line in lines)

    for line in lines:
        stripped = line.lstrip(" └├│")
        assert ("└──" in line) or ("├──" in line)
        if stripped.endswith("/"):
            continue
        name, _, metrics = stripped.partition(": ")
        assert name
        assert metrics.startswith("line = ")
