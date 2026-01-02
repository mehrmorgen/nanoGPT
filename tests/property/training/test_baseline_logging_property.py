from __future__ import annotations

import string

from hypothesis import given, settings, strategies as st

from ml_playground.self_play.baseline_logging import (
    baseline_metric_items,
    emit_baseline_metrics,
)


def _metric_map_strategy() -> st.SearchStrategy[dict[str, float]]:
    keys = st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=6)
    values = st.floats(
        min_value=-100.0,
        max_value=100.0,
        allow_nan=False,
        allow_infinity=False,
    )
    return st.dictionaries(keys=keys, values=values, min_size=1, max_size=6)


@settings(max_examples=40, deadline=50, derandomize=True)
@given(
    current_metrics=_metric_map_strategy(),
    baseline_metrics=_metric_map_strategy(),
    current_depth=st.integers(min_value=0, max_value=10),
    baseline_depth=st.integers(min_value=0, max_value=10),
    include_baseline=st.booleans(),
)
def test_baseline_logging_items_include_expected_depths(
    current_metrics: dict[str, float],
    baseline_metrics: dict[str, float],
    current_depth: int,
    baseline_depth: int,
    include_baseline: bool,
) -> None:
    """Baseline logging includes current metrics and optional baseline metrics."""
    items = baseline_metric_items(
        current_metrics,
        baseline_metrics,
        current_depth=current_depth,
        baseline_depth=baseline_depth,
        include_baseline=include_baseline,
    )
    current_prefix = f"self_play.depth.{current_depth}."
    baseline_prefix = f"self_play.depth.{baseline_depth}."

    assert all(
        name.startswith(current_prefix) or name.startswith(baseline_prefix)
        for name, _ in items
    )
    assert any(name.startswith(current_prefix) for name, _ in items)
    if include_baseline:
        assert any(name.startswith(baseline_prefix) for name, _ in items)
    else:
        assert all(name.startswith(current_prefix) for name, _ in items)


@settings(max_examples=30, deadline=50, derandomize=True)
@given(
    current_metrics=_metric_map_strategy(),
    baseline_metrics=_metric_map_strategy(),
    current_depth=st.integers(min_value=0, max_value=10),
    baseline_depth=st.integers(min_value=0, max_value=10),
    include_baseline=st.booleans(),
)
def test_emit_baseline_metrics_forwards_items(
    current_metrics: dict[str, float],
    baseline_metrics: dict[str, float],
    current_depth: int,
    baseline_depth: int,
    include_baseline: bool,
) -> None:
    """Emission forwards the same items as baseline_metric_items."""
    expected = baseline_metric_items(
        current_metrics,
        baseline_metrics,
        current_depth=current_depth,
        baseline_depth=baseline_depth,
        include_baseline=include_baseline,
    )
    emitted: list[tuple[str, float]] = []

    def _emit(name: str, value: float) -> None:
        emitted.append((name, value))

    emit_baseline_metrics(
        _emit,
        current_metrics,
        baseline_metrics,
        current_depth=current_depth,
        baseline_depth=baseline_depth,
        include_baseline=include_baseline,
    )
    assert emitted == expected
