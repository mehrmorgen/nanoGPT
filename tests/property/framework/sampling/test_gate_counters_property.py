from __future__ import annotations

import math

from hypothesis import given, settings, strategies as st

from ml_playground.framework.self_play.utilities import (
    Outcome,
    accumulate_outcomes,
    emit_gate_metrics,
    gate_metrics,
)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    outcomes=st.lists(st.sampled_from(list(Outcome)), max_size=40)
)
def test_gate_counts_accumulate_matches_outcomes(outcomes: list[Outcome]) -> None:
    """Gate counters match outcome frequency and totals."""
    counts = accumulate_outcomes(outcomes)
    assert counts.total == len(outcomes)
    assert counts.wins == outcomes.count(Outcome.WIN)
    assert counts.losses == outcomes.count(Outcome.LOSS)
    assert counts.draws == outcomes.count(Outcome.DRAW)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    outcomes=st.lists(st.sampled_from(list(Outcome)), max_size=40)
)
def test_gate_metrics_rate_bounds(outcomes: list[Outcome]) -> None:
    """Gate metrics rates are bounded and normalized."""
    counts = accumulate_outcomes(outcomes)
    metrics = gate_metrics(counts)
    win_rate = metrics["win_rate"]
    loss_rate = metrics["loss_rate"]
    draw_rate = metrics["draw_rate"]

    assert 0.0 <= win_rate <= 1.0
    assert 0.0 <= loss_rate <= 1.0
    assert 0.0 <= draw_rate <= 1.0

    if counts.total == 0:
        assert win_rate == 0.0
        assert loss_rate == 0.0
        assert draw_rate == 0.0
    else:
        assert math.isclose(win_rate + loss_rate + draw_rate, 1.0, rel_tol=1e-6)


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    outcomes=st.lists(st.sampled_from(list(Outcome)), max_size=20)
)
def test_emit_gate_metrics_prefixes_names(outcomes: list[Outcome]) -> None:
    """Metric emission prefixes names and forwards values."""
    counts = accumulate_outcomes(outcomes)
    emitted: list[tuple[str, float]] = []

    def _emit(name: str, value: float) -> None:
        emitted.append((name, value))

    emit_gate_metrics(counts, _emit, prefix="self_play.gate")
    assert emitted
    assert all(name.startswith("self_play.gate.") for name, _ in emitted)
