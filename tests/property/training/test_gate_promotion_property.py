from __future__ import annotations

import pytest
from hypothesis import assume, given, settings, strategies as st

from ml_playground.training.gating import (
    GateCriteria,
    evaluate_gate,
    should_promote,
    should_stop,
)


@settings(max_examples=40, deadline=50, derandomize=True)
@given(
    trials=st.integers(min_value=1, max_value=1_000),
    successes=st.integers(min_value=0, max_value=1_000),
    min_games=st.integers(min_value=1, max_value=1_000),
    promote_threshold=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    stop_threshold=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
def test_gate_decision_matches_helpers(
    trials: int,
    successes: int,
    min_games: int,
    promote_threshold: float,
    stop_threshold: float,
) -> None:
    """Gate decision aligns with helper predicates."""
    assume(successes <= trials)
    criteria = GateCriteria(
        min_games=min_games,
        promote_threshold=promote_threshold,
        stop_threshold=stop_threshold,
    )
    decision = evaluate_gate(successes, trials, criteria)
    assert decision.promote == should_promote(
        successes, trials, min_games, promote_threshold
    )
    assert decision.stop == should_stop(successes, trials, min_games, stop_threshold)


@settings(max_examples=30, deadline=50, derandomize=True)
@given(
    trials=st.integers(min_value=0, max_value=50),
    successes=st.integers(min_value=0, max_value=50),
    threshold=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
def test_gate_helpers_ignore_before_min_games(
    trials: int, successes: int, threshold: float
) -> None:
    """Promotion and stop are false until min_games are met."""
    assume(successes <= trials)
    min_games = trials + 1
    assert not should_promote(successes, trials, min_games, threshold)
    assert not should_stop(successes, trials, min_games, threshold)


def test_gate_criteria_rejects_invalid_values() -> None:
    """Gate criteria validation rejects invalid bounds."""
    with pytest.raises(ValueError):
        GateCriteria(min_games=0)
    with pytest.raises(ValueError):
        GateCriteria(promote_threshold=-0.1)
    with pytest.raises(ValueError):
        GateCriteria(stop_threshold=1.1)
