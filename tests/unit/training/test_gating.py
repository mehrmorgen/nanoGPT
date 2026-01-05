from __future__ import annotations

import pytest

from ml_playground.training.gating import (
    GateCriteria,
    evaluate_gate,
    should_promote,
    should_stop,
    wilson_interval,
)


def test_wilson_interval_raises_on_non_positive_trials() -> None:
    with pytest.raises(ValueError, match="trials must be positive"):
        wilson_interval(successes=0, trials=0)


def test_wilson_interval_raises_on_successes_out_of_range() -> None:
    with pytest.raises(ValueError, match="successes must be within"):
        wilson_interval(successes=-1, trials=10)

    with pytest.raises(ValueError, match="successes must be within"):
        wilson_interval(successes=11, trials=10)


def test_wilson_interval_raises_on_non_positive_z() -> None:
    with pytest.raises(ValueError, match="z must be positive"):
        wilson_interval(successes=5, trials=10, z=0.0)


def test_gate_criteria_validates_min_games() -> None:
    with pytest.raises(ValueError, match="min_games must be >= 1"):
        GateCriteria(min_games=0)


def test_should_promote_returns_false_when_trials_below_min_games() -> None:
    assert not should_promote(successes=5, trials=5, min_games=10, threshold=0.5)


def test_should_stop_returns_false_when_trials_below_min_games() -> None:
    assert not should_stop(successes=5, trials=5, min_games=10, threshold=0.5)


def test_evaluate_gate_returns_decision_with_bounds() -> None:
    criteria = GateCriteria(min_games=1, promote_threshold=0.6, stop_threshold=0.4)
    decision = evaluate_gate(successes=5, trials=10, criteria=criteria)
    assert 0.0 <= decision.lower_bound <= 1.0
    assert 0.0 <= decision.upper_bound <= 1.0
    assert isinstance(decision.promote, bool)
    assert isinstance(decision.stop, bool)
