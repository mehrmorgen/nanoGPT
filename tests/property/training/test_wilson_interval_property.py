from __future__ import annotations

from hypothesis import assume, given, settings, strategies as st

from ml_playground.training.gating import wilson_interval


@settings(max_examples=50, deadline=50, derandomize=True)
@given(
    trials=st.integers(min_value=1, max_value=1_000),
    successes=st.integers(min_value=0, max_value=1_000),
    z=st.floats(min_value=0.1, max_value=3.0, allow_nan=False, allow_infinity=False),
)
def test_wilson_interval_bounds(trials: int, successes: int, z: float) -> None:
    """Wilson interval stays within [0, 1]."""
    assume(successes <= trials)
    lower, upper = wilson_interval(successes, trials, z=z)
    assert 0.0 <= lower <= upper <= 1.0


@settings(max_examples=40, deadline=50, derandomize=True)
@given(
    trials=st.integers(min_value=1, max_value=1_000),
    success_a=st.integers(min_value=0, max_value=1_000),
    success_b=st.integers(min_value=0, max_value=1_000),
)
def test_wilson_interval_is_monotonic(
    trials: int, success_a: int, success_b: int
) -> None:
    """Higher success counts do not reduce the lower bound."""
    assume(success_a <= trials and success_b <= trials)
    assume(success_a <= success_b)
    lower_a, _ = wilson_interval(success_a, trials)
    lower_b, _ = wilson_interval(success_b, trials)
    assert lower_b >= lower_a
