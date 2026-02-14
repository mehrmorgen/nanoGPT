from __future__ import annotations

from hypothesis import assume, given, settings, strategies as st
import pytest

from ml_playground.framework.training.gating import wilson_interval


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    trials=st.integers(min_value=1, max_value=1_000),
    successes=st.integers(min_value=0, max_value=1_000),
    z=st.floats(min_value=0.1, max_value=3.0, allow_nan=False, allow_infinity=False),
)
def test_wilson_interval_bounds(trials: int, successes: int, z: float) -> None:
    """Wilson interval stays within [0, 1]."""
    assume(successes <= trials)
    lower, upper = wilson_interval(successes, trials, z=z)
    assert 0.0 <= lower <= upper <= 1.0


@settings(max_examples=40, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
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


@settings(max_examples=15, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    trials=st.integers(max_value=0)
)
def test_wilson_interval_rejects_non_positive_trials(trials: int) -> None:
    """Wilson interval rejects non-positive trial counts."""
    with pytest.raises(ValueError):
        wilson_interval(0, trials)


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    trials=st.integers(min_value=1, max_value=500), successes=st.integers()
)
def test_wilson_interval_rejects_out_of_range_successes(
    trials: int, successes: int
) -> None:
    """Wilson interval rejects successes outside [0, trials]."""
    assume(successes < 0 or successes > trials)
    with pytest.raises(ValueError):
        wilson_interval(successes, trials)


@settings(max_examples=15, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    z=st.floats(max_value=0.0, allow_nan=False, allow_infinity=False)
)
def test_wilson_interval_rejects_non_positive_z(z: float) -> None:
    """Wilson interval rejects non-positive z values."""
    with pytest.raises(ValueError):
        wilson_interval(0, 1, z=z)
