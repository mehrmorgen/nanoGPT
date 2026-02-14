from __future__ import annotations

import math

import pytest
from hypothesis import assume, given, settings, strategies as st

from ml_playground.framework.configuration.models import PoolSizePolicy
from ml_playground.framework.self_play.pool_size import derive_pool_size


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    target=st.integers(min_value=0, max_value=100_000),
    avg_positions=st.integers(min_value=1, max_value=1_000),
)
def test_pool_size_derived_for_unit_oversample(target: int, avg_positions: int) -> None:
    """Unit oversample derives the expected pool size."""
    expected = 0 if target == 0 else int(math.ceil(target / avg_positions))
    assert derive_pool_size(target, avg_positions, oversample_factor=1.0) == expected


@settings(max_examples=40, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    target_a=st.integers(min_value=0, max_value=50_000),
    target_b=st.integers(min_value=0, max_value=50_000),
    avg_positions=st.integers(min_value=1, max_value=1_000),
    oversample=st.floats(min_value=0.1, max_value=5.0, allow_nan=False),
)
def test_pool_size_monotonic_in_target(
    target_a: int, target_b: int, avg_positions: int, oversample: float
) -> None:
    """Larger targets do not reduce pool sizes."""
    assume(target_a <= target_b)
    size_a = derive_pool_size(target_a, avg_positions, oversample_factor=oversample)
    size_b = derive_pool_size(target_b, avg_positions, oversample_factor=oversample)
    assert size_b >= size_a


@settings(max_examples=40, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    target=st.integers(min_value=0, max_value=50_000),
    avg_positions=st.integers(min_value=1, max_value=1_000),
    oversample=st.floats(min_value=0.1, max_value=5.0, allow_nan=False),
)
def test_pool_size_policy_matches_helper(
    target: int, avg_positions: int, oversample: float
) -> None:
    """PoolSizePolicy uses the same derivation as the helper."""
    policy = PoolSizePolicy(
        target_labeled_positions=target,
        avg_positions_per_game=avg_positions,
        oversample_factor=oversample,
    )
    assert policy.pool_size == derive_pool_size(
        target, avg_positions, oversample_factor=oversample
    )


def test_pool_size_rejects_invalid_inputs() -> None:
    """Invalid pool size inputs raise errors."""
    with pytest.raises(ValueError):
        derive_pool_size(-1, 10)
    with pytest.raises(ValueError):
        derive_pool_size(10, 0)
    with pytest.raises(ValueError):
        derive_pool_size(10, 10, oversample_factor=0)
