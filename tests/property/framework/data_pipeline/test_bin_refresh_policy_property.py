from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.framework.configuration.models import BinRefreshPolicy
from ml_playground.framework.data_pipeline.transforms.refresh_policy import (
    should_refresh_bins,
)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    previous_tokens=st.integers(min_value=0, max_value=1_000_000),
    new_tokens=st.integers(min_value=0, max_value=1_000_000),
    min_new_tokens=st.integers(min_value=0, max_value=1_000_000),
    min_new_ratio=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
def test_bin_refresh_policy_thresholds(
    previous_tokens: int,
    new_tokens: int,
    min_new_tokens: int,
    min_new_ratio: float,
) -> None:
    """Refresh decisions honor token and ratio thresholds."""
    policy = BinRefreshPolicy(
        min_new_tokens=min_new_tokens,
        min_new_ratio=min_new_ratio,
    )
    expected = new_tokens >= min_new_tokens and (
        previous_tokens == 0 or (new_tokens / previous_tokens) >= min_new_ratio
    )
    assert (
        should_refresh_bins(
            previous_tokens=previous_tokens,
            new_tokens=new_tokens,
            policy=policy,
        )
        is expected
    )


def test_bin_refresh_policy_rejects_negative_counts() -> None:
    """Negative token counts are rejected."""
    policy = BinRefreshPolicy()
    with pytest.raises(ValueError):
        should_refresh_bins(previous_tokens=-1, new_tokens=0, policy=policy)
    with pytest.raises(ValueError):
        should_refresh_bins(previous_tokens=0, new_tokens=-1, policy=policy)
