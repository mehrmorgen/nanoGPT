from __future__ import annotations

import pytest

from ml_playground.framework.configuration.models import BinRefreshPolicy
from ml_playground.framework.data_pipeline.transforms.refresh_policy import (
    should_refresh_bins,
)


def test_should_refresh_bins_negative_tokens_raises() -> None:
    """should_refresh_bins should raise for negative token counts."""
    policy = BinRefreshPolicy(min_new_tokens=100, min_new_ratio=0.1)

    with pytest.raises(ValueError, match="token counts must be non-negative"):
        should_refresh_bins(previous_tokens=-1, new_tokens=100, policy=policy)

    with pytest.raises(ValueError, match="token counts must be non-negative"):
        should_refresh_bins(previous_tokens=100, new_tokens=-1, policy=policy)


def test_should_refresh_bins_below_min_new_tokens_returns_false() -> None:
    """should_refresh_bins should return False when new_tokens < min_new_tokens."""
    policy = BinRefreshPolicy(min_new_tokens=100, min_new_ratio=0.1)

    result = should_refresh_bins(previous_tokens=1000, new_tokens=50, policy=policy)
    assert result is False


def test_should_refresh_bins_first_run_returns_true() -> None:
    """should_refresh_bins should return True on first run (previous_tokens=0)."""
    policy = BinRefreshPolicy(min_new_tokens=100, min_new_ratio=0.1)

    result = should_refresh_bins(previous_tokens=0, new_tokens=100, policy=policy)
    assert result is True


def test_should_refresh_bins_ratio_below_threshold_returns_false() -> None:
    """should_refresh_bins should return False when ratio < min_new_ratio."""
    policy = BinRefreshPolicy(min_new_tokens=10, min_new_ratio=0.5)

    # 100 new tokens vs 1000 previous = 0.1 ratio < 0.5 threshold
    result = should_refresh_bins(previous_tokens=1000, new_tokens=100, policy=policy)
    assert result is False


def test_should_refresh_bins_ratio_at_threshold_returns_true() -> None:
    """should_refresh_bins should return True when ratio >= min_new_ratio."""
    policy = BinRefreshPolicy(min_new_tokens=10, min_new_ratio=0.5)

    # 500 new tokens vs 1000 previous = 0.5 ratio = threshold
    result = should_refresh_bins(previous_tokens=1000, new_tokens=500, policy=policy)
    assert result is True


def test_should_refresh_bins_ratio_above_threshold_returns_true() -> None:
    """should_refresh_bins should return True when ratio > min_new_ratio."""
    policy = BinRefreshPolicy(min_new_tokens=10, min_new_ratio=0.5)

    # 600 new tokens vs 1000 previous = 0.6 ratio > threshold
    result = should_refresh_bins(previous_tokens=1000, new_tokens=600, policy=policy)
    assert result is True


def test_should_refresh_bins_both_conditions_met_returns_true() -> None:
    """should_refresh_bins should return True when both conditions are met."""
    policy = BinRefreshPolicy(min_new_tokens=100, min_new_ratio=0.1)

    result = should_refresh_bins(previous_tokens=1000, new_tokens=200, policy=policy)
    assert result is True
