from __future__ import annotations

import pytest

from ml_playground.data_pipeline.transforms.depth_pools import (
    normalize_blend_weights,
    allocate_blend_counts,
    blend_pools,
)


def test_normalize_blend_weights_raises_on_empty() -> None:
    with pytest.raises(ValueError, match="weights must not be empty"):
        normalize_blend_weights({})


def test_normalize_blend_weights_raises_on_negative_weights() -> None:
    with pytest.raises(ValueError, match="weights must be non-negative"):
        normalize_blend_weights({1: -0.5, 2: 1.5})


def test_normalize_blend_weights_raises_on_non_positive_total() -> None:
    with pytest.raises(ValueError, match="weights must sum to a positive value"):
        normalize_blend_weights({1: 0.0, 2: 0.0})


def test_allocate_blend_counts_raises_on_mismatched_depths() -> None:
    pools = {1: [1, 2], 2: [3]}
    weights = {1: 0.5, 3: 0.5}
    with pytest.raises(ValueError, match="weights must cover the same depths"):
        allocate_blend_counts(pools, weights)


def test_allocate_blend_counts_raises_on_invalid_target_size() -> None:
    pools = {1: [1, 2]}
    weights = {1: 1.0}
    with pytest.raises(
        ValueError, match="target_size must be within available pool size"
    ):
        allocate_blend_counts(pools, weights, target_size=5)


def test_allocate_blend_counts_returns_zero_when_total_available_is_zero() -> None:
    pools = {1: [], 2: []}
    weights = {1: 0.5, 2: 0.5}
    result = allocate_blend_counts(pools, weights)
    assert result == {1: 0, 2: 0}


def test_blend_pools_returns_empty_when_pools_empty() -> None:
    assert blend_pools({}) == []


def test_blend_pools_defaults_weights_to_one() -> None:
    pools = {1: [1, 2], 2: [3, 4]}
    result = blend_pools(pools)
    assert len(result) == 4
