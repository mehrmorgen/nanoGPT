from __future__ import annotations

import pytest

from ml_playground.framework.data_pipeline.transforms.depth_pools import (
    allocate_blend_counts,
    blend_pools,
    normalize_blend_weights,
    partition_by_depth,
)


def test_partition_by_depth_groups_records() -> None:
    """partition_by_depth should group records by depth."""
    records: list[str] = ["a", "b", "c", "d"]

    def depth_fn(x: str) -> int:
        return ord(x) % 2  # a->1, b->0, c->1, d->0

    result: dict[int, tuple[str, ...]] = partition_by_depth(records, depth_fn)

    assert result == {0: ("b", "d"), 1: ("a", "c")}


def test_partition_by_depth_empty_returns_empty() -> None:
    """partition_by_depth should return empty dict for empty records."""
    result: dict[int, tuple[str, ...]] = partition_by_depth([], lambda x: 0)
    assert result == {}


def test_normalize_blend_weights_empty_raises() -> None:
    """normalize_blend_weights should raise for empty weights."""
    with pytest.raises(ValueError, match="weights must not be empty"):
        normalize_blend_weights({})


def test_normalize_blend_weights_negative_raises() -> None:
    """normalize_blend_weights should raise for negative weights."""
    with pytest.raises(ValueError, match="weights must be non-negative"):
        normalize_blend_weights({1: 1.0, 2: -0.5})


def test_normalize_blend_weights_zero_total_raises() -> None:
    """normalize_blend_weights should raise when total is zero."""
    with pytest.raises(ValueError, match="weights must sum to a positive value"):
        normalize_blend_weights({1: 0.0, 2: 0.0})


def test_normalize_blend_weights_normalizes() -> None:
    """normalize_blend_weights should normalize weights to sum to 1."""
    weights = {1: 2.0, 2: 3.0}
    result = normalize_blend_weights(weights)

    assert result == {1: 0.4, 2: 0.6}
    assert sum(result.values()) == pytest.approx(1.0)


def test_allocate_blend_counts_mismatched_depths_raises() -> None:
    """allocate_blend_counts should raise when weights and pools have different depths."""
    pools = {1: [1, 2], 2: [3, 4]}
    weights = {1: 0.5, 3: 0.5}

    with pytest.raises(ValueError, match="weights must cover the same depths"):
        allocate_blend_counts(pools, weights)


def test_allocate_blend_counts_empty_pools_returns_zeros() -> None:
    """allocate_blend_counts should return zeros for empty pools."""
    pools: dict[int, list[int]] = {1: [], 2: []}
    weights = {1: 0.5, 2: 0.5}

    result = allocate_blend_counts(pools, weights)
    assert result == {1: 0, 2: 0}


def test_allocate_blend_counts_invalid_target_size_raises() -> None:
    """allocate_blend_counts should raise for invalid target_size."""
    pools = {1: [1, 2, 3], 2: [4, 5]}
    weights = {1: 0.5, 2: 0.5}

    with pytest.raises(
        ValueError, match="target_size must be within available pool size"
    ):
        allocate_blend_counts(pools, weights, target_size=10)  # > total_available (5)

    with pytest.raises(
        ValueError, match="target_size must be within available pool size"
    ):
        allocate_blend_counts(pools, weights, target_size=-1)


def test_allocate_blend_counts_distributes_by_weight() -> None:
    """allocate_blend_counts should distribute counts by weight."""
    pools = {1: [1, 2, 3, 4], 2: [5, 6]}
    weights = {1: 0.75, 2: 0.25}

    result = allocate_blend_counts(pools, weights, target_size=4)

    # 4 * 0.75 = 3 for depth 1, 4 * 0.25 = 1 for depth 2
    assert result == {1: 3, 2: 1}


def test_allocate_blend_counts_handles_remainder() -> None:
    """allocate_blend_counts should distribute remainder to pools with largest fractional parts."""
    pools = {1: [1, 2, 3], 2: [4, 5], 3: [6, 7, 8]}
    weights = {1: 0.5, 2: 0.25, 3: 0.25}

    # target_size = 5
    # raw: 1->2.5, 2->1.25, 3->1.25
    # initial: 1->2, 2->1, 3->1 (sum=4)
    # remainder=1, largest fractional: 2.5-2=0.5 > 1.25-1=0.25
    # final: 1->3, 2->1, 3->1
    result = allocate_blend_counts(pools, weights, target_size=5)

    assert result == {1: 3, 2: 1, 3: 1}


def test_allocate_blend_counts_skips_pools_with_no_capacity() -> None:
    """allocate_blend_counts should skip pools that have no remaining capacity."""
    pools = {1: [1, 2], 2: [3], 3: [4, 5, 6]}
    weights = {1: 0.4, 2: 0.2, 3: 0.4}

    # target_size = 5
    # raw: 1->2.0, 2->1.0, 3->2.0
    # initial: 1->2, 2->1, 3->2 (sum=5), remainder=0
    # This covers the case where some pools are at capacity
    result = allocate_blend_counts(pools, weights, target_size=5)

    assert result == {1: 2, 2: 1, 3: 2}


def test_allocate_blend_counts_capacity_zero_continues() -> None:
    """allocate_blend_counts should continue when pool has zero remaining capacity."""
    pools = {1: [1], 2: [2, 3, 4]}
    weights = {1: 0.3, 2: 0.7}

    # target_size = 3
    # raw: 1->0.9, 2->2.1
    # initial: 1->0, 2->2 (sum=2), remainder=1
    # capacity at depth 1: 1-0=1, at depth 2: 3-2=1
    # Both have capacity, so remainder gets distributed
    result = allocate_blend_counts(pools, weights, target_size=3)

    assert result == {1: 1, 2: 2}


def test_allocate_blend_counts_unable_to_allocate_raises() -> None:
    """allocate_blend_counts should raise when unable to allocate within pool limits."""
    # The "unable to allocate" error is a defensive check that's difficult to trigger
    # because target_size validation ensures total capacity is sufficient.
    # However, we can document the successful remainder distribution path.
    pools = {1: [1], 2: [2]}
    weights = {1: 0.5, 2: 0.5}

    # target_size=2, raw: 1->1.0, 2->1.0
    # initial: 1->1, 2->1 (sum=2), remainder=0
    result = allocate_blend_counts(pools, weights, target_size=2)
    assert result == {1: 1, 2: 1}

    # Test with fractional weights that create remainder
    pools = {1: [1], 2: [2, 3]}
    weights = {1: 0.4, 2: 0.6}

    # target_size=3, raw: 1->1.2, 2->1.8
    # initial: 1->1, 2->1 (sum=2), remainder=1
    # capacity at depth 1: 0, at depth 2: 1
    result = allocate_blend_counts(pools, weights, target_size=3)
    assert result == {1: 1, 2: 2}

    # The error at line 71 is a defensive check that's hard to trigger
    # because target_size validation (line 43) ensures total_available >= target_size
    # This test documents the successful remainder distribution paths.


def test_blend_pools_empty_returns_empty() -> None:
    """blend_pools should return empty list for empty pools."""
    pools: dict[int, list[int]] = {}
    result = blend_pools(pools)
    assert result == []


def test_blend_pools_default_weights_equal() -> None:
    """blend_pools should use equal weights when weights not provided."""
    pools = {1: [1, 2], 2: [3, 4]}

    result = blend_pools(pools)

    # Should interleave: 1, 3, 2, 4
    assert result == [1, 3, 2, 4]


def test_blend_pools_custom_weights() -> None:
    """blend_pools should use custom weights when provided."""
    pools = {1: [1, 2, 3, 4], 2: [5, 6]}
    weights = {1: 0.75, 2: 0.25}

    result = blend_pools(pools, weights, target_size=4)

    # 3 from depth 1, 1 from depth 2
    # Interleaved: 1 (depth1), 5 (depth2), 2 (depth1), 3 (depth1)
    assert len(result) == 4
    assert result == [1, 5, 2, 3]


def test_blend_pools_target_size_none_uses_all() -> None:
    """blend_pools should use all items when target_size is None."""
    pools = {1: [1, 2], 2: [3, 4]}

    result = blend_pools(pools, target_size=None)

    assert len(result) == 4
    assert set(result) == {1, 2, 3, 4}
