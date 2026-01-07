from __future__ import annotations

from hypothesis import given, settings, strategies as st
import pytest

from ml_playground.data_pipeline.transforms.depth_pools import (
    allocate_blend_counts,
    blend_pools,
    normalize_blend_weights,
)


def test_normalize_blend_weights_rejects_empty() -> None:
    """Normalize weights rejects empty mappings."""
    with pytest.raises(ValueError):
        normalize_blend_weights({})


@settings(max_examples=15, deadline=50, derandomize=True)
@given(weight=st.floats(max_value=-0.1, allow_nan=False, allow_infinity=False))
def test_normalize_blend_weights_rejects_negative(weight: float) -> None:
    """Normalize weights rejects negative values."""
    with pytest.raises(ValueError):
        normalize_blend_weights({1: weight})


def test_normalize_blend_weights_rejects_non_positive_sum() -> None:
    """Normalize weights rejects totals that sum to zero."""
    with pytest.raises(ValueError):
        normalize_blend_weights({1: 0.0, 2: 0.0})


def test_allocate_blend_counts_rejects_weight_key_mismatch() -> None:
    """Blend allocation rejects mismatched pool and weight keys."""
    pools = {1: (1,), 2: (2,)}
    weights = {1: 1.0}
    with pytest.raises(ValueError):
        allocate_blend_counts(pools, weights)


def test_allocate_blend_counts_handles_zero_available() -> None:
    """Blend allocation returns zeros when pools are empty."""
    pools = {1: (), 2: ()}
    weights = {1: 1.0, 2: 1.0}
    counts = allocate_blend_counts(pools, weights)
    assert counts == {1: 0, 2: 0}


@settings(max_examples=20, deadline=50, derandomize=True)
@given(
    sizes=st.lists(st.integers(min_value=1, max_value=4), min_size=1, max_size=3),
    target_extra=st.integers(min_value=1, max_value=3),
)
def test_allocate_blend_counts_rejects_out_of_range_target(
    sizes: list[int], target_extra: int
) -> None:
    """Blend allocation rejects targets outside available size."""
    pools = {idx: tuple(range(size)) for idx, size in enumerate(sizes, start=1)}
    weights = {idx: 1.0 for idx in pools}
    total_available = sum(sizes)
    with pytest.raises(ValueError):
        allocate_blend_counts(
            pools, weights, target_size=total_available + target_extra
        )


def test_allocate_blend_counts_rejects_negative_target() -> None:
    """Blend allocation rejects negative target sizes."""
    pools = {1: (1, 2)}
    weights = {1: 1.0}
    with pytest.raises(ValueError):
        allocate_blend_counts(pools, weights, target_size=-1)


@settings(max_examples=20, deadline=50, derandomize=True)
@given(
    sizes=st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=4),
)
def test_allocate_blend_counts_defaults_to_total_available(sizes: list[int]) -> None:
    """Blend allocation defaults target size to total available tokens."""
    pools = {idx: tuple(range(size)) for idx, size in enumerate(sizes, start=1)}
    weights = {idx: 1.0 for idx in pools}
    counts = allocate_blend_counts(pools, weights, target_size=None)
    assert sum(counts.values()) == sum(sizes)


@settings(max_examples=20, deadline=50, derandomize=True)
@given(sizes=st.lists(st.integers(min_value=1, max_value=4), min_size=1, max_size=3))
def test_blend_pools_defaults_weights_when_missing(sizes: list[int]) -> None:
    """Blend pools uses equal weights when weights are omitted."""
    pools = {idx: tuple(range(size)) for idx, size in enumerate(sizes, start=1)}
    blended = blend_pools(pools, weights=None)
    assert len(blended) == sum(sizes)
