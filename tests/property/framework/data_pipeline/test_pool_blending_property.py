from __future__ import annotations

from collections import Counter

from hypothesis import given, settings, strategies as st

from ml_playground.framework.data_pipeline.transforms.depth_pools import (
    allocate_blend_counts,
    blend_pools,
)


@settings(max_examples=30, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    data=st.data()
)
def test_blend_pools_respects_allocated_counts(data: st.DataObject) -> None:
    """Blended pools respect target sizes and allocated counts."""
    depths = data.draw(
        st.lists(
            st.integers(min_value=1, max_value=5),
            min_size=1,
            max_size=4,
            unique=True,
        )
    )
    sizes = data.draw(
        st.lists(
            st.integers(min_value=1, max_value=6),
            min_size=len(depths),
            max_size=len(depths),
        )
    )
    pools = {
        depth: tuple((depth, idx) for idx in range(size))
        for depth, size in zip(depths, sizes, strict=True)
    }
    weights = {
        depth: data.draw(
            st.floats(
                min_value=0.1, max_value=5.0, allow_nan=False, allow_infinity=False
            )
        )
        for depth in depths
    }
    total_available = sum(sizes)
    target_size = data.draw(st.integers(min_value=1, max_value=total_available))

    counts = allocate_blend_counts(pools, weights, target_size=target_size)
    blended = blend_pools(pools, weights, target_size=target_size)
    blended_counts = Counter(record[0] for record in blended)

    assert len(blended) == target_size
    assert blended_counts == Counter(counts)


def test_blend_pools_when_empty_then_returns_empty() -> None:
    """Blending empty pools yields no records."""
    assert blend_pools({}) == []
