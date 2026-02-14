from __future__ import annotations

from collections import Counter

from hypothesis import given, settings, strategies as st

from ml_playground.framework.data_pipeline.transforms.depth_pools import (
    partition_by_depth,
)


@settings(max_examples=50, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    records=st.lists(st.tuples(st.integers(min_value=0, max_value=5), st.integers()))
)
def test_partition_by_depth_groups_records(records: list[tuple[int, int]]) -> None:
    """Partitioning preserves all records and depth labels."""
    pools = partition_by_depth(records, depth_fn=lambda record: record[0])
    flattened = [record for items in pools.values() for record in items]
    assert Counter(flattened) == Counter(records)
    for depth, items in pools.items():
        assert all(record[0] == depth for record in items)
