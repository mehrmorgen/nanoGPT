from __future__ import annotations

from hypothesis import given, settings, strategies as st
import pytest

from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.data_pipeline.transforms.streaming import (
    REQUIRED_STREAM_FIELDS,
    validate_streaming_records,
)


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    start=st.text(max_size=32),
    winner=st.text(max_size=32),
    moves=st.lists(st.integers(min_value=0, max_value=10), max_size=5),
    policy_targets=st.lists(st.floats(min_value=0.0, max_value=1.0), max_size=5),
)
def test_validate_streaming_records_when_complete_then_accepts(
    start: str, winner: str, moves: list[int], policy_targets: list[float]
) -> None:
    """Validate streaming records when complete then accepts."""
    record = {
        "start": start,
        "winner": winner,
        "moves": moves,
        "policy_targets": policy_targets,
    }
    validated = list(validate_streaming_records([record]))
    assert validated == [record]


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    payload=st.dictionaries(
        keys=st.sampled_from(["start", "moves", "policy_targets"]),
        values=st.integers(min_value=0, max_value=10),
        min_size=1,
        max_size=3,
    )
)
def test_validate_streaming_records_when_missing_field_then_raises(
    payload: dict[str, int],
) -> None:
    """Validate streaming records when missing field then raises."""
    missing = [field for field in REQUIRED_STREAM_FIELDS if field not in payload]
    assert missing
    with pytest.raises(DataError):
        list(validate_streaming_records([payload]))
