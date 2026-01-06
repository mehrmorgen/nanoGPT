from __future__ import annotations

from typing import Any

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.streaming import validate_streaming_records


def _record(**overrides: Any) -> dict[str, Any]:
    base = {
        "start": "",
        "winner": 1,
        "moves": [1, 2, 3],
        "policy_targets": [0.1, 0.2, 0.3],
    }
    base.update(overrides)
    return base


def test_validate_streaming_records_yields_records() -> None:
    record = _record()
    validated = list(validate_streaming_records([record]))
    assert validated == [record]


def test_validate_streaming_records_rejects_non_mapping() -> None:
    with pytest.raises(DataError):
        list(validate_streaming_records([object()]))


def test_validate_streaming_records_rejects_missing_fields() -> None:
    record = _record()
    record.pop("moves")
    with pytest.raises(DataError):
        list(validate_streaming_records([record]))
