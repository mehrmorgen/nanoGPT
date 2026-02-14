from __future__ import annotations

import csv
from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import given, settings, strategies as st

from ml_playground.framework.data_pipeline.transforms.ingestion import stream_csv_column
from ml_playground.framework.core.error_handling import DataError
import pytest


@settings(max_examples=15, deadline=100, derandomize=True)
@given(  # type: ignore[reportAny]
    rows=st.lists(st.text(min_size=0, max_size=10), min_size=1, max_size=10)
)
def test_stream_csv_column_when_rows_then_yields_values(rows: list[str]) -> None:
    """Stream csv column when rows then yields values."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["text"])
            writer.writeheader()
            for row in rows:
                writer.writerow({"text": row})

        streamed = list(stream_csv_column(path))
        assert streamed == rows


def test_stream_csv_column_when_missing_column_then_raises() -> None:
    """Stream csv column when missing column then raises."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["other"])
            writer.writeheader()
            writer.writerow({"other": "value"})
        with pytest.raises(DataError):
            list(stream_csv_column(path))
