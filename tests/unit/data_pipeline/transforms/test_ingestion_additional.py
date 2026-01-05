from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.ingestion import (
    stream_csv_column,
    stream_jsonl,
)


def test_stream_csv_column_raises_when_no_rows(tmp_path: Path) -> None:
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("text\n", encoding="utf-8")
    with pytest.raises(DataError, match="CSV row missing column 'text'"):
        list(stream_csv_column(csv_file, column="text"))


def test_stream_jsonl_raises_on_decode_error(tmp_path: Path) -> None:
    path = tmp_path / "data.jsonl"
    path.write_text("{bad json}\n", encoding="utf-8")
    with pytest.raises(DataError, match="Invalid JSONL at line 1"):
        list(stream_jsonl(path))


def test_stream_jsonl_raises_on_non_object(tmp_path: Path) -> None:
    path = tmp_path / "data.jsonl"
    path.write_text('["not-object"]\n', encoding="utf-8")
    with pytest.raises(DataError, match="JSONL record must be an object"):
        list(stream_jsonl(path))
