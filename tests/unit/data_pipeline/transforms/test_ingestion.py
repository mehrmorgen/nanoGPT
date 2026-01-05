from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.ingestion import (
    stream_text_lines,
    stream_csv_column,
    validate_jsonl_record,
)


def test_stream_text_lines_raises_on_os_error(tmp_path: Path) -> None:
    unreadable = tmp_path / "unreadable.txt"
    unreadable.write_text("test")
    unreadable.chmod(0o000)
    with pytest.raises(DataError, match="Failed to stream lines"):
        list(stream_text_lines(unreadable))


def test_stream_csv_column_raises_on_missing_column(tmp_path: Path) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,alice")
    with pytest.raises(DataError, match="CSV missing required column"):
        list(stream_csv_column(csv_file, column="text"))


def test_stream_csv_column_raises_on_row_missing_column(tmp_path: Path) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("text\n")
    with pytest.raises(DataError, match="CSV row missing column"):
        list(stream_csv_column(csv_file, column="text"))


def test_stream_csv_column_raises_on_os_error(tmp_path: Path) -> None:
    unreadable = tmp_path / "data.csv"
    unreadable.write_text("text\n")
    unreadable.chmod(0o000)
    with pytest.raises(DataError, match="Failed to stream CSV"):
        list(stream_csv_column(unreadable, column="text"))


def test_validate_jsonl_record_raises_on_non_mapping() -> None:
    with pytest.raises(DataError, match="must be a mapping"):
        validate_jsonl_record(["not", "a", "mapping"])


def test_validate_jsonl_record_raises_on_missing_text_field() -> None:
    with pytest.raises(DataError, match="missing 'text' field"):
        validate_jsonl_record({"meta": "data"})


def test_validate_jsonl_record_raises_on_non_string_text_field() -> None:
    with pytest.raises(DataError, match="missing 'text' field"):
        validate_jsonl_record({"text": 123})
