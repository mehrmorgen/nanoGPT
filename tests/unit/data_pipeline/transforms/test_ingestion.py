from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.ingestion import (
    stream_csv_column,
    stream_jsonl,
    stream_text_lines,
    validate_jsonl_record,
)


def test_stream_text_lines_raises_on_directory(tmp_path: Path) -> None:
    """Streaming text lines raises DataError on unreadable paths."""
    path = tmp_path / "dir"
    path.mkdir()

    with pytest.raises(DataError):
        list(stream_text_lines(path))


def test_stream_csv_column_missing_row_value_raises(tmp_path: Path) -> None:
    """Streaming CSV raises when a row omits the requested column."""
    path = tmp_path / "rows.csv"
    path.write_text("text,other\nonlytext\n", encoding="utf-8")

    with pytest.raises(DataError):
        list(stream_csv_column(path, column="other"))


def test_stream_csv_column_raises_on_directory(tmp_path: Path) -> None:
    """Streaming CSV raises DataError on unreadable paths."""
    path = tmp_path / "dir"
    path.mkdir()

    with pytest.raises(DataError):
        list(stream_csv_column(path, column="text"))


def test_stream_jsonl_skips_blank_lines(tmp_path: Path) -> None:
    """Streaming JSONL skips blank lines and returns objects."""
    path = tmp_path / "records.jsonl"
    path.write_text('\n{"text": "hello"}\n', encoding="utf-8")

    assert list(stream_jsonl(path)) == [{"text": "hello"}]


def test_stream_jsonl_rejects_invalid_json(tmp_path: Path) -> None:
    """Streaming JSONL raises DataError on malformed JSON."""
    path = tmp_path / "bad.jsonl"
    path.write_text("{bad json}\n", encoding="utf-8")

    with pytest.raises(DataError):
        list(stream_jsonl(path))


def test_stream_jsonl_rejects_non_object_records(tmp_path: Path) -> None:
    """Streaming JSONL raises DataError when a line decodes to non-object."""
    path = tmp_path / "list.jsonl"
    path.write_text("[1, 2]\n", encoding="utf-8")

    with pytest.raises(DataError):
        list(stream_jsonl(path))


def test_stream_jsonl_raises_on_directory(tmp_path: Path) -> None:
    """Streaming JSONL raises DataError on unreadable paths."""
    path = tmp_path / "dir"
    path.mkdir()

    with pytest.raises(DataError):
        list(stream_jsonl(path))


def test_validate_jsonl_record_rejects_invalid_meta_tokens() -> None:
    """JSONL records reject non-string meta tokens."""
    with pytest.raises(DataError):
        validate_jsonl_record({"text": "hi", "meta_tokens": ["ok", 1]})


def test_validate_jsonl_record_rejects_non_mapping() -> None:
    """JSONL records reject non-mapping payloads."""
    with pytest.raises(DataError):
        validate_jsonl_record("nope")  # type: ignore[arg-type]


def test_validate_jsonl_record_rejects_meta_not_mapping() -> None:
    """JSONL records reject meta fields that are not mappings."""
    with pytest.raises(DataError):
        validate_jsonl_record({"text": "hi", "meta": ["nope"]})
