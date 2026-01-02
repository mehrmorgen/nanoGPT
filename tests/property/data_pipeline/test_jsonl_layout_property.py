from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.ingestion import (
    stream_jsonl,
    validate_jsonl_record,
)


@settings(max_examples=15, deadline=100, derandomize=True)
@given(
    text=st.text(min_size=1, max_size=20),
    meta=st.dictionaries(
        keys=st.text(min_size=1, max_size=5),
        values=st.text(min_size=0, max_size=5),
        max_size=3,
    ),
    meta_tokens=st.lists(st.text(min_size=1, max_size=5), max_size=3),
)
def test_validate_jsonl_record_when_valid_then_accepts(
    text: str,
    meta: dict[str, str],
    meta_tokens: list[str],
) -> None:
    """Validate jsonl record when valid then accepts."""
    record: dict[str, object] = {"text": text}
    if meta:
        record["meta"] = meta
    if meta_tokens:
        record["meta_tokens"] = meta_tokens
    validate_jsonl_record(record)


@settings(max_examples=15, deadline=100, derandomize=True)
@given(records=st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=5))
def test_stream_jsonl_when_valid_then_yields(records: list[str]) -> None:
    """Stream jsonl when valid then yields."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for text in records:
                handle.write(json.dumps({"text": text}) + "\n")

        streamed = [record["text"] for record in stream_jsonl(path)]
        assert streamed == records


def test_stream_jsonl_when_missing_text_then_raises() -> None:
    """Stream jsonl when missing text then raises."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps({"meta": {"k": "v"}}) + "\n")
        with pytest.raises(DataError):
            list(stream_jsonl(path))


def test_stream_jsonl_when_invalid_json_then_raises() -> None:
    """Stream jsonl when invalid json then raises."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.jsonl"
        path.write_text("{not-json}\\n", encoding="utf-8")
        with pytest.raises(DataError):
            list(stream_jsonl(path))


def test_stream_jsonl_when_non_object_then_raises() -> None:
    """Stream jsonl when non object then raises."""
    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "data.jsonl"
        path.write_text(json.dumps(["text"]) + "\n", encoding="utf-8")
        with pytest.raises(DataError):
            list(stream_jsonl(path))
