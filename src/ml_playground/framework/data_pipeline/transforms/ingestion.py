"""Streaming ingestion helpers for large text/CSV/JSONL sources."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Mapping, cast

from ml_playground.framework.core.di_implementations import DefaultJsonParser
from ml_playground.framework.core.error_handling import DataError

__all__ = [
    "JSONL_OPTIONAL_FIELDS",
    "JSONL_REQUIRED_FIELDS",
    "stream_csv_column",
    "stream_jsonl",
    "stream_text_lines",
    "validate_jsonl_record",
]

JSONL_REQUIRED_FIELDS = ("text",)
JSONL_OPTIONAL_FIELDS = ("meta", "meta_tokens")


def stream_text_lines(path: Path) -> Iterable[str]:
    """Yield lines from a text file without loading the entire file."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                yield line.rstrip("\n")
    except OSError as exc:
        raise DataError(
            f"Failed to stream lines from {path}: {exc}",
            reason="Unable to read text source",
            rationale="Streaming ingestion requires readable text inputs",
        ) from exc


def stream_csv_column(path: Path, *, column: str = "text") -> Iterable[str]:
    """Yield values from a CSV column without loading the full file."""
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None or column not in reader.fieldnames:
                raise DataError(
                    f"CSV missing required column '{column}'",
                    reason="CSV schema missing expected column",
                    rationale="Streaming CSV ingestion needs a text column",
                )
            for row in reader:
                value = row.get(column)
                if value is None:
                    raise DataError(
                        f"CSV row missing column '{column}'",
                        reason="CSV row missing expected column",
                        rationale="Streaming CSV ingestion requires per-row text",
                    )
                yield value
    except OSError as exc:
        raise DataError(
            f"Failed to stream CSV from {path}: {exc}",
            reason="Unable to read CSV source",
            rationale="Streaming CSV ingestion requires readable inputs",
        ) from exc


JsonRecord = dict[str, object]


def _is_json_record(value: object) -> bool:
    return isinstance(value, Mapping)


def validate_jsonl_record(record: JsonRecord) -> None:
    """Validate a single JSONL record against the layout contract."""
    if "text" not in record or not isinstance(record["text"], str):
        raise DataError(
            "JSONL record missing 'text' field",
            reason="Required text field missing or invalid",
            rationale="JSONL layout requires 'text' for each record",
        )
    if "meta" in record and not isinstance(record["meta"], Mapping):
        raise DataError(
            "JSONL record meta must be a mapping",
            reason="meta field present but not a mapping",
            rationale="JSONL meta must be a key/value mapping",
        )
    meta_tokens_obj: object | None = record.get("meta_tokens")
    if meta_tokens_obj is not None:
        if not isinstance(meta_tokens_obj, list):
            raise DataError(
                "JSONL record meta_tokens must be list[str]",
                reason="meta_tokens present but invalid",
                rationale="JSONL meta tokens must be a list of strings",
            )
        tokens_list: list[str] = []
        for item_obj in cast(list[object], meta_tokens_obj):
            item = str(item_obj)
            if not isinstance(item_obj, str):
                raise DataError(
                    "JSONL record meta_tokens must be list[str]",
                    reason="meta_tokens present but invalid",
                    rationale="JSONL meta tokens must be a list of strings",
                )
            tokens_list.append(item)


def stream_jsonl(path: Path) -> Iterable[dict[str, object]]:
    """Stream JSONL records using DI JsonParser and validate the layout."""
    if not path.exists():
        return

    json_parser = DefaultJsonParser()
    try:
        with path.open("r", encoding="utf-8") as handle:
            for idx, line in enumerate(handle):
                if not line.strip():
                    continue
                try:
                    raw_record = json_parser.parse_json(line)
                except json.JSONDecodeError as exc:
                    raise DataError(
                        f"Invalid JSONL at line {idx + 1}: {exc}",
                        reason="JSON decoding failed",
                        rationale="JSONL requires one JSON object per line",
                    ) from exc
                if not _is_json_record(raw_record):
                    raise DataError(
                        "JSONL record must be an object",
                        reason="Decoded JSON is not an object",
                        rationale="JSONL requires object records",
                    )
                record = cast(JsonRecord, raw_record)
                validate_jsonl_record(record)
                yield record
    except OSError as exc:
        raise DataError(
            f"Failed to stream JSONL from {path}: {exc}",
            reason="Unable to read JSONL source",
            rationale="Streaming JSONL ingestion requires readable inputs",
        ) from exc
