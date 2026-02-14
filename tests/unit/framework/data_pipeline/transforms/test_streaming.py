from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import pytest
import numpy as np

from ml_playground.framework.configuration.models import DataConfig
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.data_pipeline.transforms.streaming import (
    append_bin_and_meta,
    validate_streaming_records,
)


def _record(**overrides: Any) -> dict[str, Any]:
    base = {
        "start": "",
        "winner": 1,
        "moves": [1, 2, 3],
        "policy_targets": [0.1, 0.2, 0.3],
    }
    base.update(overrides)
    return base


class _StubLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def debug(self, msg: object, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))


def test_validate_streaming_records_yields_records() -> None:
    """Streaming validation yields records that satisfy the schema."""
    record = _record()
    validated = list(validate_streaming_records([record]))
    assert validated == [record]


def test_validate_streaming_records_rejects_non_mapping() -> None:
    """Streaming validation rejects non-mapping records."""
    with pytest.raises(DataError):
        list(validate_streaming_records([object()]))


def test_validate_streaming_records_rejects_missing_fields() -> None:
    """Streaming validation rejects records missing required fields."""
    record = _record()
    record.pop("moves")
    with pytest.raises(DataError):
        list(validate_streaming_records([record]))


def test_append_bin_and_meta_uses_data_config_paths(tmp_path: Path) -> None:
    """Append logic respects data config path overrides."""
    cfg = DataConfig(
        train_bin="train.custom",
        val_bin="val.custom",
        meta_pkl="meta.custom",
        batch_size=1,
        block_size=1,
        grad_accum_steps=1,
        ngram_size=1,
    )
    train = np.arange(2, dtype=np.uint16)
    val = np.arange(2, dtype=np.uint16)
    meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 0,
        "val_tokens": 0,
        "stoi": {"a": 0},
    }

    append_bin_and_meta(tmp_path, train, val, meta, logger=_StubLogger(), data_cfg=cfg)

    assert (tmp_path / "train.custom").exists()
    assert (tmp_path / "val.custom").exists()
    assert (tmp_path / "meta.custom").exists()


def test_append_bin_and_meta_rejects_unreadable_meta(tmp_path: Path) -> None:
    """Append logic raises when meta.pkl cannot be deserialized."""
    meta_path = tmp_path / "meta.pkl"
    meta_path.write_text("not-a-pickle", encoding="utf-8")
    train = np.arange(1, dtype=np.uint16)
    val = np.arange(1, dtype=np.uint16)
    meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }

    with pytest.raises(DataError):
        append_bin_and_meta(tmp_path, train, val, meta, logger=_StubLogger())


def test_append_bin_and_meta_rejects_missing_token_counts(tmp_path: Path) -> None:
    """Append logic rejects existing metadata without token counters."""
    meta_path = tmp_path / "meta.pkl"
    bad_meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": "nope",
        "val_tokens": 1,
        "stoi": {"a": 0},
    }
    meta_path.write_bytes(pickle.dumps(bad_meta))
    train = np.arange(1, dtype=np.uint16)
    val = np.arange(1, dtype=np.uint16)
    meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }

    with pytest.raises(DataError):
        append_bin_and_meta(tmp_path, train, val, meta, logger=_StubLogger())


def test_append_bin_and_meta_swallows_logger_errors(tmp_path: Path) -> None:
    """Append logic ignores logger failures when reporting."""
    meta_path = tmp_path / "meta.pkl"
    good_meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }
    meta_path.write_bytes(pickle.dumps(good_meta))
    train = np.arange(1, dtype=np.uint16)
    val = np.arange(1, dtype=np.uint16)

    class _BadLogger:
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise TypeError("nope")

        def debug(self, msg: object, *args: object, **kwargs: object) -> None: ...

        def warning(self, msg: object, *args: object, **kwargs: object) -> None: ...

        def error(self, msg: object, *args: object, **kwargs: object) -> None: ...

    append_bin_and_meta(tmp_path, train, val, good_meta, logger=_BadLogger())


def test_append_bin_and_meta_logs_updated_and_skipped(tmp_path: Path) -> None:
    """Append logic logs updated and skipped files successfully."""
    meta_path = tmp_path / "meta.pkl"
    good_meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }
    meta_path.write_bytes(pickle.dumps(good_meta))
    train = np.arange(1, dtype=np.uint16)
    val = np.arange(1, dtype=np.uint16)

    logger = _StubLogger()
    append_bin_and_meta(tmp_path, train, val, good_meta, logger=logger)

    # Check that all three logging calls were made
    logged_messages = logger.messages
    assert any("[streaming] Created:" in msg for msg in logged_messages)
    assert any("[streaming] Updated:" in msg for msg in logged_messages)
    assert any("[streaming] Skipped:" in msg for msg in logged_messages)


def test_append_bin_and_meta_updates_non_existing_numeric_fields(
    tmp_path: Path,
) -> None:
    """Append logic adds new numeric fields when they don't exist in metadata."""
    meta_path = tmp_path / "meta.pkl"
    good_meta = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
    }
    meta_path.write_bytes(pickle.dumps(good_meta))
    train = np.arange(1, dtype=np.uint16)
    val = np.arange(1, dtype=np.uint16)

    # Add a new numeric field that doesn't exist in existing metadata
    meta_updates = {
        "meta_version": 1,
        "tokenizer_type": "char",
        "train_tokens": 1,
        "val_tokens": 1,
        "stoi": {"a": 0},
        "games_played": 5.0,  # New numeric field
    }

    updated_meta = append_bin_and_meta(
        tmp_path, train, val, meta_updates, logger=_StubLogger()
    )

    # The new numeric field should be added
    assert updated_meta["games_played"] == 5.0
