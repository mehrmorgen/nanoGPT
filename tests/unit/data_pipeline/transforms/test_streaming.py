from __future__ import annotations

from pathlib import Path

import pytest
import pickle

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.streaming import (
    validate_streaming_records,
    _load_meta,
    _refresh_metadata,
    append_bin_and_meta,
)


def test_validate_streaming_records_raises_on_non_mapping() -> None:
    with pytest.raises(DataError, match="must be a mapping"):
        list(validate_streaming_records([{"a": 1}, "not a mapping"]))


def test_validate_streaming_records_raises_on_missing_fields() -> None:
    with pytest.raises(DataError, match="missing fields"):
        list(validate_streaming_records([{"start": 0, "winner": 1}]))


def test_load_meta_raises_on_pickle_error(tmp_path: Path) -> None:
    bad_meta = tmp_path / "meta.pkl"
    bad_meta.write_bytes(b"not valid pickle")
    with pytest.raises(DataError, match="Failed to read existing meta.pkl"):
        _load_meta(bad_meta)


def test_load_meta_raises_on_os_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing.pkl"
    with pytest.raises(DataError, match="Failed to read existing meta.pkl"):
        _load_meta(missing)


def test_refresh_metadata_raises_on_missing_token_counts() -> None:
    with pytest.raises(DataError, match="missing train/val token counts"):
        _refresh_metadata({}, train_tokens_added=10, val_tokens_added=5, updates={})


def test_append_bin_and_meta_raises_when_existing_meta_missing_counts(
    tmp_path: Path,
) -> None:
    """Append raises when existing metadata lacks train/val token counts."""
    ds_dir = tmp_path
    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"
    train_path.write_bytes(b"\x00\x01")
    val_path.write_bytes(b"\x02\x03")
    meta_path.write_bytes(
        pickle.dumps({"meta_version": 1, "tokenizer_type": "char", "games": 1})
    )

    class _Logger:
        def info(self, *args, **kwargs):
            pass

    with pytest.raises(DataError, match="train_tokens.*val_tokens"):
        append_bin_and_meta(
            ds_dir,
            train=b"\x00\x01",
            val=b"\x02\x03",
            meta={"meta_version": 1, "tokenizer_type": "char", "games": 2},
            logger=_Logger(),
        )
