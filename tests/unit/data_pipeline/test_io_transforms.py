from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pytest

from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.transforms.io import (
    setup_tokenizer,
    write_bin_and_meta,
)
from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.core.tokenizer_protocol import Tokenizer


class _NullLogger:
    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def debug(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, msg: str, *args: object, **kwargs: object) -> None:
        pass

    def error(self, msg: str, *args: object, **kwargs: object) -> None:
        pass


def _arrays() -> tuple[np.ndarray, np.ndarray, dict]:
    train = np.arange(4, dtype=np.uint16)
    val = np.arange(4, dtype=np.uint16)
    meta = {"meta_version": 1, "tokenizer_type": "char"}
    return train, val, meta


def test_write_bin_and_meta_raises_on_unreadable_existing_meta(tmp_path: Path) -> None:
    ds = tmp_path / "dataset"
    ds.mkdir()
    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    (ds / "meta.pkl").write_text("not a pickle", encoding="utf-8")

    train, val, meta = _arrays()

    with pytest.raises(DataError) as exc:
        write_bin_and_meta(ds, train, val, meta, logger=_NullLogger())

    assert "Failed to read existing meta.pkl" in str(exc.value)


def test_write_bin_and_meta_detects_invalid_existing_meta(tmp_path: Path) -> None:
    ds = tmp_path / "dataset"
    ds.mkdir()
    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"not_meta": True}, f)

    train, val, meta = _arrays()

    with pytest.raises(DataError) as exc:
        write_bin_and_meta(ds, train, val, meta, logger=_NullLogger())

    assert "Invalid existing meta.pkl" in str(exc.value)


def test_write_bin_and_meta_existing_meta_swallow_logger_errors(tmp_path: Path) -> None:
    ds = tmp_path / "dataset"
    ds.mkdir()

    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"meta_version": 1}, f)

    class RaisingLogger:
        def info(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def debug(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def warning(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def error(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

    train, val, meta = _arrays()

    # Should not raise despite logger failures
    write_bin_and_meta(ds, train, val, meta, logger=RaisingLogger())


def test_setup_tokenizer_returns_char_tokenizer(tmp_path: Path) -> None:
    meta = {
        "tokenizer_type": "char",
        "stoi": {"a": 0},
        "meta_version": 1,
    }
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    tokenizer = setup_tokenizer(tmp_path)
    assert tokenizer is not None
    assert tokenizer.name == "char"
    assert tokenizer.decode([0]) == "a"


def test_setup_tokenizer_uses_tiktoken_encoding(tmp_path: Path) -> None:
    class DummyEncoding:
        n_vocab = 1
        _mergeable_ranks = {"a": 0}

        def encode(self, text: str, allowed_special: set[str]) -> list[int]:
            return [0 for _ in text]

        def decode(self, token_ids: list[int]) -> str:
            return "a" * len(token_ids)

    class DummyModule:
        @staticmethod
        def get_encoding(name: str) -> DummyEncoding:
            return DummyEncoding()

    def fake_factory(
        tokenizer_type: Literal["char", "word", "tiktoken"],
        **kwargs: Any,
    ) -> Tokenizer:
        if tokenizer_type == "tiktoken":
            kwargs = dict(kwargs)
            kwargs.setdefault("loader", lambda: DummyModule())
        return create_tokenizer(tokenizer_type, **kwargs)

    meta = {
        "tokenizer_type": "tiktoken",
        "encoding_name": "dummy",
        "meta_version": 1,
    }
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    tokenizer = setup_tokenizer(tmp_path, token_factory=fake_factory)
    assert tokenizer is not None
    assert tokenizer.name == "tiktoken"
    assert tokenizer.decode([0, 0]) == "aa"


def test_setup_tokenizer_word_branch(tmp_path: Path) -> None:
    meta = {
        "tokenizer_type": "word",
        "stoi": {"hello": 0},
        "meta_version": 1,
    }
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    tokenizer = setup_tokenizer(tmp_path)
    assert tokenizer is not None
    assert tokenizer.name == "word"
