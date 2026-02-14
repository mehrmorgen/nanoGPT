from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pytest

from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.data_pipeline.transforms.io import (
    coerce_seed_policy,
    seed_text_file_with_policy,
    setup_tokenizer,
    write_bin_and_meta,
)
from ml_playground.framework.core.tokenizer import create_tokenizer
from ml_playground.framework.core.tokenizer_protocol import Tokenizer
from ml_playground.framework.configuration.models import DataConfig


class _NullLogger:
    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        pass

    def debug(self, msg: object, *args: object, **kwargs: object) -> None:
        pass

    def warning(self, msg: object, *args: object, **kwargs: object) -> None:
        pass

    def error(self, msg: object, *args: object, **kwargs: object) -> None:
        pass


Metadata = dict[str, object]


def _arrays() -> tuple[np.ndarray, np.ndarray, Metadata]:
    train = np.arange(4, dtype=np.uint16)
    val = np.arange(4, dtype=np.uint16)
    meta: Metadata = {"meta_version": 1, "tokenizer_type": "char"}
    return train, val, meta


def test_write_bin_and_meta_raises_on_unreadable_existing_meta(tmp_path: Path) -> None:
    """Test write bin and meta raises on unreadable existing meta."""
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
    """Test write bin and meta detects invalid existing meta."""
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
    """Test write bin and meta existing meta swallow logger errors."""
    ds = tmp_path / "dataset"
    ds.mkdir()

    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"meta_version": 1}, f)

    class RaisingLogger:
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def debug(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def warning(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def error(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

    train, val, meta = _arrays()

    # Should not raise despite logger failures
    write_bin_and_meta(ds, train, val, meta, logger=RaisingLogger())


def test_setup_tokenizer_returns_char_tokenizer(tmp_path: Path) -> None:
    """Test setup tokenizer returns char tokenizer."""
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
    """Test setup tokenizer uses tiktoken encoding."""

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
    """Test setup tokenizer word branch."""
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


def test_setup_tokenizer_tiktoken_with_loader(tmp_path: Path) -> None:
    """Test setup_tokenizer with tiktoken and custom loader to cover lines 141, 144."""

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

    # Use a simple string instead of a function to avoid pickle issues
    loader_marker = "custom_loader_present"

    def fake_factory(
        tokenizer_type: Literal["char", "word", "tiktoken"],
        **kwargs: Any,
    ) -> Tokenizer:
        if tokenizer_type == "tiktoken":
            # Verify that loader was passed through
            assert "loader" in kwargs
            assert kwargs["loader"] == loader_marker
            # Replace with actual loader for create_tokenizer
            kwargs = dict(kwargs)
            kwargs["loader"] = lambda: DummyModule()
        return create_tokenizer(tokenizer_type, **kwargs)

    # Test with tokenizer_loader in meta (covers lines 141, 144)
    meta = {
        "tokenizer_type": "tiktoken",
        "encoding_name": "dummy",
        "tokenizer_loader": loader_marker,
        "meta_version": 1,
    }
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    tokenizer = setup_tokenizer(tmp_path, token_factory=fake_factory)
    assert tokenizer is not None
    assert tokenizer.name == "tiktoken"


def test_setup_tokenizer_tiktoken_without_loader(tmp_path: Path) -> None:
    """Test setup_tokenizer with tiktoken but no loader to ensure branch coverage."""

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
            # Verify that loader was NOT passed through when not in meta
            assert "loader" not in kwargs or kwargs["loader"] is None
            kwargs = dict(kwargs)
            kwargs["loader"] = lambda: DummyModule()
        return create_tokenizer(tokenizer_type, **kwargs)

    # Test without tokenizer_loader in meta (ensures line 141 condition is false)
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


def test_write_bin_and_meta_without_data_cfg(tmp_path: Path) -> None:
    """write_bin_and_meta should use default paths when data_cfg is None."""
    ds = tmp_path / "dataset"
    train, val, meta = _arrays()

    write_bin_and_meta(ds, train, val, meta, logger=_NullLogger(), data_cfg=None)

    assert (ds / "train.bin").exists()
    assert (ds / "val.bin").exists()
    assert (ds / "meta.pkl").exists()


def test_write_bin_and_meta_new_write_swallow_logger_errors(tmp_path: Path) -> None:
    """write_bin_and_meta should swallow logger errors during new write."""
    ds = tmp_path / "dataset"
    train, val, meta = _arrays()

    class RaisingLogger:
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise OSError("logger unavailable")

        def debug(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

        def warning(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

        def error(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

    # Should not raise despite logger failures
    write_bin_and_meta(ds, train, val, meta, logger=RaisingLogger())
    assert (ds / "train.bin").exists()


def test_setup_tokenizer_no_meta_file(tmp_path: Path) -> None:
    """setup_tokenizer should return None when meta.pkl doesn't exist."""
    tokenizer = setup_tokenizer(tmp_path)
    assert tokenizer is None


def test_setup_tokenizer_missing_tokenizer_type(tmp_path: Path) -> None:
    """setup_tokenizer should raise when tokenizer_type is missing."""
    meta = {"meta_version": 1}
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    with pytest.raises(DataError) as exc:
        setup_tokenizer(tmp_path)

    assert "missing 'tokenizer_type'" in str(exc.value)


def test_setup_tokenizer_unknown_type_fallback(tmp_path: Path) -> None:
    """setup_tokenizer should raise DataError for unknown tokenizer types."""
    meta = {
        "tokenizer_type": "unknown",
        "meta_version": 1,
    }
    with (tmp_path / "meta.pkl").open("wb") as f:
        pickle.dump(meta, f)

    # Should raise DataError via coerce_tokenizer_type
    with pytest.raises(DataError) as exc:
        setup_tokenizer(tmp_path)

    assert "Unsupported tokenizer type" in str(exc.value)


def test_seed_text_file_dst_exists(tmp_path: Path) -> None:
    """seed_text_file should return early if dst already exists."""
    from ml_playground.framework.data_pipeline.transforms.io import seed_text_file

    dst = tmp_path / "dst.txt"
    dst.write_text("existing content")

    candidate = tmp_path / "candidate.txt"
    candidate.write_text("candidate content")

    # Should not overwrite
    seed_text_file(dst, [candidate])
    assert dst.read_text() == "existing content"


def test_seed_text_file_creates_parent_dirs(tmp_path: Path) -> None:
    """seed_text_file should create parent directories if needed."""
    from ml_playground.framework.data_pipeline.transforms.io import seed_text_file

    dst = tmp_path / "subdir" / "nested" / "dst.txt"
    candidate = tmp_path / "candidate.txt"
    candidate.write_text("content")

    seed_text_file(dst, [candidate])
    assert dst.exists()
    assert dst.read_text() == "content"


def test_seed_text_file_no_candidates_raises(tmp_path: Path) -> None:
    """seed_text_file should raise when no candidates exist."""
    from ml_playground.framework.data_pipeline.transforms.io import seed_text_file

    dst = tmp_path / "dst.txt"
    candidates = [tmp_path / "missing1.txt", tmp_path / "missing2.txt"]

    with pytest.raises(FileNotFoundError) as exc:
        seed_text_file(dst, candidates)

    assert "none of the candidate paths exist" in str(exc.value)


def test_write_bin_and_meta_with_data_cfg(tmp_path: Path) -> None:
    """write_bin_and_meta should use data_cfg paths when provided."""
    ds = tmp_path / "dataset"
    train, val, meta = _arrays()

    data_cfg = DataConfig(
        train_bin="custom_train.bin",
        val_bin="custom_val.bin",
        meta_pkl="custom_meta.pkl",
    )

    write_bin_and_meta(ds, train, val, meta, logger=_NullLogger(), data_cfg=data_cfg)

    assert (ds / "custom_train.bin").exists()
    assert (ds / "custom_val.bin").exists()
    assert (ds / "custom_meta.pkl").exists()


def test_write_bin_and_meta_existing_meta_with_logger_error(tmp_path: Path) -> None:
    """write_bin_and_meta should swallow logger errors when reusing existing meta."""
    ds = tmp_path / "dataset"
    ds.mkdir()

    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"meta_version": 1}, f)

    class RaisingLogger:
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def debug(self, msg: object, *args: object, **kwargs: object) -> None:
            raise ValueError("logger unavailable")

        def warning(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

        def error(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

    train, val, meta = _arrays()

    # Should not raise despite logger failures
    write_bin_and_meta(ds, train, val, meta, logger=RaisingLogger())


def test_write_bin_and_meta_existing_meta_logs_successfully(tmp_path: Path) -> None:
    """write_bin_and_meta should log when reusing existing meta successfully."""
    ds = tmp_path / "dataset"
    ds.mkdir()

    (ds / "train.bin").write_bytes(b"train")
    (ds / "val.bin").write_bytes(b"val")
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"meta_version": 1}, f)

    logged_messages: list[str] = []

    class RecordingLogger:
        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            logged_messages.append(str(msg))

        def debug(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

        def warning(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

        def error(self, msg: object, *args: object, **kwargs: object) -> None:
            pass

    train, val, meta = _arrays()

    write_bin_and_meta(ds, train, val, meta, logger=RecordingLogger())

    # Both logger.info calls should have been made
    assert any("Created:" in msg for msg in logged_messages)
    assert any("Skipped:" in msg for msg in logged_messages)


def test_coerce_seed_policy_none_returns_auto() -> None:
    """coerce_seed_policy should return 'auto' when policy is None."""
    assert coerce_seed_policy(None) == "auto"


def test_coerce_seed_policy_auto_returns_auto() -> None:
    """coerce_seed_policy should return 'auto' when policy is 'auto'."""
    assert coerce_seed_policy("auto") == "auto"


def test_coerce_seed_policy_fail_fast_returns_fail_fast() -> None:
    """coerce_seed_policy should return 'fail_fast' when policy is 'fail_fast'."""
    assert coerce_seed_policy("fail_fast") == "fail_fast"


def test_coerce_seed_policy_unsupported_raises() -> None:
    """coerce_seed_policy should raise DataError for unsupported policies."""
    with pytest.raises(DataError) as exc:
        coerce_seed_policy("unsupported")

    assert "Unsupported seed policy" in str(exc.value)


def test_coerce_seed_policy_non_string_raises() -> None:
    """coerce_seed_policy should raise DataError for non-string policies."""
    with pytest.raises(DataError) as exc:
        coerce_seed_policy(123)

    assert "Unsupported seed policy" in str(exc.value)


def test_seed_text_file_with_policy_auto(tmp_path: Path) -> None:
    """seed_text_file_with_policy with 'auto' should call seed_text_file."""
    dst = tmp_path / "dst.txt"
    candidate = tmp_path / "candidate.txt"
    candidate.write_text("content")

    seed_text_file_with_policy(dst, [candidate], policy="auto")

    assert dst.exists()
    assert dst.read_text() == "content"


def test_seed_text_file_with_policy_fail_fast(tmp_path: Path) -> None:
    """seed_text_file_with_policy with 'fail_fast' should copy first existing candidate."""
    dst = tmp_path / "dst.txt"
    candidate1 = tmp_path / "candidate1.txt"
    candidate1.write_text("content1")
    candidate2 = tmp_path / "candidate2.txt"
    candidate2.write_text("content2")

    seed_text_file_with_policy(dst, [candidate1, candidate2], policy="fail_fast")

    assert dst.exists()
    assert dst.read_text() == "content1"


def test_seed_text_file_with_policy_fail_fast_no_candidates_raises(
    tmp_path: Path,
) -> None:
    """seed_text_file_with_policy with 'fail_fast' should raise when no candidates exist."""
    dst = tmp_path / "dst.txt"
    candidates = [tmp_path / "missing1.txt", tmp_path / "missing2.txt"]

    with pytest.raises(FileNotFoundError) as exc:
        seed_text_file_with_policy(dst, candidates, policy="fail_fast")

    assert "none of the candidate paths exist" in str(exc.value)


def test_seed_text_file_with_policy_none_defaults_to_auto(tmp_path: Path) -> None:
    """seed_text_file_with_policy with None policy should default to 'auto'."""
    dst = tmp_path / "dst.txt"
    candidate = tmp_path / "candidate.txt"
    candidate.write_text("content")

    seed_text_file_with_policy(dst, [candidate], policy=None)

    assert dst.exists()
    assert dst.read_text() == "content"
