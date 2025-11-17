from __future__ import annotations

from pathlib import Path
import pickle
from typing import Dict, List, Mapping

import numpy as np
import pytest

from ml_playground.configuration.models import DataConfig, PreparerConfig, SharedConfig
from ml_playground.core.error_handling import DataError
from ml_playground.data_pipeline.preparer import (
    PreparationOutcome,
    create_pipeline,
)
from ml_playground.data_pipeline.transforms.io import (
    seed_text_file,
    write_bin_and_meta,
)
from ml_playground.core.tokenizer_protocol import Tokenizer


"""Logging helpers are provided via fixtures in conftest.py (list_logger, list_logger_factory)."""


class DummyTok:
    def __init__(self) -> None:
        self._name = "dummy"
        # Provide a minimal vocab mapping to satisfy the protocol
        self._vocab: dict[str, int] = {"a": 1, "b": 2}

    @property
    def name(self) -> str:  # noqa: D401
        return self._name

    @property
    def vocab_size(self) -> int:  # noqa: D401
        # Return a fixed size to match test expectations
        return 123

    @property
    def vocab(self) -> Mapping[str, int]:  # noqa: D401
        return self._vocab

    def encode(self, text: str) -> List[int]:  # noqa: D401
        return [self._vocab.get(ch, 0) for ch in text]

    def decode(self, token_ids: List[int]) -> str:  # noqa: D401
        inv = {v: k for k, v in self._vocab.items()}
        return "".join(inv.get(tid, "?") for tid in token_ids)


# ---- small helpers ----


Metadata = Dict[str, object]


def _mk_arrays(n: int) -> tuple[np.ndarray, np.ndarray, Metadata]:
    train: np.ndarray = np.arange(n, dtype=np.uint16)
    val: np.ndarray = np.arange(n, dtype=np.uint16)
    meta: Metadata = {"meta_version": 1}
    return train, val, meta


# Additional fake tokenizer for tiktoken-like metadata enrichment tests
class _FakeTiktoken(Tokenizer):
    def __init__(self) -> None:
        self._name = "tiktoken"
        self.encoding_name = "gpt2"
        self._vocab_size = 1000
        self._vocab: dict[str, int] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def vocab_size(self) -> int:
        return self._vocab_size

    @property
    def vocab(self) -> Mapping[str, int]:
        return self._vocab

    def encode(self, text: str) -> list[int]:
        return list(range(len(text)))

    def decode(self, token_ids: list[int]) -> str:
        return "".join("x" for _ in token_ids)


# ---- split helpers ----


# These split tests are now covered by property tests in tests/property/data_pipeline/test_preparer_property.py
# Property tests cover split invariants with comprehensive input generation using Hypothesis strategies
# See: test_split_train_val_properties


# ---- metadata helpers ----


# These metadata tests are now covered by property tests in tests/property/data_pipeline/test_preparer_property.py
# Property tests cover metadata creation invariants with comprehensive tokenizer types using Hypothesis strategies
# See: test_create_standardized_metadata_properties


# ---- prepare_with_tokenizer ----


# These prepare_with_tokenizer tests are now covered by property tests in tests/property/data_pipeline/test_preparer_property.py
# Property tests cover tokenizer preparation invariants with comprehensive text and tokenizer types using Hypothesis strategies
# See: test_prepare_with_tokenizer_properties


def test_write_bin_and_meta_logging_exception_is_ignored(tmp_path: Path) -> None:
    """Test write bin and meta logging exception is ignored."""

    class RaisingLogger:
        def info(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("fail")

        def debug(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("fail")

        def warning(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("fail")

        def error(self, msg: str, *args: object, **kwargs: object) -> None:
            raise ValueError("fail")

    train, val, meta = _mk_arrays(3)
    write_bin_and_meta(tmp_path, train, val, meta, logger=RaisingLogger())


def test_write_bin_and_meta_already_exists_logs(tmp_path: Path) -> None:
    """Test write bin and meta already exists logs."""
    ds = tmp_path / "ds"
    ds.mkdir(parents=True, exist_ok=True)
    # Pre-create valid artifacts to trigger early-return logging path
    (ds / "train.bin").write_bytes(np.arange(4, dtype=np.uint16).tobytes())
    (ds / "val.bin").write_bytes(np.arange(4, dtype=np.uint16).tobytes())
    with (ds / "meta.pkl").open("wb") as f:
        pickle.dump({"meta_version": 1}, f)

    class ListLogger:
        def __init__(self) -> None:
            self.infos: list[str] = []

        def info(self, msg: str, *args: object, **kwargs: object) -> None:
            message = msg % args if args else msg
            self.infos.append(str(message))

        def debug(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

        def warning(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

        def error(self, msg: str, *args: object, **kwargs: object) -> None:
            pass

    logger = ListLogger()
    train = np.arange(2, dtype=np.uint16)
    val = np.arange(2, dtype=np.uint16)
    meta: Metadata = {"meta_version": 1}

    write_bin_and_meta(ds, train, val, meta, logger=logger, data_cfg=None)

    # Ensure the logging branch executed (we do not assert exact content to avoid brittleness)
    assert any("[prepare] Created" in m for m in logger.infos)
    assert any("[prepare] Skipped" in m for m in logger.infos)


# ---- preparation pipeline helpers ----


def _make_shared(tmp_path: Path) -> SharedConfig:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_out_dir = tmp_path / "train"
    train_out_dir.mkdir(parents=True, exist_ok=True)
    sample_out_dir = tmp_path / "sample"
    sample_out_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("{}", encoding="utf-8")
    return SharedConfig(
        experiment="unit",
        config_path=cfg_path,
        project_home=tmp_path,
        dataset_dir=dataset_dir,
        train_out_dir=train_out_dir,
        sample_out_dir=sample_out_dir,
    )


def test_pipeline_run_uses_tokenizer_factory(tmp_path: Path) -> None:
    """Test pipeline run uses tokenizer factory."""
    text_path = tmp_path / "raw.txt"
    text_path.write_text("abba", encoding="utf-8")
    shared = _make_shared(tmp_path)

    called: dict[str, int] = {"factory": 0}

    def _factory(kind: object) -> DummyTok:
        called["factory"] += 1
        assert kind == "char"
        return DummyTok()

    cfg = PreparerConfig(
        raw_text_path=text_path,
        tokenizer_type="char",
        tokenizer_factory=_factory,
    )
    pipeline = create_pipeline(cfg, shared)
    outcome = pipeline.run()
    assert isinstance(outcome, PreparationOutcome)
    assert called["factory"] == 1
    # Artifacts are written into dataset_dir
    assert (shared.dataset_dir / "train.bin").exists()
    assert (shared.dataset_dir / "val.bin").exists()
    assert (shared.dataset_dir / "meta.pkl").exists()


def test_pipeline_prepare_from_text_respects_meta_extras(tmp_path: Path) -> None:
    """Test pipeline prepare from text respects meta extras."""
    shared = _make_shared(tmp_path)
    cfg = PreparerConfig(
        raw_text_path=tmp_path / "raw.txt",  # unused; prepare_from_text provides text
        tokenizer_type="char",
    )
    pipeline = create_pipeline(cfg, shared)
    outcome = pipeline.prepare_from_text(
        "abba",
        DummyTok(),
        meta_extras={"source": "unit-test"},
    )
    assert outcome.metadata["source"] == "unit-test"


def test_pipeline_resolves_custom_data_config(tmp_path: Path) -> None:
    """Test pipeline resolves custom data config."""
    shared = _make_shared(tmp_path)
    data_cfg = DataConfig(
        train_bin="train-custom.bin",
        val_bin="val-custom.bin",
        meta_pkl="meta-custom.pkl",
    )
    raw_path = tmp_path / "text.txt"
    raw_path.write_text("abba", encoding="utf-8")
    cfg = PreparerConfig(
        raw_text_path=raw_path,
        tokenizer_type="char",
        extras={"data_config": data_cfg},
    )
    pipeline = create_pipeline(cfg, shared)
    pipeline.run()
    # Custom paths should be respected
    assert (shared.dataset_dir / "train-custom.bin").exists()
    assert (shared.dataset_dir / "val-custom.bin").exists()
    assert (shared.dataset_dir / "meta-custom.pkl").exists()


def test_pipeline_rejects_invalid_data_config(tmp_path: Path) -> None:
    """Test pipeline rejects invalid data config."""
    shared = _make_shared(tmp_path)
    cfg = PreparerConfig(
        raw_text_path=tmp_path / "text.txt",
        tokenizer_type="char",
        extras={"data_config": {"train_bin": "custom.bin"}},
    )
    pipeline = create_pipeline(cfg, shared)
    with pytest.raises(DataError, match="data_config must be a DataConfig"):
        pipeline.prepare_from_text("abba", DummyTok())


def test_pipeline_split_validation(tmp_path: Path) -> None:
    """Test pipeline split validation."""
    shared = _make_shared(tmp_path)
    cfg = PreparerConfig(
        raw_text_path=tmp_path / "text.txt",
        tokenizer_type="char",
        extras={"split": "not-a-number"},
    )
    pipeline = create_pipeline(cfg, shared)
    with pytest.raises(DataError, match="Invalid split ratio"):
        pipeline.prepare_from_text("abba", DummyTok())

    cfg_bad = cfg.model_copy(update={"extras": {"split": 1.5}})
    pipeline_bad = create_pipeline(cfg_bad, shared)
    with pytest.raises(DataError, match="within \\[0.0, 1.0]"):
        pipeline_bad.prepare_from_text("abba", DummyTok())


def test_pipeline_load_raw_text_with_read_fn(tmp_path: Path) -> None:
    """Test pipeline load raw text with read fn."""
    text_path = tmp_path / "raw.txt"
    text_path.write_text("ignored", encoding="utf-8")
    shared = _make_shared(tmp_path)
    seen: dict[str, Path] = {}

    def _reader(path: Path) -> str:
        seen["path"] = path
        return "hello"

    cfg = PreparerConfig(
        raw_text_path=text_path,
        tokenizer_type="char",
        read_text_fn=_reader,
    )
    pipeline = create_pipeline(cfg, shared)
    pipeline.run()
    assert seen["path"] == text_path
    with (shared.dataset_dir / "meta.pkl").open("rb") as fh:
        meta = pickle.load(fh)
    assert meta["train_tokens"] > 0


def test_pipeline_load_raw_text_missing_path(tmp_path: Path) -> None:
    """Test pipeline load raw text missing path."""
    shared = _make_shared(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char")  # raw_text_path defaults to None
    pipeline = create_pipeline(cfg, shared)
    with pytest.raises(DataError, match="No raw text path provided"):
        pipeline.run()


# ---- seed file helpers ----


def test_seed_text_file_copies_first_existing_candidate(tmp_path: Path) -> None:
    """Test seed text file copies first existing candidate."""
    src1 = tmp_path / "a.txt"
    src2 = tmp_path / "b.txt"
    dst = tmp_path / "out" / "seed.txt"

    # Only src2 exists
    src2.write_text("hello", encoding="utf-8")

    seed_text_file(dst, [src1, src2])
    assert dst.exists()
    assert dst.read_text(encoding="utf-8") == "hello"


def test_seed_text_file_noop_if_dst_exists(tmp_path: Path) -> None:
    """Test seed text file noop if dst exists."""
    src = tmp_path / "in.txt"
    dst = tmp_path / "dst.txt"
    src.write_text("hello", encoding="utf-8")
    dst.write_text("old", encoding="utf-8")

    seed_text_file(dst, [src])
    # Should not overwrite existing dst
    assert dst.read_text(encoding="utf-8") == "old"


def test_seed_text_file_raises_when_no_candidates_exist(tmp_path: Path) -> None:
    """Test seed text file raises when no candidates exist."""
    dst = tmp_path / "dst.txt"
    with pytest.raises(FileNotFoundError):
        seed_text_file(dst, [tmp_path / "missing1.txt", tmp_path / "missing2.txt"])
