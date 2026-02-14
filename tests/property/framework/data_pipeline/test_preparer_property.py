from __future__ import annotations

from collections.abc import Callable
from typing import Literal, cast
from pathlib import Path
from tempfile import TemporaryDirectory

import hypothesis.strategies as st
import numpy as np
import pytest
from hypothesis import HealthCheck, example, given, settings

from ml_playground.framework.configuration.models import (
    DataConfig,
    PreparerConfig,
    MetadataConfig,
)
from ml_playground.framework.core.error_handling import DataError
from ml_playground.framework.core.tokenizer import create_tokenizer
from ml_playground.framework.data_pipeline.preparer import (
    PreparationOutcome,
    create_pipeline,
)
from ml_playground.framework.core.file_state import FileState

_TEXT = st.text(alphabet="abcdefghijklmnopqrstuvwxyz \n", min_size=1, max_size=128)
_SPLITS = st.floats(
    min_value=0.1,
    max_value=0.9,
    allow_nan=False,
    allow_infinity=False,
)
_META_EXTRAS = st.dictionaries(
    keys=st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=1, max_size=6),
    values=st.integers(min_value=0, max_value=10),
    max_size=3,
)


@given(  # type: ignore[reportAny]
    text=_TEXT, split=_SPLITS, meta=_META_EXTRAS
)
@example(text="abc", split=0.5, meta={"mode": 1})
@settings(
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
def test_prepare_pipeline_creates_artifacts(
    text: str,
    split: float,
    meta: dict[str, int],
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """`create_pipeline().prepare_from_text` yields deterministic artifacts matching metadata expectations."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        shared = metadata_config_factory(base)
        cfg = PreparerConfig(tokenizer_type="char", extras={"split": split})
        pipeline = create_pipeline(cfg, shared)

        tokenizer = create_tokenizer("char")
        outcome = pipeline.prepare_from_text(
            text,
            tokenizer,
            split=split,
            meta_extras=meta,
        )

        assert isinstance(outcome, PreparationOutcome)
        for file_path in outcome.created_files + outcome.updated_files:
            assert file_path.exists()
        assert not outcome.skipped_files

        train_expected = int(len(text) * split)
        val_expected = len(text) - train_expected
        assert outcome.metadata["train_tokens"] == train_expected
        assert outcome.metadata["val_tokens"] == val_expected
        for key, value in meta.items():
            assert outcome.metadata.get(key) == value

        train_arr = np.fromfile(shared.dataset_dir / "train.bin", dtype=np.uint16)
        val_arr = np.fromfile(shared.dataset_dir / "val.bin", dtype=np.uint16)
        assert train_arr.size == train_expected
        assert val_arr.size == val_expected


def test_prepare_pipeline_uses_data_config_paths(
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Custom `DataConfig` extras should control output artifact locations."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        shared = metadata_config_factory(base)
        data_cfg = DataConfig(
            train_bin="train_custom.bin",
            val_bin="val_custom.bin",
            meta_pkl="meta_custom.pkl",
        )
        cfg = PreparerConfig(tokenizer_type="char", extras={"data_config": data_cfg})
        pipeline = create_pipeline(cfg, shared)

        tokenizer = create_tokenizer("char")
        pipeline.prepare_from_text("hello world", tokenizer)

        assert (shared.dataset_dir / "train_custom.bin").exists()
        assert (shared.dataset_dir / "val_custom.bin").exists()
        assert (shared.dataset_dir / "meta_custom.pkl").exists()


def test_prepare_pipeline_rejects_non_dataconfig_extra(
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Providing a non-`DataConfig` `data_config` extra must raise `DataError`."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        shared = metadata_config_factory(base)
        cfg = PreparerConfig(tokenizer_type="char", extras={"data_config": "invalid"})
        pipeline = create_pipeline(cfg, shared)

        with pytest.raises(DataError, match="data_config"):
            pipeline.prepare_from_text("data", create_tokenizer("char"))


@pytest.mark.parametrize(  # type: ignore[reportAny]
    "raw_split", ["bad", -0.1, 1.5]
)
def test_prepare_pipeline_invalid_split_extra(
    raw_split: object,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Invalid `split` extras should raise `DataError` when no explicit split is provided."""

    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        shared = metadata_config_factory(base)
        cfg = PreparerConfig(tokenizer_type="char", extras={"split": raw_split})
        pipeline = create_pipeline(cfg, shared)

        with pytest.raises(DataError, match="split ratio"):
            pipeline.prepare_from_text("sample", create_tokenizer("char"))


def test_pipeline_run_reads_raw_text_path(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Test pipeline run reads raw text path."""
    base = tmp_path
    raw_dir = base / "raw"
    raw_dir.mkdir()
    raw_file = raw_dir / "input.txt"
    raw_file.write_text("hello world", encoding="utf-8")

    shared = metadata_config_factory(base)
    cfg = PreparerConfig(tokenizer_type="char", raw_text_path=raw_file)
    pipeline = create_pipeline(cfg, shared)

    outcome = pipeline.run()

    assert outcome.metadata["train_tokens"] > 0
    assert (shared.dataset_dir / "train.bin").exists()
    assert (shared.dataset_dir / "val.bin").exists()
    assert (shared.dataset_dir / "meta.pkl").exists()


def test_pipeline_run_uses_tokenizer_factory(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Test pipeline run uses tokenizer factory."""
    base = tmp_path
    raw_file = base / "input.txt"
    raw_file.write_text("abc", encoding="utf-8")

    shared = metadata_config_factory(base)
    calls: list[object] = []

    def _factory(kind: object) -> object:
        calls.append(kind)
        if not isinstance(kind, str) or kind not in {"char", "word", "tiktoken"}:
            raise AssertionError(f"Unexpected tokenizer kind: {kind!r}")
        resolved = cast(Literal["char", "word", "tiktoken"], kind)
        return create_tokenizer(resolved)

    cfg = PreparerConfig(
        tokenizer_type="char",
        raw_text_path=raw_file,
        tokenizer_factory=_factory,
    )
    pipeline = create_pipeline(cfg, shared)
    pipeline.run()

    assert calls  # factory invoked
    assert (shared.dataset_dir / "meta.pkl").exists()


def test_pipeline_run_requires_raw_text_path(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Test pipeline run requires raw text path."""
    shared = metadata_config_factory(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char")
    pipeline = create_pipeline(cfg, shared)

    with pytest.raises(DataError, match="No raw text path"):
        pipeline.run()


def test_pipeline_run_uses_custom_read_text_fn(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Pipeline should use custom `read_text_fn` when provided."""
    base = tmp_path
    raw_file = base / "input.txt"
    raw_file.write_text("original content", encoding="utf-8")

    shared = metadata_config_factory(base)
    calls: list[Path] = []

    def _custom_reader(path: Path) -> str:
        calls.append(path)
        return "custom content"

    cfg = PreparerConfig(
        tokenizer_type="char",
        raw_text_path=raw_file,
        read_text_fn=_custom_reader,
    )
    pipeline = create_pipeline(cfg, shared)
    outcome = pipeline.run()

    assert calls  # custom reader invoked
    assert outcome.metadata["train_tokens"] > 0


def test_pipeline_cfg_property(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Pipeline `cfg` property should return the preparer config."""
    shared = metadata_config_factory(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char")
    pipeline = create_pipeline(cfg, shared)

    assert pipeline.cfg is cfg


def test_pipeline_shared_property(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Pipeline `shared` property should return the shared config."""
    shared = metadata_config_factory(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char")
    pipeline = create_pipeline(cfg, shared)

    assert pipeline.shared is shared


def test_pipeline_output_snapshot(
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Pipeline `output_snapshot` should capture file states."""
    shared = metadata_config_factory(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char")
    pipeline = create_pipeline(cfg, shared)

    test_file = tmp_path / "test.txt"
    test_file.write_text("data", encoding="utf-8")

    snapshot: dict[str, FileState] = pipeline.output_snapshot([test_file])
    assert str(test_file) in snapshot
    assert snapshot[str(test_file)] is not None


@pytest.mark.parametrize(  # type: ignore[reportAny]
    "split_value", [0.0, 0.5, 1.0]
)
def test_pipeline_default_split_valid_range(
    split_value: float,
    tmp_path: Path,
    metadata_config_factory: Callable[[Path], MetadataConfig],
) -> None:
    """Pipeline should accept valid split ratios in [0.0, 1.0]."""
    shared = metadata_config_factory(tmp_path)
    cfg = PreparerConfig(tokenizer_type="char", extras={"split": split_value})
    pipeline = create_pipeline(cfg, shared)

    # Accessing _default_split indirectly via prepare_from_text
    tokenizer = create_tokenizer("char")
    outcome = pipeline.prepare_from_text("test data", tokenizer)

    assert isinstance(outcome, PreparationOutcome)
