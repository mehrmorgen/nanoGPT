from __future__ import annotations

import logging
import pickle
from pathlib import Path

import numpy as np
import pytest

from ml_playground.experiments.copy_stage1.preparer import CopyStage1Preparer
from ml_playground.framework.configuration.models import PreparerConfig


def _make_cfg(dataset_dir: Path, raw_text_path: Path | None = None) -> PreparerConfig:
    return PreparerConfig(
        logger=logging.getLogger("copy-stage1-test"),
        raw_text_path=raw_text_path,
        extras={"dataset_dir_override": str(dataset_dir)},
    )


def test_copy_stage1_preparer_writes_expected_artifacts(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    cfg = _make_cfg(dataset_dir)

    report = CopyStage1Preparer().prepare(cfg)

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()

    assert len(report.created_files) == 3
    assert len(report.updated_files) == 0
    assert len(report.skipped_files) == 0


def test_copy_stage1_preparer_is_idempotent(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    cfg = _make_cfg(dataset_dir)

    first_report = CopyStage1Preparer().prepare(cfg)
    second_report = CopyStage1Preparer().prepare(cfg)

    assert len(first_report.created_files) == 3
    assert len(second_report.created_files) == 0
    assert len(second_report.updated_files) == 0
    assert len(second_report.skipped_files) == 3


def test_copy_stage1_preparer_is_deterministic(tmp_path: Path) -> None:
    ds_one = tmp_path / "run_one"
    ds_two = tmp_path / "run_two"

    CopyStage1Preparer().prepare(_make_cfg(ds_one))
    CopyStage1Preparer().prepare(_make_cfg(ds_two))

    assert (ds_one / "train.bin").read_bytes() == (ds_two / "train.bin").read_bytes()
    assert (ds_one / "val.bin").read_bytes() == (ds_two / "val.bin").read_bytes()

    with (ds_one / "meta.pkl").open("rb") as file_one:
        meta_one = pickle.load(file_one)
    with (ds_two / "meta.pkl").open("rb") as file_two:
        meta_two = pickle.load(file_two)

    assert meta_one == meta_two
    assert meta_one["tokenizer_type"] == "char"
    assert meta_one["vocab_size"] == 2
    assert meta_one["stoi"] == {"A": 0, "B": 1}


def test_copy_stage1_preparer_produces_balanced_tokens(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"

    CopyStage1Preparer().prepare(_make_cfg(dataset_dir))

    train_ids = np.fromfile(dataset_dir / "train.bin", dtype=np.uint16)
    val_ids = np.fromfile(dataset_dir / "val.bin", dtype=np.uint16)
    all_ids = np.concatenate((train_ids, val_ids))

    assert all_ids.size == 640
    assert int(np.sum(all_ids == 0)) == int(np.sum(all_ids == 1)) == 320


def test_copy_stage1_preparer_uses_raw_text_path_symbols(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    input_path = tmp_path / "input.txt"
    input_path.write_text("BA\n", encoding="utf-8")

    CopyStage1Preparer().prepare(_make_cfg(dataset_dir, raw_text_path=input_path))

    with (dataset_dir / "meta.pkl").open("rb") as file:
        meta = pickle.load(file)
    assert meta["stoi"] == {"A": 0, "B": 1}
    assert meta["vocab_size"] == 2


def test_copy_stage1_preparer_rejects_single_symbol_raw_text(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    input_path = tmp_path / "input.txt"
    input_path.write_text("AAAA", encoding="utf-8")

    with pytest.raises(ValueError, match="exactly two unique symbols"):
        CopyStage1Preparer().prepare(_make_cfg(dataset_dir, raw_text_path=input_path))
