from __future__ import annotations

import logging
import pickle
from pathlib import Path

from ml_playground.experiments.copy_stage0.preparer import CopyStage0Preparer
from ml_playground.framework.configuration.models import PreparerConfig


def _make_cfg(dataset_dir: Path) -> PreparerConfig:
    return PreparerConfig(
        logger=logging.getLogger("copy-stage0-test"),
        extras={"dataset_dir_override": str(dataset_dir)},
    )


def test_copy_stage0_preparer_writes_expected_artifacts(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    cfg = _make_cfg(dataset_dir)

    report = CopyStage0Preparer().prepare(cfg)

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()

    assert len(report.created_files) == 3
    assert len(report.updated_files) == 0
    assert len(report.skipped_files) == 0


def test_copy_stage0_preparer_is_idempotent(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "datasets"
    cfg = _make_cfg(dataset_dir)

    first_report = CopyStage0Preparer().prepare(cfg)
    second_report = CopyStage0Preparer().prepare(cfg)

    assert len(first_report.created_files) == 3
    assert len(second_report.created_files) == 0
    assert len(second_report.updated_files) == 0
    assert len(second_report.skipped_files) == 3


def test_copy_stage0_preparer_is_deterministic(tmp_path: Path) -> None:
    ds_one = tmp_path / "run_one"
    ds_two = tmp_path / "run_two"

    CopyStage0Preparer().prepare(_make_cfg(ds_one))
    CopyStage0Preparer().prepare(_make_cfg(ds_two))

    assert (ds_one / "train.bin").read_bytes() == (ds_two / "train.bin").read_bytes()
    assert (ds_one / "val.bin").read_bytes() == (ds_two / "val.bin").read_bytes()

    with (ds_one / "meta.pkl").open("rb") as file_one:
        meta_one = pickle.load(file_one)
    with (ds_two / "meta.pkl").open("rb") as file_two:
        meta_two = pickle.load(file_two)

    assert meta_one == meta_two
    assert meta_one["tokenizer_type"] == "char"
    assert meta_one["vocab_size"] == 1
    assert meta_one["stoi"] == {"A": 0}
