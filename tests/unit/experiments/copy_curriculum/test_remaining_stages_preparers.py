from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Any

import pytest

from ml_playground.experiments.copy_stage2.preparer import CopyStage2Preparer
from ml_playground.experiments.copy_stage3.preparer import CopyStage3Preparer
from ml_playground.experiments.copy_stage4.preparer import CopyStage4Preparer
from ml_playground.experiments.copy_stage5.preparer import CopyStage5Preparer
from ml_playground.experiments.copy_stage6.preparer import CopyStage6Preparer
from ml_playground.framework.configuration.models import PreparerConfig


@pytest.mark.parametrize(
    ("preparer", "stage", "min_vocab_size"),
    [
        (CopyStage2Preparer(), "copy_stage2", 3),
        (CopyStage3Preparer(), "copy_stage3", 4),
        (CopyStage4Preparer(), "copy_stage4", 4),
        (CopyStage5Preparer(), "copy_stage5", 8),
        (CopyStage6Preparer(), "copy_stage6", 12),
    ],
)
def test_remaining_stage_preparers_write_artifacts_and_are_idempotent(
    tmp_path: Path,
    preparer: Any,
    stage: str,
    min_vocab_size: int,
) -> None:
    dataset_dir = tmp_path / stage
    cfg = PreparerConfig(
        logger=logging.getLogger(f"{stage}-test"),
        raw_text_path=None,
        extras={"dataset_dir_override": str(dataset_dir)},
    )

    first_report = preparer.prepare(cfg)
    second_report = preparer.prepare(cfg)

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()

    assert len(first_report.created_files) == 3
    assert len(second_report.created_files) == 0
    assert len(second_report.updated_files) == 0
    assert len(second_report.skipped_files) == 3

    with (dataset_dir / "meta.pkl").open("rb") as f:
        meta = pickle.load(f)

    assert meta["stage"] == stage
    assert meta["tokenizer_type"] == "char"
    assert meta["vocab_size"] >= min_vocab_size


def test_stage2_metadata_contains_eos_token(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "stage2"
    cfg = PreparerConfig(
        logger=logging.getLogger("copy-stage2-test"),
        raw_text_path=None,
        extras={"dataset_dir_override": str(dataset_dir)},
    )

    CopyStage2Preparer().prepare(cfg)

    with (dataset_dir / "meta.pkl").open("rb") as f:
        meta = pickle.load(f)

    assert "~" in meta["stoi"]


def test_stage3_metadata_contains_sos_and_eos_tokens(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "stage3"
    cfg = PreparerConfig(
        logger=logging.getLogger("copy-stage3-test"),
        raw_text_path=None,
        extras={"dataset_dir_override": str(dataset_dir)},
    )

    CopyStage3Preparer().prepare(cfg)

    with (dataset_dir / "meta.pkl").open("rb") as f:
        meta = pickle.load(f)

    assert "^" in meta["stoi"]
    assert "~" in meta["stoi"]


def test_stage4_metadata_contains_length_bounds(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "stage4"
    cfg = PreparerConfig(
        logger=logging.getLogger("copy-stage4-test"),
        raw_text_path=None,
        extras={"dataset_dir_override": str(dataset_dir), "min_length": 3, "max_length": 7},
    )

    CopyStage4Preparer().prepare(cfg)

    with (dataset_dir / "meta.pkl").open("rb") as f:
        meta = pickle.load(f)

    assert meta["min_length"] == 3
    assert meta["max_length"] == 7


def test_stage5_and_stage6_store_record_counts(tmp_path: Path) -> None:
    stage5_dir = tmp_path / "stage5"
    stage6_dir = tmp_path / "stage6"

    CopyStage5Preparer().prepare(
        PreparerConfig(
            logger=logging.getLogger("copy-stage5-test"),
            raw_text_path=None,
            extras={"dataset_dir_override": str(stage5_dir)},
        )
    )
    CopyStage6Preparer().prepare(
        PreparerConfig(
            logger=logging.getLogger("copy-stage6-test"),
            raw_text_path=None,
            extras={"dataset_dir_override": str(stage6_dir)},
        )
    )

    with (stage5_dir / "meta.pkl").open("rb") as f:
        stage5_meta = pickle.load(f)
    with (stage6_dir / "meta.pkl").open("rb") as f:
        stage6_meta = pickle.load(f)

    assert stage5_meta["record_count"] > 0
    assert stage6_meta["record_count"] > 0
