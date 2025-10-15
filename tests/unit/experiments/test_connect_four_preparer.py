from __future__ import annotations

import logging
import pickle
from pathlib import Path

import numpy as np

from ml_playground.configuration.models import PreparerConfig
from ml_playground.experiments.connect_four.preparer import ConnectFourPreparer


def _prep_cfg(tmp_path: Path, **extras: object) -> PreparerConfig:
    data_dir = tmp_path / "connect_four"
    return PreparerConfig(
        extras={
            "dataset_dir_override": data_dir,
            **extras,
        },
        logger=logging.getLogger(__name__),
    )


def test_connect_four_preparer_generates_sequences(tmp_path: Path) -> None:
    cfg = _prep_cfg(tmp_path, force_rebuild=True, num_games=32, val_split=0.25, seed=7)

    report = ConnectFourPreparer().prepare(cfg)

    ds_dir = tmp_path / "connect_four" / "datasets"
    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"

    assert train_path.exists()
    assert val_path.exists()
    assert meta_path.exists()

    # Train/val tokens should be multiples of the encoded sequence length (44)
    train_tokens = np.frombuffer(train_path.read_bytes(), dtype=np.uint16)
    val_tokens = np.frombuffer(val_path.read_bytes(), dtype=np.uint16)
    assert train_tokens.size % 44 == 0
    assert val_tokens.size % 44 == 0

    with meta_path.open("rb") as handle:
        meta = pickle.load(handle)

    assert meta["meta_version"] == 1
    assert meta["tokenizer_type"] == "connect_four"
    assert meta["board_rows"] == 6
    assert meta["board_cols"] == 7
    assert train_tokens.size + val_tokens.size == meta["train_tokens"] + meta["val_tokens"]

    assert set(report.created_files) == {train_path, val_path, meta_path}
    assert not report.updated_files
    assert not report.skipped_files


def test_connect_four_preparer_skips_when_artifacts_exist(tmp_path: Path) -> None:
    cfg = _prep_cfg(tmp_path, force_rebuild=True, num_games=4, val_split=0.25)

    preparer = ConnectFourPreparer()
    preparer.prepare(cfg)

    # Second run without force_rebuild should skip writing outputs
    cfg_no_force = _prep_cfg(tmp_path, num_games=4, val_split=0.25)
    report = preparer.prepare(cfg_no_force)

    ds_dir = tmp_path / "connect_four" / "datasets"
    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"

    assert not report.created_files
    assert not report.updated_files
    assert set(report.skipped_files) == {train_path, val_path, meta_path}
