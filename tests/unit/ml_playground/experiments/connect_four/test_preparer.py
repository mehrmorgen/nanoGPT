from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pytest

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError
from ml_playground.experiments.connect_four.preparer import ConnectFourPreparer


def _make_config(tmp_path: Path, **extras) -> PreparerConfig:
    cfg = PreparerConfig()
    merged = {"base_dir": tmp_path, **extras}
    cfg.extras.update(merged)
    return cfg


def test_connect_four_preparer_writes_artifacts(tmp_path: Path) -> None:
    exp_dir = tmp_path / "experiment"
    exp_dir.mkdir()
    cfg = _make_config(exp_dir, num_games=12, train_fraction=0.75, seed=7)

    report = ConnectFourPreparer().prepare(cfg)

    ds_dir = exp_dir / "datasets"
    train_bin = ds_dir / "train.bin"
    val_bin = ds_dir / "val.bin"
    meta_file = ds_dir / "meta.pkl"

    assert train_bin.exists()
    assert val_bin.exists()
    assert meta_file.exists()
    assert report.created_files or report.updated_files

    train_tokens = np.fromfile(train_bin, dtype=np.uint16)
    val_tokens = np.fromfile(val_bin, dtype=np.uint16)

    assert train_tokens.size > 0
    assert val_tokens.size > 0
    assert train_tokens.size > val_tokens.size

    with meta_file.open("rb") as fh:
        meta = pickle.load(fh)

    assert meta["tokenizer_type"] == "char"
    assert meta["board_rows"] == 6
    assert meta["board_columns"] == 7
    assert meta["tokens_per_position"] == 45
    assert meta["examples_train"] > 0
    assert meta["examples_val"] > 0


def test_connect_four_preparer_validates_fraction(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path, num_games=8, train_fraction=1.0)

    with pytest.raises(DataError):
        ConnectFourPreparer().prepare(cfg)


def test_connect_four_preparer_rejects_invalid_num_games(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path, num_games=0)

    with pytest.raises(DataError):
        ConnectFourPreparer().prepare(cfg)
