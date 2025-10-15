"""Tests for the Connect Four synthetic dataset preparer."""

from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Iterable

import pytest

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError
from ml_playground.experiments.connect_four.preparer import (
    ConnectFourPreparer,
    generate_connect_four_sequences,
)


def _load_meta(ds_dir: Path) -> dict:
    with (ds_dir / "meta.pkl").open("rb") as fh:
        return pickle.load(fh)


def test_connect_four_preparer_writes_expected_artifacts(tmp_path: Path) -> None:
    base_dir = tmp_path / "connect_four"
    cfg = PreparerConfig(
        tokenizer_type="char",
        extras={"base_dir": str(base_dir), "num_games": 8, "seed": 7},
        logger=logging.getLogger(__name__),
    )

    preparer = ConnectFourPreparer()
    report = preparer.prepare(cfg)

    ds_dir = base_dir / "datasets"
    train_path = ds_dir / "train.bin"
    val_path = ds_dir / "val.bin"
    meta_path = ds_dir / "meta.pkl"

    assert train_path.exists()
    assert val_path.exists()
    assert meta_path.exists()

    assert train_path.stat().st_size > 0
    assert val_path.stat().st_size > 0

    meta = _load_meta(ds_dir)
    assert meta["meta_version"] == 1
    assert meta["tokenizer_type"] == "char"
    assert meta["alphabet"]
    assert meta["examples"] >= 8
    assert meta["tokens_per_example"] == 44

    assert report.created_files or report.updated_files
    assert "connect_four" in "\n".join(report.messages)


def test_generate_connect_four_sequences_requires_positive_games() -> None:
    with pytest.raises(DataError, match="must be positive"):
        generate_connect_four_sequences(num_games=0)


def test_connect_four_preparer_validates_split(tmp_path: Path) -> None:
    base_dir = tmp_path / "connect_four"
    cfg = PreparerConfig(
        tokenizer_type="char",
        extras={"base_dir": str(base_dir), "train_split": 1.5},
        logger=logging.getLogger(__name__),
    )

    preparer = ConnectFourPreparer()

    with pytest.raises(DataError, match="between 0 and 1"):
        preparer.prepare(cfg)


def test_connect_four_preparer_accepts_custom_generator(tmp_path: Path) -> None:
    base_dir = tmp_path / "connect_four"

    def generator(*, num_games: int, rng: object | None = None) -> Iterable[str]:
        assert num_games == 3
        yield "." * 42 + "1\n"

    cfg = PreparerConfig(
        tokenizer_type="char",
        extras={
            "base_dir": str(base_dir),
            "num_games": 3,
            "game_generator": generator,
        },
        logger=logging.getLogger(__name__),
    )

    preparer = ConnectFourPreparer()
    preparer.prepare(cfg)

    meta = _load_meta(base_dir / "datasets")
    assert meta["examples"] == 1
    assert set(meta["alphabet"]) == {".", "1", "\n"}


def test_connect_four_preparer_respects_dataset_dir(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "explicit_ds"
    cfg = PreparerConfig(
        tokenizer_type="char",
        extras={
            "dataset_dir": str(dataset_dir),
            "num_games": 4,
            "seed": 11,
        },
        logger=logging.getLogger(__name__),
    )

    preparer = ConnectFourPreparer()
    preparer.prepare(cfg)

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()
