from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path

import pytest

from ml_playground.configuration.models import PreparerConfig
from ml_playground.core.error_handling import DataError
from ml_playground.experiments.connect_four.preparer import ConnectFourPreparer


@pytest.fixture()
def experiment_dir(tmp_path: Path) -> Path:
    exp_dir = tmp_path / "connect_four"
    exp_dir.mkdir()
    return exp_dir


@pytest.fixture()
def datasets_dir(experiment_dir: Path) -> Path:
    ds_dir = experiment_dir / "datasets"
    ds_dir.mkdir()
    return ds_dir


@contextmanager
def _patched_module_file(experiment_dir: Path):
    import ml_playground.experiments.connect_four.preparer as preparer_module

    original = preparer_module.__file__
    preparer_module.__file__ = str(experiment_dir / "preparer.py")
    try:
        yield
    finally:
        preparer_module.__file__ = original


def test_connect_four_preparer_creates_dataset(experiment_dir: Path, datasets_dir: Path) -> None:
    """The preparer should write train/val/meta artifacts in the dataset directory."""
    preparer = ConnectFourPreparer()
    cfg = PreparerConfig(logger=logging.getLogger(__name__))

    with _patched_module_file(experiment_dir):
        report = preparer.prepare(cfg)

    train_bin = datasets_dir / "train.bin"
    val_bin = datasets_dir / "val.bin"
    meta_pkl = datasets_dir / "meta.pkl"

    assert train_bin.exists()
    assert val_bin.exists()
    assert meta_pkl.exists()

    assert train_bin.stat().st_size > 0
    assert val_bin.stat().st_size > 0

    assert any("connect_four" in message for message in report.messages)
    # The preparer should report at least one created file.
    assert report.created_files


def test_connect_four_preparer_respects_dataset_override(experiment_dir: Path) -> None:
    """Passing dataset_dir_override should redirect outputs."""
    override_dir = experiment_dir / "override"
    override_dir.mkdir()
    override_ds = override_dir / "datasets"

    preparer = ConnectFourPreparer()
    cfg = PreparerConfig(
        logger=logging.getLogger(__name__),
        extras={"dataset_dir_override": str(override_ds)},
    )

    with _patched_module_file(experiment_dir):
        preparer.prepare(cfg)

    assert (override_ds / "train.bin").exists()
    assert (override_ds / "val.bin").exists()
    assert (override_ds / "meta.pkl").exists()


def test_connect_four_preparer_raises_on_invalid_split(experiment_dir: Path, datasets_dir: Path) -> None:
    """An invalid train/val split ratio should surface a DataError."""
    preparer = ConnectFourPreparer()
    cfg = PreparerConfig(
        logger=logging.getLogger(__name__),
        extras={"train_val_split": 1.25},
    )

    with _patched_module_file(experiment_dir):
        with pytest.raises(DataError, match="train_val_split"):
            preparer.prepare(cfg)


def test_connect_four_preparer_raises_when_no_sequences(experiment_dir: Path, datasets_dir: Path) -> None:
    """If simulation yields no sequences, the preparer should fail fast."""
    preparer = ConnectFourPreparer()
    cfg = PreparerConfig(
        logger=logging.getLogger(__name__),
        extras={"num_games": 0},
    )

    with _patched_module_file(experiment_dir):
        with pytest.raises(DataError, match="no games"):
            preparer.prepare(cfg)
