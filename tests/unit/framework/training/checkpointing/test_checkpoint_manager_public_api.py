"""Unit tests for checkpoint_manager.py using only public APIs."""

from __future__ import annotations

from pathlib import Path

import pytest
import torch

from ml_playground.framework.core.error_handling import CheckpointError
from ml_playground.framework.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointManager,
    resolve_posix_path_cls,
)

from tests.unit.framework.training._helpers import LoggerStub


def test_checkpoint_manager_basic_functionality(tmp_path: Path) -> None:
    """Test basic CheckpointManager functionality without mocking."""
    manager = CheckpointManager(out_dir=tmp_path)

    checkpoint = Checkpoint(
        model={"w": [1.0]},
        optimizer={"state": {}},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
    )

    # Save checkpoint as both last and best
    path_last = manager.save_checkpoint(
        checkpoint=checkpoint,
        metric=0.5,
        iter_num=100,
        logger=LoggerStub(),
        is_best=False,
    )

    path_best = manager.save_checkpoint(
        checkpoint=checkpoint,
        metric=0.5,
        iter_num=100,
        logger=LoggerStub(),
        is_best=True,
    )

    assert path_last.exists()
    assert path_last.name == "ckpt_last_00000100.pt"
    assert path_best.exists()
    assert path_best.name == "ckpt_best_00000100_0.500000.pt"

    # Load latest checkpoint
    loaded = manager.load_latest_checkpoint("cpu", LoggerStub())
    assert loaded.iter_num == 100
    assert loaded.best_val_loss == 0.5

    # Load best checkpoint
    best_loaded = manager.load_best_checkpoint("cpu", LoggerStub())
    assert best_loaded.iter_num == 100
    assert best_loaded.best_val_loss == 0.5


def test_checkpoint_manager_retention_policy(tmp_path: Path) -> None:
    """Test retention policy without mocking."""
    manager = CheckpointManager(
        out_dir=tmp_path,
        keep_last=2,
        keep_best=2,
    )

    checkpoint = Checkpoint(
        model={},
        optimizer={},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
    )

    # Save multiple checkpoints
    for i in range(3):
        checkpoint.iter_num = i * 10
        manager.save_checkpoint(
            checkpoint=checkpoint,
            metric=0.5 - i * 0.1,
            iter_num=i * 10,
            logger=LoggerStub(),
            is_best=True,
        )

    # Should only have 2 best checkpoints
    best_files = list(tmp_path.glob("ckpt_best_*.pt"))
    assert len(best_files) == 2


def test_checkpoint_manager_domain_naming(tmp_path: Path) -> None:
    """Test domain naming policy."""
    manager = CheckpointManager(
        out_dir=tmp_path,
        naming_policy="domain",
        counter_label="steps",
    )

    checkpoint = Checkpoint(
        model={},
        optimizer={},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
    )

    path = manager.save_checkpoint(
        checkpoint=checkpoint,
        metric=0.5,
        iter_num=100,
        logger=LoggerStub(),
        is_best=False,
    )

    assert path.name == "ckpt_last_steps_00000100.pt"


def test_checkpoint_manager_non_atomic_save(tmp_path: Path) -> None:
    """Test non-atomic save."""
    manager = CheckpointManager(
        out_dir=tmp_path,
        atomic=False,
    )

    checkpoint = Checkpoint(
        model={},
        optimizer={},
        model_args={},
        iter_num=100,
        best_val_loss=0.5,
        config={},
    )

    path = manager.save_checkpoint(
        checkpoint=checkpoint,
        metric=0.5,
        iter_num=100,
        logger=LoggerStub(),
        is_best=False,
    )

    assert path.exists()


def test_checkpoint_to_dict() -> None:
    """Test Checkpoint.to_dict method."""
    checkpoint = Checkpoint(
        model={"w1": torch.tensor([1.0])},
        optimizer={"state": {}},
        model_args={"n_layer": 2},
        iter_num=100,
        best_val_loss=0.5,
        config={"lr": 0.001},
        ema={"shadow": {}},
    )

    result = checkpoint.to_dict()

    assert result.get("model") == {"w1": torch.tensor([1.0])}
    assert result.get("optimizer") == {"state": {}}
    assert result.get("model_args") == {"n_layer": 2}
    assert result.get("iter_num") == 100
    assert result.get("best_val_loss") == 0.5
    assert result.get("config") == {"lr": 0.001}
    assert result.get("ema") == {"shadow": {}}


def test_checkpoint_from_payload() -> None:
    """Test Checkpoint.from_payload method."""
    payload = {
        "model": {"w": [1.0]},
        "optimizer": {"state": {}},
        "model_args": {"n_layer": 2},
        "iter_num": 100,
        "best_val_loss": 0.5,
        "config": {"lr": 0.001},
    }

    checkpoint = Checkpoint.from_payload(payload)

    assert checkpoint.model == {"w": [1.0]}
    assert checkpoint.optimizer == {"state": {}}
    assert checkpoint.model_args == {"n_layer": 2}
    assert checkpoint.iter_num == 100
    assert checkpoint.best_val_loss == 0.5
    assert checkpoint.config == {"lr": 0.001}
    assert checkpoint.ema is None


def test_public_api_functions() -> None:
    """Test public API functions."""
    # Test resolve_posix_path_cls
    result = resolve_posix_path_cls()
    # Result could be None or a class depending on the Python version
    assert result is None or isinstance(result, type)


def test_checkpoint_manager_error_handling(tmp_path: Path) -> None:
    """Test error handling."""
    manager = CheckpointManager(out_dir=tmp_path)

    # Try to load when no checkpoints exist
    with pytest.raises(CheckpointError, match="No last checkpoints discovered"):
        manager.load_latest_checkpoint("cpu", LoggerStub())

    # Try to load best when no checkpoints exist
    with pytest.raises(CheckpointError, match="No best checkpoints discovered"):
        manager.load_best_checkpoint("cpu", LoggerStub())


def test_checkpoint_manager_invalid_retention(tmp_path: Path) -> None:
    """Test invalid retention policy."""
    with pytest.raises(CheckpointError, match="Invalid checkpoint keep policy"):
        CheckpointManager(out_dir=tmp_path, keep_last=-1)

    with pytest.raises(CheckpointError, match="Invalid checkpoint keep policy"):
        CheckpointManager(out_dir=tmp_path, keep_best=-1)


def test_checkpoint_manager_domain_naming_requires_label(tmp_path: Path) -> None:
    """Test that domain naming requires counter_label."""
    with pytest.raises(CheckpointError, match="counter_label is required"):
        CheckpointManager(out_dir=tmp_path, naming_policy="domain")
