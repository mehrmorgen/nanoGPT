from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings, strategies as st

from ml_playground.training.checkpointing.checkpoint_manager import (
    Checkpoint,
    CheckpointError,
    CheckpointManager,
)


def _checkpoint(iter_num: int) -> Checkpoint:
    return Checkpoint(
        model={},
        optimizer={},
        model_args={},
        iter_num=iter_num,
        best_val_loss=1.0,
        config={},
        ema=None,
    )


@settings(max_examples=30, deadline=50, derandomize=True)
@given(iter_num=st.integers(min_value=0, max_value=1_000_000))
def test_checkpoint_naming_steps_uses_counter(iter_num: int) -> None:
    """Step naming uses the counter in filenames."""
    with TemporaryDirectory() as tmp_dir:
        manager = CheckpointManager(
            out_dir=Path(tmp_dir),
            naming_policy="steps",
        )
        path = manager.save_checkpoint(
            _checkpoint(iter_num),
            base_filename="ckpt_last.pt",
            metric=1.0,
            iter_num=iter_num,
            logger=logging.getLogger("test"),
            is_best=False,
        )
        assert path.name == f"ckpt_last_{iter_num:08d}.pt"


@settings(max_examples=30, deadline=50, derandomize=True)
@given(counter_value=st.integers(min_value=0, max_value=1_000_000))
def test_checkpoint_naming_domain_uses_label(counter_value: int) -> None:
    """Domain naming includes the counter label."""
    with TemporaryDirectory() as tmp_dir:
        manager = CheckpointManager(
            out_dir=Path(tmp_dir),
            naming_policy="domain",
            counter_label="games",
        )
        path = manager.save_checkpoint(
            _checkpoint(counter_value),
            base_filename="ckpt_last.pt",
            metric=1.0,
            iter_num=counter_value,
            counter_value=counter_value,
            logger=logging.getLogger("test"),
            is_best=False,
        )
        assert path.name == f"ckpt_last_games_{counter_value:08d}.pt"


def test_checkpoint_naming_strict_rejects_legacy_files() -> None:
    """Strict naming rejects legacy checkpoint filenames for domain counters."""
    with TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir)
        (out_dir / "ckpt_last_00000001.pt").write_bytes(b"test")
        with pytest.raises(CheckpointError):
            CheckpointManager(
                out_dir=out_dir,
                naming_policy="domain",
                counter_label="games",
                strict_naming=True,
            )


def test_checkpoint_naming_requires_label_for_domain_policy() -> None:
    """Domain naming requires an explicit counter label."""
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(CheckpointError):
            CheckpointManager(
                out_dir=Path(tmp_dir),
                naming_policy="domain",
                counter_label=None,
            )


def test_checkpoint_naming_strict_rejects_legacy_best_files() -> None:
    """Strict naming rejects legacy best-checkpoint filenames."""
    with TemporaryDirectory() as tmp_dir:
        out_dir = Path(tmp_dir)
        (out_dir / "ckpt_best_00000001_1.000000.pt").write_bytes(b"test")
        with pytest.raises(CheckpointError):
            CheckpointManager(
                out_dir=out_dir,
                naming_policy="domain",
                counter_label="games",
                strict_naming=True,
            )
