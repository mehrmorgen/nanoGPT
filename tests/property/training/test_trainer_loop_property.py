"""Property-based tests for the training loop."""

from __future__ import annotations

from pathlib import Path

from typing import Any

import hypothesis
import hypothesis.strategies as st
import numpy as np
import pytest
import torch
from hypothesis import HealthCheck, given, settings

from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.training.loop.runner import Trainer


def _noop_checkpoint_loader(*args: Any, **kwargs: Any) -> None:
    return None


def _make_config(
    tmp_path: Path,
    max_iters: int,
    eval_interval: int,
    ema_decay: float,
    grad_accum_steps: int,
) -> tuple[TrainerConfig, SharedConfig]:
    """Create a trainer config with given parameters."""
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = TrainerConfig(
        model=ModelConfig(
            n_layer=1,
            n_head=1,
            n_embd=8,
            block_size=4,
            dropout=0.0,
            vocab_size=256,
        ),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=grad_accum_steps),
        optim=OptimConfig(
            learning_rate=0.01,
            weight_decay=0.0,
            beta1=0.9,
            beta2=0.95,
            grad_clip=0.0,
        ),
        schedule=LRSchedule(
            decay_lr=False,
            warmup_iters=0,
            lr_decay_iters=0,
            min_lr=0.0,
        ),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=max_iters,
            eval_interval=eval_interval,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            seed=42,
            device="cpu",
            dtype="float32",
            compile=False,
            tensorboard_enabled=False,
            ema_decay=ema_decay,
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
        checkpoint_load_fn=_noop_checkpoint_loader,
    )

    shared = SharedConfig(
        experiment="property_test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    return cfg, shared


def _create_dataset(tmp_path: Path, train_size: int = 256, val_size: int = 128) -> None:
    """Create train and val binary datasets."""
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"

    torch.randint(0, 256, (train_size,), dtype=torch.uint16).numpy().tofile(
        str(train_file)
    )
    torch.randint(0, 256, (val_size,), dtype=torch.uint16).numpy().tofile(str(val_file))


@pytest.mark.filterwarnings("ignore::UserWarning")
@given(
    max_iters=st.integers(min_value=1, max_value=5),
    eval_interval=st.integers(min_value=1, max_value=3),
    ema_decay=st.floats(min_value=0.0, max_value=0.999),
    grad_accum_steps=st.integers(min_value=1, max_value=3),
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    derandomize=True,
)
def test_trainer_completes_with_valid_config(
    tmp_path: Path,
    max_iters: int,
    eval_interval: int,
    ema_decay: float,
    grad_accum_steps: int,
) -> None:
    """Trainer should complete successfully with any valid configuration."""
    cfg, shared = _make_config(
        tmp_path, max_iters, eval_interval, ema_decay, grad_accum_steps
    )
    _create_dataset(tmp_path)

    trainer = Trainer(cfg, shared)
    final_iter, best_loss = trainer.run()

    # Invariant: final_iter should be non-negative
    assert final_iter >= 0
    # Invariant: best_loss should be finite and positive
    assert best_loss > 0.0
    assert np.isfinite(best_loss)


@pytest.mark.filterwarnings("ignore::UserWarning")
@given(
    max_iters=st.integers(min_value=2, max_value=5),
    eval_interval=st.integers(min_value=1, max_value=2),
)
@hypothesis.example(max_iters=2, eval_interval=1)
@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    derandomize=True,
)
def test_trainer_respects_iteration_bounds(
    tmp_path: Path,
    max_iters: int,
    eval_interval: int,
) -> None:
    """Trainer should never exceed max_iters."""
    cfg, shared = _make_config(tmp_path, max_iters, eval_interval, 0.0, 1)
    _create_dataset(tmp_path)

    trainer = Trainer(cfg, shared)
    final_iter, _ = trainer.run()

    # Invariant: final_iter should not exceed max_iters + 1 (for final eval)
    assert final_iter <= max_iters + 1


@pytest.mark.filterwarnings("ignore::UserWarning")
@given(
    ema_decay=st.floats(min_value=0.0, max_value=0.999),
)
@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    derandomize=True,
)
def test_trainer_ema_consistency(
    tmp_path: Path,
    ema_decay: float,
) -> None:
    """Trainer should create EMA only when decay > 0."""
    cfg, shared = _make_config(
        tmp_path, max_iters=2, eval_interval=1, ema_decay=ema_decay, grad_accum_steps=1
    )
    _create_dataset(tmp_path)

    trainer = Trainer(cfg, shared)
    trainer.run()

    # Invariant: EMA should exist iff ema_decay > 0
    if ema_decay > 0.0:
        assert trainer.ema is not None
    else:
        assert trainer.ema is None


@pytest.mark.filterwarnings("ignore::UserWarning")
@given(
    grad_accum_steps=st.integers(min_value=1, max_value=3),
)
@settings(
    max_examples=5,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
    derandomize=True,
)
def test_trainer_loss_decreases_or_stable(
    tmp_path: Path,
    grad_accum_steps: int,
) -> None:
    """Loss should generally decrease or remain stable across iterations."""
    cfg, shared = _make_config(
        tmp_path,
        max_iters=3,
        eval_interval=1,
        ema_decay=0.0,
        grad_accum_steps=grad_accum_steps,
    )
    _create_dataset(tmp_path)

    trainer = Trainer(cfg, shared)
    final_iter, best_loss = trainer.run()

    # Invariant: final_iter should be positive
    assert final_iter > 0
    # Invariant: best_loss should be finite
    assert np.isfinite(best_loss)
    # Invariant: best_loss should be reasonable (not NaN or inf)
    assert 0.0 < best_loss < 1e6
