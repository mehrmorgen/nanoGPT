"""Integration tests for the training loop with real components."""

from __future__ import annotations

import warnings
from pathlib import Path

import torch

from typing import cast

from ml_playground.configuration.models import (
    DataConfig,
    DeviceKind,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.training.loop.runner import Trainer


def _make_trainer_config(
    tmp_path: Path,
    max_iters: int = 2,
    eval_interval: int = 1,
    device: DeviceKind = cast(DeviceKind, "cpu"),
    eval_only: bool = False,
    ema_decay: float = 0.0,
    grad_accum_steps: int = 1,
) -> tuple[TrainerConfig, SharedConfig]:
    """Create minimal trainer config and shared config for testing."""
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
            eval_only=eval_only,
            seed=42,
            device=device,
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
    )

    shared = SharedConfig(
        experiment="integration_test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    return cfg, shared


def test_trainer_runs_full_loop(tmp_path: Path) -> None:
    """Trainer should execute a complete training loop with real components."""
    # Create config
    cfg, shared = _make_trainer_config(tmp_path, max_iters=2, eval_interval=1)

    # Create dataset directory structure
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Create train and val data files
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    tokens_train = torch.randint(0, 256, (256,), dtype=torch.uint16)
    tokens_val = torch.randint(0, 256, (128,), dtype=torch.uint16)
    tokens_train.numpy().tofile(str(train_file))
    tokens_val.numpy().tofile(str(val_file))

    # Create trainer with real dependencies
    trainer = Trainer(cfg, shared)

    # Run training
    final_iter, best_loss = trainer.run()

    # Verify training completed
    assert final_iter > 0
    assert best_loss > 0.0
    assert best_loss < 1e9  # Sanity check


def test_trainer_respects_max_iters(tmp_path: Path) -> None:
    """Trainer should stop at max_iters."""
    cfg, shared = _make_trainer_config(tmp_path, max_iters=3, eval_interval=2)

    # Create dataset
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    torch.randint(0, 256, (256,), dtype=torch.uint16).numpy().tofile(str(train_file))
    torch.randint(0, 256, (128,), dtype=torch.uint16).numpy().tofile(str(val_file))

    trainer = Trainer(cfg, shared)
    final_iter, _ = trainer.run()

    # Should stop after max_iters (3) + 1 for the final eval
    assert final_iter == 4


def test_trainer_eval_only_mode(tmp_path: Path) -> None:
    """Trainer should exit early in eval_only mode."""
    cfg, shared = _make_trainer_config(
        tmp_path, max_iters=10, eval_interval=1, eval_only=True
    )

    # Create dataset
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    torch.randint(0, 256, (256,), dtype=torch.uint16).numpy().tofile(str(train_file))
    torch.randint(0, 256, (128,), dtype=torch.uint16).numpy().tofile(str(val_file))

    trainer = Trainer(cfg, shared)
    final_iter, _ = trainer.run()

    # Should exit after first eval (iter 0, then increment to 1)
    assert final_iter == 0


def test_trainer_with_ema(tmp_path: Path) -> None:
    """Trainer should work with EMA enabled."""
    cfg, shared = _make_trainer_config(tmp_path, max_iters=2, ema_decay=0.999)

    # Create dataset
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    torch.randint(0, 256, (256,), dtype=torch.uint16).numpy().tofile(str(train_file))
    torch.randint(0, 256, (128,), dtype=torch.uint16).numpy().tofile(str(val_file))

    trainer = Trainer(cfg, shared)

    # Should not raise
    final_iter, best_loss = trainer.run()
    assert final_iter > 0
    assert best_loss > 0.0
    assert trainer.ema is not None


def test_trainer_with_grad_accumulation(tmp_path: Path) -> None:
    """Trainer should handle gradient accumulation."""
    cfg, shared = _make_trainer_config(tmp_path, max_iters=2, grad_accum_steps=2)

    # Create dataset
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    torch.randint(0, 256, (256,), dtype=torch.uint16).numpy().tofile(str(train_file))
    torch.randint(0, 256, (128,), dtype=torch.uint16).numpy().tofile(str(val_file))

    trainer = Trainer(cfg, shared)

    # Suppress PyTorch performance warning about batching rule for CPU
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        final_iter, best_loss = trainer.run()

    assert final_iter > 0
    assert best_loss > 0.0
