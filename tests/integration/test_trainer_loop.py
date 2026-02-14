"""Integration tests for the training loop with minimal runtime budget."""

from __future__ import annotations

from pathlib import Path

import torch

from ml_playground.framework.configuration.models import (
    DataConfig,
    DeviceKind,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    MetadataConfig,
    TrainerConfig,
)
from ml_playground.framework.training.loop.runner import Trainer


def _make_trainer_config(
    tmp_path: Path,
    *,
    max_iters: int = 0,
    eval_only: bool = True,
    device: DeviceKind = "cpu",
) -> tuple[TrainerConfig, MetadataConfig]:
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
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=1),
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
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=eval_only,
            seed=42,
            device=device,
            dtype="float32",
            compile=False,
            tensorboard_enabled=False,
            ema_decay=0.0,
        ),
        hf_model=TrainerConfig.HFModelConfig(
            model_name="hf/model",
            gradient_checkpointing=False,
            block_size=128,
        ),
        peft=TrainerConfig.PeftConfig(enabled=False),
    )

    shared = MetadataConfig(
        experiment="integration_test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path / "data",
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )

    return cfg, shared


def _write_minimal_dataset(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "data"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    train_file = dataset_dir / "train.bin"
    val_file = dataset_dir / "val.bin"
    tokens_train = torch.randint(0, 256, (64,), dtype=torch.uint16)
    tokens_val = torch.randint(0, 256, (32,), dtype=torch.uint16)
    tokens_train.numpy().tofile(str(train_file))
    tokens_val.numpy().tofile(str(val_file))


def test_trainer_eval_only_minimal(tmp_path: Path) -> None:
    """Trainer should run a minimal eval-only loop."""
    cfg, shared = _make_trainer_config(tmp_path, max_iters=0, eval_only=True)
    _write_minimal_dataset(tmp_path)

    trainer = Trainer(cfg, shared)
    final_iter, best_loss = trainer.run()

    assert final_iter == 0
    assert best_loss > 0.0
