from __future__ import annotations

from pathlib import Path

import torch

from ml_playground.configuration.models import (
    DataConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    RuntimeConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.training.lightning import (
    LightningBatchDataModule,
    LightningDataDependencies,
    LightningGPTModule,
    run_lightning_training,
)


def _make_cfg(tmp_path: Path) -> tuple[TrainerConfig, SharedConfig]:
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    cfg = TrainerConfig(
        model=ModelConfig(
            n_layer=1,
            n_head=1,
            n_embd=8,
            block_size=4,
            dropout=0.0,
            vocab_size=32,
        ),
        data=DataConfig(batch_size=2, block_size=4, grad_accum_steps=1),
        optim=OptimConfig(
            learning_rate=1e-3,
            weight_decay=0.0,
            beta1=0.9,
            beta2=0.95,
            grad_clip=0.0,
        ),
        schedule=LRSchedule(
            decay_lr=False,
            warmup_iters=0,
            lr_decay_iters=1,
            min_lr=1e-3,
        ),
        runtime=RuntimeConfig(
            out_dir=out_dir,
            max_iters=2,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            device="cpu",
            dtype="float32",
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
    shared = SharedConfig(
        experiment="unit",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=data_dir,
        train_out_dir=out_dir,
        sample_out_dir=out_dir,
    )
    return cfg, shared


class _FakeBatches:
    def __init__(self, cfg: TrainerConfig) -> None:
        self.cfg = cfg
        self.requests: list[str] = []

    def get_batch(self, split: str) -> tuple[torch.Tensor, torch.Tensor]:
        self.requests.append(split)
        shape = (self.cfg.data.batch_size, self.cfg.data.block_size)
        vocab = self.cfg.model.vocab_size or 32
        x = torch.randint(low=0, high=vocab, size=shape, dtype=torch.long)
        y = torch.randint(low=0, high=vocab, size=shape, dtype=torch.long)
        return x, y


def test_lightning_module_training_step_computes_loss(tmp_path):
    cfg, _ = _make_cfg(tmp_path)
    module = LightningGPTModule(cfg)

    x = torch.randint(0, cfg.model.vocab_size or 32, (2, 4), dtype=torch.long)
    y = torch.randint(0, cfg.model.vocab_size or 32, (2, 4), dtype=torch.long)
    _, loss = module(x, y)
    assert loss is not None
    assert loss.requires_grad


def test_lightning_data_module_yields_batches(tmp_path):
    cfg, shared = _make_cfg(tmp_path)
    fake_batches = _FakeBatches(cfg)
    deps = LightningDataDependencies(
        initialize_batches=lambda _cfg, _shared: fake_batches
    )

    data_module = LightningBatchDataModule(cfg, shared, deps=deps)
    data_module.setup(stage=None)

    batch = next(iter(data_module.train_dataloader()))
    assert fake_batches.requests[0] == "train"
    assert batch[0].shape == (cfg.data.batch_size, cfg.data.block_size)


def test_run_lightning_training_executes(tmp_path):
    cfg, shared = _make_cfg(tmp_path)
    fake_batches = _FakeBatches(cfg)
    deps = LightningDataDependencies(
        initialize_batches=lambda _cfg, _shared: fake_batches
    )

    step, best_val = run_lightning_training(
        cfg,
        shared,
        data_deps=deps,
        trainer_kwargs={"limit_train_batches": cfg.runtime.max_iters},
    )

    assert step == cfg.runtime.max_iters
    assert isinstance(best_val, float)
    assert "val" in fake_batches.requests
