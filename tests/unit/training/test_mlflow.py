from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import torch
from torch.amp.grad_scaler import GradScaler

from ml_playground.configuration.models import (
    TrainerConfig,
    SharedConfig,
    ModelConfig,
)
from ml_playground.models.core.model import GPT
from ml_playground.training.loop.runner import Trainer, TrainerDependencies


class MockMLflowManager:
    def __init__(self, *args, **kwargs):
        self.setup_called = False
        self.log_config_called = False
        self.finish_called = False
        self.metrics = []

    def setup(self):
        self.setup_called = True

    def log_config(self, cfg):
        self.log_config_called = True

    def log_metrics(self, metrics, step):
        self.metrics.append((metrics, step))

    def finish(self):
        self.finish_called = True


def test_trainer_initialization_with_mlflow(tmp_path: Path):
    model_cfg = {
        "n_layer": 1,
        "n_head": 1,
        "n_embd": 32,
        "block_size": 16,
        "vocab_size": 100,
    }
    data_cfg = {
        "train_bin": "train.bin",
        "val_bin": "val.bin",
        "meta_pkl": "meta.pkl",
        "batch_size": 1,
        "block_size": 16,
        "grad_accum_steps": 1,
    }
    optim_cfg = {"learning_rate": 6e-4}
    sched_cfg = {"decay_lr": False, "warmup_iters": 0}
    runtime_cfg = {"out_dir": tmp_path, "max_iters": 1, "mlflow_enabled": True}

    cfg_dict = {
        "model": model_cfg,
        "data": data_cfg,
        "optim": optim_cfg,
        "schedule": sched_cfg,
        "runtime": runtime_cfg,
    }
    cfg = TrainerConfig.model_validate(cfg_dict)
    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )

    def mock_init_batches(c, s):
        class MockBatches:
            def get_batch(self, split):
                return torch.zeros((1, 16), dtype=torch.long), torch.zeros(
                    (1, 16), dtype=torch.long
                )

        return MockBatches()

    def mock_init_model(c, logger):
        model_config = ModelConfig(
            n_layer=1, n_head=1, n_embd=32, block_size=16, vocab_size=100
        )
        model = GPT(model_config, logger=logging.getLogger("test"))
        opt = torch.optim.AdamW(model.parameters())
        return model, opt

    def mock_init_components(m, c, r, ld):
        return m, GradScaler(enabled=False), None

    mock_mlflow = MockMLflowManager()

    deps = TrainerDependencies(
        initialize_batches=mock_init_batches,
        initialize_model=mock_init_model,
        initialize_components=mock_init_components,
        create_manager=lambda c, s: None,
        create_mlflow_manager=lambda r, s, logger: cast(Any, mock_mlflow),
        load_checkpoint=lambda *args, **kwargs: None,
        apply_checkpoint=lambda *args, **kwargs: (0, 1e9),
        save_checkpoint=lambda *args, **kwargs: None,
        propagate_metadata=lambda *args, **kwargs: None,
        run_evaluation=lambda *args, **kwargs: {"train": 0.1, "val": 0.1},
        get_lr=lambda *args, **kwargs: 0.001,
    )

    trainer = Trainer(cfg, shared, deps=deps)
    assert mock_mlflow.setup_called
    assert mock_mlflow.log_config_called

    trainer.run()
    assert mock_mlflow.finish_called
