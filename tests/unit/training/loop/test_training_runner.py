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


class MockLogger:
    def __init__(self):
        self.infos = []
        self.errors = []
        self.warnings = []

    def info(self, msg):
        self.infos.append(msg)

    def error(self, msg):
        self.errors.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)


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


def test_trainer_run_basic(tmp_path: Path):
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
    runtime_cfg = {"out_dir": tmp_path, "max_iters": 1}

    cfg = TrainerConfig.model_validate(
        {
            "model": model_cfg,
            "data": data_cfg,
            "optim": optim_cfg,
            "schedule": sched_cfg,
            "runtime": runtime_cfg,
        }
    )
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
        return model, torch.optim.AdamW(model.parameters())

    deps = TrainerDependencies(
        initialize_batches=mock_init_batches,
        initialize_model=mock_init_model,
        initialize_components=lambda m, c, r, ld: (m, GradScaler(enabled=False), None),
        create_manager=lambda c, s: None,
        create_mlflow_manager=lambda r, s, logger: cast(Any, MockMLflowManager()),
        load_checkpoint=lambda *args, **kwargs: None,
        apply_checkpoint=lambda *args, **kwargs: (0, 1e9),
        save_checkpoint=lambda *args, **kwargs: None,
        propagate_metadata=lambda *args, **kwargs: None,
        run_evaluation=lambda *args, **kwargs: {"train": 0.1, "val": 0.1},
        get_lr=lambda *args, **kwargs: 0.001,
    )

    trainer = Trainer(cfg, shared, deps=deps)
    iter_num, best_loss = trainer.run()
    assert iter_num == 1
    assert best_loss == 0.1


def test_train_shared_none_and_provided_branches(tmp_path: Path) -> None:
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
    cfg = TrainerConfig.model_validate(
        {
            "model": model_cfg,
            "data": data_cfg,
            "optim": {"learning_rate": 6e-4},
            "schedule": {"decay_lr": False, "warmup_iters": 0},
            "runtime": {"out_dir": tmp_path, "max_iters": 0, "eval_only": True},
        }
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
        return model, torch.optim.AdamW(model.parameters())

    deps = TrainerDependencies(
        initialize_batches=mock_init_batches,
        initialize_model=mock_init_model,
        initialize_components=lambda m, c, r, ld: (m, GradScaler(enabled=False), None),
        create_manager=lambda c, s: None,
        create_mlflow_manager=lambda r, s, logger: cast(Any, MockMLflowManager()),
        load_checkpoint=lambda *args, **kwargs: None,
        apply_checkpoint=lambda *args, **kwargs: (0, 1e9),
        save_checkpoint=lambda *args, **kwargs: None,
        propagate_metadata=lambda *args, **kwargs: None,
        run_evaluation=lambda *args, **kwargs: {"train": 0.1, "val": 0.1},
        get_lr=lambda *args, **kwargs: 0.001,
    )

    from ml_playground.training.loop.runner import train

    iter_num, _ = train(cfg, shared=None, deps=deps)
    assert iter_num == 0

    shared = SharedConfig(
        experiment="test",
        config_path=tmp_path / "cfg.toml",
        project_home=tmp_path,
        dataset_dir=tmp_path,
        train_out_dir=tmp_path,
        sample_out_dir=tmp_path,
    )
    iter_num2, _ = train(cfg, shared=shared, deps=deps)
    assert iter_num2 == 0


def test_trainer_run_saves_best_only_when_iter_positive_and_uses_custom_train_step(
    tmp_path: Path,
) -> None:
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
    cfg = TrainerConfig.model_validate(
        {
            "model": model_cfg,
            "data": data_cfg,
            "optim": {"learning_rate": 6e-4, "grad_clip": 0.0},
            "schedule": {"decay_lr": False, "warmup_iters": 0},
            "runtime": {
                "out_dir": tmp_path,
                "max_iters": 2,
                "eval_interval": 1,
                "log_interval": 1,
            },
        }
    )
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
        return model, torch.optim.AdamW(model.parameters())

    saved_best_iters: list[int] = []

    def save_checkpoint(*args: Any, **kwargs: Any) -> None:
        if kwargs.get("is_best") is True:
            saved_best_iters.append(int(kwargs.get("iter_num", -1)))

    call = {"n": 0}

    def run_evaluation(*args: Any, **kwargs: Any) -> dict[str, float]:
        # iter 0: does not trigger best-save because iter_num == 0
        # iter 1: triggers best-save because iter_num > 0
        call["n"] += 1
        return {"train": 1.0, "val": 1.0 - call["n"]}

    def custom_train_step(trainer: Trainer, X: torch.Tensor, Y: torch.Tensor):
        _ = trainer
        _ = Y
        return (X.sum() * 0.0) + torch.tensor(0.0)

    deps = TrainerDependencies(
        initialize_batches=mock_init_batches,
        initialize_model=mock_init_model,
        initialize_components=lambda m, c, r, ld: (m, GradScaler(enabled=False), None),
        create_manager=lambda c, s: None,
        create_mlflow_manager=lambda r, s, logger: cast(Any, MockMLflowManager()),
        load_checkpoint=lambda *args, **kwargs: None,
        apply_checkpoint=lambda *args, **kwargs: (0, 1e9),
        save_checkpoint=save_checkpoint,
        propagate_metadata=lambda *args, **kwargs: None,
        run_evaluation=run_evaluation,
        get_lr=lambda *args, **kwargs: 0.001,
        train_step=custom_train_step,
    )

    trainer = Trainer(cfg, shared, deps=deps)
    trainer.run()
    assert 1 in saved_best_iters


def test_trainer_train_step_validation_and_ema_and_vmap_branches(
    tmp_path: Path,
) -> None:
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
        "grad_accum_steps": 2,
    }
    cfg = TrainerConfig.model_validate(
        {
            "model": model_cfg,
            "data": data_cfg,
            "optim": {"learning_rate": 6e-4, "grad_clip": 0.0},
            "schedule": {"decay_lr": False, "warmup_iters": 0},
            "runtime": {"out_dir": tmp_path, "max_iters": 0, "eval_only": True},
        }
    )
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
        return model, torch.optim.AdamW(model.parameters())

    class _Ema:
        def __init__(self):
            self.updated = 0

        def update(self, _m: Any) -> None:
            self.updated += 1

    def vmap(fn):
        def wrapped(x, y):
            # apply fn along first dimension
            return torch.stack([fn(xi, yi) for xi, yi in zip(x, y, strict=True)])

        return wrapped

    deps = TrainerDependencies(
        initialize_batches=mock_init_batches,
        initialize_model=mock_init_model,
        initialize_components=lambda m, c, r, ld: (
            m,
            GradScaler(enabled=False),
            _Ema(),
        ),
        create_manager=lambda c, s: None,
        create_mlflow_manager=lambda r, s, logger: cast(Any, MockMLflowManager()),
        load_checkpoint=lambda *args, **kwargs: None,
        apply_checkpoint=lambda *args, **kwargs: (0, 1e9),
        save_checkpoint=lambda *args, **kwargs: None,
        propagate_metadata=lambda *args, **kwargs: None,
        run_evaluation=lambda *args, **kwargs: {"train": 0.1, "val": 0.1},
        get_lr=lambda *args, **kwargs: 0.001,
        vmap=vmap,
    )

    trainer = Trainer(cfg, shared, deps=deps)
    X, Y = trainer.batches.get_batch("train")

    # _train_step_vmap branch when vmap is unavailable
    trainer._vmap = None
    try:
        trainer._train_step_vmap(X, Y, 2)
    except RuntimeError:
        pass

    # _train_step_accum uses vmap when available
    trainer._vmap = vmap
    loss = trainer._train_step(X, Y)
    assert isinstance(loss, torch.Tensor)
    assert trainer.ema is not None
