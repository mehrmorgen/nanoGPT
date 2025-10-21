"""Helper to orchestrate Lightning-based training sessions."""

from __future__ import annotations

import warnings
from typing import Any, Dict

import pytorch_lightning as pl
import torch
from lightning_fabric.utilities.warnings import PossibleUserWarning

from ml_playground.configuration.models import SharedConfig, TrainerConfig

from .datamodule import LightningBatchDataModule, LightningDataDependencies
from .module import LightningGPTModule, LightningModuleDependencies


__all__ = ["run_lightning_training"]


def _accelerator_for_device(device: str) -> str:
    if device == "cuda" and torch.cuda.is_available():
        return "gpu"
    if device == "cuda":
        return "cpu"
    if device == "mps":
        return "mps"
    return "cpu"


def _precision_for_dtype(dtype: str) -> str | int:
    if dtype == "float16":
        return 16
    if dtype == "bfloat16":
        return "bf16"
    return 32


def run_lightning_training(
    cfg: TrainerConfig,
    shared: SharedConfig,
    *,
    module_deps: LightningModuleDependencies | None = None,
    data_deps: LightningDataDependencies | None = None,
    trainer_kwargs: Dict[str, Any] | None = None,
) -> tuple[int, float]:
    """Run training via PyTorch Lightning and return the last step and best val loss."""

    module = LightningGPTModule(cfg, deps=module_deps)
    data_module = LightningBatchDataModule(cfg, shared, deps=data_deps)

    trainer_options: Dict[str, Any] = {
        "max_steps": cfg.runtime.max_iters,
        "accelerator": _accelerator_for_device(cfg.runtime.device),
        "devices": 1,
        "logger": False,
        "enable_checkpointing": False,
        "enable_model_summary": False,
        "gradient_clip_val": cfg.optim.grad_clip if cfg.optim.grad_clip > 0 else None,
        "gradient_clip_algorithm": "norm",
        "val_check_interval": cfg.runtime.eval_interval,
        "limit_val_batches": cfg.runtime.eval_iters,
        "log_every_n_steps": cfg.runtime.log_interval,
        "precision": _precision_for_dtype(cfg.runtime.dtype),
        "num_sanity_val_steps": 0,
    }
    if trainer_kwargs:
        trainer_options.update(trainer_kwargs)

    warnings.filterwarnings("ignore", category=PossibleUserWarning)
    warnings.filterwarnings(
        "ignore",
        message="Your `IterableDataset` has `__len__` defined",
        category=UserWarning,
    )

    trainer = pl.Trainer(**trainer_options)
    trainer.fit(module, datamodule=data_module)

    metrics = trainer.callback_metrics
    best_val = metrics.get("val_loss")
    if best_val is None:
        best_val_loss = float("inf")
    elif hasattr(best_val, "item"):
        best_val_loss = float(best_val.item())
    else:
        best_val_loss = float(best_val)

    return trainer.global_step, best_val_loss
