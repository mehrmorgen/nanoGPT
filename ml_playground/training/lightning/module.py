"""LightningModule wrapper around the GPT training model."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pytorch_lightning as pl
import torch
from torch.optim.lr_scheduler import LambdaLR

from ml_playground.configuration.models import TrainerConfig
from ml_playground.models.core.model import GPT
from ml_playground.training.loop.scheduler import get_lr


__all__ = [
    "LightningGPTModule",
    "LightningModuleDependencies",
]


@dataclass(frozen=True)
class LightningModuleDependencies:
    """Dependency set for constructing the Lightning GPT module."""

    create_model: Callable[[TrainerConfig, Any], GPT]


def default_module_dependencies() -> LightningModuleDependencies:
    """Wire default GPT model construction for Lightning."""

    def _create_model(cfg: TrainerConfig, logger: Any) -> GPT:
        model = GPT(cfg.model, logger=logger)
        model.to(cfg.runtime.device)
        return model

    return LightningModuleDependencies(create_model=_create_model)


class LightningGPTModule(pl.LightningModule):
    """Expose the GPT model using PyTorch Lightning semantics."""

    def __init__(
        self,
        cfg: TrainerConfig,
        *,
        deps: LightningModuleDependencies | None = None,
    ) -> None:
        super().__init__()
        self.cfg = cfg
        self.deps = deps or default_module_dependencies()
        self.gpt = self.deps.create_model(cfg, cfg.logger)
        self.save_hyperparameters(ignore=["cfg", "deps", "gpt"])
        self.example_input_array = torch.zeros(
            (cfg.data.batch_size, cfg.data.block_size), dtype=torch.long
        )

    def forward(
        self, idx: torch.Tensor, targets: torch.Tensor | None = None
    ) -> tuple[torch.Tensor, torch.Tensor | None]:  # pragma: no cover - delegation
        return self.gpt(idx, targets)

    def training_step(self, batch, batch_idx):
        del batch_idx
        x, y = batch
        _, loss = self.gpt(x, y)
        if loss is None:
            raise RuntimeError("Training step requires labels to compute loss")
        self.log(
            "train_loss",
            loss,
            prog_bar=True,
            on_step=True,
            on_epoch=False,
            batch_size=x.size(0),
        )
        return loss

    def validation_step(self, batch, batch_idx):
        del batch_idx
        x, y = batch
        _, loss = self.gpt(x, y)
        if loss is None:
            raise RuntimeError("Validation step requires labels to compute loss")
        self.log(
            "val_loss",
            loss,
            prog_bar=True,
            on_step=False,
            on_epoch=True,
            batch_size=x.size(0),
            sync_dist=True,
        )
        return loss

    def configure_optimizers(self):  # pragma: no cover - relies on torch internals
        optimizer = self.gpt.configure_optimizers(
            self.cfg.optim.weight_decay,
            self.cfg.optim.learning_rate,
            (self.cfg.optim.beta1, self.cfg.optim.beta2),
            self.cfg.runtime.device,
        )

        base_lr = self.cfg.optim.learning_rate or 1.0

        def _lr_lambda(step: int) -> float:
            current_lr = get_lr(step, self.cfg.schedule, self.cfg.optim)
            return current_lr / base_lr if base_lr else 1.0

        scheduler = LambdaLR(optimizer, lr_lambda=_lr_lambda)
        return {
            "optimizer": optimizer,
            "lr_scheduler": {
                "scheduler": scheduler,
                "interval": "step",
                "frequency": 1,
                "name": "learning_rate",
            },
        }
