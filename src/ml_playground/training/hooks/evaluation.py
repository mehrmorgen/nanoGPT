"""Evaluation helpers for the training loop."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Any, Callable, Dict

from ml_playground.configuration.models import TrainerConfig
from ml_playground.training.types import BatchProvider
from ml_playground.models.utils.estimator import estimate_loss
from ml_playground.models.core.model import GPT
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.training.types import TensorboardWriter


__all__ = ["run_evaluation"]


EstimateLossFn = Callable[[GPT, BatchProvider, int, Any], Dict[str, float]]


def run_evaluation(
    cfg: TrainerConfig,
    *,
    logger: LoggerLike,
    iter_num: int,
    lr: float,
    raw_model: GPT,
    batches: BatchProvider,
    ctx: AbstractContextManager[object],
    writer: TensorboardWriter | None,
    estimate_loss_fn: EstimateLossFn | None = None,
) -> dict[str, float]:
    """Run validation, log metrics, and optionally record TensorBoard scalars."""
    loss_fn = estimate_loss_fn or estimate_loss
    losses = loss_fn(raw_model, batches, cfg.runtime.eval_iters, ctx)
    logger.info(
        f"step {iter_num}: train loss {losses['train']:.4f}, val loss {losses['val']:.4f}"
    )

    if writer:
        writer.add_scalar("Loss/train", losses["train"], iter_num)
        writer.add_scalar("Loss/val", losses["val"], iter_num)
        writer.add_scalar("LR", lr, iter_num)

    return losses
