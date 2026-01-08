from __future__ import annotations

from typing import Any, Dict, Literal

import torch

from ml_playground.training.types import BatchProvider
from ml_playground.models.core.model import GPT


__all__ = ["estimate_loss"]


def estimate_loss(
    model: GPT, batches: BatchProvider, eval_iters: int, ctx: Any
) -> Dict[str, float]:
    """Estimate loss on train/val splits."""
    out: Dict[str, float] = {}
    model.eval()
    splits: tuple[Literal["train"], Literal["val"]] = ("train", "val")
    with torch.no_grad():
        for split in splits:
            losses = torch.zeros(eval_iters, dtype=torch.float32)
            for k in range(eval_iters):
                x_batch, y_batch = batches.get_batch(split)
                with ctx:
                    _, loss = model(x_batch, y_batch)
                losses[k] = loss.item()
            out[split] = losses.mean().item()
    model.train()
    return out
