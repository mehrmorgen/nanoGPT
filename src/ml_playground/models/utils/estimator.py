from __future__ import annotations

from typing import Any, Dict, Literal, Tuple

import torch

from ml_playground.data_pipeline.sampling.batches import SimpleBatches
from ml_playground.models.core.model import GPT


__all__ = ["estimate_loss"]


def estimate_loss(
    model: GPT, batches: SimpleBatches, eval_iters: int, ctx: Any
) -> Dict[str, float]:
    """Estimate loss on train/val splits."""
    out: Dict[str, float] = {}
    model.eval()
    splits: Tuple[Literal["train"], Literal["val"]] = ("train", "val")
    with torch.no_grad():
        for split in splits:
            split_name: Literal["train", "val"] = split
            losses = torch.zeros(eval_iters, dtype=torch.float32)
            for k in range(eval_iters):
                X, Y = batches.get_batch(split_name)
                with ctx:
                    _, loss = model(X, Y)
                losses[k] = loss.item()
            out[split_name] = losses.mean().item()
    model.train()
    return out
