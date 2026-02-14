from __future__ import annotations

from typing import Dict, Literal, Tuple, cast, ContextManager

import torch

from ml_playground.framework.data_pipeline.sampling.batches import SimpleBatches
from ml_playground.framework.models.core.model import GPT


__all__ = ["estimate_loss"]


def estimate_loss(
    model: GPT, batches: SimpleBatches, eval_iters: int, ctx: ContextManager[object]
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
                    # Capture the model call result as object to break Any tracking
                    raw_outputs = cast(object, model(X, Y))
                    outputs = cast(Tuple[object, object], raw_outputs)
                    # Cast the loss (second element) to torch.Tensor
                    loss = cast(torch.Tensor, outputs[1])
                losses[k] = loss.item()
            out[split_name] = losses.mean().item()
    model.train()
    return out
