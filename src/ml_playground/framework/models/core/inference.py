from __future__ import annotations

from typing import TYPE_CHECKING

import torch
from torch import Tensor
from torch.nn import functional as F

from ml_playground.framework.models.core.config import GPTConfig

if TYPE_CHECKING:
    from ml_playground.framework.models.core.model import GPT


def estimate_model_mfu(
    model: torch.nn.Module, fwdbwd_per_iter: int, dt: float
) -> float:
    """Estimate model flops utilization (MFU) relative to A100 peak FLOPs."""

    n_params = sum(p.numel() for p in model.parameters())
    cfg_candidate = getattr(model, "config", None)
    if not isinstance(cfg_candidate, GPTConfig):
        raise AttributeError("model is expected to expose a `config` attribute")
    cfg = cfg_candidate

    L = cfg.n_layer
    H = cfg.n_head
    Q = cfg.n_embd // cfg.n_head
    T = cfg.block_size

    flops_per_token = 6 * n_params + 12 * L * H * Q * T
    flops_per_fwdbwd = flops_per_token * T
    flops_per_iter = flops_per_fwdbwd * fwdbwd_per_iter
    flops_achieved = flops_per_iter / dt
    flops_promised = 312e12
    return flops_achieved / flops_promised


def generate_tokens(
    model: GPT,
    idx: torch.Tensor,
    *,
    max_new_tokens: int,
    temperature: float = 1.0,
    top_k: int | None = None,
) -> torch.Tensor:
    """Autoregressively generate tokens from ``model``.

    Args:
        model: The language model with a ``config`` attribute.
        idx: Token indices with shape ``(batch, time)``.
        max_new_tokens: Number of additional tokens to sample.
        temperature: Sampling temperature (0.0 for greedy decoding).
        top_k: Optional nucleus filtering.
    """

    if temperature < 0.0:
        raise ValueError("temperature must be >= 0.0")

    cfg_candidate = getattr(model, "config", None)
    if not isinstance(cfg_candidate, GPTConfig):
        raise AttributeError("model is expected to expose a `config` attribute")
    cfg = cfg_candidate

    max_vocab_idx = cfg.vocab_size - 1

    if torch.any(idx >= cfg.vocab_size):
        idx = torch.clamp(idx, 0, max_vocab_idx)

    for _ in range(max_new_tokens):
        idx_cond = idx if idx.size(1) <= cfg.block_size else idx[:, -cfg.block_size :]
        logits_tuple: tuple[Tensor, Tensor | None] = model.forward(idx_cond)
        logits_tensor: Tensor = logits_tuple[0]
        logits: Tensor = logits_tensor[:, -1, :]
        if temperature == 0.0:
            idx_next = torch.argmax(logits, dim=-1, keepdim=True)
        else:
            scaled_logits: Tensor = logits / temperature
            if top_k is not None and top_k > 0:
                dims = scaled_logits.size(-1)
                k = min(top_k, dims)
                v, _ = torch.topk(scaled_logits, k)
                scaled_logits[scaled_logits < v[:, [-1]]] = -float("inf")
            probs = F.softmax(scaled_logits, dim=-1)
            idx_next = torch.multinomial(probs, num_samples=1)

        idx_next = torch.clamp(idx_next, 0, max_vocab_idx)
        idx = torch.cat((idx, idx_next), dim=1)

    return idx


__all__ = ["estimate_model_mfu", "generate_tokens"]
