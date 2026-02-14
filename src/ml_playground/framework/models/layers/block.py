from __future__ import annotations

import torch
import torch.nn as nn
from typing import cast

from ml_playground.framework.models.core.config import GPTConfig
from ml_playground.framework.models.layers.attention import CausalSelfAttention
from ml_playground.framework.models.layers.mlp import MLP
from ml_playground.framework.models.layers.normalization import LayerNorm


class Block(nn.Module):
    """Single transformer block (attention + MLP with residuals)."""

    def __init__(self, config: GPTConfig) -> None:
        super().__init__()  # type: ignore[reportUnknownMemberType]
        self.ln_1 = LayerNorm(config.n_embd, bias=config.bias)
        self.attn = CausalSelfAttention(config)
        self.ln_2 = LayerNorm(config.n_embd, bias=config.bias)
        self.mlp = MLP(config)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        res = x + cast(torch.Tensor, self.attn(self.ln_1(x)))
        res = res + cast(torch.Tensor, self.mlp(self.ln_2(res)))
        return res


__all__ = ["Block"]
