from __future__ import annotations

import torch
import torch.nn as nn
from typing import cast

from ml_playground.framework.models.core.config import GPTConfig


class MLP(nn.Module):
    """Feed-forward block used inside each transformer block."""

    def __init__(self, config: GPTConfig) -> None:
        super().__init__()  # type: ignore[reportUnknownMemberType]
        self.c_fc = nn.Linear(config.n_embd, 4 * config.n_embd, bias=config.bias)
        self.gelu = nn.GELU(approximate="tanh")
        self.c_proj = nn.Linear(4 * config.n_embd, config.n_embd, bias=config.bias)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        res = cast(torch.Tensor, self.c_fc(x))
        res = cast(torch.Tensor, self.gelu(res))
        res = cast(torch.Tensor, self.c_proj(res))
        return res


__all__ = ["MLP"]
