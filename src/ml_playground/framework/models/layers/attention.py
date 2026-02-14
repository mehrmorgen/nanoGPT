from __future__ import annotations

import torch
import torch.nn as nn
from typing import cast
from torch.nn import functional as F

from ml_playground.framework.models.core.config import GPTConfig


class CausalSelfAttention(nn.Module):
    """Causal self-attention block shared across GPT variants."""

    def __init__(self, config: GPTConfig) -> None:
        super().__init__()  # type: ignore[reportUnknownMemberType]
        if config.n_head <= 0:
            raise ValueError("n_head must be a positive integer")
        if config.n_embd % config.n_head != 0:
            raise ValueError("n_embd must be divisible by n_head")

        self.c_attn = nn.Linear(config.n_embd, 3 * config.n_embd, bias=config.bias)
        self.c_proj = nn.Linear(config.n_embd, config.n_embd, bias=config.bias)
        self.n_head = config.n_head
        self.n_embd = config.n_embd
        self.dropout = config.dropout

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        batch_size, seq_len, embed_dim = x.size()

        attn_out = cast(torch.Tensor, self.c_attn(x))
        reshaped = torch.reshape(
            attn_out,
            (batch_size, seq_len, 3, self.n_head, embed_dim // self.n_head),
        )
        q_tensor, k_tensor, v_tensor = cast(
            tuple[torch.Tensor, torch.Tensor, torch.Tensor],
            torch.unbind(reshaped, dim=2),
        )

        k = torch.permute(k_tensor, (0, 2, 1, 3))
        q = torch.permute(q_tensor, (0, 2, 1, 3))
        v = torch.permute(v_tensor, (0, 2, 1, 3))

        y_raw = F.scaled_dot_product_attention(
            q,
            k,
            v,
            attn_mask=None,
            dropout_p=self.dropout if self.training else 0.0,
            is_causal=True,
        )
        y = torch.reshape(
            torch.transpose(y_raw, 1, 2).contiguous(), (batch_size, seq_len, embed_dim)
        )
        res = cast(torch.Tensor, self.c_proj(y))
        return res


__all__ = ["CausalSelfAttention"]
