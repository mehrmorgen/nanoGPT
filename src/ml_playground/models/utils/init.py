from __future__ import annotations

from typing import cast

import torch.nn as nn


def init_transformer_weights(module: nn.Module) -> None:
    """Initialize transformer weights following the nanoGPT defaults."""

    if isinstance(module, nn.Linear):
        nn.init.normal_(module.weight, mean=0.0, std=0.02)
        bias = cast(nn.Parameter | None, module.bias)
        if bias is not None:
            nn.init.zeros_(bias)
    elif isinstance(module, nn.Embedding):
        nn.init.normal_(module.weight, mean=0.0, std=0.02)


__all__ = ["init_transformer_weights"]
