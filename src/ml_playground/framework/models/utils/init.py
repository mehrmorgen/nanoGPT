from __future__ import annotations

import torch
import torch.nn as nn


def init_transformer_weights(module: nn.Module, init_std: float = 0.02) -> None:
    """Initialize transformer weights using normal distribution with configurable std."""
    if isinstance(module, nn.Linear):
        torch.nn.init.normal_(module.weight, mean=0.0, std=init_std)
        if module.bias is not None:
            torch.nn.init.zeros_(module.bias)
    elif isinstance(module, nn.Embedding):
        torch.nn.init.normal_(module.weight, mean=0.0, std=init_std)


__all__ = ["init_transformer_weights"]
