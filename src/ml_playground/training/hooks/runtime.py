"""Runtime initialization helpers for training."""

from __future__ import annotations

from typing import Any, Callable, ContextManager

import torch
from ml_playground.core.runtime_context import RuntimeContext, runtime_context

from ml_playground.configuration.models import TrainerConfig


__all__ = ["RuntimeContext", "setup_runtime"]


def setup_runtime(
    cfg: TrainerConfig,
    *,
    cuda_available_func: Callable[[], bool] | None = None,
    cuda_seed_func: Callable[[int], None] | None = None,
    autocast_func: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
    torch_module: Any | None = None,
) -> RuntimeContext:
    """Seed torch RNGs and configure autocast context based on runtime settings."""
    return runtime_context(
        cfg.runtime,
        logger_name="ml_playground.training.runtime",
        cuda_available_fn=cuda_available_func,
        cuda_manual_seed_fn=cuda_seed_func,
        autocast_factory=autocast_func,
        torch_module=torch_module,
    )
