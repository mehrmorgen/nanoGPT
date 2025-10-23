"""Optional component setup utilities for training."""

from __future__ import annotations

from typing import Callable, Optional, Tuple, cast

import torch
from torch.amp.grad_scaler import GradScaler
from torch.utils.tensorboard import SummaryWriter

from ml_playground.configuration.models import TrainerConfig
from ml_playground.training.ema import EMA
from ml_playground.models.core.model import GPT
from ml_playground.training.hooks.runtime import RuntimeContext
from ml_playground.training.types import TensorboardWriter


__all__ = ["initialize_components"]


def initialize_components(
    model: GPT,
    cfg: TrainerConfig,
    runtime: RuntimeContext,
    *,
    log_dir: str,
    compile_fn: Optional[Callable[[GPT], GPT]] = None,
) -> Tuple[GPT, GradScaler, Optional[EMA], Optional[TensorboardWriter]]:
    """Compile model, create scaler/EMA, and initialize TensorBoard writer."""
    compiled_model = model
    if cfg.runtime.compile:
        compiler = compile_fn
        if compiler is None:
            compiler = getattr(torch, "compile", None)
        if compiler is None:
            raise RuntimeError("torch.compile requested but unavailable")
        compiled_model = cast(GPT, compiler(model))

    scaler_kwargs: dict[str, str | bool] = {}
    if runtime.device_type == "cuda":
        scaler_kwargs["device"] = runtime.device_type
        scaler_kwargs["enabled"] = cfg.runtime.dtype == "float16"
    else:
        scaler_kwargs["enabled"] = False

    scaler = GradScaler(**scaler_kwargs)  # type: ignore[arg-type]

    ema: Optional[EMA] = None
    if cfg.runtime.ema_decay > 0.0:
        ema = EMA(compiled_model, cfg.runtime.ema_decay, cfg.runtime.device)

    writer: Optional[TensorboardWriter] = None
    if cfg.runtime.tensorboard_enabled:
        writer = SummaryWriter(log_dir=log_dir)

    return compiled_model, scaler, ema, writer
