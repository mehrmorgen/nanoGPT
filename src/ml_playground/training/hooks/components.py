"""Optional component setup utilities for training."""

from __future__ import annotations

from typing import Callable, cast

import torch
from torch.amp.grad_scaler import GradScaler
from torch.utils.tensorboard.writer import SummaryWriter

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
    compile_fn: Callable[[GPT], GPT] | None = None,
) -> tuple[GPT, GradScaler, EMA | None, TensorboardWriter | None]:
    """Compile model, create scaler/EMA, and initialize TensorBoard writer."""
    compiled_model = model
    if cfg.runtime.compile:
        compiler = compile_fn
        if compiler is None:
            compiler = getattr(torch, "compile", None)
        if compiler is None:
            raise RuntimeError("torch.compile requested but unavailable")
        try:
            compiled_model = cast(GPT, compiler(model))
        except AttributeError as exc:  # torch.compile unavailable or stubbed
            raise RuntimeError("torch.compile requested but unavailable") from exc

    # GradScaler is only meaningful on CUDA with float16; use the new torch.amp API.
    enabled_arg = runtime.device_type == "cuda" and cfg.runtime.dtype == "float16"
    scaler = GradScaler(device="cuda", enabled=enabled_arg)

    ema: EMA | None = None
    if cfg.runtime.ema_decay > 0.0:
        ema = EMA(compiled_model, cfg.runtime.ema_decay, cfg.runtime.device)

    writer: TensorboardWriter | None = None
    if cfg.runtime.tensorboard_enabled:
        writer = SummaryWriter(log_dir=log_dir)

    return compiled_model, scaler, ema, writer
