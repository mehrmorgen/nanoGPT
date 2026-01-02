"""Runtime context helpers for consistent device, dtype, and logging setup."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable, ContextManager

import torch
from torch import autocast

from ml_playground.configuration.models import RuntimeConfig
from ml_playground.core.error_handling import setup_logging
from ml_playground.core.logging_protocol import LoggerLike

__all__ = ["RuntimeContext", "runtime_context"]


_PT_DTYPES: dict[str, torch.dtype] = {
    "float32": torch.float32,
    "bfloat16": torch.bfloat16,
    "float16": torch.float16,
}


@dataclass(slots=True)
class RuntimeContext:
    """Runtime artifacts required by custom loops."""

    device_type: str
    autocast_context: ContextManager[Any]
    logger: LoggerLike


def runtime_context(
    runtime: RuntimeConfig,
    *,
    logger_name: str = "ml_playground.runtime",
    logger_level: int = logging.INFO,
    stream_handler_factory: Callable[[], logging.Handler] | None = None,
    cuda_available_fn: Callable[[], bool] | None = None,
    cuda_manual_seed_fn: Callable[[int], None] | None = None,
    autocast_factory: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
) -> RuntimeContext:
    """Configure logging, RNG seeding, and autocast context for a runtime config."""

    logger = setup_logging(
        logger_name,
        level=logger_level,
        stream_handler_factory=stream_handler_factory,
    )

    torch.manual_seed(runtime.seed)
    try:
        cuda_available = (
            cuda_available_fn()
            if cuda_available_fn is not None
            else torch.cuda.is_available()
        )
        if cuda_available:
            (
                cuda_manual_seed_fn(runtime.seed)
                if cuda_manual_seed_fn is not None
                else torch.cuda.manual_seed(runtime.seed)
            )
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
    except (RuntimeError, AssertionError, AttributeError, OSError):
        pass

    device_type = "cuda" if "cuda" in runtime.device else "cpu"
    dtype = _PT_DTYPES[runtime.dtype]
    ctx: ContextManager[Any] = (
        nullcontext()
        if device_type == "cpu"
        else (
            autocast_factory(device_type, dtype)
            if autocast_factory is not None
            else autocast(device_type=device_type, dtype=dtype)
        )
    )

    return RuntimeContext(
        device_type=device_type,
        autocast_context=ctx,
        logger=logger,
    )
