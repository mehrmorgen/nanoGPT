"""Runtime initialization helpers for training."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from typing import ContextManager, Any, Callable, cast

import torch
from torch import autocast

from ml_playground.configuration.models import TrainerConfig


__all__ = ["RuntimeContext", "setup_runtime"]


@dataclass(slots=True)
class RuntimeContext:
    """Runtime artifacts required by the training loop."""

    device_type: str
    autocast_context: ContextManager[Any]


_PT_DTYPES: dict[str, torch.dtype] = {
    "float32": torch.float32,
    "bfloat16": torch.bfloat16,
    "float16": torch.float16,
}


def _manual_seed(seed: int, torch_module: Any = torch) -> None:
    manual_seed_func = cast(Callable[[int], None], torch_module.manual_seed)
    manual_seed_func(seed)


def _cuda_manual_seed(seed: int, torch_module: Any = torch) -> None:
    manual_seed_func = cast(Callable[[int], None], torch_module.cuda.manual_seed)
    manual_seed_func(seed)


def setup_runtime(
    cfg: TrainerConfig,
    *,
    cuda_available_func: Callable[[], bool] | None = None,
    cuda_seed_func: Callable[[int], None] | None = None,
    autocast_func: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
    torch_module: Any = None,
) -> RuntimeContext:
    """Seed torch RNGs and configure autocast context based on runtime settings.
    Optional callables allow injecting test doubles for CUDA availability, seeding, and autocast creation.
    """
    torch_mod = torch_module if torch_module is not None else torch
    _manual_seed(int(cfg.runtime.seed), torch_mod)
    try:
        cuda_available = (
            cuda_available_func()
            if cuda_available_func is not None
            else torch_mod.cuda.is_available()
        )
        if cuda_available:
            (
                cuda_seed_func(int(cfg.runtime.seed))
                if cuda_seed_func is not None
                else _cuda_manual_seed(int(cfg.runtime.seed), torch_mod)
            )
            # Use new TF32 API to avoid deprecation warnings
            cuda_backends = torch_mod.backends.cuda
            cuda_matmul = cuda_backends.matmul
            if hasattr(cuda_matmul, "fp32_precision"):
                cuda_matmul.fp32_precision = "tf32"
            cudnn_backends = cast(Any, torch_mod.backends.cudnn)
            if hasattr(cudnn_backends, "fp32_precision"):
                cudnn_backends.fp32_precision = "tf32"
    except (RuntimeError, AssertionError, AttributeError):
        pass

    device_type = "cuda" if "cuda" in cfg.runtime.device else "cpu"
    dtype = _PT_DTYPES[cfg.runtime.dtype]
    ctx: ContextManager[Any] = (
        nullcontext()
        if device_type == "cpu"
        else (
            autocast_func(device_type, dtype)
            if autocast_func is not None
            else autocast(device_type=device_type, dtype=dtype)
        )
    )
    return RuntimeContext(device_type=device_type, autocast_context=ctx)
