from __future__ import annotations

from typing import Any, Callable, Optional

import torch


def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: Any = None,
) -> None:
    """Set global seeds and enable TF32 as needed.

    Centralizes side-effectful setup so other modules don't repeat it.
    """
    torch_mod: Any = torch_module if torch_module is not None else torch
    try:
        torch_mod.manual_seed(seed)
        _cuda_available = (
            cuda_is_available()
            if cuda_is_available is not None
            else getattr(torch_mod, "cuda").is_available()
        )
        if _cuda_available:
            getattr(torch_mod, "cuda").manual_seed(seed)
            # Guarded attribute assignments to avoid optional member access warnings
            try:
                b = getattr(torch_mod, "backends", object())
                mm = getattr(getattr(b, "cuda", object()), "matmul", object())
                try:
                    setattr(mm, "fp32_precision", "tf32")
                except Exception:
                    pass
                cudnn = getattr(b, "cudnn", object())
                try:
                    setattr(cudnn, "fp32_precision", "tf32")
                except Exception:
                    pass
            except Exception:
                pass
    except (RuntimeError, AssertionError, AttributeError):
        # Never fail CLI due to environment-specific torch issues
        pass
