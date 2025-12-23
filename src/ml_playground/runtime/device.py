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
    torch_mod.manual_seed(seed)
    _cuda_available = (
        cuda_is_available()
        if cuda_is_available is not None
        else torch_mod.cuda.is_available()
    )
    if _cuda_available:
        torch_mod.cuda.manual_seed(seed)
        # Enable TF32 for better performance on Ampere+
        if hasattr(torch_mod.backends, "cuda"):
            torch_mod.backends.cuda.matmul.allow_tf32 = True
        if hasattr(torch_mod.backends, "cudnn"):
            torch_mod.backends.cudnn.allow_tf32 = True
