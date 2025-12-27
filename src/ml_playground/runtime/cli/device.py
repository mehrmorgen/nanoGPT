from __future__ import annotations

from typing import Any, Callable, Optional
import importlib

import torch

__all__ = ["global_device_setup"]


def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: Any = None,
) -> None:
    """Set global seeds and enable TF32 as needed.

    Delegates to the runtime.device implementation to keep CLI and runtime aligned.
    """
    rt_device = importlib.import_module("ml_playground.runtime.device")
    try:
        rt_device.global_device_setup(
            device,
            dtype,
            seed,
            cuda_is_available=cuda_is_available,
            torch_module=torch_module if torch_module is not None else torch,
        )
    except Exception:
        return
