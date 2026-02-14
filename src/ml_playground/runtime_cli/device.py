from __future__ import annotations

import logging
from typing import Callable, Optional

import torch

from ml_playground.framework.runtime.device import (
    global_device_setup as _framework_device_setup,
)

__all__ = ["global_device_setup"]


def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: object | None = None,
) -> None:
    """Set global seeds and enable TF32 as needed.

    Delegates to the runtime.device implementation to keep CLI and runtime aligned.
    """
    try:
        _framework_device_setup(
            device,
            dtype,
            seed,
            cuda_is_available=cuda_is_available,
            torch_module=torch_module if torch_module is not None else torch,
        )
    except Exception:
        # Runtime CLI must not crash due to environment- or torch-specific issues.
        logging.getLogger(__name__).warning(
            "global_device_setup failed; continuing without device setup.",
            exc_info=True,
        )
        return
