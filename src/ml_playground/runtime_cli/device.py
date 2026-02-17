from __future__ import annotations

import logging
from typing import Callable, Optional

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
        resolved_torch_module: object
        if torch_module is None:
            import torch as resolved_torch_module
        else:
            resolved_torch_module = torch_module

        _framework_device_setup(
            device,
            dtype,
            seed,
            cuda_is_available=cuda_is_available,
            torch_module=resolved_torch_module,
        )
    except Exception:
        # Runtime CLI must not crash due to environment- or torch-specific issues.
        logging.getLogger(__name__).warning(
            "global_device_setup failed; continuing without device setup.",
            exc_info=True,
        )
        return
