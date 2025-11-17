from __future__ import annotations

from typing import Any, Callable, Optional

import torch

from ml_playground.runtime.device import global_device_setup as rt_global_device_setup


def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: Any | None = None,
) -> None:
    """Invoke the shared runtime device setup helper.

    Accepts optional overrides for CUDA availability checks and the torch module,
    enabling tests to supply fakes without touching global state.
    """
    if torch_module is not None:
        torch_arg = torch_module
    else:
        torch_arg = torch
    rt_global_device_setup(
        device,
        dtype,
        seed,
        cuda_is_available=cuda_is_available,
        torch_module=torch_arg,
    )
