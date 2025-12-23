from __future__ import annotations

from typing import Any, Callable, Optional, cast
import sys
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

    Matches legacy monolithic CLI behavior; never raises on torch env issues.
    """
    if torch_module is not None:
        torch_mod = torch_module
    else:
        cli_mod = sys.modules.get("ml_playground.runtime.cli")
        if cli_mod is None:
            cli_mod = importlib.import_module("ml_playground.runtime.cli")
        torch_mod = cast(Any, getattr(cli_mod, "torch", torch))
    try:
        manual_seed = cast(Callable[[int], object], torch_mod.manual_seed)
        manual_seed(seed)
        _cuda_available = (
            cuda_is_available()
            if cuda_is_available is not None
            else torch_mod.cuda.is_available()
        )
        if _cuda_available:
            cuda_manual_seed = cast(Callable[[int], None], torch_mod.cuda.manual_seed)
            cuda_manual_seed(seed)
            try:
                torch_mod.backends.cuda.matmul.fp32_precision = "tf32"
            except AttributeError:
                pass
            try:
                torch_mod.backends.cudnn.fp32_precision = "tf32"
            except AttributeError:
                pass
    except (RuntimeError, AssertionError, AttributeError):
        pass
