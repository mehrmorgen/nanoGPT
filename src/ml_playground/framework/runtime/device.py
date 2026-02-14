from __future__ import annotations

from typing import Any, Callable, Optional, cast

import torch


def global_device_setup(
    device: str,
    dtype: str,
    seed: int,
    *,
    cuda_is_available: Optional[Callable[[], bool]] = None,
    torch_module: Optional[Any] = None,
) -> None:
    """Set global seeds and enable TF32 as needed.

    Centralizes side-effectful setup so other modules don't repeat it.
    """
    torch_mod_raw: object = torch_module if torch_module is not None else torch
    manual_seed_fn_raw = getattr(torch_mod_raw, "manual_seed", None)
    if callable(manual_seed_fn_raw):
        cast(Callable[[int], object], manual_seed_fn_raw)(seed)

    cuda_available_fn: Optional[Callable[[], bool]] = cuda_is_available
    if cuda_available_fn is not None:
        _cuda_available = cuda_available_fn()
    else:
        # Access cuda via getattr to avoid Any from the module access
        cuda_mod: object = getattr(torch_mod_raw, "cuda", None)
        if cuda_mod is not None:
            is_available_fn: object = getattr(cuda_mod, "is_available", None)
            _cuda_available = (
                bool(cast(Callable[[], bool], is_available_fn)())
                if callable(is_available_fn)
                else False
            )
        else:
            _cuda_available = False

    if _cuda_available:
        cuda_mod_obj: object = getattr(torch_mod_raw, "cuda", None)
        if cuda_mod_obj is not None:
            manual_seed_fn: object = getattr(cuda_mod_obj, "manual_seed", None)
            if callable(manual_seed_fn):
                cast(Callable[[int], None], manual_seed_fn)(seed)

        # Enable TF32 for better performance on Ampere+
        backends_obj: object = getattr(torch_mod_raw, "backends", None)
        if backends_obj is not None:
            cuda_backend: object = getattr(backends_obj, "cuda", None)
            if cuda_backend is not None:
                matmul_obj: object = getattr(cuda_backend, "matmul", None)
                if matmul_obj is not None:
                    setattr(matmul_obj, "allow_tf32", True)

            cudnn_backend: object = getattr(backends_obj, "cudnn", None)
            if cudnn_backend is not None:
                setattr(cudnn_backend, "allow_tf32", True)
