"""Runtime initialization helpers for training."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from typing import ContextManager, Any, Callable, cast, Protocol, runtime_checkable

import torch
from torch import autocast

from ml_playground.framework.configuration.models import TrainerConfig


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


@runtime_checkable
class TorchCuda(Protocol):
    """Protocol for torch.cuda subset."""

    def is_available(self) -> bool: ...
    def manual_seed(self, seed: int, /) -> None: ...


@runtime_checkable
class TorchBackendMatmul(Protocol):
    """Protocol for torch.backends.cuda.matmul subset."""

    @property
    def fp32_precision(self) -> str: ...
    @fp32_precision.setter
    def fp32_precision(self, value: str) -> None: ...


@runtime_checkable
class TorchCudaBackends(Protocol):
    """Protocol for torch.backends.cuda subset."""

    @property
    def matmul(self) -> TorchBackendMatmul: ...


@runtime_checkable
class TorchCudnnBackends(Protocol):
    """Protocol for torch.backends.cudnn subset."""

    @property
    def fp32_precision(self) -> str: ...
    @fp32_precision.setter
    def fp32_precision(self, value: str) -> None: ...


@runtime_checkable
class TorchBackends(Protocol):
    """Protocol for torch.backends subset."""

    @property
    def cuda(self) -> TorchCudaBackends: ...

    @property
    def cudnn(self) -> TorchCudnnBackends: ...


@runtime_checkable
class TorchModule(Protocol):
    """Protocol describing the subset of torch we depend on for runtime initialization."""

    def manual_seed(self, seed: int, /) -> object: ...

    @property
    def cuda(self) -> TorchCuda: ...

    @property
    def backends(self) -> TorchBackends: ...


def _manual_seed(seed: int, torch_module: TorchModule) -> None:
    torch_module.manual_seed(seed)


def _cuda_manual_seed(seed: int, torch_module: TorchModule) -> None:
    torch_module.cuda.manual_seed(seed)


def setup_runtime(
    cfg: TrainerConfig,
    *,
    cuda_available_func: Callable[[], bool] | None = None,
    cuda_seed_func: Callable[[int], None] | None = None,
    autocast_func: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
    torch_module: TorchModule | None = None,
) -> RuntimeContext:
    """Seed torch RNGs and configure autocast context based on runtime settings.
    Optional callables allow injecting test doubles for CUDA availability, seeding, and autocast creation.
    """
    torch_mod = torch_module if torch_module is not None else cast(TorchModule, torch)
    _manual_seed(int(cfg.runtime.seed), torch_mod)
    try:
        if cuda_available_func is not None:
            cuda_available = cuda_available_func()
        else:
            cuda_available = torch_mod.cuda.is_available()

        if cuda_available:
            (
                cuda_seed_func(int(cfg.runtime.seed))
                if cuda_seed_func is not None
                else _cuda_manual_seed(int(cfg.runtime.seed), torch_mod)
            )
            # Use new TF32 API to avoid deprecation warnings
            backends_node = torch_mod.backends
            cuda_backends_node = backends_node.cuda
            cuda_matmul_node = cuda_backends_node.matmul

            # Use getattr/setattr to avoid protocol setter issues if needed,
            # but protocols should work.
            if hasattr(cuda_matmul_node, "fp32_precision"):
                cuda_matmul_node.fp32_precision = "tf32"

            cudnn_backends_node = backends_node.cudnn
            if hasattr(cudnn_backends_node, "fp32_precision"):
                cudnn_backends_node.fp32_precision = "tf32"
    except (RuntimeError, AssertionError, AttributeError):
        pass

    device_type = "cuda" if "cuda" in cfg.runtime.device else "cpu"
    dtype = _PT_DTYPES[cfg.runtime.dtype]
    ctx: ContextManager[Any] = (  # type: ignore[assignment]
        nullcontext()
        if device_type == "cpu"
        else cast(
            ContextManager[Any],
            (
                autocast_func(device_type, dtype)
                if autocast_func is not None
                else autocast(device_type=device_type, dtype=dtype)
            ),
        )
    )
    return RuntimeContext(device_type=device_type, autocast_context=ctx)
