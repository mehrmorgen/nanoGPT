"""Runtime context helpers for consistent device, dtype, and logging setup."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable, ContextManager, Protocol, cast, runtime_checkable

import torch

from ml_playground.framework.configuration.models import RuntimeConfig
from ml_playground.framework.core.error_handling import setup_logging
from ml_playground.framework.core.logging_protocol import LoggerLike

__all__ = ["RuntimeContext", "runtime_context"]


_PT_DTYPES: dict[str, torch.dtype] = {
    "float32": torch.float32,
    "bfloat16": torch.bfloat16,
    "float16": torch.float16,
}


@runtime_checkable
class TorchCuda(Protocol):
    def is_available(self) -> bool: ...
    def manual_seed(self, seed: int, /) -> None: ...


@runtime_checkable
class TorchBackendMatmul(Protocol):
    @property
    def fp32_precision(self) -> str: ...

    @fp32_precision.setter
    def fp32_precision(self, value: str) -> None: ...

    @property
    def allow_tf32(self) -> bool: ...

    @allow_tf32.setter
    def allow_tf32(self, value: bool) -> None: ...


@runtime_checkable
class TorchCudaBackends(Protocol):
    @property
    def matmul(self) -> TorchBackendMatmul: ...


@runtime_checkable
class TorchCudnnBackends(Protocol):
    @property
    def fp32_precision(self) -> str: ...

    @fp32_precision.setter
    def fp32_precision(self, value: str) -> None: ...

    @property
    def allow_tf32(self) -> bool: ...

    @allow_tf32.setter
    def allow_tf32(self, value: bool) -> None: ...


@runtime_checkable
class TorchBackends(Protocol):
    @property
    def cuda(self) -> TorchCudaBackends: ...

    @property
    def cudnn(self) -> TorchCudnnBackends: ...


@runtime_checkable
class TorchModule(Protocol):
    def manual_seed(self, seed: int, /) -> None: ...

    @property
    def cuda(self) -> TorchCuda: ...

    @property
    def backends(self) -> TorchBackends: ...

    def autocast(
        self, *, device_type: str, dtype: torch.dtype
    ) -> ContextManager[Any]: ...


@dataclass(slots=True)
class RuntimeContext:
    """Runtime artifacts required by custom loops."""

    device_type: str
    autocast_context: ContextManager[Any]
    logger: LoggerLike | None = None


def runtime_context(
    runtime: RuntimeConfig,
    *,
    logger_name: str = "ml_playground.framework.runtime",
    logger_level: int = logging.INFO,
    stream_handler_factory: Callable[[], logging.Handler] | None = None,
    cuda_available_fn: Callable[[], bool] | None = None,
    cuda_manual_seed_fn: Callable[[int], None] | None = None,
    autocast_factory: Callable[[str, torch.dtype], ContextManager[Any]] | None = None,
    torch_module: TorchModule | None = None,
) -> RuntimeContext:
    """Configure logging, RNG seeding, and autocast context for a runtime config."""

    torch_mod = torch_module if torch_module is not None else cast(TorchModule, torch)

    logger = setup_logging(
        logger_name,
        level=logger_level,
        stream_handler_factory=stream_handler_factory,
    )

    torch_mod.manual_seed(runtime.seed)
    try:
        cuda_available = (
            cuda_available_fn()
            if cuda_available_fn is not None
            else torch_mod.cuda.is_available()
        )
        if cuda_available:
            (
                cuda_manual_seed_fn(runtime.seed)
                if cuda_manual_seed_fn is not None
                else torch_mod.cuda.manual_seed(runtime.seed)
            )
            torch_mod.backends.cuda.matmul.allow_tf32 = True
            torch_mod.backends.cudnn.allow_tf32 = True
    except (RuntimeError, AssertionError, AttributeError, OSError):
        pass

    device_type = "cuda" if "cuda" in runtime.device else "cpu"
    dtype = _PT_DTYPES[runtime.dtype]
    ctx: ContextManager[Any] = (
        nullcontext()
        if device_type == "cpu"
        else (
            autocast_factory(device_type, dtype)
            if autocast_factory is not None
            else torch_mod.autocast(device_type=device_type, dtype=dtype)
        )
    )

    return RuntimeContext(
        device_type=device_type,
        autocast_context=ctx,
        logger=logger,
    )
