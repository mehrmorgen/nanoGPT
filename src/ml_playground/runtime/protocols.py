from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from ml_playground.core.logging_protocol import LoggerLike


@runtime_checkable
class DeviceSetup(Protocol):
    def __call__(
        self,
        device: str,
        dtype: str,
        seed: int,
        *,
        cuda_is_available: Any | None = None,
        torch_module: Any | None = None,
    ) -> None: ...


@runtime_checkable
class PrepareConfigLike(Protocol):
    logger: LoggerLike


@runtime_checkable
class TrainConfigLike(Protocol):
    logger: LoggerLike
    runtime: Any | None
    # optional hooks to align with runtime training flow; kept optional for compatibility
    data: Any | None  # pragma: no cover - protocol attribute
    model: Any | None  # pragma: no cover - protocol attribute
    optim: Any | None  # pragma: no cover - protocol attribute
    schedule: Any | None  # pragma: no cover - protocol attribute


@runtime_checkable
class SampleConfigLike(Protocol):
    logger: LoggerLike
    runtime: Any | None


@runtime_checkable
class SharedConfigLike(Protocol):
    config_path: Path
    dataset_dir: Path
    train_out_dir: Path
    sample_out_dir: Path


__all__ = [
    "DeviceSetup",
    "PrepareConfigLike",
    "TrainConfigLike",
    "SampleConfigLike",
    "SharedConfigLike",
]
