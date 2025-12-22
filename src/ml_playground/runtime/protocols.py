from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from ml_playground.core.logging_protocol import LoggerLike


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


class PrepareConfigLike(Protocol):
    logger: LoggerLike


class TrainConfigLike(Protocol):
    logger: LoggerLike
    runtime: Any | None


class SampleConfigLike(Protocol):
    logger: LoggerLike
    runtime: Any | None


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
