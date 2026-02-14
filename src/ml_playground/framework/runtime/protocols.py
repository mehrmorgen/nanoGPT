from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from ml_playground.framework.core.logging_protocol import LoggerLike


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
    data: Any | None
    model: Any | None
    optim: Any | None
    schedule: Any | None


@runtime_checkable
class SampleConfigLike(Protocol):
    logger: LoggerLike
    runtime: Any | None


@runtime_checkable
class MetadataConfigLike(Protocol):
    config_path: Path
    dataset_dir: Path
    train_out_dir: Path
    sample_out_dir: Path


@runtime_checkable
class LoadedExperiment(Protocol):
    @property
    def training(self) -> TrainConfigLike | None: ...

    @property
    def sampling(self) -> SampleConfigLike | None: ...

    @property
    def prepare(self) -> PrepareConfigLike | None: ...

    @property
    def metadata(self) -> MetadataConfigLike: ...


__all__ = [
    "DeviceSetup",
    "PrepareConfigLike",
    "TrainConfigLike",
    "SampleConfigLike",
    "MetadataConfigLike",
    "LoadedExperiment",
]
