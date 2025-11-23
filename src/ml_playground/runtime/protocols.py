from __future__ import annotations

from typing import Any, Protocol

from ml_playground.core.logging_protocol import LoggerLike


class HasRuntime(Protocol):
    """Object with a runtime configuration attribute.

    This is intentionally minimal: concrete runtime config shapes live in
    configuration models; runners only require a subset of fields.
    """

    runtime: Any


class HasLogger(Protocol):
    """Object with an attached logger following LoggerLike."""

    logger: LoggerLike


class PrepareConfigLike(HasRuntime, HasLogger, Protocol):
    """Configuration shape required by run_prepare_impl.

    Implementations must expose a ``data`` attribute with the fields that
    ``create_pipeline`` and related helpers expect.
    """

    data: Any


class TrainConfigLike(HasRuntime, HasLogger, Protocol):
    """Configuration shape required by run_train_impl.

    The nested ``model``, ``optimizer``, and ``trainer`` attributes are
    consumed by downstream training components.
    """

    model: Any
    optimizer: Any
    trainer: Any


class SampleConfigLike(HasRuntime, HasLogger, Protocol):
    """Configuration shape required by run_sample_impl.

    The nested ``model`` and ``sampler`` attributes are consumed by the
    sampling pipeline.
    """

    model: Any
    sampler: Any


class SharedConfigLike(Protocol):
    """Minimal shape required for shared runtime configuration.

    Currently used by helpers.log_command_status, which only relies on the
    presence of a ``dataset_dir`` attribute.
    """

    dataset_dir: Any


class DeviceSetup(Protocol):
    """Protocol for global device setup function."""

    def __call__(self, device: str, dtype: str, seed: int) -> None: ...
