from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .models import (
    ResolveFn,
    ConfigCrossFieldValidator as _ModelsConfigCrossFieldValidator,
    coerce_path as _models_coerce_path,
    resolve_if_relative as _models_resolve_if_relative,
)


__all__ = [
    "resolve_if_relative",
    "coerce_path",
    "ConfigCrossFieldValidator",
]


def resolve_if_relative(
    value: object,
    base_dir: Path,
    *,
    resolve: ResolveFn | None = None,
) -> Path | str | object:
    """Public wrapper around the configuration path resolver."""

    return _models_resolve_if_relative(value, base_dir, resolve=resolve)


def coerce_path(value: object) -> Path | None:
    """Convert arbitrary inputs to `Path` when possible."""

    return _models_coerce_path(value)


class ConfigCrossFieldValidator(_ModelsConfigCrossFieldValidator):
    """Expose cross-field validation helpers for configuration models."""

    runtime: Callable[[Any], None] = staticmethod(
        _ModelsConfigCrossFieldValidator.runtime
    )
    trainer: Callable[[Any], None] = staticmethod(
        _ModelsConfigCrossFieldValidator.trainer
    )
    lr_schedule: Callable[[Any], None] = staticmethod(
        _ModelsConfigCrossFieldValidator.lr_schedule
    )
    data: Callable[[Any], None] = staticmethod(_ModelsConfigCrossFieldValidator.data)
