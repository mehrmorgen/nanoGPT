from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .models import (
    ResolveFn,
    _ConfigCrossFieldValidator as _InternalConfigCrossFieldValidator,
    _coerce_path as _internal_coerce_path,
    _resolve_if_relative as _internal_resolve_if_relative,
)


__all__ = [
    "resolve_if_relative",
    "coerce_path",
    "ConfigCrossFieldValidator",
]


def resolve_if_relative(
    value: Any,
    base_dir: Path,
    *,
    resolve: ResolveFn | None = None,
) -> Any:
    """Public wrapper around the configuration path resolver."""

    return _internal_resolve_if_relative(value, base_dir, resolve=resolve)


def coerce_path(value: Any) -> Path | None:
    """Convert arbitrary inputs to `Path` when possible."""

    return _internal_coerce_path(value)


class ConfigCrossFieldValidator:
    """Expose cross-field validation helpers for configuration models."""

    runtime: Callable[[Any], None] = staticmethod(
        _InternalConfigCrossFieldValidator.runtime
    )
    trainer: Callable[[Any], None] = staticmethod(
        _InternalConfigCrossFieldValidator.trainer
    )
    lr_schedule: Callable[[Any], None] = staticmethod(
        _InternalConfigCrossFieldValidator.lr_schedule
    )
    data: Callable[[Any], None] = staticmethod(_InternalConfigCrossFieldValidator.data)
