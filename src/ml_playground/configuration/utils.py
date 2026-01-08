from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import (
    ResolveFn,
    ConfigCrossFieldValidator,
    coerce_path as _models_coerce_path,
    resolve_if_relative as _models_resolve_if_relative,
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

    return _models_resolve_if_relative(value, base_dir, resolve=resolve)


def coerce_path(value: Any) -> Path | None:
    """Convert arbitrary inputs to `Path` when possible."""

    return _models_coerce_path(value)
