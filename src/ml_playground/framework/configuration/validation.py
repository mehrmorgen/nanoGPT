"""Utility functions for configuration validation.

These functions are internal utilities but made public to enable testing
without violating the no-private-import policy.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable


ResolveFn = Callable[[Path], Path]


def coerce_path(value: object) -> Path | None:
    """Coerce a value to Path if possible, returns None for invalid types."""
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        try:
            return Path(value)
        except (ValueError, OSError):
            return None
    return None


def resolve_if_relative(path: Path | str | None, base_dir: Path) -> Path | None:
    """Resolve path relative to base_dir if it's relative."""
    if path is None:
        return None

    if isinstance(path, str):
        path = Path(path)

    if not path.is_absolute():
        return base_dir / path

    return path


def resolve_path_strict(
    value: Path | str | None, *, resolve: ResolveFn | None = None
) -> Path:
    """Resolve a path strictly, raising ValueError for invalid inputs."""
    if value is None:
        raise ValueError("Path cannot be None")

    if isinstance(value, str):
        value = Path(value)

    if not isinstance(value, Path):
        raise ValueError(f"Expected Path or str, got {type(value)}")

    if not value.is_absolute():
        raise ValueError("Path must be absolute")

    if resolve is not None:
        return resolve(value)

    try:
        return value.resolve()
    except OSError as e:
        raise ValueError(f"Cannot resolve path: {e}") from e


__all__ = [
    "coerce_path",
    "resolve_if_relative",
    "resolve_path_strict",
    "ResolveFn",
]
