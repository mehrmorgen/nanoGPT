"""Type stubs for cosmic_ray.modules used in tooling."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from pathlib import Path

__all__ = ["find_modules"]

def find_modules(module_paths: Sequence[Path]) -> Generator[Path, None, None]: ...
