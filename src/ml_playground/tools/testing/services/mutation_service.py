"""Service for interacting with Cosmic Ray mutation testing tool."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol


class MutationService(Protocol):
    """Protocol for mutation testing services."""

    def load_config(self, path: str | Path) -> Mapping[str, Any]:
        """Load Cosmic Ray configuration from a file."""
        ...

    def find_modules(self, config: object) -> Iterable[Any]:
        """Find modules to mutate based on configuration."""
        ...


class CosmicRayService:
    """Production implementation of MutationService using cosmic-ray."""

    def load_config(self, path: str | Path) -> Mapping[str, Any]:
        from cosmic_ray.config import load_config

        return load_config(str(path))

    def find_modules(self, config: object) -> Iterable[Any]:
        from cosmic_ray.modules import find_modules

        # find_modules expects a sequence of paths or a single module path string.
        # We pass it through and let cosmic-ray handle the type dispatch.
        return find_modules(config)  # type: ignore[no-any-return]
