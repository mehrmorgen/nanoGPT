"""Local type stubs for the cosmic_ray package."""

from .config import ConfigDict, load_config
from .modules import find_modules

__all__ = ["ConfigDict", "find_modules", "load_config"]
