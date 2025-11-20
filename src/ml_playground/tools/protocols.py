from __future__ import annotations

from typing import Protocol

from ml_playground.tools.core.config import (
    CIToolsConfig,
    EnvironmentToolsConfig,
    QualityToolsConfig,
    TestToolsConfig,
)


class ToolsConfigLike(Protocol):
    """Minimal protocol for tools configuration used by CLI runtime.

    This protocol is intentionally small and only captures the attributes
    accessed through the shared tools runtime state. Concrete configuration
    models like ``ToolsConfig`` are expected to provide at least these
    attributes and may contain many more fields.
    """

    learning_mode_default: bool
    default_verbosity: int

    quality: QualityToolsConfig
    testing: TestToolsConfig
    environment: EnvironmentToolsConfig
    ci: CIToolsConfig
