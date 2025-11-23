from __future__ import annotations

from typing import Protocol


class ToolCategoryConfigLike(Protocol):
    """Protocol for tool category configuration.

    This matches the common fields in ToolConfig that the CLI might inspect,
    primarily ``enabled``.
    """

    @property
    def enabled(self) -> bool: ...


class ToolsConfigLike(Protocol):
    """Minimal protocol for tools configuration used by CLI runtime.

    This protocol is intentionally small and only captures the attributes
    accessed through the shared tools runtime state. Concrete configuration
    models like ``ToolsConfig`` are expected to provide at least these
    attributes and may contain many more fields.
    """

    @property
    def learning_mode_default(self) -> bool: ...

    @property
    def default_verbosity(self) -> int: ...

    @property
    def display_command_prefix(self) -> str | None: ...

    @property
    def quality(self) -> ToolCategoryConfigLike: ...

    @property
    def testing(self) -> ToolCategoryConfigLike: ...

    @property
    def environment(self) -> ToolCategoryConfigLike: ...

    @property
    def ci(self) -> ToolCategoryConfigLike: ...
