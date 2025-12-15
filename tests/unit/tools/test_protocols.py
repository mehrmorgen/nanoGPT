from __future__ import annotations

from ml_playground.tools import protocols


def test_protocols_module_exports_symbols() -> None:
    assert protocols.ToolCategoryConfigLike is not None
    assert protocols.ToolsConfigLike is not None


def test_protocol_property_stubs_are_executable() -> None:
    assert protocols.ToolCategoryConfigLike.enabled.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]

    assert (
        protocols.ToolsConfigLike.learning_mode_default.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    )
    assert (
        protocols.ToolsConfigLike.default_verbosity.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    )
    assert (
        protocols.ToolsConfigLike.display_command_prefix.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    )
    assert protocols.ToolsConfigLike.quality.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    assert protocols.ToolsConfigLike.testing.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    assert (
        protocols.ToolsConfigLike.environment.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
    )
    assert protocols.ToolsConfigLike.ci.fget(object()) is None  # pyright: ignore[reportUnknownMemberType]
