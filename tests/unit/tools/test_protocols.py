from __future__ import annotations

from ml_playground.tools import protocols


def test_protocols_module_exports_symbols() -> None:
    assert protocols.ToolCategoryConfigLike is not None
    assert protocols.ToolsConfigLike is not None
