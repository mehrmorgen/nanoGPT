from __future__ import annotations

from pathlib import Path

from ml_playground.tools.dev.ai_guidelines import TOOL_MAP, SetupResult, ToolSpec, run_setup_ai_guidelines


def test_run_setup_ai_guidelines_wraps_unexpected_exceptions(tmp_path: Path) -> None:
    """Unexpected errors inside the setup workflow should be wrapped.

    This exercises the top-level defensive ``except Exception`` branch by
    registering a ToolSpec whose root path is invalid (uses ".."), which
    causes _project_path to raise ValueError under the main try block.
    """

    # Snapshot and extend TOOL_MAP with a deliberately invalid entry.
    original_spec = TOOL_MAP.get("broken")
    TOOL_MAP["broken"] = ToolSpec(primary_link="AGENTS.md", root="../invalid-root")
    try:
        result = run_setup_ai_guidelines("broken", project_dir=tmp_path, dry_run=False)
    finally:
        # Restore previous value to avoid leaking state across tests.
        if original_spec is None:
            TOOL_MAP.pop("broken", None)
        else:
            TOOL_MAP["broken"] = original_spec

    assert isinstance(result, SetupResult)
    assert result.success is False
    assert result.error is not None
    assert "Failed to setup AI guidelines" in result.error
    assert any("Failed to setup AI guidelines" in line for line in result.logs)
