"""External smoke tests for shared ChatGPT extraction."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.agentic import agentic as agentic_module
from ml_playground.tools.core import config as config_module
from ml_playground.tools.core.config import ToolsConfig
from tests.unit.tools.fakes import FakeSubprocessRunner


_SHARE_URLS = [
    "https://chatgpt.com/share/6990887b-c1a0-8012-aacb-f1aaba433b79",
    "https://chatgpt.com/share/6983c323-c7d4-8012-81d9-e904741ebd18",
]


@pytest.mark.external  # type: ignore[attr-defined]
@pytest.mark.parametrize("url", _SHARE_URLS, ids=["share_6990887b", "share_6983c323"])
def test_scrape_chat_share_external_urls_extract_conversation(
    url: str, tmp_path: Path
) -> None:
    """Validate external share URLs still produce extracted conversation markdown."""
    cfg = ToolsConfig(
        agentic=config_module.AgenticToolsConfig(timeout=300, enabled=True)
    )
    tools = agentic_module.AgenticTools(cfg, tmp_path, FakeSubprocessRunner())
    output_path = tmp_path / "share.md"

    result = tools.scrape_chat_share(url, output_path=output_path, timeout=30.0)

    assert result.success is True, result.stderr
    assert output_path.exists()

    markdown = output_path.read_text(encoding="utf-8")
    assert markdown.startswith("# ")
    assert "## User" in markdown
    assert "## Assistant" in markdown
    assert "Sign up for free" not in markdown
    assert len(markdown) > 1_000
