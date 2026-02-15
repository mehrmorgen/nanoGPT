from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest
import typer

import ml_playground.tools.cli.commands.agentic as agentic_commands
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        object.__setattr__(obj, name, original)


def _tool_result(
    command: str, *, success: bool = True, stdout: str = "ok"
) -> ToolResult:
    return ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="tools",
        category="agentic",
        command=command,
        stdout=stdout,
        stderr="" if success else "error",
    )


def test_scrape_chat_share_command_delegates() -> None:
    captured: list[ToolResult] = []

    class StubTools:
        def scrape_chat_share(
            self,
            url: str,
            *,
            output_path: Path | None,
            timeout: float,
            learning_mode: bool,
            verbosity_level: int,
        ) -> ToolResult:
            assert url == "https://chatgpt.com/share/test"
            assert output_path == Path("out.md")
            assert timeout == 10.0
            assert learning_mode is False
            assert verbosity_level == 1
            return _tool_result("share", stdout="markdown")

    with override_attr(
        agentic_commands.cli_helpers, "get_agentic_tools", lambda: StubTools()
    ):
        with override_attr(
            agentic_commands.cli_helpers,
            "handle_tool_result",
            lambda result: captured.append(result),
        ):
            agentic_commands.agentic_scrape_chat_share(
                "https://chatgpt.com/share/test",
                output=Path("out.md"),
                timeout=10.0,
            )

    assert captured and captured[0].stdout == "markdown"


def test_website_to_markdown_command_delegates() -> None:
    captured: list[ToolResult] = []

    class StubTools:
        def website_to_markdown(
            self,
            url: str,
            *,
            output_path: Path | None,
            wait_until: str,
            timeout_ms: int,
            selector: str | None,
            learning_mode: bool,
            verbosity_level: int,
        ) -> ToolResult:
            assert url == "https://example.com"
            assert output_path is None
            assert wait_until == "networkidle"
            assert timeout_ms == 42_000
            assert selector == ".message"
            assert learning_mode is False
            assert verbosity_level == 1
            return _tool_result("website-to-markdown", stdout="# Example")

    with override_attr(
        agentic_commands.cli_helpers, "get_agentic_tools", lambda: StubTools()
    ):
        with override_attr(
            agentic_commands.cli_helpers,
            "handle_tool_result",
            lambda result: captured.append(result),
        ):
            agentic_commands.agentic_website_to_markdown(
                "https://example.com",
                output=None,
                wait_until="networkidle",
                timeout_ms=42_000,
                selector=".message",
            )

    assert captured and captured[0].stdout == "# Example"


def test_website_to_markdown_command_handles_tool_error() -> None:
    class FailingTools:
        def website_to_markdown(self, *_args: object, **_kwargs: object) -> ToolResult:
            raise ToolExecutionError("boom", reason="r", rationale="x")

    with override_attr(
        agentic_commands.cli_helpers, "get_agentic_tools", lambda: FailingTools()
    ):
        with pytest.raises(typer.Exit):
            agentic_commands.agentic_website_to_markdown("https://example.com")
