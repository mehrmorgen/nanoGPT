from __future__ import annotations

import pytest
import typer

from ml_playground.tools.cli.helpers import (
    get_agentic_tools,
    get_dev_tools,
    handle_tool_result,
    run_tool_command,
    get_configured_root,
)
from ml_playground.tools.cli.state import state, reset_state
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.core.errors import ToolExecutionError


def test_helpers_config_none_raises() -> None:
    reset_state()
    state.config = None
    with pytest.raises(RuntimeError, match="Tools config must be loaded before use"):
        get_configured_root()
    with pytest.raises(RuntimeError, match="Tools config must be loaded before use"):
        get_agentic_tools()
    with pytest.raises(RuntimeError, match="Tools config must be loaded before use"):
        get_dev_tools()


def test_handle_tool_result_coverage(capsys: pytest.CaptureFixture[str]) -> None:
    reset_state()
    res = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="tools",
        category="dev",
        command="test-cmd",
        stdout="hello",
        stderr="world",
    )
    # Use capsys instead of patch
    handle_tool_result(res)
    captured = capsys.readouterr()
    assert "hello" in captured.out
    assert "world" in captured.err


def test_run_tool_command_execution_error() -> None:
    def boom_cmd():
        raise ToolExecutionError("boom", reason="r", rationale="rat")

    with pytest.raises(typer.Exit) as exc:
        run_tool_command(boom_cmd)
    assert exc.value.exit_code == 1
