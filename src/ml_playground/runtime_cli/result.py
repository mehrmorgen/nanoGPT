from __future__ import annotations

from typing import Callable

from ml_playground.framework.runtime import helpers as rt_helpers
from ml_playground.framework.runtime.core.results import ToolResult


def handle_tool_result(result: ToolResult, learning_mode: bool = False) -> None:
    """Handle a ``ToolResult`` and surface learning-mode messaging."""
    rt_helpers.handle_tool_result(result, learning_mode)


def run_or_exit(
    func: Callable[[], None],
    *,
    keyboard_interrupt_msg: str | None = None,
    exception_exit_code: int = 1,
) -> None:
    """Run a function and exit gracefully on exceptions."""
    rt_helpers.run_or_exit(
        func,
        keyboard_interrupt_msg=keyboard_interrupt_msg,
        exception_exit_code=exception_exit_code,
    )
