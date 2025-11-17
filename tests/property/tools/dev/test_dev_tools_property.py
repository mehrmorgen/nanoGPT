from __future__ import annotations

import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, ContextManager

import hypothesis.strategies as st
from hypothesis import given, settings

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev import dev as dev_module
from ml_playground.tools.dev.dev import DevTools

from tests.property.tools._helpers import DeterministicRunner

_MISSING = object()

@contextmanager
def _override_attr(obj: object, name: str, value: object) -> ContextManager[None]:
    """Manual context manager to replace the override_attr fixture."""
    original = getattr(obj, name, _MISSING)
    setattr(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            setattr(obj, name, original)


def _success(command: str) -> ToolResult:
    return ToolResult(
        success=True,
        exit_code=0,
        stdout="ok",
        stderr="",
        operation_id=OperationId(namespace="tools", category="dev", command=command),
    )


@settings(max_examples=25, deadline=None, derandomize=True)
@given(
    pr_number=st.integers(min_value=1, max_value=1000),
    unreplied=st.booleans(),
    unresolved=st.booleans(),
)
def test_review_list_delegates(
    pr_number: int,
    unreplied: bool,
    unresolved: bool,
):
    """DevTools.review_list should delegate to run_review_list with original parameters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_review_list(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("review-list")

        with _override_attr(dev_module, "run_review_list", fake_review_list):
            tools = DevTools(
                ToolsConfig(), subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.review_list(
                pr_number, unreplied=unreplied, unresolved=unresolved
            )

        assert result.success is True
        args, kwargs = seen[0]
        assert kwargs["pr_number"] == pr_number
        assert kwargs["remote"] == "origin"
        assert kwargs["unreplied"] is unreplied
        assert kwargs["unresolved"] is unresolved
        assert kwargs["subprocess_runner"] == tools.subprocess_runner
        assert kwargs["root_path"] == tmp_path
        assert kwargs["review_module_factory"] is None


@settings(max_examples=25, deadline=None, derandomize=True)
@given(
    pr_number=st.integers(min_value=1, max_value=1000),
    remote=st.sampled_from(["origin", "upstream"]),
)
def test_review_bulk_reply_delegates(
    pr_number: int,
    remote: str,
):
    """DevTools.review_bulk_reply should delegate to run_review_bulk_reply."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        replies_file = tmp_path / "replies.json"
        replies_file.write_text("{}", encoding="utf-8")
        seen: list[tuple[Any, ...]] = []

        def fake_review_bulk_reply(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("review-bulk-reply")

        with _override_attr(dev_module, "run_review_bulk_reply", fake_review_bulk_reply):
            tools = DevTools(
                ToolsConfig(), subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.review_bulk_reply(
                pr_number, replies_file=replies_file, remote=remote
            )

        assert result.success is True
        args, kwargs = seen[0]
        assert kwargs["pr_number"] == pr_number
        assert kwargs["replies_file"] == replies_file
        assert kwargs["remote"] == remote
        assert kwargs["subprocess_runner"] == tools.subprocess_runner
        assert kwargs["root_path"] == tmp_path
        assert kwargs["review_module_factory"] is None


@settings(max_examples=25, deadline=None, derandomize=True)
@given(st.just(()))
def test_cleanup_ignored_tracked_delegates(
    _: tuple[()],
):
    """DevTools.cleanup_ignored_tracked should call run_cleanup_ignored_tracked."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_cleanup(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("cleanup-ignored-tracked")

        with _override_attr(dev_module, "run_cleanup_ignored_tracked", fake_cleanup):
            tools = DevTools(
                ToolsConfig(), subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.cleanup_ignored_tracked()

        assert result.success is True
        args, kwargs = seen[0]
        assert kwargs["subprocess_runner"] == tools.subprocess_runner
        assert kwargs["root_path"] == tmp_path


@settings(max_examples=25, deadline=None, derandomize=True)
@given(port=st.integers(min_value=1024, max_value=65535))
def test_kill_port_delegates(
    port: int,
):
    """DevTools.kill_port should delegate to run_kill_port and preserve the port number."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_kill_port(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("kill-port")

        with _override_attr(dev_module, "run_kill_port", fake_kill_port):
            tools = DevTools(
                ToolsConfig(), subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.kill_port(port)

        assert result.success is True
        args, kwargs = seen[0]
        assert kwargs["port"] == port
        assert kwargs["subprocess_runner"] == tools.subprocess_runner
        assert kwargs["root_path"] == tmp_path
