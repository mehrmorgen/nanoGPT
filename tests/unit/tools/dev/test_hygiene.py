"""Unit tests for hygiene module - keeping only tests not covered by property tests."""

from __future__ import annotations

from pathlib import Path

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.core.interfaces import ToolResult, OperationId
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_cleanup_ignored_tracked_exception_path() -> None:
    """Test that unexpected exceptions are handled gracefully."""

    # Simulate an unexpected exception by providing no results and raising via runner behavior
    class RaisingRunner(FakeSubprocessRunner):
        def run_subprocess(self, *args, **kwargs):  # type: ignore[override]
            raise RuntimeError("unexpected")

    tools = DevTools(
        config=ToolsConfig(),
        subprocess_runner=RaisingRunner(),
        root_path=Path("/test"),
    )

    result = tools.cleanup_ignored_tracked()

    assert result.success is False
    assert "Failed to cleanup ignored tracked files" in (result.stderr or "")


def test_cleanup_ignored_tracked_stop_after_failed_removal() -> None:
    """Test that cleanup stops after a failed file removal."""
    runner = FakeSubprocessRunner()
    tools = DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=Path("/test")
    )

    # First call: listing returns files
    # Second call: first file removal fails
    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    runner.set_results(
        [
            ToolResult(
                success=True,
                exit_code=0,
                stdout="file1.txt\nfile2.txt\n",
                operation_id=op,
            ),
            ToolResult(
                success=False, exit_code=1, stderr="Permission denied", operation_id=op
            ),
        ]
    )

    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert "Permission denied" in (result.stderr or "")


def test_cleanup_ignored_tracked_listing_error_passes_through() -> None:
    """Test that listing errors are passed through."""
    runner = FakeSubprocessRunner()
    tools = DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=Path("/test")
    )

    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    runner.set_results(
        [ToolResult(success=False, exit_code=1, stderr="Git error", operation_id=op)]
    )

    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert "Git error" in (result.stderr or "")
