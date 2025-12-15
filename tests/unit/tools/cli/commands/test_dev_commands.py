from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest
import typer

import ml_playground.tools.cli.commands.dev as dev_commands
from ml_playground.tools.core.errors import ToolConfigurationError, ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def _tool_result(
    command: str, *, success: bool = True, stdout: str = "ok"
) -> ToolResult:
    return ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="tools",
        category="dev",
        command=command,
        stdout=stdout,
        stderr="" if success else "error",
    )


class TestReviewListCommand:
    def test_review_list_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def review_list(
                self,
                *,
                pr_number: int,
                unreplied: bool,
                unresolved: bool,
                remote: str,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "pr_number": pr_number,
                        "unreplied": unreplied,
                        "unresolved": unresolved,
                        "remote": remote,
                    }
                )
                return _tool_result("review-list")

        stub = StubTools()

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: stub):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_review_list(
                    123, unreplied=True, unresolved=False, remote="upstream"
                )

        assert stub.calls == [
            {
                "pr_number": 123,
                "unreplied": True,
                "unresolved": False,
                "remote": "upstream",
            }
        ]
        assert captured and captured[0].operation_id.command == "review-list"

    def test_review_list_handles_tool_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def review_list(self, **_: object) -> ToolResult:  # noqa: ANN401
                raise ToolExecutionError("boom", reason="r", rationale="x")

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            try:
                result = command_func(*args, **kwargs)
            except ToolExecutionError as e:  # type: ignore[unused-ignore]
                result = ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace="tools",
                    category="utils",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: FailingTools()):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_review_list(1)

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "utils"
        assert captured[0].operation_id.command == "generic-error"
        assert "boom" in (captured[0].stderr or "")


class TestBatchReviewCommand:
    def test_batch_review_handles_configuration_error(self) -> None:
        captured: list[ToolResult] = []

        class FailingTools:
            def batch_review(self, *, output_format: str) -> ToolResult:
                assert output_format == "yaml"
                raise ToolConfigurationError("bad", reason="x", rationale="y")

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            try:
                result = command_func(*args, **kwargs)
            except ToolConfigurationError as e:  # type: ignore[unused-ignore]
                result = ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace="tools",
                    category="utils",
                    command="generic-error",
                    stderr=str(e),
                )
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: FailingTools()):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_batch_review(output_format="yaml")

        assert captured and captured[0].success is False
        assert captured[0].operation_id.category == "utils"
        assert captured[0].operation_id.command == "generic-error"
        assert "bad" in (captured[0].stderr or "")


class TestCleanupAndKillPort:
    def test_cleanup_ignored_tracked_propagates_typer_exit(self) -> None:
        class FailingTools:
            def cleanup_ignored_tracked(self) -> ToolResult:
                raise ToolExecutionError("cleanup failed", reason="a", rationale="b")

        with override_attr(dev_commands, "get_dev_tools", lambda: FailingTools()):
            with pytest.raises(typer.Exit):
                dev_commands.dev_cleanup_ignored_tracked()

    def test_kill_port_propagates_typer_exit(self) -> None:
        class FailingTools:
            def kill_port(self, *, port: int) -> ToolResult:
                assert port == 8080
                raise ToolExecutionError("kill failed", reason="a", rationale="b")

        with override_attr(dev_commands, "get_dev_tools", lambda: FailingTools()):
            with pytest.raises(typer.Exit):
                dev_commands.dev_kill_port(8080)


class TestSetupAIGuidelinesCommand:
    def test_setup_ai_guidelines_success(self) -> None:
        captured: list[ToolResult] = []

        class StubTools:
            def setup_ai_guidelines(self, *, tool: str, dry_run: bool) -> ToolResult:
                assert tool == "cursor"
                assert dry_run is True
                return _tool_result("setup-ai-guidelines", stdout="done")

        with override_attr(dev_commands, "get_dev_tools", lambda: StubTools()):
            with override_attr(
                dev_commands,
                "run_tool_command",
                lambda func, *a, **kw: captured.append(func(*a, **kw)),
            ):
                dev_commands.dev_setup_ai_guidelines("cursor", dry_run=True)

        assert captured and captured[0].stdout == "done"
        assert captured[0].success is True


class TestReviewDeleteCommand:
    def test_review_delete_delegates_to_tools(self, tmp_path: Path) -> None:
        captured: list[ToolResult] = []

        class StubTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def review_delete(
                self,
                *,
                pr_number: int,
                comments_file: Path,
                remote: str,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "pr_number": pr_number,
                        "comments_file": comments_file,
                        "remote": remote,
                    }
                )
                return _tool_result("review-delete")

        comments_file = tmp_path / "comments.json"
        comments_file.write_text("[]", encoding="utf-8")

        stub = StubTools()

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: stub):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_review_delete(
                    7,
                    comments_file=comments_file,
                    remote="origin",
                )

        assert stub.calls == [
            {
                "pr_number": 7,
                "comments_file": comments_file,
                "remote": "origin",
            }
        ]
        assert captured and captured[0].operation_id.command == "review-delete"


class TestWorkflowStatusCommand:
    def test_workflow_status_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def workflow_status(self, *, output_format: str) -> ToolResult:
                self.calls.append({"output_format": output_format})
                return _tool_result("workflow-status")

        stub = StubTools()

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: stub):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_workflow_status(output_format="yaml")

        assert stub.calls == [{"output_format": "yaml"}]
        assert captured and captured[0].operation_id.command == "workflow-status"


class TestGithubActionsCommand:
    def test_gha_delegates_to_tools(self) -> None:
        captured: list[ToolResult] = []

        class StubTools:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def gha(
                self,
                *,
                limit: int,
                run_id: int | None,
                latest: bool,
                log_failed: bool,
                remote: str,
                repo: str | None,
            ) -> ToolResult:
                self.calls.append(
                    {
                        "limit": limit,
                        "run_id": run_id,
                        "latest": latest,
                        "log_failed": log_failed,
                        "remote": remote,
                        "repo": repo,
                    }
                )
                return _tool_result("gha")

        stub = StubTools()

        def _capture_run_tool_command(
            command_func: Any, *args: Any, **kwargs: Any
        ) -> None:
            result = command_func(*args, **kwargs)
            captured.append(result)

        with override_attr(dev_commands, "get_dev_tools", lambda: stub):
            with override_attr(
                dev_commands,
                "run_tool_command",
                _capture_run_tool_command,
            ):
                dev_commands.dev_github_actions(
                    limit=3,
                    run_id=123,
                    latest=False,
                    log_failed=True,
                    remote="origin",
                    repo="owner/repo",
                )

        assert stub.calls == [
            {
                "limit": 3,
                "run_id": 123,
                "latest": False,
                "log_failed": True,
                "remote": "origin",
                "repo": "owner/repo",
            }
        ]
        assert captured and captured[0].operation_id.command == "gha"
