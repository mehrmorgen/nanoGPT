"""Unit tests for `ml_playground.tools.categories.dev`."""
# ruff: noqa: TID251

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest

from ml_playground.tools.categories import dev
from ml_playground.tools.categories import environment as environment_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result


@pytest.fixture()
def dev_tools(tmp_path: Path) -> tuple[dev.DevTools, FakeSubprocessRunner]:
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    return tools, runner


def _make_result(command: str, *, stdout: str = "", success: bool = True) -> ToolResult:
    category = "env" if command.startswith("env-") else "dev"
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr="" if success else "error",
        operation_id=OperationId(namespace="tools", category=category, command=command),
    )


class _ReviewStub:
    def __init__(self) -> None:
        self.filters_called_with: dict[str, object] | None = None
        self.bulk_called = False
        self.deleted_ids: list[str] = []

    @staticmethod
    def _infer_repo(remote: str) -> tuple[str, str]:
        assert remote == "origin"
        return ("owner", "repo")

    def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> object:  # noqa: ANN401
        assert owner == "owner"
        assert repo == "repo"
        assert pr_number == 42
        return SimpleNamespace(
            threads=[
                SimpleNamespace(
                    url="https://example/review/1",
                    is_resolved=False,
                    comments=[
                        SimpleNamespace(
                            author="alice", viewer_did_author=False, body="Looks good"
                        )
                    ],
                )
            ],
            viewer="bob",
        )

    def apply_filters(
        self,
        threads: list[object],  # noqa: ANN401
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[object]:  # noqa: ANN401
        self.filters_called_with = {
            "threads": threads,
            "unreplied": unreplied,
            "unresolved": unresolved,
            "viewer": viewer,
        }
        return threads

    def _load_replies(self, replies_file: Path) -> list[str]:
        assert replies_file.name == "replies.json"
        return ["reply"]

    def _bulk_reply(self, *, fetch: object, replies: list[str]) -> None:  # noqa: ANN401
        assert fetch.threads
        assert replies == ["reply"]
        self.bulk_called = True

    def _load_comment_targets(self, path: Path) -> list[str]:
        assert path.name == "targets.json"
        return ["c1", "c2"]

    def _comment_lookup(self, fetch: object) -> dict[str, str]:  # noqa: ANN401
        assert fetch.threads
        return {"c1": "comment-1", "c2": "comment-2"}


def test_review_list_renders_threads(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools
    stub = _ReviewStub()
    monkeypatch.setattr(tools, "_review_module", lambda: stub)

    result = tools.review_list(pr_number=42, unreplied=True, unresolved=False)

    assert result.success is True
    assert "Thread:" in result.stdout
    assert stub.filters_called_with is not None
    assert stub.filters_called_with["unreplied"] is True
    assert stub.filters_called_with["viewer"] == "bob"


def test_review_list_includes_full_comment_body(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    class _VerboseStub(_ReviewStub):
        def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> object:  # noqa: ANN401
            assert owner == "owner"
            assert repo == "repo"
            assert pr_number == 42
            body = (
                "First line of comment that exceeds any truncation threshold by being long."
                " Second sentence continues with more details to ensure we keep everything.\n"
                "Second line with additional guidance."
            )
            return SimpleNamespace(
                threads=[
                    SimpleNamespace(
                        url="https://example/review/verbose",
                        is_resolved=False,
                        comments=[
                            SimpleNamespace(
                                author="mentor",
                                viewer_did_author=False,
                                body=body,
                            )
                        ],
                    )
                ],
                viewer="bob",
            )

    monkeypatch.setattr(tools, "_review_module", lambda: _VerboseStub())

    result = tools.review_list(pr_number=42)

    assert "First line of comment" in result.stdout
    assert "Second sentence continues" in result.stdout
    assert "Second line with additional guidance." in result.stdout
    # Ensure the output is not truncated with ellipsis
    assert "... Second sentence" not in result.stdout


def test_review_list_uses_builtin_review_module(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        if command[:4] == ["git", "remote", "get-url", "origin"]:
            return _make_result("git-remote", stdout="git@github.com:owner/repo.git\n")
        if command[:3] == ["gh", "api", "graphql"]:
            # Minimal GraphQL response, matching gh api graphql: root data{}, thread has no url; comment carries url
            payload = (
                '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
                '"isResolved":false,"comments":{"nodes":[{'
                '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
            )
            return _make_result("gh-graphql", stdout=payload)
        return _make_result("noop", stdout="")

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.review_list(pr_number=42, unreplied=True, unresolved=False)

    assert result.success is True
    assert "Thread:" in result.stdout
    # Verify we attempted both git remote and gh graphql
    assert any(cmd[:4] == ["git", "remote", "get-url", "origin"] for cmd in calls)
    assert any(cmd[:3] == ["gh", "api", "graphql"] for cmd in calls)


def test_review_bulk_reply_graphql_post(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        # First fetch threads
        if command[:3] == ["gh", "api", "graphql"] and any(
            "reviewThreads" in p for p in command if isinstance(p, str)
        ):
            payload = (
                '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
                '"isResolved":false,"comments":{"nodes":[{'
                '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
            )
            return _make_result("gh-graphql", stdout=payload)
        # Reply mutation
        if command[:3] == ["gh", "api", "graphql"] and any(
            "addPullRequestReviewComment" in p for p in command if isinstance(p, str)
        ):
            return _make_result("gh-reply", stdout="{}\n")
        # Repo inference (not required for GraphQL reply but may be called elsewhere)
        if command[:4] == ["git", "remote", "get-url", "origin"]:
            return _make_result("git-remote", stdout="git@github.com:owner/repo.git\n")
        return _make_result("noop", stdout="")

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    replies = tmp_path / "replies.json"
    replies.write_text('{"discussion_r1": "Thanks!"}')

    result = tools.review_bulk_reply(42, replies)

    assert result.success is True
    # Ensure we posted a GraphQL reply using the resolved comment id
    assert any(
        cmd[:3] == ["gh", "api", "graphql"]
        and any("inReplyTo=C_xyz" in part for part in cmd if isinstance(part, str))
        for cmd in calls
    )


def test_review_bulk_reply_reports_failures(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        if command[:4] == ["git", "remote", "get-url", "origin"]:
            return _make_result("git-remote", stdout="git@github.com:owner/repo.git\n")
        if command[:3] == ["gh", "repo", "view"]:
            return _make_result(
                "gh-repo-view", stdout='{"owner":{"login":"owner"},"name":"repo"}\n'
            )
        if command[:3] == ["gh", "api", "graphql"] and any(
            "reviewThreads" in p for p in command if isinstance(p, str)
        ):
            payload = (
                '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
                '"isResolved":false,"comments":{"nodes":[{'
                '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
            )
            return _make_result("gh-graphql", stdout=payload)
        if command[:3] == ["gh", "api", "graphql"] and any(
            "addPullRequestReviewComment" in p for p in command if isinstance(p, str)
        ):
            return _make_result("gh-reply", stdout="", success=False)
        return _make_result("noop", stdout="")

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    replies = tmp_path / "replies.json"
    replies.write_text('{"discussion_r1": "Thanks!"}', encoding="utf-8")

    with pytest.raises(ToolExecutionError):
        tools.review_bulk_reply(42, replies)

    assert any(
        cmd[:3] == ["gh", "api", "graphql"]
        and any(
            "addPullRequestReviewComment" in part
            for part in cmd
            if isinstance(part, str)
        )
        for cmd in calls
    )


def test_review_bulk_reply_invalid_replies_format_is_ignored(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools

    # Minimal fetch to allow reaching _load_replies
    calls: list[list[str]] = []

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        if command[:3] == ["gh", "api", "graphql"] and any(
            "reviewThreads" in p for p in command if isinstance(p, str)
        ):
            payload = '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[]}}}}}\n'
            return _make_result("gh-graphql", stdout=payload)
        return _make_result("noop", stdout="")

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    bad = tmp_path / "replies.json"
    bad.write_text("[]")  # list instead of object

    result = tools.review_bulk_reply(42, bad)
    assert result.success is True
    # ensure no reply mutation attempted
    assert not any(
        c[:3] == ["gh", "api", "graphql"]
        and any("addPullRequestReviewComment" in p for p in c if isinstance(p, str))
        for c in calls
    )


def test_review_bulk_reply_invokes_helpers(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools
    stub = _ReviewStub()
    monkeypatch.setattr(tools, "_review_module", lambda: stub)

    replies_file = tmp_path / "replies.json"
    replies_file.write_text("[]")

    result = tools.review_bulk_reply(42, replies_file)

    assert result.success is True
    assert stub.bulk_called is True


def test_review_list_no_threads_reports_empty(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools
    stub = _ReviewStub()

    def apply_filters(*args: object, **kwargs: object) -> list[object]:  # noqa: ANN401
        return []

    stub.apply_filters = apply_filters  # type: ignore[assignment]
    stub.fetch_review_threads = lambda *a, **k: SimpleNamespace(threads=[], viewer=None)  # type: ignore[assignment]

    monkeypatch.setattr(tools, "_review_module", lambda: stub)

    result = tools.review_list(pr_number=9)

    assert result.success is True
    assert "No matching review threads found." in result.stdout


def test_review_delete_removes_comments(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools
    stub = _ReviewStub()
    monkeypatch.setattr(tools, "_review_module", lambda: stub)

    delete_calls: list[list[str]] = []
    results: Iterator[ToolResult] = iter(
        [
            _make_result("delete", stdout=""),
            _make_result("delete", stdout=""),
        ]
    )

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        delete_calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    targets_file = tmp_path / "targets.json"
    targets_file.write_text("[]")

    result = tools.review_delete(42, targets_file)

    assert result.success is True
    assert len(delete_calls) == 2
    assert delete_calls[0][0] == "gh"
    assert delete_calls[1][0] == "gh"


def test_review_delete_handles_missing_comment_ids(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tools, _ = dev_tools
    stub = _ReviewStub()

    def lookup(_fetch: object) -> dict[str, str]:  # noqa: ANN401
        return {"c1": "comment-1"}

    stub._comment_lookup = lookup  # type: ignore[assignment]
    monkeypatch.setattr(tools, "_review_module", lambda: stub)

    delete_calls: list[list[str]] = []
    results = iter([_make_result("delete", stdout="")])

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        delete_calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    targets_file = tmp_path / "targets.json"
    targets_file.write_text("[]")

    result = tools.review_delete(42, targets_file)

    assert result.success is True
    # Only known comment should trigger deletion
    assert len(delete_calls) == 1


def test_cleanup_ignored_tracked_removes_files(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []
    results = iter(
        [
            _make_result("list", stdout="one\ntwo\n"),
            _make_result("rm", stdout=""),
            _make_result("rm", stdout=""),
        ]
    )

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.cleanup_ignored_tracked()

    assert result.success is True
    assert "Removed 2" in result.stdout
    assert calls[0][:3] == ["git", "ls-files", "-i"]
    assert calls[1][:3] == ["git", "rm", "--cached"]


def test_cleanup_ignored_tracked_returns_listing_failure(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    failure = _make_result("list", success=False)

    monkeypatch.setattr(dev, "run_subprocess", lambda *args, **kwargs: failure)

    result = tools.cleanup_ignored_tracked()

    assert result is failure


def test_cleanup_ignored_tracked_no_files(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []
    results = iter([_make_result("list", stdout="")])

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.cleanup_ignored_tracked()

    assert result.success is True
    assert "No ignored tracked files" in result.stdout
    assert len(calls) == 1


def test_cleanup_ignored_tracked_stop_after_failed_removal(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    calls: list[list[str]] = []
    results = iter(
        [
            _make_result("list", stdout="alpha\nbeta\n"),
            _make_result("rm", success=False),
        ]
    )

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.cleanup_ignored_tracked()

    assert result.success is False
    assert calls[0][:3] == ["git", "ls-files", "-i"]
    assert calls[1][:3] == ["git", "rm", "--cached"]
    assert len(calls) == 2


def test_kill_port_on_darwin_kills_each_pid(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    monkeypatch.setattr(dev.platform, "system", lambda: "Darwin")

    calls: list[list[str]] = []
    results = iter(
        [
            _make_result("lookup", stdout="123\n456\n"),
            _make_result("kill", stdout=""),
            _make_result("kill", stdout=""),
        ]
    )

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.kill_port(8080)

    assert result.success is True
    # First call lists pids, following calls kill each pid.
    assert calls[0][0] == "lsof"
    assert calls[1][0] == "kill"
    assert calls[2][0] == "kill"


def test_kill_port_on_darwin_with_no_pids(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    monkeypatch.setattr(dev.platform, "system", lambda: "Darwin")

    calls: list[list[str]] = []
    results = iter([_make_result("lookup", stdout="")])

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return next(results)

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.kill_port(9000)

    assert result.success is True
    assert "No processes found" in result.stdout
    assert len(calls) == 1


def test_kill_port_non_darwin_uses_fuser(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    monkeypatch.setattr(dev.platform, "system", lambda: "Linux")

    calls: list[list[str]] = []
    result_stub = _make_result("fuser", stdout="", success=True)

    def fake_run_subprocess(command: list[str], **kwargs: object) -> ToolResult:
        calls.append(command)
        return result_stub

    monkeypatch.setattr(dev, "run_subprocess", fake_run_subprocess)

    result = tools.kill_port(4200)

    assert result.success is True
    assert "Attempted to kill processes on port 4200" in result.stdout
    assert calls[0][:2] == ["fuser", "-k"]


def test_setup_ai_guidelines_delegates_to_environment(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, runner = dev_tools

    captured_kwargs: dict[str, object] = {}

    class FakeEnvironmentTools:
        def __init__(self, **kwargs: object) -> None:
            captured_kwargs.update(kwargs)

        def ai_guidelines(
            self, _args: list[str], *, tool: str, dry_run: bool
        ) -> ToolResult:
            assert tool == "ruff"
            assert dry_run is True
            return create_success_result(
                OperationId(namespace="tools", category="env", command="ai-guidelines"),
                stdout="delegated",
            )

    monkeypatch.setattr(environment_module, "EnvironmentTools", FakeEnvironmentTools)

    result = tools.setup_ai_guidelines(tool="ruff", dry_run=True)

    assert result.success is True
    assert captured_kwargs["config"] == tools.config
    assert captured_kwargs["subprocess_runner"] is runner
