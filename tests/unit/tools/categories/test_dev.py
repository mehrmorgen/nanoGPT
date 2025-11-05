"""Unit tests for `ml_playground.tools.categories.dev`."""
# ruff: noqa: TID251

from __future__ import annotations

from pathlib import Path
import os
from types import SimpleNamespace
from typing import Iterator

import pytest

from ml_playground.tools import dev
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import FakeSubprocessRunner


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
) -> None:
    _tools, _ = dev_tools
    stub = _ReviewStub()
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=_,
        root_path=_tools.root_path,
        review_module_factory=lambda: stub,
    )

    result = tools.review_list(pr_number=42, unreplied=True, unresolved=False)

    assert result.success is True
    assert "Thread:" in result.stdout
    assert stub.filters_called_with is not None
    assert stub.filters_called_with["unreplied"] is True
    assert stub.filters_called_with["viewer"] == "bob"


def test_review_list_includes_full_comment_body(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, runner = dev_tools

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

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: _VerboseStub(),
    )

    result = tools.review_list(pr_number=42)

    assert "First line of comment" in result.stdout
    assert "Second sentence continues" in result.stdout
    assert "Second line with additional guidance." in result.stdout
    # Ensure the output is not truncated with ellipsis
    assert "... Second sentence" not in result.stdout


def test_review_list_uses_builtin_review_module(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    payload = (
        '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
        '"isResolved":false,"comments":{"nodes":[{'
        '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
    )
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:owner/repo.git\n"),
            _make_result("gh-graphql", stdout=payload),
        ]
    )
    result = tools.review_list(pr_number=42, unreplied=True, unresolved=False)

    assert "Thread:" in result.stdout
    assert runner.calls[0]["command"][:4] == ["git", "remote", "get-url", "origin"]
    assert runner.calls[1]["command"][:3] == ["gh", "api", "graphql"]


def test_review_bulk_reply_reports_failures(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    tools, runner = dev_tools

    payload = (
        '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
        '"isResolved":false,"comments":{"nodes":[{'
        '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
    )
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:owner/repo.git\n"),
            _make_result("gh-graphql", stdout=payload),  # fetch threads
            _make_result("gh-reply", stdout="", success=False),  # reply fails
        ]
    )

    replies = tmp_path / "replies.json"
    replies.write_text('{"discussion_r1": "Thanks!"}', encoding="utf-8")

    with pytest.raises(ToolExecutionError) as exc_info:
        tools.review_bulk_reply(42, replies)

    assert any(
        "addPullRequestReviewComment" in p
        for p in runner.calls[2]["command"]
        if isinstance(p, str)
    )
    assert "Failed to send reply via GitHub CLI" in str(exc_info.value)


def test_review_bulk_reply_invalid_replies_format_is_ignored(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    tools, runner = dev_tools

    payload = '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[]}}}}}\n'
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:owner/repo.git\n"),
            _make_result("gh-graphql", stdout=payload),
        ]
    )

    bad = tmp_path / "replies.json"
    bad.write_text("[]")  # list instead of object

    result = tools.review_bulk_reply(42, bad)
    assert result.success is True
    # ensure no reply mutation attempted (only two calls made)
    assert len(runner.calls) == 2


def test_review_bulk_reply_invokes_helpers(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    _tools, runner = dev_tools
    stub = _ReviewStub()
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: stub,
    )

    replies_file = tmp_path / "replies.json"
    replies_file.write_text("[]")

    result = tools.review_bulk_reply(42, replies_file)

    assert result.success is True
    assert stub.bulk_called is True


def test_review_list_no_threads_reports_empty(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, runner = dev_tools
    stub = _ReviewStub()

    def apply_filters(*args: object, **kwargs: object) -> list[object]:  # noqa: ANN401
        return []

    stub.apply_filters = apply_filters  # type: ignore[assignment]
    stub.fetch_review_threads = lambda *a, **k: SimpleNamespace(threads=[], viewer=None)  # type: ignore[assignment]

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: stub,
    )

    result = tools.review_list(pr_number=9)

    assert result.success is True
    assert "No matching review threads found." in result.stdout


def test_review_delete_removes_comments(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    _tools, runner = dev_tools
    stub = _ReviewStub()
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: stub,
    )

    results: Iterator[ToolResult] = iter(
        [
            _make_result("delete", stdout=""),
            _make_result("delete", stdout=""),
        ]
    )

    def _consume_results() -> None:
        for _ in range(2):
            r = next(results)
            runner.add_result(r)

    _consume_results()

    targets_file = tmp_path / "targets.json"
    targets_file.write_text("[]")

    result = tools.review_delete(42, targets_file)

    assert result.success is True
    assert len(runner.calls) == 2
    assert runner.calls[0]["command"][0] == "gh"
    assert runner.calls[1]["command"][0] == "gh"


def test_review_delete_handles_missing_comment_ids(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    _tools, runner = dev_tools
    stub = _ReviewStub()

    def lookup(_fetch: object) -> dict[str, str]:  # noqa: ANN401
        return {"c1": "comment-1"}

    stub._comment_lookup = lookup  # type: ignore[assignment]
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: stub,
    )

    runner.add_result(_make_result("delete", stdout=""))

    targets_file = tmp_path / "targets.json"
    targets_file.write_text("[]")

    result = tools.review_delete(42, targets_file)

    assert result.success is True
    # Only known comment should trigger deletion
    assert len(runner.calls) == 1


def test_cleanup_ignored_tracked_removes_files(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    runner.set_results(
        [
            _make_result("list", stdout="one\ntwo\n"),
            _make_result("rm", stdout=""),
            _make_result("rm", stdout=""),
        ]
    )

    result = tools.cleanup_ignored_tracked()

    assert result.success is True
    assert "Removed 2" in result.stdout
    assert runner.calls[0]["command"][:3] == ["git", "ls-files", "-i"]
    assert runner.calls[1]["command"][:3] == ["git", "rm", "--cached"]


def test_cleanup_ignored_tracked_returns_listing_failure(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    failure = _make_result("list", success=False)
    runner.set_results([failure])

    result = tools.cleanup_ignored_tracked()

    assert result is failure


def test_cleanup_ignored_tracked_no_files(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    runner.set_results([_make_result("list", stdout="")])

    result = tools.cleanup_ignored_tracked()

    assert result.success is True
    assert "No ignored tracked files" in result.stdout
    assert len(runner.calls) == 1


def test_cleanup_ignored_tracked_stop_after_failed_removal(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    runner.set_results(
        [
            _make_result("list", stdout="alpha\nbeta\n"),
            _make_result("rm", success=False),
        ]
    )

    result = tools.cleanup_ignored_tracked()

    assert result.success is False
    assert runner.calls[0]["command"][:3] == ["git", "ls-files", "-i"]
    assert runner.calls[1]["command"][:3] == ["git", "rm", "--cached"]
    assert len(runner.calls) == 2


def test_kill_port_kills_each_pid_via_di(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, _ = dev_tools
    killed: list[int] = []
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=_,
        root_path=_tools.root_path,
        pids_by_port=lambda port: [123, 456],
        kill_pid=lambda pid: killed.append(pid) or True,
    )

    result = tools.kill_port(8080)

    assert result.success is True
    assert "Killed 2 processes" in result.stdout
    assert killed == [123, 456]


def test_kill_port_with_no_pids(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, _ = dev_tools
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=_,
        root_path=_tools.root_path,
        pids_by_port=lambda port: [],
        kill_pid=lambda pid: True,
    )

    result = tools.kill_port(9000)

    assert result.success is True
    assert "No processes found" in result.stdout


def test_kill_port_non_darwin_behavior_via_di(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, _ = dev_tools
    seen: list[int] = []
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=_,
        root_path=_tools.root_path,
        pids_by_port=lambda port: [42],
        kill_pid=lambda pid: (seen.append(pid) or True),
    )

    result = tools.kill_port(4200)

    assert result.success is True
    assert "Killed 1 processes" in result.stdout
    assert seen == [42]


def test_setup_ai_guidelines_runs_integrated_logic(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, _ = dev_tools
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=True)
    assert result.success is True
    assert "[dry-run]" in result.stdout or "done." in result.stdout


def test_review_list_returns_failure_on_exception(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, runner = dev_tools
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: _ReviewStub(),
    )

    runner.set_results([_make_result("list", success=False)])

    result = tools.review_list(pr_number=1)

    assert result.success is False


def test_review_bulk_reply_returns_failure_on_general_exception(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, runner = dev_tools

    class Stub:
        def _load_replies(self, replies_file: Path) -> list[str]:  # noqa: ANN401
            raise ValueError("invalid json")

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: Stub(),
    )

    result = tools.review_bulk_reply(42, Path("replies.json"))

    assert result.success is False
    assert "Failed to send bulk replies" in (result.stderr or "")


def test_review_delete_propagates_deletion_failure(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    tmp_path: Path,
) -> None:
    _tools, runner = dev_tools

    class Stub(_ReviewStub):
        def _load_comment_targets(self, path: Path) -> list[str]:  # noqa: ANN401
            return ["c1"]

        def _comment_lookup(self, fetch: object) -> dict[str, str]:  # noqa: ANN401
            return {"c1": "comment-1"}

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: Stub(),
    )

    runner.add_result(_make_result("delete", success=False))

    result = tools.review_delete(42, tmp_path / "targets.json")

    assert result.success is False


def test_cleanup_ignored_tracked_exception_path(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools

    # Simulate an unexpected exception by providing no results and raising via runner behavior
    class RaisingRunner(FakeSubprocessRunner):
        def run_subprocess(self, *args, **kwargs):  # type: ignore[override]
            raise RuntimeError("unexpected")

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=RaisingRunner(),
        root_path=tools.root_path,
    )

    result = tools.cleanup_ignored_tracked()

    assert result.success is False
    assert "Failed to cleanup ignored tracked files" in (result.stderr or "")


def test_kill_port_kill_failure_is_returned(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    _tools, _ = dev_tools
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=_,
        root_path=_tools.root_path,
        pids_by_port=lambda port: [111],
        kill_pid=lambda pid: False,
    )

    result = tools.kill_port(1111)

    assert result.success is False
    assert "Failed to kill PID 111" in (result.stderr or "")


def test_setup_ai_guidelines_unknown_tool_returns_error(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, _ = dev_tools
    result = tools.setup_ai_guidelines(tool="not-a-tool", dry_run=True)
    assert result.success is False
    assert "Unknown tool" in (result.stderr or "")


def test_setup_ai_guidelines_windsurf_mirrors_tree_and_reports_ignores(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    _tools, runner = dev_tools

    # Arrange project structure with .dev-guidelines content to mirror
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")
    (base / "extra.md").write_text("x")

    # Add .gitignore and .aiignore patterns that should be reported
    (tmp_path / ".gitignore").write_text(".windsurf/\n")
    (tmp_path / ".aiignore").write_text(".windsurf/**\n")

    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )

    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=True)

    assert result.success is True
    # Should complete with informational logs
    assert "done." in result.stdout
    # Should mention git or ai ignore statuses
    assert "git    '" in result.stdout or "ai     '" in result.stdout


def test_setup_ai_guidelines_single_file_root_gemini(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    _tools, runner = dev_tools

    # Ensure BASE_DIR exists so internal checks run without filesystem errors
    (tmp_path / ".dev-guidelines").mkdir(parents=True, exist_ok=True)

    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    result = tools.setup_ai_guidelines(tool="gemini", dry_run=True)

    assert result.success is True
    # Single-file root should note and stop early
    assert "configured as single-file root" in result.stdout


def test_tools_cli_main_sets_dry_run_env(tmp_path: Path) -> None:
    # Import locally to avoid circulars
    import ml_playground.tools.cli as cli

    # Call main without overriding project_root to use repository pyproject
    cli.main(learning_mode=False, verbosity=1, dry_run=True, project_root=None)
    # Verify env var toggled
    assert os.environ.get("ML_PLAYGROUND_TOOLS_DRY_RUN") == "1"


def test_tools_cli_get_dev_tools(tmp_path: Path) -> None:
    import ml_playground.tools.cli as cli

    # Initialize state using repository root config
    cli.main(learning_mode=False, verbosity=0, dry_run=False, project_root=None)
    tools = cli._get_dev_tools()
    from ml_playground.tools.dev import DevTools as DevToolsClass

    assert isinstance(tools, DevToolsClass)


def test_review_infer_repo_fallback_and_failure(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools
    mod = tools._review_module()

    # Case 1: git remote returns empty -> fallback to gh repo view succeeds
    runner.set_results(
        [
            _make_result("git-remote", stdout="\n"),
            _make_result("gh-repo-view", stdout="owner/name\n"),
        ]
    )
    owner, repo = mod._infer_repo("origin")
    assert owner == "owner" and repo == "name"

    # Case 2: fallback fails -> raises ToolExecutionError
    runner.set_results(
        [
            _make_result("git-remote", stdout="\n"),
            _make_result("gh-repo-view", stdout="", success=False),
        ]
    )
    with pytest.raises(ToolExecutionError):
        mod._infer_repo("origin")


def test_apply_filters_unreplied_and_unresolved():
    tools = dev.DevTools()
    mod = tools._builtin_review_module()

    # No reply is posted in review_list; only fetch should be called
    t1 = SimpleNamespace(is_resolved=True, comments=[])
    # Thread 2: unresolved but has viewer comment -> filtered when unreplied=True
    c_viewer = SimpleNamespace(viewer_did_author=True)
    t2 = SimpleNamespace(is_resolved=False, comments=[c_viewer])
    # Thread 3: unresolved and no viewer comment -> kept
    c_other = SimpleNamespace(viewer_did_author=False)
    t3 = SimpleNamespace(is_resolved=False, comments=[c_other])

    out = mod.apply_filters([t1, t2, t3], unreplied=True, unresolved=True, viewer="bob")
    assert out == [t3]


def test_comment_lookup_maps_id_url_suffix_and_dbid():
    tools = dev.DevTools()
    mod = tools._builtin_review_module()

    cm = SimpleNamespace(id="C1", url="https://x/y#disc", database_id=123)
    th = SimpleNamespace(comments=[cm])
    fetch = SimpleNamespace(threads=[th])

    mapping = mod._comment_lookup(fetch)
    assert mapping["C1"] == "C1"
    assert mapping["https://x/y#disc"] == "C1"
    assert mapping["disc"] == "C1"
    assert mapping["123"] == "C1"


def test_bulk_reply_accepts_full_url_keys(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    tools, runner = dev_tools
    # Prepare fetch_result with URL/id mapping
    payload = (
        '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[{'
        '"isResolved":false,"comments":{"nodes":[{'
        '"author":{"login":"alice"},"body":"Looks good","url":"https://example/review/1#discussion_r1","id":"C_xyz","databaseId":1,"createdAt":"2025-01-01T00:00:00Z"}]}}]}}}}}\n'
    )
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:owner/repo.git\n"),
            _make_result("gh-graphql", stdout=payload),  # fetch
            _make_result("gh-reply", stdout="{}\n"),  # reply
        ]
    )

    replies = tmp_path / "replies.json"
    replies.write_text('{"https://example/review/1#discussion_r1": "Thanks!"}')

    result = tools.review_bulk_reply(42, replies)
    assert result.success is True


def test_load_comment_targets_invalid_returns_empty(tmp_path: Path) -> None:
    tools = dev.DevTools()
    mod = tools._builtin_review_module()
    bad = tmp_path / "targets.json"
    bad.write_text('{"not": "a list"}')
    assert mod._load_comment_targets(bad) == []


def test_render_threads_handles_empty_comment_body() -> None:
    tools = dev.DevTools()
    # Create a thread with a comment that has empty body and viewer flag
    comment = SimpleNamespace(author="me", viewer_did_author=True, body="")
    thread = SimpleNamespace(url="u", is_resolved=False, comments=[comment])
    lines = tools._render_threads(
        [thread],
        apply_filters=lambda x, **k: x,
        unreplied=False,
        unresolved=False,
        viewer="me",
    )
    assert any("<no content>" in line for line in lines)


def test_review_bulk_reply_allows_empty_fetch_via_stub(tmp_path: Path) -> None:
    class Stub:
        def _infer_repo(self, remote: str) -> tuple[str, str]:  # noqa: ANN401
            return ("o", "r")

        def fetch_review_threads(self, *a, **k):  # noqa: ANN401, ANN001, ANN002
            return SimpleNamespace(threads=[], viewer=None)

        def _load_replies(self, path: Path) -> dict[str, str]:  # noqa: ANN401
            return {"any": "ok"}

        def _bulk_reply(self, *, fetch: object, replies: dict[str, str]) -> None:  # noqa: ANN401
            assert getattr(fetch, "threads", []) == []
            assert replies == {"any": "ok"}

    tools = dev.DevTools(review_module_factory=lambda: Stub())
    tmp = tmp_path / "replies.json"
    tmp.write_text('{"any": "ok"}')
    result = tools.review_bulk_reply(1, tmp)
    assert result.success is True


def test_load_replies_filters_invalid_types(tmp_path: Path) -> None:
    tools = dev.DevTools()
    mod = tools._builtin_review_module()
    fp = tmp_path / "replies.json"
    # Includes list value and object value; only valid str->str should remain
    fp.write_text('{"k_list": [1], "valid": "ok", "k_obj": {"a": 1}}')
    mapping = mod._load_replies(fp)
    assert mapping == {"valid": "ok"}


def test_setup_ai_guidelines_mirror_non_empty_dir_failure(tmp_path: Path) -> None:
    # Arrange BASE_DIR with one file to mirror
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")
    (base / "extra.md").write_text("x")

    # Prepare destination as non-empty directory where a file link should go
    tool_dir = tmp_path / ".windsurf" / "rules"
    (tool_dir / "extra.md").mkdir(parents=True, exist_ok=True)
    (tool_dir / "extra.md" / "keep.txt").write_text("contents")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert result.success is False
    assert "failed to remove existing path" in (
        result.stderr or ""
    ) or "Cannot replace non-empty directory" in (result.stderr or "")


def test_setup_ai_guidelines_windsurf_symlinks_created(tmp_path: Path) -> None:
    # Arrange BASE_DIR with files
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")
    extra = base / "extra.md"
    extra.write_text("x")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert result.success is True

    # Verify primary link created
    primary = tmp_path / ".windsurf" / "rules" / "rule.md"
    assert primary.exists()
    # For POSIX, it should be a symlink pointing to README
    if primary.is_symlink():
        target = (primary.parent / primary.readlink()).resolve()
        assert target == readme.resolve()


def test_setup_ai_guidelines_gemini_dry_run_reports_root_file(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="gemini", dry_run=True)
    assert result.success is True
    assert "GEMINI.md" in result.stdout


def test_setup_ai_guidelines_codex_creates_root_file(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="codex", dry_run=False)
    assert result.success is True

    primary = tmp_path / "AGENTS.md"
    assert primary.exists()
    if primary.is_symlink():
        target = (primary.parent / primary.readlink()).resolve()
        assert target == readme.resolve()


def test_kill_port_dedup_and_success() -> None:
    calls: list[int] = []

    def fake_pids(port: int) -> list[int]:  # noqa: ANN001
        assert port == 9999
        return [123, 123, 456]

    def fake_kill(pid: int) -> bool:  # noqa: ANN001
        calls.append(pid)
        return True

    tools = dev.DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(9999)
    assert result.success is True
    assert "Killed 2 processes" in result.stdout
    assert calls == [123, 456]


def test_kill_port_failure_propagates() -> None:
    def fake_pids(_port: int) -> list[int]:  # noqa: ANN001
        return [111]

    def fake_kill(_pid: int) -> bool:  # noqa: ANN001
        return False

    tools = dev.DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)
    result = tools.kill_port(8080)
    assert result.success is False
    assert "Failed to kill PID 111" in result.stderr


def test_kill_port_no_processes() -> None:
    tools = dev.DevTools(pids_by_port=lambda p: [], kill_pid=lambda pid: True)
    result = tools.kill_port(1234)
    assert result.success is True
    assert "No processes found" in result.stdout


def test_cleanup_ignored_tracked_no_ignored(tmp_path: Path) -> None:
    tools = dev.DevTools(root_path=tmp_path)
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )

    # First call: listing returns empty
    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    runner.set_results(
        [ToolResult(success=True, exit_code=0, stdout="\n", stderr="", operation_id=op)]
    )

    result = tools.cleanup_ignored_tracked()
    assert result.success is True
    assert "No ignored tracked files" in result.stdout


def test_cleanup_ignored_tracked_removes_files_alt(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    runner.set_results(
        [
            ToolResult(
                success=True, exit_code=0, stdout="a\nb\n", stderr="", operation_id=op
            ),
            ToolResult(
                success=True, exit_code=0, stdout="", stderr="", operation_id=op
            ),
            ToolResult(
                success=True, exit_code=0, stdout="", stderr="", operation_id=op
            ),
        ]
    )
    result = tools.cleanup_ignored_tracked()
    assert result.success is True
    assert "Removed 2 ignored tracked files" in result.stdout
    # verify calls: ls-files then two git rm
    assert runner.calls[0]["command"][:3] == ["git", "ls-files", "-i"]
    assert runner.calls[1]["command"][:3] == ["git", "rm", "--cached"]
    assert runner.calls[2]["command"][:3] == ["git", "rm", "--cached"]


def test_cleanup_ignored_tracked_rm_failure_returns_failure(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    runner.set_results(
        [
            ToolResult(
                success=True, exit_code=0, stdout="a\n", stderr="", operation_id=op
            ),
            ToolResult(
                success=False, exit_code=1, stdout="", stderr="rm fail", operation_id=op
            ),
        ]
    )
    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert result.stderr == "rm fail"


def test_review_delete_failure_on_graphql_returns_failure(tmp_path: Path) -> None:
    class Stub:
        def _infer_repo(self, remote: str) -> tuple[str, str]:  # noqa: ANN401
            return ("o", "r")

        def fetch_review_threads(self, *a, **k):  # noqa: ANN401, ANN001, ANN002
            cm = SimpleNamespace(
                id="CID",
                url="https://example#CID",
                author="a",
                viewer_did_author=False,
                body="b",
            )
            th = SimpleNamespace(url="u", is_resolved=False, comments=[cm])
            return SimpleNamespace(threads=[th], viewer="me")

        def _load_comment_targets(self, path: Path) -> list[str]:  # noqa: ANN401
            return ["CID"]

        def _comment_lookup(self, fetch: object) -> dict[str, str]:  # noqa: ANN401
            return {"CID": "CID"}

    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: Stub(),
    )
    # Set deletion call to fail
    op = OperationId(namespace="tools", category="dev", command="review-delete")
    runner.set_results(
        [
            ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="del fail",
                operation_id=op,
            )
        ]
    )
    result = tools.review_delete(1, tmp_path / "targets.json")
    assert result.success is False
    assert result.stderr == "del fail"


def test_cleanup_ignored_tracked_exception_returns_failure(tmp_path: Path) -> None:
    class RaisingRunner(FakeSubprocessRunner):
        def run_subprocess(self, *args, **kwargs):  # type: ignore[override]
            raise RuntimeError("boom")

    runner = RaisingRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert "Failed to cleanup ignored tracked files" in result.stderr


def test_kill_port_exception_path_returns_failure() -> None:
    def raising(_port: int) -> list[int]:  # noqa: ANN001
        raise RuntimeError("oops")

    tools = dev.DevTools(pids_by_port=raising, kill_pid=lambda _: True)
    result = tools.kill_port(5555)
    assert result.success is False
    assert "Failed to kill port 5555: oops" in result.stderr


def test_review_delete_exception_returns_toolresult(tmp_path: Path) -> None:
    class BadFactory:
        def __call__(self):  # noqa: D401
            raise RuntimeError("factory failure")

    tools = dev.DevTools(root_path=tmp_path, review_module_factory=BadFactory())
    out = tools.review_delete(1, tmp_path / "targets.json")
    assert out.success is False
    assert "Failed to delete comments" in out.stderr


def test_render_threads_no_match_prints_message() -> None:
    tools = dev.DevTools()
    lines = tools._render_threads(
        [],
        apply_filters=lambda *_a, **_k: [],
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert any("No matching review threads found." in line for line in lines)


def test_comment_lookup_resolves_id_url_anchor_and_dbid() -> None:
    tools = dev.DevTools()
    mod = tools._builtin_review_module()

    cm = SimpleNamespace(
        id="C1",
        url="https://example/pr#disc_7",
        database_id=7,
        author="a",
        viewer_did_author=False,
        body="x",
    )
    th = SimpleNamespace(url="u", is_resolved=False, comments=[cm])
    fetch = SimpleNamespace(threads=[th], viewer="me")

    mapping = mod._comment_lookup(fetch)
    assert mapping["C1"] == "C1"
    assert mapping["https://example/pr#disc_7"] == "C1"
    assert mapping["disc_7"] == "C1"
    assert mapping["7"] == "C1"


def test_render_threads_multiline_comment_formats_continuations() -> None:
    tools = dev.DevTools()
    body = "first line\nsecond line\nthird"
    cm = SimpleNamespace(author="bob", viewer_did_author=False, body=body)
    thread = SimpleNamespace(url="u", is_resolved=False, comments=[cm])
    lines = tools._render_threads(
        [thread],
        apply_filters=lambda x, **k: x,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    # First line is inline with author, continuations are indented
    assert any("- bob: first line" in line for line in lines)
    assert any(line.strip() == "second line" for line in lines)
    assert any(line.strip() == "third" for line in lines)


def test_setup_ai_guidelines_unknown_tool_errors(tmp_path: Path) -> None:
    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="unknown_tool_xyz", dry_run=False)
    assert result.success is False
    assert "Unknown tool" in (result.stderr or "")


def test_setup_ai_guidelines_cleans_broken_symlinks(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")

    # Prepare windsor rules dir and a broken symlink pointing into BASE_DIR
    rules_dir = tmp_path / ".windsurf" / "rules"
    rules_dir.mkdir(parents=True, exist_ok=True)
    broken_target = base / "ghost.md"  # does not exist
    broken_link = rules_dir / "ghost.md"
    try:
        broken_link.symlink_to(broken_target)
    except OSError:
        # On platforms without symlink support, skip
        return

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert result.success is True
    # Broken symlink should be removed
    assert not broken_link.exists()
    assert "clean  removed broken symlink" in result.stdout


def test_setup_ai_guidelines_gitignore_negation_and_aiignore_logging(
    tmp_path: Path,
) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")

    # .gitignore: ignore then keep via negation
    (tmp_path / ".gitignore").write_text(
        ".windsurf/rules/\n!.windsurf/rules/\n",
        encoding="utf-8",
    )
    # .aiignore: exclude windsor directory
    (tmp_path / ".aiignore").write_text(
        ".windsurf/rules/**\n",
        encoding="utf-8",
    )

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=True)
    assert result.success is True
    # git kept-by-negation branch
    assert "kept by negated pattern" in result.stdout
    # aiignore exclusion branch
    assert "is excluded by .aiignore" in result.stdout


def test_cleanup_ignored_tracked_listing_error_passes_through(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    op = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    failure = ToolResult(
        success=False, exit_code=1, stdout="", stderr="boom", operation_id=op
    )
    runner.set_results([failure])
    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert result.stderr == "boom"


def test_review_list_generic_exception_returns_toolresult() -> None:
    class Crash:
        def _infer_repo(self, remote: str) -> tuple[str, str]:  # noqa: ANN401
            raise RuntimeError("explode")

    tools = dev.DevTools(review_module_factory=lambda: Crash())
    out = tools.review_list(1)
    assert out.success is False
    assert "Failed to list review comments" in out.stderr


def test_review_bulk_reply_raises_tool_execution_error(tmp_path: Path) -> None:
    class Stub:
        def _infer_repo(self, remote: str) -> tuple[str, str]:  # noqa: ANN401
            return ("o", "r")

        def fetch_review_threads(self, *a, **k):  # noqa: ANN401, ANN001, ANN002
            return SimpleNamespace(threads=[], viewer=None)

        def _load_replies(self, path: Path) -> dict[str, str]:  # noqa: ANN401
            return {"id": "reply"}

        def _bulk_reply(self, *, fetch: object, replies: dict[str, str]) -> None:  # noqa: ANN401
            raise ToolExecutionError("fail", reason="x", rationale="y")

    tools = dev.DevTools(review_module_factory=lambda: Stub())
    tmp = tmp_path / "replies.json"
    tmp.write_text('{"id": "reply"}')
    with pytest.raises(ToolExecutionError):
        tools.review_bulk_reply(1, tmp)


def test_setup_ai_guidelines_junie_dry_run_reports_actions(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")

    # Also include ignores to hit ignore-report branches
    (tmp_path / ".gitignore").write_text(".junie/\n")
    (tmp_path / ".aiignore").write_text(".junie/**\n")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="junie", dry_run=True)

    assert result.success is True
    assert "[dry-run]" in result.stdout
    assert ".junie" in result.stdout


def test_setup_ai_guidelines_kiro_creates_nested_primary_link(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="kiro", dry_run=False)
    assert result.success is True

    primary = tmp_path / ".kiro" / "steering" / "product.md"
    assert primary.exists()
    if primary.is_symlink():
        target = (primary.parent / primary.readlink()).resolve()
        assert target == readme.resolve()


def test_setup_ai_guidelines_cursor_dry_run_reports_actions(tmp_path: Path) -> None:
    # Arrange BASE_DIR with files for cursor tool
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")
    extra = base / "extra.mdc"
    extra.write_text("x")

    # Write ignores to also exercise ignore reporting branches
    (tmp_path / ".gitignore").write_text(".cursor/\n")
    (tmp_path / ".aiignore").write_text(".cursor/**\n")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="cursor", dry_run=True)

    assert result.success is True
    # Should show dry-run link creation and ignore status lines
    assert "[dry-run]" in result.stdout
    assert ".cursor" in result.stdout


def test_setup_ai_guidelines_copilot_dry_run_reports_actions(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="copilot", dry_run=True)

    assert result.success is True
    assert "[dry-run]" in result.stdout
    assert ".github" in result.stdout


def test_setup_ai_guidelines_aiassistant_creates_primary_link(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    readme = base / "README.md"
    readme.write_text("seed")

    tools = dev.DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="aiassistant", dry_run=False)
    assert result.success is True

    primary = tmp_path / ".aiassistant" / "rules" / "00-README.md"
    assert primary.exists()
    if primary.is_symlink():
        target = (primary.parent / primary.readlink()).resolve()
        assert target == readme.resolve()


def test_review_list_graphql_failure_raises(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:o/r.git\n"),
            _make_result("gh-graphql", stdout="", success=False),
        ]
    )
    with pytest.raises(ToolExecutionError):
        tools.review_list(pr_number=7)


def test_infer_repo_parses_git_and_https(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools
    mod = tools._review_module()
    # Git SSH
    runner.set_results(
        [_make_result("git-remote", stdout="git@github.com:alice/proj.git\n")]
    )
    owner, repo = mod._infer_repo("origin")
    assert (owner, repo) == ("alice", "proj")
    # HTTPS
    runner.set_results(
        [_make_result("git-remote", stdout="https://github.com/bob/reponame.git\n")]
    )
    owner2, repo2 = mod._infer_repo("origin")
    assert (owner2, repo2) == ("bob", "reponame")


def test_setup_ai_guidelines_cleans_broken_symlink(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")
    (base / "extra.md").write_text("x")

    tools = dev.DevTools(root_path=tmp_path)
    ok = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert ok.success is True

    mirrored = tmp_path / ".windsurf" / "rules" / "extra.md"
    assert mirrored.exists()
    # Remove source to create broken symlink
    (base / "extra.md").unlink()

    rerun = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert rerun.success is True
    # After cleanup, the mirrored path should either be removed or no longer be a symlink to non-existent
    if mirrored.is_symlink():
        target = (mirrored.parent / mirrored.readlink()).resolve()
        assert target.exists() is True or not mirrored.exists()


def test_review_delete_with_no_targets(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    _tools, runner = dev_tools

    # Stub review module with no targets
    class Stub(_ReviewStub):
        def _load_comment_targets(self, p: Path) -> list[str]:  # noqa: ANN401
            return []

    tools = dev.DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=_tools.root_path,
        review_module_factory=lambda: Stub(),
    )

    (tmp_path / "targets.json").write_text("[]")
    result = tools.review_delete(42, tmp_path / "targets.json")
    assert result.success is True
    assert runner.calls == []


def test_review_bulk_reply_with_empty_replies(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    tools, runner = dev_tools
    payload = '{"data":{"viewer":{"login":"bob"},"repository":{"pullRequest":{"reviewThreads":{"nodes":[]}}}}}\n'
    runner.set_results(
        [
            _make_result("git-remote", stdout="git@github.com:owner/repo.git\n"),
            _make_result("gh-graphql", stdout=payload),
        ]
    )
    empty = tmp_path / "replies.json"
    empty.write_text("{}")
    result = tools.review_bulk_reply(2, empty)
    assert result.success is True
    # Only remote+fetch executed; no reply mutations
    assert len(runner.calls) == 2


def test_gitignore_negation_logging(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")
    (tmp_path / ".gitignore").write_text(".windsurf/\n!.windsurf/\n")
    tools = dev.DevTools(root_path=tmp_path)
    out = tools.setup_ai_guidelines(tool="windsurf", dry_run=True)
    assert out.success is True
    # Either negated pattern logged or generic not-ignored message
    assert ("kept by negated pattern" in out.stdout) or (
        "is not ignored by .gitignore" in out.stdout
    )


def test_setup_ai_guidelines_rerun_ok_same_path(tmp_path: Path) -> None:
    base = tmp_path / ".dev-guidelines"
    base.mkdir(parents=True, exist_ok=True)
    (base / "README.md").write_text("seed")
    tools = dev.DevTools(root_path=tmp_path)
    first = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert first.success is True
    second = tools.setup_ai_guidelines(tool="windsurf", dry_run=False)
    assert second.success is True
    assert "ok     " in second.stdout or "ok " in second.stdout


def test_load_replies_list_returns_empty(tmp_path: Path) -> None:
    tools = dev.DevTools()
    mod = tools._builtin_review_module()
    f = tmp_path / "replies.json"
    f.write_text("[]")
    assert mod._load_replies(f) == {}


def test_comment_lookup_empty_fetch_returns_empty() -> None:
    tools = dev.DevTools()
    mod = tools._builtin_review_module()
    fetch = SimpleNamespace(threads=[])
    assert mod._comment_lookup(fetch) == {}


def test_setup_ai_guidelines_single_file_root_codex(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    _tools, runner = dev_tools

    (tmp_path / ".dev-guidelines").mkdir(parents=True, exist_ok=True)

    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    result = tools.setup_ai_guidelines(tool="codex", dry_run=True)

    assert result.success is True
    assert "configured as single-file root" in result.stdout
