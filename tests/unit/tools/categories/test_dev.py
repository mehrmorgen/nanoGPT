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


def test_review_list_missing_module_raises(
    dev_tools: tuple[dev.DevTools, FakeSubprocessRunner],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tools, _ = dev_tools

    call_count = {"count": 0}

    def fake_import(_module: str) -> object:
        call_count["count"] += 1
        raise ModuleNotFoundError("scripts.review not found")

    monkeypatch.setattr(dev.importlib, "import_module", fake_import)

    with pytest.raises(ToolExecutionError) as excinfo:
        tools.review_list(pr_number=1)

    assert "Review helpers unavailable" in str(excinfo.value)
    assert call_count["count"] == 1


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
