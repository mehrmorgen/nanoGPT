from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from pathlib import Path
from typing import Any, Iterable, Optional

from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev.dev import (
    run_cleanup_ignored_tracked,
    run_review_delete,
    run_review_list,
)
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


class _StubRunner(SubprocessRunner):
    def __init__(self, results: Iterable[ToolResult]) -> None:
        self._results = list(results)
        self.calls: list[tuple[list[str], str | Path | None, OperationId]] = []

    def run_subprocess(
        self,
        command: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        self.calls.append((command, cwd, operation_id))
        return self._results.pop(0)

    def run_uv_command(
        self,
        args: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        python: str | None = None,
        no_project: bool = False,
    ) -> ToolResult:
        return self.run_subprocess(
            args, cwd=cwd, env=env, timeout=timeout, operation_id=operation_id
        )

    def run_pytest_command(
        self,
        args: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
    ) -> ToolResult:
        return self.run_subprocess(
            args, cwd=cwd, env=env, timeout=timeout, operation_id=operation_id
        )


def _success(command: str, stdout: str = "") -> ToolResult:
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace="tools",
        category="dev",
        command=command,
        stdout=stdout,
    )


@dataclass
class _ReviewFetch:
    threads: list[SimpleNamespace]
    viewer: Optional[str] = None


def _make_fetch(viewer: str | None = None) -> _ReviewFetch:
    comments = [
        SimpleNamespace(
            author="alice",
            viewer_did_author=True,
            body="hi",
            url="u1",
        )
    ]
    return _ReviewFetch(
        threads=[
            SimpleNamespace(url="u1", is_resolved=False, comments=comments),
        ],
        viewer=viewer,
    )


class _FakeReviewList:
    def __init__(self) -> None:
        self.applied: dict[str, Any] = {}

    def _infer_repo(self, remote: str) -> tuple[str, str]:
        self.applied["remote"] = remote
        return ("owner", "repo")

    def fetch_review_threads(
        self, owner: str, repo: str, pr_number: int
    ) -> _ReviewFetch:
        self.applied["owner"] = owner
        self.applied["repo"] = repo
        self.applied["pr_number"] = pr_number
        return _make_fetch(viewer="alice")

    def apply_filters(
        self,
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ):
        self.applied["unreplied"] = unreplied
        self.applied["unresolved"] = unresolved
        self.applied["viewer"] = viewer
        return list(threads)


class _FakeReviewDelete:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def _infer_repo(self, remote: str) -> tuple[str, str]:
        return ("owner", "repo")

    def fetch_review_threads(
        self, owner: str, repo: str, pr_number: int
    ) -> _ReviewFetch:
        return _make_fetch()

    def _load_comment_targets(self, comments_file: Path) -> list[str]:
        self.calls.append(f"load:{comments_file}")
        return ["t1"]

    def _comment_lookup(self, fetch: _ReviewFetch) -> dict[str, str]:
        self.calls.append(f"lookup:{len(fetch.threads)}")
        return {"t1": "cid-1"}


def test_run_review_list_success(tmp_path: Path) -> None:
    fake = _FakeReviewList()
    result = run_review_list(
        pr_number=5,
        unreplied=True,
        unresolved=False,
        remote="origin",
        subprocess_runner=_StubRunner([]),
        root_path=tmp_path,
        review_module_factory=lambda: fake,
    )
    assert result.success is True
    assert "Thread: u1" in result.stdout
    assert fake.applied["remote"] == "origin"
    assert fake.applied["pr_number"] == 5
    assert fake.applied["unreplied"] is True
    assert fake.applied["unresolved"] is False
    assert fake.applied["viewer"] == "alice"


def test_run_review_list_failure(tmp_path: Path) -> None:
    result = run_review_list(
        pr_number=1,
        unreplied=False,
        unresolved=False,
        remote="origin",
        subprocess_runner=_StubRunner([]),
        root_path=tmp_path,
        review_module_factory=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert result.success is False
    assert result.exit_code == 1
    assert "boom" in result.stderr


def test_run_review_delete_success(tmp_path: Path) -> None:
    runner = _StubRunner([_success("delete")])
    fake = _FakeReviewDelete()

    result = run_review_delete(
        pr_number=3,
        comments_file=tmp_path / "comments.json",
        remote="origin",
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: fake,
    )

    assert result.success is True
    assert result.stdout.endswith("PR #3")
    assert runner.calls  # deletion attempted
    assert fake.calls == [
        f"load:{tmp_path / 'comments.json'}",
        "lookup:1",
    ]


def test_run_cleanup_ignored_tracked_happy_path(tmp_path: Path) -> None:
    listing = _success("list", stdout="a.txt\nb.txt\n")
    removals = [
        _success("rm-a"),
        _success("rm-b"),
    ]
    runner = _StubRunner([listing, *removals])
    result = run_cleanup_ignored_tracked(
        subprocess_runner=runner,
        root_path=tmp_path,
    )
    assert result.success is True
    assert "Removed 2 ignored tracked files" in result.stdout
    assert len(runner.calls) == 3
