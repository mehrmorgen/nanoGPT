"""Shared fixtures for dev tools unit tests."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

from ml_playground.tools.dev import dev
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from tests.unit.tools.fakes import FakeSubprocessRunner


@pytest.fixture
def dev_tools(tmp_path: Path) -> tuple[dev.DevTools, FakeSubprocessRunner]:
    runner = FakeSubprocessRunner()
    tools = dev.DevTools(
        config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path
    )
    return tools, runner


def make_result(
    command: str, *, stdout: str = "", stderr: str = "", success: bool = True
) -> ToolResult:
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr=stderr if stderr else ("" if success else "error"),
        operation_id=OperationId(namespace="tools", category="dev", command=command),
    )


class ReviewStub:
    def __init__(self) -> None:
        self.filters_called_with: dict[str, object] | None = None
        self.bulk_called = False
        self.deleted_ids: list[str] = []

    @staticmethod
    def _infer_repo(remote: str) -> tuple[str, str]:
        assert remote == "origin"
        return ("owner", "repo")

    def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> object:
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
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[Any]:
        self.filters_called_with = {
            "threads": list(threads),
            "unreplied": unreplied,
            "unresolved": unresolved,
            "viewer": viewer,
        }
        return list(threads)

    def _load_replies(self, replies_file: Path) -> dict[str, str]:
        assert replies_file.name == "replies.json"
        return {"id": "reply"}

    def _bulk_reply(self, *, fetch: object, replies: dict[str, str]) -> None:
        assert getattr(fetch, "threads", None) is not None
        assert replies == {"id": "reply"}
        self.bulk_called = True

    def _load_comment_targets(self, path: Path) -> list[str]:
        assert path.name == "targets.json"
        return ["c1", "c2"]

    def _comment_lookup(self, fetch: object) -> dict[str, str]:
        assert getattr(fetch, "threads", None) is not None
        return {"c1": "comment-1", "c2": "comment-2"}
