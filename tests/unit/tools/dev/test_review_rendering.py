from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId
from ml_playground.tools.dev.dev import DevTools

from tests.unit.tools.fakes import FakeSubprocessRunner, create_success_result


class ReviewStub:
    def __init__(self, body: str, viewer: str = "bob") -> None:
        self._body = body
        self.viewer = viewer
        self.filters_called_with: dict[str, object] | None = None

    def infer_repo(self, remote: str) -> tuple[str, str]:
        assert remote == "origin"
        return "owner", "repo"

    def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> object:
        assert pr_number == 42
        comment = SimpleNamespace(
            author="mentor", viewer_did_author=False, body=self._body
        )
        thread = SimpleNamespace(
            url="https://example/review/verbose", is_resolved=False, comments=[comment]
        )
        return SimpleNamespace(threads=[thread], viewer=self.viewer)

    def apply_filters(
        self,
        threads: list[object],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[object]:
        self.filters_called_with = {
            "threads": threads,
            "unreplied": unreplied,
            "unresolved": unresolved,
            "viewer": viewer,
        }
        return threads


@pytest.fixture()
def dev_tools(tmp_path: Path) -> tuple[DevTools, FakeSubprocessRunner]:
    runner = FakeSubprocessRunner()
    tools = DevTools(config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path)
    return tools, runner


def test_review_list_renders_verbose_body(
    dev_tools: tuple[DevTools, FakeSubprocessRunner],
) -> None:
    tools, runner = dev_tools
    body = (
        "First line of comment that exceeds any truncation threshold by being long. "
        "Second sentence continues with more details to ensure we keep everything.\n"
        "Second line with additional guidance."
    )

    stub = ReviewStub(body)
    tools = DevTools(
        config=ToolsConfig(),
        subprocess_runner=runner,
        root_path=tools.root_path,
        review_module_factory=lambda: stub,
    )

    result = tools.review_list(pr_number=42)

    assert body.split("\n")[0] in result.stdout
    assert "Second line with additional guidance." in result.stdout
    assert stub.filters_called_with is not None
    assert stub.filters_called_with["viewer"] == "bob"


def test_review_list_uses_builtin_module(
    dev_tools: tuple[DevTools, FakeSubprocessRunner], tmp_path: Path
) -> None:
    tools, runner = dev_tools
    payload = {
        "data": {
            "viewer": {"login": "bob"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "isResolved": False,
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": "alice"},
                                            "body": "Looks good",
                                            "url": "https://example/review/1#discussion_r1",
                                            "id": "C_xyz",
                                            "databaseId": 1,
                                            "createdAt": "2025-01-01T00:00:00Z",
                                        }
                                    ]
                                },
                            }
                        ]
                    }
                }
            },
        }
    }
    payload_json = json.dumps(payload)
    runner.set_results(
        [
            create_success_result(
                OperationId(
                    namespace="tools", category="dev", command="review-infer-repo"
                ),
                stdout="git@github.com:owner/repo.git\n",
            ),
            create_success_result(
                OperationId(namespace="tools", category="dev", command="review-fetch"),
                stdout=payload_json,
            ),
        ]
    )

    result = tools.review_list(pr_number=42, unreplied=True, unresolved=False)

    assert "Thread:" in result.stdout
    assert runner.calls[0]["command"][:4] == ["git", "remote", "get-url", "origin"]
    assert runner.calls[1]["command"][0:3] == ["gh", "api", "graphql"]
