from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev.review import (
    ReviewModule,
    run_review_list,
    run_review_bulk_reply,
    run_review_delete,
)
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_infer_repo_git_remote_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="https://github.com/owner/repo.git\n",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    review = ReviewModule(runner, tmp_path)
    owner, repo = review.infer_repo("origin")
    assert owner == "owner"
    assert repo == "repo"


def test_infer_repo_gh_fallback_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    # git remote fails
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="error",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    # gh repo view succeeds
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="owner/repo\n",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    review = ReviewModule(runner, tmp_path)
    owner, repo = review.infer_repo("origin")
    assert owner == "owner"
    assert repo == "repo"


def test_infer_repo_all_fail(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    review = ReviewModule(runner, tmp_path)
    with pytest.raises(ToolExecutionError, match="Failed to infer repository"):
        review.infer_repo("origin")


def test_fetch_review_threads_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    data = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "isResolved": False,
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": "them"},
                                            "body": "comment body",
                                            "url": "http://url",
                                            "id": "MD123",
                                            "databaseId": 123,
                                            "createdAt": "2023-01-01",
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
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(data),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )
    review = ReviewModule(runner, tmp_path)
    result = review.fetch_review_threads("owner", "repo", 1)
    assert result.viewer == "me"
    assert len(result.threads) == 1
    assert result.threads[0].comments[0].body == "comment body"


def test_run_review_list_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    # 1. infer repo
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="https://github.com/o/r.git\n",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    # 2. fetch threads
    data = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}},
        }
    }
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(data),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )

    result = run_review_list(1, "origin", False, False, runner, tmp_path)
    assert result.success is True
    assert "No matching review threads found" in result.stdout


def test_run_review_bulk_reply_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    # 1. infer repo
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="https://github.com/o/r.git\n",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    # 2. fetch threads
    data = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "C_1",
                                            "url": "u",
                                            "body": "b",
                                            "author": {"login": "me"},
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            },
        }
    }
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(data),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )
    # 3. reply
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="{}",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-reply-gql"
            ),
        )
    )

    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"C_1": "reply"}))

    result = run_review_bulk_reply(1, replies_file, "origin", runner, tmp_path)
    assert result.success is True
    assert "Successfully sent bulk replies" in result.stdout


def test_bulk_reply_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    # Mock comment lookup via fetch result structure
    # fetch result is Any, we can mock it
    class MockFetch:
        threads = [
            type(
                "Thread",
                (),
                {
                    "comments": [
                        type(
                            "Comment",
                            (),
                            {"id": "C_1", "url": "http://url", "database_id": 1},
                        )()
                    ]
                },
            )()
        ]

    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="{}",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-reply-gql"
            ),
        )
    )

    review = ReviewModule(runner, tmp_path)
    replies = {"C_1": "reply body"}
    review.bulk_reply(fetch=MockFetch(), replies=replies)

    assert len(runner.calls) == 1
    assert "inReplyTo=C_1" in runner.calls[0]["command"]


def test_run_review_delete_success(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    # 1. infer repo (git remote)
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="https://github.com/o/r.git\n",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )
    # 2. fetch threads
    data = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "C_ToDel",
                                            "url": "u",
                                            "body": "b",
                                            "author": {"login": "me"},
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            },
        }
    }
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(data),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )

    # 3. delete comment
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="{}",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-delete"
            ),
        )
    )

    comments_file = tmp_path / "comments.json"
    comments_file.write_text('["C_ToDel"]')

    result = run_review_delete(1, comments_file, "origin", runner, tmp_path)

    assert result.success is True
    assert "deleted 1 comments" in result.stdout
