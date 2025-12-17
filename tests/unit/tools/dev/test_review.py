from __future__ import annotations

import json
from pathlib import Path

import pytest

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev.review import (
    ReviewModule,
    ReviewFetchResult,
    ReviewThread,
    ReviewComment,
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
    data: dict[str, object] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "id": "TH_1",
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
    data: dict[str, object] = {
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
                                "id": "TH_1",
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "C_1",
                                            "url": "u",
                                            "body": "b",
                                            "author": {"login": "me"},
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
    fetch_result = ReviewFetchResult(
        threads=[
            ReviewThread(
                id="TH_1",
                url="http://url",
                is_resolved=False,
                comments=[
                    ReviewComment(
                        author="me",
                        viewer_did_author=True,
                        body="comment",
                        url="http://url",
                        id="C_1",
                        database_id=1,
                        created_at="2023-01-01",
                    )
                ],
            )
        ],
        viewer="me",
    )

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
    review.bulk_reply(fetch=fetch_result, replies=replies)

    assert len(runner.calls) == 1
    assert "threadId=TH_1" in runner.calls[0]["command"]


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
                                "id": "TH_ToDel",
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "C_ToDel",
                                            "url": "u",
                                            "body": "b",
                                            "author": {"login": "me"},
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


def test_run_review_bulk_reply_value_error(tmp_path: Path) -> None:
    """ValueError during bulk reply should return error ToolResult."""

    class StubReview:
        def infer_repo(self, remote: str) -> tuple[str, str]:
            assert remote == "origin"
            return "o", "r"

        def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> object:
            assert (owner, repo, pr_number) == ("o", "r", 5)
            return object()

        def load_replies(self, replies_file: Path) -> dict[str, str]:
            assert replies_file.name == "replies.json"
            return {"id": "body"}

        def bulk_reply(self, *, fetch: object, replies: dict[str, str]) -> None:
            raise ValueError("boom")

    def factory() -> object:
        return StubReview()

    result = run_review_bulk_reply(
        pr_number=5,
        replies_file=tmp_path / "replies.json",
        remote="origin",
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=factory,
    )

    assert result.success is False
    assert "Failed to send bulk replies" in (result.stderr or "")


def test_run_review_delete_returns_failed_deletion(tmp_path: Path) -> None:
    """run_review_delete should propagate deletion failure."""
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
    payload: dict[str, object] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "id": "TH1",
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": "me"},
                                            "body": "body",
                                            "url": "http://c/1#frag",
                                            "id": "C1",
                                            "databaseId": 99,
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
            stdout=json.dumps(payload),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )
    deletion_fail = ToolResult(
        success=False,
        exit_code=1,
        stdout="",
        stderr="delete failed",
        operation_id=OperationId(namespace="tools", category="dev", command="review"),
    )
    runner.add_result(deletion_fail)

    comments_file = tmp_path / "comments.json"
    comments_file.write_text('["C1"]')

    result = run_review_delete(
        pr_number=7,
        comments_file=comments_file,
        remote="origin",
        subprocess_runner=runner,
        root_path=tmp_path,
    )

    assert result is deletion_fail


def test_comment_lookup_maps_all_identifiers(tmp_path: Path) -> None:
    """comment_lookup should map id, url fragment, and database_id."""
    runner = FakeSubprocessRunner()
    review = ReviewModule(runner, tmp_path)

    thread = ReviewThread(
        id="TH",
        url="http://url/path",
        is_resolved=False,
        comments=[
            ReviewComment(
                author="me",
                viewer_did_author=True,
                body="b",
                url="http://url/path#frag",
                id="CID",
                database_id=123,
                created_at="2023-01-01",
            )
        ],
    )

    mapping = review.comment_lookup(ReviewFetchResult(viewer="me", threads=[thread]))

    assert mapping["CID"] == "CID"
    assert mapping["http://url/path#frag"] == "CID"
    assert mapping["frag"] == "CID"
    assert mapping["123"] == "CID"
