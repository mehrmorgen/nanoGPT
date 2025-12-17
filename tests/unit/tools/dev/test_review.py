from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

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
    run_review_resolve,
)
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_run_review_bulk_reply_reraises_tool_execution_error(tmp_path: Path) -> None:
    class ExplodingReview:
        def infer_repo(self, remote: str) -> tuple[str, str]:
            raise ToolExecutionError("boom", reason="fail", rationale="test")

    def factory() -> object:
        return ExplodingReview()

    with pytest.raises(ToolExecutionError, match="boom"):
        run_review_bulk_reply(
            pr_number=1,
            replies_file=tmp_path / "replies.json",
            remote="origin",
            subprocess_runner=FakeSubprocessRunner(),
            root_path=tmp_path,
            review_module_factory=factory,
        )


def test_run_review_delete_reraises_tool_execution_error(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    # git remote fails
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
    # gh repo view fails -> infer_repo raises ToolExecutionError
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="no gh",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
    )

    comments_file = tmp_path / "comments.json"
    comments_file.write_text("[]")

    with pytest.raises(ToolExecutionError, match="Failed to infer repository"):
        run_review_delete(1, comments_file, "origin", runner, tmp_path)


def test_run_review_delete_skips_unknown_targets(tmp_path: Path) -> None:
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
                                            "url": "http://c/1",
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

    comments_file = tmp_path / "comments.json"
    comments_file.write_text('["UNKNOWN"]')

    result = run_review_delete(7, comments_file, "origin", runner, tmp_path)
    assert result.success is True


def test_bulk_resolve_executes_graphql_mutation(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    # resolve call
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(
                {"data": {"resolveReviewThread": {"thread": {"id": "TH1"}}}}
            ),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-resolve-gql"
            ),
        )
    )

    review = ReviewModule(runner, tmp_path)
    fetch = ReviewFetchResult(
        threads=[
            ReviewThread(
                id="TH1",
                url="http://t",
                is_resolved=False,
                comments=[
                    ReviewComment(
                        author="a",
                        viewer_did_author=False,
                        body="b",
                        url="http://example#comment-1",
                        id="C_1",
                        database_id=None,
                        created_at=None,
                    )
                ],
            )
        ],
        viewer=None,
    )

    review.bulk_resolve(fetch=fetch, targets=["http://example#comment-1"])

    assert runner.calls
    cmd = runner.calls[0]["command"]
    assert cmd[:3] == ["gh", "api", "graphql"]


def test_run_review_resolve_happy_path(tmp_path: Path) -> None:
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
                                "isResolved": False,
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": "me"},
                                            "body": "body",
                                            "url": "http://c/1",
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
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout=json.dumps(
                {"data": {"resolveReviewThread": {"thread": {"id": "TH1"}}}}
            ),
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-resolve-gql"
            ),
        )
    )

    threads_file = tmp_path / "threads.json"
    threads_file.write_text('["http://c/1"]', encoding="utf-8")

    result = run_review_resolve(
        pr_number=1,
        threads_file=threads_file,
        remote="origin",
        subprocess_runner=runner,
        root_path=tmp_path,
    )

    assert result.success is True


def test_thread_lookup_adds_url_fragment_key(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    fetch_result = ReviewFetchResult(
        threads=[
            ReviewThread(
                id="TH_1",
                url="http://t",
                is_resolved=False,
                comments=[
                    ReviewComment(
                        author="me",
                        viewer_did_author=False,
                        body="b",
                        url="http://example#comment-1",
                        id="C_1",
                        database_id=None,
                        created_at=None,
                    )
                ],
            )
        ],
        viewer=None,
    )

    mapping = review.thread_lookup(fetch_result)

    assert mapping["http://example#comment-1"] == "TH_1"
    assert mapping["comment-1"] == "TH_1"


def test_thread_lookup_skips_missing_url_and_missing_id(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    class _Comment:
        def __init__(self, *, cid: str | None, url: str | None) -> None:
            self.id = cid
            self.url = url

    class _Thread:
        def __init__(self) -> None:
            self.id = "TH_1"
            self.comments = [
                _Comment(cid="C_1", url=None),
                _Comment(cid=None, url="http://example"),
            ]

    fetch = SimpleNamespace(threads=[_Thread()])
    mapping = review.thread_lookup(fetch)

    assert mapping["C_1"] == "TH_1"
    assert mapping["http://example"] == "TH_1"
    assert "example" not in mapping


def test_comment_lookup_adds_database_id_and_url_fragment(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    fetch_result = ReviewFetchResult(
        threads=[
            ReviewThread(
                id="TH_1",
                url="http://t",
                is_resolved=False,
                comments=[
                    ReviewComment(
                        author="me",
                        viewer_did_author=False,
                        body="b",
                        url="http://example#comment-2",
                        id="C_2",
                        database_id=22,
                        created_at=None,
                    )
                ],
            )
        ],
        viewer=None,
    )

    mapping = review.comment_lookup(fetch_result)

    assert mapping["C_2"] == "C_2"
    assert mapping["http://example#comment-2"] == "C_2"
    assert mapping["comment-2"] == "C_2"
    assert mapping["22"] == "C_2"


def test_fetch_review_threads_handles_non_dict_root(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="[]",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-fetch"
            ),
        )
    )

    review = ReviewModule(runner, tmp_path)
    fetch = review.fetch_review_threads(owner="o", repo="r", pr_number=1)

    assert fetch.threads == []


def test_thread_lookup_skips_threads_without_id(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)
    fetch = SimpleNamespace(threads=[SimpleNamespace(id="", comments=[])])
    assert review.thread_lookup(fetch) == {}


def test_comment_lookup_skips_missing_fields_and_no_fragment(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment_missing = SimpleNamespace(id=None, url=None, database_id=None)
    comment_url_no_fragment = SimpleNamespace(
        id=None,
        url="http://example",
        database_id=None,
    )
    thread = SimpleNamespace(
        id="TH", comments=[comment_missing, comment_url_no_fragment]
    )
    fetch = SimpleNamespace(threads=[thread])

    mapping = review.comment_lookup(fetch)

    assert mapping["http://example"] is None


def test_load_comment_targets_non_list_returns_empty(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    path = tmp_path / "targets.json"
    path.write_text("{}", encoding="utf-8")

    assert review.load_comment_targets(path) == []


def test_load_comment_targets_filters_non_strings(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    path = tmp_path / "targets.json"
    path.write_text('["ok", 123, null]', encoding="utf-8")

    assert review.load_comment_targets(path) == ["ok"]


def test_load_comment_targets_multiple_strings_hits_loop_backedge(
    tmp_path: Path,
) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    path = tmp_path / "targets.json"
    path.write_text('["a", "b"]', encoding="utf-8")

    assert review.load_comment_targets(path) == ["a", "b"]


def test_comment_lookup_id_url_fragment_and_database_id(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment = SimpleNamespace(
        id="C_1",
        url="http://example#comment-1",
        database_id=11,
    )
    thread = SimpleNamespace(id="TH", comments=[comment])
    fetch = SimpleNamespace(threads=[thread])

    mapping = review.comment_lookup(fetch)

    assert mapping["C_1"] == "C_1"
    assert mapping["http://example#comment-1"] == "C_1"
    assert mapping["comment-1"] == "C_1"
    assert mapping["11"] == "C_1"


def test_comment_lookup_database_id_when_url_falsy(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment = SimpleNamespace(
        id=None,
        url="",
        database_id=22,
    )
    fetch = SimpleNamespace(threads=[SimpleNamespace(id="TH", comments=[comment])])

    mapping = review.comment_lookup(fetch)
    assert mapping["22"] is None


def test_comment_lookup_skips_url_branch_when_url_missing(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment = SimpleNamespace(id="C_9", url=None, database_id=None)
    fetch = SimpleNamespace(threads=[SimpleNamespace(id="TH", comments=[comment])])

    mapping = review.comment_lookup(fetch)
    assert mapping["C_9"] == "C_9"


def test_comment_lookup_handles_falsy_id_empty_string(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment = SimpleNamespace(id="", url="http://example#f", database_id=None)
    fetch = SimpleNamespace(threads=[SimpleNamespace(id="TH", comments=[comment])])

    mapping = review.comment_lookup(fetch)

    assert mapping["http://example#f"] == ""
    assert mapping["f"] == ""


def test_comment_lookup_handles_falsy_url_empty_string_with_id(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)

    comment = SimpleNamespace(id="C_X", url="", database_id=33)
    fetch = SimpleNamespace(threads=[SimpleNamespace(id="TH", comments=[comment])])

    mapping = review.comment_lookup(fetch)

    assert mapping["C_X"] == "C_X"
    assert mapping["33"] == "C_X"


def test_render_threads_formats_empty_and_multiline_bodies(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)
    threads = [
        ReviewThread(
            id="TH",
            url="http://t",
            is_resolved=False,
            comments=[
                ReviewComment(
                    author="me",
                    viewer_did_author=True,
                    body="",
                    url="http://c",
                    id="CID",
                ),
                ReviewComment(
                    author="them",
                    viewer_did_author=False,
                    body="line1\nline2",
                    url="http://c2",
                    id="CID2",
                ),
            ],
        )
    ]

    lines = review.render_threads(
        threads,
        apply_filters=review.apply_filters,
        unreplied=False,
        unresolved=False,
        viewer="me",
    )

    assert any("(viewer)" in line for line in lines)
    assert any("<no content>" in line for line in lines)
    assert any("line2" in line for line in lines)


def test_render_threads_no_matches_message(tmp_path: Path) -> None:
    review = ReviewModule(FakeSubprocessRunner(), tmp_path)
    lines = review.render_threads(
        [],
        apply_filters=review.apply_filters,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert lines == ["No matching review threads found."]


def test_bulk_reply_handles_empty_replies_and_http_fragment_lookup(
    tmp_path: Path,
) -> None:
    runner = FakeSubprocessRunner()
    review = ReviewModule(runner, tmp_path)

    fetch = ReviewFetchResult(
        viewer="me",
        threads=[
            ReviewThread(
                id="TH_1",
                url="http://t",
                is_resolved=False,
                comments=[
                    ReviewComment(
                        author="me",
                        viewer_did_author=True,
                        body="b",
                        url="",
                        id="CID",
                    )
                ],
            )
        ],
    )

    review.bulk_reply(fetch=fetch, replies={})
    assert runner.calls == []

    # Unknown key should be skipped
    review.bulk_reply(fetch=fetch, replies={"http://x#NOPE": "body"})
    assert runner.calls == []

    # http fragment fallback resolves to comment id -> thread id
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="nope",
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-reply-gql"
            ),
        )
    )
    with pytest.raises(ToolExecutionError, match="Failed to send reply"):
        review.bulk_reply(fetch=fetch, replies={"http://x#CID": "body"})


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
