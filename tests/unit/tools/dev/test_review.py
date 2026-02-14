from __future__ import annotations

import json
import pytest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult
import ml_playground.tools.dev.dev as dev_module
from ml_playground.tools.dev.dev import DevTools
from tests.unit.tools.fakes import FakeSubprocessRunner


def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    object.__setattr__(target, name, value)
    return original, original is not missing


def restore_attr(target: object, name: str, original: object, had_attr: bool) -> None:
    if had_attr:
        object.__setattr__(target, name, original)
    else:
        delattr(target, name)


def test_review_list_integrated_public(tmp_path: Path) -> None:
    """Exercise review_list and rendering via public DevTools API."""
    runner = FakeSubprocessRunner()

    # Payload with various conditions to hit branches:
    # 1. Comment with URL anchor (#discussion_r1) to hit anchor logic (430)
    # 2. Comment with empty body to hit empty body logic (1018-1020)
    # 3. Viewer login to hit viewer attribution
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
                                            "id": "C_1",
                                            "author": {"login": "alice"},
                                            "body": "  Line 1  \nLine 2",
                                            "url": "https://github.com/o/r/pull/1#discussion_r1",
                                            "databaseId": 101,
                                            "viewerDidAuthor": False,
                                        },
                                        {
                                            "id": "C_2",
                                            "author": {"login": "bob"},
                                            "body": "",
                                            "url": "https://github.com/o/r/pull/1#discussion_r2",
                                            "databaseId": 102,
                                            "viewerDidAuthor": True,
                                        },
                                    ]
                                },
                            }
                        ]
                    }
                }
            },
        }
    }

    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="git@github.com:owner/repo.git\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=42)

    assert result.success is True
    assert "alice:" in result.stdout
    assert "Line 1" in result.stdout
    assert "bob (viewer):" in result.stdout
    assert "<no content>" in result.stdout  # Confirms 1018-1020 hit


def test_review_list_no_data_public(tmp_path: Path) -> None:
    """Exercise branch where fetch returns no data."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=42)
    assert "No matching review threads found" in result.stdout


def test_review_list_fetch_empty_data_failure(tmp_path: Path) -> None:
    """When fetch returns empty data, run_review_list yields no threads."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=1)
    assert result.success is True
    assert "No matching review threads found" in result.stdout


def test_review_list_apply_filters_error(tmp_path: Path) -> None:
    """Unexpected apply_filters error is caught and returned as failure ToolResult."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            return SimpleNamespace(threads=[], viewer=None)

        def apply_filters(self, *args: Any, **kwargs: Any) -> list[Any]:
            raise RuntimeError("boom")

    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: ReviewModule(),
    )
    result = tools.review_list(pr_number=1)
    assert result.success is False
    assert "Failed to list review comments" in result.stderr


def test_review_list_missing_repo_nodes(tmp_path: Path) -> None:
    """Ensure missing repo/pr nodes are handled (return empty threads)."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=99)
    assert result.success is True
    assert "No matching review threads found" in result.stdout


def test_review_list_reviewthreads_shape_variants(tmp_path: Path) -> None:
    """Handle non-list reviewThreads nodes and empty comments gracefully."""
    runner = FakeSubprocessRunner()
    # Case 1: reviewThreads not a dict
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": null}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=5)
    assert result.success is True
    assert "No matching review threads found" in result.stdout

    # Case 2: nodes not a list, comments not a dict, comments empty
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(
                    {
                        "data": {
                            "viewer": {"login": "me"},
                            "repository": {
                                "pullRequest": {
                                    "reviewThreads": {
                                        "nodes": [
                                            {
                                                "isResolved": False,
                                                "comments": {"nodes": []},
                                            },
                                            {"isResolved": True, "comments": None},
                                            {
                                                "isResolved": False,
                                                "comments": {"nodes": "bad"},
                                            },
                                        ]
                                    }
                                }
                            },
                        }
                    }
                ),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    result2 = tools.review_list(pr_number=6)
    assert result2.success is True
    assert "No matching review threads found" in result2.stdout


def test_review_list_nodes_nonlist_and_bad_comments(tmp_path: Path) -> None:
    """Drive _fetch_review_threads branches for nodes non-list and bad comments."""
    runner = FakeSubprocessRunner()
    # nodes is non-list -> line 241
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": {"nodes": "oops"}}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    r1 = tools.review_list(pr_number=11)
    assert r1.success is True
    assert "No matching review threads found" in r1.stdout

    # comment_nodes list with non-dict to hit comment skip branch (264)
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(
                    {
                        "data": {
                            "viewer": {"login": "me"},
                            "repository": {
                                "pullRequest": {
                                    "reviewThreads": {
                                        "nodes": [
                                            {
                                                "isResolved": False,
                                                "comments": {"nodes": ["bad"]},
                                            }
                                        ]
                                    }
                                }
                            },
                        }
                    }
                ),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    r2 = tools.review_list(pr_number=12)
    assert r2.success is True
    assert "No matching review threads found" in r2.stdout


def test_review_list_filters_unreplied_unresolved(tmp_path: Path) -> None:
    """Exercise _apply_filters branches for unreplied and unresolved."""
    runner = FakeSubprocessRunner()
    payload = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "isResolved": True,
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": "me"},
                                            "body": "viewer comment",
                                            "url": "u1",
                                            "id": "c1",
                                            "databaseId": 1,
                                            "viewerDidAuthor": True,
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
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=13, unreplied=True, unresolved=True)
    assert result.success is True
    assert "No matching review threads found" in result.stdout


def test_review_list_unhandled_exception_to_toolresult(tmp_path: Path) -> None:
    """Generic exceptions in review module should return failure ToolResult."""
    runner = FakeSubprocessRunner()

    class Boom:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            raise ValueError("boom")

    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: Boom(),
    )
    result = tools.review_list(pr_number=14)
    assert result.success is False
    assert "Failed to list review comments" in result.stderr


def test_review_list_infer_repo_gh_fallback_failure(tmp_path: Path) -> None:
    """_infer_repo falls back to gh and surfaces failure via ToolResult."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=False,
                exit_code=1,
                stdout="",
                stderr="git remote failed",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stdout="",
                stderr="gh view failed",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=2)
    assert result.success is False
    assert "Failed to list review comments" in result.stderr


def test_review_bulk_reply_anchor_lookup_and_skip(tmp_path: Path) -> None:
    """_comment_lookup anchors and _bulk_reply skip branches via public API."""
    replies_file = tmp_path / "replies.json"
    # Only a missing key to force skip and error path.
    replies_file.write_text(json.dumps({"missing": "body2"}))

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            comments = [
                SimpleNamespace(
                    id="c1",
                    author="a",
                    viewer_did_author=False,
                    body="x",
                    url="http://x#discussion_r1",
                    database_id=10,
                    created_at=None,
                ),
            ]
            thread = SimpleNamespace(
                url="http://x", is_resolved=False, comments=comments
            )
            return SimpleNamespace(threads=[thread], viewer=None)

        def _load_replies(self, replies_file: Path) -> dict[str, str]:
            return json.loads(replies_file.read_text())

        def _bulk_reply(self, *, fetch: Any, replies: dict[str, str]) -> None:
            raise RuntimeError("no targets")

        def _comment_lookup(self, fetch: Any) -> dict[str, str]:
            return {}

    runner = FakeSubprocessRunner()
    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: ReviewModule(),
    )
    result = tools.review_bulk_reply(pr_number=3, replies_file=replies_file)
    assert result.success is False
    assert "Failed to send bulk replies" in result.stderr


def test_review_bulk_reply_default_module_gh_failure(tmp_path: Path) -> None:
    """Default module bulk reply should propagate gh failure and anchor lookup."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"discussion_r1": "hi"}))
    payload = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stdout="",
                stderr="gh api fail",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    with pytest.raises(Exception):
        tools.review_bulk_reply(pr_number=4, replies_file=replies_file)


def test_review_bulk_reply_invalid_replies_file(tmp_path: Path) -> None:
    """Invalid replies payload returns empty mapping but still succeeds."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text("[]")
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=5, replies_file=replies_file)
    assert result.success is True


def test_review_bulk_reply_invalid_mapping_entries(tmp_path: Path) -> None:
    """_load_replies should ignore non-str key/values and still succeed."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"valid": "ok", "bad": 1, 2: "no"}))
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=7, replies_file=replies_file)
    assert result.success is True


def test_review_bulk_reply_comment_lookup_with_anchors(tmp_path: Path) -> None:
    """Default comment_lookup should map id/url/anchor/databaseId."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"discussion_r1": "hi", "10": "db"}))
    payload = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=8, replies_file=replies_file)
    assert result.success is True


def test_review_bulk_reply_comment_lookup_with_bad_id_type(tmp_path: Path) -> None:
    """_comment_lookup should tolerate unhashable id values."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"missing": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": [],
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": None,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=9, replies_file=replies_file)
    assert result.success is True


def test_review_bulk_reply_http_anchor_lookup(tmp_path: Path) -> None:
    """HTTP reply key should fall back to anchor when id not found."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"http://x#discussion_r1": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=10, replies_file=replies_file)
    assert result.success is True
    assert len(runner.calls) == 3


def test_comment_lookup_anchor_direct() -> None:
    """Direct call to _comment_lookup should map anchor fragment (430-432)."""
    comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        id="cid",
        author="a",
        viewer_did_author=False,
        body="body",
        url="http://x#frag",
        database_id=None,
        created_at=None,
    )
    fetch = dev_module._FetchResult(  # type: ignore[reportPrivateUsage]
        threads=[
            dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u",
                is_resolved=False,
                comments=[comment],
            )
        ],
        viewer=None,
    )
    mapping = dev_module._comment_lookup(fetch)  # type: ignore[reportPrivateUsage]
    assert mapping["frag"] == "cid"
    assert mapping["http://x#frag"] == "cid"


def test_comment_lookup_anchor_empty_fragment() -> None:
    """_comment_lookup should skip empty anchor fragment (branch false path)."""
    comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        id="cid2",
        author="a",
        viewer_did_author=False,
        body="body",
        url="http://x#",
        database_id=None,
        created_at=None,
    )
    fetch = dev_module._FetchResult(  # type: ignore[reportPrivateUsage]
        threads=[
            dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u",
                is_resolved=False,
                comments=[comment],
            )
        ],
        viewer=None,
    )
    mapping = dev_module._comment_lookup(fetch)  # type: ignore[reportPrivateUsage]
    assert "http://x#" in mapping
    assert "" not in mapping


def test_review_bulk_reply_anchor_key_default_module(tmp_path: Path) -> None:
    """Anchor mapping via default review module (covers 430-432 in _comment_lookup)."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"frag": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": "cid",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#frag",
                                            "databaseId": None,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=17, replies_file=replies_file)
    assert result.success is True
    assert len(runner.calls) == 3


def test_review_bulk_reply_comment_lookup_bad_url_type(tmp_path: Path) -> None:
    """_comment_lookup should skip when url processing raises TypeError."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"c1": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": {"login": None},
                                            "body": None,
                                            "url": {"bad": True},
                                            "databaseId": None,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=10, replies_file=replies_file)
    assert result.success is True


def test_review_list_toolexecutionerror_propagation(tmp_path: Path) -> None:
    """ToolExecutionError bubbles through review_list handling."""
    from ml_playground.tools.core.errors import ToolExecutionError

    runner = FakeSubprocessRunner()

    class Boom:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            raise ToolExecutionError("fail", reason="r", rationale="rat")

    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: Boom(),
    )
    result = tools.review_list(pr_number=15)
    assert result.success is False
    assert "Failed to list review comments" in result.stderr


def test_review_bulk_reply_default_module_toolexecution_error(tmp_path: Path) -> None:
    """bulk_reply should propagate ToolExecutionError raised from gh call."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"discussion_r1": "hi"}))
    payload = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stdout="",
                stderr="gh api fail",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    with pytest.raises(Exception):
        tools.review_bulk_reply(pr_number=6, replies_file=replies_file)


def test_review_delete_success_and_failure(tmp_path: Path) -> None:
    """Exercise review_delete success path and gh deletion failure."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["c1", "c2"]))
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-delete",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stdout="",
                stderr="gh fail",
                namespace="tools",
                category="dev",
                command="review-delete",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    ok = tools.review_delete(pr_number=1, comments_file=comments_file)
    assert ok.success is True
    fail = tools.review_delete(pr_number=2, comments_file=comments_file)
    assert fail.success is False
    assert "Failed to delete comments" in fail.stderr


def test_review_list_infer_repo_failure_public(tmp_path: Path) -> None:
    """review_list should surface infer_repo ToolExecutionError via ToolResult."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="no remote",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="no gh",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=7)
    assert result.success is False
    assert "Failed to list review comments" in result.stderr


def test_review_list_invalid_json_public(tmp_path: Path) -> None:
    """review_list should report JSON parsing errors from fetch."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="{ not-json",
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=8)
    assert result.success is False
    assert "Failed to parse GitHub CLI output" in result.stderr


def test_review_module_factory_public(tmp_path: Path) -> None:
    """Exercise review_module_factory branch (952) via public API."""

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("o", "r")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            return SimpleNamespace(threads=[], viewer="me")

    tools = DevTools(root_path=tmp_path, review_module_factory=lambda: ReviewModule())
    tools.review_list(pr_number=1)

    # No exception raised and fetch_review_threads invoked via branch
    # (implicit through DevTools.review_list)


def test_review_bulk_reply_public(tmp_path: Path) -> None:
    """Exercise review_bulk_reply via public API."""
    runner = FakeSubprocessRunner()
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"discussion_r1": "Thanks!"}), encoding="utf-8")

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
                                            "id": "C_1",
                                            "author": {"login": "alice"},
                                            "body": "Fixed?",
                                            "url": "https://github.com/o/r/pull/1#discussion_r1",
                                            "databaseId": 101,
                                            "viewerDidAuthor": False,
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

    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":{}}',
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=42, replies_file=replies_file)

    assert result.success is True
    # Check if gh api was called with the correct comment ID
    calls = [
        c
        for c in runner.calls
        if "gh" in c["command"] and "addPullRequestReviewComment" in str(c["command"])
    ]
    assert len(calls) == 1
    assert "C_1" in str(calls[0]["command"])


def test_review_delete_public(tmp_path: Path) -> None:
    """Exercise review_delete via public API."""
    runner = FakeSubprocessRunner()
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["C_1"]), encoding="utf-8")

    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":{}}',
                namespace="tools",
                category="dev",
                command="review-delete",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_delete(pr_number=42, comments_file=comments_file)

    assert result.success is True
    assert len(runner.calls) == 2


def test_cleanup_ignored_tracked_public(tmp_path: Path) -> None:
    """Exercise cleanup_ignored_tracked via public API."""
    runner = FakeSubprocessRunner()
    # 1. Listing succeeds, 2. Removal succeeds
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ignored.txt\n",
                namespace="tools",
                category="dev",
                command="cleanup-ignored-tracked",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="",
                namespace="tools",
                category="dev",
                command="cleanup-ignored-tracked",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.cleanup_ignored_tracked()

    assert result.success is True
    assert "Removed 1" in result.stdout  # Corrected assertion string


def test_cleanup_ignored_tracked_failure_public(tmp_path: Path) -> None:
    """Exercise failure branches in run_cleanup_ignored_tracked (744-745)."""
    runner = FakeSubprocessRunner()
    # 1. Listing succeeds, 2. Removal fails
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="bad.txt\n",
                namespace="tools",
                category="dev",
                command="cleanup-ignored-tracked",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="perm denied",
                namespace="tools",
                category="dev",
                command="cleanup-ignored-tracked",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.cleanup_ignored_tracked()
    assert result.success is False
    assert "perm denied" in result.stderr


def test_workflow_status_public(tmp_path: Path) -> None:
    """Exercise workflow_status via public API."""
    tools = DevTools(root_path=tmp_path)
    result = tools.workflow_status()
    assert result.success is True
    assert '"quality_status"' in result.stdout


def test_batch_review_public(tmp_path: Path) -> None:
    """Exercise batch_review (824) via public API."""
    original, had = override_attr(
        dev_module,
        "run_dev_batch_review",
        lambda *args, **kwargs: ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="dev",
            command="batch-review",
            stdout="Batch ok",
        ),
    )
    try:
        tools = DevTools(root_path=tmp_path)
        result = tools.batch_review()
        assert result.success is True
        assert "Batch ok" in result.stdout
    finally:
        restore_attr(dev_module, "run_dev_batch_review", original, had)


def test_review_list_infer_repo_fallbacks_public(tmp_path: Path) -> None:
    """Exercise _infer_repo fallbacks via public review_list."""
    runner = FakeSubprocessRunner()
    # 1. git fails, 2. gh succeeds
    runner.set_results(
        [
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="no origin",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    tools.review_list(pr_number=1)
    # 3 calls: git, gh, fetch
    assert len(runner.calls) == 3


def test_fetch_review_threads_parsing_public(tmp_path: Path) -> None:
    """Exercise parsing logic in _fetch_review_threads via public API."""
    runner = FakeSubprocessRunner()

    # Payload with invalid types to hit branch parts
    payload = {
        "data": {
            "viewer": {"login": 123},  # not a string
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            None,  # not a dict
                            {
                                "isResolved": True,
                                "comments": {
                                    "nodes": [
                                        {
                                            "author": {"login": 456},
                                            "body": 789,
                                        }  # not strings
                                    ]
                                },
                            },
                        ]
                    }
                }
            },
        }
    }

    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="o/r",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=1)
    assert result.success is True


def test_review_delete_exception_handling_public(tmp_path: Path) -> None:
    """Exercise generic exception handling in review_delete via public API."""
    runner = FakeSubprocessRunner()
    comments_file = tmp_path / "del.json"
    comments_file.write_text(json.dumps(["C_1"]))

    # Generic exception branch (700) triggered if factory fails with non-ToolExecutionError
    def failing_factory():
        raise RuntimeError("generic fail")

    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=failing_factory,
    )
    result = tools.review_delete(pr_number=1, comments_file=comments_file)
    assert result.success is False
    assert "generic fail" in result.stderr


def test_dev_tools_missing_branches(tmp_path: Path) -> None:
    """Cover remaining dev.py branches (38, 51-53, 75-77, 132, 233, 237, 241, 310, 323, 327, 434, 466, 633) using ONLY public APIs where possible."""
    runner = FakeSubprocessRunner()
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)

    # 132: _infer_repo failure
    # Verify that repository inference failure results in a failed ToolResult via review_list.
    runner.set_results(
        [
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="no git",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="no gh",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
        ]
    )
    result_fail = tools.review_list(pr_number=1, remote="origin")
    assert result_fail.success is False
    assert "Failed to infer repository" in result_fail.stderr

    # 38, 51-53, 75-77, 310: logic branches
    # Using public APIs to exercise internal models/logic
    # We use a comment with specific body/author to trigger rendering and filtering
    payload_logic = {
        "data": {
            "viewer": {"login": "viewer_user"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "isResolved": True,
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "C_LOGIC",
                                            "author": {"login": "alice"},
                                            "body": "Logic test",
                                            "url": "https://github.com/o/r/pull/1#discussion_r100",
                                            "databaseId": 1001,
                                            "viewerDidAuthor": False,
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
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload_logic),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    # 310: unresolved=True should skip the resolved thread
    result_logic = tools.review_list(pr_number=1, unresolved=True)
    assert "No matching review threads found" in result_logic.stdout

    # 323, 327: _load_replies error handling via bulk_reply
    replies_bad = tmp_path / "bad_replies.json"
    replies_bad.write_text("invalid json")
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="o/r",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools.review_bulk_reply(
        pr_number=1, replies_file=replies_bad
    )  # returns success as it skips bad file

    replies_list = tmp_path / "list_replies.json"
    replies_list.write_text("[]")
    tools.review_bulk_reply(pr_number=1, replies_file=replies_list)  # returns success

    # 466: _bulk_reply skip missing comment_id
    replies_file = tmp_path / "replies_missing.json"
    replies_file.write_text('{"missing": "body"}')
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="o/r",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools.review_bulk_reply(
        pr_number=1, replies_file=replies_file
    )  # should not call gh api mutation

    # 633: run_review_bulk_reply ToolExecutionError
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="o/r",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="fetch fail",
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    result = tools.review_bulk_reply(pr_number=1, replies_file=replies_file)
    assert result.success is False
    assert "fetch fail" in result.stderr

    # Reset results for a successful retry path
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="o/r",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data":null}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    result = tools.review_bulk_reply(pr_number=1, replies_file=replies_file)
    assert result.success is True


def test_review_bulk_reply_anchor_lookup_public(tmp_path: Path) -> None:
    """Bulk reply should resolve anchors when full URL is unknown."""
    runner = FakeSubprocessRunner()
    replies_file = tmp_path / "replies_anchor.json"
    replies_file.write_text(
        json.dumps({"https://example.com/p#comment123": "pong"}), encoding="utf-8"
    )

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
                                            "id": "comment123",
                                            "author": {"login": "alice"},
                                            "body": "ok",
                                            "url": "",
                                            "databaseId": 5,
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

    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=9, replies_file=replies_file)
    assert result.success is True
    # Third call is the mutation (after infer + fetch)
    assert len(runner.calls) == 3
    assert "fetch fail" not in result.stderr


def test_review_list_toolexecutionerror_propagates(tmp_path: Path) -> None:
    """ToolExecutionError raised from apply_filters should propagate."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout='{"data": {"viewer": {"login": "me"}, "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}}',
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            return SimpleNamespace(threads=[], viewer=None)

        def apply_filters(self, *args: Any, **kwargs: Any) -> list[Any]:
            raise ToolExecutionError("boom", reason="boom", rationale="boom")

    tools = DevTools(
        subprocess_runner=runner,
        root_path=tmp_path,
        review_module_factory=lambda: ReviewModule(),
    )
    with pytest.raises(ToolExecutionError):
        tools.review_list(pr_number=1)


def test_review_delete_invalid_comments_file(tmp_path: Path) -> None:
    """Invalid comments json should be ignored and still succeed with zero deletions."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text("not json")
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {"reviewThreads": {"nodes": []}},
            },
        }
    }
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )

    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_delete(pr_number=1, comments_file=comments_file)
    assert result.success is True
    assert "Successfully deleted 0 comments" in result.stdout


def test_review_delete_fetch_toolexecutionerror(tmp_path: Path) -> None:
    """ToolExecutionError from fetch_review_threads should be converted to failure ToolResult."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["c1"]))

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            raise ToolExecutionError("nope", reason="nope", rationale="nope")

        def _load_comment_targets(self, path: Path) -> list[str]:
            return ["c1"]

        def _comment_lookup(self, fetch: Any) -> dict[str, str]:
            return {"c1": "c1"}

    tools = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=lambda: ReviewModule(),
    )
    result = tools.review_delete(pr_number=1, comments_file=comments_file)
    assert result.success is False
    assert "Failed to delete comments" in result.stderr


def test_review_bulk_reply_comment_lookup_exception_skip(tmp_path: Path) -> None:
    """_comment_lookup should swallow bad url types and still process others."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"good": "hi"}))

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(
            self, owner: str, repo: str, pr_number: int
        ) -> SimpleNamespace:
            comments = [
                SimpleNamespace(
                    id="good",
                    author="a",
                    viewer_did_author=False,
                    body="x",
                    url={"bad": True},  # triggers TypeError in _comment_lookup
                    database_id=None,
                    created_at=None,
                ),
                SimpleNamespace(
                    id="ok",
                    author="b",
                    viewer_did_author=False,
                    body="y",
                    url="http://x#anchor_ok",
                    database_id=2,
                    created_at=None,
                ),
            ]
            return SimpleNamespace(
                threads=[
                    SimpleNamespace(url="u", is_resolved=False, comments=comments)
                ],
                viewer=None,
            )

        def _load_replies(self, replies_file: Path) -> dict[str, str]:
            return json.loads(replies_file.read_text())

        def _comment_lookup(self, fetch: Any) -> dict[str, str]:
            return dev_module._comment_lookup(fetch)  # type: ignore[attr-defined,reportPrivateUsage]

        def _bulk_reply(self, *, fetch: Any, replies: dict[str, str]) -> None:
            # Only mapped ids should be processed; bad url comment is ignored
            assert replies == {"good": "hi"}

    tools = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=lambda: ReviewModule(),
    )
    result = tools.review_bulk_reply(pr_number=1, replies_file=replies_file)
    assert result.success is True


def test_comment_and_thread_eq_hash() -> None:
    """_Comment and _Thread equality/hash semantics."""
    comment_a = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="a",
        viewer_did_author=False,
        body="hi",
        url="u",
        id="1",
        database_id=2,
        created_at="now",
    )
    comment_b = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="a",
        viewer_did_author=False,
        body="hi",
        url="u",
        id="1",
        database_id=2,
        created_at="now",
    )
    comment_c = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="c",
        viewer_did_author=True,
        body="bye",
        url="u2",
        id="3",
        database_id=4,
        created_at=None,
    )

    # Equality and hashing
    assert comment_a == comment_b
    assert hash(comment_a) == hash(comment_b)
    assert comment_a != comment_c
    assert comment_a.__eq__(42) is NotImplemented

    thread_a = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="t",
        is_resolved=False,
        comments=[comment_a, comment_c],
    )
    thread_b = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="t",
        is_resolved=False,
        comments=[comment_b, comment_c],
    )
    thread_c = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="t2",
        is_resolved=True,
        comments=[comment_a],
    )

    assert thread_a == thread_b
    assert hash(thread_a) == hash(thread_b)
    assert thread_a != thread_c
    assert thread_a.__eq__("nope") is NotImplemented


def test_render_threads_tail_and_multiline(tmp_path: Path) -> None:
    """DevTools._render_threads should handle empty and multiline bodies."""
    tools = DevTools(root_path=tmp_path, review_module_factory=lambda: None)  # type: ignore[arg-type]
    render = tools._render_threads  # type: ignore[attr-defined]

    def empty_filter(
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[Any]:
        return []

    # Empty threads -> tail message
    lines_empty = render(
        [],
        apply_filters=empty_filter,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert lines_empty == ["No matching review threads found."]

    def passthrough_filter(
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[Any]:
        return list(threads)

    # Multiline body and empty body paths
    comment1 = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="me",
        viewer_did_author=True,
        body="Line1\nLine2",
        url="u1",
        id="c1",
        database_id=None,
        created_at=None,
    )
    comment2 = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="you",
        viewer_did_author=False,
        body="",
        url="u2",
        id="c2",
        database_id=None,
        created_at=None,
    )
    thread = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="thread-url", is_resolved=False, comments=[comment1, comment2]
    )

    lines = render(
        [thread],
        apply_filters=passthrough_filter,
        unreplied=False,
        unresolved=False,
        viewer="me",
    )
    assert "Thread: thread-url" in lines[0]
    assert "Line1" in "\n".join(lines)
    assert "<no content>" in "\n".join(lines)


def test_bulk_reply_empty_replies_skip(tmp_path: Path) -> None:
    """Empty replies mapping should short-circuit without gh mutation."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({}))
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {"reviewThreads": {"nodes": []}},
            },
        }
    }
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=11, replies_file=replies_file)
    assert result.success is True
    # Only infer + fetch should have been called
    assert len(runner.calls) == 2


def test_comment_lookup_exception_branch() -> None:
    """_comment_lookup should swallow exceptions on bad url/database_id."""
    fetch = SimpleNamespace(
        threads=[
            SimpleNamespace(
                comments=[
                    SimpleNamespace(
                        id=["cid"],  # unhashable to trigger exception before mapping
                        url={"bad": True},  # unhashable
                        database_id={"bad": True},  # unhashable
                        viewer_did_author=False,
                        author="a",
                        body="b",
                        created_at=None,
                    )
                ],
                url="u",
                is_resolved=False,
            )
        ]
    )
    mapping = dev_module._comment_lookup(fetch)  # type: ignore[reportPrivateUsage]
    # Mapping remains empty because the entry triggers the exception branch
    assert mapping == {}


def test_review_module_wrappers(tmp_path: Path) -> None:
    """DevTools._review_module should wire defaults correctly."""
    runner = FakeSubprocessRunner()
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {"nodes": []},
                }
            },
        }
    }
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    module = tools._review_module()  # type: ignore[attr-defined]
    owner, repo = module._infer_repo("origin")
    assert (owner, repo) == ("owner", "repo")
    fetch = module.fetch_review_threads(owner, repo, 1)
    assert fetch.threads == []
    assert (
        module.apply_filters([], unreplied=False, unresolved=False, viewer=None) == []
    )
    # _load_replies returns {} on empty file
    replies_file = tmp_path / "r.json"
    replies_file.write_text(json.dumps({"a": "b"}))
    assert module._load_replies(replies_file) == {"a": "b"}


def test_devtools_default_helpers_and_review_module(tmp_path: Path) -> None:
    """Cover DevTools default helpers (_path_resolve, _review_module return, bulk_reply wrapper)."""
    tools = DevTools(subprocess_runner=FakeSubprocessRunner(), root_path=tmp_path)
    # _path_resolve uses internal _resolve (line ~975)
    assert tools._path_resolve(tmp_path) == tmp_path.resolve()  # type: ignore[attr-defined]

    # Default review module wiring (bulk_reply wrapper line ~1012)
    fetch = dev_module._FetchResult(  # type: ignore[reportPrivateUsage]
        threads=[],
        viewer=None,
    )
    module = tools._review_module()  # type: ignore[attr-defined]
    module._bulk_reply(fetch=fetch, replies={})  # no-op but exercises wrapper

    # _review_module returns factory when provided (line ~994)
    sentinel = object()

    def factory() -> object:
        return sentinel

    tools_with_factory = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=factory,
    )
    assert tools_with_factory._review_module() is sentinel  # type: ignore[attr-defined]


def test_fetch_review_threads_author_missing(tmp_path: Path) -> None:
    """Author missing should default to empty string and still render."""
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": None,
                                            "body": "body",
                                            "url": "u1",
                                            "databaseId": 1,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=4)
    assert result.success is True
    assert "Thread: u1" in result.stdout


def test_apply_filters_branches(tmp_path: Path) -> None:
    """Exercise unreplied/unresolved branches in apply_filters via render."""
    runner = FakeSubprocessRunner()
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [
                            {
                                "isResolved": True,
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "c1",
                                            "author": {"login": "me"},
                                            "body": "viewer comment",
                                            "url": "u1",
                                            "databaseId": 1,
                                            "viewerDidAuthor": True,
                                        }
                                    ]
                                },
                            },
                            {
                                "isResolved": False,
                                "comments": {
                                    "nodes": [
                                        {
                                            "id": "c2",
                                            "author": {"login": "other"},
                                            "body": "other comment",
                                            "url": "u2",
                                            "databaseId": 2,
                                            "viewerDidAuthor": False,
                                        }
                                    ]
                                },
                            },
                        ]
                    }
                }
            },
        }
    }
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_list(pr_number=5, unreplied=True, unresolved=True)
    # Only the unresolved/unreplied thread should remain
    assert "u2" in result.stdout
    assert "u1" not in result.stdout


def test_bulk_reply_skip_missing_non_http(tmp_path: Path) -> None:
    """Missing non-http reply key should be skipped without gh mutation."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"missing": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=12, replies_file=replies_file)
    assert result.success is True
    # Only infer + fetch should have been called
    assert len(runner.calls) == 2


def test_comment_lookup_anchor_and_dbid_mapping() -> None:
    """_comment_lookup should map id/url/anchor/database_id."""
    comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        id="cid",
        author="a",
        viewer_did_author=False,
        body="b",
        url="http://x#anchor",
        database_id=42,
        created_at=None,
    )
    fetch = dev_module._FetchResult(  # type: ignore[reportPrivateUsage]
        threads=[
            dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u",
                is_resolved=False,
                comments=[comment],
            )
        ],
        viewer=None,
    )
    mapping = dev_module._comment_lookup(fetch)  # type: ignore[reportPrivateUsage]
    assert mapping["cid"] == "cid"
    assert mapping["http://x#anchor"] == "cid"
    assert mapping["anchor"] == "cid"
    assert mapping["42"] == "cid"


def test_render_threads_function_branches() -> None:
    """render_threads function should cover empty and content branches."""
    comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="a",
        viewer_did_author=True,
        body="Line1\nLine2",
        url="u",
        id="c1",
        database_id=None,
        created_at=None,
    )
    thread = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="u",
        is_resolved=False,
        comments=[comment],
    )

    def passthrough(t: Iterable[Any], **_: Any) -> list[Any]:
        return list(t)

    lines = dev_module.render_threads(  # type: ignore[reportPrivateUsage]
        [thread],
        apply_filters=passthrough,
        unreplied=False,
        unresolved=False,
        viewer="a",
    )
    assert "Thread: u" in lines[0]
    assert "Line1" in "\n".join(lines)
    # Empty body branch
    empty_body = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="b",
        viewer_did_author=False,
        body="",
        url="u2",
        id="c2",
        database_id=None,
        created_at=None,
    )
    lines_empty_body = dev_module.render_threads(  # type: ignore[reportPrivateUsage]
        [
            dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u2", is_resolved=True, comments=[empty_body]
            )
        ],
        apply_filters=passthrough,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert "<no content>" in "\n".join(lines_empty_body)

    # Empty result path
    empty_lines = dev_module.render_threads(  # type: ignore[reportPrivateUsage]
        [],
        apply_filters=lambda *_, **__: [],
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert empty_lines == ["No matching review threads found."]


def test_bulk_reply_http_missing_anchor_skip(tmp_path: Path) -> None:
    """HTTP reply key with missing anchor mapping should skip gh call."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"http://x#missing": "hi"}))
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {
                    "reviewThreads": {"nodes": []},
                }
            },
        }
    }
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=13, replies_file=replies_file)
    assert result.success is True
    assert len(runner.calls) == 2


def test_review_delete_subprocess_failure(tmp_path: Path) -> None:
    """Deletion subprocess failure returns that ToolResult (721)."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["c1"]))
    payload: dict[str, Any] = {
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
                                            "id": "c1",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#discussion_r1",
                                            "databaseId": 10,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=False,
                exit_code=1,
                stderr="gh delete failed",
                namespace="tools",
                category="dev",
                command="review-delete",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_delete(pr_number=5, comments_file=comments_file)
    assert result.success is False
    assert "gh delete failed" in (result.stderr or "")


def test_review_delete_factory_toolexecutionerror(tmp_path: Path) -> None:
    """ToolExecutionError from factory should hit outer handler (733)."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["c1"]))

    def factory():
        raise ToolExecutionError("factory boom", reason="r", rationale="r")

    tools = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=factory,
    )
    result = tools.review_delete(pr_number=6, comments_file=comments_file)
    assert result.success is False
    assert "factory boom" in (result.stderr or "")


def test_review_delete_generic_exception(tmp_path: Path) -> None:
    """Generic exception should hit outer handler (771)."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text(json.dumps(["c1"]))

    def factory():
        raise RuntimeError("unexpected boom")

    tools = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=factory,
    )
    result = tools.review_delete(pr_number=7, comments_file=comments_file)
    assert result.success is False
    assert "unexpected boom" in (result.stderr or "")


def test_bulk_reply_skip_branches(tmp_path: Path) -> None:
    """Cover _bulk_reply skip for non-http and http anchors with no mapping."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"missing": "hi", "http://x#missing": "yo"}))
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}},
        }
    }
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=14, replies_file=replies_file)
    assert result.success is True
    assert len(runner.calls) == 2  # only infer + fetch, skips gh mutation


def test_review_delete_comments_file_not_list(tmp_path: Path) -> None:
    """Non-list comments file should trigger empty targets (line 448) and succeed."""
    comments_file = tmp_path / "comments.json"
    comments_file.write_text("{}")  # not a list -> _load_comment_targets returns []
    payload: dict[str, Any] = {
        "data": {
            "viewer": {"login": "me"},
            "repository": {
                "pullRequest": {"reviewThreads": {"nodes": []}},
            },
        }
    }
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_delete(pr_number=8, comments_file=comments_file)
    assert result.success is True
    assert "Successfully deleted 0 comments" in (result.stdout or "")


def test_comment_lookup_anchor_mapping() -> None:
    """Anchor mapping should be added for URLs with fragments (430-432)."""
    comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        id="cid",
        author="a",
        viewer_did_author=False,
        body="body",
        url="http://x#frag",
        database_id=None,
        created_at=None,
    )
    fetch = dev_module._FetchResult(  # type: ignore[reportPrivateUsage]
        threads=[
            dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u",
                is_resolved=False,
                comments=[comment],
            )
        ],
        viewer=None,
    )
    mapping = dev_module._comment_lookup(fetch)  # type: ignore[reportPrivateUsage]
    assert mapping["frag"] == "cid"
    assert mapping["http://x#frag"] == "cid"


def test_kill_port_psutil_fallback(tmp_path: Path) -> None:
    """kill_port should use _default_pids_by_port fallback when net_connections fails (523->519, 525->523)."""

    class FakeAddr:
        def __init__(self, port: int) -> None:
            self.port = port

    class FakeConn:
        def __init__(self, port: int) -> None:
            self.laddr = FakeAddr(port)

    class FakeProc:
        def __init__(self, pid: int, port: int) -> None:
            self.pid = pid
            self._port = port

        def net_connections(self, kind: str = "inet"):
            return [FakeConn(self._port)]

    # net_connections raises to force fallback, process_iter provides connections
    net_orig, net_had = override_attr(
        dev_module.psutil,
        "net_connections",
        lambda kind="inet": (_ for _ in ()).throw(RuntimeError("fail")),
    )

    def fake_process_iter(*_args: Any, **_kwargs: Any) -> list[FakeProc]:
        return [FakeProc(123, 5555)]

    proc_orig, proc_had = override_attr(
        dev_module.psutil, "process_iter", fake_process_iter
    )

    killed: list[int] = []

    def kill_pid(pid: int) -> bool:
        killed.append(pid)
        return True

    try:
        tools = DevTools(
            root_path=tmp_path,
            pids_by_port=None,  # use default _default_pids_by_port
            kill_pid=kill_pid,
        )
        result = tools.kill_port(5555)
        assert result.success is True
        assert killed == [123]
    finally:
        restore_attr(dev_module.psutil, "net_connections", net_orig, net_had)
        restore_attr(dev_module.psutil, "process_iter", proc_orig, proc_had)


def test_kill_port_psutil_fallback_no_laddr(tmp_path: Path) -> None:
    """Fallback path with connection lacking laddr should return no processes found."""

    class FakeConnNoLaddr:
        def __init__(self) -> None:
            self.laddr = None

    class FakeProcNoLaddr:
        def __init__(self) -> None:
            self.pid = 321

        def net_connections(self, kind: str = "inet"):
            return [FakeConnNoLaddr()]

    net_orig, net_had = override_attr(
        dev_module.psutil,
        "net_connections",
        lambda kind="inet": (_ for _ in ()).throw(RuntimeError("fail")),
    )

    def fake_process_iter_no_laddr(
        *_args: Any, **_kwargs: Any
    ) -> list[FakeProcNoLaddr]:
        return [FakeProcNoLaddr()]

    proc_orig, proc_had = override_attr(
        dev_module.psutil, "process_iter", fake_process_iter_no_laddr
    )

    try:
        tools = DevTools(
            root_path=tmp_path, pids_by_port=None, kill_pid=lambda pid: True
        )
        result = tools.kill_port(5555)
        assert result.success is True
        assert "No processes found" in (result.stdout or "")
    finally:
        restore_attr(dev_module.psutil, "net_connections", net_orig, net_had)
        restore_attr(dev_module.psutil, "process_iter", proc_orig, proc_had)


def test_cleanup_ignored_tracked_no_ignored(tmp_path: Path) -> None:
    """cleanup_ignored_tracked should return early when no ignored files are present (771)."""
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="",
                namespace="tools",
                category="dev",
                command="cleanup-ignored-tracked",
            )
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.cleanup_ignored_tracked()
    assert result.success is True
    assert "No ignored tracked files found" in (result.stdout or "")


def test_review_bulk_reply_anchor_key(tmp_path: Path) -> None:
    """Anchor key in replies should resolve via comment_lookup anchors (430-432)."""
    replies_file = tmp_path / "replies.json"
    replies_file.write_text(json.dumps({"frag": "hi"}))
    payload: dict[str, Any] = {
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
                                            "id": "cid",
                                            "author": {"login": "a"},
                                            "body": "x",
                                            "url": "http://x#frag",
                                            "databaseId": None,
                                            "viewerDidAuthor": False,
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
    runner = FakeSubprocessRunner()
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            ),
            ToolResult.create(
                success=True,
                exit_code=0,
                stdout="ok",
                namespace="tools",
                category="dev",
                command="review-reply-gql",
            ),
        ]
    )
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.review_bulk_reply(pr_number=16, replies_file=replies_file)
    assert result.success is True
    assert len(runner.calls) == 3


def test_render_threads_instance_tail(tmp_path: Path) -> None:
    """DevTools._render_threads should emit tail message when no threads and handle empty bodies."""
    tools = DevTools(root_path=tmp_path, review_module_factory=lambda: None)  # type: ignore[arg-type]
    render = tools._render_threads  # type: ignore[attr-defined]

    def no_threads(_: Iterable[Any], **__: Any) -> list[Any]:
        return []

    lines_tail = render(
        [], apply_filters=no_threads, unreplied=False, unresolved=False, viewer=None
    )
    assert lines_tail == ["No matching review threads found."]

    def passthrough(t: Iterable[Any], **__: Any) -> list[Any]:
        return list(t)

    comment_empty = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="a",
        viewer_did_author=False,
        body="",
        url="u",
        id="c1",
        database_id=None,
        created_at=None,
    )
    thread = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="u",
        is_resolved=False,
        comments=[comment_empty],
    )
    lines_empty_body = render(
        [thread],
        apply_filters=passthrough,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert "<no content>" in "\n".join(lines_empty_body)


def test_render_threads_module_tail_and_body() -> None:
    """Module-level render_threads tail and empty-body handling (975/994/1012)."""
    # Empty filtered -> tail
    lines_tail = dev_module.render_threads(  # type: ignore[reportPrivateUsage]
        [],
        apply_filters=lambda *_args, **_kwargs: [],
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert lines_tail == ["No matching review threads found."]

    comment_empty = dev_module._Comment(  # type: ignore[reportPrivateUsage]
        author="",
        viewer_did_author=False,
        body="",
        url="u",
        id="c1",
        database_id=None,
        created_at=None,
    )
    thread = dev_module._Thread(  # type: ignore[reportPrivateUsage]
        url="u",
        is_resolved=False,
        comments=[comment_empty],
    )

    def passthrough(t: Iterable[Any], **_: Any) -> Iterable[Any]:
        return t

    lines = dev_module.render_threads(  # type: ignore[reportPrivateUsage]
        [thread],
        apply_filters=passthrough,
        unreplied=False,
        unresolved=False,
        viewer=None,
    )
    assert "<no content>" in "\n".join(lines)


def test_review_list_render_tail_and_empty_author(tmp_path: Path) -> None:
    """review_list via DevTools should cover _render_threads tail and empty author/body."""

    class ReviewModule:
        def _infer_repo(self, remote: str) -> tuple[str, str]:
            return ("owner", "repo")

        def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> Any:
            comment = dev_module._Comment(  # type: ignore[reportPrivateUsage]
                author="",
                viewer_did_author=False,
                body="",
                url="u",
                id="c1",
                database_id=None,
                created_at=None,
            )
            thread = dev_module._Thread(  # type: ignore[reportPrivateUsage]
                url="u",
                is_resolved=False,
                comments=[comment],
            )
            return dev_module._FetchResult(threads=[thread], viewer=None)  # type: ignore[reportPrivateUsage]

        def apply_filters(
            self,
            threads: Iterable[Any],
            *,
            unreplied: bool,
            unresolved: bool,
            viewer: str | None,
        ) -> Iterable[Any]:
            return threads

    tools = DevTools(
        subprocess_runner=FakeSubprocessRunner(),
        root_path=tmp_path,
        review_module_factory=ReviewModule,
    )
    result = tools.review_list(pr_number=18, unreplied=False, unresolved=False)
    assert result.success is True
    assert "<no content>" in (result.stdout or "")
