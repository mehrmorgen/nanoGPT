from __future__ import annotations

import tempfile
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from types import SimpleNamespace

import hypothesis.strategies as st
from hypothesis import given, settings, HealthCheck

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev import dev as dev_module
from ml_playground.tools.dev.dev import DevTools

from tests.property.tools._helpers import DeterministicRunner

_MISSING = object()


@contextmanager
def _override_attr(obj: object, name: str, value: object) -> Iterator[None]:
    """Manual context manager to replace the override_attr fixture."""
    original = getattr(obj, name, _MISSING)
    object.__setattr__(obj, name, value)
    try:
        yield
    finally:
        if original is _MISSING:
            delattr(obj, name)
        else:
            object.__setattr__(obj, name, original)


def _success(command: str) -> ToolResult:
    return ToolResult(
        success=True,
        exit_code=0,
        stdout="ok",
        stderr="",
        operation_id=OperationId(namespace="tools", category="dev", command=command),
    )


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    pr_number=st.integers(min_value=1, max_value=1000),
    unreplied=st.booleans(),
    unresolved=st.booleans(),
)
def test_review_list_delegates(
    pr_number: int,
    unreplied: bool,
    unresolved: bool,
):
    """DevTools.review_list should delegate to run_review_list with original parameters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_review_list(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("review-list")

        with _override_attr(dev_module, "run_review_list", fake_review_list):
            tools = DevTools(
                ToolsConfig(),
                subprocess_runner=DeterministicRunner(),
                root_path=tmp_path,
            )
            result = tools.review_list(
                pr_number, unreplied=unreplied, unresolved=unresolved
            )

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["pr_number"] == pr_number
        assert kwargs["remote"] == "origin"
        assert kwargs["unreplied"] is unreplied
        assert kwargs["unresolved"] is unresolved
        assert kwargs["subprocess_runner"] == tools._subprocess_runner
        assert kwargs["root_path"] == tmp_path
        assert kwargs["review_module_factory"] is None


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    gh_payload=st.sampled_from(
        [
            {
                "data": {
                    "viewer": {"login": "me"},
                    "repository": {"pullRequest": {"reviewThreads": {"nodes": []}}},
                }
            },
            {
                "data": {
                    "viewer": {"login": "me"},
                    "repository": {"pullRequest": {"reviewThreads": {"nodes": [None]}}},
                }
            },
        ]
    ),
)
def test_review_list_fetch_minimal_valid_data_property(
    gh_payload: dict[str, Any],
) -> None:
    """review_list should succeed on minimal well-formed data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        runner = DeterministicRunner()
        runner.queue_result(
            lambda op: ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                stderr="",
                namespace="tools",
                category="dev",
                command=op.command,
            )
        )
        runner.queue_result(
            lambda op: ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(gh_payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            )
        )
        tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
        result = tools.review_list(pr_number=1)
        assert result.success
        assert "No matching review threads found" in result.stdout


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    empty_body=st.text(max_size=0),
    multi_body=st.text(min_size=3, max_size=20),
)
def test_render_threads_property(empty_body: str, multi_body: str) -> None:
    """_render_threads should cover empty and multi-line bodies via review_list."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        runner = DeterministicRunner()
        payload = {
            "data": {
                "viewer": {"login": "viewer1"},
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
                                                "author": {"login": "a1"},
                                                "body": empty_body,
                                                "url": "https://x/y#c1",
                                                "databaseId": 1,
                                            },
                                            {
                                                "id": "c2",
                                                "author": {"login": "viewer1"},
                                                "body": f"{multi_body}\nmore",
                                                "url": "https://x/y#c2",
                                                "databaseId": 2,
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
        runner.queue_result(
            lambda op: ToolResult.create(
                success=True,
                exit_code=0,
                stdout="owner/repo\n",
                namespace="tools",
                category="dev",
                command="review-infer-repo",
            )
        )
        runner.queue_result(
            lambda op: ToolResult.create(
                success=True,
                exit_code=0,
                stdout=json.dumps(payload),
                namespace="tools",
                category="dev",
                command="review-fetch",
            )
        )
        tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
        result = tools.review_list(pr_number=2)
        assert result.success is True
        assert "viewer)" in result.stdout or "viewer" in result.stdout
        assert "<no content>" in result.stdout
        assert "more" in result.stdout


@settings(max_examples=10, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    st.text(min_size=1, max_size=10)
)
def test_review_bulk_reply_comment_lookup_property(key: str) -> None:
    """_comment_lookup robustness via bulk_reply with malformed comment entries."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        replies_file = Path(tmp_path) / "replies.json"
        replies_file.write_text(json.dumps({key: "body"}), encoding="utf-8")

        class ReviewModule:
            def _infer_repo(self, remote: str) -> tuple[str, str]:
                return ("o", "r")

            def fetch_review_threads(
                self, owner: str, repo: str, pr_number: int
            ) -> Any:
                # Include malformed entries to exercise lookup robustness
                comments: list[Any] = [
                    SimpleNamespace(
                        id="c1",
                        author="a",
                        viewer_did_author=False,
                        body="x",
                        url="http://x#c1",
                        database_id=1,
                        created_at=None,
                    ),
                    "not-a-dict",
                    SimpleNamespace(
                        id=None,
                        author="b",
                        viewer_did_author=False,
                        body="y",
                        url="",
                        database_id=None,
                        created_at=None,
                    ),
                ]
                thread = SimpleNamespace(
                    url="http://x", is_resolved=False, comments=comments
                )
                return SimpleNamespace(threads=[thread], viewer=None)

            def apply_filters(
                self,
                threads: Any,
                *,
                unreplied: bool,
                unresolved: bool,
                viewer: str | None,
            ):
                return threads

            def _load_replies(self, replies_file: Path) -> dict[str, str]:
                return {key: "body"}

            def _bulk_reply(self, *, fetch: Any, replies: dict[str, str]) -> None:
                return None

            def _load_comment_targets(self, path: Path) -> list[str]:
                return []

            def _comment_lookup(self, fetch: Any) -> dict[str, str]:
                return {"c1": "c1", "c2": "c2"}

        runner = DeterministicRunner()
        tools = DevTools(
            subprocess_runner=runner,
            root_path=tmp_path,
            review_module_factory=lambda: ReviewModule(),
        )
        result = tools.review_bulk_reply(pr_number=3, replies_file=replies_file)
        assert result.success is True


@settings(max_examples=5, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    st.sampled_from(["json", "yaml", "text"])
)
def test_batch_review_entry_property(fmt: str) -> None:
    """Ensure batch_review public entry path returns ToolResult."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        runner = DeterministicRunner()
        # batch_review uses subprocess_runner; we only assert call wiring via ToolResult
        tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
        result = tools.batch_review(output_format=fmt)
        assert isinstance(result, ToolResult)


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    pr_number=st.integers(min_value=1, max_value=1000),
    remote=st.sampled_from(["origin", "upstream"]),
)
def test_review_bulk_reply_delegates(
    pr_number: int,
    remote: str,
):
    """DevTools.review_bulk_reply should delegate to run_review_bulk_reply."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        replies_file = tmp_path / "replies.json"
        replies_file.write_text("{}", encoding="utf-8")
        seen: list[tuple[Any, ...]] = []

        def fake_review_bulk_reply(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("review-bulk-reply")

        with _override_attr(
            dev_module, "run_review_bulk_reply", fake_review_bulk_reply
        ):
            tools = DevTools(
                ToolsConfig(),
                subprocess_runner=DeterministicRunner(),
                root_path=tmp_path,
            )
            result = tools.review_bulk_reply(
                pr_number, replies_file=replies_file, remote=remote
            )

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["pr_number"] == pr_number
        assert kwargs["replies_file"] == replies_file
        assert kwargs["remote"] == remote
        assert kwargs["subprocess_runner"] == tools._subprocess_runner
        assert kwargs["root_path"] == tmp_path
        assert kwargs["review_module_factory"] is None


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    st.just(())
)
def test_cleanup_ignored_tracked_delegates(
    _: tuple[()],
):
    """DevTools.cleanup_ignored_tracked should call run_cleanup_ignored_tracked."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_cleanup(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("cleanup-ignored-tracked")

        with _override_attr(dev_module, "run_cleanup_ignored_tracked", fake_cleanup):
            tools = DevTools(
                ToolsConfig(),
                subprocess_runner=DeterministicRunner(),
                root_path=tmp_path,
            )
            result = tools.cleanup_ignored_tracked()

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["subprocess_runner"] == tools._subprocess_runner
        assert kwargs["root_path"] == tmp_path


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    port=st.integers(min_value=1024, max_value=65535)
)
def test_kill_port_delegates(
    port: int,
):
    """DevTools.kill_port should delegate to run_kill_port and preserve the port number."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_kill_port(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("kill-port")

        with _override_attr(dev_module, "run_kill_port", fake_kill_port):
            tools = DevTools(
                ToolsConfig(),
                subprocess_runner=DeterministicRunner(),
                root_path=tmp_path,
            )
            result = tools.kill_port(port)

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["port"] == port
        assert kwargs["subprocess_runner"] == tools._subprocess_runner
        assert kwargs["root_path"] == tmp_path


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    pr_number=st.integers(min_value=1, max_value=1000),
    remote=st.sampled_from(["origin", "upstream"]),
)
def test_review_delete_delegates(
    pr_number: int,
    remote: str,
):
    """DevTools.review_delete should delegate to run_review_delete with original parameters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        comments_file = tmp_path / "comments.json"
        comments_file.write_text("{}", encoding="utf-8")
        seen: list[tuple[Any, ...]] = []

        def fake_review_delete(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("review-delete")

        with _override_attr(dev_module, "run_review_delete", fake_review_delete):
            tools = DevTools(
                ToolsConfig(),
                subprocess_runner=DeterministicRunner(),
                root_path=tmp_path,
            )
            result = tools.review_delete(
                pr_number, comments_file=comments_file, remote=remote
            )

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["pr_number"] == pr_number
        assert kwargs["comments_file"] == comments_file
        assert kwargs["remote"] == remote
        assert kwargs["subprocess_runner"] == tools._subprocess_runner
        assert kwargs["root_path"] == tmp_path
        assert kwargs["review_module_factory"] is None


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    output_format=st.sampled_from(["json", "yaml", "text"])
)
def test_batch_review_delegates(
    output_format: str,
):
    """DevTools.batch_review should delegate to run_dev_batch_review with original parameters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_batch_review(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("batch-review")

        with _override_attr(dev_module, "run_dev_batch_review", fake_batch_review):
            config = ToolsConfig()
            tools = DevTools(
                config, subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.batch_review(output_format=output_format)

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["config"] == config
        assert kwargs["root_path"] == tmp_path
        assert kwargs["output_format"] == output_format


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    output_format=st.sampled_from(["json", "yaml", "text"])
)
def test_workflow_status_delegates(
    output_format: str,
):
    """DevTools.workflow_status should delegate to run_dev_workflow_status with original parameters."""
    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_path = Path(temp_dir)
        seen: list[tuple[Any, ...]] = []

        def fake_workflow_status(*args: Any, **kwargs: Any) -> ToolResult:
            seen.append((args, kwargs))
            return _success("workflow-status")

        with _override_attr(
            dev_module, "run_dev_workflow_status", fake_workflow_status
        ):
            config = ToolsConfig()
            tools = DevTools(
                config, subprocess_runner=DeterministicRunner(), root_path=tmp_path
            )
            result = tools.workflow_status(output_format=output_format)

        assert result.success is True
        _, kwargs = seen[0]
        assert kwargs["config"] == config
        assert kwargs["root_path"] == tmp_path
        assert kwargs["output_format"] == output_format


@settings(
    max_examples=50,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    threads_data=st.lists(
        st.fixed_dictionaries(
            {
                "url": st.text(
                    min_size=1,
                    alphabet=st.characters(
                        whitelist_categories=("L", "Nd", "Pc", "Pd", "Zs")
                    ),
                ),
                "is_resolved": st.booleans(),
                "comments": st.lists(
                    st.fixed_dictionaries(
                        {
                            "author": st.text(min_size=1),
                            "viewer_did_author": st.booleans(),
                            "body": st.text(),
                            "id": st.text(
                                min_size=1,
                                alphabet=st.characters(
                                    whitelist_categories=("L", "Nd"),
                                    min_codepoint=48,
                                    max_codepoint=57,
                                ),
                            ),
                        }
                    ),
                    min_size=1,
                    max_size=3,
                ),
            }
        ),
        min_size=0,
        max_size=10,
    ),
    unreplied=st.booleans(),
    unresolved=st.booleans(),
    viewer=st.text(min_size=1) | st.none(),
)
def test_apply_filters_logic(
    threads_data: list[dict[str, Any]],
    unreplied: bool,
    unresolved: bool,
    viewer: str | None,
):
    """Test _apply_filters logic with generated threads."""
    _Thread = getattr(dev_module, "_Thread")
    _Comment = getattr(dev_module, "_Comment")
    apply_filters = getattr(dev_module, "_apply_filters")

    threads = []
    for t_dict in threads_data:
        comments = [_Comment(**c_dict) for c_dict in t_dict["comments"]]
        threads.append(
            _Thread(
                url=t_dict["url"], is_resolved=t_dict["is_resolved"], comments=comments
            )
        )

    filtered = apply_filters(
        threads, unreplied=unreplied, unresolved=unresolved, viewer=viewer
    )

    for t in filtered:
        if unresolved:
            assert not t.is_resolved
        if unreplied and viewer:
            assert not any(c.viewer_did_author for c in t.comments)


@settings(
    max_examples=50,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(  # type: ignore[reportAny]
    threads_data=st.lists(
        st.fixed_dictionaries(
            {
                "url": st.text(
                    min_size=1,
                    alphabet=st.characters(
                        whitelist_categories=("L", "Nd", "Pc", "Pd", "Zs")
                    ),
                ),
                "is_resolved": st.booleans(),
                "comments": st.lists(
                    st.fixed_dictionaries(
                        {
                            "author": st.text(min_size=1),
                            "viewer_did_author": st.booleans(),
                            "body": st.text(),
                            "id": st.text(
                                min_size=1,
                                alphabet=st.characters(
                                    whitelist_categories=("L", "Nd"),
                                    min_codepoint=48,
                                    max_codepoint=57,
                                ),
                            ),
                            "url": st.text(),
                            "database_id": st.integers(min_value=1) | st.none(),
                        }
                    ),
                    min_size=1,
                    max_size=3,
                ),
            }
        ),
        min_size=1,
        max_size=5,
    )
)
def test_comment_lookup_logic(threads_data: list[dict[str, Any]]):
    """Test _comment_lookup mapping logic with guaranteed unique identifiers."""
    _Thread = getattr(dev_module, "_Thread")
    _Comment = getattr(dev_module, "_Comment")
    _FetchResult = getattr(dev_module, "_FetchResult")
    _comment_lookup = getattr(dev_module, "_comment_lookup")

    used_db_ids = set()
    counter = 0

    threads = []
    for t_dict in threads_data:
        unique_comments = []
        for c_dict in t_dict["comments"]:
            cid = f"cid_{counter}"
            counter += 1

            c_dict["id"] = cid
            if c_dict["url"] and "#" not in c_dict["url"]:
                c_dict["url"] += f"#anchor_{cid}"

            # Ensure database_id is unique if it exists to avoid clashes in the mapping
            db_id = c_dict["database_id"]
            if db_id is not None:
                while db_id in used_db_ids:
                    db_id += 1
                used_db_ids.add(db_id)
                c_dict["database_id"] = db_id

            unique_comments.append(_Comment(**c_dict))

        threads.append(
            _Thread(
                url=t_dict["url"],
                is_resolved=t_dict["is_resolved"],
                comments=unique_comments,
            )
        )

    fetch = _FetchResult(threads=threads, viewer=None)
    mapping = _comment_lookup(fetch)

    for t in threads:
        for c in t.comments:
            if c.id:
                assert mapping[c.id] == c.id
                if c.url and "#" in c.url:
                    anchor = c.url.split("#")[-1]
                    if anchor:
                        # This covers line 430 anchor logic
                        assert mapping[anchor] == c.id
                if c.database_id is not None:
                    # In case of database_id clash, the last one wins in the code.
                    # Our unique_db_ids logic prevents this clash in the test data.
                    assert mapping[str(c.database_id)] == c.id
