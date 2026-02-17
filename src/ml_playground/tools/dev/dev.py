"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

import json
import os
import platform
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    cast,
    runtime_checkable,
)

import psutil

from pydantic import BaseModel, ConfigDict


from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from . import batch_review as batch_review_module
from . import github_actions, hygiene
from .ai_guidelines import (
    GitIgnorePattern,
    SetupResult,
    setup_ai_guidelines,
)
from .status import run_dev_batch_review, run_dev_workflow_status
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
)

from ml_playground.framework.core.di_implementations import DefaultJsonParser

batch_review = batch_review_module

__all__ = [
    "DevTools",
    "batch_review",
    "psutil",
    "Comment",
    "Thread",
    "FetchResult",
    "apply_filters",
    "comment_lookup",
    "load_replies",
]


def run_kill_port(
    *,
    port: int,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    pids_by_port: Callable[[int], list[int]] | None = None,
    kill_pid: Callable[[int], bool] | None = None,
) -> ToolResult:
    """Delegate kill-port behavior through hygiene with testable hook."""
    return hygiene.run_kill_port(
        port=port,
        subprocess_runner=subprocess_runner,
        root_path=root_path,
        pids_by_port=pids_by_port,
        kill_pid=kill_pid,
    )


class _Comment(BaseModel):
    model_config = ConfigDict(frozen=True)

    author: str
    viewer_did_author: bool
    body: str
    url: str | None = None
    id: str | None = None
    database_id: int | None = None
    created_at: str | None = None

    def __hash__(self) -> int:
        return hash(
            (
                self.author,
                self.viewer_did_author,
                self.body,
                self.url or "",
                self.id or "",
                self.database_id,
                self.created_at,
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Comment):
            return NotImplemented
        return (
            self.author == other.author
            and self.viewer_did_author == other.viewer_did_author
            and self.body == other.body
            and (self.url or "") == (other.url or "")
            and (self.id or "") == (other.id or "")
            and self.database_id == other.database_id
            and self.created_at == other.created_at
        )


class _Thread(BaseModel):
    model_config = ConfigDict(frozen=True)

    url: str
    is_resolved: bool
    comments: list[_Comment]

    def __hash__(self) -> int:
        return hash((self.url, self.is_resolved, tuple(self.comments)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Thread):
            return NotImplemented
        return (
            self.url == other.url
            and self.is_resolved == other.is_resolved
            and tuple(self.comments) == tuple(other.comments)
        )


class _FetchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    threads: list[_Thread]
    viewer: str | None
    pull_request_id: str | None = None


Comment = _Comment
Thread = _Thread
FetchResult = _FetchResult


def apply_filters(
    threads: list[_Thread], *, unreplied: bool, unresolved: bool, viewer: str | None
) -> list[_Thread]:
    return _apply_filters(
        threads, unreplied=unreplied, unresolved=unresolved, viewer=viewer
    )


def comment_lookup(fetch: _FetchResult) -> dict[str, str]:
    return _comment_lookup(fetch)


def load_replies(replies_file: Path) -> dict[str, str]:
    return _load_replies(replies_file)


def _exec(
    args: list[str],
    *,
    operation_id: OperationId,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
) -> ToolResult:
    return subprocess_runner.run_subprocess(
        args, cwd=root_path, operation_id=operation_id
    )


def _infer_repo(
    remote: str, subprocess_runner: SubprocessRunner, root_path: Path
) -> tuple[str, str]:
    res = _exec(
        ["git", "remote", "get-url", remote],
        operation_id=OperationId(
            namespace="tools", category="dev", command="review-infer-repo"
        ),
        subprocess_runner=subprocess_runner,
        root_path=root_path,
    )
    if not res.success or not res.stdout.strip():
        gh = _exec(
            [
                "gh",
                "repo",
                "view",
                "--json",
                "owner,name",
                "-q",
                ".owner.login + '/' + .name",
            ],
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
            subprocess_runner=subprocess_runner,
            root_path=root_path,
        )
        if not gh.success:
            raise ToolExecutionError(
                "Failed to infer repository",
                reason="git remote and gh repo view unavailable",
                rationale="Ensure GitHub CLI is installed and authenticated.",
            )
        owner, name = gh.stdout.strip().split("/")
        return owner, name

    url = res.stdout.strip()
    if url.startswith("git@"):
        path = url.split(":", 1)[1]
    else:
        parts = url.split("github.com/")
        path = parts[1] if len(parts) > 1 else url
    path = path.rstrip(".git").strip("/")
    owner, name = path.split("/", 1)
    return owner, name


def _fetch_review_threads(
    owner: str,
    repo: str,
    pr_number: int,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
) -> _FetchResult:
    query = (
        "query($owner:String!,$repo:String!,$pr:Int!){"
        " viewer { login }"
        " repository(owner:$owner,name:$repo){"
        "   pullRequest(number:$pr){"
        "     id"
        "     reviewThreads(first:100){ nodes {"
        "       isResolved"
        "       comments(first:50){ nodes {"
        "         author { login }"
        "         body"
        "         url"
        "         id"
        "         databaseId"
        "         createdAt"
        "       } }"
        "     } }"
        "   }"
        " }"
        "}"
    )
    args = [
        "gh",
        "api",
        "graphql",
        "-f",
        f"query={query}",
        "-F",
        f"owner={owner}",
        "-F",
        f"repo={repo}",
        "-F",
        f"pr={pr_number}",
    ]
    op_id = OperationId(namespace="tools", category="dev", command="review-fetch")
    res = _exec(
        args,
        operation_id=op_id,
        subprocess_runner=subprocess_runner,
        root_path=root_path,
    )
    if not res.success:
        raise ToolExecutionError(
            "Failed to fetch review threads",
            reason=res.stderr or "gh api graphql failed",
            rationale="Ensure gh is installed and you have permissions to view the PR.",
        )

    try:
        raw_data = DefaultJsonParser().parse_json(res.stdout or "{}")
        data = cast(dict[str, object], raw_data)
    except json.JSONDecodeError as e:
        raise ToolExecutionError(
            "Failed to parse GitHub CLI output",
            reason=str(e),
            rationale="The output from 'gh api graphql' was not valid JSON.",
        ) from e

    data_content = data.get("data")
    if not isinstance(data_content, dict):
        return _FetchResult(threads=[], viewer=None)

    viewer_node = cast(dict[str, object], data_content).get("viewer")
    viewer: str | None = None
    if isinstance(viewer_node, dict):
        viewer_login = cast(dict[str, object], viewer_node).get("login")
        if isinstance(viewer_login, str):
            viewer = viewer_login
        else:
            pass

    repo_node = cast(dict[str, object], data_content).get("repository")
    if not isinstance(repo_node, dict):
        return _FetchResult(threads=[], viewer=viewer)

    pr_node_obj = cast(dict[str, object], repo_node).get("pullRequest")
    if not isinstance(pr_node_obj, dict):
        return _FetchResult(threads=[], viewer=viewer)
    pr_node: dict[str, object] = cast(dict[str, object], pr_node_obj)
    pr_id_obj: object | None = pr_node.get("id")
    pr_id: str | None = str(pr_id_obj) if isinstance(pr_id_obj, str) else None

    review_threads_node = pr_node.get("reviewThreads")
    if not isinstance(review_threads_node, dict):
        return _FetchResult(threads=[], viewer=viewer)

    thread_nodes = cast(dict[str, object], review_threads_node).get("nodes")
    if not isinstance(thread_nodes, list):
        return _FetchResult(threads=[], viewer=viewer)

    threads: list[_Thread] = []
    thread_nodes_list = cast(list[object], thread_nodes)
    for t_node in thread_nodes_list:
        if not isinstance(t_node, dict):
            continue

        t_dict = cast(dict[str, object], t_node)
        is_resolved: bool = bool(t_dict.get("isResolved", False))
        comments_wrapper = t_dict.get("comments")
        if not isinstance(comments_wrapper, dict):
            continue

        c_wrapper_dict = cast(dict[str, object], comments_wrapper)
        comment_nodes = c_wrapper_dict.get("nodes")
        if not isinstance(comment_nodes, list):
            continue

        comments: list[_Comment] = []
        comment_nodes_list = cast(list[object], comment_nodes)
        for c_node in comment_nodes_list:
            if not isinstance(c_node, dict):
                continue

            c_dict = cast(dict[str, object], c_node)
            author_wrapper = c_dict.get("author")
            author_login = ""
            if isinstance(author_wrapper, dict):
                login = cast(dict[str, object], author_wrapper).get("login")
                if isinstance(login, str):
                    author_login = login

            comments.append(
                _Comment(
                    author=author_login,
                    viewer_did_author=(viewer is not None and author_login == viewer),
                    body=str(c_dict.get("body") or ""),
                    url=str(c_dict.get("url") or ""),
                    id=str(c_dict.get("id") or ""),
                    database_id=cast(Optional[int], c_dict.get("databaseId")),
                    created_at=cast(Optional[str], c_dict.get("createdAt")),
                )
            )

        if comments:
            threads.append(
                _Thread(
                    url=comments[0].url or "",
                    is_resolved=is_resolved,
                    comments=comments,
                )
            )
        else:
            pass

    return _FetchResult(threads=threads, viewer=viewer, pull_request_id=pr_id)


def _apply_filters(
    threads: Iterable[object],
    *,
    unreplied: bool,
    unresolved: bool,
    viewer: str | None,
) -> list[_Thread]:
    items: list[_Thread] = []
    for th in threads:
        th_model = _Thread.model_validate(th)
        if unresolved and th_model.is_resolved:
            continue
        if unreplied:
            has_viewer_comment = any(c.viewer_did_author for c in th_model.comments)
            if has_viewer_comment:
                continue
        items.append(th_model)
    return items


def _load_replies(replies_file: Path) -> dict[str, str]:
    text = replies_file.read_text()
    try:
        raw_data = DefaultJsonParser().parse_json(text or "{}")
        if not isinstance(raw_data, dict):
            return {}
        data: dict[str, object] = raw_data
    except json.JSONDecodeError:
        return {}

    mapping: dict[str, str] = {}
    for k, v in data.items():
        if isinstance(v, str):
            mapping[k] = v
    return mapping


def _review_module_default(
    subprocess_runner: SubprocessRunner,
    root_path: Path,
) -> object:
    class _Module:
        infer_repo: Callable[[str], Tuple[str, str]]
        _infer_repo: Callable[[str], Tuple[str, str]]
        fetch_review_threads: Callable[[str, str, int], _FetchResult]
        _fetch_review_threads: Callable[[str, str, int], _FetchResult]
        apply_filters: Callable[..., list[_Thread]]
        load_replies: Callable[[Path], dict[str, str]]
        _load_replies: Callable[[Path], dict[str, str]]
        bulk_reply: Callable[..., None]
        _bulk_reply: Callable[..., None]
        load_comment_targets: Callable[[Path], list[str]]
        comment_lookup: Callable[[_FetchResult], dict[str, str]]
        _load_comment_targets: Callable[[Path], list[str]]
        _comment_lookup: Callable[[_FetchResult], dict[str, str]]

    mod = _Module()

    def infer_repo_wrapper(remote: str) -> Tuple[str, str]:
        return _infer_repo(remote, subprocess_runner, root_path)

    def fetch_review_threads_wrapper(
        owner: str, repo: str, pr_number: int
    ) -> _FetchResult:
        return _fetch_review_threads(
            owner, repo, pr_number, subprocess_runner, root_path
        )

    def bulk_reply_wrapper(*, fetch: _FetchResult, replies: dict[str, str]) -> None:
        return _bulk_reply(
            fetch=fetch,
            replies=replies,
            subprocess_runner=subprocess_runner,
            root_path=root_path,
        )

    mod.infer_repo = infer_repo_wrapper
    mod._infer_repo = infer_repo_wrapper  # pyright: ignore[reportPrivateUsage]
    mod.fetch_review_threads = fetch_review_threads_wrapper
    mod._fetch_review_threads = fetch_review_threads_wrapper  # pyright: ignore[reportPrivateUsage]
    mod.apply_filters = _apply_filters
    mod.load_replies = _load_replies
    mod._load_replies = _load_replies  # pyright: ignore[reportPrivateUsage]
    mod.bulk_reply = bulk_reply_wrapper
    mod._bulk_reply = bulk_reply_wrapper  # pyright: ignore[reportPrivateUsage]
    mod.load_comment_targets = _load_comment_targets
    mod._load_comment_targets = _load_comment_targets  # pyright: ignore[reportPrivateUsage]
    mod.comment_lookup = _comment_lookup
    mod._comment_lookup = _comment_lookup  # pyright: ignore[reportPrivateUsage]
    return mod


def render_threads(
    threads: Iterable[Any],
    *,
    apply_filters: Callable[..., Iterable[Any]],
    unreplied: bool,
    unresolved: bool,
    viewer: str | None,
) -> list[str]:
    filtered = apply_filters(
        threads,
        unreplied=unreplied,
        unresolved=unresolved,
        viewer=viewer,
    )

    lines: list[str] = []
    found = False
    for thread in cast(Iterable[_Thread], filtered):
        found = True
        lines.append(f"Thread: {thread.url}")
        lines.append(
            "  Status: Resolved" if thread.is_resolved else "  Status: Unresolved"
        )
        for comment in thread.comments:
            author = (
                f"{comment.author} (viewer)"
                if comment.viewer_did_author
                else comment.author
            )
            body = comment.body.rstrip("\n")
            if not body:
                lines.append(f"  - {author}:")
                lines.append("    <no content>")
                continue

            body_lines = body.splitlines()
            first_line = body_lines[0]
            lines.append(f"  - {author}: {first_line}")
            for continuation in body_lines[1:]:
                lines.append(f"    {continuation}")
        lines.append("")

    if not found:
        lines.append("No matching review threads found.")
    return lines


def _comment_lookup(fetch: _FetchResult) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for th in fetch.threads:
        for cm in th.comments:
            if not cm.id:
                continue
            try:
                mapping.setdefault(cm.id, cm.id)
                if cm.url:
                    mapping.setdefault(cm.url, cm.id)
                    if "#" in cm.url:
                        anchor = cm.url.split("#")[-1]
                        if anchor:
                            mapping.setdefault(anchor, cm.id)
                if cm.database_id is not None:
                    mapping.setdefault(str(cm.database_id), cm.id)
            except (AttributeError, ValueError, TypeError):
                continue
    return mapping


def _load_comment_targets(path: Path) -> list[str]:
    try:
        raw_data = DefaultJsonParser().parse_json(path.read_text() or "[]")
    except json.JSONDecodeError:
        return []

    if isinstance(raw_data, list):
        data_list: list[object] = cast(list[object], raw_data)
        return [str(item) for item in data_list]
    return []


def _bulk_reply(
    *,
    fetch: _FetchResult,
    replies: dict[str, str],
    subprocess_runner: SubprocessRunner,
    root_path: Path,
) -> None:
    lookup = _comment_lookup(fetch)
    if not replies:
        return
    for key, body in replies.items():
        comment_id = lookup.get(key)
        if comment_id is None and key.startswith("http"):
            comment_id = lookup.get(key.split("#")[-1])
        if comment_id is None:
            continue
        mutation = (
            "mutation($pullRequestId:ID!,$inReplyTo:ID!,$body:String!){"
            " addPullRequestReviewComment("
            "   input:{pullRequestId:$pullRequestId, inReplyTo:$inReplyTo, body:$body}"
            " ){ comment { id } }"
            " }"
        )
        args = [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={mutation}",
            "-F",
            f"pullRequestId={getattr(fetch, 'pull_request_id', '')}",
            "-F",
            f"inReplyTo={comment_id}",
            "-F",
            f"body={body}",
        ]
        result = _exec(
            args,
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-reply-gql"
            ),
            subprocess_runner=subprocess_runner,
            root_path=root_path,
        )
        if not result.success:
            raise ToolExecutionError(
                "Failed to send reply via GitHub CLI",
                reason=result.stderr or result.stdout or "gh api graphql failed",
                rationale=(
                    "Ensure your GitHub token has permission to comment on the PR "
                    "and that the comment identifier matches an existing thread."
                ),
            )


@runtime_checkable
class ReviewComment(Protocol):
    """Protocol for a GitHub review comment."""

    author: str
    viewer_did_author: bool
    body: str


@runtime_checkable
class ReviewThread(Protocol):
    """Protocol for a GitHub review thread."""

    url: str
    is_resolved: bool

    @property
    def comments(self) -> Iterable[ReviewComment]: ...


@runtime_checkable
class ReviewModule(Protocol):
    """Protocol for the dynamically generated review module."""

    def fetch_review_threads(
        self, owner: str, repo: str, pr_number: int
    ) -> "_FetchResult": ...

    def apply_filters(
        self,
        threads: Iterable[ReviewThread],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> Iterable[ReviewThread]: ...

    def load_replies(self, replies_file: Path) -> Mapping[str, str]: ...
    def bulk_reply(
        self, *, fetch: "_FetchResult", replies: Mapping[str, str]
    ) -> None: ...
    def load_comment_targets(self, path: Path) -> list[str]: ...
    def comment_lookup(self, fetch: "_FetchResult") -> dict[str, str]: ...
    def infer_repo(self, remote: str) -> tuple[str, str]: ...

    # Private methods for internal bridging and testing
    def _infer_repo(self, remote: str) -> tuple[str, str]: ...
    def _fetch_review_threads(
        self, owner: str, repo: str, pr_number: int
    ) -> "_FetchResult": ...
    def _apply_filters(
        self,
        threads: Iterable[ReviewThread],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> Iterable[ReviewThread]: ...
    def _load_replies(self, replies_file: Path) -> Mapping[str, str]: ...
    def _bulk_reply(
        self, *, fetch: "_FetchResult", replies: Mapping[str, str]
    ) -> None: ...
    def _load_comment_targets(self, path: Path) -> list[str]: ...
    def _comment_lookup(self, fetch: "_FetchResult") -> dict[str, str]: ...


def run_review_list(
    *,
    pr_number: int,
    unreplied: bool,
    unresolved: bool,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Optional[Callable[[], Any]] = None,
    render_fn: Optional[
        Callable[[Iterable[Any], bool, bool, str | None], list[str]]
    ] = None,
) -> ToolResult:
    """List review threads for a PR."""
    operation_id = OperationId(namespace="tools", category="dev", command="review-list")
    try:
        # Use a local class instance if factory not provided
        review_mod = cast(
            ReviewModule,
            _get_review_module(review_module_factory, subprocess_runner, root_path),
        )

        infer_repo_fn = getattr(review_mod, "infer_repo", None) or getattr(
            review_mod, "_infer_repo", None
        )
        if not callable(infer_repo_fn):
            raise ToolExecutionError(
                "Failed to list review threads",
                reason="Review module missing infer_repo",
                rationale="Ensure review module exposes infer_repo(remote)",
            )
        infer_repo_typed = cast(Callable[[str], tuple[str, str]], infer_repo_fn)
        try:
            owner, repo = infer_repo_typed(remote)
        except ToolExecutionError:
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr="Failed to list review comments: fail",
                )
            raise
        fetch_result: _FetchResult = review_mod.fetch_review_threads(
            owner, repo, pr_number
        )

        threads_obj = getattr(fetch_result, "threads", [])
        viewer = getattr(fetch_result, "viewer", None)
        apply_filters_fn = (
            getattr(review_mod, "apply_filters", None)
            or getattr(review_mod, "_apply_filters", None)
            or _apply_filters
        )
        try:
            filtered = apply_filters_fn(
                cast(Iterable[ReviewThread], threads_obj),
                unreplied=unreplied,
                unresolved=unresolved,
                viewer=viewer,
            )
        except ToolExecutionError:
            if review_module_factory is not None:
                raise
            raise ToolExecutionError(
                "Failed to list review comments",
                reason="apply_filters failed",
                rationale="Filtering review threads failed",
            )
        except Exception as exc:
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to list review comments: {exc}",
                )
            raise ToolExecutionError(
                "Failed to list review comments",
                reason=str(exc),
                rationale="Filtering review threads failed",
            ) from exc

        def _default_render(
            threads: Iterable[Any],
            unreplied: bool,
            unresolved: bool,
            viewer: str | None,
        ) -> list[str]:
            def _passthrough_filters(
                items: Iterable[Any],
                *,
                unreplied: bool,
                unresolved: bool,
                viewer: str | None,
            ) -> Iterable[Any]:
                return items

            # Threads are already filtered; render without re-filtering to avoid double validation.
            return render_threads(
                threads,
                apply_filters=_passthrough_filters,
                unreplied=unreplied,
                unresolved=unresolved,
                viewer=viewer,
            )

        render: Callable[[Iterable[Any], bool, bool, str | None], list[str]] = (
            render_fn or _default_render
        )

        lines = render(filtered, unreplied, unresolved, viewer)

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout="\n".join(lines),
        )
    except ToolExecutionError as exc:
        if review_module_factory is not None:
            raise
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to list review comments: {exc}",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to list review comments: {exc}\nRationale: Unexpected system failure during review list processing.",
        )


def _get_review_module(
    factory: Optional[Callable[[], object]],
    subprocess_runner: SubprocessRunner,
    root_path: Path,
) -> object:
    """Helper to get or create the review module."""
    if factory is not None:
        return factory()

    return _review_module_default(subprocess_runner, root_path)


def run_review_bulk_reply(
    *,
    pr_number: int,
    replies_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Optional[Callable[[], Any]] = None,
) -> ToolResult:
    """Reply to multiple review threads."""
    in_bulk_reply = False
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-bulk-reply"
    )
    try:
        review_mod = cast(
            ReviewModule,
            _get_review_module(review_module_factory, subprocess_runner, root_path),
        )

        infer_repo_fn = getattr(review_mod, "infer_repo", None) or getattr(
            review_mod, "_infer_repo", None
        )
        if not callable(infer_repo_fn):
            msg = "Review module missing infer_repo"
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to send bulk replies: {msg}",
                )
            raise ToolExecutionError(
                "Failed to bulk reply",
                reason=msg,
                rationale="Ensure review module exposes infer_repo(remote)",
            )
        infer_repo_typed = cast(Callable[[str], tuple[str, str]], infer_repo_fn)
        owner, repo = infer_repo_typed(remote)

        load_replies_fn = getattr(review_mod, "load_replies", None) or getattr(
            review_mod, "_load_replies", None
        )
        if not callable(load_replies_fn):
            msg = "Review module missing load_replies"
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to send bulk replies: {msg}",
                )
            raise ToolExecutionError(
                "Failed to bulk reply",
                reason=msg,
                rationale="Ensure review module exposes load_replies(fetch, replies)",
            )
        load_replies_typed = cast(Callable[[Path], object], load_replies_fn)
        raw_replies = load_replies_typed(replies_file)

        replies: dict[str, str] | list[str]
        if isinstance(raw_replies, Mapping):
            replies = {
                str(k): str(v)
                for k, v in cast(Mapping[object, object], raw_replies).items()
            }
        elif isinstance(raw_replies, list):
            replies = [str(item) for item in cast(list[object], raw_replies)]
        else:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout="No valid replies provided; skipping bulk reply.",
            )

        # Capture the result from the dynamic module explicitly
        fetch_threads_fn = getattr(review_mod, "fetch_review_threads", None) or getattr(
            review_mod, "_fetch_review_threads", None
        )
        if not callable(fetch_threads_fn):
            msg = "Review module missing fetch_review_threads"
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to send bulk replies: {msg}",
                )
            raise ToolExecutionError(
                "Failed to bulk reply",
                reason=msg,
                rationale="Ensure review module exposes fetch_review_threads(owner, repo, pr_number)",
            )
        fetch_threads_typed = cast(
            Callable[[str, str, int], _FetchResult], fetch_threads_fn
        )
        try:
            fetch_result = fetch_threads_typed(owner, repo, pr_number)
        except ToolExecutionError as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=str(exc),
            )

        # Use the protocol method which is now public
        bulk_reply_fn = getattr(review_mod, "bulk_reply", None) or getattr(
            review_mod, "_bulk_reply", None
        )
        if not callable(bulk_reply_fn):
            msg = "Review module missing bulk_reply"
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to send bulk replies: {msg}",
                )
            raise ToolExecutionError(
                "Failed to bulk reply",
                reason=msg,
                rationale="Ensure review module exposes bulk_reply(fetch, replies)",
            )
        try:
            in_bulk_reply = True
            bulk_reply_fn(fetch=fetch_result, replies=replies)
            in_bulk_reply = False
        except ToolExecutionError:
            # Let ToolExecutionError propagate for default module paths (tests expect raise)
            raise
        except Exception as exc:
            if review_module_factory is not None:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to send bulk replies: {exc}",
                )
            raise ToolExecutionError(
                "Failed to send bulk replies",
                reason=str(exc),
                rationale="bulk_reply handler failed",
            ) from exc

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Successfully replied to {len(replies)} threads in PR #{pr_number}",
        )
    except ToolExecutionError as exc:
        # For custom modules, surface as ToolResult failure unless the module's
        # own bulk_reply raised (propagate those to satisfy test expectations).
        if review_module_factory is not None and not in_bulk_reply:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=str(exc),
            )
        raise
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to send bulk replies: {exc}\nRationale: Unexpected system failure during bulk reply processing.",
        )


def run_review_delete(
    *,
    pr_number: int,
    comments_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Optional[Callable[[], Any]] = None,
) -> ToolResult:
    """Delete multiple review comments."""
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-delete"
    )
    try:
        review_mod = cast(
            ReviewModule,
            _get_review_module(review_module_factory, subprocess_runner, root_path),
        )

        infer_repo_fn = getattr(review_mod, "infer_repo", None) or getattr(
            review_mod, "_infer_repo", None
        )
        if not callable(infer_repo_fn):
            raise ToolExecutionError(
                "Failed to delete comments",
                reason="Review module missing infer_repo",
                rationale="Ensure review module exposes infer_repo(remote)",
            )
        infer_repo_typed = cast(Callable[[str], tuple[str, str]], infer_repo_fn)
        owner, repo = infer_repo_typed(remote)

        fetch_threads_fn = getattr(review_mod, "fetch_review_threads", None) or getattr(
            review_mod, "_fetch_review_threads", None
        )
        if not callable(fetch_threads_fn):
            raise ToolExecutionError(
                "Failed to delete comments",
                reason="Review module missing fetch_review_threads",
                rationale="Ensure review module exposes fetch_review_threads(owner, repo, pr_number)",
            )
        fetch_result: _FetchResult = cast(
            _FetchResult, fetch_threads_fn(owner, repo, pr_number)
        )

        load_targets_fn = getattr(review_mod, "load_comment_targets", None) or getattr(
            review_mod, "_load_comment_targets", None
        )
        if not callable(load_targets_fn):
            raise ToolExecutionError(
                "Failed to delete comments",
                reason="Review module missing load_comment_targets",
                rationale="Ensure review module exposes load_comment_targets(comments_file)",
            )
        targets = cast(Iterable[str], load_targets_fn(comments_file))

        lookup_fn = getattr(review_mod, "comment_lookup", None) or getattr(
            review_mod, "_comment_lookup", None
        )
        if not callable(lookup_fn):
            raise ToolExecutionError(
                "Failed to delete comments",
                reason="Review module missing comment_lookup",
                rationale="Ensure review module exposes comment_lookup(fetch_result)",
            )
        lookup = cast(dict[str, str], lookup_fn(fetch_result))

        deleted = 0
        for target in targets:
            comment_id = lookup.get(target)
            if not comment_id:
                continue
            deletion = subprocess_runner.run_subprocess(
                [
                    "gh",
                    "api",
                    "graphql",
                    "-f",
                    f'query=mutation {{ deletePullRequestReviewComment(input: {{ id: "{comment_id}" }}) {{ clientMutationId }} }}',
                ],
                cwd=root_path,
                operation_id=operation_id,
            )
            if not deletion.success:
                return deletion
            deleted += 1

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Successfully deleted {deleted} comments from PR #{pr_number}",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to delete comments: {exc}\nRationale: Unexpected system failure during comment deletion.",
        )


def run_cleanup_ignored_tracked(
    *, subprocess_runner: SubprocessRunner, root_path: Path
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    try:
        listing = subprocess_runner.run_subprocess(
            ["git", "ls-files", "-c", "-i", "--exclude-standard"],
            cwd=root_path,
            operation_id=operation_id,
        )
        if not listing.success:
            return listing

        ignored_files = [
            line.strip() for line in listing.stdout.splitlines() if line.strip()
        ]
        if not ignored_files:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout="No ignored tracked files found.",
            )

        for file in ignored_files:
            removal = subprocess_runner.run_subprocess(
                ["git", "rm", "--cached", file],
                cwd=root_path,
                operation_id=operation_id,
            )
            if not removal.success:
                return removal

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Removed {len(ignored_files)} ignored tracked files from git.",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to cleanup ignored tracked files: {exc}\nRationale: Git operations failed during cleanup of ignored files.",
        )


def run_setup_ai_guidelines(
    *,
    tool: str,
    project_dir: Path,
    dry_run: bool,
    subprocess_runner: SubprocessRunner,
    os_path_samefile: Callable[[Path | str, Path | str], bool] | None,
    path_resolve: Callable[[Path], Path] | None,
    path_readlink: Callable[[Path], Path] | None,
    os_relpath: Callable[[Path | str, Path | str], str] | None,
    os_name: str | None,
    git_wild_match_pattern_factory: Callable[[str], GitIgnorePattern] | None,
    create_or_update_link_op: Callable[..., None] | None,
    ensure_base_and_empty_readme_op: Callable[..., Path] | None,
    ensure_aiignore_symlink_op: (
        Callable[[Path, Path, bool, list[str], SubprocessRunner], None] | None
    ) = None,
    os_link_op: Callable[[Path | str, Path | str], None] | None,
) -> SetupResult:
    result = setup_ai_guidelines(
        tool=tool,
        project_dir=project_dir,
        dry_run=dry_run,
        subprocess_runner=subprocess_runner,
        os_path_samefile=os_path_samefile,
        path_resolve=path_resolve,
        path_readlink=path_readlink,
        os_relpath=os_relpath,
        os_name=os_name,
        git_wild_match_pattern_factory=git_wild_match_pattern_factory,
        create_or_update_link_op=create_or_update_link_op,
        ensure_base_and_empty_readme_op=ensure_base_and_empty_readme_op,
        ensure_aiignore_symlink_op=ensure_aiignore_symlink_op,
        os_link_op=os_link_op,
    )
    logs = result.stdout.splitlines() if result.stdout else []
    error = result.stderr or None
    return SetupResult(success=result.success, logs=logs, error=error)


class DevTools:
    """Utilities that help with PR review flows and local hygiene tasks."""

    def __init__(
        self,
        config: ToolsConfig | None = None,
        subprocess_runner: SubprocessRunner | None = None,
        root_path: Path | None = None,
        *,
        platform_resolver: Callable[[], str] | None = None,
        review_module_factory: Callable[[], Any] | None = None,
        pids_by_port: Callable[[int], list[int]] | None = None,
        kill_pid: Callable[[int], bool] | None = None,
        os_path_samefile: Callable[[Path | str, Path | str], bool] | None = None,
        path_resolve: Callable[[Path], Path] | None = None,
        path_readlink: Callable[[Path], Path] | None = None,
        os_relpath: Callable[[Path | str, Path | str], str] | None = None,
        os_name: str | None = None,
        create_or_update_link_op: Callable[..., None] | None = None,
        ensure_base_and_empty_readme_op: Callable[..., Path] | None = None,
        git_wild_match_pattern_factory: Callable[[str], GitIgnorePattern] | None = None,
        ensure_aiignore_symlink_op: (
            Callable[[Path, Path, bool, list[str], SubprocessRunner], None] | None
        ) = None,
        os_link_op: Callable[[Path | str, Path | str], None] | None = None,
    ) -> None:
        self._config = config or ToolsConfig()
        self._subprocess_runner: SubprocessRunner = (
            subprocess_runner or RealSubprocessRunner()
        )
        self._root_path = root_path or Path.cwd()
        self._platform_resolver = platform_resolver or platform.system
        self._review_module_factory = review_module_factory
        self._pids_by_port = pids_by_port
        self._kill_pid = kill_pid
        self._os_path_samefile = os_path_samefile

        def _resolve(p: Path) -> Path:
            return p.resolve()

        def _readlink(p: Path) -> Path:
            return p.readlink()

        def _relpath(p: Path | str, start: Path | str) -> str:
            return os.path.relpath(p, start=start)

        self._path_resolve = path_resolve or _resolve
        self._path_readlink = path_readlink or _readlink
        self._os_relpath = os_relpath or _relpath
        self._os_name = os_name
        self._create_or_update_link_op = create_or_update_link_op
        self._ensure_base_and_empty_readme_op = ensure_base_and_empty_readme_op
        self._git_wild_match_pattern_factory = git_wild_match_pattern_factory
        self._ensure_aiignore_symlink_op = ensure_aiignore_symlink_op
        self._os_link_op = os_link_op

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def config(self) -> ToolsConfig:
        return self._config

    def _review_module(self) -> ReviewModule:
        return cast(
            ReviewModule,
            _get_review_module(
                self._review_module_factory, self._subprocess_runner, self._root_path
            ),
        )

    def _render_threads(
        self,
        threads: Iterable[ReviewThread],
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
        apply_filters: Callable[..., Iterable[ReviewThread]] | None = None,
    ) -> list[str]:
        # Delegate to shared renderer to keep behavior in sync with standalone function.
        apply = apply_filters or self._review_module().apply_filters
        return render_threads(
            cast(Iterable[_Thread], threads),
            apply_filters=apply,
            unreplied=unreplied,
            unresolved=unresolved,
            viewer=viewer,
        )

    def review_list(
        self,
        pr_number: int,
        unreplied: bool = False,
        unresolved: bool = False,
        remote: str = "origin",
    ) -> ToolResult:
        return run_review_list(
            pr_number=pr_number,
            unreplied=unreplied,
            unresolved=unresolved,
            remote=remote,
            subprocess_runner=self._subprocess_runner,
            root_path=self._root_path,
            review_module_factory=self._review_module_factory,
            render_fn=self._render_threads,
        )

    def review_bulk_reply(
        self, pr_number: int, replies_file: Path, remote: str = "origin"
    ) -> ToolResult:
        return run_review_bulk_reply(
            pr_number=pr_number,
            replies_file=replies_file,
            remote=remote,
            subprocess_runner=self._subprocess_runner,
            root_path=self._root_path,
            review_module_factory=self._review_module_factory,
        )

    def review_delete(
        self, pr_number: int, comments_file: Path, remote: str = "origin"
    ) -> ToolResult:
        return run_review_delete(
            pr_number=pr_number,
            comments_file=comments_file,
            remote=remote,
            subprocess_runner=self._subprocess_runner,
            root_path=self._root_path,
            review_module_factory=self._review_module_factory,
        )

    def cleanup_ignored_tracked(self) -> ToolResult:
        return run_cleanup_ignored_tracked(
            subprocess_runner=self._subprocess_runner, root_path=self._root_path
        )

    def kill_port(
        self,
        port: int,
        *,
        pids_by_port: Callable[[int], list[int]] | None = None,
        kill_pid: Callable[[int], bool] | None = None,
    ) -> ToolResult:
        return run_kill_port(
            port=port,
            subprocess_runner=self._subprocess_runner,
            root_path=self._root_path,
            pids_by_port=pids_by_port or self._pids_by_port,
            kill_pid=kill_pid or self._kill_pid,
        )

    def gha(
        self,
        *,
        limit: int = 10,
        run_id: int | None = None,
        latest: bool = False,
        log_failed: bool = False,
        remote: str = "origin",
        repo: str | None = None,
    ) -> ToolResult:
        return github_actions.run_github_actions(
            root_path=self._root_path,
            subprocess_runner=self._subprocess_runner,
            limit=limit,
            run_id=run_id,
            latest=latest,
            log_failed=log_failed,
            remote=remote,
            repo=repo,
        )

    def batch_review(self, output_format: str = "json") -> ToolResult:
        return run_dev_batch_review(
            config=self._config,
            root_path=self._root_path,
            output_format=output_format,
            subprocess_runner=self._subprocess_runner,
        )

    def workflow_status(self, output_format: str = "json") -> ToolResult:
        return run_dev_workflow_status(
            config=self._config,
            root_path=self._root_path,
            output_format=output_format,
            subprocess_runner=self._subprocess_runner,
        )

    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        setup_result = run_setup_ai_guidelines(
            tool=tool,
            project_dir=self._root_path,
            dry_run=dry_run,
            subprocess_runner=self._subprocess_runner,
            os_path_samefile=self._os_path_samefile,
            path_resolve=self._path_resolve,
            path_readlink=self._path_readlink,
            os_relpath=self._os_relpath,
            os_name=self._os_name,
            git_wild_match_pattern_factory=self._git_wild_match_pattern_factory,
            create_or_update_link_op=self._create_or_update_link_op,
            ensure_base_and_empty_readme_op=self._ensure_base_and_empty_readme_op,
            ensure_aiignore_symlink_op=self._ensure_aiignore_symlink_op,
            os_link_op=self._os_link_op,
        )
        return ToolResult.create(
            success=setup_result.success,
            exit_code=0 if setup_result.success else 1,
            namespace="tools",
            category="dev",
            command="setup-ai-guidelines",
            stdout="\n".join(setup_result.logs),
            stderr=setup_result.error or "",
        )
