"""PR review management utilities for the tools CLI."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Protocol, TypeAlias, cast

from ..core.errors import ToolExecutionError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


@dataclass
class ReviewComment:
    author: str
    viewer_did_author: bool
    body: str
    url: str
    id: str
    database_id: int | None = None
    created_at: str | None = None


@dataclass
class ReviewThread:
    id: str
    url: str
    is_resolved: bool
    comments: list[ReviewComment]


@dataclass
class ReviewFetchResult:
    threads: Sequence[ReviewThread]
    viewer: str | None


ThreadInput: TypeAlias = Iterable[ReviewThread | object]


class FetchLike(Protocol):
    threads: Sequence[ReviewThread]


def _coerce_comment(obj: object) -> ReviewComment | None:
    raw_author = getattr(obj, "author", "")
    raw_body = getattr(obj, "body", "")
    raw_viewer = getattr(obj, "viewer_did_author", False)
    raw_url = getattr(obj, "url", "")
    raw_cid = getattr(obj, "id", "")
    raw_dbid = getattr(obj, "database_id", None)
    raw_created = getattr(obj, "created_at", None)

    author = raw_author if isinstance(raw_author, str) else str(raw_author or "")
    body = raw_body if isinstance(raw_body, str) else str(raw_body or "")
    url_val = raw_url if isinstance(raw_url, str) else str(raw_url or "")
    cid = raw_cid if isinstance(raw_cid, str) else str(raw_cid or "")
    viewer_flag = bool(raw_viewer)
    database_id = raw_dbid if isinstance(raw_dbid, int) else None
    created_at = raw_created if isinstance(raw_created, str) else None

    return ReviewComment(
        author=author,
        viewer_did_author=viewer_flag,
        body=body,
        url=url_val,
        id=cid,
        database_id=database_id,
        created_at=created_at,
    )


def _coerce_thread(obj: object) -> ReviewThread | None:
    if isinstance(obj, ReviewThread):
        return obj
    raw_url = getattr(obj, "url", "")
    url = raw_url if isinstance(raw_url, str) else str(raw_url or "")
    is_resolved = bool(getattr(obj, "is_resolved", False))
    comments_raw_obj = getattr(obj, "comments", [])
    if comments_raw_obj is None:
        comments_raw_obj = []
    if not isinstance(comments_raw_obj, list):
        return None
    comments_raw: list[object] = cast(list[object], comments_raw_obj)
    comments: list[ReviewComment] = []
    for comment_obj in comments_raw:
        cm = _coerce_comment(comment_obj)
        if cm is not None:
            comments.append(cm)
    return ReviewThread(id="", url=url, is_resolved=is_resolved, comments=comments)


def _normalize_threads(threads: ThreadInput) -> list[ReviewThread]:
    normalized: list[ReviewThread] = []
    for t in threads:
        ct = _coerce_thread(t)
        if ct is not None:
            normalized.append(ct)
    return normalized


def run_review_list(
    pr_number: int,
    remote: str,
    unreplied: bool,
    unresolved: bool,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], "ReviewModule"] | None = None,
) -> ToolResult:
    """List GitHub PR review comments with optional filtering."""
    operation_id = OperationId(namespace="tools", category="dev", command="review-list")
    try:
        review: ReviewModule = _resolve_review_module(
            subprocess_runner=subprocess_runner,
            root_path=root_path,
            review_module_factory=review_module_factory,
        )
        owner, repo = review.infer_repo(remote)
        fetch_result = review.fetch_review_threads(owner, repo, pr_number)
        output_lines = review.render_threads(
            fetch_result.threads,
            apply_filters=review.apply_filters,
            unreplied=unreplied,
            unresolved=unresolved,
            viewer=fetch_result.viewer,
        )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout="\n".join(output_lines),
        )
    except ToolExecutionError:
        raise
    except (OSError, ValueError) as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to list review comments: {exc}",
        )


def run_review_bulk_reply(
    pr_number: int,
    replies_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], "ReviewModule"] | None = None,
) -> ToolResult:
    """Bulk reply to GitHub PR review comments."""
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-bulk-reply"
    )
    try:
        review = _resolve_review_module(
            subprocess_runner=subprocess_runner,
            root_path=root_path,
            review_module_factory=review_module_factory,
        )
        owner, repo = review.infer_repo(remote)
        fetch_result = review.fetch_review_threads(owner, repo, pr_number)
        replies = review.load_replies(replies_file)
        review.bulk_reply(fetch=fetch_result, replies=replies)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Successfully sent bulk replies to PR #{pr_number}",
        )
    except ToolExecutionError:
        raise
    except (OSError, ValueError) as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to send bulk replies: {exc}",
        )


def run_review_delete(
    pr_number: int,
    comments_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], "ReviewModule"] | None = None,
) -> ToolResult:
    """Delete GitHub PR review comments."""
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-delete"
    )
    try:
        review = ReviewModule(subprocess_runner, root_path)
        owner, repo = review.infer_repo(remote)
        fetch_result = review.fetch_review_threads(owner, repo, pr_number)
        targets = review.load_comment_targets(comments_file)
        lookup = review.comment_lookup(fetch_result)

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
    except ToolExecutionError:
        raise
    except (OSError, ValueError) as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to delete comments: {exc}",
        )


def run_review_resolve(
    pr_number: int,
    threads_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], "ReviewModule"] | None = None,
) -> ToolResult:
    """Resolve GitHub PR review threads."""
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-resolve"
    )
    try:
        review = _resolve_review_module(
            subprocess_runner=subprocess_runner,
            root_path=root_path,
            review_module_factory=review_module_factory,
        )
        owner, repo = review.infer_repo(remote)
        fetch_result = review.fetch_review_threads(owner, repo, pr_number)
        targets = review.load_comment_targets(threads_file)
        review.bulk_resolve(fetch=fetch_result, targets=targets)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Successfully resolved review threads for PR #{pr_number}",
        )
    except ToolExecutionError:
        raise
    except (OSError, ValueError) as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to resolve review threads: {exc}",
        )


class ReviewModule:
    """Review module for GitHub PR operations."""

    def __init__(self, subprocess_runner: SubprocessRunner, root_path: Path):
        self._subprocess_runner = subprocess_runner
        self._root_path = root_path

    def _exec(self, args: list[str], *, operation_id: OperationId) -> ToolResult:
        return self._subprocess_runner.run_subprocess(
            args, cwd=self._root_path, operation_id=operation_id
        )

    class _Comment:
        def __init__(
            self,
            *,
            author: str,
            viewer_did_author: bool,
            body: str,
            url: str | None = None,
            id: str | None = None,
            database_id: int | None = None,
            created_at: str | None = None,
        ) -> None:
            self.author = author
            self.viewer_did_author = viewer_did_author
            self.body = body
            self.url = url or ""
            self.id = id or ""
            self.database_id = database_id
            self.created_at = created_at

    class _Thread:
        def __init__(
            self, id: str, url: str, is_resolved: bool, comments: list[Any]
        ) -> None:
            self.id = id
            self.url = url
            self.is_resolved = is_resolved
            self.comments = comments

    class _FetchResult:
        def __init__(self, threads: list[Any], viewer: str | None) -> None:
            self.threads = threads
            self.viewer = viewer

    def infer_repo(self, remote: str) -> tuple[str, str]:
        res = self._exec(
            ["git", "remote", "get-url", remote],
            operation_id=OperationId(
                namespace="tools", category="dev", command="review-infer-repo"
            ),
        )
        if not res.success or not res.stdout.strip():
            gh = self._exec(
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

    def fetch_review_threads(
        self, owner: str, repo: str, pr_number: int
    ) -> ReviewFetchResult:
        query = (
            "query($owner:String!,$repo:String!,$pr:Int!){"
            " viewer { login }"
            " repository(owner:$owner,name:$repo){"
            "   pullRequest(number:$pr){"
            "     reviewThreads(first:100){ nodes {"
            "       id"
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
        res = self._exec(args, operation_id=op_id)
        if not res.success:
            raise ToolExecutionError(
                "Failed to fetch review threads",
                reason=res.stderr or "gh api graphql failed",
                rationale="Ensure gh is installed and you have permissions to view the PR.",
            )

        def _as_dict(obj: object) -> dict[str, object]:
            if isinstance(obj, Mapping):
                mapping_obj = cast(Mapping[str, object], obj)
                result: dict[str, object] = {}
                for key, value in mapping_obj.items():
                    result[str(key)] = value
                return result
            return {}

        def _as_list(obj: object) -> list[object]:
            if isinstance(obj, list):
                return list(cast(list[object], obj))
            return []

        data_raw = cast(object, json.loads(res.stdout or "{}"))
        data: Mapping[str, object] = (
            cast(Mapping[str, object], data_raw)
            if isinstance(data_raw, Mapping)
            else {}
        )
        root = _as_dict(_as_dict(data).get("data"))
        viewer_login = _as_dict(root.get("viewer")).get("login")
        viewer: str | None = (
            str(viewer_login) if isinstance(viewer_login, str) else None
        )
        repo_obj = _as_dict(root.get("repository"))
        pr_obj = _as_dict(repo_obj.get("pullRequest"))
        review_threads = _as_dict(pr_obj.get("reviewThreads"))
        threads_json = _as_list(review_threads.get("nodes"))

        threads: list[ReviewThread] = []
        for t in threads_json:
            tdict = _as_dict(t)
            thread_id = str(tdict.get("id", ""))
            is_resolved = bool(tdict.get("isResolved", False))
            comments_nodes = _as_list(_as_dict(tdict.get("comments")).get("nodes"))
            comments: list[ReviewComment] = []
            for c in comments_nodes:
                cdict = _as_dict(c)
                author_login = _as_dict(cdict.get("author")).get("login") or ""
                comments.append(
                    ReviewComment(
                        author=str(author_login)
                        if isinstance(author_login, str)
                        else "",
                        viewer_did_author=(
                            viewer is not None
                            and isinstance(author_login, str)
                            and author_login == viewer
                        ),
                        body=str(cdict.get("body"))
                        if isinstance(cdict.get("body"), str)
                        else "",
                        url=str(cdict.get("url"))
                        if isinstance(cdict.get("url"), str)
                        else "",
                        id=str(cdict.get("id"))
                        if isinstance(cdict.get("id"), str)
                        else "",
                        database_id=(
                            int(db_val)
                            if isinstance((db_val := cdict.get("databaseId")), int)
                            else None
                        ),
                        created_at=(
                            str(created_val)
                            if isinstance((created_val := cdict.get("createdAt")), str)
                            else None
                        ),
                    )
                )
            thread_url = comments[0].url if comments else ""
            threads.append(
                ReviewThread(
                    id=thread_id,
                    url=thread_url,
                    is_resolved=is_resolved,
                    comments=comments,
                )
            )

        return ReviewFetchResult(threads=threads, viewer=viewer)

    def apply_filters(
        self,
        threads: ThreadInput,
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[ReviewThread]:
        normalized = _normalize_threads(threads)
        items: list[ReviewThread] = []
        for th in normalized:
            if unresolved and th.is_resolved:
                continue
            if unreplied:
                has_viewer_comment = any(c.viewer_did_author for c in th.comments)
                if has_viewer_comment:
                    continue
            items.append(th)
        return items

    def render_threads(
        self,
        threads: ThreadInput,
        *,
        apply_filters: Callable[..., Iterable[ReviewThread]],
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[str]:
        normalized_threads = _normalize_threads(list(threads))
        filtered = list(
            apply_filters(
                normalized_threads,
                unreplied=unreplied,
                unresolved=unresolved,
                viewer=viewer,
            )
        )

        lines: list[str] = []
        found = False
        for thread in filtered:
            found = True
            thread_url = thread.url
            lines.append(f"Thread: {thread_url}")
            is_resolved = thread.is_resolved
            lines.append(
                "  Status: Resolved" if is_resolved else "  Status: Unresolved"
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

    def load_replies(self, replies_file: Path) -> dict[str, str]:
        text = replies_file.read_text()
        data_raw = cast(object, json.loads(text or "{}"))
        if not isinstance(data_raw, Mapping):
            return {}
        data: Mapping[str, object] = cast(Mapping[str, object], data_raw)
        mapping: dict[str, str] = {}
        for key_obj, value_obj in data.items():
            if isinstance(value_obj, str):
                mapping[key_obj] = value_obj
        return mapping

    def bulk_reply(self, *, fetch: FetchLike, replies: dict[str, str]) -> None:
        # Resolve identifiers to GraphQL thread IDs from review-list output and reply via GraphQL
        # Note: addPullRequestReviewComment is deprecated; use addPullRequestReviewThreadReply
        lookup = self._thread_lookup(fetch)
        if not replies:
            return
        for key, body in replies.items():
            thread_id = lookup.get(key)
            if thread_id is None and key.startswith("http"):
                thread_id = lookup.get(key.split("#")[-1])
            if thread_id is None:
                continue
            mutation = (
                "mutation($threadId:ID!,$body:String!){"
                " addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:$threadId, body:$body}){"
                "   comment { id }"
                " }"
                " }"
            )
            args = [
                "gh",
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-F",
                f"threadId={thread_id}",
                "-F",
                f"body={body}",
            ]
            result = self._exec(
                args,
                operation_id=OperationId(
                    namespace="tools", category="dev", command="review-reply-gql"
                ),
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

    def bulk_resolve(self, *, fetch: FetchLike, targets: list[str]) -> None:
        """Resolve review threads identified by URL/ID keys."""
        lookup = self._thread_lookup(fetch)
        if not targets:
            return
        mutation = (
            "mutation($threadId:ID!){"
            " resolveReviewThread(input:{threadId:$threadId}){"
            "  thread { id isResolved }"
            " }"
            "}"
        )
        for key in targets:
            thread_id = lookup.get(key)
            if thread_id is None and key.startswith("http"):
                thread_id = lookup.get(key.split("#")[-1])
            if thread_id is None:
                continue
            args = [
                "gh",
                "api",
                "graphql",
                "-f",
                f"query={mutation}",
                "-F",
                f"threadId={thread_id}",
            ]
            result = self._exec(
                args,
                operation_id=OperationId(
                    namespace="tools", category="dev", command="review-resolve-gql"
                ),
            )
            if not result.success:
                raise ToolExecutionError(
                    "Failed to resolve review thread",
                    reason=result.stderr or result.stdout or "gh api graphql failed",
                    rationale=(
                        "Ensure your GitHub token has permission to resolve threads on the PR "
                        "and that the provided identifier matches an existing thread."
                    ),
                )

    def load_comment_targets(self, path: Path) -> list[str]:
        import json

        data_raw = cast(object, json.loads(path.read_text() or "[]"))
        if not isinstance(data_raw, list):
            return []
        data_list: list[object] = cast(list[object], data_raw)
        items: list[str] = []
        for x in data_list:
            if isinstance(x, str):
                items.append(x)
        return items

    def thread_lookup(self, fetch: FetchLike) -> dict[str, str]:
        return self._thread_lookup(fetch)

    def _thread_lookup(self, fetch: FetchLike) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for th in fetch.threads:
            thread_id = th.id
            if not thread_id:
                continue
            for cm in th.comments:
                comment_id = cm.id
                comment_url = cm.url
                if comment_id:
                    mapping.setdefault(comment_id, thread_id)
                if comment_url:
                    mapping.setdefault(comment_url, thread_id)
                    if "#" in comment_url:
                        anchor = comment_url.split("#")[-1]
                        if anchor:
                            mapping.setdefault(anchor, thread_id)
        return mapping

    def comment_lookup(self, fetch: FetchLike) -> dict[str, str]:
        return self._comment_lookup(fetch)

    def _comment_lookup(self, fetch: FetchLike) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for th in fetch.threads:
            for cm in th.comments:
                comment_id = cm.id
                if comment_id:
                    mapping.setdefault(comment_id, comment_id)
                comment_url = cm.url
                if comment_url:
                    mapping.setdefault(comment_url, comment_id)
                    if "#" in comment_url:
                        anchor = comment_url.split("#")[-1]
                        if anchor:
                            mapping.setdefault(anchor, comment_id)
                if cm.database_id is not None:
                    mapping.setdefault(str(cm.database_id), comment_id)
        return mapping


def _resolve_review_module(
    *,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], "ReviewModule"] | None,
) -> "ReviewModule":
    if review_module_factory is not None:
        return review_module_factory()
    return ReviewModule(subprocess_runner, root_path)
