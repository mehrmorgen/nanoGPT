"""PR review management utilities for the tools CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol, Iterable, Callable, cast

from ..core.errors import ToolExecutionError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner


class _ReviewModuleProtocol(Protocol):
    def infer_repo(self, remote: str) -> tuple[str, str]: ...

    def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> Any: ...

    def apply_filters(
        self,
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[Any]: ...

    def render_threads(
        self,
        threads: Iterable[Any],
        *,
        apply_filters: Callable[..., list[Any]],
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[str]: ...

    def load_replies(self, replies_file: Path) -> dict[str, str]: ...

    def bulk_reply(self, *, fetch: Any, replies: dict[str, str]) -> None: ...

    def load_comment_targets(self, path: Path) -> list[str]: ...

    def comment_lookup(self, fetch: Any) -> dict[str, str]: ...


def run_review_list(
    pr_number: int,
    remote: str,
    unreplied: bool,
    unresolved: bool,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], object] | None = None,
) -> ToolResult:
    """List GitHub PR review comments with optional filtering."""
    operation_id = OperationId(namespace="tools", category="dev", command="review-list")
    try:
        review = _resolve_review_module(
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
            viewer=getattr(fetch_result, "viewer", None),
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
    review_module_factory: Callable[[], object] | None = None,
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
    review_module_factory: Callable[[], object] | None = None,
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
                    f'query=mutation {{ deleteIssueComment(input: {{ id: "{comment_id}" }}) {{ clientMutationId }} }}',
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


class ReviewModule:
    """Review module for GitHub PR operations."""

    def __init__(self, subprocess_runner: SubprocessRunner, root_path: Path):
        self.subprocess_runner = subprocess_runner
        self.root_path = root_path

    def _exec(self, args: list[str], *, operation_id: OperationId) -> ToolResult:
        return self.subprocess_runner.run_subprocess(
            args, cwd=self.root_path, operation_id=operation_id
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
        def __init__(self, url: str, is_resolved: bool, comments: list[Any]) -> None:
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

    def fetch_review_threads(self, owner: str, repo: str, pr_number: int) -> Any:
        query = (
            "query($owner:String!,$repo:String!,$pr:Int!){"
            " viewer { login }"
            " repository(owner:$owner,name:$repo){"
            "   pullRequest(number:$pr){"
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
        res = self._exec(args, operation_id=op_id)
        if not res.success:
            raise ToolExecutionError(
                "Failed to fetch review threads",
                reason=res.stderr or "gh api graphql failed",
                rationale="Ensure gh is installed and you have permissions to view the PR.",
            )

        def _as_dict(obj: Any) -> dict[str, Any]:
            result: dict[str, Any] = {}
            if isinstance(obj, dict):
                for k, v in cast(Mapping[Any, Any], obj).items():
                    if isinstance(k, str):
                        result[k] = v
            return result

        def _as_list(obj: Any) -> list[Any]:
            return cast(list[Any], obj) if isinstance(obj, list) else []

        data = json.loads(res.stdout or "{}")
        root = _as_dict(_as_dict(data).get("data"))
        viewer_login = _as_dict(root.get("viewer")).get("login")
        viewer: str | None = (
            str(viewer_login) if isinstance(viewer_login, str) else None
        )
        repo_obj = _as_dict(root.get("repository"))
        pr_obj = _as_dict(repo_obj.get("pullRequest"))
        review_threads = _as_dict(pr_obj.get("reviewThreads"))
        threads_json = _as_list(review_threads.get("nodes"))

        threads: list[Any] = []
        for t in threads_json:
            tdict = _as_dict(t)
            is_resolved = bool(tdict.get("isResolved", False))
            comments_nodes = _as_list(_as_dict(tdict.get("comments")).get("nodes"))
            comments: list[ReviewModule._Comment] = []
            for c in comments_nodes:
                cdict = _as_dict(c)
                author_login = _as_dict(cdict.get("author")).get("login") or ""
                comments.append(
                    ReviewModule._Comment(
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
                        database_id=cdict.get("databaseId"),
                        created_at=cdict.get("createdAt"),
                    )
                )
            thread_url = comments[0].url if comments else ""
            threads.append(
                ReviewModule._Thread(
                    url=thread_url, is_resolved=is_resolved, comments=comments
                )
            )

        return ReviewModule._FetchResult(threads=threads, viewer=viewer)

    def apply_filters(
        self,
        threads: Iterable[Any],
        *,
        unreplied: bool,
        unresolved: bool,
        viewer: str | None,
    ) -> list[Any]:
        items: list[Any] = []
        for th in threads:
            if unresolved and getattr(th, "is_resolved", False):
                continue
            if unreplied:
                has_viewer_comment = any(
                    getattr(c, "viewer_did_author", False)
                    for c in getattr(th, "comments", [])
                )
                if has_viewer_comment:
                    continue
            items.append(th)
        return items

    def render_threads(
        self,
        threads: Iterable[Any],
        *,
        apply_filters: Callable[..., list[Any]],
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
        for thread in filtered:
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

    def load_replies(self, replies_file: Path) -> dict[str, str]:
        import json

        text = replies_file.read_text()
        data = json.loads(text or "{}")
        if not isinstance(data, dict):
            # Treat invalid format as empty mapping (no-op)
            return {}
        # coerce to str->str
        mapping: dict[str, str] = {}
        from typing import Any as _Any, Mapping as _Mapping

        for k, v in cast(_Mapping[_Any, _Any], data).items():
            if isinstance(k, str) and isinstance(v, str):
                mapping[k] = v
        return mapping

    def bulk_reply(self, *, fetch: Any, replies: dict[str, str]) -> None:
        # Resolve identifiers to GraphQL comment IDs from review-list output and reply via GraphQL
        lookup = self.comment_lookup(fetch)
        if not replies:
            return
        for key, body in replies.items():
            comment_id = lookup.get(key)
            if comment_id is None and key.startswith("http"):
                comment_id = lookup.get(key.split("#")[-1])
            if comment_id is None:
                continue
            mutation = (
                "mutation($inReplyTo:ID!,$body:String!){"
                " addPullRequestReviewComment(input:{inReplyTo:$inReplyTo, body:$body}){"
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
                f"inReplyTo={comment_id}",
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

    def load_comment_targets(self, path: Path) -> list[str]:
        import json

        data = json.loads(path.read_text() or "[]")
        if not isinstance(data, list):
            return []
        data_list = cast(list[Any], data)
        items: list[str] = []
        for x in data_list:
            if isinstance(x, str):
                items.append(x)
        return items

    def comment_lookup(self, fetch: Any) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for th in getattr(fetch, "threads", []) or []:
            for cm in getattr(th, "comments", []) or []:
                if getattr(cm, "id", None):
                    mapping.setdefault(cm.id, cm.id)
                if getattr(cm, "url", None):
                    mapping.setdefault(cm.url, cm.id)
                    if "#" in cm.url:
                        mapping.setdefault(cm.url.split("#")[-1], cm.id)
                if getattr(cm, "database_id", None) is not None:
                    mapping.setdefault(str(cm.database_id), cm.id)
        return mapping


def _resolve_review_module(
    *,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], object] | None,
) -> _ReviewModuleProtocol:
    if review_module_factory is not None:
        return cast(_ReviewModuleProtocol, review_module_factory())
    return ReviewModule(subprocess_runner, root_path)
