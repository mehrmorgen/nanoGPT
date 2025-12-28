"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

import json
import os
import platform
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, cast

from pydantic import BaseModel, ConfigDict

import psutil

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev.ai_guidelines import SetupResult, setup_ai_guidelines
from ml_playground.tools.dev import batch_review as batch_review_module
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
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
        data = cast(Dict[str, Any], json.loads(res.stdout or "{}"))
    except json.JSONDecodeError as e:
        raise ToolExecutionError(
            "Failed to parse GitHub CLI output",
            reason=str(e),
            rationale="The output from 'gh api graphql' was not valid JSON.",
        ) from e

    data_content = data.get("data")
    if not isinstance(data_content, dict):
        return _FetchResult(threads=[], viewer=None)

    viewer_node = cast(dict[str, Any], data_content).get("viewer")
    viewer: str | None = None
    if isinstance(viewer_node, dict):
        viewer_login = cast(dict[str, Any], viewer_node).get("login")
        if isinstance(viewer_login, str):
            viewer = viewer_login
        else:
            pass

    repo_node = cast(dict[str, Any], data_content).get("repository")
    if not isinstance(repo_node, dict):
        return _FetchResult(threads=[], viewer=viewer)

    pr_node = cast(dict[str, Any], repo_node).get("pullRequest")
    if not isinstance(pr_node, dict):
        return _FetchResult(threads=[], viewer=viewer)

    review_threads_node = cast(dict[str, Any], pr_node).get("reviewThreads")
    if not isinstance(review_threads_node, dict):
        return _FetchResult(threads=[], viewer=viewer)

    thread_nodes = cast(dict[str, Any], review_threads_node).get("nodes")
    if not isinstance(thread_nodes, list):
        return _FetchResult(threads=[], viewer=viewer)

    threads: list[_Thread] = []
    thread_nodes_list = cast(list[Any], thread_nodes)
    for t_node in thread_nodes_list:
        if not isinstance(t_node, dict):
            continue

        t_dict = cast(dict[str, Any], t_node)
        is_resolved: bool = bool(t_dict.get("isResolved", False))
        comments_wrapper = t_dict.get("comments")
        if not isinstance(comments_wrapper, dict):
            continue

        c_wrapper_dict = cast(dict[str, Any], comments_wrapper)
        comment_nodes = c_wrapper_dict.get("nodes")
        if not isinstance(comment_nodes, list):
            continue

        comments: list[_Comment] = []
        comment_nodes_list = cast(list[Any], comment_nodes)
        for c_node in comment_nodes_list:
            if not isinstance(c_node, dict):
                continue

            c_dict = cast(dict[str, Any], c_node)
            author_wrapper = c_dict.get("author")
            author_login = ""
            if isinstance(author_wrapper, dict):
                login = cast(dict[str, Any], author_wrapper).get("login")
                if isinstance(login, str):
                    author_login = login

            comments.append(
                _Comment(
                    author=author_login,
                    viewer_did_author=(viewer is not None and author_login == viewer),
                    body=str(c_dict.get("body") or ""),
                    url=str(c_dict.get("url") or ""),
                    id=str(c_dict.get("id") or ""),
                    database_id=c_dict.get("databaseId"),
                    created_at=c_dict.get("createdAt"),
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

    return _FetchResult(threads=threads, viewer=viewer)


def _apply_filters(
    threads: Iterable[_Thread],
    *,
    unreplied: bool,
    unresolved: bool,
    viewer: str | None,
) -> list[_Thread]:
    items: list[_Thread] = []
    for th in threads:
        if unresolved and th.is_resolved:
            continue
        if unreplied:
            has_viewer_comment = any(c.viewer_did_author for c in th.comments)
            if has_viewer_comment:
                continue
        items.append(th)
    return items


def _load_replies(replies_file: Path) -> dict[str, str]:
    text = replies_file.read_text()
    try:
        data: object = json.loads(text or "{}")
    except json.JSONDecodeError:
        return {}

    if not isinstance(data, dict):
        return {}
    mapping: dict[str, str] = {}
    for k, v in cast(Dict[Any, Any], data).items():
        if isinstance(k, str) and isinstance(v, str):
            mapping[k] = v
        else:
            pass
    return mapping


def _review_module_default(subprocess_runner: SubprocessRunner, root_path: Path) -> Any:
    class _Module:
        pass

    mod: Any = _Module()

    def infer_repo_wrapper(remote: str) -> tuple[str, str]:
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

    mod._infer_repo = infer_repo_wrapper
    mod.fetch_review_threads = fetch_review_threads_wrapper
    mod.apply_filters = _apply_filters
    mod._load_replies = _load_replies
    mod._bulk_reply = bulk_reply_wrapper
    mod._load_comment_targets = _load_comment_targets
    mod._comment_lookup = _comment_lookup
    return mod


def render_threads(
    threads: Iterable[_Thread],
    *,
    apply_filters: Callable[..., Iterable[_Thread]],
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
        data: Any = json.loads(path.read_text() or "[]")
    except json.JSONDecodeError:
        return []

    if isinstance(data, list):
        data_list: list[Any] = cast(list[Any], data)
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


def _default_pids_by_port(port: int) -> list[int]:
    pids: set[int] = set()
    try:
        for conn in psutil.net_connections(kind="inet"):
            try:
                laddr = getattr(conn, "laddr", None)
                if not laddr:
                    continue
                conn_port = getattr(laddr, "port", None)
                if conn_port == port and conn.pid is not None:
                    pids.add(int(conn.pid))
            except Exception:
                continue
    except Exception:
        try:
            for proc in psutil.process_iter(["pid", "name"]):  # type: ignore[arg-type]
                try:
                    p: Any = proc
                    connections = p.net_connections(kind="inet")
                    for c in connections:
                        laddr = getattr(c, "laddr", None)
                        if laddr and getattr(laddr, "port", None) == port:
                            pids.add(int(p.pid))
                            break
                except Exception:
                    continue
        except Exception:
            return []
    return sorted(pids)


def _default_kill_pid(pid: int) -> bool:
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        try:
            proc.wait(timeout=1.0)
        except psutil.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=1.0)
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
        return False


def run_review_list(
    *,
    pr_number: int,
    unreplied: bool,
    unresolved: bool,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], Any] | None,
    render_fn: Callable[..., list[str]] = render_threads,
) -> ToolResult:
    operation_id = OperationId(namespace="tools", category="dev", command="review-list")
    try:
        review = (
            review_module_factory()
            if review_module_factory is not None
            else _review_module_default(subprocess_runner, root_path)
        )
        try:
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
            )
        except ToolExecutionError as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to list review comments: {exc}",
            )

        output_lines = render_fn(
            fetch_result.threads,
            apply_filters=getattr(review, "apply_filters"),
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
        # Propagate known execution errors for upstream callers expecting exceptions.
        raise
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to list review comments: {exc}",
        )


def run_review_bulk_reply(
    *,
    pr_number: int,
    replies_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], Any] | None,
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-bulk-reply"
    )
    try:
        review = (
            review_module_factory()
            if review_module_factory is not None
            else _review_module_default(subprocess_runner, root_path)
        )
        try:
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
            )
        except ToolExecutionError as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to send bulk replies: {exc}",
            )

        replies = getattr(review, "_load_replies")(replies_file)
        getattr(review, "_bulk_reply")(fetch=fetch_result, replies=replies)
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Successfully sent bulk replies to PR #{pr_number}",
        )
    except ToolExecutionError:
        # Propagate expected execution errors for callers/tests to handle.
        raise
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to send bulk replies: {exc}",
        )


def run_review_delete(
    *,
    pr_number: int,
    comments_file: Path,
    remote: str,
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    review_module_factory: Callable[[], Any] | None,
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category="dev", command="review-delete"
    )
    try:
        review = (
            review_module_factory()
            if review_module_factory is not None
            else _review_module_default(subprocess_runner, root_path)
        )
        try:
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
            )
        except ToolExecutionError as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to delete comments: {exc}",
            )

        targets = getattr(review, "_load_comment_targets")(comments_file)
        lookup = getattr(review, "_comment_lookup")(fetch_result)

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
    except ToolExecutionError as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to delete comments: {exc}",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to delete comments: {exc}",
        )


def run_cleanup_ignored_tracked(
    *, subprocess_runner: SubprocessRunner, root_path: Path
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category="dev", command="cleanup-ignored-tracked"
    )
    try:
        listing = subprocess_runner.run_subprocess(
            ["git", "ls-files", "-i", "--exclude-standard"],
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
            stderr=f"Failed to cleanup ignored tracked files: {exc}",
        )


def run_kill_port(
    *,
    port: int,
    subprocess_runner: SubprocessRunner,
    pids_by_port: Callable[[int], Iterable[int]],
    kill_pid: Callable[[int], bool],
    root_path: Path | None = None,
) -> ToolResult:
    operation_id = OperationId(namespace="tools", category="dev", command="kill-port")
    try:
        pids = list(dict.fromkeys(pids_by_port(port)))
        if not pids:
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"No processes found running on port {port}.",
            )

        for pid in pids:
            if not kill_pid(pid):
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to kill PID {pid} on port {port}.",
                )

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=f"Killed {len(pids)} processes running on port {port}.",
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to kill port {port}: {exc}",
        )


def run_dev_batch_review(
    *,
    config: ToolsConfig,
    root_path: Path,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    return batch_review_module.run_batch_review(
        config,
        root_path,
        output_format=output_format,
        subprocess_runner=subprocess_runner,
    )


def run_dev_workflow_status(
    *,
    config: ToolsConfig,
    root_path: Path,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner,
) -> ToolResult:
    operation_id = OperationId(
        namespace="tools", category="dev", command="workflow-status"
    )
    status_payload = {
        "status": "ok",
        "root": str(root_path),
        "output_format": output_format,
    }
    stdout = (
        json.dumps(status_payload, indent=2)
        if output_format == "json"
        else str(status_payload)
    )
    return ToolResult.create(
        success=True,
        exit_code=0,
        namespace=operation_id.namespace,
        category=operation_id.category,
        command=operation_id.command,
        stdout=stdout,
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
    git_wild_match_pattern_factory: Callable[[str], Any] | None,
    create_or_update_link_op: Callable[..., None] | None,
    ensure_base_and_empty_readme_op: Callable[..., Path] | None,
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
        git_wild_match_pattern_factory: Callable[[str], Any] | None = None,
        os_link_op: Callable[[Path | str, Path | str], None] | None = None,
    ) -> None:
        self.config = config or ToolsConfig()
        self.subprocess_runner: SubprocessRunner = (
            subprocess_runner or RealSubprocessRunner()
        )
        self.root_path = root_path or Path.cwd()
        self._platform_resolver = platform_resolver or platform.system
        self._review_module_factory = review_module_factory
        self._pids_by_port = pids_by_port or _default_pids_by_port
        self._kill_pid = kill_pid or _default_kill_pid
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
        self._os_link_op = os_link_op

    def _review_module(self) -> Any:
        if self._review_module_factory is not None:
            return self._review_module_factory()

        class _Module:
            pass

        mod: Any = _Module()

        def infer_repo_wrapper(remote: str) -> tuple[str, str]:
            return _infer_repo(remote, self.subprocess_runner, self.root_path)

        def fetch_review_threads_wrapper(
            owner: str, repo: str, pr_number: int
        ) -> _FetchResult:
            return _fetch_review_threads(
                owner, repo, pr_number, self.subprocess_runner, self.root_path
            )

        def bulk_reply_wrapper(*, fetch: _FetchResult, replies: dict[str, str]) -> None:
            return _bulk_reply(
                fetch=fetch,
                replies=replies,
                subprocess_runner=self.subprocess_runner,
                root_path=self.root_path,
            )

        mod._infer_repo = infer_repo_wrapper
        mod.fetch_review_threads = fetch_review_threads_wrapper
        mod.apply_filters = _apply_filters
        mod._load_replies = _load_replies
        mod._bulk_reply = bulk_reply_wrapper
        mod._load_comment_targets = _load_comment_targets
        mod._comment_lookup = _comment_lookup
        return mod

    def _render_threads(
        self,
        threads: Iterable[Any],
        *,
        apply_filters: Any,
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
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
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
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
            review_module_factory=self._review_module_factory,
        )

    def review_delete(
        self, pr_number: int, comments_file: Path, remote: str = "origin"
    ) -> ToolResult:
        return run_review_delete(
            pr_number=pr_number,
            comments_file=comments_file,
            remote=remote,
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
            review_module_factory=self._review_module_factory,
        )

    def cleanup_ignored_tracked(self) -> ToolResult:
        return run_cleanup_ignored_tracked(
            subprocess_runner=self.subprocess_runner, root_path=self.root_path
        )

    def kill_port(self, port: int) -> ToolResult:
        return run_kill_port(
            port=port,
            subprocess_runner=self.subprocess_runner,
            pids_by_port=self._pids_by_port,
            kill_pid=self._kill_pid,
            root_path=self.root_path,
        )

    def batch_review(self, output_format: str = "json") -> ToolResult:
        return run_dev_batch_review(
            config=self.config,
            root_path=self.root_path,
            output_format=output_format,
            subprocess_runner=self.subprocess_runner,
        )

    def workflow_status(self, output_format: str = "json") -> ToolResult:
        return run_dev_workflow_status(
            config=self.config,
            root_path=self.root_path,
            output_format=output_format,
            subprocess_runner=self.subprocess_runner,
        )

    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        setup_result = run_setup_ai_guidelines(
            tool=tool,
            project_dir=self.root_path,
            dry_run=dry_run,
            subprocess_runner=self.subprocess_runner,
            os_path_samefile=self._os_path_samefile,
            path_resolve=self._path_resolve,
            path_readlink=self._path_readlink,
            os_relpath=self._os_relpath,
            os_name=self._os_name,
            git_wild_match_pattern_factory=self._git_wild_match_pattern_factory,
            create_or_update_link_op=self._create_or_update_link_op,
            ensure_base_and_empty_readme_op=self._ensure_base_and_empty_readme_op,
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
