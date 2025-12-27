"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

import platform
from pathlib import Path
from typing import Any, Iterable, Callable

from ..core.config import ToolsConfig
from ..core.errors import ToolExecutionError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import (
    SubprocessRunner,
    _default_runner,
)


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
    ) -> None:
        self.config = config or ToolsConfig()
        self.subprocess_runner: SubprocessRunner = subprocess_runner or _default_runner
        self.root_path = root_path or Path.cwd()
        self._platform_resolver = platform_resolver or platform.system
        self._review_module_factory = review_module_factory
        # Process inspection/termination seams (pure Python; default to psutil-backed)
        self._pids_by_port = pids_by_port or self._default_pids_by_port
        self._kill_pid = kill_pid or self._default_kill_pid

    # ------------------------------------------------------------------
    # Review utilities
    # ------------------------------------------------------------------
    def _review_module(self) -> Any:
        # Prefer injected factory when provided to enable DI and avoid monkeypatching in tests
        if self._review_module_factory is not None:
            return self._review_module_factory()
        return self._builtin_review_module()

    # ------------------------- Built-in review impl -------------------------
    def _builtin_review_module(self) -> Any:
        # Local executor honoring the DI flag
        def _exec(args: list[str], *, operation_id: OperationId) -> ToolResult:
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
            def __init__(
                self, url: str, is_resolved: bool, comments: list[Any]
            ) -> None:
                self.url = url
                self.is_resolved = is_resolved
                self.comments = comments

        class _FetchResult:
            def __init__(self, threads: list[Any], viewer: str | None) -> None:
                self.threads = threads
                self.viewer = viewer

        def _infer_repo(remote: str) -> tuple[str, str]:
            res = _exec(
                ["git", "remote", "get-url", remote],
                operation_id=OperationId(
                    namespace="tools", category="dev", command="review-infer-repo"
                ),
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

        def fetch_review_threads(owner: str, repo: str, pr_number: int) -> Any:
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
            op_id = OperationId(
                namespace="tools", category="dev", command="review-fetch"
            )
            res = _exec(args, operation_id=op_id)
            if not res.success:
                raise ToolExecutionError(
                    "Failed to fetch review threads",
                    reason=res.stderr or "gh api graphql failed",
                    rationale="Ensure gh is installed and you have permissions to view the PR.",
                )
            import json

            data = json.loads(res.stdout or "{}")
            root = data.get("data", {}) if isinstance(data, dict) else {}
            viewer = (
                ((root.get("viewer") or {}).get("login"))
                if isinstance(root, dict)
                else None
            )
            repo_obj = (root.get("repository") or {}) if isinstance(root, dict) else {}
            pr_obj = (
                (repo_obj.get("pullRequest") or {})
                if isinstance(repo_obj, dict)
                else {}
            )
            review_threads = (
                (pr_obj.get("reviewThreads") or {}) if isinstance(pr_obj, dict) else {}
            )
            threads_json = (
                review_threads.get("nodes", [])
                if isinstance(review_threads, dict)
                else []
            )

            threads: list[Any] = []
            for t in threads_json:
                is_resolved = bool(t.get("isResolved", False))
                comments_nodes = ((t.get("comments") or {}).get("nodes")) or []
                comments: list[_Comment] = []
                for c in comments_nodes:
                    author_login = (c.get("author") or {}).get("login") or ""
                    comments.append(
                        _Comment(
                            author=author_login,
                            viewer_did_author=(
                                viewer is not None and author_login == viewer
                            ),
                            body=c.get("body") or "",
                            url=c.get("url") or "",
                            id=c.get("id") or "",
                            database_id=c.get("databaseId"),
                            created_at=c.get("createdAt"),
                        )
                    )
                thread_url = comments[0].url if comments else ""
                threads.append(
                    _Thread(url=thread_url, is_resolved=is_resolved, comments=comments)
                )

            return _FetchResult(threads=threads, viewer=viewer)

        def apply_filters(
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

        def _load_replies(replies_file: Path) -> dict[str, str]:
            import json

            text = replies_file.read_text()
            data = json.loads(text or "{}")
            if not isinstance(data, dict):
                # Treat invalid format as empty mapping (no-op)
                return {}
            # coerce to str->str
            mapping: dict[str, str] = {}
            for k, v in data.items():
                if isinstance(k, str) and isinstance(v, str):
                    mapping[k] = v
            return mapping

        def _bulk_reply(*, fetch: Any, replies: dict[str, str]) -> None:
            # Resolve identifiers to GraphQL comment IDs from review-list output and reply via GraphQL
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
                )
                if not result.success:
                    raise ToolExecutionError(
                        "Failed to send reply via GitHub CLI",
                        reason=result.stderr
                        or result.stdout
                        or "gh api graphql failed",
                        rationale=(
                            "Ensure your GitHub token has permission to comment on the PR "
                            "and that the comment identifier matches an existing thread."
                        ),
                    )

        def _load_comment_targets(path: Path) -> list[str]:
            import json

            data = json.loads(path.read_text() or "[]")
            return data if isinstance(data, list) else []

        def _comment_lookup(fetch: Any) -> dict[str, str]:
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

        class _Module:
            pass

        mod: Any = _Module()
        mod._infer_repo = _infer_repo
        mod.fetch_review_threads = fetch_review_threads
        mod.apply_filters = apply_filters
        mod._load_replies = _load_replies
        mod._bulk_reply = _bulk_reply
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
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-list"
        )
        try:
            review = self._review_module()
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
            )
            output_lines = self._render_threads(
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

    def review_bulk_reply(
        self, pr_number: int, replies_file: Path, remote: str = "origin"
    ) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-bulk-reply"
        )
        try:
            review = self._review_module()
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
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

    def review_delete(
        self, pr_number: int, comments_file: Path, remote: str = "origin"
    ) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-delete"
        )
        try:
            review = self._review_module()
            owner, repo = getattr(review, "_infer_repo")(remote)
            fetch_result = getattr(review, "fetch_review_threads")(
                owner, repo, pr_number
            )
            targets = getattr(review, "_load_comment_targets")(comments_file)
            lookup = getattr(review, "_comment_lookup")(fetch_result)

            deleted = 0
            for target in targets:
                comment_id = lookup.get(target)
                if not comment_id:
                    continue
                deletion = self.subprocess_runner.run_subprocess(
                    [
                        "gh",
                        "api",
                        "graphql",
                        "-f",
                        f'query=mutation {{ deleteIssueComment(input: {{ id: "{comment_id}" }}) {{ clientMutationId }} }}',
                    ],
                    cwd=self.root_path,
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
        except Exception as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to delete comments: {exc}",
            )

    # ------------------------------------------------------------------
    # Repository hygiene utilities
    # ------------------------------------------------------------------
    def cleanup_ignored_tracked(self) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="cleanup-ignored-tracked"
        )
        try:
            listing = self.subprocess_runner.run_subprocess(
                ["git", "ls-files", "-i", "--exclude-standard"],
                cwd=self.root_path,
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
                removal = self.subprocess_runner.run_subprocess(
                    ["git", "rm", "--cached", file],
                    cwd=self.root_path,
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

    def kill_port(self, port: int) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="kill-port"
        )
        try:
            pids = list(
                dict.fromkeys(self._pids_by_port(port))
            )  # dedupe, preserve order
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
                if not self._kill_pid(pid):
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

    # ---------------------- Default process helpers (psutil) ----------------------
    @staticmethod
    def _default_pids_by_port(port: int) -> list[int]:
        import psutil  # type: ignore

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
            # Fallback: iterate processes if net_connections is restricted
            try:
                for proc in psutil.process_iter(attrs=["pid"]):
                    try:
                        for c in proc.connections(kind="inet"):
                            laddr = getattr(c, "laddr", None)
                            if laddr and getattr(laddr, "port", None) == port:
                                pids.add(int(proc.pid))
                                break
                    except Exception:
                        continue
            except Exception:
                return []
        return sorted(pids)

    @staticmethod
    def _default_kill_pid(pid: int) -> bool:
        import psutil  # type: ignore

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

    # ------------------------------------------------------------------
    # Delegation helpers
    # ------------------------------------------------------------------
    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        from ml_playground.tools.dev import ai_guidelines

        return ai_guidelines.setup_ai_guidelines(
            tool=tool,
            project_dir=self.root_path,
            dry_run=dry_run,
        )
