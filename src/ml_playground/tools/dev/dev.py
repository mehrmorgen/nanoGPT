"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

import os
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Callable

from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
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
        operation_id = OperationId(
            namespace="tools", category="dev", command="setup-ai-guidelines"
        )
        try:
            # ---------------------- Configuration & types ----------------------
            README_NAME = "README.md"
            PROJECT_DIR = self.root_path
            BASE_DIR = PROJECT_DIR / ".dev-guidelines"

            @dataclass(frozen=True)
            class ToolSpec:
                primary_link: str
                root: str
                single_file_root: bool = False

            TOOL_MAP: dict[str, ToolSpec] = {
                "copilot": ToolSpec(".github/copilot-instructions.md", ".github"),
                "aiassistant": ToolSpec(
                    ".aiassistant/rules/00-README.md", ".aiassistant/rules"
                ),
                "junie": ToolSpec(".junie/guidelines.md", ".junie"),
                "kiro": ToolSpec(".kiro/steering/product.md", ".kiro/steering"),
                "windsurf": ToolSpec(".windsurf/rules/rule.md", ".windsurf/rules"),
                "cursor": ToolSpec(".cursor/rules/00-readme.mdc", ".cursor/rules"),
                "gemini": ToolSpec("GEMINI.md", ".", True),
                "codex": ToolSpec("AGENTS.md", ".", True),
            }

            logs: list[str] = []

            def info(msg: str) -> None:
                logs.append(msg)

            def warn(msg: str) -> None:
                logs.append(f"WARNING: {msg}")

            def err(msg: str) -> None:
                logs.append(f"ERROR: {msg}")

            # ----------------------------- Helpers -----------------------------
            def ensure_dir(path: Path, dry: bool) -> None:
                try:
                    if path.exists():
                        return
                except OSError:
                    pass

                is_file_like = path.suffix != ""
                if dry:
                    action = "touch" if is_file_like else "mkdir -p"
                    info(f"[dry-run] {action} {path}")
                    return
                if is_file_like:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if not path.exists():
                        path.touch()
                        info(f"create {path} (empty)")
                else:
                    path.mkdir(parents=True, exist_ok=True)

            def ensure_base_and_empty_readme(dry: bool) -> Path:
                ensure_dir(BASE_DIR, dry)
                readme = BASE_DIR / README_NAME
                ensure_dir(readme, dry)
                return readme

            def _windows_create_junction(link_path: Path, target_path: Path) -> None:
                cmd = [
                    "cmd",
                    "/c",
                    "mklink",
                    "/J",
                    str(link_path),
                    str(target_path),
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(
                        f"failed to create junction {link_path} -> {target_path}: {result.stderr.strip()}"
                    )

            def create_or_update_link(
                link_path: Path, target_path: Path, dry: bool
            ) -> None:
                link_exists = False
                link_is_symlink = False
                try:
                    link_exists = link_path.exists()
                    link_is_symlink = link_path.is_symlink()
                except OSError:
                    link_exists = False
                    link_is_symlink = False

                is_windows = os.name == "nt"
                try:
                    target_is_dir = target_path.is_dir()
                except OSError:
                    target_is_dir = False

                desired_link_repr: str | None = None
                if not is_windows:
                    try:
                        desired_link_repr = os.path.relpath(
                            target_path, start=link_path.parent
                        )
                    except ValueError:
                        desired_link_repr = str(target_path)
                elif target_is_dir:
                    desired_link_repr = str(target_path)

                current_link_repr: str | None = None
                if link_is_symlink:
                    try:
                        current_link_repr = link_path.readlink().as_posix()
                    except OSError:
                        current_link_repr = None

                same = False
                if link_exists or (link_is_symlink and current_link_repr):
                    try:
                        same = os.path.samefile(link_path, target_path)
                    except OSError:
                        try:
                            same = link_path.resolve() == target_path.resolve()
                        except OSError:
                            same = False

                if same and link_is_symlink:
                    if not link_exists:
                        same = False
                    elif (
                        desired_link_repr is not None and current_link_repr is not None
                    ):
                        if current_link_repr.replace(
                            "\\", "/"
                        ) != desired_link_repr.replace("\\", "/"):
                            same = False

                if same:
                    info(f"ok     {link_path} == {target_path} (same path)")
                    return

                if link_exists or link_is_symlink:
                    if dry:
                        info(f"[dry-run] rm {link_path}")
                    else:
                        try:
                            if link_path.is_symlink() or link_path.is_file():
                                link_path.unlink()
                            elif link_path.is_dir():
                                if any(link_path.iterdir()):
                                    raise RuntimeError(
                                        f"Cannot replace non-empty directory at {link_path}. Remove it first."
                                    )
                                link_path.rmdir()
                            else:
                                link_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                        except OSError as e:
                            raise RuntimeError(
                                f"failed to remove existing path {link_path}: {e}"
                            ) from e

                ensure_dir(link_path.parent, dry)
                if dry:
                    if is_windows and not target_is_dir:
                        info(f"[dry-run] hardlink {link_path} -> {target_path}")
                    elif is_windows and target_is_dir:
                        info(f"[dry-run] junction {link_path} -> {target_path}")
                    else:
                        info(f"[dry-run] ln -s {target_path} {link_path}")
                    return

                if is_windows:
                    if target_is_dir:
                        _windows_create_junction(link_path, target_path)
                        info(f"link   {link_path} => {target_path} (junction)")
                    else:
                        try:
                            os.link(target_path, link_path)
                            info(f"link   {link_path} == {target_path} (hardlink)")
                        except OSError as e:
                            message = (
                                f"failed to create hardlink {link_path} -> {target_path}: {e}. "
                                "Ensure both paths are on the same volume."
                            )
                            raise RuntimeError(message) from e
                else:
                    rel = os.path.relpath(target_path, start=link_path.parent)
                    try:
                        if target_is_dir:
                            link_path.symlink_to(rel, target_is_directory=True)
                        else:
                            link_path.symlink_to(rel)
                        info(f"link   {link_path} -> {target_path}")
                    except OSError as e:
                        err(
                            f"failed to create symlink {link_path} -> {target_path}: {e}"
                        )

            def mirror_tree(
                src_dir: Path, dest_dir: Path, exclude: set[Path] | None, dry: bool
            ) -> None:
                if not src_dir.exists():
                    return
                for entry in src_dir.iterdir():
                    target = entry.resolve()
                    if exclude and target in exclude:
                        continue
                    dest_path = (dest_dir / entry.name).resolve()
                    create_or_update_link(dest_path, target, dry)

            def _relative_tool_path(tool_dir: Path) -> str:
                return os.path.relpath(tool_dir, start=PROJECT_DIR).replace(os.sep, "/")

            def _project_path(relative_path: str) -> Path:
                path = Path(relative_path)
                if path.is_absolute():
                    raise ValueError(
                        f"ToolSpec paths must be project-relative; got absolute '{relative_path}'."
                    )
                if any(part == ".." for part in path.parts):
                    raise ValueError(
                        f"ToolSpec paths must not contain parent directory references: '{relative_path}'."
                    )
                if path == Path("."):
                    return PROJECT_DIR
                return PROJECT_DIR / path

            def _gitignore_match(
                relative_path: str, *, directory: bool
            ) -> tuple[bool, str | None]:
                gitignore = PROJECT_DIR / ".gitignore"
                if not gitignore.exists():
                    return False, None
                candidates = {relative_path.replace(os.sep, "/")}
                if directory:
                    base = relative_path.rstrip("/")
                    if base and not base.endswith("/"):
                        candidates.add(f"{base}/")
                    elif not base:
                        candidates.add("/")
                ignored = False
                matched_pattern: str | None = None
                with gitignore.open("r", encoding="utf-8") as handle:
                    for raw_line in handle:
                        line = raw_line.rstrip("\n")
                        stripped = line.lstrip()
                        if not stripped or stripped.startswith("#"):
                            continue
                        try:
                            pattern = GitWildMatchPattern(line)
                        except ValueError:
                            continue
                        if any(
                            pattern.match_file(candidate) for candidate in candidates
                        ):
                            ignored = bool(pattern.include)
                            matched_pattern = line.strip()
                return ignored, matched_pattern

            def log_gitignore_status(path: Path, *, directory: bool) -> None:
                relative_path = _relative_tool_path(path).replace(os.sep, "/")
                display_path = relative_path.rstrip("/")
                if directory:
                    display_path = (display_path + "/") if display_path else "/"
                ignored, matched_pattern = _gitignore_match(
                    relative_path, directory=directory
                )
                if ignored:
                    info(
                        f"git    '{display_path}' ignored by pattern '{matched_pattern or '<unknown>'}'."
                    )
                    return
                if matched_pattern and matched_pattern.startswith("!"):
                    info(
                        f"git    '{display_path}' kept by negated pattern '{matched_pattern}'."
                    )
                    return
                warn(
                    f"git    '{display_path}' is not ignored by .gitignore. Add an entry if you want Git to skip committing these files."
                )

            def is_listed_in_aiignore(tool_dir: Path) -> bool:
                ignore_path = PROJECT_DIR / ".aiignore"
                if not ignore_path.exists():
                    return False
                with ignore_path.open("r", encoding="utf-8") as f:
                    lines = [line.rstrip("\n") for line in f]
                patterns = [
                    line.strip()
                    for line in lines
                    if line.strip() and not line.lstrip().startswith("#")
                ]
                if not patterns:
                    return False
                spec = PathSpec.from_lines("gitwildmatch", patterns)
                relative_path = _relative_tool_path(tool_dir).rstrip("/")
                return bool(
                    spec.match_file(relative_path)
                    or spec.match_file(relative_path + "/")
                )

            def log_aiignore_status(tool_dir: Path) -> None:
                relative_path = _relative_tool_path(tool_dir).rstrip("/") + "/"
                if is_listed_in_aiignore(tool_dir):
                    warn(
                        f"ai     '{relative_path}' is excluded by .aiignore. Remove this entry so AI tools can access their guidelines."
                    )
                else:
                    info(f"ai     '{relative_path}' accessible to AI tools")

            # ------------------------------ Command -----------------------------
            tool_key = tool.lower()
            if tool_key not in TOOL_MAP:
                err(f"Unknown tool '{tool}'. Supported: {', '.join(sorted(TOOL_MAP))}")
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=logs[-1] if logs else f"Unknown tool '{tool}'",
                    stdout="\n".join(logs),
                )

            spec = TOOL_MAP[tool_key]

            # 1) Ensure base + empty README
            readme = ensure_base_and_empty_readme(dry_run)

            # 2) Ensure tool directory exists
            tool_dir = (
                PROJECT_DIR if spec.single_file_root else _project_path(spec.root)
            )
            ensure_dir(tool_dir, dry_run)

            # 3) Create primary link from tool map path to README
            if spec.single_file_root:
                primary_path = PROJECT_DIR / Path(spec.primary_link).name
            else:
                primary_path = _project_path(spec.primary_link)
            ensure_dir(primary_path.parent, dry_run)
            create_or_update_link(primary_path, readme, dry=dry_run)

            if spec.single_file_root:
                log_gitignore_status(primary_path, directory=False)
                info(
                    f"note   {tool_key} configured as single-file root; skipping tree mirroring."
                )
                info("done.")
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stdout="\n".join(logs),
                )

            # 4) Mirror entire BASE_DIR contents into tool_dir (exclude README)
            exclude: set[Path] = {readme.resolve()}
            mirror_tree(BASE_DIR.resolve(), tool_dir, exclude, dry_run)

            # 5) Clean broken symlinks pointing into BASE_DIR within tool's directory
            #    Only for POSIX symlinks; junctions/hardlinks won't be broken in the same way
            if tool_dir.exists():
                for path in tool_dir.rglob("*"):
                    if not path.is_symlink():
                        continue
                    try:
                        target = (path.parent / path.readlink()).resolve()
                    except OSError:
                        target = None
                    if (
                        target
                        and str(target).startswith(str(BASE_DIR.resolve()))
                        and not target.exists()
                    ):
                        if dry_run:
                            info(f"[dry-run] rm broken symlink {path} (-> {target})")
                        else:
                            path.unlink()
                            info(f"clean  removed broken symlink {path}")

            # 6) Report ignore status for the tool's directory
            log_gitignore_status(tool_dir, directory=True)
            log_aiignore_status(tool_dir)

            info("done.")
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout="\n".join(logs),
            )
        except Exception as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to setup AI guidelines: {exc}",
            )
