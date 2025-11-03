"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

import importlib
import platform
from pathlib import Path
from typing import Any, Iterable

from ..core.config import ToolsConfig
from ..core.errors import ToolExecutionError
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import (
    SubprocessRunner,
    _default_runner,
    run_subprocess,
)


class DevTools:
    """Utilities that help with PR review flows and local hygiene tasks."""

    def __init__(
        self,
        config: ToolsConfig | None = None,
        subprocess_runner: SubprocessRunner | None = None,
        root_path: Path | None = None,
    ) -> None:
        self.config = config or ToolsConfig()
        self.subprocess_runner: SubprocessRunner = subprocess_runner or _default_runner
        self.root_path = root_path or Path.cwd()

    # ------------------------------------------------------------------
    # Review utilities
    # ------------------------------------------------------------------
    def _review_module(self) -> Any:
        try:
            return importlib.import_module("scripts.review")
        except ModuleNotFoundError as exc:  # pragma: no cover - defensive guard
            raise ToolExecutionError(
                "Review helpers unavailable",
                reason="scripts.review module is missing",
                rationale="Development review commands depend on scripts/review.py",
            ) from exc

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
                snippet = comment.body.replace("\n", " ")[:100]
                lines.append(f"  - {author}: {snippet}...")
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
                deletion = run_subprocess(
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
            listing = run_subprocess(
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
                removal = run_subprocess(
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
            system = platform.system()
            if system == "Darwin":
                lookup = run_subprocess(
                    ["lsof", "-ti", f":{port}"],
                    cwd=self.root_path,
                    operation_id=operation_id,
                )
                if not lookup.success:
                    return lookup

                pids = [
                    pid.strip() for pid in lookup.stdout.splitlines() if pid.strip()
                ]
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
                    kill_result = run_subprocess(
                        ["kill", "-9", pid],
                        cwd=self.root_path,
                        operation_id=operation_id,
                    )
                    if not kill_result.success:
                        return kill_result

                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stdout=f"Killed {len(pids)} processes running on port {port}.",
                )

            result = run_subprocess(
                ["fuser", "-k", f"{port}/tcp"],
                cwd=self.root_path,
                operation_id=operation_id,
            )
            return ToolResult.create(
                success=result.success,
                exit_code=result.exit_code,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"Attempted to kill processes on port {port}.",
                stderr=result.stderr,
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

    # ------------------------------------------------------------------
    # Delegation helpers
    # ------------------------------------------------------------------
    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="setup-ai-guidelines"
        )
        try:
            from .environment import EnvironmentTools

            env_tools = EnvironmentTools(
                config=self.config,
                root_path=self.root_path,
                subprocess_runner=self.subprocess_runner,
            )
            return env_tools.ai_guidelines([], tool=tool, dry_run=dry_run)
        except Exception as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to setup AI guidelines: {exc}",
            )
