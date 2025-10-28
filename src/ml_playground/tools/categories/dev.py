"""Development workflow tools.

This module provides tools for development workflows including:
- GitHub PR review management
- Code cleanup utilities
- Port management
- AI development guidelines setup
"""

from __future__ import annotations

from pathlib import Path

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import SubprocessRunner, _default_runner


class DevTools:
    """Development workflow tools."""

    def __init__(
        self,
        config: ToolsConfig | None = None,
        subprocess_runner: SubprocessRunner | None = None,
    ) -> None:
        self.config = config or ToolsConfig()
        self._subprocess_runner = subprocess_runner or _default_runner()

    def review_list(
        self,
        pr_number: int,
        unreplied: bool = False,
        unresolved: bool = False,
        remote: str = "origin",
    ) -> ToolResult:
        """List GitHub PR review comments with optional filtering.

        Args:
            pr_number: Pull request number
            unreplied: Only show threads without viewer reply
            unresolved: Only show unresolved threads
            remote: Git remote name for owner/repo inference

        Returns:
            ToolResult with review thread information
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-list"
        )

        try:
            # Import the review functionality
            from scripts.review import fetch_review_threads, apply_filters, _infer_repo

            owner, repo = _infer_repo(remote)
            fetch_result = fetch_review_threads(owner, repo, pr_number)

            # Apply filters
            filtered_threads = apply_filters(
                fetch_result.threads,
                unreplied=unreplied,
                unresolved=unresolved,
                viewer=fetch_result.viewer,
            )

            # Format output
            output_lines = []
            found_any = False
            for thread in filtered_threads:
                found_any = True
                output_lines.append(f"Thread: {thread.url}")
                if thread.is_resolved:
                    output_lines.append("  Status: Resolved")
                else:
                    output_lines.append("  Status: Unresolved")

                for comment in thread.comments:
                    author_info = (
                        f"{comment.author} (viewer)"
                        if comment.viewer_did_author
                        else comment.author
                    )
                    output_lines.append(f"  - {author_info}: {comment.body[:100]}...")
                output_lines.append("")

            if not found_any:
                output_lines.append("No matching review threads found.")

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout="\\n".join(output_lines),
            )

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to list review comments: {e}",
            )

    def review_bulk_reply(
        self,
        pr_number: int,
        replies_file: Path,
        remote: str = "origin",
    ) -> ToolResult:
        """Bulk reply to GitHub PR review comments.

        Args:
            pr_number: Pull request number
            replies_file: JSON file mapping comment URLs/IDs to reply text
            remote: Git remote name for owner/repo inference

        Returns:
            ToolResult with bulk reply results
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-bulk-reply"
        )

        try:
            from scripts.review import (
                fetch_review_threads,
                _infer_repo,
                _load_replies,
                _bulk_reply,
            )

            owner, repo = _infer_repo(remote)
            fetch_result = fetch_review_threads(owner, repo, pr_number)
            replies = _load_replies(replies_file)

            _bulk_reply(
                fetch=fetch_result,
                replies=replies,
            )

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"Successfully sent bulk replies to PR #{pr_number}",
            )

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to send bulk replies: {e}",
            )

    def review_delete(
        self,
        pr_number: int,
        comments_file: Path,
        remote: str = "origin",
    ) -> ToolResult:
        """Delete GitHub PR review comments.

        Args:
            pr_number: Pull request number
            comments_file: JSON file with list of comment IDs/URLs to delete
            remote: Git remote name for owner/repo inference

        Returns:
            ToolResult with deletion results
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="review-delete"
        )

        try:
            from scripts.review import (
                fetch_review_threads,
                _infer_repo,
                _load_comment_targets,
                _comment_lookup,
            )

            owner, repo = _infer_repo(remote)
            fetch_result = fetch_review_threads(owner, repo, pr_number)
            comment_targets = _load_comment_targets(comments_file)
            comment_lookup = _comment_lookup(fetch_result)

            deleted_count = 0
            for target in comment_targets:
                comment_id = comment_lookup.get(target)
                if comment_id:
                    # Delete the comment using GitHub API
                    result = self._subprocess_runner.run(
                        [
                            "gh",
                            "api",
                            "graphql",
                            "-f",
                            f'query=mutation {{ deleteIssueComment(input: {{ id: "{comment_id}" }}) {{ clientMutationId }} }}',
                        ]
                    )
                    if result.returncode == 0:
                        deleted_count += 1

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"Successfully deleted {deleted_count} comments from PR #{pr_number}",
            )

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to delete comments: {e}",
            )

    def cleanup_ignored_tracked(self) -> ToolResult:
        """Clean up Git-ignored files that are still tracked.

        Returns:
            ToolResult with cleanup results
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="cleanup-ignored-tracked"
        )

        try:
            # Get list of tracked files that are ignored
            result = self._subprocess_runner.run(
                ["git", "ls-files", "-i", "--exclude-standard"]
            )

            if result.returncode != 0:
                return ToolResult.create(
                    success=False,
                    exit_code=result.returncode,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=f"Failed to list ignored tracked files: {result.stderr}",
                )

            ignored_files = [f.strip() for f in result.stdout.split("\\n") if f.strip()]

            if not ignored_files:
                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stdout="No ignored tracked files found.",
                )

            # Remove the files from git tracking
            for file in ignored_files:
                remove_result = self._subprocess_runner.run(
                    ["git", "rm", "--cached", file]
                )
                if remove_result.returncode != 0:
                    return ToolResult.create(
                        success=False,
                        exit_code=remove_result.returncode,
                        namespace=operation_id.namespace,
                        category=operation_id.category,
                        command=operation_id.command,
                        stderr=f"Failed to remove {file} from tracking: {remove_result.stderr}",
                    )

            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=f"Removed {len(ignored_files)} ignored tracked files from git.",
            )

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to cleanup ignored tracked files: {e}",
            )

    def kill_port(self, port: int) -> ToolResult:
        """Kill processes running on a specific port.

        Args:
            port: Port number to kill processes on

        Returns:
            ToolResult with port kill results
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="kill-port"
        )

        try:
            import platform

            if platform.system() == "Darwin":  # macOS
                # Find processes using the port
                result = self._subprocess_runner.run(["lsof", "-ti", f":{port}"])

                if result.returncode != 0:
                    return ToolResult.create(
                        success=True,
                        exit_code=0,
                        namespace=operation_id.namespace,
                        category=operation_id.category,
                        command=operation_id.command,
                        stdout=f"No processes found running on port {port}.",
                    )

                pids = [
                    pid.strip() for pid in result.stdout.split("\\n") if pid.strip()
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

                # Kill the processes
                for pid in pids:
                    kill_result = self._subprocess_runner.run(["kill", "-9", pid])
                    if kill_result.returncode != 0:
                        return ToolResult.create(
                            success=False,
                            exit_code=kill_result.returncode,
                            namespace=operation_id.namespace,
                            category=operation_id.category,
                            command=operation_id.command,
                            stderr=f"Failed to kill process {pid}: {kill_result.stderr}",
                        )

                return ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stdout=f"Killed {len(pids)} processes running on port {port}.",
                )

            else:
                # Linux/other systems
                result = self._subprocess_runner.run(["fuser", "-k", f"{port}/tcp"])

                return ToolResult.create(
                    success=result.returncode == 0,
                    exit_code=result.returncode,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stdout=f"Attempted to kill processes on port {port}.",
                    stderr=result.stderr,
                )

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to kill port {port}: {e}",
            )

    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        """Set up AI development guidelines for a specific tool.

        Args:
            tool: Target tool name (e.g., 'kiro', 'cursor', 'copilot')
            dry_run: Whether to preview actions without executing

        Returns:
            ToolResult with setup results
        """
        operation_id = OperationId(
            namespace="tools", category="dev", command="setup-ai-guidelines"
        )

        # This functionality is complex and tool-specific
        # For now, delegate to the environment tools which already have this integrated
        try:
            from ..environment import EnvironmentTools

            env_tools = EnvironmentTools(config=self.config)
            return env_tools.ai_guidelines([], tool=tool, dry_run=dry_run)

        except Exception as e:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"Failed to setup AI guidelines: {e}",
            )
