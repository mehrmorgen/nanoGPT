"""Development workflow utilities for the tools CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..utils.subprocess_utils import (
    SubprocessRunner,
    RealSubprocessRunner,
)
from .ai_guidelines import run_setup_ai_guidelines
from .review import run_review_list, run_review_bulk_reply, run_review_delete
from .hygiene import run_cleanup_ignored_tracked, run_kill_port
from .status import run_dev_batch_review, run_dev_workflow_status


# Module-level default runner removed in favor of direct instantiation in __init__


class DevTools:
    """Utilities that help with PR review flows and local hygiene tasks."""

    def __init__(
        self,
        config: ToolsConfig | None = None,
        subprocess_runner: SubprocessRunner | None = None,
        root_path: Path | None = None,
        review_module_factory: Callable[[], object] | None = None,
    ) -> None:
        self.config = config or ToolsConfig()
        self.subprocess_runner = subprocess_runner or RealSubprocessRunner()
        self.root_path = root_path or Path.cwd()
        self._review_module_factory = review_module_factory

    # ------------------------------------------------------------------
    # Review utilities
    # ------------------------------------------------------------------

    def review_list(
        self,
        pr_number: int,
        unreplied: bool = False,
        unresolved: bool = False,
        remote: str = "origin",
    ) -> ToolResult:
        """List GitHub PR review comments with optional filtering."""
        return run_review_list(
            pr_number=pr_number,
            remote=remote,
            unreplied=unreplied,
            unresolved=unresolved,
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
            review_module_factory=self._review_module_factory,
        )

    def review_bulk_reply(
        self, pr_number: int, replies_file: Path, remote: str = "origin"
    ) -> ToolResult:
        """Bulk reply to GitHub PR review comments."""
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
        """Delete GitHub PR review comments."""
        return run_review_delete(
            pr_number=pr_number,
            comments_file=comments_file,
            remote=remote,
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
            review_module_factory=self._review_module_factory,
        )

    # ------------------------------------------------------------------
    # Repository hygiene utilities
    # ------------------------------------------------------------------
    def cleanup_ignored_tracked(self) -> ToolResult:
        """Clean up Git-ignored files that are still tracked."""
        return run_cleanup_ignored_tracked(
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
        )

    def kill_port(self, port: int) -> ToolResult:
        """Kill processes running on a specific port."""
        return run_kill_port(
            port=port,
            subprocess_runner=self.subprocess_runner,
            root_path=self.root_path,
        )

    def batch_review(self, output_format: str = "json") -> ToolResult:
        """Perform batch review operations for AI consumption."""
        return run_dev_batch_review(
            config=self.config,
            root_path=self.root_path,
            output_format=output_format,
        )

    def workflow_status(self, output_format: str = "json") -> ToolResult:
        """Get current workflow status for AI decision-making."""
        return run_dev_workflow_status(
            config=self.config,
            root_path=self.root_path,
            output_format=output_format,
        )

    # ------------------------------------------------------------------
    # Delegation helpers
    # ------------------------------------------------------------------
    def setup_ai_guidelines(self, tool: str, dry_run: bool = False) -> ToolResult:
        operation_id = OperationId(
            namespace="tools", category="dev", command="setup-ai-guidelines"
        )
        setup_result = run_setup_ai_guidelines(
            tool=tool, project_dir=self.root_path, dry_run=dry_run
        )
        stdout = "\n".join(setup_result.logs)
        if not setup_result.success:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stdout=stdout,
                stderr=setup_result.error or "Failed to setup AI guidelines",
            )
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout=stdout,
        )
