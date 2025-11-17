"""Quality tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import (
    SubprocessRunner,
    RealSubprocessRunner,
)

from .linting import run_lint, run_lint_check
from .formatting import run_format
from .deadcode import run_deadcode
from .typing import run_basedpyright, run_mypy, run_typecheck


# Module-level default runner for tests to patch if needed
_default_runner: SubprocessRunner | None = None


class QualityTools:
    """Quality tools implementation."""

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: Optional[SubprocessRunner] = None,
    ) -> None:
        """Initialize quality tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.pkg_path = root_path / "src" / "ml_playground"
        # Module-level patch point for tests
        global _default_runner  # noqa: PLW0603
        if _default_runner is None:
            _default_runner = RealSubprocessRunner()
        self.subprocess_runner = subprocess_runner or _default_runner
        self.learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "quality"

    def lint(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff lint checks."""
        return run_lint(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def format(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Format code with Ruff."""
        return run_format(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def lint_check(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff in check-only mode (alias for lint)."""
        return run_lint_check(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def deadcode(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Scan for dead code using vulture."""
        return run_deadcode(
            config=self.config,
            root_path=self.root_path,
            pkg_path=self.pkg_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def basedpyright(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run BasedPyright type checks."""
        return run_basedpyright(
            config=self.config,
            root_path=self.root_path,
            pkg_path=self.pkg_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def mypy(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Mypy type checks."""
        return run_mypy(
            config=self.config,
            root_path=self.root_path,
            pkg_path=self.pkg_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def typecheck(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run both BasedPyright and Mypy type checks."""
        return run_typecheck(
            config=self.config,
            root_path=self.root_path,
            pkg_path=self.pkg_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def all_checks(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run all quality checks (lint, typecheck, deadcode).

        Args:
            args: Additional arguments (applied to all tools)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="all"
        )

        # Run all quality checks
        lint_result = self.lint(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )
        typecheck_result = self.typecheck(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )
        deadcode_result = self.deadcode(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

        # Combine results
        results = [
            ("Lint", lint_result),
            ("Typecheck", typecheck_result),
            ("Deadcode", deadcode_result),
        ]

        combined_stdout = ""
        combined_stderr = ""
        all_success = True
        final_exit_code = 0

        for name, result in results:
            if result.stdout:
                combined_stdout += f"{name}:\n{result.stdout}\n"
            if result.stderr:
                combined_stderr += f"{name} errors:\n{result.stderr}\n"

            if not result.success:
                all_success = False
                if final_exit_code == 0:
                    final_exit_code = result.exit_code

        result = ToolResult(
            success=all_success,
            exit_code=final_exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="all",
                context="Running all quality checks for comprehensive code analysis",
                category=self.category,
                executed_commands=[
                    (
                        f"{prefix} ruff check . {' '.join(args)}".strip()
                        if prefix
                        else f"ruff check . {' '.join(args)}".strip()
                    ),
                    (
                        f"{prefix} basedpyright {self.pkg_path} {' '.join(args)}".strip()
                        if prefix
                        else f"basedpyright {self.pkg_path} {' '.join(args)}".strip()
                    ),
                    (
                        f"{prefix} mypy --incremental {self.pkg_path} {' '.join(args)}".strip()
                        if prefix
                        else f"mypy --incremental {self.pkg_path} {' '.join(args)}".strip()
                    ),
                    (
                        f"{prefix} vulture {self.pkg_path} --min-confidence 90 {' '.join(args)}".strip()
                        if prefix
                        else f"vulture {self.pkg_path} --min-confidence 90 {' '.join(args)}".strip()
                    ),
                ],
            )

        return result
