"""Quality tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, DEFAULT_RUNNER

_STRICT_WARNING_KINDS = [
    "redundant-cast",
    "redundant-condition",
    "unnecessary-comparison",
    "unreachable",
    "unused-ignore",
    "deprecated",
]


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
        self._config = config
        self._root_path = root_path
        self._pkg_path = root_path / "src" / "ml_playground"
        self._subprocess_runner = subprocess_runner or DEFAULT_RUNNER
        self._learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "quality"

    def lint(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff lint checks."""
        return self._lint(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _lint(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal Ruff lint implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="lint"
        )

        # Default to check mode, allow args to override
        ruff_args = ["ruff", "check", "."]
        if args:
            ruff_args = ["ruff", *args]

        result = self._subprocess_runner.run_uv_command(
            ruff_args,
            cwd=self._root_path,
            timeout=self._config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="lint",
                context="Analyzing code for style violations and potential bugs",
                category=self.category,
                executed_commands=[" ".join(ruff_args)],
            )

        return result

    def format(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Format code with Ruff."""
        return self._format(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _format(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal Ruff format implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="format"
        )

        # Run both check --fix and format
        check_result = self._subprocess_runner.run_uv_command(
            ["ruff", "check", "--fix", ".", *args],
            cwd=self._root_path,
            timeout=self._config.quality.timeout,
            operation_id=operation_id,
        )

        if not check_result.success:
            if learning_mode:
                self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
                check_result.learning_info = self._learning_engine.explain_command(
                    command="format",
                    context="Automatically formatting code to match style standards",
                    category=self.category,
                    executed_commands=[f"ruff check --fix . {' '.join(args)}".strip()],
                )
            return check_result

        # Then run format
        format_result = self._subprocess_runner.run_uv_command(
            ["ruff", "format", ".", *args],
            cwd=self._root_path,
            timeout=self._config.quality.timeout,
            operation_id=operation_id,
        )

        # Combine outputs
        combined_stdout = ""
        if check_result.stdout:
            combined_stdout += f"Ruff check --fix:\n{check_result.stdout}\n"
        if format_result.stdout:
            combined_stdout += f"Ruff format:\n{format_result.stdout}"

        combined_stderr = ""
        if check_result.stderr:
            combined_stderr += f"Ruff check --fix errors:\n{check_result.stderr}\n"
        if format_result.stderr:
            combined_stderr += f"Ruff format errors:\n{format_result.stderr}"

        result = ToolResult(
            success=format_result.success,
            exit_code=format_result.exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="format",
                context="Automatically formatting code to match style standards",
                category=self.category,
                executed_commands=[
                    f"ruff check --fix . {' '.join(args)}".strip(),
                    f"ruff format . {' '.join(args)}".strip(),
                ],
            )

        return result

    def lint_check(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff in check-only mode (alias for lint)."""
        return self._lint(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def deadcode(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Scan for dead code using vulture."""
        return self._deadcode(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _deadcode(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal vulture deadcode implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="deadcode"
        )

        vulture_args = ["vulture", str(self._pkg_path), "--min-confidence", "90"]
        if args:
            vulture_args.extend(args)

        result = self._subprocess_runner.run_uv_command(
            vulture_args,
            cwd=self._root_path,
            timeout=self._config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="deadcode",
                context="Scanning for unused code that can be safely removed",
                category=self.category,
                executed_commands=[" ".join(vulture_args)],
            )

        return result

    def _run_typechecker(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run strict static type checking."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="typecheck"
        )

        pyrefly_args = ["pyrefly", "check", str(self._pkg_path)]
        for kind in _STRICT_WARNING_KINDS:
            pyrefly_args.extend(["--error", kind])
        if args:
            pyrefly_args.extend(args)

        result = self._subprocess_runner.run_uv_command(
            pyrefly_args,
            cwd=self._root_path,
            timeout=self._config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="typecheck",
                context="Performing strict static type checking",
                category=self.category,
                executed_commands=[" ".join(pyrefly_args)],
            )

        return result

    def typecheck(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run strict static type checks."""
        return self._typecheck(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _typecheck(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal typecheck implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="typecheck"
        )
        pyrefly_result = self._run_typechecker(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

        result = ToolResult(
            success=pyrefly_result.success,
            exit_code=pyrefly_result.exit_code,
            stdout=pyrefly_result.stdout,
            stderr=pyrefly_result.stderr,
            operation_id=operation_id,
        )

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="typecheck",
                context="Running strict static type checking",
                category=self.category,
                executed_commands=[
                    f"pyrefly check {self._pkg_path} {' '.join(args)}".strip(),
                ],
            )

        return result

    def all_checks(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run all quality checks (lint, typecheck, deadcode)."""
        return self._all_checks(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _all_checks(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal all_checks implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="all"
        )

        # Run all quality checks
        lint_result = self._lint(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )
        typecheck_result = self._typecheck(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )
        deadcode_result = self._deadcode(
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
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="all",
                context="Running all quality checks for comprehensive code analysis",
                category=self.category,
                executed_commands=[
                    f"ruff check . {' '.join(args)}".strip(),
                    f"pyrefly check {self._pkg_path} {' '.join(args)}".strip(),
                    f"vulture {self._pkg_path} --min-confidence 90 {' '.join(args)}".strip(),
                ],
            )

        return result
