"""Quality tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


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
        self.subprocess_runner = subprocess_runner or _default_runner
        self.learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "quality"

    def lint(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff lint checks.

        Args:
            args: Additional ruff arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="lint"
        )

        # Default to check mode, allow args to override
        ruff_args = ["ruff", "check", "."]
        if args:
            # Replace default args if user provides custom ones
            ruff_args = ["ruff", *args]

        result = self.subprocess_runner.run_uv_command(
            ruff_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="lint",
                context="Analyzing code for style violations and potential bugs",
                category=self.category,
                executed_commands=[
                    f"{prefix} {' '.join(ruff_args)}" if prefix else " ".join(ruff_args)
                ],
            )

        return result

    def format(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Format code with Ruff.

        Args:
            args: Additional ruff arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="format"
        )

        # Run both check --fix and format
        # First, run check with --fix
        check_result = self.subprocess_runner.run_uv_command(
            ["ruff", "check", "--fix", ".", *args],
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )

        if not check_result.success:
            if learning_mode:
                self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
                check_result.learning_info = self.learning_engine.explain_command(
                    command="format",
                    context="Automatically formatting code to match style standards",
                    category=self.category,
                    executed_commands=[f"ruff check --fix . {' '.join(args)}".strip()],
                )
            return check_result

        # Then run format
        format_result = self.subprocess_runner.run_uv_command(
            ["ruff", "format", ".", *args],
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
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
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="format",
                context="Automatically formatting code to match style standards",
                category=self.category,
                executed_commands=[
                    (
                        f"{prefix} ruff check --fix . {' '.join(args)}".strip()
                        if prefix
                        else f"ruff check --fix . {' '.join(args)}".strip()
                    ),
                    (
                        f"{prefix} ruff format . {' '.join(args)}".strip()
                        if prefix
                        else f"ruff format . {' '.join(args)}".strip()
                    ),
                ],
            )

        return result

    def lint_check(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Ruff in check-only mode (alias for lint).

        Args:
            args: Additional ruff arguments

        Returns:
            ToolResult with execution details
        """
        # This is an alias for lint command
        return self.lint(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def deadcode(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Scan for dead code using vulture.

        Args:
            args: Additional vulture arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="deadcode"
        )

        vulture_args = ["vulture", str(self.pkg_path), "--min-confidence", "90"]
        if args:
            vulture_args.extend(args)

        result = self.subprocess_runner.run_uv_command(
            vulture_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="deadcode",
                context="Scanning for unused code that can be safely removed",
                category=self.category,
                executed_commands=[
                    f"{prefix} {' '.join(vulture_args)}"
                    if prefix
                    else " ".join(vulture_args)
                ],
            )

        return result

    def basedpyright(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run BasedPyright type checks.

        Args:
            args: Additional basedpyright arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="basedpyright"
        )

        basedpyright_args = ["basedpyright", str(self.pkg_path)]
        if args:
            basedpyright_args.extend(args)

        result = self.subprocess_runner.run_uv_command(
            basedpyright_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="basedpyright",
                context="Performing static type checking using BasedPyright",
                category=self.category,
                executed_commands=[
                    f"{prefix} {' '.join(basedpyright_args)}"
                    if prefix
                    else " ".join(basedpyright_args)
                ],
            )

        return result

    def mypy(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run Mypy type checks.

        Args:
            args: Additional mypy arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mypy"
        )

        mypy_args = ["mypy", "--incremental", str(self.pkg_path)]
        if args:
            mypy_args.extend(args)

        result = self.subprocess_runner.run_uv_command(
            mypy_args,
            cwd=self.root_path,
            timeout=self.config.quality.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="mypy",
                context="Performing static type checking using MyPy",
                category=self.category,
                executed_commands=[
                    f"{prefix} {' '.join(mypy_args)}" if prefix else " ".join(mypy_args)
                ],
            )

        return result

    def typecheck(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run both BasedPyright and Mypy type checks.

        Args:
            args: Additional arguments (applied to both tools)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="typecheck"
        )

        # Run BasedPyright first
        basedpyright_result = self.basedpyright(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

        # Run Mypy regardless of BasedPyright result
        mypy_result = self.mypy(
            args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

        # Combine results
        combined_stdout = ""
        if basedpyright_result.stdout:
            combined_stdout += f"BasedPyright:\n{basedpyright_result.stdout}\n"
        if mypy_result.stdout:
            combined_stdout += f"Mypy:\n{mypy_result.stdout}"

        combined_stderr = ""
        if basedpyright_result.stderr:
            combined_stderr += f"BasedPyright errors:\n{basedpyright_result.stderr}\n"
        if mypy_result.stderr:
            combined_stderr += f"Mypy errors:\n{mypy_result.stderr}"

        # Success only if both succeed
        success = basedpyright_result.success and mypy_result.success
        exit_code = (
            0 if success else (basedpyright_result.exit_code or mypy_result.exit_code)
        )

        result = ToolResult(
            success=success,
            exit_code=exit_code,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            prefix = self.config.display_command_prefix
            result.learning_info = self.learning_engine.explain_command(
                command="typecheck",
                context="Running multiple type checkers for comprehensive analysis",
                category=self.category,
                executed_commands=[
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
                ],
            )

        return result

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
