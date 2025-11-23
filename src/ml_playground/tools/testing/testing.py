"""Testing tools category implementation."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Callable, List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
)

from . import mutation as _mutation
from .coverage import run_coverage, run_coverage_report, run_coverage_test
from .coverage_helpers import (
    clean_pytest_output as _clean_pytest_output_helper,
)
from .e2e import run_acceptance, run_e2e
from .integration import run_integration
from .property import run_property_tests
from .unit import run_regression, run_unit

# Module-level default runner for tests to patch if needed
_default_runner: SubprocessRunner | None = None


class TestingTools:
    """Testing tools implementation."""

    _PYTEST_PROGRESS_RE = re.compile(r"^[\.s]+(?:\s+\[\s*\d+%])?$")

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: Optional[SubprocessRunner] = None,
    ) -> None:
        """Initialize testing tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
        global _default_runner  # noqa: PLW0603 - providing a test patch point
        if _default_runner is None:
            _default_runner = RealSubprocessRunner()
        self.subprocess_runner = subprocess_runner or _default_runner
        self.learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "test"

    def _clean_pytest_output(self, output: str) -> str:
        """Remove pytest progress lines and xdist status messages."""
        return _clean_pytest_output_helper(output)

    def _clean_pytest_result(self, result: ToolResult) -> ToolResult:
        if result.stdout:
            result.stdout = self._clean_pytest_output(result.stdout)
        return result

    def unit(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run unit tests."""
        return run_unit(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def regression(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run regression policy suites."""
        return run_regression(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def integration(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run integration tests."""
        return run_integration(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def e2e(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run end-to-end tests."""
        return run_e2e(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def acceptance(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run acceptance tests."""
        return run_acceptance(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def property_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run property-based tests."""
        return run_property_tests(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def all_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run all tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="all"
        )

        result = self.subprocess_runner.run_pytest_command(
            [
                "tests/unit",
                "tests/property",
                "tests/regression",
                *args,
            ],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="all",
                context="Running complete test suite for comprehensive validation",
                category=self.category,
                executed_commands=[
                    "pytest tests/unit tests/property tests/regression"
                    + (f" {' '.join(args)}" if args else "")
                ],
            )

        return result

    def coverage_test(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run tests with coverage collection."""
        return run_coverage_test(
            config=self.config,
            root_path=self.root_path,
            args=args,
            subprocess_runner=self.subprocess_runner,
            cache_dir=self.cache_dir,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def coverage_report(
        self,
        args: List[str],
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Generate coverage reports."""
        return run_coverage_report(
            config=self.config,
            root_path=self.root_path,
            args=args,
            verbose=verbose,
            subprocess_runner=self.subprocess_runner,
            cache_dir=self.cache_dir,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def coverage_threshold(
        self,
        args: List[str],
        line_threshold: float = 0.0,
        branch_threshold: float = 0.0,
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Check coverage thresholds."""
        from .coverage import run_coverage_threshold

        return run_coverage_threshold(
            config=self.config,
            root_path=self.root_path,
            args=args,
            line_threshold=line_threshold,
            branch_threshold=branch_threshold,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            subprocess_runner=self.subprocess_runner,
            cache_dir=self.cache_dir,
            force_regen=force_regen,
        )

    def coverage(
        self,
        args: List[str],
        *,
        line_threshold: float | None = None,
        branch_threshold: float | None = None,
        verbose: bool = False,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Run the complete coverage pipeline (report + threshold)."""
        return run_coverage(
            config=self.config,
            root_path=self.root_path,
            args=args,
            line_threshold=line_threshold,
            branch_threshold=branch_threshold,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
            subprocess_runner=self.subprocess_runner,
            cache_dir=self.cache_dir,
        )

    def clean(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Clean test artifacts and caches.

        Args:
            args: Additional arguments (ignored)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="clean"
        )

        # Paths to clean
        paths_to_clean = [
            self.root_path / ".pytest_cache",
            self.root_path / "htmlcov",
            self.cache_dir / "coverage",
            self.cache_dir / "hypothesis",
        ]

        cleaned: list[str] = []
        for path in paths_to_clean:
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                cleaned.append(str(path.relative_to(self.root_path)))

        output = f"Cleaned {len(cleaned)} paths" if cleaned else "No artifacts to clean"
        if cleaned:
            output += ":\n" + "\n".join(f"  - {path}" for path in cleaned)

        result = ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="clean",
                context="Cleaning test artifacts and caches to ensure clean test environment",
                category=self.category,
                executed_commands=[f"Removed {len(cleaned)} artifact paths"],
            )

        return result

    def mutation_reset(self, args: List[str]) -> ToolResult:
        """Remove the cached Cosmic Ray session (delegated)."""
        return _mutation.mutation_reset(self.config, self.root_path)

    def mutation_summary(self, args: List[str]) -> ToolResult:
        """Show a summary of the current Cosmic Ray configuration (delegated)."""
        return _mutation.mutation_summary(self.config, self.root_path)

    def mutation_init(self, args: List[str]) -> ToolResult:
        """Initialize the Cosmic Ray session database if needed (delegated)."""
        return _mutation.mutation_init(
            self.config, self.root_path, self.subprocess_runner
        )

    def mutation_exec(self, args: List[str]) -> ToolResult:
        """Execute mutation tests with Cosmic Ray (delegated)."""
        return _mutation.mutation_exec(
            self.config, self.root_path, self.subprocess_runner
        )

    def mutation_report(self, args: List[str]) -> ToolResult:
        """Generate a mutation testing report (delegated)."""
        return _mutation.mutation_report(self.config, self.root_path)

    def mutation_run(self, args: List[str]) -> ToolResult:
        """Run the full mutation testing pipeline, calling instance methods.

        This preserves subclass override points for tests while keeping
        individual step implementations delegated to the mutation module.
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-run"
        )

        steps: list[tuple[str, Callable[[], ToolResult]]] = [
            ("reset", lambda: self.mutation_reset([])),
            ("summary", lambda: self.mutation_summary([])),
            ("init", lambda: self.mutation_init([])),
            ("exec", lambda: self.mutation_exec([])),
            ("report", lambda: self.mutation_report([])),
        ]

        combined_stdout = ""
        combined_stderr = ""

        for step_name, step in steps:
            try:
                result = step()
                if result.stdout:
                    combined_stdout += f"Mutation {step_name}:\n{result.stdout}\n"
                if result.stderr:
                    combined_stderr += (
                        f"Mutation {step_name} warnings:\n{result.stderr}\n"
                    )
                if not result.success:
                    return ToolResult(
                        success=False,
                        exit_code=result.exit_code,
                        stdout=combined_stdout,
                        stderr=combined_stderr or result.stderr,
                        operation_id=operation_id,
                    )
            except Exception as e:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout=combined_stdout,
                    stderr=f"Mutation {step_name} failed: {e}",
                    operation_id=operation_id,
                )

        return ToolResult(
            success=True,
            exit_code=0,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
