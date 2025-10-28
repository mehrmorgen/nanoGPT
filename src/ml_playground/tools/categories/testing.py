"""Testing tools category implementation."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


class TestingTools:
    """Testing tools implementation."""

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
        self.subprocess_runner = subprocess_runner or _default_runner
        self.learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "test"

    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self.cache_dir / subdir).mkdir(parents=True, exist_ok=True)

    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.sqlite"

    def _coverage_env(self, coverage_file: Optional[Path] = None) -> Dict[str, str]:
        """Get environment variables for coverage execution."""
        if coverage_file is None:
            coverage_file = self._coverage_file()

        self._ensure_cache_dirs("coverage", "hypothesis")
        coverage_file.parent.mkdir(parents=True, exist_ok=True)

        return {
            "HYPOTHESIS_DATABASE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
            "COVERAGE_FILE": str(coverage_file),
        }

    def unit(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run unit tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="unit"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["tests/unit", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="unit",
                context="Running unit tests to verify individual components work correctly",
                category=self.category,
                executed_commands=[f"pytest tests/unit {' '.join(args)}".strip()],
            )

        return result

    def integration(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run integration tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="integration"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["-m", "integration", "--no-cov", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="integration",
                context="Running integration tests to verify components work together correctly",
                category=self.category,
                executed_commands=[
                    f"pytest -m integration --no-cov {' '.join(args)}".strip()
                ],
            )

        return result

    def e2e(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run end-to-end tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="e2e"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["tests/e2e", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="e2e",
                context="Running end-to-end tests to verify complete user workflows",
                category=self.category,
                executed_commands=[f"pytest tests/e2e {' '.join(args)}".strip()],
            )

        return result

    def acceptance(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run acceptance tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="acceptance"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["tests/acceptance", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="acceptance",
                context="Running acceptance tests to validate business requirements",
                category=self.category,
                executed_commands=[f"pytest tests/acceptance {' '.join(args)}".strip()],
            )

        return result

    def property_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run property-based tests.

        Args:
            args: Additional pytest arguments
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="property"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["tests/property", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="property",
                context="Running property-based tests to find edge cases with random inputs",
                category=self.category,
                executed_commands=[f"pytest tests/property {' '.join(args)}".strip()],
            )

        return result

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
            ["tests", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="all",
                context="Running complete test suite for comprehensive validation",
                category=self.category,
                executed_commands=[f"pytest tests {' '.join(args)}".strip()],
            )

        return result

    def coverage_test(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run tests with coverage collection.

        Args:
            args: Additional arguments (ignored for coverage test)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-test"
        )

        # Clean up existing coverage data
        coverage_file = self._coverage_file()
        if coverage_file.exists():
            coverage_file.unlink()

        # Remove any coverage fragments
        for fragment in coverage_file.parent.glob("coverage.sqlite.*"):
            if fragment.name != coverage_file.name:
                fragment.unlink()

        # Set up coverage environment
        env = self._coverage_env(coverage_file)

        # Run coverage with pytest
        result = self.subprocess_runner.run_uv_command(
            [
                "coverage",
                "run",
                f"--data-file={coverage_file}",
                "-m",
                "pytest",
                "-n",
                "0",  # No parallel execution for coverage
                "tests/unit",
                "tests/property",
            ],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="coverage-test",
                context="Running tests while measuring code coverage to identify untested code",
                category=self.category,
                executed_commands=[
                    f"coverage run --data-file={coverage_file} -m pytest -n 0 tests/unit tests/property"
                ],
            )

        return result

    def coverage_report(
        self,
        args: List[str],
        fail_under: float = 0.0,
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Generate coverage reports.

        Args:
            args: Additional arguments (ignored)
            fail_under: Minimum coverage threshold
            verbose: Whether to show verbose output
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-report"
        )

        coverage_file = self._coverage_file()
        if not coverage_file.exists():
            raise ToolExecutionError(
                "Coverage data file not found",
                reason=f"Missing coverage file: {coverage_file}",
                rationale="Coverage reports require prior execution of coverage-test command",
            )

        # Check for empty coverage file in CI
        ci_strict = os.environ.get("CI", "").lower() == "true"
        if ci_strict and coverage_file.stat().st_size == 0:
            raise ToolExecutionError(
                "Coverage data file is empty",
                reason="Coverage file exists but contains no data",
                rationale="Empty coverage files indicate test execution problems in CI",
            )

        env = {"COVERAGE_FILE": str(coverage_file)}
        coverage_dir = coverage_file.parent

        # Generate multiple report formats
        commands = [
            (
                ["coverage", "report", "-m", "--fail-under", f"{fail_under:.2f}"],
                "terminal report",
            ),
            (["coverage", "html", "-d", str(coverage_dir / "htmlcov")], "HTML report"),
            (
                ["coverage", "json", "-o", str(coverage_dir / "coverage.json")],
                "JSON report",
            ),
            (
                ["coverage", "xml", "-o", str(coverage_dir / "coverage.xml")],
                "XML report",
            ),
        ]

        results = []
        for command, description in commands:
            try:
                result = self.subprocess_runner.run_uv_command(
                    command,
                    cwd=self.root_path,
                    env=env,
                    timeout=self.config.testing.timeout,
                    operation_id=operation_id,
                )
                if not result.success:
                    return result  # Return first failure
                results.append(f"Generated {description}")
            except Exception as exc:
                raise ToolExecutionError(
                    f"Failed to generate {description}",
                    reason=str(exc),
                    rationale="Coverage report generation must succeed for quality gates",
                ) from exc

        # Show artifacts if verbose
        output = "\n".join(results)
        if verbose:
            artifacts = []
            for path in sorted(coverage_dir.iterdir()):
                artifacts.append(f"  - {path.relative_to(self.root_path)}")
            if artifacts:
                output += "\n\nCoverage artifacts:\n" + "\n".join(artifacts)

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
                command="coverage-report",
                context="Generating coverage reports in multiple formats for analysis",
                category=self.category,
                executed_commands=[
                    "coverage report -m",
                    "coverage html",
                    "coverage json",
                    "coverage xml",
                ],
            )

        return result

    def coverage_threshold(
        self,
        args: List[str],
        line_threshold: float = 0.0,
        branch_threshold: float = 0.0,
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Check coverage thresholds.

        Args:
            args: Additional arguments (ignored)
            line_threshold: Minimum line coverage percentage
            branch_threshold: Minimum branch coverage percentage
            verbose: Whether to show verbose output
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-threshold"
        )

        coverage_file = self._coverage_file()
        if not coverage_file.exists():
            raise ToolExecutionError(
                "Coverage data file not found",
                reason=f"Missing coverage file: {coverage_file}",
                rationale="Coverage threshold checks require prior execution of coverage-test command",
            )

        # Generate JSON report for analysis
        env = {"COVERAGE_FILE": str(coverage_file)}
        json_path = coverage_file.parent / "coverage.json"

        result = self.subprocess_runner.run_uv_command(
            ["coverage", "json", "-o", str(json_path)],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if not result.success:
            return result

        # Parse coverage data
        try:
            with json_path.open(encoding="utf-8") as f:
                coverage_data = json.load(f)
            totals = coverage_data["totals"]
        except (json.JSONDecodeError, KeyError) as exc:
            raise ToolExecutionError(
                "Failed to parse coverage data",
                reason=f"Invalid coverage JSON: {exc}",
                rationale="Coverage data must be parseable for threshold validation",
            ) from exc

        # Extract metrics
        num_branches = totals.get("num_branches", 0)
        covered_branches = totals.get("covered_branches", 0)
        covered_lines = totals.get("covered_lines", 0)
        num_statements = totals.get("num_statements", 0)

        # Calculate percentages
        line_pct = (covered_lines / num_statements) * 100 if num_statements else 0.0
        branch_pct = (covered_branches / num_branches) * 100 if num_branches else 0.0

        # Check thresholds
        messages = []

        if line_threshold > 0:
            if num_statements == 0:
                messages.append("Line coverage totals missing from coverage data")
            elif line_pct < line_threshold:
                messages.append(
                    f"Line coverage {line_pct:.2f}% < {line_threshold:.2f}%. "
                    "Run 'uv run tools test coverage-test' to collect coverage data."
                )

        if branch_threshold > 0:
            if num_branches == 0:
                messages.append("Branch coverage data missing from coverage data")
            elif branch_pct < branch_threshold:
                messages.append(
                    f"Branch coverage {branch_pct:.2f}% < {branch_threshold:.2f}%."
                )

        # Prepare output
        output = ""
        if verbose:
            output = (
                f"Coverage totals: lines={line_pct:.2f}% branches={branch_pct:.2f}%"
            )

        if messages:
            error_output = "\n".join(f"[coverage] {msg}" for msg in messages)
            result = ToolResult(
                success=False,
                exit_code=1,
                stdout=output,
                stderr=error_output,
                operation_id=operation_id,
            )

            if learning_mode:
                self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
                result.learning_info = self.learning_engine.explain_command(
                    command="coverage-threshold",
                    context="Checking coverage thresholds to enforce quality standards",
                    category=self.category,
                    executed_commands=["coverage json"],
                )

            return result

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
                command="coverage-threshold",
                context="Checking coverage thresholds to enforce quality standards",
                category=self.category,
                executed_commands=["coverage json"],
            )

        return result

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

        cleaned = []
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

    def _cosmic_ray_session_file(self) -> Path:
        """Get the path to the Cosmic Ray session file."""
        return Path(".cache/cosmic-ray/session.sqlite")

    def mutation_reset(self, args: List[str]) -> ToolResult:
        """Remove the cached Cosmic Ray session.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-reset"
        )

        session_file = self._cosmic_ray_session_file()
        if session_file.exists():
            try:
                session_file.unlink()
                output = f"Removed Cosmic Ray session: {session_file}"
            except Exception as exc:
                raise ToolExecutionError(
                    f"Failed to remove Cosmic Ray session file: {session_file}",
                    reason=f"File deletion failed: {exc}",
                    rationale="Session file must be removable for clean mutation testing",
                ) from exc
        else:
            output = f"Cosmic Ray session file does not exist: {session_file}"

        return ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )

    def mutation_summary(self, args: List[str]) -> ToolResult:
        """Show a summary of the current Cosmic Ray configuration.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with configuration summary
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-summary"
        )

        try:
            # Import cosmic ray functionality
            from cosmic_ray.config import load_config
            from cosmic_ray.modules import find_modules

            config_file = Path("pyproject.toml")
            cfg = load_config(str(config_file))

            # Extract configuration details
            session_cfg = cfg.get("session", {})
            session_path = Path(
                session_cfg.get("path", ".cache/cosmic-ray/session.sqlite")
            )

            test_runner_cfg = cfg.get("test-runner", {})
            test_command = test_runner_cfg.get("command", "pytest")

            # Find modules to mutate
            modules_cfg = cfg.get("modules", {})
            modules = find_modules(modules_cfg)

            output_lines = [
                f"[mutation] config: {config_file}",
                f"[mutation] session: {session_path}",
                f"[mutation] test command: {test_command}",
                f"[mutation] modules to mutate: {len(modules)}",
            ]

            for module in sorted(modules)[:5]:  # Show first 5 modules
                output_lines.append(f"[mutation]   - {module}")

            if len(modules) > 5:
                output_lines.append(f"[mutation]   ... and {len(modules) - 5} more")

            return ToolResult(
                success=True,
                exit_code=0,
                stdout="\\n".join(output_lines),
                stderr="",
                operation_id=operation_id,
            )

        except ImportError as e:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"cosmic_ray must be installed to use mutation testing: {e}",
                operation_id=operation_id,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Failed to generate mutation summary: {e}",
                operation_id=operation_id,
            )

    def mutation_init(self, args: List[str]) -> ToolResult:
        """Initialize the Cosmic Ray session database if needed.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-init"
        )

        session_file = self._cosmic_ray_session_file()
        session_file.parent.mkdir(parents=True, exist_ok=True)

        # Run cosmic-ray init, allowing non-zero exit (reusing existing session)
        result = self._subprocess_runner.run_uv_command(
            ["cosmic-ray", "init", "pyproject.toml", str(session_file)],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        # Adjust output based on result
        if result.success:
            output = "Cosmic Ray session initialized"
        else:
            output = "Cosmic Ray init skipped (reusing existing session)"
            # Convert to success since this is expected behavior
            result = ToolResult(
                success=True,
                exit_code=0,
                stdout=output,
                stderr=result.stderr,
                operation_id=operation_id,
            )

        return result

    def mutation_exec(self, args: List[str]) -> ToolResult:
        """Execute mutation tests with Cosmic Ray.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-exec"
        )

        session_file = self._cosmic_ray_session_file()
        if not session_file.exists():
            raise ToolExecutionError(
                "Cosmic Ray session file not found",
                reason=f"Missing session file: {session_file}",
                rationale="Mutation execution requires initialized session database",
            )

        return self._subprocess_runner.run_uv_command(
            ["cosmic-ray", "exec", "pyproject.toml", str(session_file)],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

    def mutation_report(self, args: List[str]) -> ToolResult:
        """Generate a mutation testing report.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with report details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-report"
        )

        try:
            import sqlite3
            from collections import Counter
            from cosmic_ray.config import load_config

            config_file = Path("pyproject.toml")
            cfg = load_config(str(config_file))
            session_cfg = cfg.get("session", {})
            session_path = Path(
                session_cfg.get("path", ".cache/cosmic-ray/session.sqlite")
            )

            if not session_path.exists():
                return ToolResult(
                    success=True,
                    exit_code=0,
                    stdout="[mutation] session file not found: no results to report",
                    stderr="",
                    operation_id=operation_id,
                )

            with sqlite3.connect(session_path) as conn:
                conn.row_factory = lambda cursor, row: row[0]
                total = (
                    conn.execute("SELECT COUNT(*) FROM work_results").fetchone() or 0
                )
                outcomes = Counter(
                    conn.execute(
                        "SELECT COALESCE(test_outcome, 'UNKNOWN') FROM work_results"
                    )
                )

            output_lines = [f"[mutation] mutants processed: {total}"]
            if outcomes:
                for outcome, count in sorted(outcomes.items()):
                    label = outcome.lower()
                    output_lines.append(f"[mutation]   {label}: {count}")

            return ToolResult(
                success=True,
                exit_code=0,
                stdout="\\n".join(output_lines),
                stderr="",
                operation_id=operation_id,
            )

        except ImportError as e:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"cosmic_ray must be installed to use mutation testing: {e}",
                operation_id=operation_id,
            )
        except Exception as e:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Failed to generate mutation report: {e}",
                operation_id=operation_id,
            )

    def mutation_run(self, args: List[str]) -> ToolResult:
        """Run the full mutation testing pipeline.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="mutation-run"
        )

        # Run mutation testing pipeline
        steps = [
            ("reset", self.mutation_reset),
            ("summary", self.mutation_summary),
            ("init", self.mutation_init),
            ("exec", self.mutation_exec),
            ("report", self.mutation_report),
        ]

        results = []
        combined_stdout = ""
        combined_stderr = ""

        for step_name, step_func in steps:
            try:
                result = step_func([])
                results.append((step_name, result))

                if result.stdout:
                    combined_stdout += f"Mutation {step_name}:\\n{result.stdout}\\n"
                if result.stderr:
                    combined_stderr += (
                        f"Mutation {step_name} warnings:\\n{result.stderr}\\n"
                    )

                if not result.success:
                    # Return failure at first failed step
                    return ToolResult(
                        success=False,
                        exit_code=result.exit_code,
                        stdout=combined_stdout,
                        stderr=combined_stderr,
                        operation_id=operation_id,
                    )

            except Exception as exc:
                return ToolResult(
                    success=False,
                    exit_code=1,
                    stdout=combined_stdout,
                    stderr=f"Mutation {step_name} failed: {exc}",
                    operation_id=operation_id,
                )

        return ToolResult(
            success=True,
            exit_code=0,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )
