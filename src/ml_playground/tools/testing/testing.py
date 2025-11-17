"""Testing tools category implementation."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Mapping, cast

import tomllib

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import (
    SubprocessRunner,
    RealSubprocessRunner,
)
from .unit import run_unit, run_regression
from .integration import run_integration
from .e2e import run_e2e, run_acceptance
from .property import run_property_tests
from .coverage import run_coverage_test, run_coverage_report, run_coverage
from .coverage_helpers import (
    clean_pytest_output as _clean_pytest_output_helper,
    compute_coverage_fingerprint as _compute_coverage_fingerprint_helper,
    read_coverage_manifest as _read_coverage_manifest_helper,
    write_coverage_manifest as _write_coverage_manifest_helper,
    collect_undercovered_files as _collect_undercovered_files_helper,
    format_undercovered_tree as _format_undercovered_tree_helper,
    format_tool_invocation as _format_tool_invocation_helper,
    format_command as _format_command_helper,
)
from . import mutation as _mutation

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

    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self.cache_dir / subdir).mkdir(parents=True, exist_ok=True)

    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.sqlite"

    def _coverage_manifest_path(self) -> Path:
        """Get the manifest file path storing coverage fingerprint metadata."""
        return self.cache_dir / "coverage" / "coverage_manifest.json"

    def _compute_coverage_fingerprint(self) -> str:
        """Compute a fingerprint representing the current coverage-relevant sources."""
        return _compute_coverage_fingerprint_helper(self.root_path)

    def _read_coverage_manifest(self) -> dict[str, Any] | None:
        """Load the stored coverage fingerprint manifest if it exists."""
        return _read_coverage_manifest_helper(self._coverage_manifest_path())

    def _write_coverage_manifest(self, *, fingerprint: str) -> None:
        """Persist the current coverage fingerprint."""
        _write_coverage_manifest_helper(
            self._coverage_manifest_path(), fingerprint=fingerprint
        )

    def _clean_pytest_output(self, output: str) -> str:
        """Remove pytest progress lines and xdist status messages."""
        return _clean_pytest_output_helper(output)

    def _clean_pytest_result(self, result: ToolResult) -> ToolResult:
        if result.stdout:
            result.stdout = self._clean_pytest_output(result.stdout)
        return result

    def _collect_coverage_metrics(
        self,
        env: dict[str, str],
        operation_id: OperationId,
        *,
        executed_commands: list[str] | None = None,
    ) -> tuple[ToolResult | None, list[str]]:
        coverage_file = self._coverage_file()
        coverage_dir = coverage_file.parent
        json_path = coverage_dir / "coverage.json"

        if not json_path.exists():
            coverage_json_cmd = ["coverage", "json", "-o", str(json_path)]
            formatted_cmd = self._format_command(coverage_json_cmd)
            if executed_commands is not None and formatted_cmd not in executed_commands:
                executed_commands.append(formatted_cmd)
            json_result = self.subprocess_runner.run_uv_command(
                coverage_json_cmd,
                cwd=self.root_path,
                env=env,
                timeout=self.config.testing.timeout,
                operation_id=operation_id,
            )
            if not json_result.success:
                return json_result, []
        else:
            if executed_commands is not None:
                formatted_cmd = self._format_command(
                    ["coverage", "json", "-o", str(json_path)]
                )
                if formatted_cmd not in executed_commands:
                    executed_commands.append(formatted_cmd)

        if not json_path.exists():
            raise ToolExecutionError(
                "Coverage JSON data not found",
                reason=f"Coverage JSON file missing: {json_path}",
                rationale="Coverage metrics require JSON report generation",
            )

        try:
            with json_path.open(encoding="utf-8") as f:
                coverage_data = json.load(f)
            totals = coverage_data["totals"]
        except (json.JSONDecodeError, KeyError) as exc:
            raise ToolExecutionError(
                "Failed to parse coverage JSON for summary",
                reason=str(exc),
                rationale="Coverage JSON must contain totals for reporting metrics",
            ) from exc

        statements = totals.get("num_statements", 0)
        covered_lines = totals.get("covered_lines", 0)
        num_branches = totals.get("num_branches", 0)
        covered_branches = totals.get("covered_branches", 0)

        line_pct = (covered_lines / statements * 100) if statements else 0.0
        branch_pct = (covered_branches / num_branches * 100) if num_branches else 0.0

        metrics_lines = [
            f"Coverage totals: lines={line_pct:.2f}% ({covered_lines}/{statements})",
        ]
        if num_branches:
            metrics_lines.append(
                f"Branch totals: branches={branch_pct:.2f}% ({covered_branches}/{num_branches})"
            )
        else:
            metrics_lines.append(
                "Branch totals: not available (no branch data collected)"
            )

        return None, metrics_lines

    def _format_coverage_status(
        self,
        *,
        metric: str,
        percentage: float,
        threshold: float,
        passed: bool,
    ) -> str:
        icon = "✅" if passed else "❌"
        label = "SUCCESS" if passed else "FAILURE"
        comparator = ">=" if passed else "<"
        return (
            f"[coverage] {icon} {label}: {metric} coverage "
            f"{percentage:.2f}% {comparator} {threshold:.2f}%."
        )

    def _collect_undercovered_files(
        self, coverage_data: dict[str, Any]
    ) -> list[tuple[str, float, float | None]]:
        return _collect_undercovered_files_helper(coverage_data)

    def _format_undercovered_tree(
        self, entries: list[tuple[str, float, float | None]]
    ) -> list[str]:
        return _format_undercovered_tree_helper(entries)

    def _format_tool_invocation(self, tool: str, args: List[str]) -> str:
        return _format_tool_invocation_helper(
            tool,
            args,
            prefix=self.config.display_command_prefix or "",
        )

    def _format_command(self, command: list[str]) -> str:
        return _format_command_helper(
            command, prefix=self.config.display_command_prefix or ""
        )

    def _ensure_coverage_data(
        self,
        *,
        args: List[str],
        learning_mode: bool,
        verbosity_level: int,
        verbose: bool,
        operation_id: OperationId,
        executed_commands: list[str] | None = None,
        force_regen: bool = False,
    ) -> tuple[ToolResult | None, list[str], Dict[str, str]]:
        coverage_file = self._coverage_file()
        env: Dict[str, str] = {"COVERAGE_FILE": str(coverage_file)}
        notes: list[str] = []
        if executed_commands is None:
            executed_commands = []

        current_fingerprint = self._compute_coverage_fingerprint()
        manifest = self._read_coverage_manifest()
        manifest_fingerprint = manifest.get("fingerprint") if manifest else None

        if (
            not force_regen
            and manifest_fingerprint == current_fingerprint
            and coverage_file.exists()
            and coverage_file.stat().st_size > 0
        ):
            return None, notes, env

        combine_result, combined = self._combine_coverage_fragments(
            env=env,
            operation_id=operation_id,
            executed_commands=executed_commands,
        )
        if isinstance(combine_result, ToolResult):
            return combine_result, notes, env
        if (
            not force_regen
            and manifest_fingerprint == current_fingerprint
            and combined
            and coverage_file.exists()
            and coverage_file.stat().st_size > 0
        ):
            notes.append("Combined existing coverage fragments into coverage.sqlite.")
            self._write_coverage_manifest(fingerprint=current_fingerprint)
            return None, notes, env

        generation_result, generation_notes = self._run_coverage_test_for_data(
            args=args,
            verbosity_level=verbosity_level,
            verbose=verbose,
            operation_id=operation_id,
            executed_commands=executed_commands,
        )
        if isinstance(generation_result, ToolResult):
            return generation_result, notes, env
        notes.extend(generation_notes)

        combine_result, combined = self._combine_coverage_fragments(
            env=env,
            operation_id=operation_id,
            executed_commands=executed_commands,
        )
        if isinstance(combine_result, ToolResult):
            return combine_result, notes, env
        if combined:
            notes.append("Combined coverage fragments into coverage.sqlite.")

        if coverage_file.exists() and coverage_file.stat().st_size > 0:
            self._write_coverage_manifest(fingerprint=current_fingerprint)
            return None, notes, env

        failure = ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr=(
                "Coverage data not produced automatically. Run `tools test coverage` manually "
                "and re-run the command."
            ),
            operation_id=operation_id,
        )
        return failure, notes, env

    def _combine_coverage_fragments(
        self,
        *,
        env: dict[str, str],
        operation_id: OperationId,
        executed_commands: list[str] | None = None,
    ) -> tuple[ToolResult | None, bool]:
        """Combine coverage fragments into the primary coverage.sqlite file."""

        coverage_file = self._coverage_file()
        fragments = list(coverage_file.parent.glob(f"{coverage_file.name}.*"))
        if not fragments:
            return None, False

        combine_cmd = ["coverage", "combine", f"--data-file={coverage_file}"]
        formatted = self._format_command(combine_cmd)

        if executed_commands is not None and formatted not in executed_commands:
            executed_commands.append(formatted)

        result = self.subprocess_runner.run_uv_command(
            combine_cmd,
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if not result.success:
            return result, True

        if coverage_file.exists() and coverage_file.stat().st_size > 0:
            return None, True

        return None, True

    def _run_coverage_test_for_data(
        self,
        *,
        args: List[str],
        verbosity_level: int,
        verbose: bool,
        operation_id: OperationId,
        executed_commands: list[str] | None = None,
    ) -> tuple[ToolResult | None, list[str]]:
        if executed_commands is None:
            executed_commands = []
        # Record that we invoked the unified coverage tool to generate data
        coverage_tool_cmd = self._format_tool_invocation("coverage", args)
        if coverage_tool_cmd not in executed_commands:
            executed_commands.append(coverage_tool_cmd)
        coverage_result = self.coverage_test(
            args,
            learning_mode=False,
            verbosity_level=verbosity_level,
        )
        if not coverage_result.success:
            return coverage_result, []

        message = "Automatically ran coverage to generate coverage data."
        if verbose:
            extra_lines: list[str] = []
            if coverage_result.stdout:
                extra_lines.append(coverage_result.stdout.strip())
            if coverage_result.stderr:
                extra_lines.append(coverage_result.stderr.strip())
            if extra_lines:
                message += "\n" + "\n".join(extra_lines)

        notes = [message]

        coverage_file = self._coverage_file()
        if not coverage_file.exists() or coverage_file.stat().st_size == 0:
            fallback_result, fallback_notes = self._generate_coverage_via_pytest(
                args=args,
                verbose=verbose,
                operation_id=operation_id,
                executed_commands=executed_commands,
            )
            notes.extend(fallback_notes)
            if isinstance(fallback_result, ToolResult):
                return fallback_result, notes

        return None, notes

    def _generate_coverage_via_pytest(
        self,
        *,
        args: List[str],
        verbose: bool,
        operation_id: OperationId,
        executed_commands: list[str] | None = None,
    ) -> tuple[ToolResult | None, list[str]]:
        if executed_commands is None:
            executed_commands = []
        coverage_file = self._coverage_file()
        env = {
            "COVERAGE_FILE": str(coverage_file),
            "HYPOTHESIS_DATABASE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self.cache_dir / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
        }

        pytest_args = ["tests/unit", "tests/property", *args]
        pytest_cmd = ["pytest", *pytest_args]
        formatted_pytest_cmd = self._format_command(pytest_cmd)
        if formatted_pytest_cmd not in executed_commands:
            executed_commands.append(formatted_pytest_cmd)
        result = self.subprocess_runner.run_pytest_command(
            pytest_args,
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)
        if not result.success:
            return result, []

        message = "Coverage pipeline generated no data; reran pytest to create coverage artifacts."
        if verbose:
            extra_lines: list[str] = []
            if result.stdout:
                extra_lines.append(result.stdout.strip())
            if result.stderr:
                extra_lines.append(result.stderr.strip())
            if extra_lines:
                message += "\n" + "\n".join(extra_lines)
        return None, [message]

    def _read_coverage_thresholds_from_config(self) -> dict[str, float]:
        """Read coverage thresholds from pyproject.toml."""
        pyproject_path = self.root_path / "pyproject.toml"
        if not pyproject_path.exists():
            return {}

        try:
            with pyproject_path.open("rb") as f:
                config_data: dict[str, Any] = tomllib.load(f)

            def _as_dict(obj: Any) -> dict[str, Any]:
                result: dict[str, Any] = {}
                if isinstance(obj, dict):
                    for k, v in cast(Mapping[Any, Any], obj).items():
                        if isinstance(k, str):
                            result[k] = v
                return result

            tool_cfg = _as_dict(config_data.get("tool"))
            ml_cfg = _as_dict(tool_cfg.get("ml_playground"))
            coverage_cfg = _as_dict(ml_cfg.get("coverage"))
            thresholds_cfg = _as_dict(coverage_cfg.get("thresholds"))

            return {
                "line_threshold": float(thresholds_cfg.get("line_threshold", 0.0)),
                "branch_threshold": float(thresholds_cfg.get("branch_threshold", 0.0)),
            }
        except Exception:
            # If we can't read the config, return empty dict (no thresholds)
            return {}

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
            except Exception as e:  # pragma: no cover - defensive
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
