"""Testing tools category implementation."""

from __future__ import annotations

import json
import re
import shutil
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Dict, List, Optional, cast


from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import LearningInfo, OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import DEFAULT_RUNNER, SubprocessRunner

from ml_playground.framework.core.di_implementations import DefaultJsonParser
from . import budget as budget_module
from . import coverage as coverage_module
from . import mutation as mutation_module
from . import unit as unit_module
from .services.coverage_service import CoverageService
from .services.mutation_service import (
    CosmicRayService,
    MutationService,
)


def _regression_env(config: ToolsConfig) -> dict[str, str]:
    """Return env overrides for stable regression xdist execution."""
    workers = config.testing.parallel_workers
    if workers <= 0:
        # `-n auto` with very high core counts can stall xdist startup on local Macs.
        workers = 4
    return {"PYTEST_XDIST_AUTO_NUM_WORKERS": str(workers)}


def run_unit(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    return unit_module.run_unit(
        config=config,
        root_path=root_path,
        args=args,
        subprocess_runner=subprocess_runner,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
    )


def run_coverage_report(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    force_regen: bool = False,
) -> ToolResult:
    return coverage_module.run_coverage_report(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        force_regen=force_regen,
    )


def run_coverage_map(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    force_regen: bool = False,
) -> ToolResult:
    return coverage_module.run_coverage_map(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        force_regen=force_regen,
    )


def run_test_budget_report(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    refresh: bool = False,
) -> ToolResult:
    return budget_module.run_test_budget_report(
        config=config,
        root_path=root_path,
        args=args,
        subprocess_runner=subprocess_runner,
        refresh=refresh,
    )


def run_coverage_threshold(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    line_threshold: float,
    branch_threshold: float,
    verbose: bool,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    force_regen: bool = False,
) -> ToolResult:
    return coverage_module.run_coverage_threshold(
        config=config,
        root_path=root_path,
        args=args,
        line_threshold=line_threshold,
        branch_threshold=branch_threshold,
        verbose=verbose,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        force_regen=force_regen,
    )


class TestingTools:
    """Testing tools implementation."""

    _PYTEST_PROGRESS_RE = re.compile(r"^[\.s]+(?:\s+\[\s*\d+%])?$")

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: Optional[SubprocessRunner] = None,
        mutation_service: Optional[MutationService] = None,
    ) -> None:
        """Initialize testing tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
            mutation_service: Mutation testing service for dependency injection
        """
        self._config = config
        self._root_path = root_path
        self._cache_dir = root_path / ".cache"
        self._subprocess_runner = subprocess_runner or DEFAULT_RUNNER
        self._learning_engine = LearningModeEngine()
        self._coverage_service = CoverageService(root_path)
        self._mutation_service = mutation_service or CosmicRayService()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "test"

    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self._cache_dir / subdir).mkdir(parents=True, exist_ok=True)

    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self._cache_dir / "coverage" / "coverage.sqlite"

    def _coverage_manifest_path(self) -> Path:
        """Get the manifest file path storing coverage fingerprint metadata."""
        return self._cache_dir / "coverage" / "coverage_manifest.json"

    def _read_coverage_thresholds_from_config(self) -> dict[str, float]:
        """Load coverage thresholds from pyproject.toml if present."""

        pyproject_path = self._root_path / "pyproject.toml"
        if not pyproject_path.exists():
            return {}

        try:
            with pyproject_path.open("rb") as handle:
                data = tomllib.load(handle)
        except (OSError, tomllib.TOMLDecodeError):
            return {}

        tool_cfg = cast(
            Mapping[str, object],
            cast(Mapping[str, object], data).get("tool", {}),
        )
        ml_cfg = cast(Mapping[str, object], tool_cfg.get("ml_playground", {}))
        coverage_cfg = cast(Mapping[str, object], ml_cfg.get("coverage", {}))
        thresholds = cast(Mapping[str, object], coverage_cfg.get("thresholds", {}))

        result: dict[str, float] = {}
        for key in ("line_threshold", "branch_threshold"):
            value = thresholds.get(key)
            if isinstance(value, (int, float)):
                result[key] = float(value)
        return result

    def _compute_coverage_fingerprint(self) -> str:
        """Compute a fingerprint representing the current coverage-relevant sources."""
        from .coverage_helpers import compute_coverage_fingerprint

        return compute_coverage_fingerprint(self._root_path)

    def _read_coverage_manifest(self) -> Mapping[str, object] | None:
        """Load the stored coverage fingerprint manifest if it exists."""

        manifest_path = self._coverage_manifest_path()
        if not manifest_path.exists():
            return None
        try:
            content = manifest_path.read_text(encoding="utf-8")
            raw_data = DefaultJsonParser().parse_json(content)
            if isinstance(raw_data, dict):
                return raw_data
            return None
        except (OSError, json.JSONDecodeError):
            return None

    def _write_coverage_manifest(self, *, fingerprint: str) -> None:
        """Persist the current coverage fingerprint."""

        manifest_path = self._coverage_manifest_path()
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"fingerprint": fingerprint}
        with manifest_path.open("w", encoding="utf-8") as manifest_file:
            json.dump(payload, manifest_file)

    def _clean_pytest_output(self, output: str) -> str:
        """Remove pytest progress lines and xdist status messages."""

        cleaned_lines: list[str] = []
        for line in output.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("bringing up nodes"):
                continue
            if self._PYTEST_PROGRESS_RE.fullmatch(stripped):
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines)

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
        json_result: ToolResult | None = None

        if not json_path.exists():
            coverage_json_cmd = ["coverage", "json", "-o", str(json_path)]
            formatted_cmd = self._format_command(coverage_json_cmd)
            if executed_commands is not None and formatted_cmd not in executed_commands:
                executed_commands.append(formatted_cmd)
            json_result = self._subprocess_runner.run_uv_command(
                coverage_json_cmd,
                cwd=self._root_path,
                env=env,
                timeout=self._config.testing.timeout,
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

        metrics_lines = self._coverage_service.collect_metrics(json_path)
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
        self, coverage_data: Mapping[str, object]
    ) -> list[tuple[str, float, float | None]]:
        return self._coverage_service.get_undercovered_files(coverage_data)

    def _format_undercovered_tree(
        self, entries: list[tuple[str, float, float | None]]
    ) -> list[str]:
        return self._coverage_service.render_undercovered_tree(entries)

    def _format_tool_invocation(self, tool: str, args: List[str]) -> str:
        suffix = f" {' '.join(args)}" if args else ""
        return f"Executed: uv run tools test {tool}{suffix}"

    def _format_command(self, command: list[str]) -> str:
        return "Executed: " + " ".join(command)

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

        result = self._subprocess_runner.run_uv_command(
            combine_cmd,
            cwd=self._root_path,
            env=env,
            timeout=self._config.testing.timeout,
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
            "HYPOTHESIS_DATABASE_DIRECTORY": str(self._cache_dir / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self._cache_dir / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
        }

        pytest_args = ["tests/unit", "tests/property", *args]
        pytest_cmd = ["pytest", *pytest_args]
        formatted_pytest_cmd = self._format_command(pytest_cmd)
        if formatted_pytest_cmd not in executed_commands:
            executed_commands.append(formatted_pytest_cmd)
        result = self._subprocess_runner.run_pytest_command(
            pytest_args,
            cwd=self._root_path,
            env=env,
            timeout=self._config.testing.timeout,
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

    def _coverage_env(self, coverage_file: Path | None = None) -> dict[str, str]:
        """Get environment variables for coverage execution."""
        if coverage_file is None:
            coverage_file = self._coverage_file()

        self._ensure_cache_dirs("coverage", "hypothesis")
        coverage_file.parent.mkdir(parents=True, exist_ok=True)

        return {
            "HYPOTHESIS_DATABASE_DIRECTORY": str(self._cache_dir / "hypothesis"),
            "HYPOTHESIS_STORAGE_DIRECTORY": str(self._cache_dir / "hypothesis"),
            "HYPOTHESIS_SEED": "0",
            "PYTHONHASHSEED": "0",
            "COVERAGE_FILE": str(coverage_file),
        }

    def unit(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run unit tests."""
        return self._unit(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _unit(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal unit test implementation."""
        return run_unit(
            config=self._config,
            root_path=self._root_path,
            args=args,
            subprocess_runner=self._subprocess_runner,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
        )

    def regression(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run regression policy suites."""
        return self._regression(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _regression(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal regression test implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="regression"
        )

        result = self._subprocess_runner.run_pytest_command(
            ["tests/regression", *args],
            cwd=self._root_path,
            env=_regression_env(self._config),
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="regression",
                context="Running regression guards for policy compliance",
                category=self.category,
                executed_commands=[f"pytest tests/regression {' '.join(args)}".strip()],
            )

        return result

    def integration(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run integration tests."""
        return self._integration(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _integration(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal integration test implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="integration"
        )

        result = self._subprocess_runner.run_pytest_command(
            ["-m", "integration or True", "--no-cov", "tests/integration", *args],
            cwd=self._root_path,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        # Treat "no tests collected" (pytest exit code 5) as a clean pass for optional suites.
        if result.exit_code == 5 and not result.success:
            result.success = True
            result.exit_code = 0
            note = (
                "No integration tests were collected; treating as success "
                "because the suite is optional in this context."
            )
            result.stdout = f"{(result.stdout or '').strip()}\n{note}".strip()
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="integration",
                context="Running integration tests to verify components work together correctly",
                category=self.category,
                executed_commands=[
                    "pytest -m 'integration or True' --no-cov tests/integration"
                    + (f" {' '.join(args)}" if args else "")
                ],
            )

        return result

    def e2e(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run end-to-end tests."""
        return self._e2e(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _e2e(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal e2e test implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="e2e"
        )

        result = self._subprocess_runner.run_pytest_command(
            ["tests/e2e", *args],
            cwd=self._root_path,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="e2e",
                context="Running end-to-end tests to verify complete user workflows",
                category=self.category,
                executed_commands=[f"pytest tests/e2e {' '.join(args)}".strip()],
            )

        return result

    def acceptance(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run acceptance tests."""
        return self._acceptance(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _acceptance(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal acceptance test implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="acceptance"
        )

        result = self._subprocess_runner.run_pytest_command(
            ["tests/acceptance", *args],
            cwd=self._root_path,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="acceptance",
                context="Running acceptance tests to validate business requirements",
                category=self.category,
                executed_commands=[f"pytest tests/acceptance {' '.join(args)}".strip()],
            )

        return result

    def property_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run property-based tests."""
        return self._property_tests(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _property_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal property test implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="property"
        )

        result = self._subprocess_runner.run_pytest_command(
            ["tests/property", *args],
            cwd=self._root_path,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="property",
                context="Running property-based tests to find edge cases with random inputs",
                category=self.category,
                executed_commands=[f"pytest tests/property {' '.join(args)}".strip()],
            )

        return result

    def all_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run all tests."""
        return self._all_tests(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _all_tests(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal all_tests implementation."""
        operation_id = OperationId(
            namespace="tools", category=self.category, command="all"
        )

        result = self._subprocess_runner.run_pytest_command(
            [
                "tests/unit",
                "tests/property",
                "tests/regression",
                "tests/integration",
                "tests/acceptance",
                "tests/e2e",
                *args,
            ],
            cwd=self._root_path,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="all",
                context="Running all test suites for comprehensive verification",
                category=self.category,
                executed_commands=[
                    f"pytest tests/unit tests/property tests/regression tests/integration tests/acceptance tests/e2e {' '.join(args)}".strip()
                ],
            )

        return result

    def coverage_test(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run tests with coverage collection."""
        return self._coverage_test(
            args, learning_mode=learning_mode, verbosity_level=verbosity_level
        )

    def _coverage_test(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Internal coverage test implementation."""
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
        result = self._subprocess_runner.run_uv_command(
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
            cwd=self._root_path,
            env=env,
            timeout=self._config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="coverage-test",
                context="Running tests while measuring code coverage to identify untested code",
                category=self.category,
                executed_commands=[
                    f"coverage run --data-file={coverage_file} -m pytest -n 0 tests/unit tests/property"
                ],
            )

        if (
            result.success
            and coverage_file.exists()
            and coverage_file.stat().st_size > 0
        ):
            fingerprint = self._compute_coverage_fingerprint()
            self._write_coverage_manifest(fingerprint=fingerprint)

        return result

    def coverage_report(
        self,
        args: List[str],
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Generate a coverage report."""
        return self._coverage_report(
            args,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def _coverage_report(
        self,
        args: List[str],
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Internal coverage report implementation."""
        return run_coverage_report(
            config=self._config,
            root_path=self._root_path,
            args=args,
            verbose=verbose,
            subprocess_runner=self._subprocess_runner,
            cache_dir=self._cache_dir,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def coverage_map(
        self,
        args: List[str],
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Generate a coverage map."""
        return self._coverage_map(
            args,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def _coverage_map(
        self,
        args: List[str],
        verbose: bool = False,
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
        force_regen: bool = False,
    ) -> ToolResult:
        """Internal coverage map implementation."""
        return run_coverage_map(
            config=self._config,
            root_path=self._root_path,
            args=args,
            verbose=verbose,
            subprocess_runner=self._subprocess_runner,
            cache_dir=self._cache_dir,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def budget_report(
        self,
        args: List[str],
        *,
        refresh: bool = False,
    ) -> ToolResult:
        """Generate a test budget report."""
        return self._budget_report(args, refresh=refresh)

    def _budget_report(
        self,
        args: List[str],
        *,
        refresh: bool = False,
    ) -> ToolResult:
        """Internal budget report implementation."""
        return run_test_budget_report(
            config=self._config,
            root_path=self._root_path,
            args=args,
            subprocess_runner=self._subprocess_runner,
            refresh=refresh,
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
        """Check coverage against thresholds."""
        return self._coverage_threshold(
            args,
            line_threshold=line_threshold,
            branch_threshold=branch_threshold,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

    def _coverage_threshold(
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
        """Internal coverage threshold implementation."""
        return run_coverage_threshold(
            config=self._config,
            root_path=self._root_path,
            args=args,
            line_threshold=line_threshold,
            branch_threshold=branch_threshold,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            subprocess_runner=self._subprocess_runner,
            cache_dir=self._cache_dir,
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
        """Run the complete coverage pipeline (report + threshold).

        Args:
            args: Additional passthrough arguments (currently ignored).
            line_threshold: Explicit line coverage threshold.
            branch_threshold: Explicit branch coverage threshold.
            verbose: Whether to emit verbose coverage details.
            learning_mode: Whether to populate learning metadata.
            verbosity_level: Learning mode verbosity.
            force_regen: Force regeneration of coverage data even if fingerprint matches.

        Returns:
            ToolResult summarizing combined coverage status.
        """

        report_result = self._coverage_report(
            args,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

        threshold_result = self._coverage_threshold(
            args,
            line_threshold=line_threshold or 0.0,
            branch_threshold=branch_threshold or 0.0,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=False,
        )

        success = report_result.success and threshold_result.success
        exit_code = 0 if success else 1

        stdout_parts = [
            part for part in [report_result.stdout, threshold_result.stdout] if part
        ]
        stderr_parts = [
            part for part in [report_result.stderr, threshold_result.stderr] if part
        ]

        combined_result = ToolResult(
            success=success,
            exit_code=exit_code,
            stdout="\n\n".join(stdout_parts),
            stderr="\n\n".join(stderr_parts),
            operation_id=OperationId(
                namespace="tools", category=self.category, command="coverage"
            ),
        )

        if learning_mode:
            learning = LearningInfo()
            learning.commands_executed.extend(
                report_result.learning_info.commands_executed
            )
            learning.commands_executed.extend(
                threshold_result.learning_info.commands_executed
            )
            learning.explanations.extend(report_result.learning_info.explanations)
            learning.explanations.extend(threshold_result.learning_info.explanations)
            learning.best_practices.extend(report_result.learning_info.best_practices)
            learning.best_practices.extend(
                threshold_result.learning_info.best_practices
            )
            learning.related_concepts.extend(
                report_result.learning_info.related_concepts
            )
            learning.related_concepts.extend(
                threshold_result.learning_info.related_concepts
            )
            combined_result.learning_info = learning

        return combined_result

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
            self._root_path / ".pytest_cache",
            self._root_path / "htmlcov",
            self._cache_dir / "coverage",
            self._cache_dir / "hypothesis",
        ]

        cleaned: list[str] = []
        for path in paths_to_clean:
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
                cleaned.append(str(path.relative_to(self._root_path)))

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
            self._learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self._learning_engine.explain_command(
                command="clean",
                context="Cleaning test artifacts and caches to ensure clean test environment",
                category=self.category,
                executed_commands=[f"Removed {len(cleaned)} artifact paths"],
            )

        return result

    def _cosmic_ray_session_file(self) -> Path:
        """Get the path to the Cosmic Ray session file."""
        return Path(".cache/cosmic-ray/session.sqlite")

    def mutation_summary(self, args: List[str]) -> ToolResult:
        """View a summary of the mutation testing plan."""
        return self._mutation_summary(args)

    def _mutation_summary(self, args: List[str]) -> ToolResult:
        _ = args
        return mutation_module.mutation_summary(
            self._config, self._root_path, self._mutation_service
        )

    def mutation_init(self, args: List[str]) -> ToolResult:
        """Initialize a mutation testing session."""
        return self._mutation_init(args)

    def _mutation_init(self, args: List[str]) -> ToolResult:
        return mutation_module.mutation_init(
            self._config, self._root_path, self._subprocess_runner
        )

    def mutation_exec(self, args: List[str]) -> ToolResult:
        """Execute the mutation testing plan."""
        return self._mutation_exec(args)

    def _mutation_exec(self, args: List[str]) -> ToolResult:
        return mutation_module.mutation_exec(
            self._config, self._root_path, self._subprocess_runner
        )

    def mutation_report(self, args: List[str]) -> ToolResult:
        """Generate a mutation testing report."""
        return self._mutation_report(args)

    def _mutation_report(self, args: List[str]) -> ToolResult:
        _ = args
        return mutation_module.mutation_report(
            self._config, self._root_path, self._mutation_service
        )

    def mutation_reset(self, args: List[str]) -> ToolResult:
        """Reset the mutation testing session."""
        return self._mutation_reset(args)

    def _mutation_reset(self, args: List[str]) -> ToolResult:
        _ = args
        return mutation_module.mutation_reset(self._config, self._root_path)

    def mutation_run(self, args: List[str]) -> ToolResult:
        """Run the full mutation testing pipeline."""
        return self._mutation_run(args)

    def _mutation_run(self, args: List[str]) -> ToolResult:
        """Implementation for full mutation pipeline."""
        steps = [
            ("reset", lambda: self.mutation_reset(args)),
            ("summary", lambda: self.mutation_summary(args)),
            ("init", lambda: self.mutation_init(args)),
            ("exec", lambda: self.mutation_exec(args)),
            ("report", lambda: self.mutation_report(args)),
        ]

        stdout_acc: List[str] = []
        stderr_acc: List[str] = []
        combined_result = ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="test",
            command="mutation-run",
        )

        for step_name, step_fn in steps:
            try:
                step_result = step_fn()
            except Exception as exc:  # defensive catch-all
                combined_result.success = False
                combined_result.exit_code = 1
                stderr_acc.append(f"Mutation {step_name} failed: {exc}")
                break

            if step_result.stdout:
                stdout_acc.append(f"Mutation {step_name} output:\n{step_result.stdout}")
            if step_result.stderr:
                stderr_acc.append(
                    f"Mutation {step_name} warnings:\n{step_result.stderr}"
                )

            if not step_result.success:
                combined_result.success = False
                combined_result.exit_code = step_result.exit_code
                break

        combined_result.stdout = "\n\n".join(stdout_acc)
        combined_result.stderr = "\n\n".join(stderr_acc)
        return combined_result
