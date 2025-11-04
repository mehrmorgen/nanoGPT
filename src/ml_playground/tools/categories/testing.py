"""Testing tools category implementation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict, List, Optional, cast, Literal

import tomllib

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import LearningInfo, OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


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

        include_paths = [
            self.root_path / "src" / "ml_playground" / "tools",
            self.root_path / "tests",
        ]

        parts: list[str] = []
        for base_path in include_paths:
            if not base_path.exists():
                continue
            for file_path in sorted(base_path.rglob("*.py")):
                try:
                    stat_result = file_path.stat()
                    hasher = hashlib.sha256()
                    with file_path.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(8192), b""):
                            hasher.update(chunk)
                except OSError:
                    continue
                relative = file_path.relative_to(self.root_path).as_posix()
                parts.append(
                    f"{relative}:{stat_result.st_size}:{stat_result.st_mtime_ns}:{hasher.hexdigest()}"
                )

        payload = "\n".join(parts)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _read_coverage_manifest(self) -> dict[str, Any] | None:
        """Load the stored coverage fingerprint manifest if it exists."""

        manifest_path = self._coverage_manifest_path()
        if not manifest_path.exists():
            return None
        try:
            with manifest_path.open(encoding="utf-8") as manifest_file:
                return json.load(manifest_file)
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
        files = coverage_data.get("files", {})
        undercovered: list[tuple[str, float, float | None]] = []
        for path, info in files.items():
            summary = info.get("summary", {})
            percent = summary.get("percent_covered")
            if percent is None:
                display = summary.get("percent_covered_display")
                if isinstance(display, str):
                    try:
                        percent = float(display)
                    except ValueError:
                        continue
            if percent is None:
                continue
            percent_float = float(percent)
            branch_percent: float | None = None
            num_branches = summary.get("num_branches")
            covered_branches = summary.get("covered_branches")
            if isinstance(num_branches, (int, float)) and num_branches:
                try:
                    branch_percent = float(covered_branches) / float(num_branches) * 100
                except (TypeError, ZeroDivisionError):
                    branch_percent = None
            if percent_float < 100.0:
                undercovered.append((path, percent_float, branch_percent))
        undercovered.sort(key=lambda item: (item[1], item[0]))
        return undercovered

    def _format_undercovered_tree(
        self, entries: list[tuple[str, float, float | None]]
    ) -> list[str]:
        root: dict[str, Any] = {}

        for path, line_pct, branch_pct in entries:
            parts = Path(path).parts
            node = root
            for part in parts[:-1]:
                node = node.setdefault(part, {})
            node.setdefault("__files__", []).append((parts[-1], line_pct, branch_pct))

        def render(node: dict[str, Any], prefix: str) -> list[str]:
            lines: list[str] = []
            dir_names = sorted(name for name in node.keys() if name != "__files__")
            files = sorted(node.get("__files__", []), key=lambda item: item[0])

            # Combine directories and files to determine connector usage
            combined: list[tuple[str, Literal["dir", "file"], Any]] = [
                (name, "dir", node[name]) for name in dir_names
            ]
            combined.extend((file_info[0], "file", file_info) for file_info in files)

            for idx, (name, kind, payload) in enumerate(combined):
                is_last = idx == len(combined) - 1
                connector = "└── " if is_last else "├── "
                if kind == "dir":
                    lines.append(f"{prefix}{connector}{name}/")
                    child_prefix = prefix + ("    " if is_last else "│   ")
                    lines.extend(render(payload, child_prefix))
                else:
                    _, line_pct, branch_pct = payload
                    branch_text = (
                        f" branch = {branch_pct:.2f}%" if branch_pct is not None else ""
                    )
                    lines.append(
                        f"{prefix}{connector}{name}: line = {line_pct:.2f}%{branch_text}"
                    )

            return lines

        return render(root, "")

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
                "Coverage data not produced automatically. Run `tools test coverage-test` manually "
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
        coverage_tool_cmd = self._format_tool_invocation("coverage-test", args)
        if coverage_tool_cmd not in executed_commands:
            executed_commands.append(coverage_tool_cmd)
        coverage_result = self.coverage_test(
            args,
            learning_mode=False,
            verbosity_level=verbosity_level,
        )
        if not coverage_result.success:
            return coverage_result, []

        message = "Automatically ran coverage-test to generate coverage data."
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

        message = "Coverage-test generated no data; reran pytest to create coverage artifacts."
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
                config = cast(Mapping[str, Any], tomllib.load(f))

            tool_cfg = config.get("tool", {})
            if not isinstance(tool_cfg, Mapping):
                return {}

            ml_cfg = tool_cfg.get("ml_playground", {})
            if not isinstance(ml_cfg, Mapping):
                return {}

            coverage_cfg = ml_cfg.get("coverage", {})
            if not isinstance(coverage_cfg, Mapping):
                return {}

            thresholds = coverage_cfg.get("thresholds", {})
            if not isinstance(thresholds, Mapping):
                return {}
            return {
                "line_threshold": float(thresholds.get("line_threshold", 0.0)),
                "branch_threshold": float(thresholds.get("branch_threshold", 0.0)),
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
        result = self._clean_pytest_result(result)

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="unit",
                context="Running unit tests to verify individual components work correctly",
                category=self.category,
                executed_commands=[f"pytest tests/unit {' '.join(args)}".strip()],
            )

        return result

    def regression(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Run regression policy suites."""

        operation_id = OperationId(
            namespace="tools", category=self.category, command="regression"
        )

        result = self.subprocess_runner.run_pytest_command(
            ["tests/regression", *args],
            cwd=self.root_path,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="regression",
                context="Running regression guards for policy compliance",
                category=self.category,
                executed_commands=[f"pytest tests/regression {' '.join(args)}".strip()],
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
        result = self._clean_pytest_result(result)

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
        result = self._clean_pytest_result(result)

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
        result = self._clean_pytest_result(result)

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
        result = self._clean_pytest_result(result)

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
                "-v",
                "tests/unit",
                "tests/property",
            ],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )
        result = self._clean_pytest_result(result)

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
        """Generate coverage reports.

        Args:
            args: Additional arguments (ignored)
            verbose: Whether to show verbose output
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-report"
        )

        executed: list[str] = []
        auto_result, notes, env = self._ensure_coverage_data(
            args=args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            verbose=verbose,
            operation_id=operation_id,
            executed_commands=executed,
            force_regen=force_regen,
        )
        if isinstance(auto_result, ToolResult):
            return auto_result

        coverage_file = self._coverage_file()
        # Check for empty coverage file in CI
        ci_strict = os.environ.get("CI", "").lower() == "true"
        if ci_strict and coverage_file.stat().st_size == 0:
            raise ToolExecutionError(
                "Coverage data file is empty",
                reason="Coverage file exists but contains no data",
                rationale="Empty coverage files indicate test execution problems in CI",
            )

        coverage_dir = coverage_file.parent

        # Generate multiple report formats
        commands = [
            (
                ["coverage", "report", "-m"],
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

        report_messages: list[str] = []
        first_failure: ToolResult | None = None

        for command, description in commands:
            regenerated = False
            while True:
                try:
                    result = self.subprocess_runner.run_uv_command(
                        command,
                        cwd=self.root_path,
                        env=env,
                        timeout=self.config.testing.timeout,
                        operation_id=operation_id,
                    )
                except Exception as exc:  # pragma: no cover - defensive guard
                    raise ToolExecutionError(
                        f"Failed to generate {description}",
                        reason=str(exc),
                        rationale="Coverage report generation must succeed for quality gates",
                    ) from exc

                if result.success:
                    suffix = " after refreshing coverage data" if regenerated else ""
                    report_messages.append(f"Generated {description}{suffix}")
                    run_command = self._format_command(command)
                    if run_command not in executed:
                        executed.append(run_command)
                    break

                error_detail = (
                    result.stderr.strip() or result.stdout.strip() or "command failed"
                )

                if not regenerated and "No source for code" in error_detail:
                    regen_result, regen_notes = self._run_coverage_test_for_data(
                        args=args,
                        verbosity_level=verbosity_level,
                        verbose=verbose,
                        operation_id=operation_id,
                        executed_commands=executed,
                    )
                    notes.extend(regen_notes)
                    if isinstance(regen_result, ToolResult):
                        if first_failure is None:
                            first_failure = regen_result
                        report_messages.append(
                            f"[FAILED] {description}: {error_detail}"
                        )
                        break
                    regenerated = True
                    env = self._coverage_env(self._coverage_file())
                    continue

                if first_failure is None:
                    first_failure = result
                report_messages.append(f"[FAILED] {description}: {error_detail}")
                break

        metrics_failure, metrics_lines = self._collect_coverage_metrics(
            env, operation_id, executed_commands=executed
        )
        if isinstance(metrics_failure, ToolResult):
            if first_failure is None:
                first_failure = metrics_failure
        else:
            report_messages.extend(metrics_lines)

        # Show artifacts if verbose
        coverage_json_cmd = self._format_command(
            ["coverage", "json", "-o", str(coverage_dir / "coverage.json")]
        )
        if coverage_json_cmd in executed:
            remaining_commands = [cmd for cmd in executed if cmd != coverage_json_cmd]
            command_lines = [coverage_json_cmd, *remaining_commands]
        else:
            command_lines = [*executed]
        output_lines = [*command_lines, *notes, *report_messages]
        output = "\n".join(line for line in output_lines if line)
        if verbose:
            artifacts = []
            for path in sorted(coverage_dir.iterdir()):
                artifacts.append(f"  - {path.relative_to(self.root_path)}")
            if artifacts:
                output += "\n\nCoverage artifacts:\n" + "\n".join(artifacts)

        if first_failure is not None:
            result = ToolResult(
                success=False,
                exit_code=first_failure.exit_code,
                stdout=output,
                stderr=first_failure.stderr,
                operation_id=operation_id,
            )
        else:
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
        force_regen: bool = False,
    ) -> ToolResult:
        """Check coverage thresholds.

        Args:
            args: Additional arguments (ignored)
            line_threshold: Minimum line coverage percentage (0.0 = read from config)
            branch_threshold: Minimum branch coverage percentage (0.0 = read from config)
            verbose: Whether to show verbose output
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-threshold"
        )

        executed: list[str] = []
        auto_result, notes, env = self._ensure_coverage_data(
            args=args,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            verbose=verbose,
            operation_id=operation_id,
            executed_commands=executed,
            force_regen=force_regen,
        )
        if isinstance(auto_result, ToolResult):
            return auto_result

        # Read thresholds from config if not explicitly provided
        if line_threshold == 0.0 or branch_threshold == 0.0:
            config_thresholds = self._read_coverage_thresholds_from_config()
            if line_threshold == 0.0:
                line_threshold = config_thresholds.get("line_threshold", 0.0)
            if branch_threshold == 0.0:
                branch_threshold = config_thresholds.get("branch_threshold", 0.0)

        coverage_file = self._coverage_file()
        if not coverage_file.exists():
            raise ToolExecutionError(
                "Coverage data file not found",
                reason=f"Missing coverage file: {coverage_file}",
                rationale="Coverage threshold checks require prior execution of coverage-test command",
            )

        json_path = coverage_file.parent / "coverage.json"

        coverage_json_cmd = self._format_command(
            ["coverage", "json", "-o", str(json_path)]
        )
        if coverage_json_cmd not in executed:
            executed.append(coverage_json_cmd)
        json_result = self.subprocess_runner.run_uv_command(
            ["coverage", "json", "-o", str(json_path)],
            cwd=self.root_path,
            env=env,
            timeout=self.config.testing.timeout,
            operation_id=operation_id,
        )

        if not json_path.exists():
            raise ToolExecutionError(
                "Failed to generate coverage JSON report",
                reason="Coverage JSON file was not created",
                rationale="Coverage threshold checks require JSON report generation",
            )

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

        # Build status lines for thresholds
        line_data_available = num_statements > 0
        branch_data_available = num_branches > 0

        line_pass = line_threshold <= 0
        branch_pass = branch_threshold <= 0
        status_lines: list[str] = []

        if line_threshold > 0:
            if not line_data_available:
                line_pass = False
                status_lines.append(
                    "[coverage] ❌ FAILURE: Line coverage totals missing from coverage data."
                )
            else:
                line_pass = line_pct >= line_threshold
                status_lines.append(
                    self._format_coverage_status(
                        metric="Line",
                        percentage=line_pct,
                        threshold=line_threshold,
                        passed=line_pass,
                    )
                )

        if branch_threshold > 0:
            if not branch_data_available:
                branch_pass = False
                status_lines.append(
                    "[coverage] ❌ FAILURE: Branch coverage data missing from coverage data."
                )
            else:
                branch_pass = branch_pct >= branch_threshold
                status_lines.append(
                    self._format_coverage_status(
                        metric="Branch",
                        percentage=branch_pct,
                        threshold=branch_threshold,
                        passed=branch_pass,
                    )
                )

        # Ensure coverage json command appears first
        remaining_commands = [cmd for cmd in executed if cmd != coverage_json_cmd]
        info_lines: list[str] = [coverage_json_cmd, *remaining_commands]
        if notes:
            info_lines.extend(notes)

        all_passed = line_pass and branch_pass
        coverage_files = self._collect_undercovered_files(coverage_data)
        if not all_passed and coverage_files:
            info_lines.append("")
            info_lines.append("Files below 100% coverage:")
            tree_lines = self._format_undercovered_tree(coverage_files)
            info_lines.extend(tree_lines)
            info_lines.append("")

        output = "\n".join(line for line in info_lines if line is not None)
        status_output = "\n".join(status_lines)

        if all_passed:
            threshold_result = ToolResult(
                success=True,
                exit_code=0,
                stdout=output,
                stderr=status_output,
                operation_id=operation_id,
            )
        else:
            threshold_result = ToolResult(
                success=False,
                exit_code=1,
                stdout=output,
                stderr=status_output,
                operation_id=operation_id,
            )

        if not json_result.success and json_result.stderr:
            combined_stderr = "\n".join(
                part
                for part in [
                    threshold_result.stderr,
                    f"[coverage-json] {json_result.stderr.strip()}",
                ]
                if part
            )
            threshold_result.stderr = combined_stderr

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            threshold_result.learning_info = self.learning_engine.explain_command(
                command="coverage-threshold",
                context="Checking coverage thresholds to enforce quality standards",
                category=self.category,
                executed_commands=["coverage json"],
            )

        return threshold_result

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

        report_result = self.coverage_report(
            args,
            verbose=verbose,
            learning_mode=learning_mode,
            verbosity_level=verbosity_level,
            force_regen=force_regen,
        )

        threshold_result = self.coverage_threshold(
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
            cfg = cast(Mapping[str, Any], load_config(str(config_file)))

            # Extract configuration details
            session_cfg = cfg.get("session", {})
            session_mapping = (
                cast(Mapping[str, Any], session_cfg)
                if isinstance(session_cfg, Mapping)
                else {}
            )
            session_path = Path(
                session_mapping.get("path", ".cache/cosmic-ray/session.sqlite")
            )

            test_runner_cfg = cfg.get("test-runner", {})
            test_runner_mapping = (
                cast(Mapping[str, Any], test_runner_cfg)
                if isinstance(test_runner_cfg, Mapping)
                else {}
            )
            test_command = test_runner_mapping.get("command", "pytest")

            # Find modules to mutate
            modules_cfg = cfg.get("modules", {})
            modules_input: Any = (
                modules_cfg if isinstance(modules_cfg, Mapping) else modules_cfg
            )
            modules = tuple(find_modules(cast(Any, modules_input)))

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
        result = self.subprocess_runner.run_uv_command(
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

        return self.subprocess_runner.run_uv_command(
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
            cfg = cast(Mapping[str, Any], load_config(str(config_file)))
            session_cfg = cfg.get("session", {})
            session_mapping = (
                cast(Mapping[str, Any], session_cfg)
                if isinstance(session_cfg, Mapping)
                else {}
            )
            session_path = Path(
                session_mapping.get("path", ".cache/cosmic-ray/session.sqlite")
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
                conn.row_factory = lambda _cursor, row: row[0]
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
