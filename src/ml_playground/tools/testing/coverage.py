"""Coverage testing functionality for testing tools."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Mapping, TypedDict, cast

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


def _load_coverage_json(json_path: Path) -> Mapping[str, object]:
    """Load coverage JSON as a mapping, raising ToolExecutionError on failure."""
    from ..core.errors import ToolExecutionError

    try:
        with json_path.open(encoding="utf-8") as handle:
            raw_value_obj: object = cast(object, json.load(handle))
    except (json.JSONDecodeError, OSError, TypeError) as exc:
        raise ToolExecutionError(
            "Failed to parse coverage data",
            reason=f"Invalid coverage JSON: {exc}",
            rationale="Coverage data must be parseable for threshold validation",
        ) from exc

    if not isinstance(raw_value_obj, Mapping):
        raise ToolExecutionError(
            "Failed to parse coverage data",
            reason="Coverage JSON must be an object",
            rationale="Coverage data must be parseable for threshold validation",
        )

    mapping_value = cast(Mapping[str, object], raw_value_obj)
    return dict(mapping_value)


def _ensure_mapping(value: object | None) -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]
    """Normalize JSON values to mappings with string keys."""
    if not isinstance(value, Mapping):
        return {}
    mapping_value = cast(Mapping[str, object], value)
    return dict(mapping_value)


def run_coverage_test(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
) -> ToolResult:
    """Run tests with coverage collection.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional arguments
        subprocess_runner: Subprocess runner
        cache_dir: Cache directory
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="test", command="coverage-test"
    )

    # Clean up existing coverage data
    coverage_dir = cache_dir / "coverage"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = coverage_dir / "coverage.json"
    if coverage_json.exists():
        coverage_json.unlink()

    # Set up coverage environment
    env = _coverage_env(cache_dir)

    # Run coverage with pytest
    result = subprocess_runner.run_uv_command(
        [
            "python",
            "-m",
            "coverage",
            "run",
            "-m",
            "pytest",
            "-n",
            "0",  # Single-threaded: avoids xdist subprocess instrumentation
            "-v",
            "tests/unit",
            "tests/property",
        ],
        cwd=root_path,
        env=env,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    if result.success:
        # Combine parallel/fragment coverage data.
        # --ignore-errors: test fixtures create then delete temp directories;
        # paths recorded during the run may no longer exist at combine time.
        combine_result = subprocess_runner.run_uv_command(
            ["python", "-m", "coverage", "combine", "--quiet"],
            cwd=root_path,
            env=env,
            timeout=config.testing.timeout,
            operation_id=operation_id,
        )
        if not combine_result.success:
            return combine_result

        # Generate JSON report.
        # NOTE: `coverage json` exits with code 2 when total coverage is below
        # `fail_under` in pyproject.toml even though it successfully writes the
        # JSON file.  We must not treat that as a fatal error here — the
        # threshold check is done separately by `coverage-threshold`.  Only
        # bail out if the JSON file was NOT actually written.
        json_result = subprocess_runner.run_uv_command(
            [
                "python",
                "-m",
                "coverage",
                "json",
                "-i",  # --ignore-errors: skip missing temp test-fixture source paths
                "-o",
                str(coverage_json),
            ],
            cwd=root_path,
            env=env,
            timeout=config.testing.timeout,
            operation_id=operation_id,
        )
        if not json_result.success and not (
            coverage_json.exists() and coverage_json.stat().st_size > 0
        ):
            return json_result

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="coverage-test",
            context="Running tests while measuring code coverage to identify untested code",
            category="test",
            executed_commands=[
                "python -m coverage run -m pytest -n auto -v tests/unit tests/property",
                f"python -m coverage json -o {coverage_json}",
            ],
        )

    if result.success and coverage_json.exists() and coverage_json.stat().st_size > 0:
        fingerprint = _compute_coverage_fingerprint(root_path)
        manifest_path = cache_dir / "coverage" / "coverage_manifest.json"
        _write_coverage_manifest(manifest_path, fingerprint=fingerprint)

    return result


def run_coverage_report(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    force_regen: bool = False,
) -> ToolResult:
    """Generate coverage reports.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional arguments
        verbose: Whether to show verbose output
        subprocess_runner: Subprocess runner
        cache_dir: Cache directory
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)
        force_regen: Force regeneration of coverage data

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="test", command="coverage-report"
    )
    from ..core.errors import ToolExecutionError

    executed, notes, _ = _ensure_coverage_data(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        operation_id=operation_id,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        force_regen=force_regen,
    )

    coverage_dir = cache_dir / "coverage"
    json_path = coverage_dir / "coverage.json"
    # Check for empty coverage file in CI
    ci_strict = os.environ.get("CI", "").lower() == "true"
    if ci_strict and (not json_path.exists() or json_path.stat().st_size == 0):
        raise ToolExecutionError(
            "Coverage data file is empty",
            reason="Coverage JSON file is missing or empty",
            rationale="Empty coverage files indicate test execution problems in CI",
        )
    report_messages = ["Generated JSON report"]
    failure_messages: list[str] = []

    # Combine notes and report messages
    all_messages = notes + report_messages
    combined_output = "\n".join(line for line in all_messages if line)

    success = bool(report_messages) and not failure_messages

    if verbose:
        artifacts: list[str] = []
        if coverage_dir.exists():
            for path in sorted(coverage_dir.iterdir()):
                try:
                    artifacts.append(f"  - {path.relative_to(root_path)}")
                except ValueError:
                    artifacts.append(f"  - {path}")
        if artifacts:
            combined_output = "\n".join(
                part
                for part in [
                    combined_output,
                    "",
                    "Coverage artifacts:",
                    *artifacts,
                ]
                if part
            )

    result = ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=combined_output,
        stderr="\n".join(failure_messages),
        operation_id=operation_id,
    )

    # Validate coverage.json has enough data for thresholding/reporting.
    coverage_dir = coverage_dir.resolve()
    if json_path.exists():
        try:
            raw_untyped = cast(
                object, json.loads(json_path.read_text(encoding="utf-8") or "{}")
            )
        except (OSError, ValueError, TypeError) as exc:
            raise ToolExecutionError(
                "Failed to parse coverage JSON",
                reason=str(exc),
                rationale="Coverage report must include parseable JSON data",
            ) from exc
        if not isinstance(raw_untyped, dict):
            raise ToolExecutionError(
                "Failed to parse coverage JSON",
                reason="coverage.json did not parse to a mapping",
                rationale="Coverage report must include data for thresholding and reporting",
            )

        raw = cast(dict[str, object], raw_untyped)
        totals_obj = _extract_totals(raw)
        if totals_obj["num_statements"] <= 0:
            raise ToolExecutionError(
                "Failed to parse coverage JSON",
                reason="coverage.json missing usable line totals",
                rationale="Coverage report must include totals for thresholding and reporting",
            )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="coverage-report",
            context="Generating detailed code coverage reports and analysis",
            category="test",
            executed_commands=executed,
        )

    return result


def run_coverage(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    *,
    line_threshold: float | None = None,
    branch_threshold: float | None = None,
    verbose: bool = False,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    force_regen: bool = False,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
) -> ToolResult:
    """Run the complete coverage pipeline (report + threshold).

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional passthrough arguments
        line_threshold: Explicit line coverage threshold
        branch_threshold: Explicit branch coverage threshold
        verbose: Whether to emit verbose coverage details
        learning_mode: Whether to populate learning metadata
        verbosity_level: Learning mode verbosity
        force_regen: Force regeneration of coverage data
        subprocess_runner: Subprocess runner
        cache_dir: Cache directory

    Returns:
        ToolResult summarizing combined coverage status
    """
    report_result = run_coverage_report(
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

    threshold_result = run_coverage_threshold(
        config=config,
        root_path=root_path,
        args=args,
        line_threshold=line_threshold or 0.0,
        branch_threshold=branch_threshold or 0.0,
        verbose=verbose,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
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
            namespace="tools", category="test", command="coverage"
        ),
    )

    if learning_mode:
        from ..core.interfaces import LearningInfo

        learning = LearningInfo()
        if report_result.learning_info:
            learning.commands_executed.extend(
                report_result.learning_info.commands_executed
            )
            learning.explanations.extend(report_result.learning_info.explanations)
            learning.best_practices.extend(report_result.learning_info.best_practices)
            learning.related_concepts.extend(
                report_result.learning_info.related_concepts
            )
        if threshold_result.learning_info:
            learning.commands_executed.extend(
                threshold_result.learning_info.commands_executed
            )
            learning.explanations.extend(threshold_result.learning_info.explanations)
            learning.best_practices.extend(
                threshold_result.learning_info.best_practices
            )
            learning.related_concepts.extend(
                threshold_result.learning_info.related_concepts
            )
        combined_result.learning_info = learning

    return combined_result


def run_coverage_map(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    force_regen: bool = False,
) -> ToolResult:
    """Generate a coverage map for uncovered files with suite hints."""
    operation_id = OperationId(
        namespace="tools", category="test", command="coverage-map"
    )

    executed, notes, _ = _ensure_coverage_data(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        operation_id=operation_id,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        force_regen=force_regen,
    )

    coverage_dir = cache_dir / "coverage"
    json_path = coverage_dir / "coverage.json"
    if not json_path.exists():
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="\n".join(line for line in notes if line),
            stderr="Coverage JSON file was not created",
            operation_id=operation_id,
        )

    coverage_data = cast(_CoverageJsonData, _load_coverage_json(json_path))
    from .coverage_helpers import collect_undercovered_files, format_coverage_map

    entries = collect_undercovered_files(coverage_data)
    output_lines: list[str] = [*executed, *notes]

    if entries:
        output_lines.append("Coverage map (files below 100% coverage):")
        output_lines.extend(format_coverage_map(entries, root_path))
    else:
        output_lines.append("Coverage map: no files below 100% coverage.")

    result = ToolResult(
        success=True,
        exit_code=0,
        stdout="\n".join(line for line in output_lines if line),
        stderr="",
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="coverage-map",
            context="Summarizing coverage gaps with suggested suites",
            category="test",
            executed_commands=executed,
        )

    return result


def run_coverage_threshold(
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    line_threshold: float = 0.0,
    branch_threshold: float = 0.0,
    verbose: bool = False,
    *,
    learning_mode: bool = False,
    verbosity_level: int = 1,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    force_regen: bool = False,
) -> ToolResult:
    """Check coverage thresholds.

    Args:
        config: Tool configuration
        root_path: Project root path
        args: Additional arguments
        line_threshold: Minimum line coverage percentage
        branch_threshold: Minimum branch coverage percentage
        verbose: Whether to show verbose output
        learning_mode: Whether to enable educational output
        verbosity_level: Level of detail for learning mode (0-2)
        subprocess_runner: Subprocess runner
        cache_dir: Cache directory
        force_regen: Force regeneration of coverage data

    Returns:
        ToolResult with execution details and learning information
    """
    operation_id = OperationId(
        namespace="tools", category="test", command="coverage-threshold"
    )

    executed, notes, _ = _ensure_coverage_data(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        operation_id=operation_id,
        learning_mode=learning_mode,
        verbosity_level=verbosity_level,
        force_regen=force_regen,
    )

    # Read thresholds from config if not explicitly provided
    if line_threshold == 0.0 or branch_threshold == 0.0:
        config_thresholds = _read_coverage_thresholds_from_config(root_path)
        if line_threshold == 0.0:
            line_threshold = config_thresholds.get("line_threshold", 0.0)
        if branch_threshold == 0.0:
            branch_threshold = config_thresholds.get("branch_threshold", 0.0)

    json_path = cache_dir / "coverage" / "coverage.json"
    if not json_path.exists():
        from ..core.errors import ToolExecutionError

        raise ToolExecutionError(
            "Coverage data file not found",
            reason=f"Missing coverage file: {json_path}",
            rationale="Coverage threshold checks require prior execution of coverage-test command",
        )

    if not json_path.exists():
        from ..core.errors import ToolExecutionError

        raise ToolExecutionError(
            "Failed to generate coverage JSON report",
            reason="Coverage JSON file was not created",
            rationale="Coverage threshold checks require JSON report generation",
        )

    coverage_data = cast(_CoverageJsonData, _load_coverage_json(json_path))
    normalized_totals = _extract_totals(coverage_data)

    # Extract metrics
    num_branches = normalized_totals["num_branches"]
    covered_branches = normalized_totals["covered_branches"]
    covered_lines = normalized_totals["covered_lines"]
    num_statements = normalized_totals["num_statements"]

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
                _format_coverage_status(
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
                _format_coverage_status(
                    metric="Branch",
                    percentage=branch_pct,
                    threshold=branch_threshold,
                    passed=branch_pass,
                )
            )

    info_lines: list[str] = [*executed]
    if notes:
        info_lines.extend(notes)

    all_passed = line_pass and branch_pass
    from .coverage_helpers import collect_undercovered_files, format_undercovered_tree

    # Totals summary for humans/tests (kept in stdout so it appears in combined coverage output).
    info_lines.append("Coverage totals:")
    info_lines.append(
        f"  Lines: {int(covered_lines)}/{int(num_statements)} ({line_pct:.2f}%), LOC={int(num_statements)}"
        if num_statements
        else "  Lines: <missing>"
    )
    if num_branches:
        info_lines.append(
            f"  Branches: {int(covered_branches)}/{int(num_branches)} ({branch_pct:.2f}%)"
        )
    else:
        info_lines.append("  Branches: <missing>")

    coverage_files: list[tuple[str, float, float | None, int]] = []
    if coverage_data:
        coverage_files = collect_undercovered_files(coverage_data)
    # Always report under-covered files if any exist, regardless of threshold pass/fail
    if coverage_files:
        info_lines.append("")
        info_lines.append("Files below 100% coverage:")
        tree_lines = format_undercovered_tree(coverage_files)
        info_lines.extend(tree_lines)
        info_lines.append("")

    output = "\n".join(line for line in info_lines if line)
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

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        threshold_result.learning_info = learning_engine.explain_command(
            command="coverage-threshold",
            context="Checking coverage thresholds to enforce quality standards",
            category="test",
            executed_commands=[
                "python -m slipcover --json --out .cache/coverage/coverage.json"
            ],
        )

    return threshold_result


# Helper functions (extracted from original TestingTools methods)


def _clean_pytest_output(output: str) -> str:
    """Remove pytest progress lines and xdist status messages."""
    lines = output.splitlines()
    cleaned_lines: list[str] = []

    for line in lines:
        # Skip progress indicators and xdist status
        if any(
            skip in line
            for skip in [
                "test session starts",
                "[gw",
                "workers [",
                "scheduling",
                ".",
                "=",
                "PASSED",
                "FAILED",
                "ERROR",
                "warnings summary",
                "short test summary",
            ]
        ):
            continue
        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


# Duplicate function definition removed - using the one defined earlier


def _coverage_env(cache_dir: Path) -> dict[str, str]:
    """Get environment variables for coverage execution."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "coverage").mkdir(parents=True, exist_ok=True)
    (cache_dir / "hypothesis").mkdir(parents=True, exist_ok=True)

    return {
        "HYPOTHESIS_DATABASE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_STORAGE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_SEED": "0",
        "PYTHONHASHSEED": "0",
    }


def _compute_coverage_fingerprint(root_path: Path) -> str:
    """Compute a fingerprint representing the current coverage-relevant sources."""
    from .coverage_helpers import compute_coverage_fingerprint

    return compute_coverage_fingerprint(root_path)


def _read_coverage_manifest(manifest_path: Path) -> dict[str, str] | None:
    """Load the stored coverage fingerprint manifest if it exists."""
    from .coverage_helpers import read_coverage_manifest

    return read_coverage_manifest(manifest_path)


def _write_coverage_manifest(manifest_path: Path, *, fingerprint: str) -> None:
    """Persist the current coverage fingerprint."""
    from .coverage_helpers import write_coverage_manifest

    write_coverage_manifest(manifest_path, fingerprint=fingerprint)


def _format_coverage_status(
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


def _format_command(command: list[str]) -> str:
    """Format a command for display."""
    from .coverage_helpers import format_command

    return "Executed: " + format_command(command)


class _CoverageTotals(TypedDict):
    num_branches: float
    covered_branches: float
    missing_branches: float
    covered_lines: float
    missing_lines: float
    num_statements: float


def _to_float(value: object | None) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _extract_totals(coverage_data: Mapping[str, object]) -> _CoverageTotals:
    files_section = coverage_data.get("files")
    if isinstance(files_section, Mapping):
        covered_lines_from_files = 0.0
        missing_lines_from_files = 0.0
        covered_branches_from_files = 0.0
        missing_branches_from_files = 0.0
        for raw_info in files_section.values():
            if not isinstance(raw_info, Mapping):
                continue
            info = cast(Mapping[str, object], raw_info)
            summary = info.get("summary")
            if not isinstance(summary, Mapping):
                continue
            summary_map = cast(Mapping[str, object], summary)
            covered_lines_from_files += _to_float(summary_map.get("covered_lines"))
            missing_lines_from_files += _to_float(summary_map.get("missing_lines"))
            covered_branches_from_files += _to_float(
                summary_map.get("covered_branches")
            )
            missing_branches_from_files += _to_float(
                summary_map.get("missing_branches")
            )

        num_statements_from_files = covered_lines_from_files + missing_lines_from_files
        num_branches_from_files = (
            covered_branches_from_files + missing_branches_from_files
        )
        if num_statements_from_files > 0:
            return {
                "num_branches": num_branches_from_files,
                "covered_branches": covered_branches_from_files,
                "missing_branches": missing_branches_from_files,
                "covered_lines": covered_lines_from_files,
                "missing_lines": missing_lines_from_files,
                "num_statements": num_statements_from_files,
            }

    totals_section = coverage_data.get("totals")
    if not isinstance(totals_section, Mapping):
        totals_section = coverage_data.get("summary")
    normalized_totals: Mapping[str, object]
    if isinstance(totals_section, Mapping):
        normalized_totals = cast(Mapping[str, object], totals_section)
    else:
        normalized_totals = {}

    covered_lines = _to_float(normalized_totals.get("covered_lines"))
    missing_lines = _to_float(normalized_totals.get("missing_lines"))
    num_statements = _to_float(normalized_totals.get("num_statements"))
    if num_statements <= 0:
        num_statements = covered_lines + missing_lines

    covered_branches = _to_float(normalized_totals.get("covered_branches"))
    missing_branches = _to_float(normalized_totals.get("missing_branches"))
    num_branches = _to_float(normalized_totals.get("num_branches"))
    if num_branches <= 0:
        num_branches = covered_branches + missing_branches

    return {
        "num_branches": num_branches,
        "covered_branches": covered_branches,
        "missing_branches": missing_branches,
        "covered_lines": covered_lines,
        "missing_lines": missing_lines,
        "num_statements": num_statements,
    }


class _CoverageJsonData(TypedDict, total=False):
    totals: _CoverageTotals
    files: Mapping[str, object]


def _ensure_coverage_data(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    operation_id: OperationId,
    learning_mode: bool,
    verbosity_level: int,
    force_regen: bool,
) -> tuple[list[str], list[str], dict[str, str]]:
    """Ensure coverage data is available and up-to-date."""
    executed: list[str] = []
    notes: list[str] = []
    coverage_json = cache_dir / "coverage" / "coverage.json"
    env: dict[str, str] = _coverage_env(cache_dir)

    current_fingerprint = _compute_coverage_fingerprint(root_path)
    manifest_path = cache_dir / "coverage" / "coverage_manifest.json"
    manifest = _read_coverage_manifest(manifest_path)
    manifest_fingerprint = manifest.get("fingerprint") if manifest else None

    if (
        not force_regen
        and manifest_fingerprint == current_fingerprint
        and coverage_json.exists()
        and coverage_json.stat().st_size > 0
    ):
        return executed, notes, env

    generation_result, generation_notes = _run_coverage_test_for_data(
        config=config,
        root_path=root_path,
        args=args,
        verbose=verbose,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        operation_id=operation_id,
        executed_commands=executed,
    )
    if isinstance(generation_result, ToolResult):
        from ..core.errors import ToolExecutionError

        stderr = (generation_result.stderr or "").strip()
        stdout = (generation_result.stdout or "").strip()
        raise ToolExecutionError(
            "Coverage data generation failed",
            reason=stderr or stdout or "Unknown error during coverage generation",
            rationale="Coverage data must be generated for threshold analysis",
        )
    notes.extend(generation_notes)

    if coverage_json.exists() and coverage_json.stat().st_size > 0:
        _write_coverage_manifest(manifest_path, fingerprint=current_fingerprint)
        return executed, notes, env

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
    from ..core.errors import ToolExecutionError

    raise ToolExecutionError(
        "Coverage data generation failed",
        reason=failure.stderr,
        rationale="Coverage data must be available for threshold analysis",
    )


def _combine_coverage_fragments(
    *,
    env: dict[str, str],
    subprocess_runner: SubprocessRunner,
    root_path: Path,
    cache_dir: Path,
    operation_id: OperationId,
    executed_commands: list[str] | None = None,
) -> tuple[ToolResult | None, bool]:
    """SlipCover writes a single JSON file, so fragment combination is unnecessary."""
    return None, False


def _run_coverage_test_for_data(
    *,
    config: ToolsConfig,
    root_path: Path,
    args: List[str],
    verbose: bool,
    subprocess_runner: SubprocessRunner,
    cache_dir: Path,
    operation_id: OperationId,
    executed_commands: list[str] | None = None,
) -> tuple[ToolResult | None, list[str]]:
    """Generate coverage data by running tests."""
    if executed_commands is None:
        executed_commands = []

    # Record that we invoked the coverage tool to generate data
    from .coverage_helpers import format_tool_invocation

    coverage_tool_cmd = format_tool_invocation("coverage", args)
    if coverage_tool_cmd not in executed_commands:
        executed_commands.append(coverage_tool_cmd)

    coverage_result = run_coverage_test(
        config=config,
        root_path=root_path,
        args=args,
        subprocess_runner=subprocess_runner,
        cache_dir=cache_dir,
        learning_mode=False,
        verbosity_level=1,
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
    coverage_json = cache_dir / "coverage" / "coverage.json"
    if not coverage_json.exists() or coverage_json.stat().st_size == 0:
        notes.append(
            "Coverage pipeline generated no data. Check `tools test coverage` output."
        )

    return None, notes


def _read_coverage_thresholds_from_config(root_path: Path) -> dict[str, float]:
    """Read coverage thresholds from pyproject.toml."""
    import tomllib
    from typing import cast

    pyproject_path = root_path / "pyproject.toml"
    if not pyproject_path.exists():
        return {}

    try:
        with pyproject_path.open("rb") as f:
            raw_config: Mapping[str, object] = cast(
                Mapping[str, object], tomllib.load(f)
            )

        def _as_dict(obj: Mapping[str, object], key: str) -> dict[str, object]:
            value = obj.get(key, {})
            return (
                dict(cast(Mapping[str, object], value))
                if isinstance(value, Mapping)
                else {}
            )

        config_data = raw_config
        tool_cfg = _as_dict(config_data, "tool")
        ml_cfg = _as_dict(tool_cfg, "ml_playground")
        coverage_cfg = _as_dict(ml_cfg, "coverage")
        thresholds_cfg = _as_dict(coverage_cfg, "thresholds")

        line_threshold = thresholds_cfg.get("line_threshold", 0.0)
        branch_threshold = thresholds_cfg.get("branch_threshold", 0.0)

        return {
            "line_threshold": float(line_threshold)
            if isinstance(line_threshold, (int, float))
            else 0.0,
            "branch_threshold": float(branch_threshold)
            if isinstance(branch_threshold, (int, float))
            else 0.0,
        }
    except (tomllib.TOMLDecodeError, KeyError, TypeError, ValueError) as exc:
        # If we can't read the config, return empty dict (no thresholds)
        # Log the specific error for debugging
        import sys

        print(
            f"[coverage] Warning: Could not read coverage thresholds from pyproject.toml: {exc}",
            file=sys.stderr,
        )
        return {}
