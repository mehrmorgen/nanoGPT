"""Coverage testing functionality for testing tools."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import List

from ..core.config import ToolsConfig
from ..core.interfaces import OperationId, ToolResult
from ..core.learning_mode import LearningModeEngine, VerbosityLevel
from ..utils.subprocess_utils import SubprocessRunner


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
    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    if coverage_file.exists():
        coverage_file.unlink()

    # Remove any coverage fragments
    for fragment in coverage_file.parent.glob("coverage.sqlite.*"):
        if fragment.name != coverage_file.name:
            fragment.unlink()

    # Set up coverage environment
    env = _coverage_env(coverage_file, cache_dir)

    # Run coverage with pytest
    result = subprocess_runner.run_uv_command(
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
        cwd=root_path,
        env=env,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    if learning_mode:
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        result.learning_info = learning_engine.explain_command(
            command="coverage-test",
            context="Running tests while measuring code coverage to identify untested code",
            category="test",
            executed_commands=[
                f"coverage run --data-file={coverage_file} -m pytest -n 0 tests/unit tests/property"
            ],
        )

    if result.success and coverage_file.exists() and coverage_file.stat().st_size > 0:
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

    executed, notes, env = _ensure_coverage_data(
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

    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    # Check for empty coverage file in CI
    ci_strict = os.environ.get("CI", "").lower() == "true"
    if ci_strict and coverage_file.stat().st_size == 0:
        from ..core.errors import ToolExecutionError

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
    failure_messages: list[str] = []
    first_failure: ToolResult | None = None

    for command, description in commands:
        regenerated = False
        while True:
            try:
                result = subprocess_runner.run_uv_command(
                    command,
                    cwd=root_path,
                    env=env,
                    timeout=config.testing.timeout,
                    operation_id=operation_id,
                )
            except (
                ToolExecutionError,
                TimeoutError,
                OSError,
                subprocess.SubprocessError,
                RuntimeError,
            ) as exc:
                raise ToolExecutionError(
                    f"Failed to generate {description}",
                    reason=f"Subprocess error: {exc}",
                    rationale="Coverage report generation must succeed for quality gates",
                ) from exc

            if result.success:
                suffix = " after refreshing coverage data" if regenerated else ""
                report_messages.append(f"Generated {description}{suffix}")
                run_command = _format_command(command)
                if run_command not in executed:
                    executed.append(run_command)
                break

            error_detail = (
                result.stderr.strip() or result.stdout.strip() or "command failed"
            )

            if not regenerated and "No source for code" in error_detail:
                regen_result, regen_notes = _run_coverage_test_for_data(
                    config=config,
                    root_path=root_path,
                    args=args,
                    verbose=verbose,
                    subprocess_runner=subprocess_runner,
                    cache_dir=cache_dir,
                    operation_id=operation_id,
                    executed_commands=executed,
                )
                notes.extend(regen_notes)
                if isinstance(regen_result, ToolResult):
                    return regen_result

            if not regenerated:
                regenerated = True
            else:
                if first_failure is None:
                    first_failure = result
                failure_messages.append(
                    f"Failed to generate {description}: {error_detail}"
                )
                break

    if first_failure and not report_messages:
        return ToolResult(
            success=False,
            exit_code=1,
            stdout="\n".join(line for line in notes if line),
            stderr="\n".join(failure_messages) or (first_failure.stderr or ""),
            operation_id=operation_id,
        )

    # Combine notes and report messages
    all_messages = notes + report_messages
    combined_output = "\n".join(line for line in all_messages if line)

    success = bool(report_messages) and not failure_messages

    result = ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=combined_output,
        stderr="\n".join(failure_messages),
        operation_id=operation_id,
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

    executed, notes, env = _ensure_coverage_data(
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

    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    if not coverage_file.exists():
        from ..core.errors import ToolExecutionError

        raise ToolExecutionError(
            "Coverage data file not found",
            reason=f"Missing coverage file: {coverage_file}",
            rationale="Coverage threshold checks require prior execution of coverage-test command",
        )

    json_path = coverage_file.parent / "coverage.json"
    json_cmd = ["coverage", "json", "-o", str(json_path)]
    formatted_json_cmd = _format_command(json_cmd)
    if formatted_json_cmd not in executed:
        executed.append(formatted_json_cmd)
    json_result = subprocess_runner.run_uv_command(
        json_cmd,
        cwd=root_path,
        env=env,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    if not json_path.exists():
        from ..core.errors import ToolExecutionError

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
        from ..core.errors import ToolExecutionError

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

    # Ensure coverage json command appears first
    remaining_commands = [cmd for cmd in executed if cmd != formatted_json_cmd]
    info_lines: list[str] = [formatted_json_cmd, *remaining_commands]
    if notes:
        info_lines.extend(notes)

    all_passed = line_pass and branch_pass
    from .coverage_helpers import collect_undercovered_files, format_undercovered_tree

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
        learning_engine = LearningModeEngine()
        learning_engine.verbosity = VerbosityLevel(verbosity_level)
        threshold_result.learning_info = learning_engine.explain_command(
            command="coverage-threshold",
            context="Checking coverage thresholds to enforce quality standards",
            category="test",
            executed_commands=["coverage json"],
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


def _coverage_env(coverage_file: Path, cache_dir: Path) -> dict[str, str]:
    """Get environment variables for coverage execution."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "coverage").mkdir(parents=True, exist_ok=True)
    (cache_dir / "hypothesis").mkdir(parents=True, exist_ok=True)

    return {
        "HYPOTHESIS_DATABASE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_STORAGE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_SEED": "0",
        "PYTHONHASHSEED": "0",
        "COVERAGE_FILE": str(coverage_file),
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

    return format_command(command)


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
    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    env = _coverage_env(coverage_file, cache_dir)

    current_fingerprint = _compute_coverage_fingerprint(root_path)
    manifest_path = cache_dir / "coverage" / "coverage_manifest.json"
    manifest = _read_coverage_manifest(manifest_path)
    manifest_fingerprint = manifest.get("fingerprint") if manifest else None

    if (
        not force_regen
        and manifest_fingerprint == current_fingerprint
        and coverage_file.exists()
        and coverage_file.stat().st_size > 0
    ):
        return executed, notes, env

    combine_result, combined = _combine_coverage_fragments(
        env=env,
        subprocess_runner=subprocess_runner,
        root_path=root_path,
        cache_dir=cache_dir,
        operation_id=operation_id,
        executed_commands=executed,
    )
    if isinstance(combine_result, ToolResult):
        from ..core.errors import ToolExecutionError

        stderr = (combine_result.stderr or "").strip()
        stdout = (combine_result.stdout or "").strip()
        raise ToolExecutionError(
            "Coverage fragment combination failed",
            reason=stderr or stdout or "Unknown error during coverage combination",
            rationale="Coverage data must be properly combined for threshold analysis",
        )

    if (
        not force_regen
        and manifest_fingerprint == current_fingerprint
        and combined
        and coverage_file.exists()
        and coverage_file.stat().st_size > 0
    ):
        notes.append("Combined existing coverage fragments into coverage.sqlite.")
        _write_coverage_manifest(manifest_path, fingerprint=current_fingerprint)
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

    combine_result, combined = _combine_coverage_fragments(
        env=env,
        subprocess_runner=subprocess_runner,
        root_path=root_path,
        cache_dir=cache_dir,
        operation_id=operation_id,
        executed_commands=executed,
    )
    if isinstance(combine_result, ToolResult):
        from ..core.errors import ToolExecutionError

        raise ToolExecutionError(
            "Coverage fragment combination failed",
            reason=combine_result.stderr or "Unknown error during coverage combination",
            rationale="Coverage data must be properly combined for threshold analysis",
        )

    if combined:
        notes.append("Combined coverage fragments into coverage.sqlite.")

    if coverage_file.exists() and coverage_file.stat().st_size > 0:
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
    """Combine coverage fragments into the primary coverage.sqlite file."""
    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    fragments = list(coverage_file.parent.glob(f"{coverage_file.name}.*"))
    if not fragments:
        return None, False

    combine_cmd = ["coverage", "combine", f"--data-file={coverage_file}"]
    formatted = _format_command(combine_cmd)

    if executed_commands is not None and formatted not in executed_commands:
        executed_commands.append(formatted)

    result = subprocess_runner.run_uv_command(
        combine_cmd,
        cwd=root_path,
        env=env,
        timeout=300,  # Use reasonable timeout for coverage operations
        operation_id=operation_id,
    )

    if not result.success:
        return result, True

    if coverage_file.exists() and coverage_file.stat().st_size > 0:
        return None, True

    return None, True


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

    # Record that we invoked the unified coverage tool to generate data
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
    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    if not coverage_file.exists() or coverage_file.stat().st_size == 0:
        fallback_result, fallback_notes = _generate_coverage_via_pytest(
            config=config,
            root_path=root_path,
            args=args,
            verbose=verbose,
            subprocess_runner=subprocess_runner,
            cache_dir=cache_dir,
            operation_id=operation_id,
            executed_commands=executed_commands,
        )
        notes.extend(fallback_notes)
        if isinstance(fallback_result, ToolResult):
            return fallback_result, notes

    return None, notes


def _generate_coverage_via_pytest(
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
    """Generate coverage data by running pytest directly."""
    if executed_commands is None:
        executed_commands = []

    coverage_file = cache_dir / "coverage" / "coverage.sqlite"
    env = {
        "COVERAGE_FILE": str(coverage_file),
        "HYPOTHESIS_DATABASE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_STORAGE_DIRECTORY": str(cache_dir / "hypothesis"),
        "HYPOTHESIS_SEED": "0",
        "PYTHONHASHSEED": "0",
    }

    pytest_args = [
        "tests/unit",
        "tests/property",
        *args,
    ]
    pytest_cmd = ["pytest", *pytest_args]
    formatted_pytest_cmd = _format_command(pytest_cmd)
    if formatted_pytest_cmd not in executed_commands:
        executed_commands.append(formatted_pytest_cmd)

    result = subprocess_runner.run_pytest_command(
        pytest_args,
        cwd=root_path,
        env=env,
        timeout=config.testing.timeout,
        operation_id=operation_id,
    )

    # Clean pytest output
    if result.stdout:
        result.stdout = _clean_pytest_output(result.stdout)

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


def _read_coverage_thresholds_from_config(root_path: Path) -> dict[str, float]:
    """Read coverage thresholds from pyproject.toml."""
    import tomllib
    from typing import Any, cast

    pyproject_path = root_path / "pyproject.toml"
    if not pyproject_path.exists():
        return {}

    try:
        with pyproject_path.open("rb") as f:
            config_data: dict[str, Any] = tomllib.load(f)

        def _as_dict(obj: dict[str, Any], key: str) -> dict[str, Any]:
            value = obj.get(key, {})
            return cast(dict[str, Any], value) if isinstance(value, dict) else {}

        tool_cfg = _as_dict(config_data, "tool")
        ml_cfg = _as_dict(tool_cfg, "ml_playground")
        coverage_cfg = _as_dict(ml_cfg, "coverage")
        thresholds_cfg = _as_dict(coverage_cfg, "thresholds")

        return {
            "line_threshold": float(thresholds_cfg.get("line_threshold", 0.0)),
            "branch_threshold": float(thresholds_cfg.get("branch_threshold", 0.0)),
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
