"""Workflow status functionality for AI-assisted development."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from ..core.config import ToolsConfig
from ..core.errors import (
    CommandNotFoundError,
    ToolConfigurationError,
    ToolExecutionError,
    TimeoutError,
)
from ..core.interfaces import OperationId, ToolResult
from ..testing.testing import TestingTools
from ..utils.subprocess_utils import SubprocessRunner


def run_workflow_status(
    config: ToolsConfig,
    project_root_path: Path,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner | None = None,
) -> ToolResult:
    """Get current workflow status for AI decision-making.

    Provides a comprehensive status report of the current development
    state, including quality metrics, test status, and readiness indicators.

    Args:
        config: Tool configuration
        project_root_path: Project root path
        output_format: Output format (json, yaml, text)
        subprocess_runner: Subprocess runner for dependency injection

    Returns:
        ToolResult with current workflow status
    """
    from ..utils.subprocess_utils import RealSubprocessRunner

    operation_id = OperationId(
        namespace="tools", category="dev", command="workflow-status"
    )

    # Provide default subprocess runner if none given
    if subprocess_runner is None:
        subprocess_runner = RealSubprocessRunner()

    # Gather comprehensive status information
    status_data: dict[str, object] = {
        "timestamp": _get_timestamp(),
        "project_root": str(project_root_path),
        "git_status": _get_git_status(project_root_path, subprocess_runner),
        "quality_status": _get_quality_status(
            config, project_root_path, subprocess_runner
        ),
        "test_status": _get_test_status(config, project_root_path, subprocess_runner),
        "coverage_status": _get_coverage_status(
            config, project_root_path, subprocess_runner
        ),
        "readiness": _assess_readiness(
            _get_quality_status(config, project_root_path, subprocess_runner),
            _get_test_status(config, project_root_path, subprocess_runner),
            _get_git_status(project_root_path, subprocess_runner),
        ),
    }

    # Format output
    if output_format.lower() == "json":
        formatted_output = json.dumps(status_data, indent=2)
    elif output_format.lower() == "yaml":
        import yaml

        formatted_output = yaml.dump(status_data, default_flow_style=False)
    else:
        formatted_output = _format_status_text_output(status_data)

    return ToolResult(
        success=True,  # Status check always succeeds
        exit_code=0,
        stdout=formatted_output,
        stderr="",
        operation_id=operation_id,
    )


def _get_git_status(
    project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Get git status information."""
    try:
        # Get current branch
        branch_result = subprocess_runner.run_subprocess(
            ["git", "branch", "--show-current"],
            cwd=project_root_path,
            timeout=10,
            operation_id=OperationId(
                namespace="tools", category="dev", command="git-status"
            ),
        )

        # Get status
        status_result = subprocess_runner.run_subprocess(
            ["git", "status", "--porcelain"],
            cwd=project_root_path,
            timeout=10,
            operation_id=OperationId(
                namespace="tools", category="dev", command="git-status"
            ),
        )

        return {
            "current_branch": branch_result.stdout.strip()
            if branch_result.success
            else "unknown",
            "has_changes": bool(status_result.stdout.strip())
            if status_result.success
            else False,
            "status": "clean" if not status_result.stdout.strip() else "dirty",
        }
    except Exception:
        return {"status": "unknown", "error": "Could not determine git status"}


def _get_quality_status(
    config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Get quick quality status."""
    try:
        quality_results: dict[str, Any] = _run_quality_batch(
            config, project_root_path, subprocess_runner
        )
        overall_obj = quality_results.get("overall", {})  # type: ignore[reportAny]
        overall_map: dict[str, object] = (
            dict(cast(Mapping[str, object], overall_obj))
            if isinstance(overall_obj, Mapping)
            else {}
        )
        overall_status_obj = overall_map.get("status", "unknown")
        overall_status = (
            str(overall_status_obj) if overall_status_obj is not None else "unknown"
        )
        issues_obj = overall_map.get("total_issues", 0)
        issues_count = int(issues_obj) if isinstance(issues_obj, (int, float)) else 0

        checks_passed = 0
        for check_key in ("lint", "typecheck", "deadcode"):
            check_obj = quality_results.get(check_key)
            if isinstance(check_obj, Mapping):
                check_map: dict[str, object] = dict(
                    cast(Mapping[str, object], check_obj)
                )
                check_status_obj: object | None = check_map.get("status")
                check_status: str = (
                    str(check_status_obj) if check_status_obj is not None else "unknown"
                )
                if check_status == "passed":
                    checks_passed += 1

        return {
            "overall_status": overall_status,
            "issues_count": issues_count,
            "checks_passed": checks_passed,
        }
    except Exception:
        return {"status": "unknown", "error": "Could not determine quality status"}


def _get_test_status(
    config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Get quick test status."""
    try:
        test_results: dict[str, Any] = _run_test_batch_simple(
            config, project_root_path, subprocess_runner
        )
        overall_obj = test_results.get("overall", {})  # type: ignore[reportAny]
        overall_map: dict[str, object] = (
            dict(cast(Mapping[str, object], overall_obj))
            if isinstance(overall_obj, Mapping)
            else {}
        )
        overall_status_obj = overall_map.get("status", "unknown")
        overall_status = (
            str(overall_status_obj) if overall_status_obj is not None else "unknown"
        )
        total_tests_obj = overall_map.get("total_tests", 0)
        total_tests = (
            int(total_tests_obj) if isinstance(total_tests_obj, (int, float)) else 0
        )

        def _status_for(section: str) -> str:
            section_obj = test_results.get(section)
            if not isinstance(section_obj, Mapping):
                return "unknown"
            section_map = cast(Mapping[str, object], section_obj)
            status_obj = section_map.get("status", "unknown")
            return str(status_obj) if status_obj is not None else "unknown"

        return {
            "overall_status": overall_status,
            "total_tests": total_tests,
            "unit_status": _status_for("unit"),
            "integration_status": _status_for("integration"),
        }
    except Exception:
        return {"status": "unknown", "error": "Could not determine test status"}


def _get_coverage_status(
    config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Get coverage status."""
    try:
        testing_tools = TestingTools(config, project_root_path, subprocess_runner)

        coverage_dir = project_root_path / ".cache" / "coverage"
        coverage_file = coverage_dir / "coverage.json"
        legacy_coverage_file = coverage_dir / "coverage.sqlite"
        if not coverage_file.exists() and not legacy_coverage_file.exists():
            return {
                "status": "not_available",
                "message": "Run coverage to generate data",
            }

        coverage_result = testing_tools.coverage_report([], verbose=False)
        return {
            "status": "available",
            "line_percentage": _extract_coverage_percentage(
                coverage_result.stdout, "line"
            ),
            "branch_percentage": _extract_coverage_percentage(
                coverage_result.stdout, "branch"
            ),
        }
    except (
        ToolExecutionError,
        TimeoutError,
        CommandNotFoundError,
        RuntimeError,
        OSError,
        subprocess.SubprocessError,
        FileNotFoundError,
        ImportError,
    ):
        return {"status": "unknown", "error": "Could not determine coverage status"}


def _assess_readiness(
    quality_status: dict[str, Any],
    test_status: dict[str, Any],
    git_status: dict[str, Any],
) -> dict[str, Any]:
    """Assess overall readiness for merge/deployment."""
    quality_ready = quality_status.get("overall_status") == "passed"  # type: ignore[reportAny]
    tests_ready = test_status.get("overall_status") == "passed"  # type: ignore[reportAny]
    git_clean = git_status.get("status") == "clean"  # type: ignore[reportAny]

    overall_ready = quality_ready and tests_ready  # type: ignore[reportAny]

    return {
        "ready_for_merge": overall_ready,
        "quality_ready": quality_ready,
        "tests_ready": tests_ready,
        "git_clean": git_clean,
        "blocking_issues": _get_blocking_issues(
            quality_status, test_status, git_status
        ),
    }


def _get_blocking_issues(
    quality_status: dict[str, object],
    test_status: dict[str, object],
    git_status: dict[str, object],
) -> list[str]:
    """Get list of blocking issues."""
    issues: list[str] = []

    if quality_status.get("overall_status") != "passed":
        issues.append(
            f"Quality checks failing ({quality_status.get('issues_count', 0)} issues)"
        )

    if test_status.get("overall_status") != "passed":
        issues.append("Test failures detected")

    if git_status.get("has_changes", False):
        issues.append("Uncommitted changes present")

    return issues


def _run_quality_batch(
    config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Run batch quality checks and return structured results."""
    from ..quality.quality import QualityTools

    quality_tools = QualityTools(config, project_root_path, subprocess_runner)

    results: dict[str, object] = {}
    overall_success = True
    total_issues = 0

    # Run lint check
    try:
        lint_result = quality_tools.lint([])
        lint_dict: dict[str, Any] = {
            "status": "passed" if lint_result.success else "failed",
            "exit_code": lint_result.exit_code,
            "issues": len(lint_result.stderr.splitlines()) if lint_result.stderr else 0,
            "output": lint_result.stdout[:500]
            if lint_result.stdout
            else "",  # Truncate for batch
        }
        results["lint"] = lint_dict
        if not lint_result.success:
            overall_success = False
            issues_obj = lint_dict.get("issues", 0)  # type: ignore[reportAny]
            total_issues += (
                int(issues_obj) if isinstance(issues_obj, (int, float)) else 0
            )
    except (
        ToolExecutionError,
        ToolConfigurationError,
        TimeoutError,
        OSError,
        ValueError,
    ) as e:
        results["lint"] = {"status": "error", "error": str(e)}
        overall_success = False

    # Run type check
    try:
        typecheck_result = quality_tools.typecheck([])
        results["typecheck"] = {
            "status": "passed" if typecheck_result.success else "failed",
            "exit_code": typecheck_result.exit_code,
            "errors": len(typecheck_result.stderr.splitlines())
            if typecheck_result.stderr
            else 0,
            "output": typecheck_result.stdout[:500] if typecheck_result.stdout else "",
        }
        if not typecheck_result.success:
            overall_success = False
            typecheck_result_dict = cast(dict[str, Any], results["typecheck"])
            errors_obj = typecheck_result_dict.get("errors", 0)  # type: ignore[reportAny]
            total_issues += (
                int(errors_obj) if isinstance(errors_obj, (int, float)) else 0
            )
    except (
        ToolExecutionError,
        ToolConfigurationError,
        TimeoutError,
        OSError,
        ValueError,
    ) as e:
        results["typecheck"] = {"status": "error", "error": str(e)}
        overall_success = False

    # Run dead code check
    try:
        deadcode_result = quality_tools.deadcode([])
        deadcode_dict: dict[str, Any] = {
            "status": "passed" if deadcode_result.success else "failed",
            "exit_code": deadcode_result.exit_code,
            "unused_items": len(deadcode_result.stdout.splitlines())
            if deadcode_result.stdout
            else 0,
            "output": deadcode_result.stdout[:500] if deadcode_result.stdout else "",
        }
        results["deadcode"] = deadcode_dict
        if not deadcode_result.success:
            overall_success = False
            unused_items_obj = deadcode_dict.get("unused_items", 0)  # type: ignore[reportAny]
            total_issues += (
                int(unused_items_obj)
                if isinstance(unused_items_obj, (int, float))
                else 0
            )
    except (
        ToolExecutionError,
        TimeoutError,
        CommandNotFoundError,
        RuntimeError,
    ) as e:
        results["deadcode"] = {"status": "error", "error": str(e)}
        overall_success = False

    results["overall"] = {
        "status": "passed" if overall_success else "failed",
        "total_issues": total_issues,
        "success": overall_success,
    }

    return results


def _run_test_batch_simple(
    config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    """Run simple batch test summary and return structured results."""
    testing_tools = TestingTools(config, project_root_path, subprocess_runner)

    results: dict[str, object] = {}
    overall_success = True
    total_tests = 0

    # Run unit tests
    try:
        unit_result = testing_tools.unit(["--tb=no", "-q"])  # Quiet mode for batch
        test_count = _extract_test_count(unit_result.stdout)
        results["unit"] = {
            "status": "passed" if unit_result.success else "failed",
            "exit_code": unit_result.exit_code,
            "count": test_count,
            "duration": _extract_duration(unit_result.stdout),
        }
        total_tests += test_count
        if not unit_result.success:
            overall_success = False
    except (
        ToolExecutionError,
        TimeoutError,
        CommandNotFoundError,
        RuntimeError,
    ) as e:
        results["unit"] = {"status": "error", "error": str(e)}
        overall_success = False

    # Run integration tests (if they exist)
    try:
        integration_result = testing_tools.integration(["--tb=no", "-q"])
        test_count = _extract_test_count(integration_result.stdout)
        results["integration"] = {
            "status": "passed" if integration_result.success else "failed",
            "exit_code": integration_result.exit_code,
            "count": test_count,
            "duration": _extract_duration(integration_result.stdout),
        }
        total_tests += test_count
        if not integration_result.success:
            overall_success = False
    except (
        ToolExecutionError,
        TimeoutError,
        CommandNotFoundError,
        RuntimeError,
    ) as e:
        results["integration"] = {"status": "error", "error": str(e)}
        overall_success = False

    results["overall"] = {
        "status": "passed" if overall_success else "failed",
        "total_tests": total_tests,
        "success": overall_success,
    }

    return results


def _extract_test_count(output: str) -> int:
    """Extract test count from pytest output."""
    import re

    # Look for patterns like "5 passed" or "10 failed, 2 passed"
    match = re.search(r"(\d+)\s+passed", output)
    if match:
        return int(match.group(1))
    return 0


def _extract_duration(output: str) -> str:
    """Extract duration from pytest output."""
    import re

    # Look for patterns like "in 0.12s" or "in 1.23 seconds"
    match = re.search(r"in\s+([\d.]+)s?", output)
    if match:
        return f"{match.group(1)}s"
    return "0s"


def _extract_coverage_percentage(output: str, coverage_type: str) -> float:
    """Extract coverage percentage from coverage output."""
    import re

    # This is a simplified extraction - real implementation would parse coverage JSON
    if coverage_type == "line":
        match = re.search(r"TOTAL.*?(\d+)%", output)
    else:  # branch
        match = re.search(r"TOTAL.*?\d+%.*?(\d+)%", output)

    if match:
        return float(match.group(1))
    return 0.0


def _format_status_text_output(status_data: dict[str, object]) -> str:
    """Format status data as human-readable text."""
    output = f"Workflow Status - {status_data['timestamp']}\n"
    output += "=" * 40 + "\n\n"

    # Git status
    git_obj = status_data.get("git_status")
    if isinstance(git_obj, Mapping):
        git_map: Mapping[str, object] = cast(Mapping[str, object], git_obj)
        git: dict[str, object] = dict(git_map)
        current_branch_obj = git.get("current_branch", "unknown")
        current_branch: str = (
            str(current_branch_obj) if current_branch_obj is not None else "unknown"
        )
        git_status_obj = git.get("status", "unknown")
        git_status: str = (
            str(git_status_obj) if git_status_obj is not None else "unknown"
        )
        output += f"Git: {current_branch} ({git_status})\n"

    # Quality status
    quality_obj = status_data.get("quality_status")
    if isinstance(quality_obj, Mapping):
        quality_map: Mapping[str, object] = cast(Mapping[str, object], quality_obj)
        quality: dict[str, object] = dict(quality_map)
        overall_status_obj = quality.get("overall_status", "unknown")
        overall_status: str = (
            str(overall_status_obj) if overall_status_obj is not None else "unknown"
        )
        quality_icon = "✓" if overall_status == "passed" else "✗"
        output += f"Quality: {quality_icon} {overall_status}\n"

    # Test status
    test_obj = status_data.get("test_status")
    if isinstance(test_obj, Mapping):
        test_map: Mapping[str, object] = cast(Mapping[str, object], test_obj)
        test: dict[str, object] = dict(test_map)
        test_overall_status_obj = test.get("overall_status", "unknown")
        test_overall_status: str = (
            str(test_overall_status_obj)
            if test_overall_status_obj is not None
            else "unknown"
        )
        total_tests_obj = test.get("total_tests", 0)
        total_tests: int = (
            int(total_tests_obj) if isinstance(total_tests_obj, (int, float)) else 0
        )
        test_icon = "✓" if test_overall_status == "passed" else "✗"
        output += f"Tests: {test_icon} {total_tests} tests\n"

    # Coverage status
    coverage_obj = status_data.get("coverage_status", {})
    if isinstance(coverage_obj, Mapping):
        coverage_map: Mapping[str, object] = cast(Mapping[str, object], coverage_obj)
        coverage_status: dict[str, object] = dict(coverage_map)
        status_field = coverage_status.get("status", "unknown")
        if status_field == "available":
            line_pct_obj = coverage_status.get("line_percentage", 0)
            branch_pct_obj = coverage_status.get("branch_percentage", 0)
            if isinstance(line_pct_obj, (int, float)) and isinstance(
                branch_pct_obj, (int, float)
            ):
                output += (
                    f"Coverage: {line_pct_obj:.1f}% lines, "
                    f"{branch_pct_obj:.1f}% branches\n"
                )
        else:
            status: str = str(status_field) if status_field is not None else "unknown"
            output += f"Coverage: {status}\n"
    else:
        output += "Coverage: unknown\n"

    # Readiness
    readiness_obj = status_data.get("readiness")
    if isinstance(readiness_obj, Mapping):
        readiness_map: Mapping[str, object] = cast(Mapping[str, object], readiness_obj)
        readiness: dict[str, object] = dict(readiness_map)

        ready_for_merge_obj = readiness.get("ready_for_merge", False)
        ready_for_merge: bool = bool(ready_for_merge_obj)
        ready_icon = "✓" if ready_for_merge else "✗"
        output += (
            f"\nReady for merge: {ready_icon} {'Yes' if ready_for_merge else 'No'}\n"
        )

        # Blocking issues
        blocking_issues_obj = readiness.get("blocking_issues")
        if isinstance(blocking_issues_obj, list) and blocking_issues_obj:
            blocking_issues_list: list[object] = cast(list[object], blocking_issues_obj)
            blocking_issues: list[str] = []
            for issue_obj in blocking_issues_list:
                if isinstance(issue_obj, str):
                    blocking_issues.append(issue_obj)
            output += "\nBlocking issues:\n"
            for issue in blocking_issues:
                output += f"  • {issue}\n"

    return output


def _get_timestamp() -> str:
    """Get current timestamp for structured output."""
    from datetime import datetime

    return datetime.now().isoformat()
