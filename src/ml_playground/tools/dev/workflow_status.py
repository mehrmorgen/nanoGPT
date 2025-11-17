"""Workflow status functionality for AI-assisted development."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from ..core.config import ToolsConfig
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
    status_data = {
        "timestamp": _get_timestamp(),
        "project_root": str(project_root_path),
        "git_status": _get_git_status(project_root_path, subprocess_runner),
        "quality_status": _get_quality_status(config, project_root_path, subprocess_runner),
        "test_status": _get_test_status(config, project_root_path, subprocess_runner),
        "coverage_status": _get_coverage_status(config, project_root_path, subprocess_runner),
        "readiness": _assess_readiness(
            _get_quality_status(config, project_root_path, subprocess_runner),
            _get_test_status(config, project_root_path, subprocess_runner),
            _get_git_status(project_root_path, subprocess_runner)
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


def _get_git_status(project_root_path: Path, subprocess_runner: SubprocessRunner) -> Dict[str, Any]:
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


def _get_quality_status(config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner) -> Dict[str, Any]:
    """Get quick quality status."""
    try:
        quality_results = _run_quality_batch(config, project_root_path, subprocess_runner)
        return {
            "overall_status": quality_results["overall"]["status"],
            "issues_count": quality_results["overall"]["total_issues"],
            "checks_passed": sum(
                1
                for check in ["lint", "typecheck", "deadcode"]
                if quality_results.get(check, {}).get("status") == "passed"
            ),
        }
    except Exception:
        return {"status": "unknown", "error": "Could not determine quality status"}


def _get_test_status(config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner) -> Dict[str, Any]:
    """Get quick test status."""
    try:
        test_results = _run_test_batch_simple(config, project_root_path, subprocess_runner)
        return {
            "overall_status": test_results["overall"]["status"],
            "total_tests": test_results["overall"]["total_tests"],
            "unit_status": test_results.get("unit", {}).get("status", "unknown"),
            "integration_status": test_results.get("integration", {}).get(
                "status", "unknown"
            ),
        }
    except Exception:
        return {"status": "unknown", "error": "Could not determine test status"}


def _get_coverage_status(config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner) -> Dict[str, Any]:
    """Get coverage status."""
    try:
        testing_tools = TestingTools(config, project_root_path, subprocess_runner)

        coverage_file = project_root_path / ".cache" / "coverage" / "coverage.sqlite"
        if not coverage_file.exists():
            return {
                "status": "not_available",
                "message": "Run coverage-test to generate data",
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
    except Exception:
        return {"status": "unknown", "error": "Could not determine coverage status"}


def _assess_readiness(
    quality_status: Dict[str, Any],
    test_status: Dict[str, Any],
    git_status: Dict[str, Any],
) -> Dict[str, Any]:
    """Assess overall readiness for merge/deployment."""
    quality_ready = quality_status.get("overall_status") == "passed"
    tests_ready = test_status.get("overall_status") == "passed"
    git_clean = git_status.get("status") == "clean"

    overall_ready = quality_ready and tests_ready

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
    quality_status: Dict[str, Any],
    test_status: Dict[str, Any],
    git_status: Dict[str, Any],
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
) -> Dict[str, Any]:
    """Run batch quality checks and return structured results."""
    from ..quality.quality import QualityTools

    quality_tools = QualityTools(config, project_root_path, subprocess_runner)

    results: Dict[str, Any] = {}
    overall_success = True
    total_issues = 0

    # Run lint check
    try:
        lint_result = quality_tools.lint([])
        results["lint"] = {
            "status": "passed" if lint_result.success else "failed",
            "exit_code": lint_result.exit_code,
            "issues": len(lint_result.stderr.splitlines())
            if lint_result.stderr
            else 0,
            "output": lint_result.stdout[:500]
            if lint_result.stdout
            else "",  # Truncate for batch
        }
        if not lint_result.success:
            overall_success = False
            issues_count = results["lint"]["issues"]
            if isinstance(issues_count, list):
                total_issues += len(issues_count)
            else:
                total_issues += int(issues_count)
    except Exception as e:
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
            "output": typecheck_result.stdout[:500]
            if typecheck_result.stdout
            else "",
        }
        if not typecheck_result.success:
            overall_success = False
            errors_count = results["typecheck"]["errors"]
            if isinstance(errors_count, list):
                total_issues += len(errors_count)
            else:
                total_issues += int(errors_count)
    except Exception as e:
        results["typecheck"] = {"status": "error", "error": str(e)}
        overall_success = False

    # Run dead code check
    try:
        deadcode_result = quality_tools.deadcode([])
        results["deadcode"] = {
            "status": "passed" if deadcode_result.success else "failed",
            "exit_code": deadcode_result.exit_code,
            "unused_items": len(deadcode_result.stdout.splitlines())
            if deadcode_result.stdout
            else 0,
            "output": deadcode_result.stdout[:500]
            if deadcode_result.stdout
            else "",
        }
        if not deadcode_result.success:
            overall_success = False
            unused_count = results["deadcode"]["unused_items"]
            if isinstance(unused_count, list):
                total_issues += len(unused_count)
            else:
                total_issues += int(unused_count)
    except Exception as e:
        results["deadcode"] = {"status": "error", "error": str(e)}
        overall_success = False

    results["overall"] = {
        "status": "passed" if overall_success else "failed",
        "total_issues": total_issues,
        "success": overall_success,
    }

    return results


def _run_test_batch_simple(config: ToolsConfig, project_root_path: Path, subprocess_runner: SubprocessRunner) -> Dict[str, Any]:
    """Run simple batch test summary and return structured results."""
    testing_tools = TestingTools(config, project_root_path, subprocess_runner)

    results: Dict[str, Any] = {}
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
    except Exception as e:
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
    except Exception as e:
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


def _format_status_text_output(status_data: Dict[str, Any]) -> str:
    """Format status data as human-readable text."""
    output = f"Workflow Status - {status_data['timestamp']}\n"
    output += "=" * 40 + "\n\n"

    # Git status
    git = status_data.get("git_status", {})
    output += f"Git: {git.get('current_branch', 'unknown')} ({git.get('status', 'unknown')})\n"

    # Quality status
    quality = status_data.get("quality_status", {})
    quality_icon = "✓" if quality.get("overall_status") == "passed" else "✗"
    output += (
        f"Quality: {quality_icon} {quality.get('overall_status', 'unknown')}\n"
    )

    # Test status
    test = status_data.get("test_status", {})
    test_icon = "✓" if test.get("overall_status") == "passed" else "✗"
    output += f"Tests: {test_icon} {test.get('total_tests', 0)} tests\n"

    # Coverage status
    coverage = status_data.get("coverage_status", {})
    if coverage.get("status") == "available":
        output += f"Coverage: {coverage.get('line_percentage', 0):.1f}% lines, {coverage.get('branch_percentage', 0):.1f}% branches\n"
    else:
        output += f"Coverage: {coverage.get('status', 'unknown')}\n"

    # Readiness
    readiness = status_data.get("readiness", {})
    ready_icon = "✓" if readiness.get("ready_for_merge", False) else "✗"
    output += f"\nReady for merge: {ready_icon} {'Yes' if readiness.get('ready_for_merge', False) else 'No'}\n"

    # Blocking issues
    if readiness.get("blocking_issues"):
        output += "\nBlocking issues:\n"
        for issue in readiness["blocking_issues"]:
            output += f"  • {issue}\n"

    return output


def _get_timestamp() -> str:
    """Get current timestamp for structured output."""
    from datetime import datetime

    return datetime.now().isoformat()
