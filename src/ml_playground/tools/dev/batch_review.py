"""Batch review functionality for AI-assisted code analysis."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any

from ..core.config import ToolsConfig
from ..core.errors import ToolExecutionError, ToolTimeoutError, CommandNotFoundError
from ..core.interfaces import OperationId, ToolResult
from ..quality.quality import QualityTools
from ..testing.testing import TestingTools
from ..utils.subprocess_utils import SubprocessRunner


def run_batch_review(
    config: ToolsConfig,
    project_root_path: Path,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner | None = None,
) -> ToolResult:
    """Perform batch review operations for AI consumption.

    Runs multiple quality checks and formats results in structured formats
    suitable for AI agent analysis and decision-making.

    Args:
        config: Tool configuration
        project_root_path: Project root path
        output_format: Output format (json, yaml, text)
        subprocess_runner: Subprocess runner for dependency injection

    Returns:
        ToolResult with structured output for AI consumption
    """
    operation_id = OperationId(
        namespace="tools", category="dev", command="batch-review"
    )

    # Run quality checks
    quality_results = _run_quality_batch(config, project_root_path, subprocess_runner)

    # Run test summary
    test_results = _run_test_batch(config, project_root_path, subprocess_runner)

    # Combine results
    batch_results: Dict[str, Any] = {
        "timestamp": _get_timestamp(),
        "project_root": str(project_root_path),
        "quality_checks": quality_results,
        "test_summary": test_results,
        "overall_status": _determine_overall_status(quality_results, test_results),
    }

    # Format output
    if output_format.lower() == "json":
        formatted_output = json.dumps(batch_results, indent=2)
    elif output_format.lower() == "yaml":
        import yaml

        formatted_output = yaml.dump(batch_results, default_flow_style=False)
    else:
        formatted_output = _format_text_output(batch_results)

    return ToolResult(
        success=batch_results["overall_status"]["success"],
        exit_code=0 if batch_results["overall_status"]["success"] else 1,
        stdout=formatted_output,
        stderr="",
        operation_id=operation_id,
    )


def _run_quality_batch(
    config: ToolsConfig,
    project_root_path: Path,
    subprocess_runner: SubprocessRunner | None,
) -> Dict[str, Any]:
    """Run batch quality checks and return structured results."""
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
            "issues": len(lint_result.stderr.splitlines()) if lint_result.stderr else 0,
            "output": lint_result.stdout[:500]
            if lint_result.stdout
            else "",  # Truncate for batch
        }
        if not lint_result.success:
            overall_success = False
            total_issues += int(results["lint"]["issues"])
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
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
            total_issues += int(results["typecheck"]["errors"])
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
        ValueError,
    ) as e:
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
            "output": deadcode_result.stdout[:500] if deadcode_result.stdout else "",
        }
        if not deadcode_result.success:
            overall_success = False
            total_issues += int(results["deadcode"]["unused_items"])
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
        ValueError,
    ) as e:
        results["deadcode"] = {"status": "error", "error": str(e)}
        overall_success = False

    results["overall"] = {
        "status": "passed" if overall_success else "failed",
        "total_issues": total_issues,
        "success": overall_success,
    }

    return results


def _run_test_batch(
    config: ToolsConfig,
    project_root_path: Path,
    subprocess_runner: SubprocessRunner | None,
) -> Dict[str, Any]:
    """Run batch test summary and return structured results."""
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
            "output": unit_result.stdout[:300] if unit_result.stdout else "",
        }
        total_tests += test_count
        if not unit_result.success:
            overall_success = False
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
        ValueError,
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
            "output": integration_result.stdout[:300]
            if integration_result.stdout
            else "",
        }
        total_tests += test_count
        if not integration_result.success:
            overall_success = False
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
        ValueError,
    ) as e:
        results["integration"] = {"status": "error", "error": str(e)}
        overall_success = False

    # Get coverage information if available
    try:
        coverage_file = project_root_path / ".cache" / "coverage" / "coverage.sqlite"
        if coverage_file.exists():
            # Try to get coverage data
            coverage_result = testing_tools.coverage_report([], verbose=False)
            results["coverage"] = {
                "status": "available",
                "line_pct": int(
                    _extract_coverage_percentage(coverage_result.stdout, "line")
                ),
                "branch_pct": int(
                    _extract_coverage_percentage(coverage_result.stdout, "branch")
                ),
            }
        else:
            results["coverage"] = {
                "status": "not_available",
                "line_pct": 0,
                "branch_pct": 0,
                "note": "Run 'uv run tools test coverage-test' to generate coverage data",
            }
    except (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
        RuntimeError,
        ValueError,
    ) as e:
        results["coverage"] = {"status": "error", "error": str(e)}

    results["overall"] = {
        "status": "passed" if overall_success else "failed",
        "total_tests": total_tests,
        "success": overall_success,
    }

    return results


def _determine_overall_status(
    quality_results: Dict[str, Any], test_results: Dict[str, Any]
) -> Dict[str, Any]:
    """Determine overall status from batch results."""
    quality_passed = quality_results["overall"]["status"] == "passed"
    tests_passed = test_results["overall"]["status"] == "passed"

    return {
        "success": quality_passed and tests_passed,
        "quality_status": quality_results["overall"]["status"],
        "test_status": test_results["overall"]["status"],
        "ready_for_merge": quality_passed and tests_passed,
    }


def _format_text_output(batch_results: Dict[str, Any]) -> str:
    """Format batch results as human-readable text."""
    output = f"Batch Review Results - {batch_results['timestamp']}\n"
    output += "=" * 50 + "\n\n"

    output += "Quality Checks:\n"
    for check, result in batch_results["quality_checks"].items():
        if isinstance(result, dict) and "status" in result:
            status_icon = "✓" if result["status"] == "passed" else "✗"
            output += f"  {status_icon} {check}: {result['status']}\n"

    output += "\nTest Summary:\n"
    for test_type, result in batch_results["test_summary"].items():
        if isinstance(result, dict) and "status" in result:
            status_icon = "✓" if result["status"] == "passed" else "✗"
            output += f"  {status_icon} {test_type}: {result['status']}\n"

    overall = batch_results["overall_status"]
    output += f"\nOverall Status: {'✓ PASSED' if overall['success'] else '✗ FAILED'}\n"
    output += f"Ready for merge: {'Yes' if overall['ready_for_merge'] else 'No'}\n"

    return output


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


def _get_timestamp() -> str:
    """Get current timestamp for structured output."""
    from datetime import datetime

    return datetime.now().isoformat()
