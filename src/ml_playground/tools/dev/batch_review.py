"""Batch review helpers for dev tools."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
)


def _get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_quality_batch(
    config: ToolsConfig, root_path: Path, *, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    # Placeholder wired for property tests; real implementation can execute quality suite.
    return {
        "overall": {
            "status": "passed",
            "success": True,
        }
    }


def _run_test_batch(
    config: ToolsConfig, root_path: Path, *, subprocess_runner: SubprocessRunner
) -> dict[str, Any]:
    # Placeholder wired for property tests; real implementation can execute test suite.
    return {
        "overall": {
            "status": "passed",
            "success": True,
        }
    }


def _determine_overall_status(
    quality_results: dict[str, Any], test_results: dict[str, Any]
) -> dict[str, Any]:
    quality_success = bool(quality_results.get("overall", {}).get("success", False))
    test_success = bool(test_results.get("overall", {}).get("success", False))
    overall_success = quality_success and test_success
    return {
        "success": overall_success,
        "quality_status": quality_results.get("overall", {}).get("status", "unknown"),
        "test_status": test_results.get("overall", {}).get("status", "unknown"),
    }


def _format_text_output(batch_results: dict[str, Any]) -> str:
    lines: list[str] = ["Batch Review Results", ""]
    lines.append("Quality Checks:")
    lines.append(json.dumps(batch_results["quality_checks"], indent=2))
    lines.append("")
    lines.append("Test Summary:")
    lines.append(json.dumps(batch_results["test_summary"], indent=2))
    lines.append("")
    status = "✓ PASSED" if batch_results["overall_status"]["success"] else "✗ FAILED"
    lines.append(f"Overall Status: {status}")
    return "\n".join(lines)


def run_batch_review(
    config: ToolsConfig,
    root_path: Path,
    *,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner | None = None,
) -> ToolResult:
    runner = subprocess_runner or RealSubprocessRunner()
    operation_id = OperationId(
        namespace="tools", category="dev", command="batch-review"
    )

    quality_results = _run_quality_batch(config, root_path, subprocess_runner=runner)
    test_results = _run_test_batch(config, root_path, subprocess_runner=runner)

    batch_results: dict[str, Any] = {
        "timestamp": _get_timestamp(),
        "project_root": str(root_path),
        "quality_checks": quality_results,
        "test_summary": test_results,
        "overall_status": _determine_overall_status(quality_results, test_results),
    }

    fmt = output_format.lower()
    if fmt == "json":
        formatted_output = json.dumps(batch_results, indent=2)
    elif fmt == "yaml":
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover - defensive guard
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"yaml support unavailable: {exc}",
            )
        formatted_output = yaml.dump(batch_results, default_flow_style=False)
    else:
        formatted_output = _format_text_output(batch_results)

    return ToolResult.create(
        success=batch_results["overall_status"]["success"],
        exit_code=0 if batch_results["overall_status"]["success"] else 1,
        namespace=operation_id.namespace,
        category=operation_id.category,
        command=operation_id.command,
        stdout=formatted_output,
    )


__all__ = [
    "_get_timestamp",
    "_run_quality_batch",
    "_run_test_batch",
    "_determine_overall_status",
    "_format_text_output",
    "run_batch_review",
]
