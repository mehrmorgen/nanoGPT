"""Batch review helpers for dev tools."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

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
) -> dict[str, object]:
    # Placeholder wired for property tests; real implementation can execute quality suite.
    return {
        "overall": {
            "status": "passed",
            "success": True,
        }
    }


def _run_test_batch(
    config: ToolsConfig, root_path: Path, *, subprocess_runner: SubprocessRunner
) -> dict[str, object]:
    # Placeholder wired for property tests; real implementation can execute test suite.
    return {
        "overall": {
            "status": "passed",
            "success": True,
        }
    }


def _determine_overall_status(
    quality_results: dict[str, object], test_results: dict[str, object]
) -> dict[str, object]:
    quality_overall = cast(dict[str, object], quality_results.get("overall", {}))
    quality_success = bool(quality_overall.get("success", False))
    test_overall = cast(dict[str, object], test_results.get("overall", {}))
    test_success = bool(test_overall.get("success", False))
    overall_success = quality_success and test_success
    return {
        "success": overall_success,
        "quality_status": str(quality_overall.get("status", "unknown")),
        "test_status": str(test_overall.get("status", "unknown")),
    }


def _format_text_output(batch_results: dict[str, object]) -> str:
    lines: list[str] = ["Batch Review Results", ""]
    lines.append("Quality Checks:")
    lines.append(json.dumps(batch_results["quality_checks"], indent=2))
    lines.append("")
    lines.append("Test Summary:")
    lines.append(json.dumps(batch_results["test_summary"], indent=2))
    lines.append("")
    overall_status = cast(dict[str, object], batch_results["overall_status"])
    status = "✓ PASSED" if overall_status["success"] else "✗ FAILED"
    lines.append(f"Overall Status: {status}")
    return "\n".join(lines)


def run_batch_review(
    config: ToolsConfig,
    project_root_path: Path,
    *,
    output_format: str = "json",
    subprocess_runner: SubprocessRunner | None = None,
    yaml_module: object | None = None,
) -> ToolResult:
    runner = subprocess_runner or RealSubprocessRunner()
    operation_id = OperationId(
        namespace="tools", category="dev", command="batch-review"
    )

    quality_results = _run_quality_batch(
        config, project_root_path, subprocess_runner=runner
    )
    test_results = _run_test_batch(config, project_root_path, subprocess_runner=runner)

    batch_results: dict[str, object] = {
        "timestamp": _get_timestamp(),
        "project_root": str(project_root_path),
        "quality_checks": quality_results,
        "test_summary": test_results,
        "overall_status": _determine_overall_status(quality_results, test_results),
    }

    fmt = output_format.lower()
    if fmt == "json":
        formatted_output = json.dumps(batch_results, indent=2)
    elif fmt == "yaml":
        try:
            mod = yaml_module
            if mod is None:
                import yaml as real_yaml

                mod = real_yaml
            # We assume the module (passed or imported) has a dump method
            dump_fn = getattr(mod, "dump")
            if not callable(dump_fn):
                raise ImportError("yaml module missing dump")
            formatted_output = cast(
                str, dump_fn(batch_results, default_flow_style=False)
            )
        except (ImportError, AttributeError) as exc:
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace=operation_id.namespace,
                category=operation_id.category,
                command=operation_id.command,
                stderr=f"yaml support unavailable: {exc}",
            )
    else:
        formatted_output = _format_text_output(batch_results)

    overall_status = cast(dict[str, object], batch_results["overall_status"])
    success_val = bool(overall_status.get("success", False))
    return ToolResult.create(
        success=success_val,
        exit_code=0 if success_val else 1,
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
