from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import ml_playground.tools.dev.batch_review as batch_review_module
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult


def _tool_result(
    *,
    category: str,
    command: str,
    success: bool,
    stdout: str = "",
    stderr: str = "",
) -> ToolResult:
    return ToolResult(
        success=success,
        exit_code=0 if success else 1,
        stdout=stdout,
        stderr=stderr,
        operation_id=OperationId(namespace="tools", category=category, command=command),
    )


def test_run_quality_batch_aggregates_results(tmp_path: Path) -> None:
    """`_run_quality_batch` should compile tool outputs and total failure counts."""

    class StubQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, args: list[str]) -> ToolResult:
            assert args == []
            return _tool_result(
                category="quality",
                command="lint",
                success=False,
                stderr="missing import\nunused var",
            )

        def typecheck(self, args: list[str]) -> ToolResult:
            assert args == []
            return _tool_result(
                category="quality",
                command="typecheck",
                success=False,
                stderr="error line",
            )

        def deadcode(self, args: list[str]) -> ToolResult:
            assert args == []
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="unused\nthing",
            )

    original = batch_review_module.QualityTools
    batch_review_module.QualityTools = StubQualityTools
    try:
        result = batch_review_module._run_quality_batch(ToolsConfig(), tmp_path, None)
    finally:
        batch_review_module.QualityTools = original

    assert result["lint"]["status"] == "failed"
    assert result["lint"]["issues"] == 2
    assert result["typecheck"]["errors"] == 1
    assert result["deadcode"]["unused_items"] == 2
    assert result["overall"] == {
        "status": "failed",
        "total_issues": 3,
        "success": False,
    }


def test_run_quality_batch_records_errors(tmp_path: Path) -> None:
    """Exceptions raised by quality tools should surface as error entries."""

    class RaisingQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, _: list[str]) -> ToolResult:  # noqa: ANN001
            raise RuntimeError("lint boom")

        def typecheck(self, _: list[str]) -> ToolResult:  # noqa: ANN001
            return _tool_result(
                category="quality",
                command="typecheck",
                success=True,
                stdout="ok",
            )

        def deadcode(self, _: list[str]) -> ToolResult:  # noqa: ANN001
            raise ValueError("deadcode fail")

    original = batch_review_module.QualityTools
    batch_review_module.QualityTools = RaisingQualityTools
    try:
        result = batch_review_module._run_quality_batch(ToolsConfig(), tmp_path, None)
    finally:
        batch_review_module.QualityTools = original

    assert result["lint"]["status"] == "error"
    assert result["deadcode"]["status"] == "error"
    assert result["overall"]["success"] is False


def test_run_test_batch_collects_counts_and_coverage(tmp_path: Path) -> None:
    """`_run_test_batch` should summarize test counts, durations, and coverage."""

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_text("data", encoding="utf-8")

    class HappyTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, args: list[str]) -> ToolResult:
            assert "--tb=no" in args and "-q" in args
            return _tool_result(
                category="test",
                command="unit",
                success=True,
                stdout="2 passed in 0.15s",
            )

        def integration(self, args: list[str]) -> ToolResult:
            assert "--tb=no" in args and "-q" in args
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="3 passed in 1.01s",
            )

        def coverage_report(self, args: list[str], verbose: bool = False) -> ToolResult:
            assert args == [] and verbose is False
            return _tool_result(
                category="test",
                command="coverage-report",
                success=True,
                stdout="TOTAL 90% 75%",
            )

    original = batch_review_module.TestingTools
    batch_review_module.TestingTools = HappyTestingTools
    try:
        result = batch_review_module._run_test_batch(ToolsConfig(), tmp_path, None)
    finally:
        batch_review_module.TestingTools = original

    assert result["unit"]["count"] == 2
    assert result["integration"]["duration"] == "1.01s"
    assert result["coverage"] == {
        "status": "available",
        "line_pct": 90,
        "branch_pct": 75,
    }
    assert result["overall"]["total_tests"] == 5
    assert result["overall"]["success"] is True


def test_run_test_batch_handles_failures(tmp_path: Path) -> None:
    """Failures and exceptions should mark test summary entries appropriately."""

    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_text("data", encoding="utf-8")

    class FailingTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, _: list[str]) -> ToolResult:  # noqa: ANN001
            return _tool_result(
                category="test",
                command="unit",
                success=False,
                stdout="0 passed in 0.01s",
                stderr="boom",
            )

        def integration(self, _: list[str]) -> ToolResult:  # noqa: ANN001
            raise RuntimeError("integration unavailable")

        def coverage_report(self, *_: object, **__: object) -> ToolResult:  # noqa: ANN401
            raise ValueError("coverage failure")

    original = batch_review_module.TestingTools
    batch_review_module.TestingTools = FailingTestingTools
    try:
        result = batch_review_module._run_test_batch(ToolsConfig(), tmp_path, None)
    finally:
        batch_review_module.TestingTools = original

    assert result["unit"]["status"] == "failed"
    assert result["integration"]["status"] == "error"
    assert result["coverage"]["status"] == "error"
    assert result["overall"]["success"] is False


def test_run_batch_review_formats_json(tmp_path: Path) -> None:
    """`run_batch_review` should combine sub-results and emit JSON output."""

    quality = {
        "overall": {"status": "passed", "success": True},
        "lint": {"status": "passed"},
    }
    tests = {
        "overall": {"status": "passed", "success": True},
        "unit": {"status": "passed"},
    }

    original_quality = batch_review_module._run_quality_batch
    original_test_batch = batch_review_module._run_test_batch
    original_ts = batch_review_module._get_timestamp

    batch_review_module._run_quality_batch = (  # type: ignore[assignment]
        lambda *args, **kwargs: quality
    )
    batch_review_module._run_test_batch = (  # type: ignore[assignment]
        lambda *args, **kwargs: tests
    )
    batch_review_module._get_timestamp = lambda: "ts"  # type: ignore[assignment]

    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )
    finally:
        batch_review_module._run_quality_batch = original_quality  # type: ignore[assignment]
        batch_review_module._run_test_batch = original_test_batch  # type: ignore[assignment]
        batch_review_module._get_timestamp = original_ts  # type: ignore[assignment]

    payload = json.loads(result.stdout)
    assert payload["quality_checks"] == quality
    assert payload["test_summary"] == tests
    assert payload["overall_status"]["success"] is True
    assert result.success is True and result.exit_code == 0


def test_run_batch_review_supports_yaml_and_text(tmp_path: Path) -> None:
    """YAML and text formats should be supported with deterministic output."""

    quality = {
        "overall": {"status": "failed", "success": False},
        "lint": {"status": "failed"},
    }
    tests = {
        "overall": {"status": "passed", "success": True},
        "unit": {"status": "passed"},
    }

    original_quality = batch_review_module._run_quality_batch
    original_test_batch = batch_review_module._run_test_batch
    original_ts = batch_review_module._get_timestamp
    original_yaml = sys.modules.get("yaml")

    batch_review_module._run_quality_batch = (  # type: ignore[assignment]
        lambda *args, **kwargs: quality
    )
    batch_review_module._run_test_batch = (  # type: ignore[assignment]
        lambda *args, **kwargs: tests
    )
    batch_review_module._get_timestamp = lambda: "fixed-ts"  # type: ignore[assignment]

    yaml_stub = types.SimpleNamespace(
        dump=lambda data, default_flow_style=False: "yaml-output"
    )
    sys.modules["yaml"] = yaml_stub

    try:
        yaml_result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="yaml"
        )
        assert yaml_result.stdout == "yaml-output"
        assert yaml_result.success is False

        text_result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="text"
        )
    finally:
        batch_review_module._run_quality_batch = original_quality  # type: ignore[assignment]
        batch_review_module._run_test_batch = original_test_batch  # type: ignore[assignment]
        batch_review_module._get_timestamp = original_ts  # type: ignore[assignment]
        if original_yaml is not None:
            sys.modules["yaml"] = original_yaml
        else:
            del sys.modules["yaml"]
    assert "Quality Checks" in text_result.stdout
    assert "Overall Status" in text_result.stdout
    assert text_result.success is False and text_result.exit_code == 1
