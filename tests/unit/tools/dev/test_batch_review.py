from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

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


def test_run_quality_batch_behavior_via_public_api(tmp_path: Path) -> None:
    """Quality batch behavior should be observable through public API."""

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

    # Mock testing tools to return minimal successful results
    class StubTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="unit",
                success=True,
                stdout="1 passed in 0.1s",
            )

        def integration(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="2 passed in 0.2s",
            )

        def coverage_report(self, args: list[str], verbose: bool = False) -> ToolResult:
            return _tool_result(
                category="test",
                command="coverage-report",
                success=True,
                stdout="TOTAL 90% 75%",
            )

    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools
    batch_review_module.QualityTools = StubQualityTools
    batch_review_module.TestingTools = StubTestingTools
    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Parse JSON output to verify quality batch behavior
        import json

        payload = json.loads(result.stdout)
        quality_checks = payload["quality_checks"]

        assert quality_checks["lint"]["status"] == "failed"
        assert quality_checks["lint"]["issues"] == 2
        assert quality_checks["typecheck"]["errors"] == 1
        assert quality_checks["deadcode"]["unused_items"] == 2
        assert quality_checks["overall"] == {
            "status": "failed",
            "total_issues": 3,
            "success": False,
        }
    finally:
        batch_review_module.QualityTools = original_quality
        batch_review_module.TestingTools = original_testing


def test_quality_error_handling_via_public_api(tmp_path: Path) -> None:
    """Quality tool exceptions should surface as error entries via public API."""

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

    # Mock testing tools to return minimal successful results
    class StubTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="unit",
                success=True,
                stdout="1 passed in 0.1s",
            )

        def integration(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="2 passed in 0.2s",
            )

        def coverage_report(self, args: list[str], verbose: bool = False) -> ToolResult:
            return _tool_result(
                category="test",
                command="coverage-report",
                success=True,
                stdout="TOTAL 90% 75%",
            )

    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools
    batch_review_module.QualityTools = RaisingQualityTools
    batch_review_module.TestingTools = StubTestingTools
    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Parse JSON output to verify error handling
        import json

        payload = json.loads(result.stdout)
        quality_checks = payload["quality_checks"]

        assert quality_checks["lint"]["status"] == "error"
        assert quality_checks["deadcode"]["status"] == "error"
        assert quality_checks["overall"]["success"] is False
    finally:
        batch_review_module.QualityTools = original_quality
        batch_review_module.TestingTools = original_testing


def test_run_test_batch_behavior_via_public_api(tmp_path: Path) -> None:
    """Test batch behavior should be observable through public API."""

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

    # Mock quality tools to return minimal successful results
    class StubQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="lint",
                success=True,
                stdout="No issues found",
            )

        def typecheck(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="typecheck",
                success=True,
                stdout="Type checking passed",
            )

        def deadcode(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="No dead code found",
            )

    original_testing = batch_review_module.TestingTools
    original_quality = batch_review_module.QualityTools
    batch_review_module.TestingTools = HappyTestingTools
    batch_review_module.QualityTools = StubQualityTools
    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Parse JSON output to verify test batch behavior
        import json

        payload = json.loads(result.stdout)
        test_summary = payload["test_summary"]

        assert test_summary["unit"]["count"] == 2
        assert test_summary["integration"]["duration"] == "1.01s"
        assert test_summary["coverage"] == {
            "status": "available",
            "line_pct": 90,
            "branch_pct": 75,
        }
        assert test_summary["overall"]["total_tests"] == 5
        assert test_summary["overall"]["success"] is True
    finally:
        batch_review_module.TestingTools = original_testing
        batch_review_module.QualityTools = original_quality


def test_test_error_handling_via_public_api(tmp_path: Path) -> None:
    """Test failures and exceptions should mark test summary entries appropriately via public API."""

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

    # Mock quality tools to return minimal successful results
    class StubQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="lint",
                success=True,
                stdout="No issues found",
            )

        def typecheck(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="typecheck",
                success=True,
                stdout="Type checking passed",
            )

        def deadcode(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="No dead code found",
            )

    original_testing = batch_review_module.TestingTools
    original_quality = batch_review_module.QualityTools
    batch_review_module.TestingTools = FailingTestingTools
    batch_review_module.QualityTools = StubQualityTools
    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Parse JSON output to verify error handling
        import json

        payload = json.loads(result.stdout)
        test_summary = payload["test_summary"]

        assert test_summary["unit"]["status"] == "failed"
        assert test_summary["integration"]["status"] == "error"
        assert test_summary["coverage"]["status"] == "error"
        assert test_summary["overall"]["success"] is False
    finally:
        batch_review_module.TestingTools = original_testing
        batch_review_module.QualityTools = original_quality


def test_run_batch_review_formats_json(tmp_path: Path) -> None:
    """`run_batch_review` should combine sub-results and emit JSON output."""

    # Create mock tools that return predictable results
    class StubQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="lint",
                success=True,
                stdout="No issues",
            )

        def typecheck(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="typecheck",
                success=True,
                stdout="Type check passed",
            )

        def deadcode(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="No dead code",
            )

    class StubTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="unit",
                success=True,
                stdout="1 passed in 0.1s",
            )

        def integration(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="2 passed in 0.2s",
            )

        def coverage_report(self, args: list[str], verbose: bool = False) -> ToolResult:
            return _tool_result(
                category="test",
                command="coverage-report",
                success=True,
                stdout="TOTAL 95% 80%",
            )

    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools
    batch_review_module.QualityTools = StubQualityTools
    batch_review_module.TestingTools = StubTestingTools
    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        payload = json.loads(result.stdout)

        # Verify structure and expected results
        assert "quality_checks" in payload
        assert "test_summary" in payload
        assert "overall_status" in payload
        assert "timestamp" in payload
        assert "project_root" in payload

        # Verify overall success
        assert payload["overall_status"]["success"] is True
        assert result.success is True and result.exit_code == 0
    finally:
        batch_review_module.QualityTools = original_quality
        batch_review_module.TestingTools = original_testing


def test_run_batch_review_supports_yaml_and_text(tmp_path: Path) -> None:
    """YAML and text formats should be supported with deterministic output."""

    # Create mock tools that return failed quality results to test failure case
    class FailingQualityTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def lint(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="lint",
                success=False,
                stderr="lint error",
            )

        def typecheck(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="typecheck",
                success=True,
                stdout="Type check passed",
            )

        def deadcode(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="No dead code",
            )

    class StubTestingTools:
        def __init__(self, *_: object, **__: object) -> None:  # noqa: D401, ANN401
            pass

        def unit(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="unit",
                success=True,
                stdout="1 passed in 0.1s",
            )

        def integration(self, args: list[str]) -> ToolResult:
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="2 passed in 0.2s",
            )

        def coverage_report(self, args: list[str], verbose: bool = False) -> ToolResult:
            return _tool_result(
                category="test",
                command="coverage-report",
                success=True,
                stdout="TOTAL 95% 80%",
            )

    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools
    original_yaml = sys.modules.get("yaml")

    batch_review_module.QualityTools = FailingQualityTools
    batch_review_module.TestingTools = StubTestingTools

    def _yaml_dump(data: Any, default_flow_style: bool = False) -> str:
        return "yaml-output"

    yaml_stub = types.SimpleNamespace(dump=_yaml_dump)
    sys.modules["yaml"] = yaml_stub  # type: ignore[assignment]

    try:
        yaml_result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="yaml"
        )
        assert yaml_result.stdout == "yaml-output"
        assert yaml_result.success is False  # Due to lint failure

        text_result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="text"
        )
        assert "Quality Checks" in text_result.stdout
        assert "Overall Status" in text_result.stdout
        assert text_result.success is False and text_result.exit_code == 1
    finally:
        batch_review_module.QualityTools = original_quality
        batch_review_module.TestingTools = original_testing
        if original_yaml is not None:
            sys.modules["yaml"] = original_yaml
        else:
            del sys.modules["yaml"]


def test_batch_review_error_downgrade_behavior(tmp_path: Path) -> None:
    """Test that exceptions are properly downgraded to structured error responses.

    This test verifies the contract: run_batch_review always returns success=True
    but individual tool failures are captured as structured "error" entries instead
    of propagating as exceptions.
    """
    from ml_playground.tools.core.errors import (
        ToolExecutionError,
        TimeoutError,
        CommandNotFoundError,
    )

    # Mock quality tools to raise different types of exceptions
    class _QualityToolsWithExceptions:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> ToolResult:
            raise ToolExecutionError(
                "Lint tool execution failed",
                reason="External tool binary failed to execute",
                rationale="Tool execution failures indicate environment issues",
            )

        def typecheck(self, _args: list[str]) -> ToolResult:
            raise TimeoutError(
                "Typecheck timed out",
                reason="Process exceeded timeout limit",
                rationale="Timeouts indicate performance issues",
            )

        def deadcode(self, _args: list[str]) -> ToolResult:
            raise CommandNotFoundError(
                "Deadcode tool not found",
                reason="Tool binary not in PATH",
                rationale="Missing tools indicate setup issues",
            )

    # Mock testing tools to raise exceptions
    class _TestingToolsWithExceptions:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def unit(self, _args: list[str]) -> ToolResult:
            raise RuntimeError("Unit test framework failed")

        def integration(self, _args: list[str]) -> ToolResult:
            raise ValueError("Integration test setup failed")

        def coverage_report(self, *_args: Any, **_kwargs: Any) -> ToolResult:
            raise ImportError("Coverage module not available")

    # Apply mocks
    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools

    batch_review_module.QualityTools = _QualityToolsWithExceptions  # type: ignore[assignment]
    batch_review_module.TestingTools = _TestingToolsWithExceptions  # type: ignore[assignment]

    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Main contract: batch review doesn't crash but reflects actual status
        assert result.success is False  # Should be False due to all the errors
        assert result.exit_code == 1

        # Parse JSON to verify structured error responses
        import json

        payload = json.loads(result.stdout)

        # Quality checks should show "error" status for each tool
        quality_checks = payload["quality_checks"]
        assert quality_checks["lint"]["status"] == "error"
        assert "execution failed" in quality_checks["lint"]["error"].lower()

        assert quality_checks["typecheck"]["status"] == "error"
        assert "timeout" in quality_checks["typecheck"]["error"].lower()

        assert quality_checks["deadcode"]["status"] == "error"
        assert "not found" in quality_checks["deadcode"]["error"].lower()

        assert quality_checks["overall"]["status"] == "failed"
        assert quality_checks["overall"]["success"] is False

        # Test summary should show "error" status for each tool
        test_summary = payload["test_summary"]
        assert test_summary["unit"]["status"] == "error"
        assert "framework failed" in test_summary["unit"]["error"].lower()

        assert test_summary["integration"]["status"] == "error"
        assert "setup failed" in test_summary["integration"]["error"].lower()

        assert test_summary["coverage"]["status"] == "not_available"  # No coverage file

        assert test_summary["overall"]["status"] == "failed"
        assert test_summary["overall"]["success"] is False

        # Overall status should reflect failures
        overall_status = payload["overall_status"]
        assert overall_status["success"] is False
        assert overall_status["quality_status"] == "failed"
        assert overall_status["test_status"] == "failed"
        assert overall_status["ready_for_merge"] is False

    finally:
        # Restore originals
        if original_quality:
            batch_review_module.QualityTools = original_quality
        if original_testing:
            batch_review_module.TestingTools = original_testing


def test_batch_review_mixed_structured_failures_and_exceptions(tmp_path: Path) -> None:
    """Test batch review with mix of structured failures and exceptions.

    Verifies that structured failures (success=False) and exceptions are both
    properly handled and downgraded to appropriate status codes.
    """
    # Create a coverage file to trigger coverage_report call
    coverage_file = tmp_path / ".cache" / "coverage" / "coverage.sqlite"
    coverage_file.parent.mkdir(parents=True, exist_ok=True)
    coverage_file.write_text("data", encoding="utf-8")

    # Mock quality tools with mixed success/failure/exception patterns
    class _QualityToolsMixed:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> ToolResult:
            # Structured failure
            return _tool_result(
                category="quality",
                command="lint",
                success=False,
                stderr="lint error 1\nlint error 2",
            )

        def typecheck(self, _args: list[str]) -> ToolResult:
            # Exception
            raise RuntimeError("Typecheck crashed")

        def deadcode(self, _args: list[str]) -> ToolResult:
            # Structured success
            return _tool_result(
                category="quality",
                command="deadcode",
                success=True,
                stdout="no dead code found",
            )

    # Mock testing tools with mixed patterns
    class _TestingToolsMixed:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def unit(self, _args: list[str]) -> ToolResult:
            # Structured failure
            return _tool_result(
                category="test",
                command="unit",
                success=False,
                stdout="1 passed, 1 failed",
                stderr="test failure details",
            )

        def integration(self, _args: list[str]) -> ToolResult:
            # Structured success
            return _tool_result(
                category="test",
                command="integration",
                success=True,
                stdout="3 passed in 0.5s",
            )

        def coverage_report(self, *_args: Any, **_kwargs: Any) -> ToolResult:
            # Exception
            raise ValueError("Coverage parsing failed")

    # Apply mocks
    original_quality = batch_review_module.QualityTools
    original_testing = batch_review_module.TestingTools

    batch_review_module.QualityTools = _QualityToolsMixed  # type: ignore[assignment]
    batch_review_module.TestingTools = _TestingToolsMixed  # type: ignore[assignment]

    try:
        result = batch_review_module.run_batch_review(
            ToolsConfig(), tmp_path, output_format="json"
        )

        # Main contract: doesn't crash but reflects actual status
        assert result.success is False  # Should be False due to failures/errors

        # Parse and verify mixed handling
        import json

        payload = json.loads(result.stdout)

        # Quality checks: mix of failed, error, passed
        quality_checks = payload["quality_checks"]
        assert quality_checks["lint"]["status"] == "failed"  # structured failure
        assert quality_checks["lint"]["issues"] == 2

        assert quality_checks["typecheck"]["status"] == "error"  # exception
        assert "crashed" in quality_checks["typecheck"]["error"].lower()

        assert quality_checks["deadcode"]["status"] == "passed"  # structured success
        assert quality_checks["overall"]["status"] == "failed"  # some failures

        # Test summary: mix of failed, passed, error
        test_summary = payload["test_summary"]
        assert test_summary["unit"]["status"] == "failed"  # structured failure
        assert (
            test_summary["unit"]["count"] == 1
        )  # Only counts passed tests, not failed

        assert test_summary["integration"]["status"] == "passed"  # structured success
        assert test_summary["integration"]["count"] == 3

        assert test_summary["coverage"]["status"] == "error"  # exception
        assert "parsing failed" in test_summary["coverage"]["error"].lower()

        assert test_summary["overall"]["status"] == "failed"  # some failures

        # Overall status should reflect the mixed results
        overall_status = payload["overall_status"]
        assert overall_status["success"] is False
        assert overall_status["quality_status"] == "failed"
        assert overall_status["test_status"] == "failed"
        assert overall_status["ready_for_merge"] is False

    finally:
        # Restore originals
        if original_quality:
            batch_review_module.QualityTools = original_quality
        if original_testing:
            batch_review_module.TestingTools = original_testing
