# pyright: reportPrivateUsage=false
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import (
    ToolConfigurationError,
    ToolExecutionError,
    ToolTimeoutError,
)
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev import workflow_status as ws
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


class _StubRunner(SubprocessRunner):
    def __init__(
        self, branch: str = "main", dirty: bool = False, fail: bool = False
    ) -> None:
        self.branch = branch
        self.dirty = dirty
        self.fail = fail
        self.subprocess_commands: list[list[str]] = []
        self.uv_commands: list[list[str]] = []
        self.pytest_commands: list[list[str]] = []

    def run_subprocess(  # type: ignore[override]
        self,
        command: list[str],
        *,
        cwd: str | Path | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        env: dict[str, str] | None = None,
        capture_output: bool = True,
    ) -> ToolResult:
        del cwd, timeout, env, capture_output
        self.subprocess_commands.append(command)
        if self.fail:
            raise RuntimeError("git subprocess failed")
        if "branch" in command:
            stdout = self.branch + "\n"
        else:
            stdout = " M file.py\n" if self.dirty else "\n"
        return ToolResult(
            success=True,
            exit_code=0,
            stdout=stdout,
            stderr="",
            operation_id=operation_id,
        )

    def run_uv_command(  # type: ignore[override]
        self,
        command: list[str],
        *,
        cwd: str | Path | None,
        timeout: int | None,
        operation_id: OperationId,
        env: dict[str, str] | None = None,
        capture_output: bool = True,
    ) -> ToolResult:
        del cwd, timeout, env, capture_output
        self.uv_commands.append(command)
        return ToolResult(
            success=True, exit_code=0, stdout="", stderr="", operation_id=operation_id
        )

    def run_pytest_command(  # type: ignore[override]
        self,
        args: list[str],
        *,
        cwd: str | Path | None,
        timeout: int | None,
        operation_id: OperationId,
        env: dict[str, str] | None = None,
    ) -> ToolResult:
        del cwd, timeout, env
        self.pytest_commands.append(args)
        # Minimal successful run to satisfy callers; output is not inspected here.
        return ToolResult(
            success=True,
            exit_code=0,
            stdout="1 passed in 0.01s",
            stderr="",
            operation_id=operation_id,
        )


def _minimal_tools_config() -> ToolsConfig:
    return ToolsConfig()


@dataclass
class _FakeToolResult:
    success: bool
    exit_code: int
    stdout: str = ""
    stderr: str = ""


def test_run_workflow_status_json_happy_path(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner(branch="feature", dirty=False)

    # Patch helpers to avoid real quality/tests/coverage work.
    def fake_quality_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {"overall_status": "passed", "issues_count": 0}

    def fake_test_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {"overall_status": "passed", "total_tests": 5}

    def fake_coverage_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "status": "available",
            "line_percentage": 91.0,
            "branch_percentage": 83.0,
        }

    orig_quality = ws._get_quality_status
    orig_test = ws._get_test_status
    orig_cov = ws._get_coverage_status
    orig_git = ws._get_git_status

    def _fake_git_status(root: Path, _runner: SubprocessRunner) -> dict[str, Any]:
        # Reimplement the small bit of logic we care about without recursion.
        branch_result = runner.run_subprocess(
            ["git", "branch", "--show-current"],
            cwd=root,
            timeout=10,
            operation_id=OperationId(
                namespace="tools", category="dev", command="git-status"
            ),
        )
        status_result = runner.run_subprocess(
            ["git", "status", "--porcelain"],
            cwd=root,
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

    ws._get_quality_status = fake_quality_status  # type: ignore[assignment]
    ws._get_test_status = fake_test_status  # type: ignore[assignment]
    ws._get_coverage_status = fake_coverage_status  # type: ignore[assignment]
    ws._get_git_status = _fake_git_status  # type: ignore[assignment]

    try:
        result = ws.run_workflow_status(
            cfg,
            project_root_path=tmp_path,
            output_format="json",
            subprocess_runner=runner,
        )
    finally:
        ws._get_git_status = orig_git  # type: ignore[assignment]
        ws._get_quality_status = orig_quality  # type: ignore[assignment]
        ws._get_test_status = orig_test  # type: ignore[assignment]
        ws._get_coverage_status = orig_cov  # type: ignore[assignment]

    assert result.success is True
    assert result.exit_code == 0
    assert '"project_root"' in result.stdout
    assert '"git_status"' in result.stdout
    assert '"quality_status"' in result.stdout
    assert '"test_status"' in result.stdout
    assert '"coverage_status"' in result.stdout
    assert '"readiness"' in result.stdout


def test_get_git_status_falls_back_on_error(tmp_path: Path) -> None:
    runner = _StubRunner(fail=True)
    status = ws._get_git_status(tmp_path, runner)
    assert status["status"] == "unknown"
    assert "error" in status


def test_get_quality_status_handles_exceptions(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    def failing_quality(*_: Any, **__: Any) -> dict[str, Any]:
        raise ToolExecutionError("boom", reason="fail", rationale="test")

    original = ws._run_quality_batch
    ws._run_quality_batch = failing_quality  # type: ignore[assignment]
    status = ws._get_quality_status(cfg, tmp_path, _StubRunner())
    ws._run_quality_batch = original  # type: ignore[assignment]
    assert status["status"] == "unknown"
    assert "error" in status


def test_get_test_status_handles_exceptions(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    def failing_test(*_: Any, **__: Any) -> dict[str, Any]:
        raise ToolExecutionError("boom", reason="fail", rationale="test")

    original = ws._run_test_batch_simple
    ws._run_test_batch_simple = failing_test  # type: ignore[assignment]
    status = ws._get_test_status(cfg, tmp_path, _StubRunner())
    ws._run_test_batch_simple = original  # type: ignore[assignment]
    assert status["status"] == "unknown"
    assert "error" in status


def test_get_coverage_status_handles_missing_file(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    status = ws._get_coverage_status(cfg, tmp_path, runner)
    assert status["status"] == "not_available"
    assert "Run coverage-test" in status["message"]


def test_get_coverage_status_reports_available_data(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    coverage_root = tmp_path / ".cache" / "coverage"
    coverage_root.mkdir(parents=True)
    (coverage_root / "coverage.sqlite").write_text("sqlite-placeholder")

    class _FakeTestingTools:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.coverage_calls: list[list[str]] = []

        def coverage_report(self, args: list[str], *, verbose: bool) -> ToolResult:
            self.coverage_calls.append(args)
            del verbose
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="TOTAL 90% 85%",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="dev", command="coverage"
                ),
            )

    original = ws.TestingTools
    ws.TestingTools = _FakeTestingTools  # type: ignore[assignment]
    try:
        status = ws._get_coverage_status(cfg, tmp_path, runner)
    finally:
        ws.TestingTools = original  # type: ignore[assignment]

    assert status["status"] == "available"
    assert status["line_percentage"] == 90.0
    assert status["branch_percentage"] == 85.0


def test_get_coverage_status_handles_exceptions(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    coverage_root = tmp_path / ".cache" / "coverage"
    coverage_root.mkdir(parents=True)
    (coverage_root / "coverage.sqlite").write_text("sqlite-placeholder")

    class _ExplodingTestingTools:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def coverage_report(self, *_args: Any, **_kwargs: Any) -> ToolResult:
            raise ToolExecutionError("boom", reason="fail", rationale="test")

    original = ws.TestingTools
    ws.TestingTools = _ExplodingTestingTools  # type: ignore[assignment]
    try:
        status = ws._get_coverage_status(cfg, tmp_path, runner)
    finally:
        ws.TestingTools = original  # type: ignore[assignment]

    assert status["status"] == "unknown"
    assert "error" in status


def test_get_blocking_issues_reports_all_sources() -> None:
    issues = ws._get_blocking_issues(
        {"overall_status": "failed", "issues_count": 3},
        {"overall_status": "failed"},
        {"has_changes": True},
    )
    assert "Quality checks failing" in issues[0]
    assert any("Test failures" in issue for issue in issues)
    assert any("Uncommitted changes" in issue for issue in issues)


def test_run_quality_batch_handles_failures_and_errors(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    class _QualityToolsStub:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=False,
                exit_code=1,
                stdout="lint output",
                stderr="issue-one\nissue-two\n",
            )

        def typecheck(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="type-error\n",
            )

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            raise ToolExecutionError(
                "deadcode failure", reason="fail", rationale="test"
            )

    module_name = "ml_playground.tools.quality.quality"
    original_module = sys.modules.get(module_name)
    stub_module = ModuleType(module_name)
    stub_module.QualityTools = _QualityToolsStub  # type: ignore[attr-defined]
    sys.modules[module_name] = stub_module
    try:
        summary = ws._run_quality_batch(cfg, tmp_path, runner)
    finally:
        if original_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original_module

    assert summary["overall"]["status"] == "failed"
    assert summary["lint"]["status"] == "failed"
    assert summary["typecheck"]["status"] == "failed"
    assert summary["deadcode"]["status"] == "error"
    assert summary["overall"]["total_issues"] > 0


def test_run_quality_batch_lint_and_typecheck_exceptions(tmp_path: Path) -> None:
    """Lint and typecheck exceptions should be captured as errors."""

    class _QualityToolsExceptions:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> Any:
            raise ToolTimeoutError("lint timeout", reason="timeout", rationale="test")

        def typecheck(self, _args: list[str]) -> Any:
            raise ToolConfigurationError(
                "bad config", reason="invalid", rationale="test"
            )

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

    module_name = "ml_playground.tools.quality.quality"
    original_module = sys.modules.get(module_name)
    stub_module = ModuleType(module_name)
    stub_module.QualityTools = _QualityToolsExceptions  # type: ignore[attr-defined]
    sys.modules[module_name] = stub_module
    try:
        summary = ws._run_quality_batch(
            _minimal_tools_config(), tmp_path, _StubRunner()
        )
    finally:
        if original_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original_module

    assert summary["lint"]["status"] == "error"
    assert summary["typecheck"]["status"] == "error"
    assert summary["deadcode"]["status"] == "passed"
    assert summary["overall"]["status"] == "failed"


def test_run_test_batch_simple_handles_failures_and_exceptions(
    tmp_path: Path,
) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    class _TestingToolsStub:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def unit(self, args: list[str]) -> _FakeToolResult:
            assert "-q" in args
            return _FakeToolResult(
                success=False,
                exit_code=1,
                stdout="2 passed in 0.50s",
                stderr="",
            )

        def integration(self, _args: list[str]) -> _FakeToolResult:
            raise ToolExecutionError(
                "integration unavailable", reason="fail", rationale="test"
            )

    original = ws.TestingTools
    ws.TestingTools = _TestingToolsStub  # type: ignore[assignment]
    try:
        results = ws._run_test_batch_simple(cfg, tmp_path, runner)
    finally:
        ws.TestingTools = original  # type: ignore[assignment]
    assert results["unit"]["status"] == "failed"
    assert results["overall"]["status"] == "failed"
    assert results["unit"]["count"] == 2
    assert results["unit"]["duration"] == "0.50s"
    assert results["integration"]["status"] == "error"


def test_run_test_batch_simple_successful_paths(tmp_path: Path) -> None:
    """Successful unit+integration runs should aggregate counts and durations."""
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    class _HappyTestingTools:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def unit(self, args: list[str]) -> _FakeToolResult:
            assert "-q" in args
            return _FakeToolResult(
                success=True, exit_code=0, stdout="2 passed in 0.20s", stderr=""
            )

        def integration(self, args: list[str]) -> _FakeToolResult:
            assert "-q" in args
            return _FakeToolResult(
                success=True, exit_code=0, stdout="3 passed in 0.30s", stderr=""
            )

    original = ws.TestingTools
    ws.TestingTools = _HappyTestingTools  # type: ignore[assignment]
    try:
        results = ws._run_test_batch_simple(cfg, tmp_path, runner)
    finally:
        ws.TestingTools = original  # type: ignore[assignment]

    assert results["overall"]["status"] == "passed"
    assert results["overall"]["total_tests"] == 5
    assert results["unit"]["count"] == 2
    assert results["integration"]["count"] == 3
    assert results["integration"]["duration"] == "0.30s"


def test_run_quality_batch_typecheck_failed_increments_issues(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    class _QualityToolsTypecheckFail:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

        def typecheck(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="e1\ne2\n",
            )

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

    module_name = "ml_playground.tools.quality.quality"
    original_module = sys.modules.get(module_name)
    stub_module = ModuleType(module_name)
    stub_module.QualityTools = _QualityToolsTypecheckFail  # type: ignore[attr-defined]
    sys.modules[module_name] = stub_module
    try:
        summary = ws._run_quality_batch(cfg, tmp_path, _StubRunner())
    finally:
        if original_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original_module

    assert summary["typecheck"]["status"] == "failed"
    assert summary["overall"]["status"] == "failed"
    assert summary["overall"]["total_issues"] >= 2


def test_run_quality_batch_typecheck_success_covers_else_branch(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    class _QualityToolsAllPass:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

        def typecheck(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(success=True, exit_code=0, stdout="", stderr="")

    module_name = "ml_playground.tools.quality.quality"
    original_module = sys.modules.get(module_name)
    stub_module = ModuleType(module_name)
    stub_module.QualityTools = _QualityToolsAllPass  # type: ignore[attr-defined]
    sys.modules[module_name] = stub_module
    try:
        summary = ws._run_quality_batch(cfg, tmp_path, _StubRunner())
    finally:
        if original_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original_module

    assert summary["overall"]["status"] == "passed"


def test_run_test_batch_simple_unit_exception_sets_error(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    class _TestingToolsUnitBoom:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def unit(self, _args: list[str]) -> _FakeToolResult:
            raise ToolExecutionError("boom", reason="fail", rationale="test")

        def integration(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=True, exit_code=0, stdout="1 passed in 0.1s", stderr=""
            )

    original = ws.TestingTools
    ws.TestingTools = _TestingToolsUnitBoom  # type: ignore[assignment]
    try:
        results = ws._run_test_batch_simple(cfg, tmp_path, _StubRunner())
    finally:
        ws.TestingTools = original  # type: ignore[assignment]

    assert results["unit"]["status"] == "error"
    assert results["overall"]["status"] == "failed"


def test_format_status_text_output_lists_sections() -> None:
    text = ws._format_status_text_output(
        {
            "timestamp": "2024-01-01T00:00:00",
            "git_status": {"current_branch": "main", "status": "clean"},
            "quality_status": {"overall_status": "passed"},
            "test_status": {"overall_status": "failed", "total_tests": 10},
            "coverage_status": {
                "status": "available",
                "line_percentage": 92.0,
                "branch_percentage": 84.0,
            },
            "readiness": {
                "ready_for_merge": False,
                "blocking_issues": ["Needs tests"],
            },
        }
    )

    assert "Workflow Status" in text
    assert "Git:" in text
    assert "Quality:" in text
    assert "Tests:" in text
    assert "Coverage:" in text
    assert "Blocking issues" in text


def test_format_status_text_output_handles_missing_coverage() -> None:
    """Coverage status not available should render status string."""
    text = ws._format_status_text_output(
        {
            "timestamp": "2024-01-01T00:00:00",
            "git_status": {"current_branch": "dev", "status": "dirty"},
            "quality_status": {"overall_status": "failed"},
            "test_status": {"overall_status": "unknown", "total_tests": 0},
            "coverage_status": {"status": "not_available"},
            "readiness": {"ready_for_merge": False, "blocking_issues": []},
        }
    )

    assert "Coverage: not_available" in text


def test_workflow_status_handles_structured_failures_vs_typed_exceptions(
    tmp_path: Path,
) -> None:
    """Test that workflow_status properly handles both structured failures and typed exceptions.

    This test ensures the contract is maintained: run_workflow_status always returns
    success=True, but individual status checks can show 'failed' vs 'error' states.
    """
    from ml_playground.tools.core.errors import (
        ToolExecutionError,
        ToolTimeoutError,
        CommandNotFoundError,
    )

    cfg = _minimal_tools_config()

    # Create a runner that simulates different failure modes
    class _MixedFailureRunner(_StubRunner):
        def run_subprocess(  # type: ignore[override]
            self,
            command: list[str],
            *,
            cwd: str | Path | None = None,
            timeout: int | None = None,
            operation_id: OperationId,
            env: dict[str, str] | None = None,
            capture_output: bool = True,
        ) -> ToolResult:
            del cwd, timeout, env, capture_output
            if "git" in command:
                # Git raises typed exception to simulate failure
                raise ToolExecutionError(
                    "Git command failed",
                    reason="Git subprocess execution failed",
                    rationale="Git operations must succeed for workflow status",
                )
            else:
                # Other commands raise typed exceptions
                raise ToolExecutionError(
                    "Tool execution failed", reason="Failure", rationale="Rationale"
                )

    # Mock quality tools to return structured failures
    class _QualityToolsWithFailures:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=False, exit_code=1, stdout="", stderr="lint errors"
            )

        def typecheck(self, _args: list[str]) -> _FakeToolResult:
            raise ToolTimeoutError(
                "Typecheck timed out",
                reason="Process exceeded the configured timeout limit",
                rationale="Timeouts indicate environmental assumptions are wrong",
            )

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            raise CommandNotFoundError(
                "Deadcode tool not found",
                reason="External tool binary is not available in PATH",
                rationale="All required tools must be installed and accessible for the development workflow to function",
            )

    # Mock testing tools to mix failures and exceptions
    class _TestingToolsWithFailures:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def unit(self, _args: list[str]) -> _FakeToolResult:
            return _FakeToolResult(
                success=False, exit_code=1, stdout="1 passed", stderr=""
            )

        def integration(self, _args: list[str]) -> _FakeToolResult:
            raise ToolExecutionError(
                "Integration tests failed", reason="fail", rationale="test"
            )

    # Apply mocks
    import ml_playground.tools.dev.workflow_status as ws_module
    import ml_playground.tools.quality.quality as quality_module

    original_quality = getattr(quality_module, "QualityTools", None)
    original_testing = getattr(ws_module, "TestingTools", None)

    quality_module.QualityTools = _QualityToolsWithFailures  # type: ignore[attr-defined]
    ws_module.TestingTools = _TestingToolsWithFailures  # type: ignore[assignment]

    try:
        result = ws.run_workflow_status(
            cfg, tmp_path, subprocess_runner=_MixedFailureRunner()
        )

        # Main contract: workflow status always succeeds
        assert result.success is True
        assert result.exit_code == 0

        # Parse the JSON output to verify detailed status
        import json

        status_data = json.loads(result.stdout)

        # Git should show 'unknown' due to structured failure
        assert status_data["git_status"]["status"] == "unknown"

        # Quality should show 'failed' due to structured failures (not 'unknown' because exceptions are caught and converted to structured failures)
        assert status_data["quality_status"]["overall_status"] == "failed"

        # Tests should show 'failed' due to structured failures
        assert status_data["test_status"]["overall_status"] == "failed"

        # Coverage should be 'not_available' (no coverage file)
        assert status_data["coverage_status"]["status"] == "not_available"

        # Readiness should reflect the failures
        assert status_data["readiness"]["ready_for_merge"] is False

    finally:
        # Restore originals
        if original_quality:
            quality_module.QualityTools = original_quality
        if original_testing:
            ws_module.TestingTools = original_testing


def test_run_quality_batch_structured_failures_vs_exceptions(tmp_path: Path) -> None:
    """Test _run_quality_batch distinguishes between structured failures and exceptions."""
    from ml_playground.tools.core.errors import ToolTimeoutError

    cfg = _minimal_tools_config()

    class _QualityToolsMixed:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def lint(self, _args: list[str]) -> _FakeToolResult:
            # Structured failure (success=False)
            return _FakeToolResult(
                success=False, exit_code=1, stdout="", stderr="lint issue"
            )

        def typecheck(self, _args: list[str]) -> _FakeToolResult:
            # Typed exception
            raise ToolTimeoutError(
                "Typecheck timeout",
                reason="Process exceeded the configured timeout limit",
                rationale="Timeouts indicate environmental assumptions are wrong",
            )

        def deadcode(self, _args: list[str]) -> _FakeToolResult:
            # Structured failure (success=False)
            return _FakeToolResult(
                success=False, exit_code=1, stdout="", stderr="dead code"
            )

    import ml_playground.tools.quality.quality as quality_module

    original = getattr(quality_module, "QualityTools", None)
    quality_module.QualityTools = _QualityToolsMixed  # type: ignore[attr-defined]

    try:
        results = ws._run_quality_batch(cfg, tmp_path, _StubRunner())

        # Structured failures should show "failed" status
        assert results["lint"]["status"] == "failed"
        assert results["deadcode"]["status"] == "failed"

        # Typed exceptions should show "error" status
        assert results["typecheck"]["status"] == "error"
        assert "timeout" in results["typecheck"]["error"].lower()

        # Overall should be failed due to failures
        assert results["overall"]["status"] == "failed"
        assert results["overall"]["success"] is False

    finally:
        if original:
            quality_module.QualityTools = original


def test_run_test_batch_simple_structured_failures_vs_exceptions(
    tmp_path: Path,
) -> None:
    """Test _run_test_batch_simple distinguishes between structured failures and exceptions."""
    from ml_playground.tools.core.errors import CommandNotFoundError

    cfg = _minimal_tools_config()

    class _TestingToolsMixed:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def unit(self, _args: list[str]) -> _FakeToolResult:
            # Structured failure (success=False)
            return _FakeToolResult(
                success=False, exit_code=1, stdout="5 passed", stderr=""
            )

        def integration(self, _args: list[str]) -> _FakeToolResult:
            # Typed exception
            raise CommandNotFoundError(
                "Integration tests not available",
                reason="External tool binary is not available in PATH",
                rationale="All required tools must be installed and accessible for the development workflow to function",
            )

    original = ws.TestingTools
    ws.TestingTools = _TestingToolsMixed  # type: ignore[assignment]

    try:
        results = ws._run_test_batch_simple(cfg, tmp_path, _StubRunner())

        # Structured failure should show "failed" status with test data
        assert results["unit"]["status"] == "failed"
        assert results["unit"]["count"] == 5
        assert results["unit"]["duration"] == "0s"  # default from _extract_duration

        # Typed exception should show "error" status
        assert results["integration"]["status"] == "error"
        assert "not available" in results["integration"]["error"].lower()

        # Overall should be failed
        assert results["overall"]["status"] == "failed"
        assert results["overall"]["success"] is False
        assert results["overall"]["total_tests"] == 5  # count from unit tests only

    finally:
        ws.TestingTools = original  # type: ignore[assignment]


def test_run_workflow_status_yaml_output(tmp_path: Path) -> None:
    """Test YAML output format."""
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    # Mock internal helpers to return deterministic data
    orig_quality = ws._get_quality_status
    orig_test = ws._get_test_status
    orig_cov = ws._get_coverage_status
    orig_git = ws._get_git_status

    ws._get_quality_status = lambda *args: {"overall_status": "passed"}  # type: ignore
    ws._get_test_status = lambda *args: {"overall_status": "passed"}  # type: ignore
    ws._get_coverage_status = lambda *args: {"status": "available"}  # type: ignore
    ws._get_git_status = lambda *args: {"status": "clean"}  # type: ignore

    try:
        result = ws.run_workflow_status(
            cfg,
            project_root_path=tmp_path,
            output_format="yaml",
            subprocess_runner=runner,
        )
    except ImportError:
        # If yaml is not installed, it might fall back or error.
        # The code imports yaml inside the function.
        pass
    else:
        assert result.success is True
        assert "overall_status: passed" in result.stdout
        assert "status: clean" in result.stdout
    finally:
        ws._get_quality_status = orig_quality
        ws._get_test_status = orig_test
        ws._get_coverage_status = orig_cov
        ws._get_git_status = orig_git


def test_run_workflow_status_text_output(tmp_path: Path) -> None:
    """Test text output format."""
    cfg = _minimal_tools_config()
    runner = _StubRunner()

    # Mock internal helpers
    orig_quality = ws._get_quality_status
    orig_test = ws._get_test_status
    orig_cov = ws._get_coverage_status
    orig_git = ws._get_git_status

    def _quality_status(*_: Any, **__: Any) -> dict[str, object]:
        return {"overall_status": "passed"}

    def _test_status(*_: Any, **__: Any) -> dict[str, object]:
        return {"overall_status": "passed", "total_tests": 5}

    def _coverage_status(*_: Any, **__: Any) -> dict[str, object]:
        return {"status": "available", "line_percentage": 80.0}

    def _git_status(*_: Any, **__: Any) -> dict[str, object]:
        return {"status": "clean", "current_branch": "main"}

    ws._get_quality_status = _quality_status  # type: ignore[assignment]
    ws._get_test_status = _test_status  # type: ignore[assignment]
    ws._get_coverage_status = _coverage_status  # type: ignore[assignment]
    ws._get_git_status = _git_status  # type: ignore[assignment]

    try:
        result = ws.run_workflow_status(
            cfg,
            project_root_path=tmp_path,
            output_format="text",
            subprocess_runner=runner,
        )

        assert result.success is True
        assert "Workflow Status" in result.stdout
        assert "Quality: ✓ passed" in result.stdout
        assert "Tests: ✓ 5 tests" in result.stdout
    finally:
        ws._get_quality_status = orig_quality
        ws._get_test_status = orig_test
        ws._get_coverage_status = orig_cov
        ws._get_git_status = orig_git


def test_run_workflow_status_default_runner(tmp_path: Path) -> None:
    """Test that default runner is used if not provided."""
    cfg = _minimal_tools_config()

    # We need to mock RealSubprocessRunner to avoid actual execution
    class _MockRealRunner:
        def run_subprocess(self, *args: Any, **kwargs: Any) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="mock",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="test", command="test"
                ),
            )

    import ml_playground.tools.utils.subprocess_utils as subprocess_utils

    orig_runner = getattr(subprocess_utils, "RealSubprocessRunner", None)
    subprocess_utils.RealSubprocessRunner = _MockRealRunner  # type: ignore

    # Mock helpers to avoid other side effects
    orig_quality = ws._get_quality_status
    orig_test = ws._get_test_status
    orig_cov = ws._get_coverage_status
    orig_git = ws._get_git_status

    ws._get_quality_status = lambda *args: {}  # type: ignore
    ws._get_test_status = lambda *args: {}  # type: ignore
    ws._get_coverage_status = lambda *args: {}  # type: ignore
    ws._get_git_status = lambda *args: {}  # type: ignore

    try:
        result = ws.run_workflow_status(
            cfg,
            project_root_path=tmp_path,
            output_format="json",
            subprocess_runner=None,
        )
    finally:
        if orig_runner:
            subprocess_utils.RealSubprocessRunner = orig_runner
        ws._get_quality_status = orig_quality
        ws._get_test_status = orig_test
        ws._get_coverage_status = orig_cov
        ws._get_git_status = orig_git

    assert result.success is True


def test_extraction_helpers_edge_cases() -> None:
    """Test extraction helpers with non-matching input."""
    assert ws._extract_test_count("no matches here") == 0
    assert ws._extract_duration("no matches here") == "0s"
    assert ws._extract_coverage_percentage("no matches", "line") == 0.0
    assert ws._extract_coverage_percentage("no matches", "branch") == 0.0
