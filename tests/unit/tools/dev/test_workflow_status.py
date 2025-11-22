# pyright: reportPrivateUsage=false
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from ml_playground.tools.core.config import ToolsConfig
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
        raise RuntimeError("boom")

    original = ws._run_quality_batch
    ws._run_quality_batch = failing_quality  # type: ignore[assignment]
    status = ws._get_quality_status(cfg, tmp_path, _StubRunner())
    ws._run_quality_batch = original  # type: ignore[assignment]
    assert status["status"] == "unknown"
    assert "error" in status


def test_get_test_status_handles_exceptions(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()

    def failing_test(*_: Any, **__: Any) -> dict[str, Any]:
        raise RuntimeError("boom")

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
            raise RuntimeError("boom")

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
            raise RuntimeError("deadcode failure")

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
            raise RuntimeError("integration unavailable")

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
