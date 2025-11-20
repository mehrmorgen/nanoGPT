from __future__ import annotations

from pathlib import Path
from typing import Any

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev import workflow_status as ws
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


class _StubRunner(SubprocessRunner):
    def __init__(self, branch: str = "main", dirty: bool = False, fail: bool = False) -> None:
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
        return ToolResult(success=True, exit_code=0, stdout="", stderr="", operation_id=operation_id)

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
        return ToolResult(success=True, exit_code=0, stdout="1 passed in 0.01s", stderr="", operation_id=operation_id)


def _minimal_tools_config() -> ToolsConfig:
    return ToolsConfig()


def test_run_workflow_status_json_happy_path(tmp_path: Path) -> None:
    cfg = _minimal_tools_config()
    runner = _StubRunner(branch="feature", dirty=False)

    # Patch helpers to avoid real quality/tests/coverage work.
    def fake_quality_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {"overall_status": "passed", "issues_count": 0}

    def fake_test_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {"overall_status": "passed", "total_tests": 5}

    def fake_coverage_status(*_: Any, **__: Any) -> dict[str, Any]:
        return {"status": "available", "line_percentage": 91.0, "branch_percentage": 83.0}

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
            operation_id=OperationId(namespace="tools", category="dev", command="git-status"),
        )
        status_result = runner.run_subprocess(
            ["git", "status", "--porcelain"],
            cwd=root,
            timeout=10,
            operation_id=OperationId(namespace="tools", category="dev", command="git-status"),
        )
        return {
            "current_branch": branch_result.stdout.strip() if branch_result.success else "unknown",
            "has_changes": bool(status_result.stdout.strip()) if status_result.success else False,
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
    assert "\"project_root\"" in result.stdout
    assert "\"git_status\"" in result.stdout
    assert "\"quality_status\"" in result.stdout
    assert "\"test_status\"" in result.stdout
    assert "\"coverage_status\"" in result.stdout
    assert "\"readiness\"" in result.stdout


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
