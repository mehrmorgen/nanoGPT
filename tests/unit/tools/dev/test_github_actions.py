from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.dev.github_actions import run_github_actions
from tests.unit.tools.fakes import FakeSubprocessRunner


def _ok(*, op: OperationId, stdout: str) -> ToolResult:
    return ToolResult(
        success=True, exit_code=0, stdout=stdout, stderr="", operation_id=op
    )


def _fail(*, op: OperationId, stderr: str) -> ToolResult:
    return ToolResult(
        success=False, exit_code=1, stdout="", stderr=stderr, operation_id=op
    )


def test_run_github_actions_repo_from_git_remote_latest_view_success(
    tmp_path: Path,
) -> None:
    runner = FakeSubprocessRunner()

    # 1) infer repo via git remote
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stdout="https://github.com/owner/repo.git\n",
        )
    )

    # 2) gh run list
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="RUNS\n",
        )
    )

    # 3) gh run list for latest id
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="123\n",
        )
    )

    # 4) gh run view
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="VIEW\n",
        )
    )

    result = run_github_actions(
        root_path=tmp_path,
        subprocess_runner=runner,
        limit=0,
        run_id=None,
        latest=True,
        log_failed=True,
        remote="origin",
        repo=None,
    )

    assert result.success is True
    assert result.exit_code == 0
    assert result.stdout is not None
    assert "== gh run list ==" in result.stdout
    assert "RUNS" in result.stdout
    assert "== gh run view ==" in result.stdout
    assert "VIEW" in result.stdout

    assert runner.calls[0]["command"] == ["git", "remote", "get-url", "origin"]
    assert runner.calls[1]["command"] == [
        "gh",
        "run",
        "list",
        "--repo",
        "owner/repo",
        "-L",
        "10",
    ]
    assert runner.calls[2]["command"] == [
        "gh",
        "run",
        "list",
        "--repo",
        "owner/repo",
        "-L",
        "1",
        "--json",
        "databaseId",
        "-q",
        ".[0].databaseId",
    ]
    assert runner.calls[3]["command"] == [
        "gh",
        "run",
        "view",
        "123",
        "--repo",
        "owner/repo",
        "--log-failed",
    ]


def test_run_github_actions_latest_id_parse_failure_is_stable(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stdout="git@github.com:owner/repo.git\n",
        )
    )
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="RUNS\n",
        )
    )
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="not-an-int\n",
        )
    )

    result = run_github_actions(
        root_path=tmp_path,
        subprocess_runner=runner,
        limit=5,
        run_id=None,
        latest=True,
        log_failed=False,
        remote="origin",
        repo=None,
    )

    assert result.success is False
    assert result.exit_code == 1
    assert result.stderr is not None
    assert "Failed to parse latest workflow run id" in result.stderr
    assert "not-an-int" in result.stderr


def test_infer_repo_fallback_to_gh_repo_view(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    runner.add_result(
        _fail(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stderr="no remote",
        )
    )
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stdout="owner/repo\n",
        )
    )
    runner.add_result(
        _ok(
            op=OperationId(namespace="tools", category="dev", command="gha"),
            stdout="RUNS\n",
        )
    )

    result = run_github_actions(
        root_path=tmp_path,
        subprocess_runner=runner,
        limit=5,
        run_id=None,
        latest=False,
        log_failed=False,
        remote="origin",
        repo=None,
    )

    assert result.success is True
    assert result.exit_code == 0

    assert runner.calls[0]["command"] == ["git", "remote", "get-url", "origin"]
    assert runner.calls[1]["command"][:3] == ["gh", "repo", "view"]
    assert runner.calls[2]["command"][:3] == ["gh", "run", "list"]
    assert "--repo" in runner.calls[2]["command"]
    assert "owner/repo" in runner.calls[2]["command"]


def test_infer_repo_all_fail_raises_tool_execution_error(tmp_path: Path) -> None:
    runner = FakeSubprocessRunner()

    runner.add_result(
        _fail(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stderr="no remote",
        )
    )
    runner.add_result(
        _fail(
            op=OperationId(namespace="tools", category="dev", command="gha-infer-repo"),
            stderr="gh unavailable",
        )
    )

    with pytest.raises(ToolExecutionError, match="Failed to infer repository"):
        run_github_actions(
            root_path=tmp_path,
            subprocess_runner=runner,
            limit=5,
            run_id=None,
            latest=False,
            log_failed=False,
            remote="origin",
            repo=None,
        )
