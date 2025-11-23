from __future__ import annotations

from pathlib import Path

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.environment.setup import run_setup, run_sync
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_run_setup_success(tmp_path: Path) -> None:
    """Test successful setup."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    # 1. venv
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="venv created",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )
    # 2. sync
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="sync done",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )

    venv_path = tmp_path / ".venv"
    result = run_setup(config, tmp_path, venv_path, "pkg", [], False, runner)

    assert result.success is True
    assert "Created virtual environment" in result.stdout
    assert "Synchronized all dependency groups" in result.stdout
    assert "Configured git hooks" in result.stdout


def test_run_setup_clear_success(tmp_path: Path) -> None:
    """Test setup with clear=True."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    venv_path = tmp_path / ".venv"
    venv_path.mkdir()
    (venv_path / "bin").mkdir()

    # 1. venv
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="venv created",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )
    # 2. sync
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="sync done",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )

    result = run_setup(config, tmp_path, venv_path, "pkg", [], True, runner)

    assert result.success is True
    assert not venv_path.exists()  # shutil.rmtree removed it
    assert "Removed existing virtual environment" in result.stdout
    assert runner.calls[0]["command"] == [
        "uv",
        "run",
        "--project",
        str(tmp_path),
        "venv",
        "--clear",
    ]


def test_run_setup_venv_failure(tmp_path: Path) -> None:
    """Test setup fails if venv creation fails."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="venv failed",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )

    venv_path = tmp_path / ".venv"
    result = run_setup(config, tmp_path, venv_path, "pkg", [], False, runner)

    assert result.success is False
    assert result.stderr == "venv failed"


def test_run_setup_sync_failure(tmp_path: Path) -> None:
    """Test setup fails if sync fails."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    # 1. venv ok
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="venv ok",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )
    # 2. sync fail
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="sync failed",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )

    venv_path = tmp_path / ".venv"
    result = run_setup(config, tmp_path, venv_path, "pkg", [], False, runner)

    assert result.success is False
    assert "sync failed" in result.stderr


def test_run_setup_git_hooks_warning(tmp_path: Path) -> None:
    """Test setup warns if git hooks fail but continues."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    # 1. venv
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )
    # 2. sync
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="setup"
            ),
        )
    )

    # Make .git a file with invalid content to trigger exception in _setup_git_hooks
    (tmp_path / ".git").write_text("invalid")

    venv_path = tmp_path / ".venv"

    result = run_setup(config, tmp_path, venv_path, "pkg", [], False, runner)

    assert result.success is True
    assert "Warning: Git hooks setup failed" in result.stdout


def test_run_sync_combinations(tmp_path: Path) -> None:
    """Test run_sync with various flags."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    # 1. Frozen
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=OperationId(namespace="tools", category="env", command="sync"),
        )
    )
    run_sync(config, tmp_path, [], None, False, True, runner)
    assert "--frozen" in runner.calls[0]["command"]

    # 2. All groups
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=OperationId(namespace="tools", category="env", command="sync"),
        )
    )
    run_sync(config, tmp_path, [], None, True, False, runner)
    assert "--all-groups" in runner.calls[1]["command"]

    # 3. Specific groups
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="",
            stderr="",
            operation_id=OperationId(namespace="tools", category="env", command="sync"),
        )
    )
    run_sync(config, tmp_path, [], ["dev", "test"], False, False, runner)
    cmd = runner.calls[2]["command"]
    assert "--group" in cmd
    assert "dev" in cmd
    assert "test" in cmd
