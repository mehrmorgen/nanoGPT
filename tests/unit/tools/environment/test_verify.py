from __future__ import annotations

from pathlib import Path


from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.environment.verify import run_info, run_verify
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_run_verify_success(tmp_path: Path) -> None:
    """Test run_verify happy path."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="✓ ml_playground import OK",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="verify"
            ),
        )
    )

    result = run_verify(config, tmp_path, "ml_playground", [], runner)

    assert result.success is True
    assert "import OK" in result.stdout
    assert len(runner.calls) == 1
    cmd = runner.calls[0]["command"]
    assert "python" in cmd
    assert "-c" in cmd
    assert "import ml_playground" in cmd[cmd.index("-c") + 1]
    assert "missing = [name for name in required" in cmd[cmd.index("-c") + 1]


def test_run_verify_missing_toolchain_includes_remediation(tmp_path: Path) -> None:
    """Test run_verify failure includes actionable remediation for missing tools."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="✓ ml_playground import OK\nmissing: yamlfix",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="verify"
            ),
        )
    )

    result = run_verify(config, tmp_path, "ml_playground", [], runner)

    assert result.success is False
    assert "missing: yamlfix" in (result.stdout or "")
    assert "uv sync --group all" in (result.stderr or "")
    assert "uv run tools env setup --clear" in (result.stderr or "")


def test_run_info_full_success(tmp_path: Path) -> None:
    """Test run_info with venv, cache, and working import."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="OK",
            stderr="",
            operation_id=OperationId(namespace="tools", category="env", command="info"),
        )
    )

    venv_path = tmp_path / ".venv"
    venv_path.mkdir()
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir()
    (cache_dir / "file").write_text("content")

    result = run_info(
        config,
        tmp_path,
        "ml_playground",
        venv_path,
        cache_dir,
        [],
        runner,
    )

    assert result.success is True
    assert f"Project root: {tmp_path}" in result.stdout
    assert "Virtual environment: " in result.stdout
    assert "(exists)" in result.stdout
    assert "Cache directory: " in result.stdout
    assert "MB)" in result.stdout
    assert "✓ ml_playground imports successfully" in result.stdout


def test_run_info_missing_paths(tmp_path: Path) -> None:
    """Test run_info with missing venv and cache."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    # Import check still runs
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="OK",
            stderr="",
            operation_id=OperationId(namespace="tools", category="env", command="info"),
        )
    )

    venv_path = tmp_path / ".venv"
    cache_dir = tmp_path / ".cache"

    result = run_info(
        config,
        tmp_path,
        "ml_playground",
        venv_path,
        cache_dir,
        [],
        runner,
    )

    assert result.success is True
    assert "Virtual environment: " in result.stdout
    assert "(missing)" in result.stdout
    assert "Cache directory: " in result.stdout
    assert "(missing)" in result.stdout


def test_run_info_import_failure(tmp_path: Path) -> None:
    """Test run_info with import failure."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="ImportError: no module named ml_playground",
            operation_id=OperationId(namespace="tools", category="env", command="info"),
        )
    )

    venv_path = tmp_path / ".venv"
    cache_dir = tmp_path / ".cache"

    result = run_info(
        config,
        tmp_path,
        "ml_playground",
        venv_path,
        cache_dir,
        [],
        runner,
    )

    assert result.success is True  # info command itself succeeds
    assert "✗ ml_playground import failed" in result.stdout


def test_run_info_import_exception(tmp_path: Path) -> None:
    """Test run_info handles exceptions during import check."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    def raise_error(op_id: OperationId) -> ToolResult:
        raise OSError("Process failed to start")

    runner.queue_result_factory(raise_error)

    venv_path = tmp_path / ".venv"
    cache_dir = tmp_path / ".cache"

    result = run_info(
        config,
        tmp_path,
        "ml_playground",
        venv_path,
        cache_dir,
        [],
        runner,
    )

    assert result.success is True
    assert "✗ Could not test ml_playground import" in result.stdout
