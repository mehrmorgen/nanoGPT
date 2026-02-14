from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.environment import services
from tests.unit.tools.fakes import FakeSubprocessRunner


def test_run_ai_guidelines_missing_tool_name(tmp_path: Path) -> None:
    """Test missing tool name validation."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    with pytest.raises(ToolExecutionError, match="Missing tool name"):
        services.run_ai_guidelines(config, tmp_path, [], "", False, runner)


def test_run_ai_guidelines_success(tmp_path: Path) -> None:
    """Test successful AI guidelines setup delegation."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    class FakeDevTools:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def setup_ai_guidelines(self, tool: str, dry_run: bool) -> ToolResult:
            return ToolResult(
                success=True,
                exit_code=0,
                stdout="AI guidelines setup",
                stderr="",
                operation_id=OperationId(
                    namespace="tools", category="dev", command="setup-ai-guidelines"
                ),
            )

    # Patch the import in services module
    mock_module = types.ModuleType("ml_playground.tools.dev.dev")
    mock_module.DevTools = FakeDevTools  # type: ignore

    original_modules = sys.modules.copy()
    sys.modules["ml_playground.tools.dev.dev"] = mock_module

    try:
        result = services.run_ai_guidelines(
            config, tmp_path, [], "pre-commit", False, runner
        )
    finally:
        # Restore modules
        for key in list(sys.modules.keys()):
            if key not in original_modules:
                del sys.modules[key]
        for key, value in original_modules.items():
            sys.modules[key] = value

    assert result.success is True
    assert result.operation_id.command == "ai-guidelines"


def test_run_ai_guidelines_exception(tmp_path: Path) -> None:
    """Test exception handling during delegation."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()

    class FakeDevToolsError:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def setup_ai_guidelines(self, tool: str, dry_run: bool) -> ToolResult:
            raise RuntimeError("Setup failed")

    mock_module = types.ModuleType("ml_playground.tools.dev.dev")
    mock_module.DevTools = FakeDevToolsError  # type: ignore

    original_modules = sys.modules.copy()
    sys.modules["ml_playground.tools.dev.dev"] = mock_module

    try:
        result = services.run_ai_guidelines(
            config, tmp_path, [], "pre-commit", False, runner
        )
    finally:
        # Restore modules
        for key in list(sys.modules.keys()):
            if key not in original_modules:
                del sys.modules[key]
        for key, value in original_modules.items():
            sys.modules[key] = value

    assert result.success is False
    assert "Failed to setup AI guidelines" in result.stderr


def test_run_tensorboard_missing_logdir(tmp_path: Path) -> None:
    """Test validation for missing logdir."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    logdir = tmp_path / "missing"

    with pytest.raises(ToolExecutionError, match="does not exist"):
        services.run_tensorboard(
            config, tmp_path, [], logdir, 6006, "localhost", runner
        )


def test_run_tensorboard_logdir_is_file(tmp_path: Path) -> None:
    """Test validation for logdir being a file."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    logdir = tmp_path / "file"
    logdir.write_text("content")

    with pytest.raises(ToolExecutionError, match="not a directory"):
        services.run_tensorboard(
            config, tmp_path, [], logdir, 6006, "localhost", runner
        )


def test_run_tensorboard_success(tmp_path: Path) -> None:
    """Test successful tensorboard command generation."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    logdir = tmp_path / "logs"
    logdir.mkdir()

    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="TensorBoard started",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="tensorboard"
            ),
        )
    )

    result = services.run_tensorboard(
        config, tmp_path, ["--extra"], logdir, 6006, "localhost", runner
    )

    assert result.success is True
    assert len(runner.calls) == 1
    cmd = runner.calls[0]["command"]
    assert "tensorboard" in cmd
    assert str(logdir) in cmd
    assert "6006" in cmd
    assert "localhost" in cmd
    assert "--extra" in cmd


def test_run_gguf_help_success(tmp_path: Path) -> None:
    """Test successful GGUF help."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=True,
            exit_code=0,
            stdout="usage: convert-hf-to-gguf.py",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="gguf-help"
            ),
        )
    )

    result = services.run_gguf_help(config, tmp_path, [], runner)

    assert result.success is True
    assert "usage:" in result.stdout


def test_run_gguf_help_nonzero_with_usage(tmp_path: Path) -> None:
    """Test GGUF help with non-zero exit code but valid usage output."""
    config = ToolsConfig()
    runner = FakeSubprocessRunner()
    runner.add_result(
        ToolResult(
            success=False,
            exit_code=1,
            stdout="usage: convert-hf-to-gguf.py",
            stderr="",
            operation_id=OperationId(
                namespace="tools", category="env", command="gguf-help"
            ),
        )
    )

    result = services.run_gguf_help(config, tmp_path, [], runner)

    assert result.success is True
    assert result.exit_code == 0
    assert "usage:" in result.stdout
    assert "exit code adjusted" in result.stderr
