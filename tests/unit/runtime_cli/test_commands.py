import logging
import pytest
from pathlib import Path
from typing import Any, cast
from ml_playground.runtime_cli.commands import (
    handle_tool_result,
    log_directory,
    run_prepare_impl,
)
from ml_playground.framework.runtime.core.results import ToolResult
from ml_playground.framework.configuration.models import PreparerConfig


def test_handle_tool_result_success(capsys: object) -> None:
    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="test",
        command="test",
        stdout="Output success",
    )
    handle_tool_result(result, learning_mode=False)
    # Use cast to Any to satisfy strict Pyright access to pytest fixtures
    captured: Any = getattr(capsys, "readouterr")()
    assert "Output success" in captured.out


def test_handle_tool_result_failure() -> None:
    result = ToolResult.create(
        success=False,
        exit_code=42,
        namespace="ml",
        category="test",
        command="test",
        stderr="Output error",
    )
    # typer.Exit inherits from click.exceptions.Exit
    # In some environments, pytest captures it as SystemExit or click.exceptions.Exit
    with pytest.raises((SystemExit, Exception)) as excinfo:
        handle_tool_result(result, learning_mode=False)

    # Check for exit code in either SystemExit or click.Exit
    val: object = excinfo.value
    code: Any = getattr(val, "code", None)
    if code is None:
        code = getattr(val, "exit_code", None)
    assert code == 42


def test_log_directory_not_set() -> None:
    class MockLogger:
        def __init__(self) -> None:
            self.msgs: list[str] = []

        def info(self, msg: str, *args: object) -> None:
            self.msgs.append(msg % args if args else msg)

    logger = MockLogger()
    log_directory("TAG", "dir_name", None, cast(Any, logger))
    assert "[TAG] dir_name: <not set>" in logger.msgs[0]


def test_log_directory_exists(tmp_path: Path) -> None:
    class MockLogger:
        def __init__(self) -> None:
            self.msgs: list[str] = []

        def info(self, msg: str, *args: object) -> None:
            self.msgs.append(msg % args if args else msg)

    logger = MockLogger()
    d = tmp_path / "test_dir"
    d.mkdir()
    (d / "file.txt").write_text("content")

    log_directory("TAG", "dir_name", d, cast(Any, logger))
    assert "(exists)" in logger.msgs[0]
    assert "['file.txt']" in logger.msgs[1]


def test_run_prepare_impl_failure() -> None:
    # Basic test for error path in run_prepare_impl
    logger = logging.getLogger("test_prepare_fail")

    from ml_playground.runtime_cli.runners import create_default_cli_dependencies

    cfg = PreparerConfig(logger=logger)
    deps = create_default_cli_dependencies()
    # Passing invalid metadata to trigger exception
    result = run_prepare_impl("demo", cfg, Path("config.toml"), cast(Any, None), deps)
    assert result.success is False
    assert "Pipeline preparation failed" in result.stderr
