"""Property-based tests for runtime/cli/runners module.

Tests command runners, result handling, and error management
using Hypothesis to discover edge cases in runner execution.
"""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import click
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.runtime_cli import runners
from ml_playground.framework.runtime.core.results import ToolResult


@st.composite
def tool_results(draw: st.DrawFn) -> ToolResult:
    """Generate various ToolResult configurations."""
    success = draw(st.booleans())
    exit_code = 0 if success else draw(st.integers(min_value=1, max_value=255))

    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace="ml",
        category=draw(st.sampled_from(["prepare", "train", "sample", "analyze"])),
        command=draw(st.text(min_size=1, max_size=20)),
        stdout=draw(st.text(max_size=100)) if draw(st.booleans()) else "",
        stderr=draw(st.text(max_size=100)) if draw(st.booleans()) else "",
    )

    return result


def _fake_dependencies() -> SimpleNamespace:
    """Create fake CLI dependencies."""
    return SimpleNamespace()


def _fake_experiment(name: str = "test_exp") -> SimpleNamespace:
    """Create a fake experiment."""
    exp = SimpleNamespace()
    exp.name = name
    return exp


def _fake_config() -> SimpleNamespace:
    """Create a fake configuration."""
    return SimpleNamespace()


@given(  # type: ignore[reportAny]
    success=st.booleans(),
    has_stdout=st.booleans(),
    has_stderr=st.booleans(),
    exit_code=st.integers(min_value=0, max_value=255),
)
@settings(max_examples=15, deadline=None, derandomize=True)
def test_handle_tool_result_processes_all_fields(
    success: bool, has_stdout: bool, has_stderr: bool, exit_code: int
) -> None:
    """Test handle_tool_result processes all result fields correctly."""
    result = ToolResult.create(
        success=success,
        exit_code=exit_code,
        namespace="ml",
        category="test",
        command="test_command",
        stdout="Test output" if has_stdout else "",
        stderr="Test error" if has_stderr else "",
    )

    # Should not raise an exception for success
    if success:
        runners.handle_tool_result(result, learning_mode=False)
    else:
        # Should raise click Exit for failure
        with pytest.raises(click.exceptions.Exit) as exc_info:
            runners.handle_tool_result(result, learning_mode=False)
        # Note: exit_code can be 0 even for failure in some cases
        assert exc_info.value.exit_code == exit_code


@given(  # type: ignore[reportAny]
    result=tool_results(),
    learning_mode=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_handle_tool_result_with_learning_mode(
    result: ToolResult, learning_mode: bool
) -> None:
    """Test handle_tool_result with learning mode enabled/disabled."""
    # Should not raise an exception for success
    if result.success:
        runners.handle_tool_result(result, learning_mode)
    else:
        # Should raise click Exit for failure
        with pytest.raises(click.exceptions.Exit):
            runners.handle_tool_result(result, learning_mode)


def test_all_runner_functions_exist() -> None:
    """Test that all expected runner functions are available."""
    assert callable(runners.run_prepare_cmd)
    assert callable(runners.run_train_cmd)
    assert callable(runners.run_sample_cmd)
    assert callable(runners.run_analyze)
    assert callable(runners.handle_tool_result)
    assert callable(runners.run_or_exit)


def test_run_or_exit_with_success() -> None:
    """Test run_or_exit with successful function."""

    def success_func():
        pass

    # Should not raise
    runners.run_or_exit(success_func)


def test_run_or_exit_with_failure() -> None:
    """Test run_or_exit with failed function."""

    def failure_func():
        raise RuntimeError("Test error")

    import click

    with pytest.raises(click.exceptions.Exit) as exc_info:
        runners.run_or_exit(failure_func)

    assert exc_info.value.exit_code == 1


@given(  # type: ignore[reportAny]
    success=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_or_exit_exit_code_matches_result(success: bool) -> None:
    """Test run_or_exit exit code matches result exit code."""

    def success_func():
        pass

    def failure_func():
        raise RuntimeError("Test error")

    if success:
        # Should not raise for success
        runners.run_or_exit(success_func)
    else:
        import click

        with pytest.raises(click.exceptions.Exit) as exc_info:
            runners.run_or_exit(failure_func)
        assert exc_info.value.exit_code == 1


@given(kind=st.sampled_from(["train", "prepare", "sample"]))
@settings(max_examples=6, deadline=None, derandomize=True)
def test_command_runners_require_non_none_metadata_config_path(kind: str) -> None:
    if kind == "train":
        exp = SimpleNamespace(
            training=SimpleNamespace(runtime=SimpleNamespace()),
            metadata=SimpleNamespace(config_path=None, train_out_dir=Path("/tmp")),
        )
        deps = runners.CLIDependencies(load_experiment=lambda *_: exp)
        with pytest.raises(
            RuntimeError, match="metadata.config_path is required for training"
        ):
            runners.run_train_cmd("demo", None, deps)
        return

    if kind == "prepare":
        exp = SimpleNamespace(
            prepare=SimpleNamespace(),
            metadata=SimpleNamespace(config_path=None),
        )
        deps = runners.CLIDependencies(load_experiment=lambda *_: exp)
        with pytest.raises(
            RuntimeError, match="metadata.config_path is required for preparation"
        ):
            runners.run_prepare_cmd("demo", None, deps)
        return

    exp = SimpleNamespace(
        sampling=SimpleNamespace(runtime=SimpleNamespace()),
        metadata=SimpleNamespace(config_path=None, train_out_dir=Path("/tmp")),
    )
    deps = runners.CLIDependencies(load_experiment=lambda *_: exp)
    with pytest.raises(
        RuntimeError, match="metadata.config_path is required for sampling"
    ):
        runners.run_sample_cmd("demo", None, deps)


@given(open_browser=st.booleans())
@settings(max_examples=8, deadline=None, derandomize=True)
def test_run_analyze_cmd_uses_module_fallback_handler(open_browser: bool) -> None:
    exp = SimpleNamespace(metadata=SimpleNamespace())
    handled: list[tuple[bool, int]] = []
    original_handler = runners.handle_tool_result
    try:
        runners.handle_tool_result = lambda result, learning_mode: handled.append(
            (result.success, result.exit_code)
        )
        deps = runners.CLIDependencies(
            load_experiment=lambda *_: exp,
            run_analyze=lambda *_a, **_k: ToolResult.create(
                success=True,
                exit_code=0,
                namespace="ml",
                category="analyze",
                command="demo",
            ),
            handle_tool_result=runners.runtime_bootstrap.CLIDependencies.handle_tool_result,
        )
        runners.run_analyze_cmd("demo", None, deps, "127.0.0.1", 8050, open_browser)
    finally:
        runners.handle_tool_result = original_handler

    assert handled == [(True, 0)]


@given(_none=st.none())
@settings(max_examples=1, deadline=None, derandomize=True)
def test_normalize_cli_path_accepts_none(_none: None) -> None:
    _ = _none
    assert runners._normalize_cli_path(None) is None


@given(command=st.text(min_size=1, max_size=8, alphabet="abcdefghijklmnopqrstuvwxyz"))
@settings(max_examples=8, deadline=None, derandomize=True)
def test_run_prepare_cmd_uses_module_fallback_handler(command: str) -> None:
    handled: list[bool] = []
    original_handler = runners.handle_tool_result
    try:
        runners.handle_tool_result = lambda result, learning_mode: handled.append(
            result.success
        )
        with TemporaryDirectory() as tmp_dir:
            cfg_path = Path(tmp_dir) / "cfg.toml"
            cfg_path.write_text("[dummy]\nvalue=1\n", encoding="utf-8")
            exp = SimpleNamespace(
                prepare=SimpleNamespace(),
                metadata=SimpleNamespace(config_path=cfg_path),
            )
            deps = runners.CLIDependencies(
                load_experiment=lambda *_: exp,
                run_prepare=lambda *_a, **_k: ToolResult.create(
                    success=True,
                    exit_code=0,
                    namespace="ml",
                    category="prepare",
                    command=command,
                ),
                handle_tool_result=runners.runtime_bootstrap.CLIDependencies.handle_tool_result,
            )
            runners.run_prepare_cmd(command, None, deps)
    finally:
        runners.handle_tool_result = original_handler

    assert handled == [True]
