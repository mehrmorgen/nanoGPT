"""Property-based tests for runtime/cli/runners module.

Tests command runners, result handling, and error management
using Hypothesis to discover edge cases in runner execution.
"""

from __future__ import annotations

from types import SimpleNamespace

import click
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.runtime.cli import runners
from ml_playground.runtime.core.results import ToolResult


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


@given(
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


@given(
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
    assert callable(runners.run_prepare_command)
    assert callable(runners.run_train_command)
    assert callable(runners.run_sample_command)
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


@given(
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
