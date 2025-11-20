"""Property-based tests for runtime/helpers module.

Tests helper functions including configuration loading, experiment
extraction, and CLI utilities using Hypothesis for comprehensive coverage.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ml_playground.runtime import helpers as rt_helpers


@st.composite
def valid_paths(draw: st.DrawFn) -> Path:
    """Generate valid file paths."""
    name = draw(st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz"))
    return Path(f"/tmp/{name}")


@st.composite
def context_objects(draw: st.DrawFn) -> SimpleNamespace:
    """Generate fake Typer context objects."""
    exp_config = draw(st.one_of([st.none(), valid_paths()]))
    return SimpleNamespace(
        obj={"exp_config": exp_config} if exp_config else {"other": "value"}
    )


@given(
    incomplete=st.text(min_size=1, max_size=10),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_complete_experiments_signature(incomplete: str) -> None:
    """Test complete_experiments function signature."""
    # Just verify the function exists and is callable
    assert callable(rt_helpers.complete_experiments)


@given(
    ctx=context_objects(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_extract_exp_config_with_context(ctx: SimpleNamespace) -> None:
    """Test extract_exp_config with various context objects."""
    result = rt_helpers.extract_exp_config(ctx)

    if hasattr(ctx, "obj") and isinstance(ctx.obj, dict) and "exp_config" in ctx.obj:
        assert result == ctx.obj["exp_config"]
    else:
        assert result is None


@given(
    ctx=st.sampled_from(
        [
            SimpleNamespace(obj=None),
            SimpleNamespace(obj="not a dict"),
            SimpleNamespace(obj={}),
            SimpleNamespace(obj={"exp_config": None}),
            SimpleNamespace(obj={"exp_config": Path("/tmp/test")}),
        ]
    )
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_extract_exp_config_edge_cases(ctx: SimpleNamespace) -> None:
    """Test extract_exp_config with edge cases."""
    result = rt_helpers.extract_exp_config(ctx)

    if isinstance(ctx.obj, dict) and "exp_config" in ctx.obj:
        exp_config = ctx.obj["exp_config"]
        if isinstance(exp_config, Path):
            assert result == exp_config
        else:
            assert result is None
    else:
        assert result is None


@given(
    success=st.booleans(),
    has_stdout=st.booleans(),
    has_stderr=st.booleans(),
    has_learning_info=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_handle_tool_result_with_various_outputs(
    success: bool, has_stdout: bool, has_stderr: bool, has_learning_info: bool
) -> None:
    """Test handle_tool_result with different output combinations."""
    from ml_playground.runtime.core.results import ToolResult, LearningInfo

    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="test",
        command="test",
        stdout="Test output" if has_stdout else "",
        stderr="Test error" if has_stderr else "",
        learning_info=LearningInfo() if has_learning_info else None,
    )

    # Should not raise for success
    if success:
        rt_helpers.handle_tool_result(result, learning_mode=False)
    else:
        # Should raise Exit for failure
        import click

        with pytest.raises(click.exceptions.Exit) as exc_info:
            rt_helpers.handle_tool_result(result, learning_mode=False)
        assert exc_info.value.exit_code == result.exit_code


@given(
    success=st.booleans(),
    learning_mode=st.booleans(),
    has_explanations=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_handle_tool_result_learning_mode(
    success: bool, learning_mode: bool, has_explanations: bool
) -> None:
    """Test handle_tool_result with learning mode."""
    from ml_playground.runtime.core.results import ToolResult, LearningInfo

    learning_info = LearningInfo(
        explanations=["Test explanation"] if has_explanations else []
    )

    result = ToolResult.create(
        success=success,
        exit_code=0 if success else 1,
        namespace="ml",
        category="test",
        command="test",
        learning_info=learning_info,
    )

    if success:
        rt_helpers.handle_tool_result(result, learning_mode=learning_mode)
    else:
        import click

        with pytest.raises(click.exceptions.Exit):
            rt_helpers.handle_tool_result(result, learning_mode=learning_mode)


@given(
    func_returns=st.booleans(),
    has_keyboard_interrupt=st.booleans(),
    has_file_error=st.booleans(),
    has_runtime_error=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_or_exit_exception_handling(
    func_returns: bool,
    has_keyboard_interrupt: bool,
    has_file_error: bool,
    has_runtime_error: bool,
) -> None:
    """Test run_or_exit handles various exceptions."""
    if func_returns:

        def success_func():
            pass

        # Should not raise
        rt_helpers.run_or_exit(success_func)
    else:
        if has_keyboard_interrupt:

            def ki_func():
                raise KeyboardInterrupt()

            # Should not raise Exit for KeyboardInterrupt
            rt_helpers.run_or_exit(ki_func)
        elif has_file_error:

            def file_func():
                raise FileNotFoundError("Test file error")

            import click

            with pytest.raises(click.exceptions.Exit) as exc_info:
                rt_helpers.run_or_exit(file_func)
            assert exc_info.value.exit_code == 1
        elif has_runtime_error:

            def runtime_func():
                raise RuntimeError("Test runtime error")

            import click

            with pytest.raises(click.exceptions.Exit) as exc_info:
                rt_helpers.run_or_exit(runtime_func)
            assert exc_info.value.exit_code == 1
        else:

            def value_func():
                raise ValueError("Test value error")

            import click

            with pytest.raises(click.exceptions.Exit) as exc_info:
                rt_helpers.run_or_exit(value_func)
            assert exc_info.value.exit_code == 1


@given(
    exit_code=st.integers(min_value=0, max_value=255),
    keyboard_msg=st.one_of([st.none(), st.text(max_size=50)]),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_run_or_exit_with_custom_exit_code(
    exit_code: int, keyboard_msg: str | None
) -> None:
    """Test run_or_exit with custom exit codes."""

    def error_func():
        raise RuntimeError("Test error")

    import click

    with pytest.raises(click.exceptions.Exit) as exc_info:
        rt_helpers.run_or_exit(error_func, exception_exit_code=exit_code)
    assert exc_info.value.exit_code == exit_code

    # Test KeyboardInterrupt with message
    def ki_func():
        raise KeyboardInterrupt()

    # Should not raise Exit for KeyboardInterrupt
    rt_helpers.run_or_exit(ki_func, keyboard_interrupt_msg=keyboard_msg)


@given(
    has_stdout=st.booleans(),
    has_stderr=st.booleans(),
    learning_mode=st.booleans(),
    has_commands=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_handle_tool_result_output_display(
    has_stdout: bool, has_stderr: bool, learning_mode: bool, has_commands: bool
) -> None:
    """Test handle_tool_result displays output correctly."""
    from ml_playground.runtime.core.results import ToolResult, LearningInfo

    learning_info = LearningInfo(commands_executed=["cmd1"] if has_commands else [])

    result = ToolResult.create(
        success=True,
        exit_code=0,
        namespace="ml",
        category="test",
        command="test",
        stdout="Test output" if has_stdout else "",
        stderr="Test error" if has_stderr else "",
        learning_info=learning_info,
    )

    # Should not raise for successful result
    rt_helpers.handle_tool_result(result, learning_mode=learning_mode)


def test_all_helper_functions_exist() -> None:
    """Test that all expected helper functions are available."""
    expected_functions = [
        "complete_experiments",
        "extract_exp_config",
        "handle_tool_result",
        "run_or_exit",
    ]

    for func_name in expected_functions:
        assert hasattr(rt_helpers, func_name)
        assert callable(getattr(rt_helpers, func_name))


@given(
    obj_value=st.sampled_from(
        [None, "string", 123, {}, {"exp_config": "/tmp"}, {"exp_config": Path("/tmp")}]
    ),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_extract_exp_config_various_objects(obj_value: Any) -> None:
    """Test extract_exp_config with various object types."""
    ctx = SimpleNamespace(obj=obj_value)
    result = rt_helpers.extract_exp_config(ctx)

    if isinstance(obj_value, dict) and "exp_config" in obj_value:
        exp_config = obj_value["exp_config"]
        if isinstance(exp_config, Path):
            # Only returns Path if the value is already a Path
            assert result == exp_config
        else:
            # Returns None for strings or other types
            assert result is None
    else:
        assert result is None
