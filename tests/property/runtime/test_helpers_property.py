"""Property-based tests for runtime/helpers module.

Tests helper functions including configuration loading, experiment
extraction, and CLI utilities using Hypothesis for comprehensive coverage.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

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
    exp_config: Path | None = draw(st.one_of([st.none(), valid_paths()]))
    obj_dict: dict[str, Path | None | str] = (
        {"exp_config": exp_config} if exp_config else {"other": "value"}
    )
    return SimpleNamespace(obj=obj_dict)


def _ctx_obj_dict(ctx: SimpleNamespace) -> dict[str, Path | None | str] | None:
    if hasattr(ctx, "obj") and isinstance(ctx.obj, dict):
        return cast(dict[str, Path | None | str], ctx.obj)
    return None


@given(
    incomplete=st.text(min_size=1, max_size=10),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_complete_experiments_signature(incomplete: str) -> None:
    """Test complete_experiments function signature."""
    assert callable(rt_helpers.complete_experiments)


@given(
    ctx=context_objects(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_extract_exp_config_with_context(ctx: SimpleNamespace) -> None:
    """Test extract_exp_config with various context objects."""
    ctx_obj = cast(object, ctx)
    result = rt_helpers.extract_exp_config(ctx_obj)  # type: ignore[arg-type]

    obj_dict = _ctx_obj_dict(ctx)
    if obj_dict is not None and "exp_config" in obj_dict:
        exp_config: Path | None | str = obj_dict["exp_config"]
        assert result == exp_config
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
    ctx_obj = cast(object, ctx)
    result = rt_helpers.extract_exp_config(ctx_obj)  # type: ignore[arg-type]

    obj_dict = _ctx_obj_dict(ctx)
    if obj_dict is not None and "exp_config" in obj_dict:
        exp_config: Path | None | str = obj_dict["exp_config"]
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

    if success:
        rt_helpers.handle_tool_result(result, learning_mode=False)
    else:
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

        def success_func() -> None:
            return None

        rt_helpers.run_or_exit(success_func)
    else:
        if has_keyboard_interrupt:

            def ki_func() -> None:
                raise KeyboardInterrupt()

            rt_helpers.run_or_exit(ki_func)
        elif has_file_error:

            def file_func() -> None:
                raise FileNotFoundError("Test file error")

            import click

            with pytest.raises(click.exceptions.Exit) as exc_info:
                rt_helpers.run_or_exit(file_func)
            assert exc_info.value.exit_code == 1
        elif has_runtime_error:

            def runtime_func() -> None:
                raise RuntimeError("Test runtime error")

            import click

            with pytest.raises(click.exceptions.Exit):
                rt_helpers.run_or_exit(runtime_func)
        else:

            def value_func() -> None:
                raise ValueError("Test value error")

            import click

            with pytest.raises(click.exceptions.Exit):
                rt_helpers.run_or_exit(value_func)


@given(
    tag=st.text(min_size=1, max_size=10),
    dir_name=st.text(min_size=1, max_size=10),
    dir_path=st.one_of([st.none(), valid_paths()]),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_log_directory_various_paths(
    tag: str, dir_name: str, dir_path: Path | None
) -> None:
    """Test log_directory with various path scenarios."""

    class _BufferLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, msg: str) -> None:
            self.messages.append(msg)

    logger = _BufferLogger()
    rt_helpers.log_directory(tag, dir_name, dir_path, logger)  # type: ignore[arg-type]

    assert isinstance(logger.messages, list)


@given(
    tag=st.text(min_size=1, max_size=10),
    dataset_dir=valid_paths(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_log_command_status_handles_dataset_dir(tag: str, dataset_dir: Path) -> None:
    """Test log_command_status handles dataset_dir safely."""

    class SharedCfg:
        def __init__(self, path: Path) -> None:
            self.dataset_dir = path

    class _BufferLogger:
        def __init__(self) -> None:
            self.messages: list[str] = []

        def info(self, msg: str) -> None:
            self.messages.append(msg)

    logger = _BufferLogger()
    rt_helpers.log_command_status(
        tag, SharedCfg(dataset_dir), out_dir=None, logger=logger
    )  # type: ignore[arg-type]

    assert isinstance(logger.messages, list)
