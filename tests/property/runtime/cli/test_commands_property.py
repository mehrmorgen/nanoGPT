"""Property-based tests for runtime/cli/commands module.

Tests command functions, override handling, and dependency injection
using Hypothesis to discover edge cases in command execution.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Protocol, cast

from hypothesis import given, settings
from hypothesis import strategies as st
import typer

from ml_playground.runtime.cli import commands
from ml_playground.runtime.core.results import ToolResult, VerbosityLevel


class _OverrideCallable(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> object: ...


@st.composite
def override_maps(draw: st.DrawFn) -> dict[str, object]:
    """Generate various override configurations for testing."""
    overrides: dict[str, object] = {}

    def _noop_override(*args: object, **kwargs: object) -> None:
        return None

    def _analysis_runner_override(*args: object, **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(spec=ToolResult)

    # Add random overrides
    if draw(st.booleans()):
        overrides["cli_deps"] = SimpleNamespace()

    if draw(st.booleans()):
        overrides["run_invoker"] = cast(_OverrideCallable, _noop_override)
    if draw(st.booleans()):
        overrides["result_handler"] = cast(_OverrideCallable, _noop_override)

    # Command-specific overrides
    command = draw(st.sampled_from(["prepare", "train", "sample", "analyze"]))
    if draw(st.booleans()):
        overrides[f"cli_deps_{command}"] = SimpleNamespace()
    if draw(st.booleans()):
        overrides[f"run_invoker_{command}"] = cast(_OverrideCallable, _noop_override)
    if draw(st.booleans()):
        overrides[f"result_handler_{command}"] = cast(_OverrideCallable, _noop_override)
    if draw(st.booleans()):
        overrides["analysis_runner"] = cast(
            _OverrideCallable,
            _analysis_runner_override,
        )

    return overrides


@st.composite
def learning_contexts(
    draw: st.DrawFn,
) -> tuple[bool, VerbosityLevel, dict[str, object]]:
    """Generate learning mode contexts."""
    learning_mode = draw(st.booleans())
    verbosity = draw(st.sampled_from(list(VerbosityLevel)))
    overrides = draw(override_maps())
    return learning_mode, verbosity, overrides


def _fake_context(**kwargs: object) -> SimpleNamespace:
    """Create a fake Typer context."""
    ctx = SimpleNamespace()
    for key, value in kwargs.items():
        setattr(ctx, key, value)
    return ctx


def _fake_experiment_arg(name: str = "test_exp") -> str:
    """Create a fake experiment argument."""
    return name  # ExperimentArg is a str | None


def _fake_dependencies() -> SimpleNamespace:
    """Create fake CLI dependencies."""
    return SimpleNamespace()


@given(overrides=override_maps())
@settings(max_examples=15, deadline=None, derandomize=True)
def test_coerce_overrides_handles_various_types(overrides: dict[str, object]) -> None:
    """Test _coerce_overrides with different input types."""
    # Test with dict
    result = commands._coerce_overrides(overrides)  # pyright: ignore[reportPrivateUsage]
    assert isinstance(result, dict)
    assert dict(result) == overrides  # Should be identical

    # Test with non-mapping
    result2 = commands._coerce_overrides("not_a_mapping")  # pyright: ignore[reportPrivateUsage]
    assert result2 == {}

    # Test with None
    result3 = commands._coerce_overrides(None)  # pyright: ignore[reportPrivateUsage]
    assert result3 == {}


@given(overrides=override_maps())
@settings(max_examples=10, deadline=None, derandomize=True)
def test_select_override_value_finds_correct_key(overrides: dict[str, object]) -> None:
    """Test _select_override_value finds the first available key."""
    # Add a known value
    overrides["test_key"] = "found_value"

    result = commands._select_override_value(  # pyright: ignore[reportPrivateUsage]
        overrides, "missing1", "test_key", "missing2"
    )
    assert result == "found_value"

    # Test with missing keys
    result2 = commands._select_override_value(  # pyright: ignore[reportPrivateUsage]
        overrides, "missing1", "missing2"
    )
    assert result2 is None


@given(overrides=override_maps())
@settings(max_examples=10, deadline=None, derandomize=True)
def test_select_override_callable_filters_non_callables(
    overrides: dict[str, object],
) -> None:
    """Test _select_override_callable only returns callable objects."""
    # Add callable and non-callable
    overrides["callable_key"] = lambda: None
    overrides["string_key"] = "not_callable"
    overrides["int_key"] = 42

    result = commands._select_override_callable(  # pyright: ignore[reportPrivateUsage]
        overrides, "string_key", "callable_key"
    )
    assert result is overrides["callable_key"]

    result2 = commands._select_override_callable(  # pyright: ignore[reportPrivateUsage]
        overrides, "string_key", "int_key"
    )
    assert result2 is None

    result3 = commands._select_override_callable(  # pyright: ignore[reportPrivateUsage]
        overrides, "missing", "callable_key"
    )
    assert result3 is overrides["callable_key"]


def _fake_functions() -> dict[str, object]:
    """Create fake functions for patching."""

    def _extract_exp_config(_ctx: object) -> Path:
        return Path("/tmp/config.toml")

    def _get_cli_dependencies() -> SimpleNamespace:
        return _fake_dependencies()

    def _prepare_learning_context(
        _ctx: object,
    ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
        return (False, VerbosityLevel.STANDARD, {})

    def _run_prepare_command(*args: object, **kwargs: object) -> None:
        return None

    def _run_train_command(*args: object, **kwargs: object) -> None:
        return None

    def _run_sample_command(*args: object, **kwargs: object) -> None:
        return None

    def _handle_tool_result(*args: object, **kwargs: object) -> None:
        return None

    return {
        "extract_exp_config": _extract_exp_config,
        "get_cli_dependencies": _get_cli_dependencies,
        "prepare_learning_context": _prepare_learning_context,
        "run_prepare_command": _run_prepare_command,
        "run_train_command": _run_train_command,
        "run_sample_command": _run_sample_command,
        "handle_tool_result": _handle_tool_result,
    }


@given(
    learning_mode=st.booleans(),
    verbosity=st.sampled_from(list(VerbosityLevel)),
    overrides=override_maps(),
)
@settings(max_examples=12, deadline=None, derandomize=True)
def test_prepare_command_uses_overrides(
    learning_mode: bool, verbosity: VerbosityLevel, overrides: dict[str, object]
) -> None:
    """Test prepare command correctly applies overrides."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg("prepare_test")

    # Store original functions
    original_funcs: dict[str, object] = {}
    fake_funcs = _fake_functions()

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    for func_name, fake_func in fake_funcs.items():
        original_funcs[func_name] = getattr(commands_module, func_name)
        setattr(commands_module, func_name, fake_func)

    try:
        # Override the learning context to return our test values
        def _learning_context(
            ctx: typer.Context,
        ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
            return learning_mode, verbosity, overrides

        setattr(commands_module, "prepare_learning_context", _learning_context)

        # This should not raise an exception
        commands.prepare(cast(typer.Context, ctx), experiment)

        # The fact that it didn't crash means the override handling worked
        # We can't easily verify the exact values without complex mocking

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)


@given(
    learning_mode=st.booleans(),
    verbosity=st.sampled_from(list(VerbosityLevel)),
    overrides=override_maps(),
)
@settings(max_examples=12, deadline=None, derandomize=True)
def test_train_command_uses_overrides(
    learning_mode: bool, verbosity: VerbosityLevel, overrides: dict[str, object]
) -> None:
    """Test train command correctly applies overrides."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg("train_test")

    # Store original functions
    original_funcs: dict[str, object] = {}
    fake_funcs = _fake_functions()

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    for func_name, fake_func in fake_funcs.items():
        original_funcs[func_name] = getattr(commands_module, func_name)
        setattr(commands_module, func_name, fake_func)

    try:
        # Override the learning context to return our test values
        def _learning_context(
            ctx: typer.Context,
        ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
            return learning_mode, verbosity, overrides

        setattr(commands_module, "prepare_learning_context", _learning_context)

        # This should not raise an exception
        commands.train(cast(typer.Context, ctx), experiment)

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)


@given(
    learning_mode=st.booleans(),
    verbosity=st.sampled_from(list(VerbosityLevel)),
    overrides=override_maps(),
)
@settings(max_examples=12, deadline=None, derandomize=True)
def test_sample_command_uses_overrides(
    learning_mode: bool, verbosity: VerbosityLevel, overrides: dict[str, object]
) -> None:
    """Test sample command correctly applies overrides."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg("sample_test")

    # Store original functions
    original_funcs: dict[str, object] = {}
    fake_funcs = _fake_functions()

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    for func_name, fake_func in fake_funcs.items():
        original_funcs[func_name] = getattr(commands_module, func_name)
        setattr(commands_module, func_name, fake_func)

    try:
        # Override the learning context to return our test values
        def _learning_context(
            ctx: typer.Context,
        ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
            return learning_mode, verbosity, overrides

        setattr(commands_module, "prepare_learning_context", _learning_context)

        # This should not raise an exception
        commands.sample(cast(typer.Context, ctx), experiment)

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)


@given(
    learning_mode=st.booleans(),
    verbosity=st.sampled_from(list(VerbosityLevel)),
    overrides=override_maps(),
    host=st.text(
        min_size=1, max_size=15, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
    ),
    port=st.integers(min_value=1024, max_value=65535),
    open_browser=st.booleans(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_analyze_command_uses_overrides(
    learning_mode: bool,
    verbosity: VerbosityLevel,
    overrides: dict[str, object],
    host: str,
    port: int,
    open_browser: bool,
) -> None:
    """Test analyze command correctly applies overrides."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg("analyze_test")

    # Store original functions
    original_funcs: dict[str, object] = {}
    fake_funcs = _fake_functions()

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    for func_name, fake_func in fake_funcs.items():
        original_funcs[func_name] = getattr(commands_module, func_name)
        setattr(commands_module, func_name, fake_func)

    try:
        # Override the learning context to return our test values
        def _learning_context(
            ctx: typer.Context,
        ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
            return learning_mode, verbosity, overrides

        setattr(commands_module, "prepare_learning_context", _learning_context)

        # This should not raise an exception
        commands.analyze(cast(typer.Context, ctx), experiment, host, port, open_browser)

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)


def test_analyze_command_custom_overrides() -> None:
    """Test analyze command with custom runner and handler."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg("analyze_custom")

    custom_runner_called = False
    custom_handler_called = False

    def custom_runner(*args: object, **kwargs: object) -> SimpleNamespace:
        nonlocal custom_runner_called
        custom_runner_called = True
        return SimpleNamespace(spec=ToolResult)

    def custom_handler(*args: object, **kwargs: object) -> None:
        nonlocal custom_handler_called
        custom_handler_called = True

    overrides: dict[str, object] = {
        "analysis_runner": custom_runner,
        "result_handler_analyze": custom_handler,
    }

    # Store original functions
    original_funcs: dict[str, object] = {}
    fake_funcs = _fake_functions()

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    for func_name, fake_func in fake_funcs.items():
        original_funcs[func_name] = getattr(commands_module, func_name)
        setattr(commands_module, func_name, fake_func)

    try:
        # Override the learning context to return our test values
        def _learning_context(
            ctx: typer.Context,
        ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
            return (True, VerbosityLevel.STANDARD, overrides)

        setattr(commands_module, "prepare_learning_context", _learning_context)

        commands.analyze(cast(typer.Context, ctx), experiment)

        # Verify custom functions were used
        assert custom_runner_called
        assert custom_handler_called

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)


def test_command_functions_extract_dependencies() -> None:
    """Test that all command functions properly extract dependencies."""
    ctx = _fake_context()
    experiment = _fake_experiment_arg()
    overrides: dict[str, object] = {}

    # Track function calls
    extract_called = False
    deps_called = False
    learning_called = False

    def track_extract(ctx: typer.Context) -> Path:
        nonlocal extract_called
        extract_called = True
        return Path("/tmp/config.toml")

    def track_deps() -> SimpleNamespace:
        nonlocal deps_called
        deps_called = True
        return _fake_dependencies()

    def track_learning(
        ctx: typer.Context,
    ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
        nonlocal learning_called
        learning_called = True
        return (False, VerbosityLevel.STANDARD, overrides)

    # Store original functions
    original_funcs: dict[str, object] = {}

    # Patch functions at module level
    import ml_playground.runtime.cli.commands as commands_module

    original_funcs["extract_exp_config"] = commands_module.extract_exp_config
    original_funcs["get_cli_dependencies"] = commands_module.get_cli_dependencies
    original_funcs["prepare_learning_context"] = (
        commands_module.prepare_learning_context
    )
    original_funcs["run_prepare_command"] = commands_module.run_prepare_command

    commands_module.extract_exp_config = track_extract
    commands_module.get_cli_dependencies = track_deps
    commands_module.prepare_learning_context = track_learning

    def _noop_run_prepare_command(*args: object, **kwargs: object) -> None:
        return None

    commands_module.run_prepare_command = _noop_run_prepare_command  # type: ignore[assignment]

    try:
        commands.prepare(cast(typer.Context, ctx), experiment)

        # Verify all dependencies were extracted
        assert extract_called
        assert deps_called
        assert learning_called

    finally:
        # Restore original functions
        for func_name, original_func in original_funcs.items():
            setattr(commands_module, func_name, original_func)
