"""Property-based tests for the runtime CLI public API.

Tests global options handling, context management, and CLI configuration
using Hypothesis to discover edge cases in option processing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Protocol, cast

import click
import pytest
import typer
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

from ml_playground.runtime_cli.main import app, global_options
from ml_playground.framework.runtime.core.results import VerbosityLevel


class _EchoFunc(Protocol):
    def __call__(self, message: str, *, err: bool = False) -> object: ...


def _verbosity_arg(value: int | VerbosityLevel) -> int:
    if isinstance(value, int):
        return value
    return int(cast(int, value.value))


@contextmanager
def _patch_attr(obj: object, name: str, value: object):
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


class _LoggerFactory(Protocol):
    def __call__(self, name: str) -> logging.Logger: ...


@st.composite
def valid_paths(draw: st.DrawFn) -> Path:
    """Generate valid file paths that might exist."""
    name = draw(
        st.text(
            min_size=1, max_size=10, alphabet="abcdefghijklmnopqrstuvwxyz0123456789"
        )
    )
    return Path(f"/tmp/{name}.toml")


@st.composite
def verbosity_values(draw: st.DrawFn) -> int | VerbosityLevel:
    """Generate verbosity values as integers or VerbosityLevel enums."""
    use_int = draw(st.booleans())
    if use_int:
        return draw(st.integers(min_value=0, max_value=2))
    else:
        return draw(st.sampled_from(VerbosityLevel))


def _fake_context(**kwargs: object) -> SimpleNamespace:
    """Create a fake Typer context with configurable attributes."""
    ctx = SimpleNamespace()
    ctx.obj = kwargs.get("obj", {})

    def ensure_object(cls):
        if not isinstance(ctx.obj, dict):
            ctx.obj = {}

    ctx.ensure_object = ensure_object
    return ctx


def _fake_click_context(
    has_subcommand: bool = True, help_text: str = "help"
) -> SimpleNamespace:
    """Create a fake Click context."""
    ctx = SimpleNamespace()
    ctx.invoked_subcommand = "prepare" if has_subcommand else None
    ctx.get_help = lambda: help_text
    return ctx


def _fake_echo() -> tuple[list[str], _EchoFunc]:
    """Create a fake echo function and capture messages."""
    messages: list[str] = []

    def echo_func(message: str, *, err: bool = False) -> None:
        messages.append(message)

    return messages, echo_func


def _fake_logger_factory() -> tuple[list[logging.LogRecord], _LoggerFactory]:
    """Create a fake logger factory and capture log records."""
    records: list[logging.LogRecord] = []

    def logger_factory(name: str) -> logging.Logger:
        logger = logging.Logger(name)

        class FakeHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        logger.addHandler(FakeHandler())
        return logger

    return records, logger_factory


@given(  # type: ignore[reportAny]
    exp_config=st.one_of(st.none(), valid_paths()),
    learning_mode=st.booleans(),
    verbosity=verbosity_values(),
)
@settings(max_examples=20, deadline=None, derandomize=True)
def test_apply_global_options_sets_context_values(
    exp_config: Path | None, learning_mode: bool, verbosity: int | VerbosityLevel
) -> None:
    """Test that global options correctly set context values."""
    ctx = _fake_context(obj={})

    # Skip non-existent config files for this test
    if exp_config is not None and not exp_config.exists():
        exp_config = None

    global_options(
        cast(typer.Context, ctx), exp_config, learning_mode, _verbosity_arg(verbosity)
    )

    assert ctx.obj.get("exp_config") == exp_config

    if learning_mode:
        assert ctx.obj.get("learning_mode") is True
    else:
        assert "learning_mode" not in ctx.obj

    expected_level = (
        verbosity
        if isinstance(verbosity, VerbosityLevel)
        else VerbosityLevel(verbosity)
    )
    if expected_level != VerbosityLevel.STANDARD:
        assert ctx.obj.get("verbosity") == expected_level
    else:
        assert "verbosity" not in ctx.obj


@given(  # type: ignore[reportAny]
    exp_config=valid_paths(),
    learning_mode=st.booleans(),
    verbosity=verbosity_values(),
)
@settings(
    max_examples=10,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_apply_global_options_missing_config_exits(
    exp_config: Path,
    learning_mode: bool,
    verbosity: int | VerbosityLevel,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that missing config files cause typer.Exit."""
    assume(not exp_config.exists())  # Ensure file doesn't exist

    ctx = _fake_context(obj={})

    with pytest.raises(typer.Exit) as exc_info:
        global_options(
            cast(typer.Context, ctx),
            exp_config,
            learning_mode,
            _verbosity_arg(verbosity),
        )

    out = capsys.readouterr()
    assert exc_info.value.exit_code == 2
    assert "Config file not found" in out.err


@given(  # type: ignore[reportAny]
    learning_mode=st.booleans(),
    verbosity=verbosity_values(),
)
@settings(
    max_examples=15,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_apply_global_options_no_subcommand_shows_help(
    learning_mode: bool, verbosity: int | VerbosityLevel
) -> None:
    """Test that missing subcommand triggers help display."""
    ctx = _fake_context(obj={})
    click_ctx = _fake_click_context(has_subcommand=False, help_text="Mock help text")
    messages, echo_func = _fake_echo()

    with (
        _patch_attr(click, "get_current_context", lambda silent=True: click_ctx),
        _patch_attr(typer, "echo", echo_func),
    ):
        with pytest.raises(typer.Exit) as exc_info:
            global_options(
                cast(typer.Context, ctx),
                None,
                learning_mode,
                _verbosity_arg(verbosity),
            )

    # No subcommand is treated as a usage error (exit code 2) while still
    # showing a friendly welcome banner and full help output.
    assert exc_info.value.exit_code == 2
    assert any("Welcome to ML Playground runtime CLI!" in msg for msg in messages)
    assert any("No workflow command was provided" in msg for msg in messages)
    assert any("Mock help text" in msg for msg in messages)


@given(  # type: ignore[reportAny]
    exp_config=st.one_of(st.none(), valid_paths()),
    learning_mode=st.booleans(),
    verbosity=verbosity_values(),
)
@settings(max_examples=15, deadline=None, derandomize=True)
def test_global_options_with_default_dependencies(
    exp_config: Path | None, learning_mode: bool, verbosity: int | VerbosityLevel
) -> None:
    """Test global_options function without custom dependency injection."""
    ctx = _fake_context(obj={})

    if exp_config is not None and not exp_config.exists():
        exp_config = None

    global_options(
        cast(typer.Context, ctx), exp_config, learning_mode, _verbosity_arg(verbosity)
    )

    assert ctx.obj.get("exp_config") == exp_config


@given(  # type: ignore[reportAny]
    learning_mode=st.booleans(),
    verbosity=verbosity_values(),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_context_object_handling_edge_cases(
    learning_mode: bool, verbosity: int | VerbosityLevel
) -> None:
    """Test edge cases in context object handling."""
    # Test with context that doesn't have ensure_object
    ctx = SimpleNamespace()
    ctx.obj = None
    ctx.ensure_object = lambda: None  # This will cause issues

    # Should handle gracefully by returning early
    global_options(
        cast(typer.Context, ctx), None, learning_mode, _verbosity_arg(verbosity)
    )

    # Test with non-dict obj (should convert to dict)
    ctx2 = SimpleNamespace()
    ctx2.obj = SimpleNamespace()

    def ensure_object(cls):
        if not isinstance(ctx2.obj, dict):
            ctx2.obj = {}

    ctx2.ensure_object = ensure_object

    global_options(
        cast(typer.Context, ctx2), None, learning_mode, _verbosity_arg(verbosity)
    )
    assert isinstance(ctx2.obj, dict)


def test_apply_global_options_context_getter_type_error_fallback() -> None:
    """_apply_global_options falls back when click context getter rejects kwargs."""

    ctx = _fake_context(obj={})
    click_ctx = _fake_click_context(has_subcommand=False, help_text="help")
    calls: list[tuple[bool]] = []

    def flaky_getter(*args: object, **kwargs: object):  # type: ignore[override]
        silent = bool(kwargs.get("silent", False))
        calls.append((silent,))
        if silent:
            raise TypeError("unexpected kwarg")
        return click_ctx

    messages, echo_func = _fake_echo()

    with (
        _patch_attr(click, "get_current_context", flaky_getter),
        _patch_attr(typer, "echo", echo_func),
    ):
        with pytest.raises(typer.Exit) as exc_info:
            global_options(
                cast(typer.Context, ctx),
                None,
                False,
                _verbosity_arg(VerbosityLevel.STANDARD),
            )

    assert exc_info.value.exit_code == 2
    assert calls == [(True,), (False,)]
    combined = "\n".join(messages)
    assert "Welcome to ML Playground runtime CLI!" in combined


@given(  # type: ignore[reportAny]
    learning_mode=st.booleans(),
    verbosity_int=st.integers(min_value=0, max_value=2),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_verbosity_conversion(learning_mode: bool, verbosity_int: int) -> None:
    """Test that integer verbosity is correctly converted to VerbosityLevel."""
    ctx = _fake_context(obj={})

    global_options(
        cast(typer.Context, ctx), None, learning_mode, _verbosity_arg(verbosity_int)
    )

    expected_level = VerbosityLevel(verbosity_int)
    if expected_level != VerbosityLevel.STANDARD:
        assert ctx.obj.get("verbosity") == expected_level
    else:
        assert "verbosity" not in ctx.obj


@given(  # type: ignore[reportAny]
    learning_mode=st.booleans(),
    verbosity_enum=st.sampled_from(list(VerbosityLevel)),
)
@settings(max_examples=10, deadline=None, derandomize=True)
def test_verbosity_enum_passthrough(
    learning_mode: bool, verbosity_enum: VerbosityLevel
) -> None:
    """Test that VerbosityLevel enum is passed through unchanged."""
    ctx = _fake_context(obj={})

    global_options(
        cast(typer.Context, ctx), None, learning_mode, _verbosity_arg(verbosity_enum)
    )

    if verbosity_enum != VerbosityLevel.STANDARD:
        assert ctx.obj.get("verbosity") is verbosity_enum
    else:
        assert "verbosity" not in ctx.obj


def test_app_configuration() -> None:
    """Test that the Typer app is properly configured."""
    assert app.info.no_args_is_help is False
    assert app.info.invoke_without_command is True
    assert app.info.help is not None
    assert "ML Playground CLI" in app.info.help
    assert "prepare data" in app.info.help.lower()


def test_commands_registration() -> None:
    """Test that commands are properly registered with the app."""
    # The app should have registered commands
    assert len(app.registered_commands) > 0

    # Check for expected command names via callback function names
    command_names = {
        cmd.callback.__name__ for cmd in app.registered_commands if cmd.callback
    }
    expected_commands = {"prepare", "train", "sample", "analyze"}
    assert expected_commands.issubset(command_names)
