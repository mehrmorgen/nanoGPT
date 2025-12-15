"""Property-based tests for runtime/cli/commands module.

Tests command functions, override handling, and dependency injection
using Hypothesis to discover edge cases in command execution.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Protocol, cast

from hypothesis import given, settings
from hypothesis import strategies as st
import typer

from ml_playground.runtime.cli import commands
from ml_playground.runtime.core.bootstrap import (
    CLIDependencies,
    override_runtime_cli_dependencies,
)
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


def _make_cli_deps(config_path: Path) -> CLIDependencies:
    shared = SimpleNamespace(config_path=config_path)
    exp = SimpleNamespace(
        prepare=SimpleNamespace(),
        train=SimpleNamespace(),
        sample=SimpleNamespace(),
        shared=shared,
    )

    def load_experiment(_: str, __: Path | None) -> object:
        return exp

    def ensure_train_prerequisites(_: object) -> None:
        return None

    def ensure_sample_prerequisites(_: object) -> None:
        return None

    def run_prepare(
        experiment: str,
        _prepare_cfg: object,
        _config_path: Path,
        _shared: object,
        _learning_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command=experiment,
            stdout="ok",
        )

    def run_train(
        experiment: str,
        _train_cfg: object,
        _config_path: Path,
        _shared: object,
        _learning_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="train",
            command=experiment,
            stdout="ok",
        )

    def run_sample(
        experiment: str,
        _sample_cfg: object,
        _config_path: Path,
        _shared: object,
        _learning_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
            stdout="ok",
        )

    return CLIDependencies(
        load_experiment=load_experiment,
        ensure_train_prerequisites=ensure_train_prerequisites,
        ensure_sample_prerequisites=ensure_sample_prerequisites,
        run_prepare=run_prepare,
        run_train=run_train,
        run_sample=run_sample,
    )


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
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg("prepare_test")

    fake_funcs = _fake_functions()
    config_path = cast(Path, fake_funcs["extract_exp_config"](ctx))
    deps = _make_cli_deps(config_path)

    def _safe_invoker(action: Callable[[], None]) -> None:
        action()

    def _safe_handler(_: ToolResult, __: bool) -> None:
        return None

    ctx.obj = {
        "exp_config": config_path,
        "learning_mode": learning_mode,
        "verbosity": verbosity,
        **overrides,
        "cli_deps_prepare": deps,
        "run_invoker_prepare": _safe_invoker,
        "result_handler_prepare": _safe_handler,
    }

    with override_runtime_cli_dependencies(deps):
        commands.prepare(cast(typer.Context, ctx), experiment)


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
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg("train_test")

    fake_funcs = _fake_functions()
    config_path = cast(Path, fake_funcs["extract_exp_config"](ctx))
    deps = _make_cli_deps(config_path)

    def _safe_invoker(action: Callable[[], None]) -> None:
        action()

    def _safe_handler(_: ToolResult, __: bool) -> None:
        return None

    ctx.obj = {
        "exp_config": config_path,
        "learning_mode": learning_mode,
        "verbosity": verbosity,
        **overrides,
        "cli_deps_train": deps,
        "run_invoker_train": _safe_invoker,
        "result_handler_train": _safe_handler,
    }

    with override_runtime_cli_dependencies(deps):
        commands.train(cast(typer.Context, ctx), experiment)


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
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg("sample_test")

    fake_funcs = _fake_functions()
    config_path = cast(Path, fake_funcs["extract_exp_config"](ctx))
    deps = _make_cli_deps(config_path)

    def _safe_invoker(action: Callable[[], None]) -> None:
        action()

    def _safe_handler(_: ToolResult, __: bool) -> None:
        return None

    ctx.obj = {
        "exp_config": config_path,
        "learning_mode": learning_mode,
        "verbosity": verbosity,
        **overrides,
        "cli_deps_sample": deps,
        "run_invoker_sample": _safe_invoker,
        "result_handler_sample": _safe_handler,
    }

    with override_runtime_cli_dependencies(deps):
        commands.sample(cast(typer.Context, ctx), experiment)


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
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg("analyze_test")

    def _analysis_runner(
        experiment_name: str,
        _host: str,
        _port: int,
        _open_browser: bool,
        _learning_engine: object | None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command=experiment_name,
            stdout="ok",
        )

    def _safe_handler(_: ToolResult, __: bool) -> None:
        return None

    ctx.obj = {
        "learning_mode": learning_mode,
        "verbosity": verbosity,
        **overrides,
        "analysis_runner": _analysis_runner,
        "result_handler_analyze": _safe_handler,
    }

    commands.analyze(cast(typer.Context, ctx), experiment, host, port, open_browser)


def test_analyze_command_custom_overrides() -> None:
    """Test analyze command with custom runner and handler."""
    ctx = _fake_context(obj={})
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

    ctx.obj = {
        "learning_mode": True,
        "verbosity": VerbosityLevel.STANDARD,
        "analysis_runner": custom_runner,
        "result_handler_analyze": custom_handler,
    }

    commands.analyze(cast(typer.Context, ctx), experiment)

    assert custom_runner_called
    assert custom_handler_called


def test_command_functions_extract_dependencies() -> None:
    """Test that all command functions properly extract dependencies."""
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg()
    config_path = Path("/tmp/config.toml")

    captured: dict[str, object] = {
        "exp_config_path": None,
        "learning_engine": None,
        "learning_mode": None,
    }

    def load_experiment(_: str, exp_config_path: Path | None) -> object:
        captured["exp_config_path"] = exp_config_path
        shared = SimpleNamespace(config_path=config_path)
        return SimpleNamespace(
            prepare=SimpleNamespace(),
            train=SimpleNamespace(),
            sample=SimpleNamespace(),
            shared=shared,
        )

    def ensure_train_prerequisites(_: object) -> None:
        return None

    def ensure_sample_prerequisites(_: object) -> None:
        return None

    def run_prepare(
        experiment_name: str,
        _prepare_cfg: object,
        _cfg_path: Path,
        _shared: object,
        learning_engine: object | None,
    ) -> ToolResult:
        captured["learning_engine"] = learning_engine
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command=experiment_name,
            stdout="ok",
        )

    def _unreachable_run(
        *_args: object,
        **_kwargs: object,
    ) -> ToolResult:
        raise RuntimeError("unexpected")

    deps = CLIDependencies(
        load_experiment=load_experiment,
        ensure_train_prerequisites=ensure_train_prerequisites,
        ensure_sample_prerequisites=ensure_sample_prerequisites,
        run_prepare=run_prepare,
        run_train=_unreachable_run,
        run_sample=_unreachable_run,
    )

    def _safe_invoker(action: Callable[[], None]) -> None:
        action()

    def _safe_handler(_: ToolResult, learning_mode: bool) -> None:
        captured["learning_mode"] = learning_mode

    ctx.obj = {
        "exp_config": config_path,
        "learning_mode": True,
        "verbosity": VerbosityLevel.STANDARD,
        "cli_deps_prepare": deps,
        "run_invoker_prepare": _safe_invoker,
        "result_handler_prepare": _safe_handler,
    }

    with override_runtime_cli_dependencies(deps):
        commands.prepare(cast(typer.Context, ctx), experiment)

    assert captured["exp_config_path"] == config_path
    assert captured["learning_engine"] is not None
    assert captured["learning_mode"] is True
