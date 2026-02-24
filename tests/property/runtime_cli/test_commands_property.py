"""Property-based tests for runtime/cli/commands module.

Tests command functions, override handling, and dependency injection
using Hypothesis to discover edge cases in command execution.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol, cast

from hypothesis import given, settings
from hypothesis import strategies as st
import typer

from ml_playground.runtime_cli import main as commands
from ml_playground.framework.runtime.core.bootstrap import (
    CLIDependencies,
    override_cli_dependencies,
)
from ml_playground.framework.runtime.core.results import (
    LearningModeEngine,
    ToolResult,
    VerbosityLevel,
)


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
        overrides["cli_deps"] = _fake_dependencies()

    if draw(st.booleans()):
        overrides["run_invoker"] = cast(_OverrideCallable, _noop_override)
    if draw(st.booleans()):
        overrides["result_handler"] = cast(_OverrideCallable, _noop_override)

    # Command-specific overrides
    command = draw(st.sampled_from(["prepare", "train", "sample", "analyze"]))
    if draw(st.booleans()):
        overrides[f"cli_deps_{command}"] = _fake_dependencies()
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
    ctx = SimpleNamespace(**kwargs)
    if "ensure_object" not in ctx.__dict__:

        def ensure_object(model_type: type) -> object:
            if "obj" not in ctx.__dict__ or ctx.obj is None:
                ctx.obj = model_type()
            return ctx.obj

        ctx.ensure_object = ensure_object
    return ctx


def _fake_experiment_arg(name: str = "test_exp") -> str:
    """Create a fake experiment argument."""
    return name  # ExperimentArg is a str | None


def _fake_dependencies() -> CLIDependencies:
    """Create fake CLI dependencies."""
    deps = CLIDependencies(
        load_experiment=cast(
            Any,
            lambda *a, **k: __import__("types").SimpleNamespace(
                prepare=__import__("types").SimpleNamespace(),
                training=__import__("types").SimpleNamespace(),
                sampling=__import__("types").SimpleNamespace(),
                metadata=__import__("types").SimpleNamespace(
                    config_path=Path(__import__("tempfile").mkdtemp()) / "cfg",
                    train_out_dir=Path(__import__("tempfile").mkdtemp()) / "train_out",
                    sample_out_dir=Path(__import__("tempfile").mkdtemp())
                    / "sample_out",
                ),
            ),
        ),
        ensure_train_prerequisites=lambda *a, **k: None,
        ensure_sample_prerequisites=lambda *a, **k: None,
        run_prepare=lambda *a, **k: ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="prepare",
            command="demo",
            stdout="ok",
        ),
        run_train=lambda *a, **k: ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="train",
            command="test",
            stdout="ok",
        ),
        run_sample=lambda *a, **k: ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command="test",
            stdout="ok",
        ),
        run_analyze=lambda *a, **k: ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command="test",
            stdout="ok",
        ),
        global_device_setup=lambda *a: None,
        log_command_status=lambda *a: None,
        handle_tool_result=lambda _r, _l: None,
        create_pipeline=lambda *a: None,
        trainer_factory=lambda *a: None,
        sampler_factory=lambda *a: None,
    )
    return deps


def _make_cli_deps(config_path: Path) -> CLIDependencies:
    metadata = SimpleNamespace(
        config_path=config_path,
        train_out_dir=config_path.parent / "train_out",
        sample_out_dir=config_path.parent / "sample_out",
    )
    exp = SimpleNamespace(
        prepare=SimpleNamespace(),
        training=SimpleNamespace(),
        sampling=SimpleNamespace(),
        metadata=metadata,
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
        _metadata: object,
        deps: CLIDependencies,
        _learning_engine: object | None = None,
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
        _metadata: object,
        deps: CLIDependencies,
        _learning_engine: object | None = None,
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
        _metadata: object,
        deps: CLIDependencies,
        _learning_engine: object | None = None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="sample",
            command=experiment,
            stdout="ok",
        )

    def run_analyze(
        experiment: str,
        _host: str,
        _port: int,
        _open_browser: bool,
        _learning_engine: object | None = None,
    ) -> ToolResult:
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
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
        run_analyze=run_analyze,
        global_device_setup=lambda *a: None,
        log_command_status=lambda *a: None,
        handle_tool_result=lambda _r, _l: None,
        create_pipeline=lambda *a: None,
        trainer_factory=lambda *a: None,
        sampler_factory=lambda *a: None,
    )


def _fake_functions() -> dict[str, object]:
    """Create fake functions for patching."""

    def _extract_exp_config(ctx: object) -> Path:
        import tempfile

        config_path = Path(tempfile.mkdtemp()) / "config.toml"
        obj = getattr(ctx, "obj", None)
        if isinstance(obj, dict):
            obj["exp_config"] = config_path
        elif obj is not None:
            obj.exp_config = config_path
        return config_path

    def _get_cli_dependencies() -> CLIDependencies:
        return _fake_dependencies()

    def _prepare_learning_context(
        _ctx: object,
    ) -> tuple[bool, VerbosityLevel, dict[str, object]]:
        return (False, VerbosityLevel.STANDARD, {})

    def _run_prepare_cmd(*args: object, **kwargs: object) -> None:
        return None

    def _run_train_cmd(*args: object, **kwargs: object) -> None:
        return None

    def _run_sample_cmd(*args: object, **kwargs: object) -> None:
        return None

    def _handle_tool_result(*args: object, **kwargs: object) -> None:
        return None

    return {
        "extract_exp_config": _extract_exp_config,
        "get_cli_dependencies": _get_cli_dependencies,
        "prepare_learning_context": _prepare_learning_context,
        "run_prepare_cmd": _run_prepare_cmd,
        "run_train_cmd": _run_train_cmd,
        "run_sample_cmd": _run_sample_cmd,
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
    extract_fn = cast(Callable[[object], Path], fake_funcs["extract_exp_config"])
    config_path = extract_fn(ctx)
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

    with override_cli_dependencies(deps):
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
    extract_fn = cast(Callable[[object], Path], fake_funcs["extract_exp_config"])
    config_path = extract_fn(ctx)
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

    with override_cli_dependencies(deps):
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
    extract_fn = cast(Callable[[object], Path], fake_funcs["extract_exp_config"])
    config_path = extract_fn(ctx)
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

    with override_cli_dependencies(deps):
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
        "result_handler_analyze": _safe_handler,
    }

    deps = CLIDependencies(
        run_analyze=_analysis_runner,
        handle_tool_result=_safe_handler,
    )

    with override_cli_dependencies(deps):
        commands.analyze(cast(typer.Context, ctx), experiment, host, port, open_browser)


def test_analyze_command_custom_overrides() -> None:
    """Test analyze command with custom runner and handler."""
    ctx = _fake_context(obj={})
    experiment = _fake_experiment_arg("analyze_custom")

    custom_runner_called = False
    custom_handler_called = False

    custom_runner_called = False
    custom_handler_called = False

    def custom_runner(*args: Any, **kwargs: Any) -> ToolResult:
        nonlocal custom_runner_called
        custom_runner_called = True
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="ml",
            category="analyze",
            command="custom",
        )

    def custom_handler(_result: ToolResult, _learning_mode: bool) -> None:
        nonlocal custom_handler_called
        custom_handler_called = True

    ctx.obj = {
        "learning_mode": True,
        "verbosity": VerbosityLevel.STANDARD,
        "result_handler_analyze": custom_handler,
    }

    deps = CLIDependencies(
        run_analyze=custom_runner,
        handle_tool_result=custom_handler,
    )

    with override_cli_dependencies(deps):
        commands.analyze(cast(typer.Context, ctx), experiment, "127.0.0.1", 5432, False)

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
        metadata = SimpleNamespace(config_path=config_path)
        return SimpleNamespace(
            prepare=SimpleNamespace(),
            training=SimpleNamespace(),
            sampling=SimpleNamespace(),
            metadata=metadata,
        )

    def ensure_train_prerequisites(_: object) -> None:
        return None

    def ensure_sample_prerequisites(_: object) -> None:
        return None

    def run_prepare(
        experiment_name: str,
        _prepare_cfg: object,
        _cfg_path: Path,
        _metadata: object,
        deps: CLIDependencies,
        learning_engine: Any | None = None,
    ) -> ToolResult:
        captured["learning_engine"] = learning_engine
        if learning_engine:
            learning_engine.explain_command(
                command=experiment_name,
                context="test",
                category="prepare",
                executed_commands=[],
            )
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

    class MockEngine(LearningModeEngine):
        def __init__(self) -> None:
            super().__init__(VerbosityLevel.STANDARD)
            self.called = False

        def explain_command(
            self,
            command: str,
            context: str,
            category: str,
            executed_commands: Iterable[str] | None = None,
            **kwargs: Any,
        ) -> Any:
            self.called = True
            return None

    engine = MockEngine()
    ctx.obj = {
        "exp_config": config_path,
        "learning_mode": True,
        "verbosity": VerbosityLevel.STANDARD,
        "learning_engine": engine,
        "cli_deps": deps,
    }

    with override_cli_dependencies(deps):
        commands.prepare(cast(typer.Context, ctx), experiment)

    assert captured["exp_config_path"] == config_path
    assert engine.called is True
