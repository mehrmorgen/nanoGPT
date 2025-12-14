from __future__ import annotations

from typing import Callable, Mapping, cast

import typer

from ml_playground.runtime.core.bootstrap import (
    CLIDependencies,
    get_runtime_cli_dependencies as get_cli_dependencies,
)
from ml_playground.runtime.cli.result import handle_tool_result
from ml_playground.runtime.cli.runners import (
    RunInvoker,
    run_prepare_command,
    run_sample_command,
    run_train_command,
)
from ml_playground.runtime.cli.typer_helpers import (
    ExperimentArg,
    extract_exp_config,
    prepare_learning_context,
)
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult


CallableResultHandler = Callable[[ToolResult, bool], None]


OverridesMap = dict[str, object]


def _coerce_overrides(overrides: object) -> OverridesMap:
    if isinstance(overrides, Mapping):
        typed_overrides = cast(Mapping[str, object], overrides)
        return dict(typed_overrides)
    return {}


def _select_override_value(overrides: OverridesMap, *keys: str) -> object | None:
    for key in keys:
        if key in overrides:
            return overrides[key]
    return None


def _select_override_callable(
    overrides: OverridesMap, *keys: str
) -> Callable[..., object] | None:
    for key in keys:
        candidate = overrides.get(key)
        if callable(candidate):
            return candidate
    return None


def _ensure_experiment_provided(ctx: typer.Context, experiment: ExperimentArg) -> str:
    """Ensure an experiment argument is provided; otherwise show help and exit.

    This converts the optional ExperimentArg into a concrete str for command logic
    while providing a consistent "error + full help" experience when the argument
    is missing.
    """
    if experiment is None:
        typer.echo("Missing argument 'EXPERIMENT'.", err=True)
        typer.echo("", err=True)
        typer.echo(ctx.get_help(), err=True)
        raise typer.Exit(2)
    return experiment


def prepare(ctx: typer.Context, experiment: ExperimentArg = None) -> None:
    """Prepare data for an experiment."""
    experiment_name = _ensure_experiment_provided(ctx, experiment)
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override_value = _select_override_value(
        overrides_map, "cli_deps_prepare", "cli_deps"
    )
    deps_override = (
        deps_override_value
        if isinstance(deps_override_value, CLIDependencies)
        else None
    )
    run_invoker_value = _select_override_callable(
        overrides_map, "run_invoker_prepare", "run_invoker"
    )
    result_handler_value = _select_override_callable(
        overrides_map, "result_handler_prepare", "result_handler"
    )
    run_invoker: RunInvoker | None = (
        cast(RunInvoker, run_invoker_value) if callable(run_invoker_value) else None
    )
    result_handler: CallableResultHandler | None = (
        cast(CallableResultHandler, result_handler_value)
        if callable(result_handler_value)
        else None
    )

    run_prepare_command(
        experiment_name,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker,
        result_handler=result_handler,
    )


def train(ctx: typer.Context, experiment: ExperimentArg = None) -> None:
    """Train a model for an experiment."""
    experiment_name = _ensure_experiment_provided(ctx, experiment)
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override_value = _select_override_value(
        overrides_map, "cli_deps_train", "cli_deps"
    )
    deps_override = (
        deps_override_value
        if isinstance(deps_override_value, CLIDependencies)
        else None
    )
    run_invoker_value = _select_override_callable(
        overrides_map, "run_invoker_train", "run_invoker"
    )
    result_handler_value = _select_override_callable(
        overrides_map, "result_handler_train", "result_handler"
    )
    run_invoker: RunInvoker | None = (
        cast(RunInvoker, run_invoker_value) if callable(run_invoker_value) else None
    )
    result_handler: CallableResultHandler | None = (
        cast(CallableResultHandler, result_handler_value)
        if callable(result_handler_value)
        else None
    )

    run_train_command(
        experiment_name,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker,
        result_handler=result_handler,
    )


def sample(ctx: typer.Context, experiment: ExperimentArg = None) -> None:
    """Sample from a trained model."""
    experiment_name = _ensure_experiment_provided(ctx, experiment)
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override_value = _select_override_value(
        overrides_map, "cli_deps_sample", "cli_deps"
    )
    deps_override = (
        deps_override_value
        if isinstance(deps_override_value, CLIDependencies)
        else None
    )
    run_invoker_value = _select_override_callable(
        overrides_map, "run_invoker_sample", "run_invoker"
    )
    result_handler_value = _select_override_callable(
        overrides_map, "result_handler_sample", "result_handler"
    )
    run_invoker: RunInvoker | None = (
        cast(RunInvoker, run_invoker_value) if callable(run_invoker_value) else None
    )
    result_handler: CallableResultHandler | None = (
        cast(CallableResultHandler, result_handler_value)
        if callable(result_handler_value)
        else None
    )

    run_sample_command(
        experiment_name,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker,
        result_handler=result_handler,
    )


def analyze(
    ctx: typer.Context,
    experiment: ExperimentArg = None,
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8050),
    open_browser: bool = typer.Option(True),
) -> None:
    """Run analysis for an experiment."""
    experiment_name = _ensure_experiment_provided(ctx, experiment)
    from ml_playground.runtime.cli.runners import run_analyze

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    handler_value = _select_override_callable(
        overrides_map, "result_handler_analyze", "result_handler"
    )
    analysis_runner_value = _select_override_callable(overrides_map, "analysis_runner")

    result_handler_fn: CallableResultHandler = (
        cast(CallableResultHandler, handler_value)
        if callable(handler_value)
        else handle_tool_result
    )
    analysis_runner_fn = (
        cast(Callable[..., ToolResult], analysis_runner_value)
        if callable(analysis_runner_value)
        else run_analyze
    )

    result = analysis_runner_fn(
        experiment_name,
        host,
        port,
        open_browser,
        learning_engine,
    )
    result_handler_fn(result, learning_mode)
