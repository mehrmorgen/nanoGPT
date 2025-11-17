from __future__ import annotations

from typing import Any, Callable, Mapping, cast

import typer

from ml_playground.runtime.cli.deps import get_cli_dependencies
from ml_playground.runtime.cli.result import handle_tool_result
from ml_playground.runtime.cli.runners import (
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


OverridesMap = dict[str, Any]


def _coerce_overrides(overrides: object) -> OverridesMap:
    if isinstance(overrides, Mapping):
        typed_overrides = cast(Mapping[str, object], overrides)
        return dict(typed_overrides)
    return {}


def _select_override_value(overrides: OverridesMap, *keys: str) -> Any | None:
    for key in keys:
        if key in overrides:
            return overrides[key]
    return None


def _select_override_callable(
    overrides: OverridesMap, *keys: str
) -> Callable[..., Any] | None:
    for key in keys:
        candidate = overrides.get(key)
        if callable(candidate):
            return candidate
    return None


def prepare(ctx: typer.Context, experiment: ExperimentArg) -> None:
    """Prepare data for an experiment."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override = _select_override_value(
        overrides_map, "cli_deps_prepare", "cli_deps"
    )
    run_invoker = _select_override_callable(
        overrides_map, "run_invoker_prepare", "run_invoker"
    )
    result_handler = _select_override_callable(
        overrides_map, "result_handler_prepare", "result_handler"
    )

    run_prepare_command(
        experiment,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker if callable(run_invoker) else None,
        result_handler=result_handler if callable(result_handler) else None,
    )


def train(ctx: typer.Context, experiment: ExperimentArg) -> None:
    """Train a model for an experiment."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override = _select_override_value(overrides_map, "cli_deps_train", "cli_deps")
    run_invoker = _select_override_callable(
        overrides_map, "run_invoker_train", "run_invoker"
    )
    result_handler = _select_override_callable(
        overrides_map, "result_handler_train", "result_handler"
    )

    run_train_command(
        experiment,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker if callable(run_invoker) else None,
        result_handler=result_handler if callable(result_handler) else None,
    )


def sample(ctx: typer.Context, experiment: ExperimentArg) -> None:
    """Sample from a trained model."""
    exp_config_path = extract_exp_config(ctx)
    deps = get_cli_dependencies()

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    deps_override = _select_override_value(overrides_map, "cli_deps_sample", "cli_deps")
    run_invoker = _select_override_callable(
        overrides_map, "run_invoker_sample", "run_invoker"
    )
    result_handler = _select_override_callable(
        overrides_map, "result_handler_sample", "result_handler"
    )

    run_sample_command(
        experiment,
        exp_config_path,
        deps=deps_override or deps,
        learning_engine=learning_engine,
        learning_mode=learning_mode,
        run_invoker=run_invoker if callable(run_invoker) else None,
        result_handler=result_handler if callable(result_handler) else None,
    )


def analyze(
    ctx: typer.Context,
    experiment: ExperimentArg,
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8050),
    open_browser: bool = typer.Option(True),
) -> None:
    """Run analysis for an experiment."""
    from ml_playground.runtime.cli.runners import run_analyze

    learning_mode, verbosity, overrides = prepare_learning_context(ctx)
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None

    overrides_map = _coerce_overrides(overrides)
    handler = _select_override_callable(
        overrides_map, "result_handler_analyze", "result_handler"
    )
    analysis_runner = _select_override_callable(overrides_map, "analysis_runner")

    result_handler_fn: CallableResultHandler = (
        handler if callable(handler) else handle_tool_result
    )
    analysis_runner_fn = analysis_runner or run_analyze

    result = analysis_runner_fn(
        experiment,
        host,
        port,
        open_browser,
        learning_engine,
    )
    result_handler_fn(result, learning_mode)
