from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from ml_playground.configuration.models import (
    PreparerConfig,
    SamplerConfig,
    SharedConfig,
    TrainerConfig,
)
from ml_playground.core.logging_protocol import LoggerLike
from ml_playground.runtime import helpers as rt_helpers
from ml_playground.runtime import runners as runtime_runners
import ml_playground.runtime.cli as _cli_pkg
from ml_playground.sampling.runner import Sampler as _DefaultSampler
from ml_playground.training.loop.runner import Trainer as _DefaultTrainer
from ml_playground.data_pipeline.preparer import (
    create_pipeline as _default_create_pipeline,
)
from ml_playground.runtime.cli.device import (
    global_device_setup as _default_device_setup,
)
from ml_playground.runtime.cli.deps import get_cli_dependencies
from ml_playground.runtime.cli.result import handle_tool_result, run_or_exit
from ml_playground.runtime.core.results import LearningModeEngine, ToolResult

RunInvoker = Callable[[Callable[[], None]], None]


def default_run_invoker(message: str) -> RunInvoker:
    def _invoke(action: Callable[[], None]) -> None:
        run_or_exit(action, keyboard_interrupt_msg=message)

    return _invoke


def _finalize_command_result(
    captured: ToolResult | None,
    *,
    category: str,
    command: str,
    handler: Callable[[ToolResult, bool], None],
    learning_mode: bool,
    call_handler_on_cancel: bool,
    cancel_message: str | None = None,
) -> ToolResult:
    if captured is not None:
        return captured

    if cancel_message:
        logging.getLogger("ml_playground.runtime.cli").info(cancel_message)

    fallback = ToolResult.create(
        success=False,
        exit_code=0,
        namespace="ml",
        category=category,
        command=command,
        stderr=(cancel_message or ""),
    )

    if call_handler_on_cancel:
        handler(fallback, learning_mode)

    return fallback


def run_prepare(
    experiment: str,
    prepare_cfg: PreparerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    *,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    hooks: runtime_runners.RuntimeRunHooks | None = None,
) -> ToolResult:
    handler = result_handler or handle_tool_result
    active_hooks = hooks or runtime_runners.RuntimeRunHooks(
        pipeline_factory=_default_create_pipeline,
        trainer_factory=_DefaultTrainer,
        sampler_factory=_DefaultSampler,
        device_setup=_default_device_setup,
        log_status=rt_helpers.log_command_status,
    )
    result = runtime_runners.run_prepare_impl(
        experiment,
        prepare_cfg,
        config_path,
        shared,
        learning_mode_engine,
        hooks=active_hooks,
    )
    handler(result, learning_mode)
    return result


def run_train(
    experiment: str,
    train_cfg: TrainerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    *,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    hooks: runtime_runners.RuntimeRunHooks | None = None,
) -> ToolResult:
    handler = result_handler or handle_tool_result
    active_hooks = hooks or runtime_runners.RuntimeRunHooks(
        pipeline_factory=_default_create_pipeline,
        trainer_factory=_DefaultTrainer,
        sampler_factory=_DefaultSampler,
        device_setup=_default_device_setup,
        log_status=log_command_status,
    )
    result = runtime_runners.run_train_impl(
        experiment,
        train_cfg,
        config_path,
        shared,
        learning_mode_engine,
        hooks=active_hooks,
    )
    handler(result, learning_mode)
    return result


def run_sample(
    experiment: str,
    sample_cfg: SamplerConfig,
    config_path: Path,
    shared: Any,
    learning_mode_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    *,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    hooks: runtime_runners.RuntimeRunHooks | None = None,
) -> ToolResult:
    handler = result_handler or handle_tool_result
    active_hooks = hooks or runtime_runners.RuntimeRunHooks(
        pipeline_factory=_default_create_pipeline,
        trainer_factory=_DefaultTrainer,
        sampler_factory=_DefaultSampler,
        device_setup=_default_device_setup,
        log_status=log_command_status,
    )
    result = runtime_runners.run_sample_impl(
        experiment,
        sample_cfg,
        config_path,
        shared,
        learning_mode_engine,
        hooks=active_hooks,
    )
    handler(result, learning_mode)
    return result


def run_prepare_command(
    experiment: str,
    exp_config_path: Path | None,
    *,
    deps: Any | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    run_invoker: RunInvoker | None = None,
) -> ToolResult:
    active_deps = deps or get_cli_dependencies()
    handler = result_handler or handle_tool_result
    captured: ToolResult | None = None

    def _action() -> None:
        nonlocal captured
        exp = active_deps.load_experiment(experiment, exp_config_path)
        result = active_deps.run_prepare(
            experiment,
            exp.prepare,
            exp.shared.config_path,
            exp.shared,
            learning_engine,
        )
        captured = result
        handler(result, learning_mode)

    cancel_message = "\nData preparation cancelled."
    invoker = run_invoker or default_run_invoker(cancel_message)
    invoker(_action)

    return _finalize_command_result(
        captured,
        category="prepare",
        command=experiment,
        handler=handler,
        learning_mode=learning_mode,
        call_handler_on_cancel=result_handler is not None,
        cancel_message=cancel_message.strip(),
    )


def run_train_command(
    experiment: str,
    exp_config_path: Path | None,
    *,
    deps: Any | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    run_invoker: RunInvoker | None = None,
) -> ToolResult:
    active_deps = deps or get_cli_dependencies()
    handler = result_handler or handle_tool_result
    captured: ToolResult | None = None

    def _action() -> None:
        nonlocal captured
        exp = active_deps.load_experiment(experiment, exp_config_path)
        active_deps.ensure_train_prerequisites(exp)
        result = active_deps.run_train(
            experiment,
            exp.train,
            exp.shared.config_path,
            exp.shared,
            learning_engine,
        )
        captured = result
        handler(result, learning_mode)

    cancel_message = "\nTraining cancelled."
    invoker = run_invoker or default_run_invoker(cancel_message)
    invoker(_action)

    return _finalize_command_result(
        captured,
        category="train",
        command=experiment,
        handler=handler,
        learning_mode=learning_mode,
        call_handler_on_cancel=result_handler is not None,
        cancel_message=cancel_message.strip(),
    )


def run_sample_command(
    experiment: str,
    exp_config_path: Path | None,
    *,
    deps: Any | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
    run_invoker: RunInvoker | None = None,
) -> ToolResult:
    active_deps = deps or get_cli_dependencies()
    handler = result_handler or handle_tool_result
    captured: ToolResult | None = None

    def _action() -> None:
        nonlocal captured
        exp = active_deps.load_experiment(experiment, exp_config_path)
        active_deps.ensure_sample_prerequisites(exp)
        result = active_deps.run_sample(
            experiment,
            exp.sample,
            exp.shared.config_path,
            exp.shared,
            learning_engine,
        )
        captured = result
        handler(result, learning_mode)

    cancel_message = "\nSampling cancelled."
    invoker = run_invoker or default_run_invoker(cancel_message)
    invoker(_action)

    return _finalize_command_result(
        captured,
        category="sample",
        command=experiment,
        handler=handler,
        learning_mode=learning_mode,
        call_handler_on_cancel=result_handler is not None,
        cancel_message=cancel_message.strip(),
    )


def run_analyze(
    experiment: str,
    host: str,
    port: int,
    open_browser: bool,
    learning_mode_engine: LearningModeEngine | None = None,
) -> ToolResult:
    return runtime_runners.run_analyze(
        experiment, host, port, open_browser, learning_mode_engine
    )


def log_directory(
    tag: str,
    dir_name: str,
    dir_path: Path | None,
    logger: LoggerLike,
) -> None:
    rt_helpers.log_directory(tag, dir_name, dir_path, logger)


def log_command_status(
    tag: str,
    shared: SharedConfig,
    out_dir: Path | None,
    logger: LoggerLike,
) -> None:
    """Log command status using the local cli.log_directory wrapper.

    This ensures tests overriding `cli.log_directory` affect behavior here.
    Swallows exceptions consistently with runtime.helpers.
    """
    # Use package-level override if present - kept for test isolation
    # Tests override this via monkeypatching to verify error handling
    pkg_log_directory = getattr(_cli_pkg, "log_directory", log_directory)
    try:
        pkg_log_directory(tag, "out_dir", out_dir, logger)
    except Exception:
        pass

    try:
        dataset_dir = shared.dataset_dir  # may raise
    except Exception:
        return

    try:
        pkg_log_directory(tag, "dataset_dir", dataset_dir, logger)
    except Exception:
        pass


def run_train_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: Any | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
) -> None:
    active_deps = deps or get_cli_dependencies()
    exp = active_deps.load_experiment(experiment, exp_config_path)
    active_deps.ensure_train_prerequisites(exp)
    result = active_deps.run_train(
        experiment,
        exp.train,
        exp.shared.config_path,
        exp.shared,
        learning_engine,
    )
    handler = result_handler or handle_tool_result
    handler(result, learning_mode)


def run_sample_cmd(
    experiment: str,
    exp_config_path: Path | None,
    deps: Any | None = None,
    learning_engine: LearningModeEngine | None = None,
    learning_mode: bool = False,
    result_handler: Callable[[ToolResult, bool], None] | None = None,
) -> None:
    active_deps = deps or get_cli_dependencies()
    exp = active_deps.load_experiment(experiment, exp_config_path)
    active_deps.ensure_sample_prerequisites(exp)
    result = active_deps.run_sample(
        experiment,
        exp.sample,
        exp.shared.config_path,
        exp.shared,
        learning_engine,
    )
    handler = result_handler or handle_tool_result
    handler(result, learning_mode)


__all__ = [
    "RunInvoker",
    "default_run_invoker",
    "run_prepare",
    "run_train",
    "run_sample",
    "run_prepare_command",
    "run_train_command",
    "run_sample_command",
    "run_analyze",
    "run_train_cmd",
    "run_sample_cmd",
    "log_directory",
    "log_command_status",
]
