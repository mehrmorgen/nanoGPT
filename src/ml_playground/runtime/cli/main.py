from __future__ import annotations

from typing import Callable, TYPE_CHECKING

import typer
from typer.main import get_command

from ml_playground.experiments import registry
from ml_playground.runtime.cli.app import EchoFunc as EchoFunc
from ml_playground.runtime.cli.app import app as app
from ml_playground.runtime.cli.app import global_options as global_options
from ml_playground.runtime.cli.commands import analyze as analyze
from ml_playground.runtime.cli.commands import prepare as prepare
from ml_playground.runtime.cli.commands import sample as sample
from ml_playground.runtime.cli.commands import train as train
from ml_playground.runtime.core.bootstrap import CLIDependencies as CLIDependencies
from ml_playground.runtime.core.bootstrap import (
    configure_runtime_cli_dependencies as configure_cli_dependencies,
)
from ml_playground.runtime.core.bootstrap import (
    reset_runtime_cli_dependencies as reset_cli_dependencies,
)
from ml_playground.runtime.core.bootstrap import (
    get_runtime_cli_dependencies as get_cli_dependencies,
)
from ml_playground.runtime.core.bootstrap import (
    override_runtime_cli_dependencies as override_cli_dependencies,
)
from ml_playground.configuration import cli as config_cli
from ml_playground.runtime import runners as runtime_runners
from ml_playground.runtime.cli.device import global_device_setup as global_device_setup
from ml_playground.runtime.cli.result import handle_tool_result as handle_tool_result
from ml_playground.runtime.cli.result import run_or_exit as run_or_exit
from ml_playground.runtime.cli.runners import RunInvoker as RunInvoker
from ml_playground.runtime.cli.runners import default_run_invoker as default_run_invoker
from ml_playground.runtime.cli.runners import log_command_status as log_command_status
from ml_playground.runtime.cli.runners import log_directory as log_directory
from ml_playground.runtime.cli.runners import run_analyze as run_analyze
from ml_playground.runtime.cli.runners import run_prepare as run_prepare
from ml_playground.runtime.cli.runners import run_prepare_command as run_prepare_command
from ml_playground.runtime.cli.runners import run_sample as run_sample
from ml_playground.runtime.cli.runners import run_sample_cmd as run_sample_cmd
from ml_playground.runtime.cli.runners import run_sample_command as run_sample_command
from ml_playground.runtime.cli.runners import run_train as run_train
from ml_playground.runtime.cli.runners import run_train_cmd as run_train_cmd
from ml_playground.runtime.cli.runners import run_train_command as run_train_command
from ml_playground.runtime.cli.typer_helpers import ExperimentArg as ExperimentArg
from ml_playground.runtime.cli.typer_helpers import (
    extract_exp_config as extract_exp_config,
)
from ml_playground.runtime.cli.typer_helpers import (
    complete_experiments as complete_experiments,
)
from ml_playground.runtime.core.results import LearningModeEngine as LearningModeEngine
from ml_playground.runtime.core.results import ToolResult as ToolResult
from ml_playground.runtime.core.results import VerbosityLevel as VerbosityLevel
from ml_playground.training.loop.runner import Trainer as CoreTrainer
from ml_playground.data_pipeline.preparer import (
    create_pipeline as create_pipeline,
)
from ml_playground.runtime.runners import RuntimeRunHooks as RuntimeRunHooks


def default_cli_dependencies() -> CLIDependencies:
    """Create default CLI dependencies for the runtime CLI system."""
    return CLIDependencies(
        load_experiment=config_cli.load_experiment,
        ensure_train_prerequisites=config_cli.ensure_train_prerequisites,
        ensure_sample_prerequisites=config_cli.ensure_sample_prerequisites,
        run_prepare=runtime_runners.run_prepare_impl,
        run_train=runtime_runners.run_train_impl,
        run_sample=runtime_runners.run_sample_impl,
    )


if TYPE_CHECKING:  # import for typing only
    pass

__all__ = [
    "app",
    "global_options",
    "prepare",
    "train",
    "sample",
    "analyze",
    "run_or_exit",
    "handle_tool_result",
    "log_directory",
    "log_command_status",
    "run_prepare",
    "run_prepare_command",
    "run_train",
    "run_train_cmd",
    "run_train_command",
    "run_sample",
    "run_sample_cmd",
    "run_sample_command",
    "run_analyze",
    "default_run_invoker",
    "RunInvoker",
    "RuntimeRunHooks",
    "CLIDependencies",
    "configure_cli_dependencies",
    "default_cli_dependencies",
    "get_cli_dependencies",
    "override_cli_dependencies",
    "reset_cli_dependencies",
    "global_device_setup",
    "ExperimentArg",
    "extract_exp_config",
    "complete_experiments",
    "LearningModeEngine",
    "ToolResult",
    "VerbosityLevel",
    "CoreTrainer",
    "main",
    "main_entry",
    "get_command",
]


def main(argv: list[str] | None = None) -> int | None:
    """Programmatic entry point for the runtime CLI."""
    registry.load_preparers()
    cmd = get_command(app)
    return cmd.main(args=argv, standalone_mode=False)


def main_entry(
    app_runner: Callable[[], None] | None = None,
    *,
    echo: EchoFunc | None = None,
) -> None:
    """Console entry point wrapping the Typer application."""
    runner = app_runner or app

    def _default_echo(message: str, *, err: bool = False) -> object:
        return typer.echo(message, err=err)

    echo_func: EchoFunc = echo or _default_echo

    try:
        runner()
    except KeyboardInterrupt:
        echo_func("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except Exception as exc:
        echo_func(f"Runtime CLI execution failed: {exc}", err=True)
        raise typer.Exit(1) from exc
