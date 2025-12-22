from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Dict, List, cast
import importlib
import sys

import click
import typer
from typer.main import get_command

from ml_playground.configuration import loading as config_loading
from ml_playground.experiments import registry
from ml_playground.runtime.core.results import LearningModeEngine, VerbosityLevel

from .typer_helpers import extract_exp_config

__all__ = ["app", "main", "main_entry", "get_command"]

EXPERIMENT_HELP = "Experiment name (directory in src/ml_playground/experiments)"


def _complete_experiments(incomplete: str) -> List[str]:
    return config_loading.list_experiments_with_config(incomplete)


ExperimentArg = Annotated[
    str,
    typer.Argument(help=EXPERIMENT_HELP, autocompletion=_complete_experiments),
]

app = typer.Typer(
    no_args_is_help=True,
    help=(
        "ML Playground CLI: prepare data, train models, sample outputs, and export models.\n"
        "This CLI loads and validates TOML configs and injects the resulting configuration\n"
        "objects into experiment code. Experiments must not read TOML directly."
    ),
)


@app.callback()
def global_options(
    ctx: typer.Context,
    exp_config: Annotated[
        Path | None,
        typer.Option(
            "--exp-config",
            help=(
                "Path to an experiment-specific config TOML. When provided, it replaces "
                "the experiment's config.toml. default_config.toml is still loaded first."
            ),
        ),
    ] = None,
    learning_mode: Annotated[
        bool,
        typer.Option(
            "--learning-mode",
            help="Enable educational explanations for ML workflow operations",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            help="Learning mode verbosity: 0=minimal, 1=standard, 2=comprehensive",
            min=0,
            max=2,
        ),
    ] = 1,
) -> None:
    """Global options applied to all subcommands."""
    if exp_config is not None and not exp_config.exists():
        logger = logging.getLogger(__name__)
        msg = f"Config file not found: {exp_config}"
        logger.error(msg)
        typer.echo(msg, err=True)
        raise typer.Exit(2)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        ctx.ensure_object(dict)
    except (AttributeError, TypeError):
        return

    ctx_dict: Dict[str, Any] = {}
    if isinstance(ctx.obj, dict):
        ctx_dict = cast(Dict[str, Any], ctx.obj)
    ctx.obj = ctx_dict

    verbosity_level = VerbosityLevel(verbosity)

    ctx_dict["exp_config"] = exp_config
    if learning_mode:
        ctx_dict["learning_mode"] = True
    if verbosity_level != VerbosityLevel.STANDARD:
        ctx_dict["verbosity"] = verbosity_level

    context = click.get_current_context(silent=True)
    if context is not None and context.invoked_subcommand is None:
        typer.echo("Welcome to ML Playground runtime CLI!", err=True)
        typer.echo(
            "No workflow command was provided. Try `uv run ml-playground prepare <experiment>`.",
            err=True,
        )
        typer.echo("", err=True)
        typer.echo(context.get_help(), err=True)
        raise typer.Exit(0)


@app.command()
def prepare(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    cli_pkg = _cli_module()
    learning_mode, learning_engine = _learning_from_ctx(ctx)

    runner = cli_pkg.run_prepare_cmd
    deps = cli_pkg.get_cli_dependencies()

    cli_pkg.run_or_exit(
        lambda: runner(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Data preparation cancelled",
    )


@app.command()
def train(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    cli_pkg = _cli_module()
    learning_mode, learning_engine = _learning_from_ctx(ctx)

    runner = cli_pkg.run_train_cmd
    deps = cli_pkg.get_cli_dependencies()

    cli_pkg.run_or_exit(
        lambda: runner(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Training cancelled",
    )


@app.command()
def sample(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    cli_pkg = _cli_module()
    learning_mode, learning_engine = _learning_from_ctx(ctx)

    runner = cli_pkg.run_sample_cmd
    deps = cli_pkg.get_cli_dependencies()

    cli_pkg.run_or_exit(
        lambda: runner(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Sampling cancelled",
    )


@app.command()
def analyze(
    ctx: typer.Context,
    experiment: ExperimentArg,
    host: str = typer.Option(
        "127.0.0.1", help="Host for the analysis server (not implemented)"
    ),
    port: int = typer.Option(
        8050, help="Port for the analysis server (not implemented)"
    ),
    open_browser: bool = typer.Option(
        True, help="Whether to open the browser automatically (not implemented)"
    ),
) -> None:
    learning_mode, learning_engine = _learning_from_ctx(ctx)
    cli_pkg = _cli_module()
    runner = cli_pkg.run_analyze
    result = runner(experiment, host, port, open_browser, learning_engine)
    cli_pkg.handle_tool_result(result, learning_mode)


def _learning_from_ctx(ctx: typer.Context) -> tuple[bool, LearningModeEngine | None]:
    learning_mode = ctx.obj.get("learning_mode", False) if ctx.obj else False
    verbosity = (
        ctx.obj.get("verbosity", VerbosityLevel.STANDARD)
        if ctx.obj
        else VerbosityLevel.STANDARD
    )
    learning_engine = LearningModeEngine(verbosity) if learning_mode else None
    return learning_mode, learning_engine


def main(argv: list[str] | None = None) -> int | None:
    """Programmatic entry point used by tests; does not sys.exit."""
    registry.load_preparers()
    cli_pkg = _cli_module()
    cmd = cli_pkg.get_command(cli_pkg.app)
    return cmd.main(args=argv or [], standalone_mode=False)


def main_entry() -> None:
    """Console entry point wrapping the Typer application."""
    cli_pkg = _cli_module()
    try:
        cli_pkg.app()
    except KeyboardInterrupt:
        cli_pkg.typer.echo("\nOperation cancelled by user", err=True)
        raise cli_pkg.typer.Exit(1)
    except Exception as exc:  # pragma: no cover - defensive for console entry point
        cli_pkg.typer.echo(f"Runtime CLI execution failed: {exc}", err=True)
        raise cli_pkg.typer.Exit(1) from exc


def _cli_module():
    mod = sys.modules.get("ml_playground.runtime.cli")
    if mod is None:
        mod = importlib.import_module("ml_playground.runtime.cli")
    return mod
