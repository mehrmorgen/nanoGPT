from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, cast

import click
import typer
from typer.main import get_command

from ml_playground.framework.configuration import loading as config_loading
from ml_playground.framework.experiment_registry import registry
from ml_playground.framework.runtime.core.bootstrap import CLIDependencies
from ml_playground.framework.runtime.core.results import (
    LearningModeEngine,
    VerbosityLevel,
)
from ml_playground.framework.core.project_config import get_default_host

from .typer_helpers import extract_exp_config
from .runners import (
    create_default_cli_dependencies,
    get_cli_dependencies,
    run_prepare_cmd,
    run_sample_cmd,
    run_train_cmd,
)
from .typer_helpers import run_or_exit

EXPERIMENT_HELP = "Experiment name (directory in src/ml_playground/experiments)"


def _complete_experiments(incomplete: str) -> List[str]:
    return config_loading.list_experiments_with_config(incomplete)


ExperimentArg = Annotated[
    str,
    typer.Argument(help=EXPERIMENT_HELP, autocompletion=_complete_experiments),
]

app = typer.Typer(
    invoke_without_command=True,
    no_args_is_help=False,
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
        Optional[Path],
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
    _apply_global_options(
        ctx,
        exp_config,
        learning_mode,
        verbosity,
    )


def _apply_global_options(
    ctx: typer.Context,
    exp_config: Path | None,
    learning_mode: bool,
    verbosity: int | VerbosityLevel,
) -> None:
    """Shared global options handler used by Typer callback and programmatic callers."""

    logger = logging.getLogger("ml_playground.cli")
    echo = typer.echo

    if exp_config is not None:
        if not exp_config.exists():
            msg = f"Config file not found: {exp_config}"
            logger.error(msg)
            echo(msg, err=True)
            raise typer.Exit(2)
        if not exp_config.is_file():
            msg = f"Config path is not a file: {exp_config}"
            logger.error(msg)
            echo(msg, err=True)
            raise typer.Exit(2)

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        ctx.ensure_object(dict)
    except (AttributeError, TypeError):
        return

    if not isinstance(ctx.obj, dict):
        ctx.obj = {}

    ctx_dict = cast(Dict[str, Any], ctx.obj)

    verbosity_level = (
        verbosity
        if isinstance(verbosity, VerbosityLevel)
        else VerbosityLevel(verbosity)
    )

    ctx_dict["exp_config"] = exp_config
    if learning_mode:
        ctx_dict["learning_mode"] = True
    if verbosity_level != VerbosityLevel.STANDARD:
        ctx_dict["verbosity"] = verbosity_level

    try:
        context = cast(click.Context | None, click.get_current_context(silent=True))
    except (TypeError, RuntimeError):
        context = cast(click.Context | None, click.get_current_context())

    if context is not None and getattr(context, "invoked_subcommand", None) is None:
        echo("Welcome to ML Playground runtime CLI!", err=True)
        echo(
            "No workflow command was provided. Try `uv run ml-playground prepare <experiment>`.",
            err=True,
        )
        echo("", err=True)
        help_text: str = cast(str, getattr(context, "get_help", lambda: "")())
        if help_text:
            echo(help_text, err=True)
        raise typer.Exit(2)


def _deps_from_ctx(ctx: typer.Context) -> CLIDependencies:
    obj = getattr(ctx, "obj", None)
    if isinstance(obj, dict):
        typed_obj = cast(dict[str, object], obj)
        deps: object = typed_obj.get("cli_deps")
        if deps:
            return cast(CLIDependencies, deps)
    return get_cli_dependencies()


@app.command()
def prepare(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    learning_mode, learning_engine = _learning_from_ctx(ctx)
    deps = _deps_from_ctx(ctx)
    run_or_exit(
        lambda: run_prepare_cmd(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Data preparation cancelled",
    )


@app.command()
def train(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    learning_mode, learning_engine = _learning_from_ctx(ctx)
    deps = _deps_from_ctx(ctx)
    run_or_exit(
        lambda: run_train_cmd(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Training cancelled",
    )


@app.command()
def sample(ctx: typer.Context, experiment: ExperimentArg) -> None:
    exp_config_path = extract_exp_config(ctx)
    learning_mode, learning_engine = _learning_from_ctx(ctx)
    deps = _deps_from_ctx(ctx)
    run_or_exit(
        lambda: run_sample_cmd(
            experiment, exp_config_path, deps, learning_engine, learning_mode
        ),
        keyboard_interrupt_msg="Sampling cancelled",
    )


def _get_default_host() -> str:
    try:
        return get_default_host()
    except (ValueError, TypeError):
        return "127.0.0.1"


@app.command()
def analyze(
    ctx: typer.Context,
    experiment: ExperimentArg,
    host: str = typer.Option(
        default_factory=_get_default_host,
        help="Host for the analysis server (not implemented)",
    ),
    port: int = typer.Option(
        8050, help="Port for the analysis server (not implemented)"
    ),
    open_browser: bool = typer.Option(
        True, help="Whether to open the browser automatically (not implemented)"
    ),
) -> None:
    # TODO Remove placeholder: implement analysis server (e.g., Dash/Plotly) that visualizes
    # training and sampling artifacts; should stream from experiment outputs and optionally
    # auto-open a browser when ready.
    learning_mode, learning_engine = _learning_from_ctx(ctx)
    deps = _deps_from_ctx(ctx)

    run_or_exit(
        lambda: deps.handle_tool_result(
            deps.run_analyze(experiment, host, port, open_browser, learning_engine),
            learning_mode,
        ),
        keyboard_interrupt_msg=f"Analysis for {experiment} cancelled by user",
    )


def _learning_from_ctx(ctx: typer.Context) -> tuple[bool, LearningModeEngine | None]:
    # Use getattr to avoid Any tracking on ctx.obj
    ctx_obj_raw: object = getattr(ctx, "obj", None)

    learning_mode: bool = False
    verbosity_raw: object = VerbosityLevel.STANDARD
    injected_engine: LearningModeEngine | None = None

    # Support both dict and objects with attributes
    if isinstance(ctx_obj_raw, dict):
        ctx_dict = cast(Dict[str, object], ctx_obj_raw)
        learning_mode = bool(ctx_dict.get("learning_mode", False))
        verbosity_raw = ctx_dict.get("verbosity", VerbosityLevel.STANDARD)
        injected_engine = cast(
            Optional[LearningModeEngine], ctx_dict.get("learning_engine")
        )
    elif ctx_obj_raw is not None:
        learning_mode = bool(getattr(ctx_obj_raw, "learning_mode", False))
        verbosity_raw = getattr(ctx_obj_raw, "verbosity", VerbosityLevel.STANDARD)
        injected_engine = cast(
            Optional[LearningModeEngine], getattr(ctx_obj_raw, "learning_engine", None)
        )

    if injected_engine is not None:
        if not issubclass(type(injected_engine), LearningModeEngine):
            raise TypeError("learning_engine must be a LearningModeEngine")
        return learning_mode, injected_engine

    verbosity: VerbosityLevel
    if isinstance(verbosity_raw, VerbosityLevel):
        verbosity = verbosity_raw
    else:
        try:
            verbosity = VerbosityLevel(verbosity_raw)
        except (ValueError, TypeError):
            verbosity = VerbosityLevel.STANDARD

    learning_engine = LearningModeEngine(verbosity) if learning_mode else None
    return learning_mode, learning_engine


def main(argv: list[str] | None = None) -> int | None:
    """Programmatic entry point used by tests; returns status instead of exiting."""
    registry.load_preparers()
    cmd = get_command(app)
    if argv is not None and len(argv) == 0:
        raise click.exceptions.NoArgsIsHelpError(click.Context(cmd))
    main_fn_raw: object = getattr(cmd, "main", None)
    if callable(main_fn_raw):
        res: object = main_fn_raw(args=argv or [], standalone_mode=False)
        return cast(Optional[int], res)
    return None


def main_entry() -> None:
    """Console entry point wrapping the Typer application."""
    deps = create_default_cli_dependencies()
    echo_fn = cast(Any, deps.echo if deps.echo is not None else typer.echo)  # type: ignore[reportAny]
    try:
        app()  # type: ignore[reportAny]
    except (typer.Exit, click.exceptions.Exit):
        # Preserve explicit Typer/Click exit codes
        raise
    except KeyboardInterrupt:
        if echo_fn is not None:
            echo_fn("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except Exception as exc:
        if echo_fn is not None:
            echo_fn(f"Runtime CLI execution failed: {exc}", err=True)
        raise typer.Exit(1) from exc


if __name__ == "__main__":
    main_entry()

__all__ = [
    "app",
    "main",
    "main_entry",
    "prepare",
    "train",
    "sample",
    "analyze",
]
