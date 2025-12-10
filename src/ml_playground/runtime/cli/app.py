from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Callable, Protocol, cast

import click
import typer

from . import commands
from ml_playground.runtime.core.results import VerbosityLevel

app = typer.Typer(
    no_args_is_help=True,
    help=(
        "ML Playground CLI: prepare data, train models, sample outputs, and export models.\n"
        "This CLI loads and validates TOML configs and injects the resulting configuration\n"
        "objects into experiment code. Use --exp-config to point to an experiment-specific\n"
        "config TOML; experiments must not read TOML directly."
    ),
)


class EchoFunc(Protocol):
    def __call__(self, message: str, *, err: bool = False) -> object: ...


class LoggerFactory(Protocol):
    def __call__(self, name: str) -> logging.Logger: ...


ContextGetter = Callable[..., Any]


def _default_echo(message: str, *, err: bool = False) -> object:
    return typer.echo(message, err=err)


def _apply_global_options(
    ctx: typer.Context,
    exp_config: Path | None,
    learning_mode: bool,
    verbosity: int | VerbosityLevel,
    *,
    context_getter: ContextGetter | None = None,
    echo_func: EchoFunc | None = None,
    logger_factory: LoggerFactory | None = None,
) -> None:
    if logger_factory is not None:
        logger = logger_factory("ml_playground.cli")
    else:
        logger = logging.getLogger("ml_playground.cli")

    echo: EchoFunc = echo_func if echo_func is not None else _default_echo

    if exp_config is not None and not exp_config.exists():
        msg = f"Config file not found: {exp_config}"
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

    ctx_dict = cast(dict[str, Any], ctx.obj)

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

    if callable(context_getter):
        context_fetcher = context_getter  # type: ignore[assignment]
    else:
        context_fetcher = click.get_current_context

    try:
        context = context_fetcher(silent=True)
    except TypeError:
        context = context_fetcher()

    if context is not None and getattr(context, "invoked_subcommand", None) is None:
        echo("Welcome to ML Playground runtime CLI!", err=True)
        echo(
            "No workflow command was provided. Try `uv run ml-playground prepare <experiment>`.",
            err=True,
        )
        echo("", err=True)
        help_text = getattr(context, "get_help", lambda: "")()
        if help_text:
            echo(help_text, err=True)
        # Treat this as an argument error (exit code 2) for consistency
        raise typer.Exit(2)


@app.callback()
def global_options_callback(
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
    _apply_global_options(
        ctx,
        exp_config,
        learning_mode,
        verbosity,
    )


def global_options(
    ctx: typer.Context,
    exp_config: Path | None = None,
    learning_mode: bool = False,
    verbosity: int | VerbosityLevel = 1,
    **overrides: object,
) -> None:
    context_getter = overrides.get("context_getter")
    echo_func = overrides.get("echo_func")
    logger_factory = overrides.get("logger_factory")
    context_getter_cb = cast(
        ContextGetter | None, context_getter if callable(context_getter) else None
    )
    echo_cb = cast(EchoFunc | None, echo_func if callable(echo_func) else None)
    logger_factory_cb = cast(
        LoggerFactory | None, logger_factory if callable(logger_factory) else None
    )
    _apply_global_options(
        ctx,
        exp_config,
        learning_mode,
        verbosity,
        context_getter=context_getter_cb,
        echo_func=echo_cb,
        logger_factory=logger_factory_cb,
    )


def _register_commands() -> None:
    app.command()(commands.prepare)
    app.command()(commands.train)
    app.command()(commands.sample)
    app.command()(commands.analyze)


_register_commands()

__all__ = ["app", "global_options"]
