from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any, Callable, Optional, cast

import click
import typer

from ml_playground.framework.runtime import helpers
from ml_playground.framework.runtime.core.results import (
    LearningModeEngine,
    VerbosityLevel,
)

from .runners import (
    get_cli_dependencies,
    run_prepare_cmd,
    run_sample_cmd,
    run_train_cmd,
)

_CONTEXT_HOOK: Callable[..., Any] = click.get_current_context


def set_click_context_hook(hook: Callable[..., Any]) -> None:
    global _CONTEXT_HOOK
    _CONTEXT_HOOK = hook


def reset_click_context_hook() -> None:
    set_click_context_hook(click.get_current_context)


EXPERIMENT_HELP = "Experiment name (directory in src/ml_playground/experiments)"

app = typer.Typer(
    help="ML Playground CLI",
    no_args_is_help=True,
    add_completion=True,
)


@app.callback()
def global_options(
    ctx: typer.Context,
    exp_config: Annotated[
        Optional[Path],
        typer.Option(
            "--exp-config",
            "-c",
            help="Path to experiment config TOML (relative to experiment dir or absolute)",
            exists=False,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    learning_mode: Annotated[
        bool,
        typer.Option(
            "--learning-mode",
            "-l",
            help="Enable learning mode (active research/development)",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Show more output. Can be used twice (up to -vv).",
            max=2,
        ),
    ] = 1,
) -> None:
    """Global options applied to all subcommands."""
    apply_global_options(
        ctx,
        exp_config=exp_config,
        learning_mode=learning_mode,
        verbosity=verbosity,
    )


def apply_global_options(
    ctx: typer.Context,
    exp_config: Annotated[
        Optional[Path], typer.Option(help="Path to config TOML")
    ] = None,
    learning_mode: Annotated[bool, typer.Option(help="Enable learning mode")] = False,
    verbosity: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Shared global options handler used by Typer callback and programmatic callers."""

    logger = logging.getLogger("ml_playground.cli")
    echo = typer.echo

    if exp_config is not None:
        if not exp_config.exists():
            msg = f"Config file not found: {exp_config}"
            logger.error(msg)
            echo(msg, err=True)
            raise typer.Exit(code=2)

    # Standardize on a dict object for the context
    if ctx.obj is None:
        ctx.obj = {}

    if not isinstance(ctx.obj, dict):
        try:
            ctx.ensure_object(dict)
        except (AttributeError, TypeError):
            # Fallback for mock objects in tests
            if ctx.obj is None:
                ctx.obj = {}

    ctx_dict = cast(dict[str, Any], ctx.obj)

    verbosity_level = (
        verbosity
        if isinstance(verbosity, VerbosityLevel)
        else VerbosityLevel(min(2, max(0, verbosity)))
    )

    ctx_dict["exp_config"] = exp_config
    ctx_dict["learning_mode"] = learning_mode
    if verbosity_level != VerbosityLevel.STANDARD:
        ctx_dict["verbosity"] = verbosity_level


_apply_global_options = apply_global_options


@app.command()
def prepare(
    ctx: typer.Context,
    experiment: Annotated[str, typer.Argument(help=EXPERIMENT_HELP)],
) -> None:
    """Run data preparation for an experiment."""
    deps = get_cli_dependencies()
    exp_config, learning_mode, _, engine = deps_from_ctx(ctx)

    run_prepare_cmd(
        experiment,
        exp_config,
        deps,
        learning_mode=learning_mode,
        learning_engine=engine,
    )


@app.command()
def train(
    ctx: typer.Context,
    experiment: Annotated[str, typer.Argument(help=EXPERIMENT_HELP)],
) -> None:
    """Run model training for an experiment."""
    deps = get_cli_dependencies()
    exp_config, learning_mode, _, engine = deps_from_ctx(ctx)

    run_train_cmd(
        experiment,
        exp_config,
        deps,
        learning_mode=learning_mode,
        learning_engine=engine,
    )


@app.command()
def sample(
    ctx: typer.Context,
    experiment: Annotated[str, typer.Argument(help=EXPERIMENT_HELP)],
) -> None:
    """Run model sampling for an experiment."""
    deps = get_cli_dependencies()
    exp_config, learning_mode, _, engine = deps_from_ctx(ctx)

    run_sample_cmd(
        experiment,
        exp_config,
        deps,
        learning_mode=learning_mode,
        learning_engine=engine,
    )


@app.command()
def analyze(
    ctx: typer.Context,
    experiment: Annotated[str, typer.Argument(help=EXPERIMENT_HELP)],
    host: Annotated[str, typer.Option(help="Server host")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Server port")] = 5432,
    open_browser: Annotated[
        bool, typer.Option(help="Open browser automatically")
    ] = False,
) -> None:
    """Run interactive analysis (e.g. LIT) for an experiment."""
    deps = get_cli_dependencies()
    _, learning_mode, _, engine = deps_from_ctx(ctx)

    result = deps.run_analyze(
        experiment,
        host,
        port,
        open_browser,
        engine,
    )
    deps.handle_tool_result(result, learning_mode)


def complete_experiments(incomplete: str) -> list[str]:
    """Typer shell completion for experiment names."""
    deps = get_cli_dependencies()
    return deps.list_experiments(incomplete)


def deps_from_ctx(
    ctx: typer.Context,
) -> tuple[Optional[Path], bool, VerbosityLevel, Optional[LearningModeEngine]]:
    """Extract standard global options from context."""
    ctx_obj_raw = getattr(ctx, "obj", None)
    exp_config = None
    learning_mode = False
    verbosity = VerbosityLevel.STANDARD
    injected_engine = None

    # Support both dict and objects with attributes
    if isinstance(ctx_obj_raw, dict):
        ctx_dict = cast(dict[str, object], ctx_obj_raw)
        learning_mode = bool(ctx_dict.get("learning_mode", False))
        verbosity_raw = ctx_dict.get("verbosity", VerbosityLevel.STANDARD)
        injected_engine = cast(
            Optional[LearningModeEngine], ctx_dict.get("learning_engine")
        )
        exp_config = cast(Optional[Path], ctx_dict.get("exp_config"))
    else:
        # Fallback to extract from context if ctx.obj hasn't been populated by callback properly
        exp_config = helpers.extract_exp_config(ctx)
        learning_mode = False
        verbosity_raw = VerbosityLevel.STANDARD
        injected_engine = None

    if isinstance(verbosity_raw, VerbosityLevel):
        verbosity = verbosity_raw
    elif isinstance(verbosity_raw, int):
        verbosity = VerbosityLevel(min(2, max(0, verbosity_raw)))

    return exp_config, learning_mode, verbosity, injected_engine


def main(*args: Any, **kwargs: Any) -> None:
    """Project-level entry point for runtime CLI.

    Supports both console_script execution (no args) and e2e test calls (with args).
    """
    try:
        if args:
            # Typer app expects either no args (uses sys.argv) or a list of strings
            if len(args) == 1 and isinstance(args[0], list):
                app(args[0])
            else:
                app(list(args))
        else:
            app()
    except SystemExit as exc:
        if args:
            # For programmatic calls, only re-raise if it's an actual error
            if exc.code != 0:
                raise
        else:
            # For console_script, always re-raise to ensure correct exit code
            raise


def main_entry() -> None:
    """Canonical entry point alias for pyproject.toml."""
    main()


def get_command(app: typer.Typer) -> Any:
    """Get click command from Typer app (for testing)."""
    return typer.main.get_command(app)


if __name__ == "__main__":
    main()
