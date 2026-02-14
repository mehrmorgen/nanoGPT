from __future__ import annotations

from typing import Callable, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.quality.quality import QualityTools

from .. import helpers as cli_helpers
from ..state import state


def get_quality_tools() -> QualityTools:
    return cli_helpers.get_quality_tools()


def run_tool_command(
    command_func: Callable[..., ToolResult], *args: object, **kwargs: object
) -> None:
    cli_helpers.run_tool_command(command_func, *args, **kwargs)


app = typer.Typer(
    help="Code quality tools (lint, format, typecheck)",
    no_args_is_help=True,
)


@app.command("lint")
def quality_lint(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff lint checks."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.lint,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("format")
def quality_format(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Auto-fix and format code with Ruff."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.format,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("lint-check")
def quality_lint_check(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional ruff arguments")
    ] = None,
) -> None:
    """Run Ruff in check-only mode (alias for lint)."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.lint_check,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("deadcode")
def quality_deadcode(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional vulture arguments")
    ] = None,
) -> None:
    """Scan for dead code using vulture."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.deadcode,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("basedpyright")
def quality_basedpyright(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional basedpyright arguments")
    ] = None,
) -> None:
    """Run BasedPyright type checks."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.basedpyright,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("mypy")
def quality_mypy(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional mypy arguments")
    ] = None,
) -> None:
    """Run Mypy type checks."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.mypy,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("typecheck")
def quality_typecheck(
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments (applied to both tools)"),
    ] = None,
) -> None:
    """Run both BasedPyright and Mypy type checks."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.typecheck,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("all")
def quality_all(
    args: Annotated[
        Optional[list[str]],
        typer.Argument(help="Additional arguments (applied to all tools)"),
    ] = None,
) -> None:
    """Run all quality checks (lint, typecheck, deadcode)."""
    try:
        tools = get_quality_tools()
        run_tool_command(
            tools.all_checks,
            args or [],
            learning_mode=state.learning_mode,
            verbosity_level=state.verbosity,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    """Provide app for dynamic mounting."""
    return app
