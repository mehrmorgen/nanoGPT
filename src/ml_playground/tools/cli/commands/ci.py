from __future__ import annotations

from typing import Callable, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.ci.ci import CITools
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import ToolResult

from .. import helpers as cli_helpers


def get_ci_tools() -> CITools:
    return cli_helpers.get_ci_tools()


def run_tool_command(
    command_func: Callable[..., ToolResult], *args: object, **kwargs: object
) -> None:
    cli_helpers.run_tool_command(command_func, *args, **kwargs)


app = typer.Typer(
    name="ci",
    help="CI/CD operations (quality gates, badges)",
    no_args_is_help=True,
)


@app.command("quality-gate")
def ci_quality_gate(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run the full pre-commit quality gate."""
    try:
        tools = get_ci_tools()
        run_tool_command(tools.quality_gate, args or [])
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("quality-fast")
def ci_quality_fast(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional pre-commit arguments")
    ] = None,
) -> None:
    """Run lint/format focused pre-commit hooks."""
    try:
        tools = get_ci_tools()
        run_tool_command(tools.quality_fast, args or [])
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("quality-ext")
def ci_quality_ext(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Run extended quality gates (mutation testing moved to testing tools)."""
    try:
        tools = get_ci_tools()
        run_tool_command(tools.quality_ext, args or [])
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("quality-ci-local")
def ci_quality_ci_local(
    bind_caches: Annotated[
        bool,
        typer.Option(
            "--bind-caches/--no-bind-caches",
            help="Bind local caches into the act container",
        ),
    ] = True,
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional act arguments")
    ] = None,
) -> None:
    """Run the GitHub quality workflow locally using act."""
    try:
        tools = get_ci_tools()
        run_tool_command(
            tools.quality_ci_local,
            args or [],
            bind_caches=bind_caches,
        )
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("coverage-badge")
def ci_coverage_badge(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Regenerate the SVG coverage badges."""
    try:
        tools = get_ci_tools()
        run_tool_command(tools.coverage_badge, args or [])
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
