from __future__ import annotations

from typing import Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import ToolExecutionError

from .. import helpers as cli_helpers

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
        tools = cli_helpers.get_ci_tools()
        result = tools.quality_gate(args or [])
        cli_helpers.handle_tool_result(result)
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
        tools = cli_helpers.get_ci_tools()
        result = tools.quality_fast(args or [])
        cli_helpers.handle_tool_result(result)
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
        tools = cli_helpers.get_ci_tools()
        result = tools.quality_ext(args or [])
        cli_helpers.handle_tool_result(result)
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
        tools = cli_helpers.get_ci_tools()
        result = tools.quality_ci_local(args or [], bind_caches=bind_caches)
        cli_helpers.handle_tool_result(result)
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
        tools = cli_helpers.get_ci_tools()
        result = tools.coverage_badge(args or [])
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
