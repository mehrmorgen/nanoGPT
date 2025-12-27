from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.errors import ToolExecutionError

from .. import helpers as cli_helpers

app = typer.Typer(
    name="env",
    help="Environment management tools (setup, sync, verify, clean, info, tb, gguf)",
    no_args_is_help=True,
)


@app.command("setup")
def env_setup(
    clear: Annotated[
        bool, typer.Option("--clear", help="Remove existing virtual environment first")
    ] = False,
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Create a fresh uv-managed virtual environment and install all dependencies."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.setup(args or [], clear=clear)
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("sync")
def env_sync(
    groups: Annotated[
        Optional[list[str]],
        typer.Option("--group", help="Sync specific dependency groups (repeatable)"),
    ] = None,
    all_groups: Annotated[
        bool,
        typer.Option("--all-groups", help="Install all optional dependency groups"),
    ] = False,
    frozen: Annotated[
        bool,
        typer.Option(
            "--frozen", help="Use existing lockfile without resolving new versions"
        ),
    ] = False,
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional uv sync arguments")
    ] = None,
) -> None:
    """Sync project dependencies using uv."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.sync(
            args or [], groups=groups, all_groups=all_groups, frozen=frozen
        )
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("verify")
def env_verify(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Ensure the project package imports correctly."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.verify(args or [])
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("clean")
def env_clean(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove caches and temporary build artifacts."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.clean(args or [])
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("info")
def env_info(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show environment information."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.info(args or [])
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("ai-guidelines")
def env_ai_guidelines(
    tool: Annotated[str, typer.Argument(help="Target tool name for AI guidelines")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview actions without executing")
    ] = False,
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Set up AI guideline symlinks for the requested tool."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.ai_guidelines(args or [], tool=tool, dry_run=dry_run)
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("tensorboard")
def env_tensorboard(
    logdir: Annotated[
        Path,
        typer.Option(
            "--logdir",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            help="TensorBoard log directory",
        ),
    ],
    port: Annotated[
        int, typer.Option("--port", help="Port to bind TensorBoard to")
    ] = 6006,
    host: Annotated[
        str, typer.Option("--host", help="Host interface to bind to")
    ] = "127.0.0.1",
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional tensorboard arguments")
    ] = None,
) -> None:
    """Launch TensorBoard for the given log directory."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.tensorboard(args or [], logdir=logdir, port=port, host=host)
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


@app.command("gguf-help")
def env_gguf_help(
    args: Annotated[
        Optional[list[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show llama.cpp GGUF conversion help."""
    try:
        tools = cli_helpers.get_environment_tools()
        result = tools.gguf_help(args or [])
        cli_helpers.handle_tool_result(result)
    except ToolExecutionError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)


def build_app() -> typer.Typer:
    return app
