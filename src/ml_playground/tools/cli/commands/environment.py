"""Environment command implementations for the tools CLI system."""

from pathlib import Path
from typing import List, Optional

import typer
from typing_extensions import Annotated

# Import shared utilities
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.cli.helpers import (
    get_environment_tools,
    handle_tool_result,
)
# Create environment app
env_app = typer.Typer(
    name="env",
    help="Environment management tools (setup, sync, clean)",
    no_args_is_help=True,
)


@env_app.command("setup")
def env_setup(
    clear: Annotated[
        bool, typer.Option("--clear", help="Remove existing virtual environment first")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Create a fresh uv-managed virtual environment and install all dependencies."""
    try:
        tools = get_environment_tools()
        result = tools.setup(clear=clear, args=args or [])
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="setup",
                stderr=f"Error setting up environment: {e}",
            )
        )


@env_app.command("sync")
def env_sync(
    groups: Annotated[
        Optional[List[str]],
        typer.Option("--group", help="Sync specific dependency groups (repeatable)"),
    ] = None,
    all_groups: Annotated[
        bool,
        typer.Option("--all-groups", help="Install all optional dependency groups"),
    ] = False,
    frozen: Annotated[
        bool,
        typer.Option("--frozen", help="Sync from uv.lock without updating dependencies"),
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional uv sync arguments")
    ] = None,
) -> None:
    """Sync dependencies using uv."""
    try:
        tools = get_environment_tools()
        result = tools.sync(
            args or [], groups=groups or [], all_groups=all_groups, frozen=frozen
        )
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="sync",
                stderr=f"Error syncing environment: {e}",
            )
        )


@env_app.command("verify")
def env_verify(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Ensure the project package imports correctly."""
    try:
        tools = get_environment_tools()
        result = tools.verify(args or [])
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="verify",
                stderr=f"Error verifying environment: {e}",
            )
        )


@env_app.command("clean")
def env_clean(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove caches and temporary build artifacts."""
    try:
        tools = get_environment_tools()
        result = tools.clean(args or [])
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="clean",
                stderr=f"Error cleaning environment: {e}",
            )
        )


@env_app.command("info")
def env_info(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show environment information."""
    try:
        tools = get_environment_tools()
        result = tools.info(
            args or [],
        )
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="info",
                stderr=f"Error getting environment info: {e}",
            )
        )


@env_app.command("ai-guidelines")
def env_ai_guidelines(
    tool: Annotated[str, typer.Argument(help="Target tool name for AI guidelines")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Preview actions without executing")
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Set up AI guideline symlinks for the requested tool."""
    try:
        tools = get_environment_tools()
        result = tools.ai_guidelines(args or [], tool=tool, dry_run=dry_run)
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="ai-guidelines",
                stderr=f"Error setting up AI guidelines: {e}",
            )
        )


@env_app.command("tensorboard")
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
    ] = Path("logs"),
    port: Annotated[
        int,
        typer.Option("--port", help="TensorBoard server port"),
    ] = 6006,
    host: Annotated[
        str,
        typer.Option("--host", help="TensorBoard server host"),
    ] = "localhost",
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Start TensorBoard server."""
    try:
        tools = get_environment_tools()
        result = tools.tensorboard(args or [], logdir=logdir, port=port, host=host)
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="tensorboard",
                stderr=f"Error starting TensorBoard: {e}",
            )
        )


@env_app.command("gguf-help")
def env_gguf_help(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show llama.cpp GGUF conversion help."""
    try:
        tools = get_environment_tools()
        result = tools.gguf_help(args or [])
        handle_tool_result(result)
    except Exception as e:
        handle_tool_result(
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="env",
                command="gguf-help",
                stderr=f"Error showing GGUF help: {e}",
            )
        )
