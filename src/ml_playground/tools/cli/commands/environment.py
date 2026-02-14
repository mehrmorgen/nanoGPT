"""Environment command implementations for the tools CLI system."""

from pathlib import Path
from typing import Callable, List, Optional

import typer
from typing_extensions import Annotated

# Import shared utilities
from ml_playground.tools.cli import helpers
from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.environment.environment import EnvironmentTools


def get_environment_tools() -> EnvironmentTools:
    return helpers.get_environment_tools()


def run_tool_command(
    command_func: Callable[..., ToolResult], *args: object, **kwargs: object
) -> None:
    helpers.run_tool_command(command_func, *args, **kwargs)


# Create environment app
env_app = typer.Typer(
    name="env",
    help="Environment management tools (setup, sync, clean)",
    no_args_is_help=True,
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
    tools = get_environment_tools()
    run_tool_command(
        tools.ai_guidelines,
        args or [],
        tool=tool,
        dry_run=dry_run,
    )


@env_app.command("clean")
def env_clean(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Remove caches and temporary build artifacts."""
    tools = get_environment_tools()
    run_tool_command(
        tools.clean,
        args or [],
    )


@env_app.command("gguf-help")
def env_gguf_help(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show llama.cpp GGUF conversion help."""
    tools = get_environment_tools()
    run_tool_command(
        tools.gguf_help,
        args or [],
    )


@env_app.command("info")
def env_info(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Show environment information."""
    tools = get_environment_tools()
    run_tool_command(
        tools.info,
        args or [],
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
    tools = get_environment_tools()
    run_tool_command(
        tools.setup,
        clear=clear,
        args=args or [],
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
        typer.Option(
            "--frozen", help="Sync from uv.lock without updating dependencies"
        ),
    ] = False,
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional uv sync arguments")
    ] = None,
) -> None:
    """Sync dependencies using uv."""
    tools = get_environment_tools()
    run_tool_command(
        tools.sync,
        groups=groups,
        all_groups=all_groups,
        frozen=frozen,
        args=args or [],
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
    tools = get_environment_tools()
    run_tool_command(
        tools.tensorboard,
        args=args or [],
        logdir=logdir,
        port=port,
        host=host,
    )


@env_app.command("verify")
def env_verify(
    args: Annotated[
        Optional[List[str]], typer.Argument(help="Additional arguments (ignored)")
    ] = None,
) -> None:
    """Ensure the project package imports correctly."""
    tools = get_environment_tools()
    run_tool_command(
        tools.verify,
        args or [],
    )
